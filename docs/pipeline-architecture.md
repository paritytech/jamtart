# Pipeline Architecture: Life of an Event

## The Problem

TART (Testing, Analytics and Research Telemetry) ingests binary telemetry from up to 1,024 concurrent JAM blockchain nodes over TCP, enriches each event with cross-event correlation data, persists it to PostgreSQL, and simultaneously streams it to WebSocket clients in real time. The core challenge is doing all of this at 600K+ events/second without dropping data, starving any consumer, or creating lock contention between the ingestion and output paths.

## Big Picture

```
                       JAM Nodes (up to 1024)
                             |
                    TCP port 9000 (binary)
                             |
            +----------------+----------------+
            |                |                |
     +-----------+    +-----------+    +-----------+
     | ingestion |    | ingestion |    | ingestion |    x8 (default)
     | runtime 0 |    | runtime 1 |    | runtime 7 |    each: dedicated OS thread
     | (tokio    |    | (tokio    |    | (tokio    |          + single-thread tokio runtime
     |  current) |    |  current) |    |  current) |          + SO_REUSEPORT listener
     +-----------+    +-----------+    +-----------+
            |                |                |
            |  Per-connection: decode -> rate-limit -> enrich -> build WS JSON
            |                |                |
            +-------+--------+--------+-------+
                    |                  |
       try_send (non-blocking)   try_send (non-blocking)
                    |                  |
                    v                  v
     +---------------------------+  +---------------------------+
     |  tokio mpsc (5M capacity) |  | tokio mpsc (500K capacity)|
     |     BatchWriter chan      |  |   EventBroadcaster chan   |
     +---------------------------+  +---------------------------+
                    |                  |
          (main tokio runtime)   (main tokio runtime)
                    |                  |
     +--------------+------+     +----v----+
     |  8 writer workers   |     |Aggregator|  single tokio task
     |  (tokio tasks,      |     | task     |  routes to:
     |   work-stealing     |     +----+-----+   - broadcast channel (500K)
     |   via Arc<Mutex<Rx>>)|          |        - per-node channels (10K each)
     +---------+-----------+          |        - ring buffer (10K recent)
               |                      |        - MetricsTracker (mpsc 50K)
               v                      |
     +-------------------+            v
     |   PostgreSQL      |    +---------------+
     |   (COPY batches)  |    | broadcast::Rx |  per WS client
     +-------------------+    +-------+-------+
                                      |
                              +-------v-------+
                              |  Axum WS      |
                              |  handlers     |  HTTP port 8080
                              +---------------+
```

One paragraph to tie it together: each TCP connection is handled by a task on one of 8 dedicated ingestion runtimes. Inside that task, events are decoded, rate-limited, enriched with correlation context (core, services, wp_hash), and pre-serialized into JSON. The enriched event is then fanned out over two non-blocking channels: one to the `BatchWriter` for PostgreSQL persistence, and one to the `EventBroadcaster` for real-time WebSocket delivery. Both consumers run on the main tokio multi-thread runtime, completely decoupled from the ingestion hot path.

## Meet the Characters

| Component | Role | Source |
|-----------|------|--------|
| `TelemetryServer` | Owns the TCP listener(s), spawns per-connection tasks, holds shared state | [server.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/server.rs#L120) |
| `ConnectionContext` | Per-connection bundle of cloned `Arc` handles to shared components | [server.rs#L531](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/server.rs#L531) |
| `NodeEventEnricher` | Per-node stateful enricher -- correlates submission IDs, cores, service IDs across events | [enricher.rs#L48](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/enricher.rs#L48) |
| `SlotTracker` | `DashMap<u32, SlotState>` -- tracks block propagation convergence per slot | [slot_tracker.rs#L12](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/slot_tracker.rs#L12) |
| `WpTracker` | `DashMap<[u8;32], WpState>` -- tracks work package pipeline stages by hash | [wp_tracker.rs#L15](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/wp_tracker.rs#L15) |
| `BatchWriter` | Channels events to 8 writer workers for batched PostgreSQL COPY writes | [batch_writer.rs#L51](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/batch_writer.rs#L51) |
| `EventBroadcaster` | Funnels events through an aggregator task to `broadcast` channels and a ring buffer | [event_broadcaster.rs#L168](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/event_broadcaster.rs#L168) |
| `MetricsTracker` | Single-writer task computing block propagation and WP pipeline snapshots in memory | [metrics_tracker.rs#L27](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/metrics_tracker.rs#L27) |
| `LiveCounters` | Lock-free atomic ring buffer of 60 per-second buckets for real-time counters | [live_counters.rs#L11](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/live_counters.rs#L11) |
| `RateLimiter` | Lock-free per-node token bucket using packed `AtomicU64` CAS | [rate_limiter.rs#L60](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/rate_limiter.rs#L60) |

## The Story

### Act 1: TCP Ingestion -- Bytes to Events

A JAM node opens a TCP connection to port 9000. The kernel distributes the connection to one of the 8 `SO_REUSEPORT` listeners via consistent hashing.

Each listener runs on a **dedicated OS thread** with its own **single-thread tokio runtime** (`new_current_thread`). This is the key design decision: by isolating ingestion from the main runtime's work-stealing scheduler, connection handlers never compete with DB writers or HTTP handlers for CPU time.

```rust
let handle = std::thread::Builder::new()
    .name(format!("ingestion-{i}"))
    .spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move { /* accept loop */ });
    })
```
[Source: server.rs#L315-L321](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/server.rs#L315-L321)

When a connection arrives, `handle_connection_optimized` spawns as a tokio task **on the same single-thread runtime**. It reads the first message (always `NodeInformation` -- the node's identity), then enters an event loop:

1. **Read** TCP bytes into a `BytesMut` buffer (8KB initial, 1MB max)
2. **Decode** all complete frames from the buffer (length-prefix framing: 4-byte LE size + payload)
3. **Rate-limit** via `RateLimiter::allow_event()` -- a lock-free CAS on a packed `AtomicU64` (count + window start in one 8-byte word)
4. **Serialize** the event to JSON once (`serde_json::to_vec`), wrap in `Arc<[u8]>` for zero-copy sharing
5. **Enrich** via the per-node `NodeEventEnricher` (more on this below)
6. **Update trackers** -- `SlotTracker` and `WpTracker` are `DashMap`s written inline
7. **Build WS envelope** -- pre-serializes the full WebSocket JSON using `serde_json::RawValue` to avoid double-serialization
8. **Batch-send** -- accumulated events from a single TCP read are sent as one `Vec` to both channels

Here is the tricky part: steps 4-7 happen **inside the ingestion thread**, parallelized across 8 runtimes. This moves the expensive serialization work off the main runtime. The channels receive pre-built payloads.

```
  TCP read wakeup (N events coalesced in one segment)
       |
       v
  +--- inner decode loop (no I/O, no await) ---+
  | for each frame:                             |
  |   decode -> rate-limit -> serialize ->      |
  |   enrich -> update trackers -> build WS     |
  |   -> push to broadcast_batch + db_batch     |
  +---------------------------------------------+
       |
       v
  Two channel sends (one for broadcaster, one for batch_writer)
       |
       v
  Next TCP read (blocks until data arrives)
```

### Act 2: Enrichment -- Cross-Event Correlation

Most telemetry events carry a `submission_or_share_id` or `submission_id` that ties them to a work package. But only the initial `WorkPackageReceived` event carries the core index, service IDs, and WP hash. The `NodeEventEnricher` solves this by maintaining per-node lookup maps:

```
WorkPackageReceived (sub_id=100, core=5, services=[10,20], wp_hash=0xAB)
     |
     +---> submissions[100] = {core: 5, wp_hash: 0xAB, services: [10,20]}

Authorized (sub_id=100)  --lookup--> enriched: core=5, services=[10,20], wp_hash=0xAB
Refined (sub_id=100)     --lookup--> same
GuaranteeBuilt (sub_id=100)  --lookup + store--> built_ids[event_id] = {core: 5}
SendingGuarantee (built_id=N) --lookup--> sending_ids[event_id] = {core: 5}
GuaranteeSent (sending_id=M) --lookup--> core=5
```

Each map is capped at 10,000 entries (hard-cleared on overflow) and stale entries are evicted every 1,000 calls. The enricher itself is stored in an `EnricherMap` (`DashMap<NodeId, NodeEventEnricher>`) keyed by node ID. Stale enrichers (no activity for 60s) are swept every ~2.5 minutes from a periodic task in `main.rs`.

[Source: enricher.rs#L91](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/enricher.rs#L91)

### Act 3: Database Persistence -- The BatchWriter

The `BatchWriter` uses a **work-stealing pool** pattern: 8 writer workers share a single `mpsc::Receiver` wrapped in `Arc<Mutex<Receiver>>`.

```
  Ingestion threads (8)
       |
  try_send(EventBatch{...})  <-- non-blocking, drops if channel full
       |
       v
  +-----------------------------+
  | tokio mpsc (5M capacity)    |
  +-----------------------------+
       |
  +----+----+----+----+----+----+----+----+
  | W0 | W1 | W2 | W3 | W4 | W5 | W6 | W7 |   8 tokio tasks
  +----+----+----+----+----+----+----+----+
       |
       | Phase 1: Lock mutex, drain via try_recv() (microseconds)
       | Phase 2: Release mutex
       | Phase 3: PostgreSQL COPY batch flush (milliseconds, no lock held)
       |
       v
  PostgreSQL (events table, event_services, node_stats)
```

Each worker blocks on `rx.recv()` for the first event, then drains aggressively for up to 100ms or 16,000 events (whichever comes first). While one worker is blocked on a DB COPY, the other 7 pick up work. This is the "work-stealing" part -- no explicit scheduling, just mutex contention that naturally load-balances.

Node stats (event counts per node) follow a different path: each worker accumulates local counts in a `HashMap`, then merges them into a shared `HashMap` every 5 seconds. A separate dedicated task flushes that aggregated map to the database, preventing deadlocks from concurrent UPDATE statements.

[Source: batch_writer.rs#L242](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/batch_writer.rs#L242)

### Act 4: WebSocket Output -- The EventBroadcaster

The broadcaster uses an **aggregator pattern** to avoid lock contention: a single tokio task owns all mutable state (node channels, ring buffer) and communicates with the outside world through channels only.

```
  Ingestion threads (8)
       |
  try_send(IncomingBatch)  <-- lock-free mpsc (500K capacity)
       |
       v
  +---------------------+
  | Aggregator task      |  single tokio task on main runtime
  |                      |
  |  for each record:    |
  |   1. broadcast::send |  -> main channel (500K, all-events)
  |   2. per-node send   |  -> node channel (10K per node)
  |   3. ring buffer push|  -> VecDeque<Arc<BroadcastEvent>> (10K)
  |   4. metrics_tx send |  -> MetricsTracker channel (50K)
  +---------------------+
       |                  \
  (broadcast::Rx)    (AggregatorCommand mpsc)
       |                  |
  WS client tasks    subscribe_node() / subscribe_all_nodes()
  (read from Rx)     (oneshot reply with broadcast::Rx)
```

WebSocket clients subscribe by sending an `AggregatorCommand` through a command channel. The aggregator replies with a `broadcast::Receiver` that the WS handler reads from. This keeps the node channel `HashMap` owned by a single task -- no shared locks.

The aggregator drains up to 10,000 events per cycle before yielding to tokio, preventing WS loop starvation that was observed at 700ms stalls without this limit.

[Source: event_broadcaster.rs#L346](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/event_broadcaster.rs#L346)

### Act 5: In-Memory Analytics -- MetricsTracker and LiveCounters

The `MetricsTracker` receives every event through a filtered `mpsc` channel (50K capacity) from the aggregator. It runs as a **single tokio task** that owns all mutable state -- no locks for writes. It computes:

- **Block propagation**: measures wall-clock delay between `BlockAnnounced` and `BlockTransferred` for the same slot across different nodes
- **WP pipeline timing**: tracks `WorkPackageReceived -> Authorized -> Refined -> WorkReportBuilt -> GuaranteeBuilt -> GuaranteesDistributed` per submission ID
- **Core status**: active/idle/stale classification per core

Snapshots are rebuilt every 2 seconds and published via `RwLock<Arc<Value>>` -- API handlers read a cloned `Arc` (cheap) while the tracker task writes behind the lock.

`LiveCounters` takes a different approach: it is a ring buffer of 60 `SecondBucket` structs, each containing `AtomicU64` fields for events, blocks, finalized blocks, announcements, and tickets. The MetricsTracker task calls `record()` on every event (one atomic increment), and API handlers can `sum_last_n_seconds()` at any time without locking. This replaced two SQL queries that took 2.7s and 3.2s.

### Act 6: Periodic Flush Tasks

Two DashMap trackers (`SlotTracker` and `WpTracker`) accumulate state in the ingestion threads but flush to PostgreSQL from a **single periodic task** on the main runtime (every 5 seconds):

```rust
tokio::spawn(async move {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        interval.tick().await;
        flush_slot_tracker(&slot_tracker, &pool).await;
        flush_wp_tracker(&wp_tracker, &pool).await;
        // Sweep stale enrichers every ~2.5 min
        tick_count += 1;
        if tick_count % 30 == 0 {
            enricher_map.retain(|_, e| !e.is_stale());
        }
    }
});
```
[Source: main.rs#L242-L256](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/main.rs#L242-L256)

Both flushers follow a three-phase pattern to avoid holding DashMap guards across `.await` points: (1) iterate and snapshot dirty entries, (2) write to DB without any guards, (3) clear dirty flags and evict stale entries.

## Deep Dives

### Threading Model

This is worth a dedicated breakdown, since the system uses three distinct threading strategies:

| Component | Thread type | Count | Runtime | Why |
|-----------|------------|-------|---------|-----|
| Ingestion listeners | Dedicated OS threads | 8 (configurable) | `tokio::runtime::Builder::new_current_thread` per thread | Isolate TCP I/O from main runtime work-stealing; `SO_REUSEPORT` distributes connections at kernel level |
| Connection handlers | tokio tasks | 1 per connection (up to 1024) | Runs on owning ingestion thread's runtime | Cooperative multitasking within each ingestion thread |
| BatchWriter workers | tokio tasks | 8 | Main multi-thread runtime | DB I/O is bursty; work-stealing via `Arc<Mutex<Rx>>` naturally balances load |
| Node stats flusher | tokio task | 1 | Main runtime | Aggregates counts from all writers, prevents deadlocks |
| Aggregator (broadcaster) | tokio task | 1 | Main runtime | Single-owner of node channels HashMap; routes events to broadcast channels |
| MetricsTracker | tokio task | 1 | Main runtime | Single-writer for propagation/pipeline state; publishes snapshots via RwLock |
| Tracker flush (slot+wp) | tokio task | 1 | Main runtime | Periodic 5s tick; DashMap snapshot -> DB write |
| Cache warming | tokio task | 1 orchestrator + N spawned | Main runtime | Fires 15+ concurrent SQL queries every 2s, results stored in TtlCache |
| HTTP/WS server | Axum (tower) | Shared on main runtime | Main multi-thread runtime | `axum::serve` uses hyper under the hood |

The main runtime is the default `#[tokio::main]` multi-thread runtime (number of worker threads = number of CPU cores).

### Channel Types and Capacities

```
                        Channel Map
  +---------------------------------------------------------+
  | Channel                  | Type           | Capacity    |
  |--------------------------|----------------|-------------|
  | BatchWriter events       | tokio mpsc     | 5,000,000   |
  | Broadcaster events       | tokio mpsc     | 500,000     |
  | Broadcaster main         | tokio broadcast| 500,000     |
  | Per-node broadcast       | tokio broadcast| 10,000      |
  | Broadcaster commands     | tokio mpsc     | 256         |
  | MetricsTracker events    | tokio mpsc     | 50,000      |
  | Shutdown signal          | tokio watch    | 1 (latest)  |
  | Connection count watch   | tokio watch    | 1 (latest)  |
  +---------------------------------------------------------+
```

The ingestion side uses `try_send()` on both the BatchWriter and Broadcaster channels. If either is full, events are dropped with metrics counters incremented -- the system prioritizes liveness over completeness under extreme backpressure.

### Drop Strategy (Why No Backpressure)

Telemetry sources (JAM nodes) produce events at whatever rate the network dictates — they won't slow down because our pipeline is full. Backpressure would just move the drop point into the node (TCP window fills → node's send blocks → node's internal telemetry buffer fills → node drops), introducing latency and memory pressure inside the node itself. The correct design is: accept everything, drop at the earliest internal point where we can't keep up, and count what was dropped.

The system has three drop points, ordered from earliest to latest:

1. **Rate limiter** (1000 events/s + 200 burst per node): drops events, increments `telemetry_events_rate_limited` counter
2. **BatchWriter channel full** (5M capacity): `try_send` returns error, connection handler counts dropped events, logs every 500
3. **Broadcaster channel full** (500K capacity): `try_send` returns false, event lost from WS stream

The large channel capacities (5M, 500K) aren't wasteful — they absorb transient DB stalls (slow COPY, connection pool exhaustion, autovacuum) without dropping events during temporary hiccups.

In `--no-database` mode, the BatchWriter still drains its channel (to prevent OOM) but discards events instead of writing to PostgreSQL.

### Why `Arc<str>` for Node IDs

Node IDs appear in every event record, broadcast record, and DB write. Using `String` would mean a 64-byte heap allocation per clone. `Arc<str>` clone is a single atomic increment -- at 600K events/second across 1024 nodes, this saves millions of allocations per second.

## Quick Reference

| Term | Meaning |
|------|---------|
| Ingestion runtime | Dedicated OS thread + single-thread tokio runtime, one per `SO_REUSEPORT` listener |
| EnricherMap | `DashMap<NodeId, NodeEventEnricher>` -- per-node cross-event correlation state |
| SlotTracker | `DashMap<u32, SlotState>` -- convergence timing for block propagation per slot |
| WpTracker | `DashMap<[u8;32], WpState>` -- work package pipeline stage tracking by hash |
| Writer worker | One of 8 tokio tasks sharing a single `mpsc::Receiver` for work-stealing DB writes |
| Aggregator task | Single tokio task that owns broadcaster node channels, ring buffer, and event routing |
| LiveCounters | 60-bucket atomic ring buffer for per-second event counting (lock-free reads) |
| JCE epoch | Timestamp epoch used in JAM telemetry: Unix micros offset by `JCE_EPOCH_UNIX_MICROS` |
| `try_send` | Non-blocking channel send; returns error if full (backpressure point) |
| `SO_REUSEPORT` | Linux socket option allowing multiple listeners on the same port; kernel distributes connections |

## Source Files

- [main.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/main.rs) - CLI parsing, component wiring, periodic tasks, HTTP server startup
- [server.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/server.rs) - `TelemetryServer`, `SO_REUSEPORT` listener creation, connection handling, event decode/enrich loop
- [batch_writer.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/batch_writer.rs) - Work-stealing writer pool, PostgreSQL COPY batch flush, node stats aggregation
- [event_broadcaster.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/event_broadcaster.rs) - Aggregator task, broadcast/per-node channels, ring buffer, WS envelope construction
- [enricher.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/enricher.rs) - `NodeEventEnricher` with submission/built/sending/request/reconstructing correlation maps
- [slot_tracker.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/slot_tracker.rs) - Block convergence tracking and `slot_convergence` table flushing
- [wp_tracker.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/wp_tracker.rs) - Work package pipeline stage tracking and `wp_tracking` table flushing
- [metrics_tracker.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/metrics_tracker.rs) - In-memory block propagation and WP pipeline analytics, snapshot publishing
- [live_counters.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/live_counters.rs) - Lock-free atomic per-second sliding window counters
- [rate_limiter.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/rate_limiter.rs) - Lock-free per-node rate limiting with packed `AtomicU64` CAS
- [decoder.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/decoder.rs) - Binary protocol decoding (length-prefix framing, JAM event types)
- [store.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/store.rs) - PostgreSQL `EventStore` with COPY-based batch inserts and analytics queries
- [api.rs](https://github.com/paritytech/jamtart/blob/cee42d7b3323c87d24c0a47074f16b5eee7a685b/src/api.rs) - Axum HTTP/WS router, REST endpoints, WebSocket upgrade handlers
