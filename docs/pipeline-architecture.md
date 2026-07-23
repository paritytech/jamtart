# Pipeline Architecture: Life of an Event

## The Problem

TART (Testing, Analytics and Research Telemetry) ingests binary telemetry from up to 1,024 concurrent JAM blockchain nodes over TCP, enriches each event with cross-event correlation data, aggregates it into TimescaleDB, and simultaneously streams it to WebSocket clients in real time. The core challenge is doing all of this at a design target of ~3M events/second without dropping data, starving any consumer, or creating lock contention between the ingestion and output paths.

Storage follows one rule: **do the work at ingestion time**. Raw events are kept only for 1 hour (browsing/drilldown); everything long-lived is aggregated *as events arrive* — per-group count tables, lifecycle trackers, latency histograms — so read queries never scan raw events.

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
            |  Per-connection: decode -> rate-limit -> enrich
            |    -> count + update trackers (in-memory) -> build WS JSON
            +-------+--------+--------+-------+
                    |                  |                    \
       try_send (non-blocking)   try_send (non-blocking)    inline DashMap writes
                    |                  |                          |
                    v                  v                          v
     +---------------------------+  +---------------------------+  +--------------------------+
     |  tokio mpsc (5M capacity) |  | tokio mpsc (500K capacity)|  | event_counter DashMap    |
     |     BatchWriter chan      |  |   EventBroadcaster chan   |  | slot / wp / convergence  |
     +---------------------------+  +---------------------------+  | / DA trackers (DashMaps) |
                    |                  |                           +------------+-------------+
          (main tokio runtime)   (main tokio runtime)                           |
                    |                  |                              periodic flush task
     +--------------+------+     +----v----+                          (5s tick, COPY BINARY)
     |  8 writer workers   |     |Aggregator|  single tokio task                 |
     |  (tokio tasks,      |     | task     |  routes to:                        v
     |   work-stealing     |     +----+-----+   - broadcast channel   +----------------------+
     |   via Arc<Mutex<Rx>>)|          |        - per-node channels   | 14 count tables      |
     +---------+-----------+          |        - ring buffer          | + tracker/histogram  |
               |                      |        - MetricsTracker       |   tables             |
               v                      v                               +----------------------+
     +----------------------+  +---------------+
     | ingested_raw_events  |  | broadcast::Rx |  per WS client
     | (1h retention)       |  +-------+-------+
     | + aux tables (COPY)  |          |
     +----------------------+  +-------v-------+
                               |  Axum WS      |
                               |  handlers     |  HTTP port 8080
                               +---------------+
```

One paragraph to tie it together: each TCP connection is handled by a task on one of 8 dedicated ingestion runtimes. Inside that task, events are decoded, rate-limited, enriched with correlation context (core, services, wp_hash), counted into in-memory aggregation state, and pre-serialized into JSON. The enriched event is then fanned out over two non-blocking channels: one to the `BatchWriter` for short-lived raw persistence, and one to the `EventBroadcaster` for real-time WebSocket delivery. The in-memory counters and trackers are drained to TimescaleDB by a periodic flush task every 5 seconds — that aggregated data, not the raw events, is what dashboards query. Every stage of the pipeline can be individually disabled with a `--disable-*` CLI flag (see `src/feature_flags.rs`) for memory/performance bisection.

## Meet the Characters

| Component | Role | Source |
|-----------|------|--------|
| `TelemetryServer` | Owns the TCP listener(s), spawns per-connection tasks, holds shared state | `src/server.rs` |
| `ConnectionContext` | Per-connection bundle of cloned `Arc` handles to shared components | `src/server.rs` |
| `NodeEventEnricher` | Per-node stateful enricher -- correlates submission IDs, cores, service IDs across events | `src/enricher.rs` |
| `EventCounter` | `DashMap<CountKey, i64>` -- counts every event into (30s bucket, node, type, dims) cells; the source of all long-term stats | `src/event_counter.rs` |
| `SlotTracker` | `DashMap<u32, SlotState>` -- tracks block propagation convergence per slot | `src/slot_tracker.rs` |
| `WpTracker` | `DashMap<[u8;32], WpState>` -- tracks work package pipeline stages by hash | `src/wp_tracker.rs` |
| Convergence trackers | Guarantee/assurance propagation timing per slot/anchor, plus a `header_hash_lookup` map | `src/convergence_tracker.rs` |
| `DaTracker` / DA latency tracker | Per-node DA stats and shard/bundle/segment/preimage latency histograms | `src/da_tracker.rs`, `src/da_latency_tracker.rs` |
| `BatchWriter` | Channels events to 8 writer workers for batched COPY writes to the 1h raw store + aux tables | `src/batch_writer.rs` |
| `EventBroadcaster` | Funnels events through an aggregator task to `broadcast` channels and a ring buffer | `src/event_broadcaster.rs` |
| `MetricsTracker` | Single-writer task computing block propagation and WP pipeline snapshots in memory | `src/metrics_tracker.rs` |
| `LiveCounters` | Lock-free atomic ring buffer of 60 per-second buckets for real-time counters | `src/live_counters.rs` |
| `RateLimiter` | Lock-free per-node token bucket using packed `AtomicU64` CAS | `src/rate_limiter.rs` |
| `FeatureFlags` | `--disable-*` switches gating each subsystem above | `src/feature_flags.rs` |

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
[Source: `src/server.rs`, `spawn_ingestion_runtimes`]

When a connection arrives, `handle_connection_optimized` spawns as a tokio task **on the same single-thread runtime**. It reads the first message (always `NodeInformation` -- the node's identity), then enters an event loop:

1. **Read** TCP bytes into a `BytesMut` buffer (8KB initial, 1MB max)
2. **Decode** all complete frames from the buffer (length-prefix framing: 4-byte LE size + payload)
3. **Rate-limit** via `RateLimiter::allow_event()` -- a lock-free CAS on a packed `AtomicU64` (count + window start in one 8-byte word)
4. **Serialize** the event to JSON once (`serde_json::to_vec`), wrap in `Arc<[u8]>` for zero-copy sharing
5. **Enrich** via the per-node `NodeEventEnricher` (more on this below)
6. **Update trackers** -- `SlotTracker`, `WpTracker`, guarantee/assurance convergence trackers, `DaTracker` and the DA latency tracker are all `DashMap`s written inline
7. **Count** -- increment the event's `(30s bucket, node, type, dims)` cell in the `EventCounter` DashMap (all 115 event types)
8. **Build WS envelope** -- pre-serializes the full WebSocket JSON using `serde_json::RawValue` to avoid double-serialization
9. **Batch-send** -- accumulated events from a single TCP read are sent as one `Vec` to both channels

Here is the tricky part: steps 4-8 happen **inside the ingestion thread**, parallelized across 8 runtimes. This moves the expensive serialization and aggregation work off the main runtime. The channels receive pre-built payloads. Each of steps 5-8 is individually gated by a feature flag (`--disable-enricher`, `--disable-slot-tracker`, `--disable-event-counter`, `--disable-ws-broadcast`, ...).

```
  TCP read wakeup (N events coalesced in one segment)
       |
       v
  +--- inner decode loop (no I/O, no await) ---+
  | for each frame:                             |
  |   decode -> rate-limit -> serialize ->      |
  |   enrich -> update trackers -> count ->     |
  |   build WS                                  |
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

Each map is capped at 50,000 entries (`MAX_MAP_ENTRIES`) and stale entries are evicted every 1,000 calls. The enricher itself is stored in an `EnricherMap` (`DashMap<NodeId, NodeEventEnricher>`) keyed by node ID. Stale enrichers (no activity for 60s) are swept every ~2.5 minutes from a periodic task in `main.rs`.

The enriched fields (`slot`, `core`, `submission_id`, `service_ids`, `wp_hash`) are written to the DB as hot columns and count-table dimensions -- they are **not** added to the WebSocket broadcast payload.

[Source: `src/enricher.rs`]

### Act 3: Raw Persistence -- The BatchWriter

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
  TimescaleDB (ingested_raw_events, event_services, node_stats)
```

Each worker blocks on `rx.recv()` for the first event, then drains aggressively for up to 100ms or 16,000 events (whichever comes first). While one worker is blocked on a DB COPY, the other 7 pick up work. This is the "work-stealing" part -- no explicit scheduling, just mutex contention that naturally load-balances.

**All event types are written raw -- but raw is not the store of record.** `ingested_raw_events` has a **1-hour retention policy** (migration 020) and exists purely for browsing and drilldown (`/api/grafana/events`, per-WP event views). Hot columns (`slot`, `core`, `submission_id`, `wp_hash`) are populated at write time so drilldowns never parse JSONB. Long-term data lives exclusively in the count tables (Act 4).

Node stats (extracted `Status` event fields) follow a different path: each worker accumulates local counts in a `HashMap`, then merges them into a shared `HashMap` every 5 seconds. A separate dedicated task flushes that aggregated map to the database, preventing deadlocks from concurrent UPDATE statements.

[Source: `src/batch_writer.rs`]

### Act 4: Count Tables -- The Long-Term Store

This is the storage heart of the system, and the part that replaced the original "query raw events" design (which grew 8TB in 48 hours -- see `docs/optimizing-db-plan.00.md`).

Step 7 of the ingestion loop increments a cell in the `EventCounter`:

```
CountKey { bucket,      // event timestamp aligned to 30s
           node_id,
           event_type,
           slot / core / reason / kind / from_proxy / epoch / service_id }  // per-group dims
      |
      v
DashMap<CountKey, i64>   +1 per event, lock-free, no I/O
```

Every 5 seconds the periodic flush task drains the map, partitions the cells by event-type range, and COPY-BINARYs them into **14 per-protocol-group count tables**: `status_counts`, `connection_counts`, `block_counts`, `block_distribution_counts`, `ticket_low_counts`, `ticket_counts`, `wp_pipeline_counts`, `guarantee_sending_counts`, `guarantee_receiving_counts`, `shard_counts`, `assurance_counts`, `bundle_counts`, `segment_counts`, `preimage_counts`. Rows are append-only `(bucket, node_id, event_type, event_count, ...dims)` -- queries always `SUM(event_count) GROUP BY`.

Each count table is a TimescaleDB hypertable (3-day retention, compressed) with `_1m` (30-day) and `_1h` (365-day) continuous-aggregate rollups. UNION views (`all_event_stats_30s/1m/1h`, `all_core_stats_1m`) present the whole family as one logical table, and the Grafana endpoints (`src/grafana_store.rs`) auto-select the tier from the requested interval and time-range age.

The same "aggregate at ingestion" pattern covers things counting can't express:

- **Lifecycle trackers**: `SlotTracker` (block propagation percentiles per slot), `WpTracker` (WP pipeline funnel/stage timestamps), guarantee/assurance convergence trackers.
- **Latency histograms**: the DA latency tracker matches request/response event pairs (shard 120→125, bundles, segments, preimages) and accumulates **additive log-scale histogram buckets** -- these merge correctly across nodes and any time window, unlike pre-computed percentiles.

[Source: `src/event_counter.rs`, `src/convergence_tracker.rs`, `src/da_latency_tracker.rs`; migrations 016 + 020]

### Act 5: WebSocket Output -- The EventBroadcaster

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

[Source: `src/event_broadcaster.rs`]

### Act 6: In-Memory Analytics -- MetricsTracker and LiveCounters

The `MetricsTracker` receives every event through a filtered `mpsc` channel (50K capacity) from the aggregator. It runs as a **single tokio task** that owns all mutable state -- no locks for writes. It computes:

- **Block propagation**: measures wall-clock delay between `BlockAnnounced` and `BlockTransferred` for the same slot across different nodes
- **WP pipeline timing**: tracks `WorkPackageReceived -> Authorized -> Refined -> WorkReportBuilt -> GuaranteeBuilt -> GuaranteesDistributed` per submission ID
- **Core status**: active/idle/stale classification per core

Snapshots are rebuilt every 2 seconds and published via `RwLock<Arc<Value>>` -- API handlers read a cloned `Arc` (cheap) while the tracker task writes behind the lock.

`LiveCounters` takes a different approach: it is a ring buffer of 60 `SecondBucket` structs, each containing `AtomicU64` fields for events, blocks, finalized blocks, announcements, and tickets. The MetricsTracker task calls `record()` on every event (one atomic increment), and API handlers can `sum_last_n_seconds()` at any time without locking. This replaced two SQL queries that took 2.7s and 3.2s.

### Act 7: The Periodic Flush Task

All in-memory aggregation state is drained to TimescaleDB by a **single periodic task** on the main runtime (`main.rs`), ticking every 5 seconds:

```
every tick (5s):    flush SlotTracker, WpTracker,
                    guarantee + assurance convergence trackers,
                    EventCounter -> 14 count tables (COPY BINARY)
every 2 ticks:      flush DaTracker + DA latency histograms (10s)
every 6 ticks:      evict header_hash_lookup entries (30s, cap 50K)
every 30 ticks:     sweep stale enrichers (~2.5 min, 60s idle TTL)
every 60 ticks:     retention cleanup (5 min): DELETE convergence rows
                    older than 7 days, drop_chunks on DA hypertables
```

Each flusher follows a three-phase pattern to avoid holding DashMap guards across `.await` points: (1) iterate and snapshot dirty entries, (2) write to DB without any guards, (3) clear dirty flags and evict stale entries. Per-flusher timings are logged each cycle (`periodic_flush cycle=...`).

Raw-event retention (`ingested_raw_events`, 1 hour) and count-table retention/compression are handled by TimescaleDB policies (migration 020), not by this task.

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
| Periodic flush | tokio task | 1 | Main runtime | 5s tick; drains all tracker DashMaps + EventCounter to DB (Act 7) |
| On-chain ingestion | tokio tasks | 1 per RPC URL | Main runtime | Subscribes to JAM node RPC `statistics()`, writes `onchain_*` tables |
| Cache warming | tokio task | 1 orchestrator + N spawned | Main runtime | Periodically refreshes hot query results into TtlCache (gated by `--disable-cache-warmer`) |
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

The ingestion side uses `try_send()` on both the BatchWriter and Broadcaster channels. If either is full, events are dropped with metrics counters incremented -- the system prioritizes liveness over completeness under extreme backpressure. (The counter/tracker path has no channel at all: it writes DashMaps inline, so it cannot drop.)

### Drop Strategy (Why No Backpressure)

Telemetry sources (JAM nodes) produce events at whatever rate the network dictates — they won't slow down because our pipeline is full. Backpressure would just move the drop point into the node (TCP window fills → node's send blocks → node's internal telemetry buffer fills → node drops), introducing latency and memory pressure inside the node itself. The correct design is: accept everything, drop at the earliest internal point where we can't keep up, and count what was dropped.

The system has three drop points, ordered from earliest to latest:

1. **Rate limiter** (1000 events/s + 200 burst per node): drops events, increments `telemetry_events_rate_limited` counter
2. **BatchWriter channel full** (5M capacity): `try_send` returns error, connection handler counts dropped events, logs every 500
3. **Broadcaster channel full** (500K capacity): `try_send` returns false, event lost from WS stream

The large channel capacities (5M, 500K) aren't wasteful — they absorb transient DB stalls (slow COPY, connection pool exhaustion, autovacuum) without dropping events during temporary hiccups.

In `--no-database` mode, the BatchWriter still drains its channel (to prevent OOM) but discards events instead of writing to PostgreSQL.

### Feature Flags

Every subsystem can be disabled independently for memory/performance bisection (`src/feature_flags.rs`): `--disable-enricher`, `--disable-slot-tracker`, `--disable-wp-tracker`, `--disable-convergence`, `--disable-da-tracker`, `--disable-event-counter`, `--disable-ws-broadcast`, `--disable-db-writes`, `--disable-metrics-tracker`, `--disable-onchain`, `--disable-cache-warmer`. All default to enabled; disabled subsystems are logged at startup.

### Why `Arc<str>` for Node IDs

Node IDs appear in every event record, broadcast record, and DB write. Using `String` would mean a 64-byte heap allocation per clone. `Arc<str>` clone is a single atomic increment -- at hundreds of thousands of events per second across 1024 nodes, this saves millions of allocations per second.

## Quick Reference

| Term | Meaning |
|------|---------|
| Ingestion runtime | Dedicated OS thread + single-thread tokio runtime, one per `SO_REUSEPORT` listener |
| EnricherMap | `DashMap<NodeId, NodeEventEnricher>` -- per-node cross-event correlation state |
| EventCounter | `DashMap<CountKey, i64>` -- 30s-bucketed event counts, flushed every 5s to 14 count tables |
| Count tables | Per-protocol-group hypertables; the long-term store (raw events keep only 1h) |
| SlotTracker | `DashMap<u32, SlotState>` -- convergence timing for block propagation per slot |
| WpTracker | `DashMap<[u8;32], WpState>` -- work package pipeline stage tracking by hash |
| Writer worker | One of 8 tokio tasks sharing a single `mpsc::Receiver` for work-stealing DB writes |
| Aggregator task | Single tokio task that owns broadcaster node channels, ring buffer, and event routing |
| LiveCounters | 60-bucket atomic ring buffer for per-second event counting (lock-free reads) |
| JCE epoch | Timestamp epoch used in JAM telemetry: Unix micros offset by `JCE_EPOCH_UNIX_MICROS` |
| `try_send` | Non-blocking channel send; returns error if full (backpressure point) |
| `SO_REUSEPORT` | Linux socket option allowing multiple listeners on the same port; kernel distributes connections |

## Source Files

- `src/main.rs` - CLI parsing (incl. `--disable-*` flags), component wiring, the periodic flush task, HTTP server startup
- `src/server.rs` - `TelemetryServer`, `SO_REUSEPORT` listener creation, connection handling, the decode/enrich/count loop
- `src/feature_flags.rs` - per-subsystem disable switches
- `src/event_counter.rs` - `EventCounter` DashMap, event-type→count-table mapping, COPY BINARY flush
- `src/batch_writer.rs` - Work-stealing writer pool, COPY flush to `ingested_raw_events` + `event_services` + `node_stats`
- `src/event_broadcaster.rs` - Aggregator task, broadcast/per-node channels, ring buffer, WS envelope construction
- `src/enricher.rs` - `NodeEventEnricher` with submission/built/sending/request/reconstructing correlation maps
- `src/slot_tracker.rs` / `src/wp_tracker.rs` - block convergence and WP pipeline tracking + table flushing
- `src/convergence_tracker.rs` - guarantee/assurance convergence tracking, `header_hash_lookup`
- `src/da_tracker.rs` / `src/da_latency_tracker.rs` - DA node stats and latency histogram accumulation
- `src/histogram.rs` - additive log-scale histograms used by the latency trackers
- `src/metrics_tracker.rs` - In-memory block propagation and WP pipeline analytics, snapshot publishing
- `src/live_counters.rs` - Lock-free atomic per-second sliding window counters
- `src/rate_limiter.rs` - Lock-free per-node rate limiting with packed `AtomicU64` CAS
- `src/decoder.rs` - Binary protocol decoding (length-prefix framing, JAM event types)
- `src/grafana.rs` / `src/grafana_store.rs` - the active query layer over count tables and tracker tables
- `src/store.rs` / `src/api.rs` - legacy `EventStore` and REST routes (mostly superseded by `/api/grafana/*`)
