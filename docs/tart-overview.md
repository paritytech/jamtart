# TART — JAM Telemetry, in One Document

TART (Testing, Analytics and Research Telemetry) is a Rust backend that ingests
[JIP-3](https://github.com/polkadot-fellows/JIPs/blob/main/JIP-3.md) telemetry
from JAM blockchain nodes, stores it in TimescaleDB, and serves it to Grafana
dashboards and a REST/WebSocket API.  It is built to handle a full-scale JAM
network (*toaster*): **1,024 nodes, 341 cores, ~3M events/second** on a single
ingestion box.

```
JAM nodes (×1024) ──binary TCP──▶ TART ──▶ TimescaleDB ──▶ Grafana dashboards
                                    │                        (Infinity plugin →
                                    └──▶ WebSocket / REST         /api/grafana/*)
```

This document is onboarding material: what the nodes send, what you can
measure, and how events flow through the system. For endpoint-level detail see
the [Swagger UI](https://michalkucharczyk.github.io/files/30-jamtart/swagger.html) (served live at `/api/docs/openapi.json`); for
the ingestion internals see [pipeline-architecture.md](pipeline-architecture.md).

---

## 1. JIP-3 in five minutes

JIP-3 specifies how a JAM node reports its runtime activity. A node started
with `--telemetry HOST:PORT` opens **one plain TCP connection** to the
telemetry server and pushes a one-directional binary feed:

- **Framing:** every message is length-prefixed (little-endian `u32` byte
  length, then the payload) — same framing as JAMNP-S.
- **Encoding:** standard JAM serialization. No handshake; the first message on
  each connection is a *node information* message (protocol version, genesis
  hash, peer ID, implementation name/version, GP version), everything after it
  is an event.
- **Event envelope:** every event starts with a `u64` timestamp (µs since the
  JAM Common Era) and a single-byte **discriminator** identifying the type.

There are **~115 event types**, allocated in blocks by category:

| Discriminators | Category | ~Count | What it covers |
|---|---|---|---|
| 0 | Meta | 1 | `Dropped` — a run of events the node had to drop (backpressure signal) |
| 10–13 | Status | 4 | Periodic (~2s) node snapshot, best/finalized block changes, sync flag |
| 20–28 | Networking | 9 | JAMNP-S connection lifecycle, peer misbehavior |
| 40–47 | Blocks | 8 | Authoring and import pipelines, verification, execution |
| 60–68 | Block distribution | 9 | Announcements (UP 0) and block requests/transfers (CE 128) |
| 80–84 | Safrole tickets | 5 | Ticket generation and transfer (CE 131/132) |
| 90–113 | Guaranteeing | 24 | Work-package submission → refine → report → guarantee (CE 133–135, 146) |
| 120–131 | Availability | 12 | Erasure-coded shard distribution and assurances (CE 137, 141) |
| 140–153 | Bundle recovery | 14 | Recovering WP bundles for auditing (CE 138, 147) |
| 160–178 | Segment recovery | 19 | Recovering exported segments (CE 139/140/148) |
| 190–199 | Preimages | 10 | Preimage announce/request/transfer/discard (CE 142/143) |

Two properties of the spec shape everything downstream:

1. **Events reference each other by ID.** Each event on a connection gets an
   implicit sequential ID, and lifecycle events carry the ID of the event that
   started them. A work package is not one event but a *chain*:
   `WorkPackageReceived(94)` → `Authorized(95)` → `Refined(101)` →
   `WorkReportBuilt(102)` → `GuaranteeBuilt(105)` → distribution → on-chain.
   Only the first event in the chain carries the core index, WP hash and
   service IDs — everything else must be correlated by following IDs.
2. **Cost telemetry is first-class.** Refine, is-authorized and accumulate
   events carry gas *and* wall-clock-nanosecond breakdowns, so the system is
   expected to support performance profiling, not just liveness monitoring.

It is a firehose by design: every networking action, every pipeline stage, on
*both* sides of every exchange, from every node.

---

## 2. What you can measure — a tour

All panels live in Grafana (10 provisioned dashboards under
`grafana/provisioning/dashboards/`) and are backed by `GET /api/grafana/*`
endpoints, so everything below is also scriptable — try
`playground/tart-cli.py list`.

### 2.1 Network heartbeat

Active nodes, events/sec, blocks/sec, event volume broken down by type, group
or node, and failure rates — the "is the network alive and how loud is it"
view.

- Dashboard: **TART Global** · Endpoints: `/api/grafana/stats`, `/api/grafana/timeseries`

> 📷 **[screenshot placeholder — TART Global: top row (Active Nodes, Events/sec, Blocks/sec) + "Events by Type" timeseries]**

### 2.2 Block propagation convergence

For every slot: how long until 50% / 99% / 100% of the network saw the new
best block, imported it, and finalized it. The canonical "is the network in
agreement, and how fast" metric — per-slot percentile curves computed at
ingestion (`slot_convergence` table), not by scanning raw events.

- Dashboard: **TART Blocks** · Endpoint: `/api/grafana/blocks/convergence`

> 📷 **[screenshot placeholder — TART Blocks: "Best Block Propagation" p50/p99/p100 curves]**

### 2.3 Work-package pipeline funnel & bottlenecks

How many work packages reached each stage (received → authorized → refined →
report built → guarantee built → distributed), and P50/P95 timings per stage —
this is where you see *which stage* of guaranteeing stalls, globally or per
core.

- Dashboard: **TART Cores** · Endpoints: `/api/grafana/wp-funnel`, `/api/grafana/bottlenecks`, `/api/grafana/wp/{wp_hash}` for single-WP drilldown

> 📷 **[screenshot placeholder — TART Cores: "WP Pipeline Funnel" bar gauge + per-stage timing panels]**

### 2.4 DA convergence

The availability layer's equivalent of block convergence: how quickly
guarantees propagate to the network, and how quickly assurances flow back for
each anchor. Proof that the data-availability guarantee is actually being met.

- Dashboard: **TART Data Availability** · Endpoints: `/api/grafana/guarantee-convergence`, `/api/grafana/assurance-convergence`

> 📷 **[screenshot placeholder — TART DA: "Guarantee Convergence" + "Assurance Convergence" panels]**

### 2.5 DA transfer & recovery latency

Percentile-over-time latency for shard round-trips (each assurer sends ~341
requests per slot, one per core), bundle reconstruction, segment fetching and
preimage transfer. Percentiles (p50/p95/p99/p100) are approximate —
interpolated from additive log-scale histograms that merge correctly across
nodes and any time window.

- Dashboard: **TART Data Availability** · Endpoints: `/api/grafana/shard-latency`, `/api/grafana/bundle-latency`, `/api/grafana/segment-latency`, `/api/grafana/preimage-latency`

> 📷 **[screenshot placeholder — TART DA: "Assurer Round-Trip" + "Bundle E2E Recovery" latency fans]**

### 2.6 Guarantor profiling

A per-validator performance leaderboard: top-10 slowest/fastest guarantors by
average pipeline time, network-wide P50/P95 guarantor pipeline latency, and
per-guarantor failure counts. Answers "*who* is slow", not just "something is
slow".

- Dashboards: **TART Node**, **TART Cores** · Endpoints: `/api/grafana/validator-profiling`, `/api/grafana/guarantees/by-guarantor`

> 📷 **[screenshot placeholder — TART Node: "Top 10 Slowest Guarantors" + network P50/P95 timeseries]**

### 2.7 Per-service accounting

Work packages, gas consumption (refine + accumulate) and failure reasons per
service, over time — fed by the `event_services` junction table populated at
ingestion.

- Dashboard: **TART Services** · Endpoints: `/api/grafana/services`, `/api/grafana/services/timeseries`

> 📷 **[screenshot placeholder — TART Services: "Gas Usage by Service" + failure-reason distribution]**

### 2.8 On-chain cross-check

Independently of telemetry, TART subscribes to a JAM node's RPC `statistics()`
and stores per-core, per-service and per-validator *on-chain* activity — all
341 cores and 1,024 validators as activity grids. Useful to reconcile what
nodes *report* against what actually landed on chain.

- Dashboards: **TART On-Chain Cores / Services / Validators** · Endpoints: `/api/grafana/onchain/*`

> 📷 **[screenshot placeholder — TART On-Chain Validators: 1024-validator activity grid]**

---

## 3. How events are processed

### The one design rule

> **Do the work at ingestion time; never compute analytics from raw events at
> read time.**

Raw-event queries do not survive contact with 3M events/s (see §4). So every
metric above is maintained *incrementally as events arrive* — counters,
trackers and histograms in memory, flushed to purpose-built tables on a 5s
cadence — and read queries only ever touch small pre-aggregated tables.

### The pipeline

```
TCP :9000 (×8 SO_REUSEPORT ingestion runtimes, one OS thread each)
  │  decode frame → rate-limit (1000/s + burst per node) → enrich
  ▼
  ├─▶ BatchWriter (5M chan, 8 workers, COPY)──▶ ingested_raw_events + aux tables
  ├─▶ DashMap counters ── flush every 5s ────▶ 14 count tables
  ├─▶ trackers (WP / slot / convergence / DA) ▶ wp_tracking, slot_convergence, histograms
  └─▶ EventBroadcaster ──────────────────────▶ WebSocket clients (/api/ws)
```

1. **Ingestion** — 8 dedicated single-thread Tokio runtimes share port 9000
   via `SO_REUSEPORT`; each connection is decoded, rate-limited and serialized
   once, off the main runtime (`src/server.rs`).
2. **Enrichment** — a per-node stateful `NodeEventEnricher`
   (`src/enricher.rs`) follows the JIP-3 event-ID chains: `WorkPackageReceived`
   seeds `{core, wp_hash, service_ids, submission_id}`, and that context is
   propagated onto ~30 downstream event types as extra DB columns. Without
   this, a `Refined` event doesn't know which core or service it belongs to.
3. **Dual-write** — every event type goes to *both* stores:
   - `ingested_raw_events` — the browsing store. **1h retention**, hot columns
     (`slot`, `core`, `submission_id`, `wp_hash`) populated at ingestion so
     drilldowns never parse JSONB. This is for "show me the raw events of this
     WP", not for analytics.
   - **14 per-group count tables** (`status_counts`, `connection_counts`,
     `block_counts`, `block_distribution_counts`, `ticket_counts`,
     `ticket_low_counts`, `wp_pipeline_counts`, `guarantee_sending_counts`,
     `guarantee_receiving_counts`, `shard_counts`, `assurance_counts`,
     `bundle_counts`, `segment_counts`, `preimage_counts`) — in-memory DashMap
     counters flushed every 5s via COPY. Append-only `(bucket, node_id,
     event_type, count, …dims)` rows.
4. **Aggregate tiers** — each count table has `_1m` and `_1h` continuous
   aggregates; UNION views (`all_event_stats_30s/1m/1h`, `all_core_stats_1m`)
   present them as one logical table, and the Grafana endpoints
   (`src/grafana_store.rs`) auto-select the tier from the requested interval
   and time-range age.
5. **Serving** — Axum on :8080. `/api/grafana/*` (OpenAPI-documented, the
   active surface, `src/grafana.rs`), a small set of legacy REST routes, and
   the WebSocket firehose at `/api/ws`.

### What's in the database

| Store | Retention | Purpose |
|---|---|---|
| `ingested_raw_events` | 1 h | Raw-event browsing / drilldown only |
| 14 count tables (30s buckets) | 3 d | Fine-grained recent rates |
| `_1m` continuous aggregates | 30 d | Standard dashboard range |
| `_1h` continuous aggregates | 365 d | Long-term trends |
| `event_services`, `node_stats` | 7 d | Per-service gas; extracted Status fields |
| `wp_tracking`, `slot_convergence` | — | WP funnel state; per-slot propagation percentiles |
| latency histogram tables | — | Additive 14/23-bucket log-scale histograms (DA, convergence) |
| `onchain_*` tables | — | Per-core/service/validator stats from JAM RPC |

Two non-obvious choices worth internalizing:

- **Percentiles are never stored — histograms are.** Pre-computed percentiles
  can't be merged across nodes or time windows (MAX/AVG of p95s is wrong), so
  TART stores additive log-scale bucket counts that SUM correctly over any
  window, and computes percentiles at query time.
- **Aggregate by default, drill down explicitly.** With 1,024 nodes and 341
  cores, any endpoint that returns one series per entity is unusable (and one
  early version returned 45 MB per panel refresh). Endpoints return
  network-wide aggregates unless you pass `?node=`/`?core=`/`?service=`.

---

## 4. Why it looks this way

The first implementation did the obvious thing — store every event as a JSONB
row and answer every question with SQL over raw events. At network scale that
failed in every dimension at once: the events table grew to **8 TB in ~48
hours** (two assurance event types alone were ~2.5B rows, 79% of storage), one
DA-statistics endpoint with an O(n²) self-join burned **~6.7 CPU-hours over
two days** while being re-fired every 2s, and per-entity panels returned 1,024
series nobody could read. The count-table architecture (migrations 015–020)
inverted the model to "aggregate at ingestion", cut storage by ~89%, and let
~6,700 lines of legacy raw-query endpoints be deleted. The full forensics live
in `docs/optimizing-db-plan.*`, `docs/issue-00--get_da_stats_enhanced.txt` and
`docs/issue-01--too-many-series.txt`.

---

## 5. Digging deeper

| Resource | What it gives you |
|---|---|
| [Swagger UI](https://michalkucharczyk.github.io/files/30-jamtart/swagger.html) / `GET /api/docs/openapi.json` | Every `/api/grafana/*` endpoint, parameters and response schemas |
| `playground/tart-cli.py` | Explore the API from the terminal: `list`, `events`, `scan`, `query <name> --since 1h` |
| [grafana-guide.md](grafana-guide.md) | Endpoint specs and dashboard recipes |
| [pipeline-architecture.md](pipeline-architecture.md) | "Life of an event" — the ingestion internals in detail |
| `migrations/` | The schema's full evolution (015/016 count tables, 020 unified architecture) |
| [JIP-3](../../JIPs/JIP-3.md) | The wire protocol and every event type's fields |
| `src/events.rs` | The Rust source of truth for event definitions |
