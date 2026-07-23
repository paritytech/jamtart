# API Migration Plan: Legacy → Grafana Endpoints

**Date:** 2025-03-25
**Status:** Planning input document
**Scope:** Rewire jamtart-ui from legacy `/api/*` endpoints to efficient `/api/grafana/*` endpoints, expand count tables to all event types, reduce `ingested_raw_events` to short retention window.

> **See also:** [`api-migration-deep-dive.md`](api-migration-deep-dive.md) — detailed analysis of 7 underdefined areas in this plan, with codebase findings and revised recommendations.

---

## 1. Motivation

Legacy endpoints in `src/store.rs` query `ingested_raw_events` directly with expensive patterns:
- `COUNT(*)` on millions of raw rows
- `jsonb_build_object()` / `jsonb_array_elements()` per row
- Self-JOINs on JSONB string fields (e.g. DA latency correlation)
- Complex CTEs with FULL OUTER JOINs
- No use of aggregate tables or count tables

Grafana endpoints in `src/grafana_store.rs` are designed for efficiency:
- Auto-select aggregate tier (30s/1m/1h) based on query range
- Query `all_event_stats_*` UNION views (transparent count table routing)
- Use dedicated tables (`wp_tracking`, `node_stats`, `da_node_stats`, `slot_convergence`, `shard_latency_hist`)
- No JSONB extraction at query time

**Future direction:** All ~115 event types will be pre-aggregated via count tables. `ingested_raw_events` will shrink to ~1h retention (for drill-down debugging only). Continuous aggregates on raw events will eventually be replaced entirely by count-table aggregates.

---

## 2. Current Architecture

### 2.1 Data flow

```
TCP ingestion (src/server.rs)
    │
    ├─► is_pre_aggregated(type)?
    │     YES → DashMap counter → flush every 5s → 9 count tables (COPY BINARY)
    │     NO  → batch_writer → ingested_raw_events (raw hypertable)
    │
    ├─► enricher (src/enricher.rs) → wp_tracking, slot_convergence
    ├─► node_stats (extracted Status fields)
    ├─► event_services (service × event junction with gas)
    ├─► da_node_stats (DA operational counters)
    └─► shard_latency_hist (latency distribution buckets)
```

### 2.2 Aggregate hierarchy

```
ingested_raw_events (1h retention, future)
    └─► event_stats_30s (continuous aggregate, 3d retention)
        └─► event_stats_1m (hierarchical, 30d retention)
            └─► event_stats_1h (hierarchical, 1y retention)

9 count tables (30s buckets, 3d retention)
    └─► *_1m (continuous aggregates, 30d retention)
        └─► *_1h (continuous aggregates, 1y retention)

UNION views (transparent query routing):
    all_event_stats_30s = event_stats_30s ∪ 9 count tables (raw)
    all_event_stats_1m  = event_stats_1m  ∪ 9 *_1m aggregates
    all_event_stats_1h  = event_stats_1h  ∪ 9 *_1h aggregates
    all_core_stats_1m   = core_stats_1m   ∪ guarantee_sending_counts_1m ∪ segment_counts_1m
```

### 2.3 Event types: 115 total

**41 types currently written to `ingested_raw_events`:**

| Group | IDs | Count |
|---|---|---|
| system | 0 | 1 |
| status | 10-13 | 4 |
| connections | 20-28 | 9 |
| blocks | 40-47 | 8 |
| tickets (low-vol) | 80-82 | 3 |
| wp_pipeline (low-vol) | 90-105 | 16 |

**74 types pre-aggregated via count tables (skip raw):**

| Group | IDs | Count table |
|---|---|---|
| block_distribution | 60-68 | `block_distribution_counts` |
| tickets (high-vol) | 83-84 | `ticket_counts` |
| wp_pipeline (high-vol) | 106-108 | `guarantee_sending_counts` |
| guarantee_receiving | 110-113 | `guarantee_receiving_counts` |
| shards | 120-125 | `shard_counts` |
| assurances | 126-131 | `assurance_counts` |
| bundles | 140-153 | `bundle_counts` |
| segments | 160-178 | `segment_counts` |
| preimages | 190-199 | `preimage_counts` |

### 2.4 Dedicated tables (not affected by raw event removal)

| Table | Fed by | Contains |
|---|---|---|
| `wp_tracking` | Enricher | WP lifecycle stages + timing (received_at, authorized_at, refined_at, report_built_at, guarantee_built_at, distributed_at, failed_at), core, service_ids, submission_id |
| `slot_convergence` | Enricher | Per-slot propagation percentiles (p50/p75/p95/p99/p100) per event type |
| `node_stats` | Ingestion (Status events) | peer_count, da_shard_count, da_preimage_count, guarantees_by_core |
| `event_services` | Ingestion | service_id, event_type, gas per event |
| `da_node_stats` | In-memory counter flush | Per-node DA operation counts, latency stats |
| `shard_latency_hist` | In-memory histogram flush | 14-bucket latency histograms per node per time bucket |
| `nodes` | Connect/disconnect events | Node metadata, connection state, event counts |
| `onchain_core_stats` | JAM RPC poller | Per-core on-chain activity (gas, DA load, imports, exports) |
| `onchain_service_stats` | JAM RPC poller | Per-service on-chain activity |
| `onchain_validator_stats` | JAM RPC poller | Per-validator on-chain activity |

### 2.5 Count table schema

All 9 count tables share core columns:

| Column | Type | Nullable | All tables |
|---|---|---|---|
| `bucket` | TIMESTAMPTZ | NOT NULL | Yes |
| `node_id` | TEXT | NOT NULL | Yes |
| `event_type` | SMALLINT | NOT NULL | Yes |
| `event_count` | BIGINT | NOT NULL | Yes |

Extra dimension columns per table:

| Table | Extra columns |
|---|---|
| `block_distribution_counts` | `slot INT`, `reason TEXT` |
| `ticket_counts` | `reason TEXT`, `from_proxy BOOLEAN`, `epoch INT` |
| `guarantee_sending_counts` | `core SMALLINT`, `reason TEXT` |
| `guarantee_receiving_counts` | `slot INT`, `reason TEXT` |
| `shard_counts` | `reason TEXT` |
| `assurance_counts` | `reason TEXT` |
| `bundle_counts` | `reason TEXT`, `kind SMALLINT` |
| `segment_counts` | `core SMALLINT`, `reason TEXT`, `kind SMALLINT` |
| `preimage_counts` | `reason TEXT`, `service_id INT` |

Indexing: `(node_id, event_type, bucket DESC)` on all tables. Hypertable with 1-day chunks, compression segmented by `(node_id, event_type)`.

---

## 3. UI Endpoint Inventory

### 3.1 All 45 endpoints called by jamtart-ui

The UI lives at `/home/miszka/parity/30-jam/jamtart-ui/`. API client is in `lib/api.ts`. Types in `types/api-types.ts`. SWR hooks in `hooks/use-api.ts`. Context in `contexts/telemetry-context.tsx`.

#### Health / Real-time (no DB, no migration needed)

| # | Endpoint | UI usage | Polling | Data source |
|---|---|---|---|---|
| 1 | `GET /api/health` | Connection check | On demand | No DB |
| 29 | `GET /api/metrics/live` | Real-time rates | 1s | In-memory LiveCounters |
| 30 | `GET /api/metrics/realtime?seconds=N` | Per-second window | 1s | In-memory LiveCounters |
| 31 | `SSE /api/metrics/stream` | Real-time push | Continuous | In-memory |
| 45 | `WS /api/ws` | Event streaming | Continuous | In-memory broadcast |

#### JAM RPC proxy (no local DB)

| # | Endpoint | UI usage | Polling |
|---|---|---|---|
| 42 | `GET /api/jam/stats` | Dashboard | 12s |
| 43 | `GET /api/jam/services` | Service metadata | 12s |
| 44 | `GET /api/jam/cores` | Cores view | 12s |

#### Endpoints needing migration (grouped by migration strategy)

See Section 5 for full migration details per endpoint.

### 3.2 UI operational patterns

**Time range:** UI uses preset durations (`5m, 15m, 1h, 6h, 24h`) passed as `?duration=1h`. Grafana endpoints expect `?start=ISO&end=ISO`. The UI already computes start/end internally in `time-range-context.tsx` but sends the preset string. Fix: change `withDuration()` helper in `lib/api.ts` to send ISO timestamps.

**Pagination:** offset/limit pattern. Used by events search (ForensicsView), node events, core work-packages. Response envelope: `{events:[], pagination: {offset, limit, total, has_more}}`. Grafana endpoints currently return flat arrays with no pagination.

**Polling intervals (SWR):**

| Interval | Endpoints |
|---|---|
| 1s | metrics/live, metrics/realtime |
| 5s | stats (via context), workpackages/active |
| 10s | events/search, WP journey, WP audit-progress |
| 12s | blocks, cores/status, network-health, failure-rates, jam/*, timeseries |
| 15s | core detail (metrics/bottlenecks/validators/work-packages), DA enhanced, node detail, slots |
| 20s | execution metrics |
| 30s | validators, guarantees/by-guarantor, sync-timeline, connections-timeline |

**Duration selector component:** `components/shared/TimeRangeSelector.tsx` — buttons for LIVE | 5m | 15m | 1h | 6h | 24h | historical calendar.

---

## 4. Response Shape Comparison (Legacy vs Grafana)

### 4.1 `/api/stats` → `/grafana/stats`

**Legacy response (ApiStats):**
```typescript
{
  total_blocks_authored: number;  // UNUSED in UI
  best_block: number;             // Used as fallback behind WS/liveMetrics
  finalized_block: number;        // Used as fallback behind WS/liveMetrics
}
```

**Grafana response (StatsResponse):**
```typescript
{
  connected_nodes: i32,
  slot_events: i64,        // BlockAuthored count
  guarantees: i64,         // GuaranteeBuilt count
  failures: i64,           // WorkPackageFailed count
  wp_events: i64,          // WorkPackageReceived count
  events_per_sec_10s: f64 | null,
  blocks_per_sec_10s: f64 | null,
  best_slot: u32 | null,       // ← maps to best_block
  finalized_slot: u32 | null,  // ← maps to finalized_block
  active_nodes: usize | null,
}
```

**UI consumers:** `summary-view.tsx` (lines 52-53), `DashboardView.tsx` (line 629), `telemetry-context.tsx` (lines 226-234).

**Migration:** Rename `best_block` → `best_slot`, `finalized_block` → `finalized_slot` in 3 files. 15 min.

---

### 4.2 `/api/blocks` → needs new grafana endpoint

**Legacy response (ApiBlockStats):**
```typescript
{
  total_blocks: number;
  blocks_last_hour: number;
  blocks_last_day: number;
  average_block_time_ms: number | null;  // Used for block progress animation
  latest_slot: number | null;
  latest_hash: string | null;
  finalized_slot: number | null;
  finalized_hash: string | null;
  recent_authored: [{                     // From raw events type 42
    hash: string;
    slot: number;
    node_id: string;
    propagation_ms: number;
    timestamp: string;
  }];
  authoring_by_node: [{node_id, blocks_authored}];
}
```

**Existing grafana coverage:**
- `/grafana/timeseries?event_types=42` → block counts per bucket (covers total_blocks, blocks_last_hour)
- `/grafana/stats` → best_slot, finalized_slot
- `/grafana/blocks/contents` → per-block extrinsic breakdown (has slot, timestamp, node_id but NOT hash, propagation_ms)
- `/grafana/blocks/convergence` → per-slot propagation percentiles

**Gaps needing new endpoint or field additions:**
- `average_block_time_ms` — compute from consecutive BlockAuthored timestamps
- `latest_hash`, `finalized_hash` — from JSONB data column (requires raw events, OK with 1h retention)
- `recent_authored[].hash` — from JSONB, same
- `recent_authored[].propagation_ms` — from slot_convergence or JSONB

**UI consumers:** `BlocksView.tsx` (metric cards + recent blocks table), `DashboardView.tsx` (chain state hero), `summary-view.tsx` (block time progress bar).

---

### 4.3 `/api/cores/status` → extend `/grafana/cores`

**Legacy response (ApiCoresStatus):**
```typescript
{
  cores: [{
    core_index: number;
    active_work_packages: number;    // In-flight WP count
    work_packages_last_hour: number;
    guarantees_last_hour: number;
    last_activity: string | null;    // ISO timestamp
    status: 'active' | 'idle' | 'stale';
  }];
  summary: {
    active_cores: number;
    idle_cores: number;
    stale_cores: number;
  };
}
```

**Grafana response (Vec<CoreSummary>):**
```typescript
[{
  core: i16;
  work_packages: i64;   // ← maps to work_packages_last_hour
  guarantees: i64;       // ← maps to guarantees_last_hour
  failures: i64;         // NEW, not in legacy
}]
```

**Gaps:**
- `active_work_packages` — derive from `wp_tracking WHERE distributed_at IS NULL AND failed_at IS NULL`
- `status` — derive from counts (active if WPs or guarantees > 0, stale if last_activity > threshold)
- `last_activity` — `MAX(first_seen)` from `wp_tracking` per core
- `summary` — compute client-side from array

**UI consumers:** `CoresView.tsx` — grid view (CoreCellDetailed) + table view. Renders core_index, active_work_packages badge, guarantees badge, status dot color, last_activity for sorting.

---

### 4.4 `/api/cores/{id}/bottlenecks` — NOT replaceable by `/grafana/bottlenecks`

**Legacy response (ApiCoreBottlenecks):** Profiles slow validators.
```typescript
{
  core_index: number;
  has_bottlenecks: boolean;
  slow_validators: [{
    validator_index: number;
    node_id: string;
    slowdown_factor: number;
    average_response_time_ms: number;
    failure_rate_pct: number;
    affected_stage: string;
  }];
  bottleneck_messages: [{
    severity: string;
    message: string;
    affected_validator_id: string;
    affected_stage: string;
    timestamp: string;
  }];
  overall_health: 'healthy' | 'degraded' | 'unhealthy';
  failure_rate_pct: number;
}
```

**Grafana response (Vec<BottlenecksResponse>):** Profiles pipeline stages.
```typescript
[{
  core: i16;
  stage_timings: [{
    stage: string;  // authorize, refine, report, guarantee, distribute, pipeline_total
    percentiles: { p50_ms: i32, p95_ms: i32 }
  }];
  failure_rate: f64;
  total_wps: i64;
  failed_wps: i64;
  avg_pipeline_ms: f64 | null;
}]
```

**These are completely different analyses.** UI renders slow_validators list, bottleneck messages, health status — none of which exist in the grafana version. Need to either keep the legacy query or rewrite it to use `wp_tracking` (which has per-node timing data).

**UI consumers:** `CoreDetailView.tsx` overview tab — renders all fields heavily.

---

### 4.5 `/api/analytics/block-propagation` — DELETE

**Not rendered by any UI component.** Hook and normalizer exist but nothing uses them. Safe to remove.

---

### 4.6 `/api/metrics/timeseries` → `/grafana/timeseries`

**Legacy response:**
```typescript
{
  metric: string;
  interval_minutes: number;    // UNUSED by UI
  duration_hours: number;      // UNUSED by UI
  data: [{
    timestamp: string;
    total_events: number;      // UI reads as "value"
    active_nodes: number;      // UNUSED
    event_types: number;       // UNUSED
    events_per_second: number; // UNUSED
  }];
}
```

**Grafana response:**
```typescript
[{
  ts: string;         // ← maps to timestamp
  count: i64;         // ← maps to total_events / "value"
  event_type: i16 | null;
  event_type_name: string | null;
  core: i16 | null;
  node_id: string | null;
}]
```

**UI consumer:** `statistical-breakdown.tsx` — only reads `data[].value` from the envelope. Envelope metadata (`metric`, `interval_minutes`, `duration_hours`) is ignored.

**Migration:** Unwrap flat array, map `count` → `value`. 30 min.

Note: `/api/metrics/timeseries/grouped` is defined in API but **never called by UI**. Delete.

---

### 4.7 `/api/nodes` → `/grafana/nodes`

**Legacy response:** `{nodes: [ApiNode]}`
**Grafana response:** `[NodeRow]` (flat array)

**Field mapping:**
```
legacy event_count    → grafana total_event_count
legacy {nodes: [...]} → grafana [...] (unwrap)
```

All other fields identical: node_id, peer_id, implementation_name, implementation_version, node_info, connected_at, disconnected_at, last_seen_at, is_connected.

**UI consumers:** `ValidatorsView.tsx` — renders node_id, implementation_name, implementation_version, is_connected, event_count, last_seen_at.

**Migration:** Unwrap array, rename 1 field. 15 min.

---

### 4.8 `/api/events/search` → extend `/grafana/events`

**Legacy response:**
```typescript
{
  events: [{
    timestamp: string;
    node_id: string;
    event_type: number;
    created_at: string;    // Rendered in UI (not timestamp!)
    data: object;          // Full JSONB
  }];
  total: number | null;    // Displayed as "X results"
  limit: number;
  offset: number;
  has_more: boolean;       // Controls "Load more" button
}
```

**Grafana response:**
```typescript
[{
  ts: string;           // ← maps to timestamp
  node_id: string;
  event_type: i16;
  data: object;
}]
```

**Gaps:**
- No `created_at` field
- No pagination (`total`, `has_more`, `offset`, `limit`)
- No `node` or `core` filter params (grafana endpoint ignores them)
- Returns 400 for pre-aggregated event types

**UI consumer:** `ForensicsView.tsx` — uses pagination actively (`total` for count display, `has_more` for load-more button, `offset` for page math). Renders `created_at` as the displayed timestamp, `event_type`, `node_id`.

---

### 4.9 DA endpoints

**Legacy `/api/da/stats` response (ApiDAStats):**
```typescript
{
  aggregate: {
    total_shards: number;
    total_shard_size_bytes: number;
    total_preimages: number;
    total_preimage_size_bytes: number;
    average_shards_per_node: number;
    nodes_reporting: number;
  };
  by_node: [{
    node_id: string;
    num_shards: number;
    shard_size_bytes: number;
    preimages_announced: number;
    preimages_in_pool: number;
    last_updated: string;
  }];
  preimage_activity: {
    announced_last_hour: number;
    requested_last_hour: number;
    received_last_hour: number;
  };
}
```

**Legacy `/api/da/stats/enhanced` extends with:**
```typescript
{
  shard_distribution: [{shard_range, node_count, total_size_bytes, replication_factor}];
  availability_rate: number;
  recent_operations: [{operation_type, node_id, timestamp, hash, size_bytes, success}];
  node_health: [{node_id, status, shards_stored, storage_used_pct, last_activity, issues[]}];
}
```

**Grafana `/grafana/da-stats` response (DaStatsRow[]):**
```typescript
[{
  node_id: string;
  shard_requests_sent: i64;
  shard_requests_received: i64;
  shard_sent_confirmed: i64;
  shard_received_confirmed: i64;
  shards_transferred: i64;
  shard_failures: i64;
  preimage_ann_failures: i64;
  preimages_announced: i64;
  preimages_forgotten: i64;
  assurer_avg_latency_ms: f32 | null;
  assurer_latency_samples: i64;
  guarantor_avg_latency_ms: f32 | null;
  guarantor_latency_samples: i64;
  active_shards: i32;
}]
```

**Grafana `/grafana/shard-latency` response (ShardLatencyRow[]):**
```typescript
[{
  ts: string;
  assurer_p50/p75/p95/p99/p100: i32 | null;
  assurer_samples: i32;
  guarantor_p50/p75/p95/p99/p100: i32 | null;
  guarantor_samples: i32;
  failed_count: i32;
}]
```

**Alignment:**
- `active_shards` ≈ `num_shards` (close but not identical)
- `preimages_announced` matches
- Grafana covers operational metrics (request/transfer counts, latency) — legacy covers inventory state (shard sizes, preimages_in_pool)
- Inventory data (`num_shards`, `shard_size_bytes`, `preimages_in_pool`) comes from Status events (type 10) → already extracted to `node_stats` table
- `node_health`, `shard_distribution`, `availability_rate`, `recent_operations` have no grafana equivalent

**Source for missing data:** `node_stats` table has `da_shard_count`, `da_preimage_count` per node per timestamp. `/grafana/node-stats` already queries this table.

---

### 4.10 Core detail sub-endpoints (no grafana equivalent)

**`/api/cores/{id}/metrics` (ApiCoreMetrics):**
```typescript
{
  core_index: number;
  processing_efficiency_pct: number;
  accumulate_efficiency_pct: number;
  network_latency_ms: number;
  p95_latency_ms: number;
  throughput_per_second: number;
  average_completion_time_ms: number | null;
  gas_utilization_pct: number;
  work_packages_processed_24h: number;
}
```
Source: Raw events + JSONB extraction. **Rewrite using:** `core_stats_1m` + `wp_tracking` + LiveCounters.

**`/api/cores/{id}/validators` (ApiCoreValidators):**
```typescript
{
  core_index: number;
  validators: [{
    validator_index: number;
    node_id: string;
    implementation_name: string;
    implementation_version: string;
    guarantee_count: number;
    is_active: boolean;
    last_guarantee_at: string;
  }];
  total_assigned: number;
  active_count: number;
}
```
Source: Raw events for guarantee activity + nodes table for implementation details. **Rewrite using:** `wp_tracking` (guarantor data) + `nodes` table join.

**`/api/cores/{id}/work-packages` (ApiCoreWorkPackages):**
```typescript
{
  core_index: number;
  work_packages: [{
    hash: string;
    status: string;
    submitted_at: string;
    completed_at: string;
    node_id: string;
    extrinsic_count: number;
    extrinsic_size: number;
    service_id: number;
    gas_used: number;
    elapsed_ms: number;
  }];
  pagination: {offset, limit, total, has_more};
  summary: {total_work_packages, completed, in_progress, failed, average_completion_time_ms};
}
```
Source: Raw events + JSONB. **Rewrite using:** `wp_tracking` (has all stages + core) + `event_services` (for gas).

**UI consumer for all three:** `CoreDetailView.tsx` — separate tabs for overview (metrics + bottlenecks), work-packages, validators.

---

### 4.11 WP endpoints

**`/api/workpackages/{hash}/journey/enhanced` (ApiWorkPackageJourneyEnhanced):**
Used in `WorkPackageDetailView.tsx`. Shows full lifecycle timeline per WP.
Source: Raw events with hex-to-JSONB matching. **Rewrite using:** `wp_tracking` — has all stage timestamps (received_at, authorized_at, refined_at, report_built_at, guarantee_built_at, distributed_at, failed_at) + core + service_ids + submission_id.

**`/api/workpackages/{hash}/audit-progress` (ApiWorkPackageAuditProgress):**
Used in `WorkPackageDetailView.tsx` (`AuditProgressPanel`, lines 821-957). Shows status badge, progress bar, judgment counts, auditor metrics, tranche visualization.

> **CORRECTION:** This endpoint is fundamentally wrong. It queries **guarantee distribution events (105-113)** and mislabels them as "audit." In JAM, audit is a separate pipeline (VRF self-selection → shard recovery → re-execution → judgment) using events 140-153. The backend returns fake data: event names are wrong (106="GuaranteeSigned" is actually SendingGuarantee), `tranche` is always 0 (field doesn't exist on GuaranteeSummary), "panic_mode" is `failed && events.len() > 5`. The UI renders status/tranche/panic but the data doesn't support it.
>
> **Migration:** Do NOT migrate this endpoint. Replace the UI's `AuditProgressPanel` with a **WP Pipeline Status panel** that queries `wp_tracking` by hash for real stage timestamps (received_at → authorized_at → refined_at → report_built_at → guarantee_built_at → distributed_at) + `guaranteed_by` count. Use new `GET /api/grafana/wp/{hash}` endpoint (2h backend). UI rework: replace fake audit panel with real pipeline timeline (3-4h).
>
> **Proper audit tracking** (tranches, re-execution, judgments via events 140-153) is a separate future initiative — requires upstream polkajam telemetry additions (`// TODO @dave` in controller.rs). See `api-migration-deep-dive.md` Section 2.

**`/api/workpackages/active` (ApiActiveWorkPackages):**
Used in `DashboardView.tsx`. Shows in-flight WP count + stage breakdown.
```typescript
{
  work_packages: [{hash, core_index, stage, service_ids, received_at, ...stage timestamps}];
  summary: {total, by_stage_counts};
  reached: {received, authorized, refined, report_built, guarantee_built, distributed, included, available};
  failure_breakdown: Record<string, number>;
  stage_duration_percentiles: Record<string, {p50_ms, p95_ms, sample_count}>;
}
```
Source: Complex CTEs on raw events. **Rewrite using:** `wp_tracking WHERE distributed_at IS NULL AND failed_at IS NULL AND first_seen > NOW() - interval`.

**`/api/workpackages` (ApiWorkPackageStats):**
Summary stats. Source: COUNT(*) on raw. **Rewrite using:** count tables + `wp_tracking` aggregates.

**`POST /api/workpackages/batch/journey`:**
Batch lookup for multiple WP hashes. **Rewrite using:** batch `wp_tracking` SELECT.

---

### 4.12 Other legacy endpoints needing migration

**`/api/guarantees` + `/api/guarantees/by-guarantor`:**
Guarantee counts (types 105-113) + per-guarantor breakdown.
Source: Raw events. Types 106-113 are pre-aggregated — legacy query returns zero for them.
**Rewrite using:** count tables (`guarantee_sending_counts`, `guarantee_receiving_counts`) + `wp_tracking` for guarantor node mapping.

**`/api/analytics/failure-rates` (ApiFailureRates):**
```typescript
{
  overall: {total_events, failed_events, failure_rate};
  by_category: [{category, attempts, failures, rate}];
  by_node: [{node_id, total_events, failures, failure_rate, top_failure_type}];
  recent_failures: [{event_type, node_id, timestamp, data}];
}
```
Source: Mixed `event_stats_1m` + raw events for recent_failures.
**Rewrite using:** `all_event_stats_1m` consistently. `recent_failures` needs raw (OK with 1h retention).

**`/api/analytics/network-health` (ApiNetworkHealth):**
Multi-signal health scoring. Source: `event_stats_1m` + raw events.
**Rewrite using:** aggregates + LiveCounters + `node_stats`.

**`/api/analytics/sync-status/timeline` + `/api/analytics/connections/timeline`:**
5-min bucketed timelines. Source: Raw events types 10-13 (status) and 20-28 (connections).
**Rewrite using:** `all_event_stats_30s` for types 10-13, 20-28 (these are NOT pre-aggregated currently, so they're in `event_stats_30s`). When all types move to count tables, need new count table groups for status + connections.

**`/api/metrics/execution` (ApiExecutionMetrics):**
```typescript
{
  by_service: [{service_id, avg_refine_time_ns, avg_accumulate_time_ns, gas_used}];
  totals: {avg_refine_time_ns, avg_accumulate_time_ns};
}
```
Source: `jsonb_array_elements()` on Refined/BlockExecuted events (types 95, 101, 47).
**Rewrite using:** `event_services` table (has gas per service per event). Timing data may need new extraction at ingestion time or raw events (1h retention).

**`/api/da/stats` + `/api/da/stats/enhanced`:**
See Section 4.9. Legacy uses raw Status events for inventory. Grafana uses `da_node_stats` for ops.
**Rewrite inventory portion using:** `node_stats` table (already has da_shard_count, da_preimage_count). Enhanced features (shard_distribution, node_health, recent_operations) need either raw events (1h OK) or new dedicated tables.

**`/api/nodes/{id}/status/enhanced`:**
Core assignment per node. Source: GROUP BY on JSONB cast. **Rewrite using:** `wp_tracking` or `node_stats`.

**`/api/validators/cores`:**
Validator-to-core mapping. Source: Raw events. **Rewrite using:** `wp_tracking` (guarantor data maps validators to cores).

**`/api/network/topology`:**
Peer topology. Source: Read-only metadata query. Low traffic — keep or migrate.

---

## 5. Migration Plan by Endpoint

### Phase 0: Operational alignment (prerequisite)

> **Phase 0 is a hard blocker for all of Phase 1** (except `/api/nodes` swap which has no param changes) **and all of Phase 2.** Must complete first.

| Task | Area | Effort |
|---|---|---|
| Change `withDuration()` in `lib/api.ts` to send `start`/`end` ISO timestamps instead of duration string. Called by 15+ endpoints. `time-range-context.tsx` already computes start/end internally — just needs to thread through API client. | UI | 1h |
| Add `offset`/`limit` params + pagination response wrapper to `/grafana/events`. Add `PaginationMeta { offset, limit, total, has_more }` response type. `total` requires second COUNT(*) query. | Backend | 2h |
| Add `node`, `core`, and `wp_hash` filter params to `/grafana/events`. All three columns exist with indexes (`node_id` from migration 001, `core` hot column from migration 004). `wp_hash` requires JSONB path extraction but only for non-pre-aggregated types. | Backend | 45 min |
| Add `created_at` field to grafana events response. Column exists in schema (migration 001, `DEFAULT NOW()`). Just add to SELECT + EventRow struct. | Backend | 15 min |
| Make `event_types` parameter optional on `/grafana/events`. Currently required — ForensicsView needs to search without type filter. If omitted, return all types present in `ingested_raw_events`. | Backend | 30 min |
| Add `last_activity` field to `/grafana/cores` response. Correlated subquery: `(SELECT MAX(first_seen) FROM wp_tracking WHERE wp_tracking.core = core_stats_1m.core)`. Uses existing index `idx_wp_tracking_core`. Add field to `CoreSummary` struct. **Type: `Option<DateTime<Utc>>`** — NULL for cores with no WP activity in the time range. Phase 2 client-side status derivation should handle NULL as "stale." | Backend | 30 min |
| **Subtotal** | | **~5h** |

### Phase 1: Quick swaps (grafana endpoint exists, UI field rename)

> **Requires Phase 0 completion** for param conversion (duration → ISO timestamps). Exception: `/api/nodes` has no params and can swap independently.

| # | Legacy endpoint | Switch to | UI change | Backend change | Effort |
|---|---|---|---|---|---|
| 3 | `/api/nodes` | `/api/grafana/nodes` | Unwrap `{nodes:[]}` → flat array, rename `event_count` → `total_event_count` in `ValidatorsView.tsx`. No param changes — cleanest swap, do first. | None | 15 min |
| 2 | `/api/stats` | `/api/grafana/stats` | Rename `best_block` → `best_slot`, `finalized_block` → `finalized_slot` in `telemetry-context.tsx`, `summary-view.tsx`, `DashboardView.tsx`. `total_blocks_authored` drops (unused in UI). Grafana response adds 7 new fields (`connected_nodes`, `slot_events`, `guarantees`, `failures`, `wp_events`, `events_per_sec_10s`, `blocks_per_sec_10s`, `active_nodes`) — UI should consume or ignore. **Opportunity:** `events_per_sec_10s`, `blocks_per_sec_10s`, `active_nodes` overlap with `/api/metrics/live` polling (1s interval). After this swap, the UI could read these from `/grafana/stats` and reduce or eliminate the separate `/api/metrics/live` poll. | None | 30 min |
| 28 | `/api/metrics/timeseries` | `/api/grafana/timeseries` | **Highest risk swap.** (1) Unwrap envelope `{metric, data:[]}` → flat array. (2) Rename `timestamp` → `ts`, `total_events` → `count`. (3) Convert `?metric=events\|blocks\|throughput` → `?event_types=...` (blocks = `event_types=42,43,...`; events = omit filter). (4) Convert `?interval=5` (int minutes) → `?interval=5m` (string duration). Unused legacy fields safely dropped: `active_nodes`, `event_types`, `events_per_second`, `interval_minutes`, `duration_hours`. | None | 45 min |
| 33 | `/api/analytics/block-propagation` | (delete) | Delete `useBlockPropagation()` hook + `normalizeBlockPropagation()` in `lib/api.ts` — verified no UI component uses them | Delete endpoint from `api.rs` | 15 min |
| — | `/api/metrics/timeseries/grouped` | (delete) | API client method exists but verified never called by any UI component | Delete endpoint from `api.rs` | 5 min |
| **Subtotal** | | | | | **~2h** |

### Phase 2: Extend existing grafana endpoints

> **Requires Phase 0 completion.** Extension 1 also partially blocked on Phase 4 #15 (`/grafana/wp-active`) for `active_work_packages` count.

| # | Legacy endpoint | Grafana replacement | Changes needed | Effort |
|---|---|---|---|---|
| 22 | `/api/cores/status` | UI composes from: (1) `/grafana/cores` for telemetry counts + `last_activity` (added in Phase 0), (2) `/grafana/onchain/cores` for on-chain activity → derive status (gas_used > 0 = active, within 24h = idle, else stale), (3) `/grafana/wp-active` (Phase 4 #15) for in-flight WP count per core. Client-side join by `core`. `summary` (active/idle/stale counts) computed client-side. `active_work_packages` shows 0 until Phase 4 #15 ships — acceptable for initial migration. | UI 2h |
| 11 | `/api/events/search` | `/grafana/events` (after Phase 0) | All backend work done in Phase 0 (pagination, node/core/wp_hash filters, created_at, event_types optional). UI update: point ForensicsView to new endpoint, adapt to new response envelope (`{events:[], pagination:{}}` vs flat array). Legacy had `event_id` field — grafana doesn't include it (not rendered in UI, safe to drop). | UI 1.5h |
| 8 | `/api/nodes/{id}/events` | `/grafana/events?node=X` | Depends on Phase 0 `node` filter. Legacy JOINs `nodes` table for `implementation_name`/`implementation_version` — grafana EventRow doesn't include these. **Verified: no UI component currently calls this endpoint.** Safe to migrate without the JOIN fields. If needed later, UI can fetch node metadata separately from `/grafana/nodes`. | UI 30 min |
| **Subtotal** | | | | **~4h** |

### Phase 3: New grafana endpoints (trivial — aggregate queries)

Most follow the pattern: query struct + `SUM(event_count) FROM all_event_stats_*` + handler + response type. Two endpoints use `guarantee_convergence` for node→core mapping (shared helper).

> **Node→core mapping caveat:** Telemetry doesn't transmit `validator_index`. There's no way to map `node_id` → `validator_index` → protocol-assigned core without upstream JIP-3 changes. Endpoints #21 and #39 use **observed guarantee behavior** from `guarantee_convergence.builder_node_id` — which core a node actually guaranteed for, not which core it was assigned to. This should be documented in endpoint API docs / utoipa annotations.

| # | Legacy endpoint | New grafana endpoint | Source | UI consumer | Effort | Notes |
|---|---|---|---|---|---|---|
| 32 | `GET /api/analytics/failure-rates` | `/grafana/failure-rates` | `all_event_stats_1m` for overall/by_category/by_node counts (failure types: 41,44,46,81,83,92,99,107,111,113,122,127). Raw events (1h) for `recent_failures[]` with reason text (JSONB extraction) + `event_name` (derive from event type mapping, same as `/grafana/event-types` endpoint). | `FailuresView.tsx` | 2h | Also fixes legacy bug: types 107,111,113 returned 0 (pre-aggregated, skipped raw). Count tables via UNION view return correct data. |
| 35 | `GET /api/analytics/sync-status/timeline` | `/grafana/sync-timeline` | `status_counts` (Phase 6) for type **11** (BestBlockChanged). Uses `slot` dimension to compute network max slot per bucket, then counts nodes within 2 slots of max as "synced." NOT types 10-13 generically — specifically type 11 with slot extraction. | `DashboardView.tsx` | 1h | Pre-Phase 6: query `all_event_stats_30s` for type 11 counts (available via `event_stats_30s`). Post-Phase 6: query `status_counts` directly for slot dimension. |
| 36 | `GET /api/analytics/connections/timeline` | `/grafana/connections-timeline` | `all_event_stats_30s` for types **23** (ConnectedIn), **26** (ConnectedOut), **27** (Disconnected) — not types 20-28 generically. `nodes` table for per-node uptime and health_stats (maintained by batch_writer on connect/disconnect events). | `DashboardView.tsx` | 1h | Corrected event types from plan's original "20-28." |
| 20 | `GET /api/guarantees` | `/grafana/guarantees` | `all_event_stats_1m` for types 105-113 (all accessible via UNION view — count tables for 106-113, event_stats for 105,109). Single query, GROUP BY event_type. Raw events (1h) for `recent[]` events with details. | Guarantees view | 2h | Fixes legacy bug: types 106-113 returned 0 from raw events. **Behavior change:** guarantee counts will jump from near-zero to real values after migration. UI may need layout adjustments for larger numbers. Document as expected improvement, not regression. |
| 21 | `GET /api/guarantees/by-guarantor` | `/grafana/guarantees/by-guarantor` | `guarantee_convergence` for per-node core mapping (`builder_node_id`, `core`, 90d retention). `all_event_stats_1m` for per-node success rates (types 105,107,109). **Shares `node_core_mapping()` helper with #39.** | Guarantees view | 3h | Node→core is observed behavior, not protocol assignment. Document in API docs. |
| 14 | `GET /api/workpackages` | `/grafana/wp-stats` | `wp_tracking` for pipeline stage counts (received through distributed/failed) + by_core breakdown. `all_event_stats_1m` for pre-pipeline counts (types 90 submissions, 91 being_shared, 93 duplicates). `recent_submissions[]` and `work_package_size` NOT rendered by UI — safe to drop. | `WorkPackagesView.tsx`, `summary-view.tsx`, `ExportPipelineTab` | 2h | Two-source: wp_tracking (stages) + aggregates (pre-pipeline counts). |
| 39 | `GET /api/validators/cores` | `/grafana/validators/cores` | `guarantee_convergence` for node→core mapping (same `node_core_mapping()` helper as #21). UI only reads `node_core_mapping[].{node_id, primary_core}` — does NOT use `guarantee_activity`, `ticket_activity`, or `core_summary` from legacy response. | `ValidatorsView.tsx`, `chain-telemetry.tsx` | 1h | Simpler than originally scoped — UI needs just the mapping, not the full breakdown. |
| 34 | `GET /api/analytics/network-health` | `/grafana/network-health` | `all_event_stats_1m` for 5-component health scoring (connectivity, block production, DA, work packages, throughput). LiveCounters for real-time throughput overlay. `node_stats` for peer counts. Scoring logic (~200 LOC) moves from `store.rs` to `grafana_store.rs`. | `DashboardView.tsx` (health card, alert badge) | 3h | `all_event_stats_1m` view survives Phase 6 (rebuilt with count tables only). |
| ~~30~~ | ~~`GET /api/metrics/realtime`~~ | ~~`/grafana/realtime`~~ | ~~LiveCounters~~ | — | ~~1h~~ | **REMOVED.** `/api/metrics/live` and `/api/metrics/realtime` are pure in-memory endpoints (LiveCounters ring buffer, no DB). Listed in Section 3.1 as "no migration needed." No reason to create grafana equivalents. |
| **Subtotal** | | | | | **~15h** | Was 16h (-1h from removing realtime) |

### Phase 4: New grafana endpoints (moderate — joins/dedicated tables)

> **Prerequisite:** Phase 6 adds `wp_hash BYTEA` hot column to `ingested_raw_events` (indexed, populated by batch_writer from enriched.wp_hash). Enables `/grafana/wp/{hash}` journey drilldown without JSONB correlation chains.
>
> **Deliberately dropped legacy stages:** The legacy `/api/workpackages/active` included post-guarantee "stages" that are NOT real pipeline stages and are removed in the migration:
> - **`included`** = GuaranteeDiscarded (113) with reason `PackageReportedOnChain` — a node cleaning up its local guarantee pool after seeing the guarantee on-chain, not an inclusion event.
> - **`available`** = shard events (120/124) matched via erasure_root — DA activity happening in parallel, not a sequential WP pipeline stage.
> - **`superseded`** = WorkPackageFailed (92) with reason "work package was refined by another guarantor" — derivable from `failure_reason` column (Phase 4 Step 2) if needed, but not a distinct pipeline stage.
>
> The telemetry-visible WP pipeline ends at `distributed_at`. After distribution, the WP's fate is determined on-chain (block inclusion, accumulation) — telemetry doesn't cover that path.

| # | New endpoint | Source tables | UI consumer | Effort | Notes |
|---|---|---|---|---|---|
| 15a | `/grafana/wp-active` (Step 1) | `wp_tracking WHERE distributed_at IS NULL AND failed_at IS NULL AND first_seen > $start`. Returns full envelope: WP list + `summary` (per-stage counts) + `reached` (cumulative funnel) + `stage_duration_percentiles` (p50/p95 on inter-stage deltas). **Requires migration:** add `node_id TEXT` and `refine_gas_used BIGINT` to `wp_tracking`. `node_id` populated from event 94 node_id at ingestion. `refine_gas_used` populated from event 101 SUM(costs[].total.gas_used). See deep-dive Section 8. | ImportPipelineTab, ExportPipelineTab, WorkPackagesView, DashboardView | 2.5h | Cross-check during impl: (1) enricher resolves type 101 → wp_hash reliably? (2) costs array accessible in server.rs? (3) multiple refinements per WP — first-write-wins or MAX? |
| 15b | `/grafana/wp-active` (Step 2) | Add `failure_reason TEXT` and `discard_reason TEXT` columns to `wp_tracking` (migration). Populate `failure_reason` from event 92 in `server.rs` (enricher already resolves WP, ~20 LOC). Populate `discard_reason` from event 113 via `guarantee_convergence.work_report_hash → wp_hash` mapping (~30 LOC). Enables `failure_breakdown` in response + per-WP failure display. | WorkPackagesView (FailureAnalysis panel, per-WP failure_reason column) | 2h | |
| 16-17 | `/grafana/wp/{hash}` | Two queries: (1) `wp_tracking WHERE wp_hash = $1` → summary (single row, instant). (2) `ingested_raw_events WHERE wp_hash = $1 ORDER BY timestamp` → all events for this WP (1h retention, indexed via Phase 6 hot column). If WP older than 1h, only summary available — acceptable for real-time investigation. | WorkPackageDetailView (journey timeline) | 3h | |
| 18 | ~~`/grafana/wp/{hash}/audit`~~ | ~~`wp_tracking` + raw events~~ | WP detail view | **Replace AuditProgressPanel with WP Pipeline Status panel using `/grafana/wp/{hash}` (row 16-17). UI rework: 3-4h. See deep-dive Section 2.** | |
| 19 | `/grafana/wp/batch` | `wp_tracking WHERE wp_hash = ANY($1)` — multi-hash version of wp/{hash} summary. | Batch WP tracking | 1h | |
| ~~25~~ | ~~`/grafana/cores/{id}/work-packages`~~ | ~~`wp_tracking` + `event_services`~~ | ~~Core detail WP tab~~ | **REMOVED. Folded into existing `/grafana/cores/:core_id` (`core_detail`).** Already returns 100 WpTrackingRow per core. Extend with: `node_id` (new column), `refine_gas_used` (new column), optional pagination params. ~1h incremental on existing endpoint. | |
| 26 | `/grafana/cores/{id}/validators` | `guarantee_convergence` (builder_node_id + core, 90d retention) + `nodes` table JOIN for implementation details. Same `node_core_mapping()` helper as Phase 3 #21/#39. | CoreDetailView (validators tab) | 2.5h | **Limitation:** `guarantee_convergence` only contains validators who actually built guarantees. Validators assigned to a core but inactive (no guarantees built) won't appear. Legacy `total_assigned` vs `active_count` distinction becomes meaningless — both equal the set of active guarantors. Document in API docs. |
| 23 | `/grafana/cores/{id}/metrics` | `core_stats_1m` for efficiency percentages (event counts). `wp_tracking` for completion times (percentile on stage deltas, same pattern as `/grafana/bottlenecks`). LiveCounters for throughput. `refine_gas_used` from `wp_tracking` for gas utilization. | CoreDetailView (metrics overview tab) | 3h | |
| 13 | `/grafana/blocks/summary` | `all_event_stats_1m` for block event totals (types 40-47, 11, 12). LiveCounters for best/finalized slot. Raw events (1h) for recent block hashes + per-block detail. `slot_convergence` for propagation percentiles. | BlocksView (metric cards + recent blocks table) | 4h | |
| **Subtotal** | | | | **~18h** | Was 19.5h (-1.5h from folding #25 into core_detail) |

### Phase 5: Hard rewrites

| # | Endpoint | Challenge | Approach | Effort |
|---|---|---|---|---|
| 24 | `/grafana/cores/{id}/bottlenecks` (validator profiling) | Current grafana version profiles pipeline stages, UI needs validator profiling | Rewrite using `wp_tracking` — group by `received_by`/`guaranteed_by` node, compute per-node stage durations | Backend 4h + UI 0 (same response shape) |
| 27 | `/grafana/execution` | `jsonb_array_elements()` cross-join is the problem | Use `event_services` table (has gas per service). For timing: extract at ingestion to new columns or accept 1h raw retention | Backend 3h + UI 1h |
| 40-41 | `/grafana/da-inventory` | Legacy reads Status JSONB for shard/preimage inventory | Query `node_stats` table (already has da_shard_count, da_preimage_count). Combine with existing `/grafana/da-stats` for ops. Enhanced features (node_health, shard_distribution) compute from node_stats aggregates | Backend 4h + UI 2h |
| **Subtotal** | | | | **~14h** |

### Phase 6: Expand count tables (backend infrastructure)

| Task | Details | Effort |
|---|---|---|
| Add count table groups for all remaining event types | **5 new tables**: `status_counts` (0, 10-13), `connection_counts` (20-28), `block_counts` (40-47), `ticket_low_counts` (80-82), `wp_pipeline_counts` (90-105). Types 90-105 now included — `core` will be NULL when enrichment fails (types 90, 91, 103 always NULL; others occasionally). Count tables handle NULL dimensions; total counts remain accurate. See deep-dive Section 9. | 3.5h |
| Extend `event_counter.rs` | All 115 types become pre-aggregated. Add new types to `PRE_AGGREGATED_TYPES`, add `CountKey` dimension extraction. Status/connections/blocks use native fields (slot, reason). WP pipeline types use enriched core (nullable). | 3h |
| Create continuous aggregates | `*_1m` and `*_1h` for each new table, same pattern as existing 9 | 1.5h |
| Update UNION views | Rebuild `all_event_stats_30s/1m/1h` and `all_core_stats_1m` to reference **only count tables** (remove `event_stats_*` and `core_stats_1m` branches). 14 count table branches total. Validate with EXPLAIN ANALYZE. See deep-dive Section 7. | 1.5h |
| Drop continuous aggregates | Remove `event_stats_30s`, `event_stats_1m`, `event_stats_1h`, `core_stats_1m`. Count tables are now the single aggregation source. | 30 min |
| Update `batch_writer.rs` | Remove `is_pre_aggregated` skip — **all 115 types** now write to `ingested_raw_events` (for 1h browsing). Hot columns (`slot`, `core`, `submission_id`) populated from enricher for all types. **Add `wp_hash BYTEA` hot column** (indexed: `idx_ire_wp_hash ON ingested_raw_events (wp_hash, timestamp DESC) WHERE wp_hash IS NOT NULL`). Populated from `enriched.wp_hash`. Enables `/grafana/wp/{hash}` journey drilldown without JSONB correlation chains. | 1.5h |
| Set retention on `ingested_raw_events` | `SELECT add_retention_policy('ingested_raw_events', INTERVAL '1 hour')`. Table becomes a pure browsing store — no aggregation depends on it. | 15 min |
| Update `/grafana/events` | Remove `is_pre_aggregated` rejection check. All 115 types browsable. (Also add `node`/`core` filter params if not done in Phase 0.) | 30 min |
| Migration SQL | Single migration file with new tables + new aggregates + rebuilt UNION views + drop old aggregates | 2h |
| **Subtotal** | | **~13.5h** |

### Phase 7: Node detail endpoints (lower priority)

| # | Endpoint | Action | Effort |
|---|---|---|---|
| 4 | `/api/nodes/{id}` | Keep as-is — simple SELECT on `nodes` table | 0 |
| 5 | `/api/nodes/{id}/status` | Keep as-is — single-row lookup | 0 |
| 6 | `/api/nodes/{id}/status/enhanced` | Rewrite to query `wp_tracking` + `node_stats` instead of raw JSONB | 2h |
| 7 | `/api/nodes/{id}/peers` | Keep as-is — small query | 0 |
| 9 | `/api/nodes/{id}/timeline` | Delete — redundant with `/grafana/events?node=X` | 0 |
| 12 | `/api/slots/{slot}` | Keep — slot column is indexed, query is bounded. Raw events OK with 1h retention (slots are recent) | 0 |
| 37 | `/api/network` | Keep — small metadata query | 0 |
| 38 | `/api/network/topology` | Keep or migrate — read-only, low frequency | 1h |
| **Subtotal** | | | **~3h** |

---

## 6. Total Effort Estimate

| Phase | Description | Backend | UI | Total | Notes |
|---|---|---|---|---|---|
| 0 | Operational alignment (time range, pagination, events filters, last_activity) | 4h | 1h | **5h** | Revised: +wp_hash filter, +event_types optional, +last_activity on /grafana/cores |
| 1 | Quick swaps | 0.5h | 1.5h | **2h** | Revised: timeseries swap needs param conversion (metric→event_types, interval format) |
| 2 | Extend existing grafana endpoints | 0h | 4h | **4h** | Revised: all backend in Phase 0. cores/status = UI composition. events/search = UI adaptation. |
| 3 | New trivial grafana endpoints | 11h | 4h | **15h** | Revised: removed /grafana/realtime (pure in-memory, no migration needed). Fixed data sources for sync-timeline, connections-timeline, guarantees/by-guarantor, validators/cores. Shared node_core_mapping() helper. |
| 4 | New moderate grafana endpoints | 11h | 7h | **18h** | Revised: #25 folded into core_detail (-1.5h). 15a adds node_id + refine_gas_used to wp_tracking (+0.5h). wp/{hash} uses wp_hash hot column from Phase 6. |
| 5 | Hard rewrites | 11h | 3h | **14h** | |
| 6 | Expand count tables + drop continuous aggregates | 14h | 0 | **14h** | Revised: +wp_hash hot column on ingested_raw_events (+0.5h). 5 new count tables. Drop continuous aggregates. All types to raw with 1h retention. See deep-dive Section 9. |
| 7 | Node detail cleanup | 2h | 1h | **3h** | |
| | **Total** | **~53h** | **~22.5h** | **~75.5h** | Revised |

---

## 7. Suggested Execution Order

> **Key insight:** Phase 6 (count table expansion) should come early, not late. It establishes the unified data architecture that all subsequent phases build on. After Phase 6, every endpoint has a single answer: "query `all_event_stats_*` for aggregates, query `ingested_raw_events` for individual events." Without it, each endpoint must reason about pre-aggregated vs raw paths.

**Week 1: Ground preparation (backend infrastructure)**
- Phase 0 (5h) — operational alignment: time range params, events filters, last_activity on `/grafana/cores`. Hard blocker for all subsequent phases.
- Phase 6 (13.5h) — expand count tables to all 115 types, write all types to raw with 1h retention, drop continuous aggregates (`event_stats_30s/1m/1h`, `core_stats_1m`), rebuild UNION views. After this: unified data path, `/grafana/events` works for all types, `all_event_stats_*` covers everything.

**Week 2: Quick wins + new endpoints**
- Phase 1 (2h) — quick swaps (nodes, stats, timeseries). Now trivial — all data paths are clean.
- Phase 3 (15h) — new trivial grafana endpoints. All use unified `all_event_stats_*`, no special cases for pre-aggregated types.

**Week 3: Extensions + moderate endpoints**
- Phase 2 (4h) — UI composition from existing endpoints (cores/status, events/search, node events).
- Phase 4 (21h) — new moderate endpoints (wp-active, wp/{hash}, core detail, blocks summary).

**Week 4: Hard rewrites + cleanup**
- Phase 5 (14h) — hard rewrites (bottlenecks, execution metrics, DA).
- Phase 7 (3h) — node detail endpoint cleanup.
- Remove dead legacy endpoints from `api.rs` + dead store methods from `store.rs`.
- Set `--disable-legacy-endpoints` flag.
- Update `lib/api.ts` in UI to point all calls to `/api/grafana/*`.

---

## 8. Risks and Dependencies

| Risk | Impact | Mitigation |
|---|---|---|
| Continuous aggregates on raw events stop working when raw retention shrinks | All `event_stats_*` aggregates return stale data | Must complete Phase 6 (count table expansion) BEFORE setting retention |
| `wp_tracking` doesn't have all fields needed for core-detail endpoints | Missing gas, extrinsic_count, implementation details | May need to add columns to `wp_tracking` or join with `event_services` + `nodes` |
| ~~1h raw retention may not cover long-running audit processes~~ | ~~WP audit-progress endpoint returns incomplete data~~ | ~~Audit typically completes in <5 min; monitor and extend retention if needed~~ **SUPERSEDED:** Legacy audit-progress endpoint queries guarantee events (105-113), not actual audit events (140-153). The endpoint is fundamentally wrong and should be replaced, not migrated. See deep-dive Section 2. |
| UI assumes specific response envelope shapes | Breaking changes if not careful | Run legacy + grafana endpoints in parallel during migration (`--disable-legacy-endpoints` flag already exists); switch UI endpoint-by-endpoint. See deep-dive Section 5. |
| Count table UNION views with 14+ sources may have query planner issues | Slow queries on `all_event_stats_*` | **Must run EXPLAIN ANALYZE before expanding.** Verify constraint exclusion prunes irrelevant UNION branches. Fallback: query-time table routing in `grafana_store.rs`. See deep-dive Section 7. |
| Legacy audit-progress endpoint is fundamentally wrong | UI (`WorkPackageDetailView.tsx` AuditProgressPanel) renders fake data — status/tranche/panic are all broken. Users see misleading information. | Replace AuditProgressPanel with WP Pipeline Status panel using real `wp_tracking` data. Proper audit tracking (tranches, re-execution, judgments) is a separate future initiative requiring upstream polkajam telemetry additions. |
| ~~Types 90-105 cannot use count tables~~ | ~~Enrichment dependency~~ | **SUPERSEDED:** Types 90-105 now included in count tables with nullable `core` dimension. NULL core when enrichment fails is acceptable — total counts remain accurate, per-core breakdown is best-effort. See deep-dive Section 9. |
| Dropping continuous aggregates is irreversible | `event_stats_30s/1m/1h` and `core_stats_1m` contain historical data. Once dropped, only count tables serve aggregation. | Ensure count tables cover all 115 types and UNION views are rebuilt BEFORE dropping continuous aggregates. Validate with EXPLAIN ANALYZE. Keep a pg_dump backup of aggregate data before dropping. |
| Writing all 115 types to raw increases write volume | `ingested_raw_events` write rate increases from ~0.3M/sec (41 types) to ~3M/sec (all 115 types). | Bounded by 1h retention — chunks drop continuously. TimescaleDB compression on older chunks. Monitor disk usage and chunk drop lag during rollout. |

---

## 9. Files to Edit

### Backend (jamtart-v3)

| File | Changes |
|---|---|
| `src/grafana.rs` | New route registrations, handler functions, query param structs, utoipa annotations |
| `src/grafana_store.rs` | New query methods (efficient SQL using aggregate tables) |
| `src/grafana_types.rs` | New response type structs |
| `src/event_counter.rs` | Expand `PRE_AGGREGATED_TYPES` to all 115 types, add new table mappings, new flush logic |
| `src/batch_writer.rs` | Update filter to skip all types (or write minimal rows) |
| `migrations/0XX_expand_count_tables.sql` | New count tables + aggregates + UNION view updates |
| `src/api.rs` | Eventually remove legacy endpoints (after UI migration complete) |
| `src/store.rs` | Eventually remove legacy store methods |

### UI (jamtart-ui)

| File | Changes |
|---|---|
| `lib/api.ts` | Change `withDuration()` to send ISO timestamps, update endpoint URLs to `/api/grafana/*`, update response unwrapping |
| `types/api-types.ts` | Update type definitions to match new grafana response shapes |
| `hooks/use-api.ts` | Update SWR hooks to point to new endpoints |
| `contexts/telemetry-context.tsx` | Update field names (best_block → best_slot, etc.) |
| `components/summary-view.tsx` | Rename fields |
| `components/views/dashboard/DashboardView.tsx` | Rename fields, unwrap new response shapes |
| `components/views/blocks/BlocksView.tsx` | Consume new blocks/summary endpoint |
| `components/views/cores/CoresView.tsx` | Consume extended grafana/cores response |
| `components/views/cores/CoreDetailView.tsx` | Point to new core detail grafana endpoints |
| `components/shared/statistical-breakdown.tsx` | Unwrap flat timeseries array |
| Various other view components | Endpoint URL updates |

---

## 10. New Count Tables Needed (Phase 6)

To cover the remaining event types that go to raw events and have only native (non-enriched) dimensions:

| New table | Event types | Extra dimensions |
|---|---|---|
| `status_counts` | 0, 10, 11, 12, 13 | `slot INT` (for BestBlock/Finalized) |
| `connection_counts` | 20-28 | `reason TEXT` |
| `block_counts` | 40-47 | `slot INT`, `reason TEXT` |
| `ticket_low_counts` | 80-82 | `reason TEXT` |
| `wp_pipeline_counts` | 90-105 | `core SMALLINT` (nullable — enrichment may fail), `reason TEXT` |

> **REINSTATED: `wp_pipeline_counts` (90-105)** — Previously removed due to enrichment concerns. Now included because: (1) count tables handle NULL dimensions fine — total counts remain accurate even when `core` is NULL, (2) with the revised architecture all events go to count tables as the single aggregation source, (3) continuous aggregates (`event_stats_30s/1m/1h`) are being dropped — count tables must cover everything. Types 90-91 and 103 will always have `core = NULL`; other types occasionally NULL when enricher lookup fails. See deep-dive Section 9.

Each new table needs:
- Hypertable with 1-day chunks
- Index on `(node_id, event_type, bucket DESC)`
- Compression policy
- `*_1m` continuous aggregate
- `*_1h` continuous aggregate

After Phase 6, UNION views (`all_event_stats_30s/1m/1h`) reference **only count tables** (14 branches). The `event_stats_*` and `core_stats_1m` continuous aggregate branches are removed.

### New `CountKey` dimensions for `event_counter.rs`

| Type range | Dimensions to extract | Source |
|---|---|---|
| 0 (Dropped) | None | — |
| 10-13 (Status) | `slot` from BestBlock/Finalized events | Native |
| 20-28 (Connections) | `reason` from failure events | Native |
| 40-47 (Blocks) | `slot` from Authored/Importing, `reason` from failures | Native |
| 80-82 (Tickets low-vol) | `reason` from TicketGenerationFailed | Native |
| 90-105 (WP pipeline) | `core` (nullable), `reason` from failures | Enriched (core), Native (reason) |

---

## 11. What Can Be Deleted After Migration

### Backend
- All store methods in `src/store.rs` that are replaced by grafana equivalents (~40 methods)
- Legacy handler functions in `src/api.rs` (~50 handlers)
- Unused response types and JSON building code
- `get_block_propagation()` — unused
- `get_timeseries_grouped()` — unused

### UI
- `useBlockPropagation()` hook + normalizer — unused
- Node timeline components — redundant with events?node=X
- Legacy duration parameter helpers (replaced by ISO timestamp approach)

### Database
- Eventually: continuous aggregates on `ingested_raw_events` (`event_stats_30s/1m/1h`, `core_stats_1m`) — replaced by count-table equivalents
- Eventually: `ingested_raw_events` hypertable if retention is 0 (or keep at 1h for debug)
