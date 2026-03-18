# Grafana Integration Guide

This guide covers the JamTart Grafana HTTP API and dashboard inventory. All endpoints return JSON and are designed for use with the Grafana Infinity data source plugin (JSON mode). The base path is `/api/grafana`.

> **Source of truth:** Route definitions live in `src/grafana.rs`. Query structs: `TimeseriesQuery`, `TimeRangeQuery`, `ServiceQuery`, `ServiceTimeseriesQuery`, `EventsQuery`, `EventTypesParams`. Event type metadata: `src/event_type_meta.rs`.

---

## Table of Contents

- [1. Endpoint Catalog](#1-endpoint-catalog)
- [2. Shared Query Types](#2-shared-query-types)
- [3. Dashboard Inventory](#3-dashboard-inventory)
- [4. Grafana Variable Templates](#4-grafana-variable-templates)
- [5. Time Range Handling](#5-time-range-handling)
- [6. Aggregate Retention Policy](#6-aggregate-retention-policy)
- [7. Docs-in-Code](#7-docs-in-code)

---

## 1. Endpoint Catalog

All endpoints are **GET** and mounted under `/api/grafana` (see `src/grafana.rs:16-33`).

### 1.1 GET /api/grafana/timeseries

Time-series event counts with automatic aggregate table selection.

**Query:** `TimeseriesQuery`

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `start` | ISO 8601 datetime | yes | - | Start of time range |
| `end` | ISO 8601 datetime | yes | - | End of time range |
| `interval` | string | no | `1m` | Bucket width. Supported: `6s`, `12s`, `18s`, `24s`, `30s`, `1m`, `2m`, `5m`, `10m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d`. Unsupported values (e.g. Grafana `$__interval`) are snapped up to the nearest valid interval. |
| `group_by` | string | no | `event_type` | Grouping column. Allowed: `node_id`, `event_type`, `core` |
| `node` | string | no | - | Filter to a single node_id |
| `event_types` | string | no | - | Comma-separated list: numeric IDs, group names, or event names. Supports Grafana `{a,b}` braces. |
| `core` | i16 | no | - | Filter to a single core index |

**Aggregate Table Auto-Selection**

| Condition | Table Used |
|-----------|-----------|
| `group_by=core` | `core_stats_1m` |
| interval < 60s | `event_stats_30s` |
| interval < 3600s | `event_stats_1m` |
| interval >= 3600s | `event_stats_1h` |

**Response:** `[{ "ts", "count", "<group_column>" }]`

The group column in the response matches `group_by`: `event_type` (i16), `node_id` (string), or `core` (i16).

```bash
curl 'http://localhost:8080/api/grafana/timeseries?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z&interval=5m&group_by=event_type&event_types=105,92'
```

---

### 1.2 GET /api/grafana/stats

Dashboard summary counters for the given time range.

**Query:** `TimeRangeQuery`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `node` | string | no | Filter to a single node_id |
| `core` | i16 | no | Filter to a single core |
| `event_type` | i16 | no | Filter to a single event type |

**Response:**

```json
{
  "connected_nodes": 12,
  "slot_events": 1800,
  "guarantees": 540,
  "failures": 3,
  "wp_events": 480
}
```

```bash
curl 'http://localhost:8080/api/grafana/stats?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'
```

---

### 1.3 GET /api/grafana/cores

Per-core work package, guarantee, and failure counts (summary).

**Query:** `TimeRangeQuery`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `node` | string | no | Filter to a single node_id |
| `core` | i16 | no | Filter to a single core |
| `event_type` | i16 | no | Filter to a single event type |

**Response:**

```json
[{ "core": 0, "work_packages": 120, "guarantees": 115, "failures": 2 }]
```

```bash
curl 'http://localhost:8080/api/grafana/cores?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'
```

---

### 1.3b GET /api/grafana/cores/:core_id

Single-core detail — summary stats plus the 100 most recent work packages from `wp_tracking`.

**Path params:** `core_id` (i16) — core index

**Query:** `TimeRangeQuery`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |

**Response:**

```json
{
  "core": 5,
  "work_packages": 120,
  "guarantees": 115,
  "failures": 2,
  "recent_work_packages": [{
    "wp_hash": "a1b2c3...",
    "first_seen": "2025-01-15T12:00:00Z",
    "last_updated": "2025-01-15T12:00:05Z",
    "stage": 6,
    "received_by": 3,
    "guaranteed_by": 7,
    "service_ids": [1, 2],
    "received_at": "2025-01-15T12:00:00Z",
    "authorized_at": "2025-01-15T12:00:01Z",
    "refined_at": "2025-01-15T12:00:02Z",
    "report_built_at": "2025-01-15T12:00:03Z",
    "guarantee_built_at": "2025-01-15T12:00:04Z",
    "distributed_at": "2025-01-15T12:00:05Z",
    "failed_at": null
  }]
}
```

```bash
curl 'http://localhost:8080/api/grafana/cores/5?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'
```

---

### 1.4 GET /api/grafana/blocks/convergence

Block propagation convergence percentiles per slot.

**Query:** `TimeRangeQuery`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `node` | string | no | Filter to a single node_id |
| `core` | i16 | no | Filter to a single core |
| `event_type` | i16 | no | Filter by event type (e.g. 11=BestBlock, 12=Finalized, 43=Importing) |

**Response:**

```json
[{
  "slot": 12345,
  "event_type": 42,
  "node_count": 12,
  "p50_ms": 150,
  "p99_ms": 480,
  "p100_ms": 520,
  "authored_at": "2025-01-15T12:00:00Z"
}]
```

```bash
curl 'http://localhost:8080/api/grafana/blocks/convergence?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'

# Filter to BestBlock convergence
curl 'http://localhost:8080/api/grafana/blocks/convergence?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z&event_type=11'
```

---

### 1.5 GET /api/grafana/blocks/contents

Block contents extracted from BlockAuthored events (event_type=42).

**Query:** `TimeRangeQuery`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `node` | string | no | Filter to a single node_id |
| `core` | i16 | no | Filter to a single core |
| `event_type` | i16 | no | Filter to a single event type |

**Response:**

```json
[{
  "slot": 12345,
  "timestamp": "2025-01-15T12:00:00Z",
  "node_id": "node-01",
  "num_guarantees": 3,
  "num_assurances": 10,
  "num_preimages": 1,
  "num_tickets": 0,
  "num_disputes": 0,
  "extrinsic_size": 4096
}]
```

---

### 1.6 GET /api/grafana/services

Per-service activity and gas usage from the `service_stats_1m` aggregate.

**Query:** `ServiceQuery`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `service` | string | no | Comma-separated service IDs (decimal or `0x` hex). Supports Grafana `{a,b}` braces. |

**Response:**

```json
[{
  "service_id": 1,
  "work_packages": 50,
  "refinements": 48,
  "refinement_gas": 120000000,
  "authorizations": 50,
  "authorization_gas": 5000000,
  "executions": 45,
  "execution_gas": 80000000
}]
```

```bash
# All services
curl 'http://localhost:8080/api/grafana/services?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'

# Specific services (decimal and hex)
curl 'http://localhost:8080/api/grafana/services?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z&service=1,0xFF'
```

---

### 1.7 GET /api/grafana/services/timeseries

Time-bucketed per-service metrics from `service_stats_1m`.

**Query:** `ServiceTimeseriesQuery`

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `start` | ISO 8601 datetime | yes | - | Start of time range |
| `end` | ISO 8601 datetime | yes | - | End of time range |
| `interval` | string | no | `1m` | Bucket width (same allowed values as `/timeseries`) |
| `service` | string | no | - | Comma-separated service IDs (decimal or `0x` hex). Supports Grafana `{a,b}` braces. |

**Response:** Array of time-bucketed rows per service with work package counts and gas metrics.

```bash
curl 'http://localhost:8080/api/grafana/services/timeseries?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z&interval=5m&service=1'
```

---

### 1.8 GET /api/grafana/nodes

All known nodes with metadata. No time range required.

**Query:** None.

**Response:**

```json
[{
  "node_id": "node-01",
  "peer_id": "12D3KooW...",
  "implementation_name": "jamtart",
  "implementation_version": "0.1.0",
  "node_info": {},
  "connected_at": "2025-01-15T00:00:00Z",
  "disconnected_at": null,
  "last_seen_at": "2025-01-15T12:30:00Z",
  "is_connected": true,
  "total_event_count": 150000,
  "address": "10.0.0.1:9000"
}]
```

Sorted by `is_connected DESC, last_seen_at DESC` (connected nodes first).

---

### 1.9 GET /api/grafana/node-stats

Raw node status rows at ~2s granularity from the `node_stats` hypertable.

**Query:** `TimeRangeQuery`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `node` | string | no | Comma-separated node_ids. Supports Grafana `{a,b}` braces. |
| `core` | i16 | no | Filter to a single core |
| `event_type` | i16 | no | Filter to a single event type |

**Response:**

```json
[{
  "timestamp": "2025-01-15T12:00:02Z",
  "node_id": "node-01",
  "num_peers": 24,
  "num_val_peers": 12,
  "num_sync_peers": 8,
  "num_shards": 342,
  "shards_size": 1073741824,
  "num_preimages": 15,
  "preimages_size": 524288,
  "min_guarantees": 0,
  "max_guarantees": 3,
  "avg_guarantees": 1.5,
  "zero_guarantee_cores": 2
}]
```

---

### 1.10 GET /api/grafana/node-stats-aggregate

1-minute aggregated node stats from `node_stats_1m`. Without a node filter, returns network-wide aggregates per bucket. With a node filter, returns per-node rows.

**Query:** `TimeRangeQuery`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `node` | string | no | Comma-separated node_ids. Supports Grafana `{a,b}` braces. |
| `core` | i16 | no | Filter to a single core |
| `event_type` | i16 | no | Filter to a single event type |

**Response (network-wide):**

```json
[{
  "bucket": "2025-01-15T12:00:00Z",
  "avg_peers": 24, "min_peers": 18, "max_peers": 30,
  "avg_val_peers": 12, "min_val_peers": 10, "max_val_peers": 14,
  "avg_sync_peers": 8, "min_sync_peers": 5, "max_sync_peers": 12,
  "avg_shards": 342, "min_shards": 300, "max_shards": 400,
  "avg_shards_size": 1073741824, "max_shards_size": 2147483648,
  "avg_preimages": 15, "max_preimages": 25,
  "avg_preimages_size": 524288, "max_preimages_size": 1048576,
  "avg_guarantees": 1.5, "min_guarantees": 0, "max_guarantees": 3,
  "max_zero_guarantee_cores": 2,
  "status_count": 360
}]
```

With a node filter, each row additionally includes `"node_id"`.

---

### 1.11 GET /api/grafana/db-stats

TimescaleDB internal metadata. No parameters required.

**Response:**

```json
{
  "tables": [{ "table_name": "events", "total_bytes": 1073741824, "table_bytes": 805306368, "index_bytes": 214748364, "toast_bytes": 53687091 }],
  "row_counts": [{ "table_name": "events", "row_count": 15000000 }],
  "compression": [{ "table_name": "events", "compressed_chunks": 12, "before_compression_bytes": 2147483648, "after_compression_bytes": 536870912 }]
}
```

---

### 1.12 GET /api/grafana/bottlenecks

Work package pipeline bottleneck analysis from `wp_tracking`. Returns percentile timings for each pipeline stage and overall failure rate.

**Query:** `TimeRangeQuery`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `node` | string | no | Filter to a single node_id |
| `core` | i16 | no | Filter to a single core index |
| `event_type` | i16 | no | Filter to a single event type |

**Pipeline Stages** (each with `p50_ms` and `p95_ms`)

| Stage | Measures |
|-------|----------|
| `authorize` | `received_at` → `authorized_at` |
| `refine` | `authorized_at` → `refined_at` |
| `report` | `refined_at` → `report_built_at` |
| `guarantee` | `report_built_at` → `guarantee_built_at` |
| `distribute` | `guarantee_built_at` → `distributed_at` |
| `pipeline_total` | `received_at` → `distributed_at` (or `last_updated`) |

**Response:**

```json
{
  "stage_timing": {
    "authorize":      { "p50_ms": 12.5,  "p95_ms": 45.0 },
    "refine":         { "p50_ms": 150.0, "p95_ms": 420.0 },
    "report":         { "p50_ms": 5.0,   "p95_ms": 15.0 },
    "guarantee":      { "p50_ms": 3.0,   "p95_ms": 10.0 },
    "distribute":     { "p50_ms": 8.0,   "p95_ms": 25.0 },
    "pipeline_total": { "p50_ms": 180.0, "p95_ms": 510.0 }
  },
  "failure_rate": 0.02,
  "total_wps": 500,
  "failed_wps": 10,
  "avg_pipeline_ms": 195.5
}
```

---

### 1.13 GET /api/grafana/wp-funnel

Work package pipeline funnel — counts how many WPs reached each stage.

**Query:** `TimeRangeQuery`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `node` | string | no | Filter to a single node_id |
| `core` | i16 | no | Filter to a single core |
| `event_type` | i16 | no | Filter to a single event type |

**Response:**

```json
{
  "received": 500,
  "authorized": 480,
  "refined": 470,
  "report_built": 465,
  "guarantee_built": 460,
  "distributed": 455,
  "failed": 15
}
```

---

### 1.14 GET /api/grafana/guarantee-convergence

Guarantee propagation convergence — per-slot overview. Measures how quickly guarantees propagate across the validator network. Each data point represents one slot, aggregating all guarantees (all cores) for that slot.

The anchor is GuaranteeBuilt(105) — emitted by the guarantor when the guarantee is created. The measured events are GuaranteeReceived(112) — emitted by each validator that receives the guarantee. Percentiles are computed from (received_timestamp - built_at) across all receiving validators, flattened across all guarantees in the slot.

**Query:** `TimeRangeQuery`

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `start` | ISO 8601 datetime | yes | — | Start of time range |
| `end` | ISO 8601 datetime | yes | — | End of time range |

**Response:** `Vec<GuaranteeConvergenceSlotRow>`

```json
[
  {
    "slot": 42,
    "guarantee_count": 15,
    "node_count": 980,
    "p50_ms": 45,
    "p75_ms": 85,
    "p95_ms": 150,
    "p99_ms": 250,
    "p100_ms": 500,
    "built_at": "2025-03-18T12:00:00Z"
  }
]
```

**curl:**

```sh
curl "http://localhost:8080/api/grafana/guarantee-convergence?start=${__from:date:iso}&end=${__to:date:iso}"
```

---

### 1.15 GET /api/grafana/guarantee-convergence/detail

Per-guarantee convergence detail for drill-down. Returns one row per `work_report_hash`, showing the propagation latency from GuaranteeBuilt(105) to GuaranteeReceived(112) for each individual guarantee. Use `core` or `wp_hash` filters to focus on a specific core or work package.

If the guarantor node is not connected to telemetry, `core` and `wp_hash` will be NULL (the enricher on the receiving validators doesn't have the submission chain context).

**Query:** `GuaranteeConvergenceDetailQuery`

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `start` | ISO 8601 datetime | yes | — | Start of time range |
| `end` | ISO 8601 datetime | yes | — | End of time range |
| `core` | i16 | no | — | Filter to a single core |
| `wp_hash` | string (hex) | no | — | Filter to a single work package hash |

**Response:** `Vec<GuaranteeConvergenceDetailRow>`

```json
[
  {
    "work_report_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "slot": 42,
    "core": 5,
    "wp_hash": "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
    "node_count": 980,
    "p50_ms": 45,
    "p75_ms": 85,
    "p95_ms": 150,
    "p99_ms": 250,
    "p100_ms": 500,
    "built_at": "2025-03-18T12:00:00Z"
  }
]
```

**curl:**

```sh
curl "http://localhost:8080/api/grafana/guarantee-convergence/detail?start=${__from:date:iso}&end=${__to:date:iso}&core=5"
```

---

### 1.16 GET /api/grafana/wp-funnel-timeseries

Work package pipeline funnel bucketed over time. Same data as `/wp-funnel` but grouped into time buckets, showing how WP stage counts evolve over time. Each row contains the count of WPs whose `first_seen` falls in that bucket, broken down by pipeline stage.

Events that feed the `wp_tracking` table: WorkPackageReceived(94), Authorized(95), Refined(101), WorkReportBuilt(102), GuaranteeBuilt(105), GuaranteesDistributed(109), WorkPackageFailed(92). The `wp_tracker` module correlates these events across nodes via submission_id chains in the enricher.

**Query:** `WpTimeseriesQuery`

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `start` | ISO 8601 datetime | yes | — | Start of time range |
| `end` | ISO 8601 datetime | yes | — | End of time range |
| `interval` | string | no | `1m` | Bucket width (snapped to nearest valid: 6s–1d) |
| `core` | i16 | no | — | Filter to a single core |

**Response:** `Vec<WpFunnelTimeseriesRow>`

```json
[
  {
    "ts": "2025-03-18T12:00:00Z",
    "total": 45,
    "received": 45,
    "authorized": 43,
    "refined": 42,
    "report_built": 40,
    "guarantee_built": 38,
    "distributed": 36,
    "failed": 2
  }
]
```

**curl:**

```sh
curl "http://localhost:8080/api/grafana/wp-funnel-timeseries?start=${__from:date:iso}&end=${__to:date:iso}&interval=1m"
```

---

### 1.17 GET /api/grafana/bottlenecks-timeseries

Work package pipeline bottleneck percentiles bucketed over time. Same data as `/bottlenecks` but grouped into time buckets, showing how stage-to-stage latency evolves. Per bucket: `percentile_cont(0.5)` and `percentile_cont(0.95)` on inter-stage timestamp deltas from `wp_tracking`.

Stages measured (each as the delta between consecutive pipeline timestamps):
- **authorize**: received_at → authorized_at (Authorized(95) - WorkPackageReceived(94))
- **refine**: authorized_at → refined_at (Refined(101) - Authorized(95))
- **report**: refined_at → report_built_at (WorkReportBuilt(102) - Refined(101))
- **guarantee**: report_built_at → guarantee_built_at (GuaranteeBuilt(105) - WorkReportBuilt(102))
- **distribute**: guarantee_built_at → distributed_at (GuaranteesDistributed(109) - GuaranteeBuilt(105))
- **pipeline_total**: received_at → COALESCE(distributed_at, last_updated) (end-to-end)

WPs where `received_at IS NULL` are excluded. Stage columns are NULL if no WPs in that bucket reached the stage.

**Query:** `WpTimeseriesQuery`

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `start` | ISO 8601 datetime | yes | — | Start of time range |
| `end` | ISO 8601 datetime | yes | — | End of time range |
| `interval` | string | no | `1m` | Bucket width (snapped to nearest valid: 6s–1d) |
| `core` | i16 | no | — | Filter to a single core |

**Response:** `Vec<BottlenecksTimeseriesRow>`

```json
[
  {
    "ts": "2025-03-18T12:00:00Z",
    "authorize_p50": 12.5,
    "authorize_p95": 45.2,
    "refine_p50": 85.0,
    "refine_p95": 210.0,
    "report_p50": 5.1,
    "report_p95": 15.3,
    "guarantee_p50": 2.0,
    "guarantee_p95": 8.5,
    "distribute_p50": 18.0,
    "distribute_p95": 55.0,
    "pipeline_p50": 125.0,
    "pipeline_p95": 340.0,
    "total_wps": 45,
    "failed_wps": 2
  }
]
```

**curl:**

```sh
curl "http://localhost:8080/api/grafana/bottlenecks-timeseries?start=${__from:date:iso}&end=${__to:date:iso}&interval=1m&core=5"
```

---

### 1.18 GET /api/grafana/event-types

Static metadata for all 115 telemetry event types. No database query — instantly cacheable.

**Query:** `EventTypesParams`

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `group` | string | no | Filter to a single event group name |

**Response:**

```json
[
  { "id": 0, "name": "Dropped", "group": "system" },
  { "id": 10, "name": "Status", "group": "status" },
  { "id": 42, "name": "Authored", "group": "blocks" },
  { "id": 92, "name": "WorkPackageFailed", "group": "wp_pipeline" }
]
```

**Event Type Groups**

| Group | Event IDs | Description |
|-------|-----------|-------------|
| `system` | 0 | Dropped |
| `status` | 10–13 | Status, BestBlock, Finalized, SyncStatus |
| `connections` | 20–28 | Connection lifecycle + PeerMisbehaved |
| `blocks` | 40–47 | Authoring, importing, verification, execution |
| `block_distribution` | 60–68 | Announcements, requests, transfers |
| `tickets` | 80–84 | Ticket generation and transfer |
| `wp_pipeline` | 90–109 | WP submission through guarantee distribution |
| `guarantee_receiving` | 110–113 | Incoming guarantees + discards |
| `shards` | 120–125 | Shard requests for availability |
| `assurances` | 126–131 | Assurance distribution |
| `bundles` | 140–153 | Bundle shard/full requests for auditing |
| `segments` | 160–178 | Segment shard requests, reconstruction, verification |
| `preimages` | 190–199 | Preimage announcements, requests, transfers |
| `failures` | (virtual) | Union of all Failed/Discarded/Duplicate events across groups |

**Using group names in `event_types` parameters**

The `event_types` parameter (on `/timeseries`, `/events`, and elsewhere) accepts a mix of numeric IDs, group names, and event names:

```
event_types=failures                    # All failure events
event_types=42,failures,10              # Specific IDs plus a group
event_types=blocks,wp_pipeline          # Multiple groups
event_types={wp_pipeline,connections}   # Grafana multi-select syntax
event_types=Authored                    # Event name lookup
```

Group names are expanded server-side via `expand_event_types()` into their constituent IDs, deduplicated and sorted.

---

### 1.19 GET /api/grafana/events

Raw event data matching criteria. Returns the most recent events first.

**Query:** `EventsQuery`

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `start` | ISO 8601 datetime | yes | - | Start of time range |
| `end` | ISO 8601 datetime | yes | - | End of time range |
| `event_types` | string | **yes** | - | Comma-separated: numeric IDs, group names, or event names |
| `limit` | i64 | no | 500 | Max rows to return |

**Response:** Array of raw event objects matching the criteria.

```bash
# Get recent WP failure events
curl 'http://localhost:8080/api/grafana/events?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z&event_types=92,99&limit=100'
```

### 1.20 GET /api/grafana/guarantee-discards

Time-bucketed guarantee discard counts grouped by discard reason. Queries the pre-aggregated `guarantee_receiving_counts` table for GuaranteeDiscarded events (type 113).

**Query:** `GuaranteeDiscardsQuery`

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `start` | ISO 8601 datetime | yes | - | Start of time range |
| `end` | ISO 8601 datetime | yes | - | End of time range |
| `interval` | string | no | 30s | Bucket width (same values as /timeseries) |

**Response:** Array of `{ ts, reason, count }` objects. Reasons are enum variants: `PackageReportedOnChain(0)`, `ReplacedByBetter(1)`, `CannotReportOnChain(2)`, `TooManyGuarantees(3)`, `Other(4)`.

```bash
# Get guarantee discards by reason over last hour
curl 'http://localhost:8080/api/grafana/guarantee-discards?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z&interval=1m'
```

---

## 2. Shared Query Types

All query parameter structs are defined in `src/grafana.rs`.

### TimeseriesQuery
Used by: `/timeseries`

```rust
struct TimeseriesQuery {
    start: DateTime<Utc>,      // required
    end: DateTime<Utc>,        // required
    interval: Option<String>,  // default "1m"
    group_by: Option<String>,  // default "event_type"
    node: Option<String>,
    event_types: Option<String>,
    core: Option<i16>,
}
```

### TimeRangeQuery
Used by: `/stats`, `/cores`, `/cores/:core_id`, `/blocks/convergence`, `/blocks/contents`, `/node-stats`, `/node-stats-aggregate`, `/bottlenecks`, `/wp-funnel`

```rust
struct TimeRangeQuery {
    start: DateTime<Utc>,       // required
    end: DateTime<Utc>,         // required
    node: Option<String>,
    core: Option<i16>,
    event_type: Option<i16>,
}
```

### ServiceQuery
Used by: `/services`

```rust
struct ServiceQuery {
    start: DateTime<Utc>,      // required
    end: DateTime<Utc>,        // required
    service: Option<String>,   // comma-sep, decimal or 0x hex, Grafana braces
}
```

### ServiceTimeseriesQuery
Used by: `/services/timeseries`

```rust
struct ServiceTimeseriesQuery {
    start: DateTime<Utc>,      // required
    end: DateTime<Utc>,        // required
    interval: Option<String>,  // default "1m"
    service: Option<String>,   // comma-sep, decimal or 0x hex, Grafana braces
}
```

### GuaranteeConvergenceDetailQuery
Used by: `/guarantee-convergence/detail`

```rust
struct GuaranteeConvergenceDetailQuery {
    start: DateTime<Utc>,      // required
    end: DateTime<Utc>,        // required
    core: Option<i16>,
    wp_hash: Option<String>,   // hex-encoded work package hash
}
```

### WpTimeseriesQuery
Used by: `/wp-funnel-timeseries`, `/bottlenecks-timeseries`

```rust
struct WpTimeseriesQuery {
    start: DateTime<Utc>,      // required
    end: DateTime<Utc>,        // required
    interval: Option<String>,  // default "1m"
    core: Option<i16>,
}
```

### EventsQuery
Used by: `/events`

```rust
struct EventsQuery {
    start: DateTime<Utc>,      // required
    end: DateTime<Utc>,        // required
    event_types: String,       // required, comma-sep IDs/groups/names
    limit: Option<i64>,        // default 500
}
```

### Special parsing

- **`parse_service_ids()`**: Strips Grafana `{a,b}` braces, accepts decimal or `0x` hex, parses as u32 then casts to i32
- **`parse_node_list()`**: Strips Grafana `{a,b}` braces, splits on commas
- **`expand_event_types()`**: Accepts numeric IDs, group names, or event names; returns deduplicated sorted `Vec<i16>`

---

## 3. Dashboard Inventory

All dashboards use the **Infinity** data source plugin (uid: `jamtart-api`, JSON mode). Dashboard JSON files live in `grafana/provisioning/dashboards/`.

### 3.1 TART Global (`tart-global.json`)

**UID:** `tart-global`
**Description:** Network-wide overview — live stats, events, blocks, health, and aggregates

**Variables:**
- `event_group` (custom): status, connections, blocks, block_distribution, tickets, wp_pipeline, guarantee_receiving, shards, assurances, bundles, segments, preimages, system — multi-select, include all
- `event_type` (query): populated from event type metadata

**Panels:**

| Panel | Type | Endpoint | Key Params |
|-------|------|----------|------------|
| Connected Nodes | stat | `/api/grafana/stats` | start, end |
| Nodes Active | stat | `/api/grafana/nodes` | — |
| Live Events | timeseries | `/api/grafana/timeseries` | event_types=${event_group}, group_by=event_type, interval=1m |
| Event Type Details | stat | `/api/grafana/timeseries` | event_types=${event_type}, group_by=event_type, interval=1m |
| Failures Rate | stat | `/api/grafana/bottlenecks` | — |
| WP Guarantee Rate | stat | `/api/grafana/timeseries` | event_types=guarantee_receiving |
| Block Rate | timeseries | `/api/grafana/timeseries` | event_types=42 |
| WP Pipeline Rate | timeseries | `/api/grafana/timeseries` | event_types=wp_pipeline |
| WP Funnel | bargauge | `/api/grafana/wp-funnel` | — |

---

### 3.2 TART Blocks (`tart-grafana-blocks.json`)

**UID:** `tart-grafana-blocks`
**Description:** Block production analytics — contents, convergence, rate, and pipeline

**Variables:** None

**Panels:**

| Panel | Type | Endpoint | Key Params |
|-------|------|----------|------------|
| Latest / Finalized Slot | stat | `/api/grafana/stats` | — |
| Block Rate | timeseries | `/api/grafana/timeseries` | event_types=42 |
| Block Contents | timeseries | `/api/grafana/blocks/contents` | — |
| Best Block Propagation | timeseries | `/api/grafana/blocks/convergence` | event_type=11 |
| Importing Convergence | timeseries | `/api/grafana/blocks/convergence` | event_type=43 |
| Finalization Convergence | timeseries | `/api/grafana/blocks/convergence` | event_type=12 |
| Block Pipeline | timeseries | `/api/grafana/timeseries` | group_by=event_type, event_types=blocks |

---

### 3.3 TART Cores (`tart-cores.json`)

**UID:** `tart-grafana-cores`
**Description:** Core analysis — status, utilization, validators, and bottlenecks

**Variables:**
- `core_index` (query): populated from `/api/grafana/cores` → `core` field

**Panels:**

| Panel | Type | Endpoint | Key Params |
|-------|------|----------|------------|
| Guarantees by Core | timeseries | `/api/grafana/timeseries` | group_by=core |
| Failures by Type | timeseries | `/api/grafana/timeseries` | group_by=event_type, event_types=failures |
| Core Status Grid | table | `/api/grafana/cores` | — |
| Pipeline Stats | stat | `/api/grafana/bottlenecks` | core=${core_index} |
| Core Work Packages | table | `/api/grafana/cores/${core_index}` | root_selector=recent_work_packages |
| Stage Timing (ms) | stat | `/api/grafana/bottlenecks` | core=${core_index} |
| Core Failures by Type | timeseries | `/api/grafana/timeseries` | group_by=event_type, event_types=failures, core=${core_index} |
| Core Events by Type | timeseries | `/api/grafana/timeseries` | group_by=event_type, core=${core_index} |
| WP Pipeline Funnel | bargauge | `/api/grafana/wp-funnel` | — |

---

### 3.4 TART Services (`tart-grafana-services.json`)

**UID:** `tart-grafana-services`
**Description:** Service-level analytics — work packages, gas usage, and activity per service

**Variables:**
- `service` (query): populated from `/api/grafana/services` → `service_id` field — multi-select, include all

**Panels:**

| Panel | Type | Endpoint | Key Params |
|-------|------|----------|------------|
| Active Services | stat | `/api/grafana/services` | service=${service} |
| Service List | table | `/api/grafana/services` | service=${service} |
| Work Packages per Service | barchart | `/api/grafana/services` | service=${service} |
| Gas Usage by Service | barchart | `/api/grafana/services` | service=${service} |
| Work Packages over Time | timeseries | `/api/grafana/services/timeseries` | interval=1m, service=${service} |
| Gas Usage over Time | timeseries | `/api/grafana/services/timeseries` | interval=1m, service=${service} |
| Work Package Failure Reasons | table | `/api/grafana/events` | event_types=92,99, limit=500 |
| Failure Reason Distribution | bargauge | `/api/grafana/events` | event_types=92,99, limit=2000 (UQL parsing) |

---

### 3.5 TART Node (`tart-node.json`)

**UID:** `tart-node`
**Description:** Per-node deep dive — status, events, timeline, and health

**Variables:**
- `node_id` (query): populated from `/api/grafana/nodes` → `node_id` field

**Panels:**

| Panel | Type | Endpoint | Key Params |
|-------|------|----------|------------|
| Node Status | stat | `/api/grafana/nodes` | — |
| Node Stats | stat/timeseries | `/api/grafana/node-stats` | node=${node_id} |
| Events by Type | timeseries | `/api/grafana/timeseries` | group_by=event_type |
| All Nodes Events | timeseries | `/api/grafana/timeseries` | group_by=node_id |
| Blocks | timeseries | `/api/grafana/timeseries` | node=${node_id}, event_types=blocks |
| Block Distribution | timeseries | `/api/grafana/timeseries` | node=${node_id}, event_types=block_distribution |
| Connections | timeseries | `/api/grafana/timeseries` | node=${node_id}, event_types=connections |
| Shards | timeseries | `/api/grafana/timeseries` | node=${node_id}, event_types=shards |
| Tickets | timeseries | `/api/grafana/timeseries` | node=${node_id}, event_types=tickets |
| Tickets by Type | timeseries | `/api/grafana/timeseries` | node=${node_id}, group_by=event_type, event_types=tickets |
| Guarantee Receiving | timeseries | `/api/grafana/timeseries` | node=${node_id}, event_types=guarantee_receiving |
| Work Package Pipeline | timeseries | `/api/grafana/timeseries` | node=${node_id}, event_types=wp_pipeline |
| Failures | timeseries | `/api/grafana/timeseries` | node=${node_id}, event_types=failures |

---

### 3.6 TART Data Availability (`tart-grafana-da.json`)

**UID:** `tart-grafana-da`
**Description:** Data availability layer — storage, peer health, and preimage statistics per node

**Variables:** None

**Panels:**

| Panel | Type | Endpoint | Key Params |
|-------|------|----------|------------|
| Total DA Storage (Avg) | stat | `/api/grafana/node-stats-aggregate` | — |
| DA Storage per Node | timeseries | `/api/grafana/node-stats` | — |
| Peer Counts per Node | timeseries | `/api/grafana/node-stats` | — |
| Network Peer Health | timeseries | `/api/grafana/node-stats-aggregate` | — |
| Preimage Stats | timeseries | `/api/grafana/node-stats` | — |

Uses `partitionByValues` transformation to split series by Node.

---

### 3.7 TART Connectivity Check (`tart-connectivity-check.json`)

**UID:** `tart-api-test`
**Description:** Minimal dashboard to verify Grafana can reach the JamTart API

**Variables:** None

**Panels:**

| Panel | Type | Endpoint | Key Params |
|-------|------|----------|------------|
| Active Nodes | stat | `/api/grafana/stats` | — |
| Nodes | table | `/api/grafana/nodes` | — |
| Stats Summary | stat | `/api/grafana/stats` | — |
| Failure Rate | stat | `/api/grafana/bottlenecks` | — |
| Dropped Events | timeseries | `/api/grafana/timeseries` | event_types=system, group_by=event_type |

---

### Endpoint Usage Matrix

Which endpoints are used by which dashboards:

| Endpoint | Global | Blocks | Cores | Services | Node | DA | Connectivity |
|----------|:------:|:------:|:-----:|:--------:|:----:|:--:|:------------:|
| `/timeseries` | x | x | x | | x | | x |
| `/stats` | x | x | | | | | x |
| `/nodes` | x | | | | x | | x |
| `/cores` | | | x | | | | |
| `/blocks/convergence` | | x | | | | | |
| `/blocks/contents` | | x | | | | | |
| `/services` | | | | x | | | |
| `/services/timeseries` | | | | x | | | |
| `/events` | | | | x | | | |
| `/node-stats` | | | | | x | x | |
| `/node-stats-aggregate` | | | | | | x | |
| `/bottlenecks` | x | | x | | | | x |
| `/wp-funnel` | x | | x | | | | |
| `/event-types` | | | | | | | |
| `/db-stats` | | | | | | | |

---

## 4. Grafana Variable Templates

Variables as implemented in the actual dashboards:

### $event_group (Global dashboard)
- **Type:** Custom
- **Values:** status, connections, blocks, block_distribution, tickets, wp_pipeline, guarantee_receiving, shards, assurances, bundles, segments, preimages, system
- **Multi-value:** yes
- **Include All:** yes

### $node_id (Node dashboard)
- **Type:** Query
- **Data source:** Infinity
- **Query URL:** `/api/grafana/nodes`
- **Selector:** `node_id`

### $core_index (Cores dashboard)
- **Type:** Query
- **Data source:** Infinity
- **Query URL:** `/api/grafana/cores?start=${__from:date:iso}&end=${__to:date:iso}`
- **Selector:** `core`

### $service (Services dashboard)
- **Type:** Query
- **Data source:** Infinity
- **Query URL:** `/api/grafana/services?start=${__from:date:iso}&end=${__to:date:iso}`
- **Selector:** `service_id`
- **Multi-value:** yes
- **Include All:** yes

---

## 5. Time Range Handling

### Passing the Grafana time range to API endpoints

All endpoints expecting `start` and `end` accept ISO 8601 timestamps. Use Grafana's built-in macros:

```
start=${__from:date:iso}&end=${__to:date:iso}
```

### Aggregate table selection (automatic)

The `/timeseries` endpoint auto-selects the underlying aggregate table:

| Your interval | Aggregate table queried |
|---------------|------------------------|
| `6s` through `30s` | `event_stats_30s` |
| `1m` through `30m` | `event_stats_1m` |
| `1h` through `1d` | `event_stats_1h` |
| Any (with `group_by=core`) | `core_stats_1m` |

### Recommended intervals by range

| Dashboard time range | Suggested interval |
|---------------------|--------------------|
| Last 5 minutes | `10s` |
| Last 15 minutes | `15s` or `30s` |
| Last 1 hour | `1m` |
| Last 6 hours | `5m` |
| Last 24 hours | `15m` or `30m` |
| Last 7 days | `1h` or `2h` |
| Last 30 days | `6h` or `12h` |
| Last 90+ days | `1d` |

---

## 6. Aggregate Retention Policy

| Aggregate table | Granularity | Retention |
|----------------|-------------|-----------|
| `event_stats_30s` | 30 seconds | 3 days |
| `event_stats_1m` | 1 minute | 30 days |
| `event_stats_1h` | 1 hour | 365 days |

**Raw tables:**

- `node_stats` (2s granularity): retention depends on TimescaleDB configuration
- `node_stats_1m`: 1-minute aggregate of node stats, longer retention
- `core_stats_1m`: 1-minute core aggregate, same retention as `event_stats_1m`
- `service_stats_1m`: 1-minute service aggregate, same retention as `event_stats_1m`

Use `/api/grafana/db-stats` to inspect current table sizes, row counts, and compression ratios.

---

## 7. Docs-in-Code (OpenAPI)

All Grafana endpoints are annotated with [`utoipa`](https://github.com/juhaku/utoipa) — the OpenAPI spec is auto-generated from code and served at:

```
GET /api/docs/openapi.json
```

This spec includes all 16 handler paths, typed request parameters (`IntoParams`), and typed response schemas (`ToSchema`). Response structs live in `src/grafana_types.rs`; each struct documents its data source pipeline (which aggregate tables, hypertables, or enricher-populated tables provide the data).

**Using the spec:**
- Paste the JSON URL into [Swagger Editor](https://editor.swagger.io) or [Redocly](https://redocly.github.io/redoc/) for interactive docs
- Import into Postman, Insomnia, or other API clients
- Use `curl http://localhost:8080/api/docs/openapi.json | jq` to inspect locally

This guide remains a human-friendly companion, but the OpenAPI spec is the authoritative source for endpoint signatures and response schemas.
