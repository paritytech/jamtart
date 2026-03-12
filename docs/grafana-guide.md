# Grafana Integration Guide

This guide covers the JamTart Grafana HTTP API. All endpoints return JSON and are designed for use with the Grafana Infinity data source plugin (JSON mode). The base path is `/api/grafana`.

---

## Table of Contents

- [1. Endpoint Catalog](#1-endpoint-catalog)
- [2. Dashboard Recipes](#2-dashboard-recipes)
- [3. Grafana Variable Templates](#3-grafana-variable-templates)
- [4. Time Range Handling](#4-time-range-handling)
- [5. Aggregate Retention Policy](#5-aggregate-retention-policy)

---

## 1. Endpoint Catalog

### 1.1 GET /api/grafana/timeseries

Time-series event counts with automatic aggregate table selection.

**Query Parameters**

| Param | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `start` | ISO 8601 datetime | yes | - | Start of time range |
| `end` | ISO 8601 datetime | yes | - | End of time range |
| `interval` | string | no | `1m` | Bucket width. Allowed: `10s`, `15s`, `30s`, `1m`, `2m`, `5m`, `10m`, `15m`, `30m`, `1h`, `2h`, `4h`, `6h`, `12h`, `1d` |
| `group_by` | string | no | `event_type` | Grouping column. Allowed: `node_id`, `event_type`, `core` |
| `node` | string | no | - | Filter to a single node_id |
| `event_types` | string | no | - | Comma-separated list of event type codes (i16) |

**Aggregate Table Auto-Selection**

| Condition | Table Used |
|-----------|-----------|
| `group_by=core` | `core_stats_1m` |
| interval < 60s | `event_stats_30s` |
| interval < 3600s | `event_stats_1m` |
| interval >= 3600s | `event_stats_1h` |

**Response Schema**

```json
[
  {
    "ts": "2025-01-15T12:00:00Z",
    "count": 42,
    "event_type": 105
  }
]
```

The group column in the response matches `group_by`: `event_type` (i16), `node_id` (string), or `core` (i16).

**Example**

```bash
curl 'http://localhost:3000/api/grafana/timeseries?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z&interval=5m&group_by=event_type&event_types=105,92'
```

---

### 1.2 GET /api/grafana/stats

Dashboard summary counters for the given time range.

**Query Parameters**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |

**Response Schema**

```json
{
  "connected_nodes": 12,
  "slot_events": 1800,
  "guarantees": 540,
  "failures": 3,
  "wp_events": 480
}
```

**Example**

```bash
curl 'http://localhost:3000/api/grafana/stats?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'
```

---

### 1.3 GET /api/grafana/cores

Per-core work package, guarantee, and failure counts. When filtering by a single core, includes the 100 most recent work packages from `wp_tracking`.

**Query Parameters**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `core` | i16 | no | Filter to a single core index |

**Response Schema (summary mode, no core filter)**

```json
[
  {
    "core": 0,
    "work_packages": 120,
    "guarantees": 115,
    "failures": 2
  }
]
```

**Response Schema (detail mode, with core filter)**

```json
[
  {
    "core": 5,
    "work_packages": 120,
    "guarantees": 115,
    "failures": 2,
    "recent_work_packages": [
      {
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
      }
    ]
  }
]
```

**Example**

```bash
# All cores summary
curl 'http://localhost:3000/api/grafana/cores?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'

# Single core detail
curl 'http://localhost:3000/api/grafana/cores?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z&core=5'
```

---

### 1.4 GET /api/grafana/blocks/convergence

Block propagation convergence percentiles per slot.

**Query Parameters**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |

**Response Schema**

```json
[
  {
    "slot": 12345,
    "event_type": 42,
    "node_count": 12,
    "p50_ms": 150,
    "p99_ms": 480,
    "p100_ms": 520,
    "authored_at": "2025-01-15T12:00:00Z"
  }
]
```

**Example**

```bash
curl 'http://localhost:3000/api/grafana/blocks/convergence?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'
```

---

### 1.5 GET /api/grafana/blocks/contents

Block contents extracted from BlockAuthored events (event_type=42).

**Query Parameters**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |

**Response Schema**

```json
[
  {
    "slot": 12345,
    "timestamp": "2025-01-15T12:00:00Z",
    "node_id": "node-01",
    "num_guarantees": 3,
    "num_assurances": 10,
    "num_preimages": 1,
    "num_tickets": 0,
    "num_disputes": 0,
    "extrinsic_size": 4096
  }
]
```

**Example**

```bash
curl 'http://localhost:3000/api/grafana/blocks/contents?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'
```

---

### 1.6 GET /api/grafana/services

Per-service activity and gas usage from the `service_stats_1m` aggregate.

**Query Parameters**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |

**Response Schema**

```json
[
  {
    "service_id": 1,
    "work_packages": 50,
    "refinements": 48,
    "refinement_gas": 120000000,
    "authorizations": 50,
    "authorization_gas": 5000000,
    "executions": 45,
    "execution_gas": 80000000
  }
]
```

**Example**

```bash
curl 'http://localhost:3000/api/grafana/services?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'
```

---

### 1.7 GET /api/grafana/nodes

All known nodes with metadata. No time range required.

**Query Parameters**

None required.

**Response Schema**

```json
[
  {
    "node_id": "node-01",
    "peer_id": "12D3KooW...",
    "implementation_name": "jamtart",
    "implementation_version": "0.1.0",
    "node_info": { },
    "connected_at": "2025-01-15T00:00:00Z",
    "disconnected_at": null,
    "last_seen_at": "2025-01-15T12:30:00Z",
    "is_connected": true,
    "total_event_count": 150000,
    "address": "10.0.0.1:9000"
  }
]
```

Results are sorted by `is_connected DESC, last_seen_at DESC` (connected nodes first).

**Example**

```bash
curl 'http://localhost:3000/api/grafana/nodes'
```

---

### 1.8 GET /api/grafana/node-stats

Raw node status rows at ~2s granularity from the `node_stats` hypertable.

**Query Parameters**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `node` | string | no | Comma-separated node_ids to filter |

**Response Schema**

```json
[
  {
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
  }
]
```

**Example**

```bash
# All nodes
curl 'http://localhost:3000/api/grafana/node-stats?start=2025-01-15T12:00:00Z&end=2025-01-15T12:05:00Z'

# Specific nodes
curl 'http://localhost:3000/api/grafana/node-stats?start=2025-01-15T12:00:00Z&end=2025-01-15T12:05:00Z&node=node-01,node-02'
```

---

### 1.9 GET /api/grafana/node-stats-aggregate

1-minute aggregated node stats from `node_stats_1m`. Without a node filter, returns network-wide aggregates per bucket. With a node filter, returns per-node rows.

**Query Parameters**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `node` | string | no | Comma-separated node_ids to filter |

**Response Schema (network-wide, no node filter)**

```json
[
  {
    "bucket": "2025-01-15T12:00:00Z",
    "avg_peers": 24,
    "min_peers": 18,
    "max_peers": 30,
    "avg_val_peers": 12,
    "min_val_peers": 10,
    "max_val_peers": 14,
    "avg_sync_peers": 8,
    "min_sync_peers": 5,
    "max_sync_peers": 12,
    "avg_shards": 342,
    "min_shards": 300,
    "max_shards": 400,
    "avg_shards_size": 1073741824,
    "max_shards_size": 2147483648,
    "avg_preimages": 15,
    "max_preimages": 25,
    "avg_preimages_size": 524288,
    "max_preimages_size": 1048576,
    "avg_guarantees": 1.5,
    "min_guarantees": 0,
    "max_guarantees": 3,
    "max_zero_guarantee_cores": 2,
    "status_count": 360
  }
]
```

With a node filter, each row additionally includes `"node_id"`.

**Example**

```bash
# Network-wide
curl 'http://localhost:3000/api/grafana/node-stats-aggregate?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'

# Per-node
curl 'http://localhost:3000/api/grafana/node-stats-aggregate?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z&node=node-01,node-02'
```

---

### 1.10 GET /api/grafana/db-stats

TimescaleDB internal metadata. No parameters required.

**Response Schema**

```json
{
  "tables": [
    {
      "table_name": "events",
      "total_bytes": 1073741824,
      "table_bytes": 805306368,
      "index_bytes": 214748364,
      "toast_bytes": 53687091
    }
  ],
  "row_counts": [
    { "table_name": "events", "row_count": 15000000 },
    { "table_name": "node_stats", "row_count": 5000000 },
    { "table_name": "event_services", "row_count": 8000000 },
    { "table_name": "wp_tracking", "row_count": 50000 },
    { "table_name": "slot_convergence", "row_count": 200000 },
    { "table_name": "nodes", "row_count": 20 }
  ],
  "compression": [
    {
      "table_name": "events",
      "compressed_chunks": 12,
      "before_compression_bytes": 2147483648,
      "after_compression_bytes": 536870912
    }
  ]
}
```

**Example**

```bash
curl 'http://localhost:3000/api/grafana/db-stats'
```

---

### 1.11 GET /api/grafana/bottlenecks

Work package pipeline bottleneck analysis from `wp_tracking`. Returns percentile timings for each pipeline stage and overall failure rate.

**Query Parameters**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |
| `core` | i16 | no | Filter to a single core index |

**Pipeline Stages** (each with `p50_ms` and `p95_ms`)

| Stage | Measures |
|-------|----------|
| `authorize` | `received_at` to `authorized_at` |
| `refine` | `authorized_at` to `refined_at` |
| `report` | `refined_at` to `report_built_at` |
| `guarantee` | `report_built_at` to `guarantee_built_at` |
| `distribute` | `guarantee_built_at` to `distributed_at` |
| `pipeline_total` | `received_at` to `distributed_at` (or `last_updated`) |

**Response Schema**

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

**Example**

```bash
curl 'http://localhost:3000/api/grafana/bottlenecks?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'

# Single core
curl 'http://localhost:3000/api/grafana/bottlenecks?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z&core=5'
```

---

### 1.12 GET /api/grafana/wp-funnel

Work package pipeline funnel — counts how many WPs reached each stage within the time range.

**Query Parameters**

| Param | Type | Required | Description |
|-------|------|----------|-------------|
| `start` | ISO 8601 datetime | yes | Start of time range |
| `end` | ISO 8601 datetime | yes | End of time range |

**Response Schema**

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

Each field is the count of work packages that reached at least that pipeline stage. `failed` counts WPs that hit a failure at any stage.

**Example**

```bash
curl 'http://localhost:3000/api/grafana/wp-funnel?start=2025-01-15T00:00:00Z&end=2025-01-15T01:00:00Z'
```

---

### 1.13 GET /api/grafana/event-types

Static metadata for all 99 telemetry event types. No parameters required, no database query — instantly cacheable.

**Response Schema**

```json
[
  { "id": 0, "name": "Dropped", "group": "system" },
  { "id": 10, "name": "Status", "group": "status" },
  { "id": 42, "name": "Authored", "group": "blocks" },
  { "id": 92, "name": "WorkPackageFailed", "group": "wp_pipeline" },
  ...
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
| `failures` | (virtual) | Union of all Failed/Discarded/Duplicate events (27 types) |

**Using group names in `event_types` parameter**

The `/api/grafana/timeseries` endpoint's `event_types` parameter accepts a mix of numeric IDs and group names:

```
# All failure events
event_types=failures

# Specific IDs plus a group
event_types=42,failures,10

# Multiple groups
event_types=blocks,wp_pipeline
```

Group names are expanded server-side into their constituent event type IDs, deduplicated and sorted.

**Example**

```bash
curl 'http://localhost:3000/api/grafana/event-types'
```

---

## 2. Dashboard Recipes

All recipes assume the **Infinity** data source (type: JSON, method: GET). Set the base URL to your JamTart instance (e.g., `http://jamtart:3000`).

### 2.1 Global Overview Dashboard

A top-level view of network health.

**Row 1 -- Stat panels** (source: `/api/grafana/stats`)

| Panel | Field | Unit |
|-------|-------|------|
| Connected Nodes | `connected_nodes` | none |
| Current Slot | `slot_events` | none |
| Guarantees | `guarantees` | short |
| Failures | `failures` | short |
| WP Events | `wp_events` | short |

Infinity config: URL = `/api/grafana/stats?start=${__from:date:iso}&end=${__to:date:iso}`, Parsing = JSON, Type = JSON, Source = URL.

**Row 2 -- Event timeseries** (source: `/api/grafana/timeseries`)

Create a Time series panel. URL:

```
/api/grafana/timeseries?start=${__from:date:iso}&end=${__to:date:iso}&interval=$interval&group_by=event_type
```

- X axis: `ts`
- Y axis: `count`
- Series: split by `event_type`

**Row 3 -- Node table** (source: `/api/grafana/nodes`)

Table panel showing all nodes. URL: `/api/grafana/nodes`. Display columns: `node_id`, `implementation_name`, `is_connected`, `total_event_count`, `last_seen_at`, `address`.

---

### 2.2 Cores Dashboard

**Row 1 -- Core grid** (source: `/api/grafana/cores`)

Table panel or heatmap. URL:

```
/api/grafana/cores?start=${__from:date:iso}&end=${__to:date:iso}
```

Show columns: `core`, `work_packages`, `guarantees`, `failures`. Add a color threshold on `failures` (green=0, red>0).

**Row 2 -- Core timeseries** (source: `/api/grafana/timeseries`)

Time series panel. URL:

```
/api/grafana/timeseries?start=${__from:date:iso}&end=${__to:date:iso}&interval=$interval&group_by=core&event_types=94
```

Series per core showing WP counts over time.

**Row 3 -- Core detail** (source: `/api/grafana/cores` with core filter)

Table panel for `recent_work_packages` when a core variable is selected. URL:

```
/api/grafana/cores?start=${__from:date:iso}&end=${__to:date:iso}&core=$core
```

Parse the `recent_work_packages` array from the first element. Columns: `wp_hash`, `stage`, `service_ids`, timestamps.

---

### 2.3 Node Dashboard

**Row 1 -- Node info header** (source: `/api/grafana/nodes`)

Stat panels pulling from the node row matching `$node`. Use a Transformation (Filter by value, `node_id == $node`) or make two queries.

**Row 2 -- Node metrics** (source: `/api/grafana/node-stats`)

Time series panels. URL:

```
/api/grafana/node-stats?start=${__from:date:iso}&end=${__to:date:iso}&node=$node
```

Panel ideas:
- Peers: `num_peers`, `num_val_peers`, `num_sync_peers`
- Storage: `shards_size`, `preimages_size`
- Guarantees: `min_guarantees`, `max_guarantees`, `avg_guarantees`

**Row 3 -- Node events** (source: `/api/grafana/timeseries`)

```
/api/grafana/timeseries?start=${__from:date:iso}&end=${__to:date:iso}&interval=$interval&group_by=event_type&node=$node
```

---

### 2.4 Services Dashboard

**Row 1 -- Service table** (source: `/api/grafana/services`)

Table panel. URL:

```
/api/grafana/services?start=${__from:date:iso}&end=${__to:date:iso}
```

Columns: `service_id`, `work_packages`, `refinements`, `authorizations`, `executions`.

**Row 2 -- Gas timeseries**

To show gas over time, use `/api/grafana/timeseries` with event_types `101` (refinement), `95` (authorization), `47` (execution) and `group_by=event_type`. The `/services` endpoint provides totals; for time-bucketed gas data, you would query the underlying timeseries.

---

### 2.5 Blocks Dashboard

**Row 1 -- Block contents table** (source: `/api/grafana/blocks/contents`)

Table panel. URL:

```
/api/grafana/blocks/contents?start=${__from:date:iso}&end=${__to:date:iso}
```

Columns: `slot`, `node_id`, `num_guarantees`, `num_assurances`, `num_preimages`, `num_tickets`, `extrinsic_size`.

**Row 2 -- Convergence timeseries** (source: `/api/grafana/blocks/convergence`)

Time series or bar chart panel. URL:

```
/api/grafana/blocks/convergence?start=${__from:date:iso}&end=${__to:date:iso}
```

- X axis: `authored_at`
- Y axes: `p50_ms`, `p99_ms`, `p100_ms`
- Filter by `event_type` if needed

---

### 2.6 DA (Data Availability) Dashboard

**Row 1 -- Storage over time** (source: `/api/grafana/node-stats-aggregate`)

Time series panel for shard and preimage storage. URL:

```
/api/grafana/node-stats-aggregate?start=${__from:date:iso}&end=${__to:date:iso}
```

Y axes: `avg_shards_size` (bytes), `avg_preimages_size` (bytes). Use Grafana byte unit.

**Row 2 -- Peer counts** (source: `/api/grafana/node-stats-aggregate`)

Same endpoint, Y axes: `avg_peers`, `avg_val_peers`, `avg_sync_peers`.

**Row 3 -- Per-node detail** (source: `/api/grafana/node-stats`)

Raw 2s granularity for a selected node:

```
/api/grafana/node-stats?start=${__from:date:iso}&end=${__to:date:iso}&node=$node
```

---

## 3. Grafana Variable Templates

Define these as dashboard variables (Settings > Variables) to make panels interactive.

### $node

- **Type:** Query
- **Data source:** Infinity
- **Query URL:** `/api/grafana/nodes`
- **Parsing:** JSONata or JSON Path -- extract `node_id` from the array
- **JSONata expression:** `$[].node_id`
- **Multi-value:** enable if needed (node-stats endpoints accept comma-separated)
- **Include All option:** yes

### $core

- **Type:** Custom
- **Values:** `0,1,2,3,4,5,...,340,341` (adjust to your chain's core count)
- **Alternative -- Query:** Use `/api/grafana/cores` and extract distinct `core` values
- **JSONata expression:** `$[].core`

### $service

- **Type:** Query
- **Data source:** Infinity
- **Query URL:** `/api/grafana/services?start=${__from:date:iso}&end=${__to:date:iso}`
- **JSONata expression:** `$[].service_id`

### $interval

- **Type:** Interval
- **Values:** `10s,15s,30s,1m,2m,5m,10m,15m,30m,1h,2h,4h,6h,12h,1d`
- **Auto option:** enable (see [section 4](#4-time-range-handling) for auto-interval rules)

---

## 4. Time Range Handling

### Passing the Grafana time range to API endpoints

All endpoints expecting `start` and `end` accept ISO 8601 timestamps. Use Grafana's built-in macros:

```
start=${__from:date:iso}&end=${__to:date:iso}
```

These resolve to values like `2025-01-15T00:00:00.000Z`.

### Auto-interval rules

When using the `$interval` variable with the Auto option enabled, Grafana picks an interval based on the panel width and time range. The `/timeseries` endpoint accepts any of its 15 whitelisted intervals, so configure the Auto option with:

- **Step count:** 100 (yields ~100 data points per panel width)
- **Min interval:** `10s`

Recommended manual mapping if not using Auto:

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

### Aggregate table selection (automatic)

The `/timeseries` endpoint auto-selects the underlying aggregate table based on the interval you request. You do not need to choose a table manually:

| Your interval | Aggregate table queried |
|---------------|------------------------|
| `10s`, `15s`, `30s` | `event_stats_30s` |
| `1m` through `30m` | `event_stats_1m` |
| `1h` through `1d` | `event_stats_1h` |
| Any (with `group_by=core`) | `core_stats_1m` |

---

## 5. Aggregate Retention Policy

Data is retained at different granularities with automatic rollup. Older fine-grained data is dropped while coarser aggregates persist longer.

| Aggregate table | Granularity | Retention |
|----------------|-------------|-----------|
| `event_stats_30s` | 30 seconds | 3 days |
| `event_stats_1m` | 1 minute | 30 days |
| `event_stats_1h` | 1 hour | 365 days |

**Implications for dashboards:**

- Queries for the last 3 days can use intervals as low as `10s` (reads from `event_stats_30s`).
- Queries for 3-30 days should use intervals of `1m` or higher (the 30s table will have no data).
- Queries beyond 30 days must use `1h` or higher intervals.
- The `/timeseries` endpoint handles this automatically -- if you request `interval=30s` for a range older than 3 days, the 30s table will simply return no rows. Set the `$interval` variable's Auto min to match your expected range.

**Raw tables:**

- `node_stats` (2s granularity): retention depends on your TimescaleDB configuration.
- `node_stats_1m`: 1-minute aggregate of node stats, longer retention.
- `core_stats_1m`: 1-minute core aggregate, same retention as `event_stats_1m`.
- `service_stats_1m`: 1-minute service aggregate, same retention as `event_stats_1m`.

Use `/api/grafana/db-stats` to inspect current table sizes, row counts, and compression ratios at any time.
