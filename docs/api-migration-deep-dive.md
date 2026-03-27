# API Migration Deep Dive: Underdefined Areas

Companion to `api-migration-plan.md`. Covers the 7 areas that were underdefined in the original plan, with concrete findings from codebase analysis.

---

## 1. Bottleneck Validator Profiling

### What the legacy endpoint answers

`GET /api/cores/{id}/bottlenecks` (`store.rs:6062`) answers: **"Which validators are slow or failing when processing work packages for a specific core?"**

It scans raw `ingested_raw_events`, extracts WP submission IDs from WorkPackageReceived (type 94) for the given core, correlates downstream events (94, 95, 101, 102, 105, 109, 92) via `submission_or_share_id`, then uses `LAG()` to compute inter-event timestamps per node. Response:

- `slow_validators[]` — per-node: `avg_processing_ms`, `failure_rate`, `slowdown_factor` (vs network avg), `is_bottleneck` (>1.5x slowdown OR >10% failure rate)
- `bottleneck_messages[]` — human-readable alerts
- `stats` — `total_events`, `total_failures`, `validator_count`, `bottleneck_count`

### What the grafana endpoint answers

`GET /api/grafana/bottlenecks` (`grafana_store.rs:937`) answers: **"What are the pipeline stage latencies for work package processing?"**

Queries `wp_tracking` table, computes `percentile_cont(0.5/0.95)` for each stage (authorize, refine, report, guarantee, distribute, pipeline_total). Response: per-stage p50/p95 timings + overall failure rate. Also has a timeseries variant (`/bottlenecks-timeseries`) bucketed by time.

### These are fundamentally different analyses

| Dimension | Legacy (validator profiling) | Grafana (pipeline profiling) |
|---|---|---|
| Granularity | Per-validator per-core | Per-stage per-core |
| Question | "Who is slow?" | "Which stage is slow?" |
| Data source | Raw events + LAG() | wp_tracking stage timestamps |

### What wp_tracking actually has

Schema (`migrations/013_wp_tracking.sql`):
- Stage timestamps: `received_at`, `authorized_at`, `refined_at`, `report_built_at`, `guarantee_built_at`, `distributed_at`, `failed_at`
- `received_by SMALLINT` — count of distinct nodes that received (NOT which nodes)
- `guaranteed_by SMALLINT` — count of distinct nodes that guaranteed (NOT which nodes)

`received_by`/`guaranteed_by` are **aggregate counters** incremented via HashSet deduplication in `wp_tracker.rs:37-40`. They answer "how many nodes participated" not "which validators were slow."

### What's missing for per-validator analysis

There is no per-node breakdown of stage timings in `wp_tracking`. To know which validator was slow at authorization vs refinement, you'd need to join back to raw events (which have 1h retention).

### Recommendation

**Don't try to replicate the legacy validator profiling.** The grafana pipeline profiling (`/bottlenecks` + `/bottlenecks-timeseries`) is the better analysis — it tells you *which stage* is the bottleneck, which is more actionable than *which validator* is slow.

If per-validator drill-down is needed later:
- **Option A**: Add `received_by_nodes TEXT[]` and `guaranteed_by_nodes TEXT[]` to `wp_tracking` (store which nodes, not just count). ~50 LOC in `wp_tracker.rs` + migration.
- **Option B**: New endpoint that joins `wp_tracking` with raw events (1h retention window). Only useful for recent WPs.

**Action**: Deprecate legacy bottlenecks. The grafana endpoint (already supports `?core=X` filter) is the replacement. UI (`CoreDetailView.tsx`) needs rework to show stage profiling instead of validator profiling.

---

## 2. WP Audit-Progress (MAJOR CORRECTION)

### The legacy endpoint is fundamentally wrong

`GET /api/workpackages/{hash}/audit-progress` (`store.rs:5940`) queries **guarantee events (105-113)** and calls them "audit." But in JAM, audit is a completely separate pipeline that happens AFTER guarantees.

The legacy endpoint answers: "how is guarantee distribution going?" — not "how is the audit going?"

**Additional bugs:**
- Event name mapping is wrong (106="GuaranteeSigned" is actually SendingGuarantee, etc.)
- Tries to extract `tranche` from JSONB but tranche doesn't exist on `GuaranteeSummary`
- "panic_mode" is `failed && events.len() > 5` — meaningless heuristic

### What JAM audit actually is

Audit is a **separate phase** after guarantees are distributed. The full pipeline:

```
WP received → refined → report built → guaranteed → distributed
    ↓
[SEPARATE PIPELINE — Audit]
    ↓
VRF self-selection → announcement → shard recovery → re-execution → judgment → verdict
```

**How it works (from graypaper, `mdbook/book/src/part3/auditing.md`):**

1. **VRF self-selection (T=0)**: Each validator uses VRF to pick 10 random cores to audit. ~30 validators per core in expectation (1023 * 10 / 341).

2. **Announcement**: Validator broadcasts `AuditAnnouncement` (protocol ID 16) — commitment to audit specific cores.

3. **Data recovery**: Auditor fetches erasure-coded shards to reconstruct the original bundle. This is **events 140-153**:
   - Shard path (140-147): Request 342+ shards → Reed-Solomon reconstruction
   - Full bundle path (148-153): Request complete bundle from guarantors (fallback)

4. **Re-execution**: Auditor runs `audit_check()` — re-runs WP in PVM to verify results match the work report.

5. **Judgment**: Validator signs and broadcasts `JudgmentPublication` (protocol ID 17) — Valid or Invalid.

6. **Subsequent tranches**: Every 8 seconds, new tranche. No-shows trigger backup validators via probabilistic selection: `entropy[0]/256 < F * |no_shows| / V` (F=2).

7. **Verdict**: Block author aggregates judgments. Majority negative → guarantors slashed.

**Key constants:**

| Constant | Value |
|---|---|
| Cores audited per validator (T=0) | 10 |
| Expected auditors per core | ~30 |
| Tranche interval | 8 seconds |
| Max tranche offset | 8 (~64 sec) |
| Recovery threshold | 342 shards (V/3) |
| Bias factor F | 2 (expected backups per no-show) |

### What telemetry exists for audit

Events 140-153 are the **audit data recovery** events (currently in tart as `bundle_counts`):

| ID | Name | Key fields | Represents |
|---|---|---|---|
| 140 | SendingBundleShardRequest | `audit_id`, assurer, shard | Auditor requests a shard |
| 141 | ReceivingBundleShardRequest | auditor | Validator gets shard request |
| 142 | BundleShardRequestFailed | request_id, reason | Shard request failed |
| 143 | BundleShardRequestSent | request_id | Shard request sent |
| 144 | BundleShardRequestReceived | request_id, erasure_root, shard | Shard data received |
| 145 | BundleShardTransferred | request_id | Shard transfer complete |
| 146 | ReconstructingBundle | `audit_id`, kind (Trivial/NonTrivial) | Starting reconstruction |
| 147 | BundleReconstructed | `audit_id` | Reconstruction succeeded |
| 148-153 | BundleRequest* (full bundle path) | `audit_id`, guarantor | Fallback: request full bundle |

### What telemetry does NOT exist yet

Polkajam `controller.rs:157` has `// TODO @dave: Telemetry events for auditing`. These audit lifecycle events are **not yet emitted**:

- AuditStarted — validator selected cores (tranche index, core list, work_report_hash)
- AuditAnnouncementSent/Received — announcement broadcast/receipt
- AuditCheckStarted — re-execution began
- AuditCheckCompleted — re-execution result (valid/invalid), elapsed_ns
- JudgmentSent/Received — judgment broadcast/receipt
- NoShowDetected — validator announced but didn't submit judgment
- TrancheEscalation — subsequent tranche triggered for a core

### What tart currently does with audit events

Events 140-153 → `bundle_counts` (pre-aggregated count table). That's pure volume counting. The `audit_id` field is **lost** because count tables only store event_type + node_id + reason + kind.

No enricher correlation for audit events. No dedicated tracker. No grafana endpoints.

### What tart needs: an `audit_tracker.rs`

Similar to `wp_tracker.rs`, `convergence_tracker.rs`, and `da_tracker.rs`:

**Phase 1 — Build audit recovery tracker (with existing events 140-153):**

New `audit_tracker.rs` correlating by `audit_id`:
- Track per-audit recovery timeline: first shard request (140) → reconstruction start (146) → complete (147)
- Record reconstruction kind (Trivial vs NonTrivial)
- Count shards requested/received/failed per audit
- Flush to `audit_recovery_stats` table (per-node aggregates, every 10s like da_tracker)

```sql
CREATE TABLE audit_recovery_stats (
    ts TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    audits_started INT DEFAULT 0,
    audits_completed INT DEFAULT 0,
    audits_failed INT DEFAULT 0,
    trivial_reconstructions INT DEFAULT 0,
    nontrivial_reconstructions INT DEFAULT 0,
    shards_requested INT DEFAULT 0,
    shards_received INT DEFAULT 0,
    shard_failures INT DEFAULT 0,
    full_bundle_requests INT DEFAULT 0,
    avg_recovery_ms REAL,
    recovery_samples INT DEFAULT 0
);
SELECT create_hypertable('audit_recovery_stats', 'ts', chunk_time_interval => '1 hour');
```

Grafana endpoints:
- `GET /api/grafana/audit-recovery` — per-node recovery metrics
- `GET /api/grafana/audit-recovery-timeseries` — recovery latency + Trivial/NonTrivial ratio over time

**Phase 2 — Propose upstream telemetry to polkajam:**

New event types (suggest IDs 200+):
- `AuditStarted(200)`: tranche_index, core, work_report_hash, audit_id
- `AuditCheckCompleted(201)`: audit_id, work_report_hash, judgment_type (Valid/Invalid), elapsed_ns
- `JudgmentSent(202)`: audit_id, work_report_hash, judgment_type, recipient_count
- `JudgmentReceived(203)`: sender, work_report_hash, judgment_type
- `NoShowDetected(204)`: work_report_hash, core, tranche_index, no_show_count

**Phase 3 — Build full audit tracker (once upstream telemetry exists):**

Full per-WP audit lifecycle in `audit_tracking` table:

```sql
CREATE TABLE audit_tracking (
    audit_id BIGINT NOT NULL,
    node_id TEXT NOT NULL,
    work_report_hash BYTEA,       -- from AuditStarted event
    core SMALLINT,                 -- from AuditStarted event
    tranche_index SMALLINT,        -- from AuditStarted event
    first_shard_request_at TIMESTAMPTZ,
    reconstruction_started_at TIMESTAMPTZ,
    reconstruction_kind SMALLINT,  -- 0=Trivial, 1=NonTrivial
    reconstructed_at TIMESTAMPTZ,
    shards_requested INT DEFAULT 0,
    shards_received INT DEFAULT 0,
    shard_failures INT DEFAULT 0,
    audit_check_started_at TIMESTAMPTZ,
    audit_check_completed_at TIMESTAMPTZ,
    judgment_type SMALLINT,        -- 0=Valid, 1=Invalid
    judgment_sent_at TIMESTAMPTZ,
    elapsed_ns BIGINT,
    PRIMARY KEY (audit_id, node_id)
);
```

Grafana endpoints:
- `GET /api/grafana/audit-stats` — per-core auditor count, judgment results, no-show rate
- `GET /api/grafana/audit-convergence` — per-slot/per-core: how fast do auditors complete?
- `GET /api/grafana/audit-timeseries` — audit volume + failure rate over time

**What this enables:**
- Per-core auditor count ("30 validators audited core X this slot")
- Audit latency distribution (VRF selection → judgment)
- Recovery strategy effectiveness (Trivial vs NonTrivial reconstruction ratio)
- No-show rate and tranche escalation frequency
- Judgment distribution (Valid/Invalid ratio per core)
- Cross-phase WP lifecycle: received → refined → guaranteed → audited → judged

### Note on the legacy endpoint

The legacy `/api/workpackages/{hash}/audit-progress` should be **renamed or removed**, not migrated. It tracks guarantee distribution, not audit. If guarantee distribution tracking is still wanted, it belongs in `wp_tracking` columns (guarantee_built_at, distributed_at, guaranteed_by are already there).

**Effort estimate:**
- Phase 1 (audit recovery tracker with existing events): 6-8h
- Phase 2 (upstream telemetry proposal): coordination effort, not code
- Phase 3 (full audit tracker): 8-12h (after upstream events exist)

---

## 3. Execution Metrics

### What the legacy endpoint answers

`GET /api/metrics/execution` (`store.rs:2567`) answers: **"What is the per-phase execution performance (gas + timing) across all nodes?"**

Three phases via JSONB extraction:

| Phase | Event type | JSONB path | Extracted |
|---|---|---|---|
| Authorization | 95 (Authorized) | `data->'Authorized'->'cost'->'total'` | gas_used, elapsed_ns |
| Refinement | 101 (Refined) | `data->'Refined'->'costs'` → `jsonb_array_elements()` | gas_used, elapsed_ns per work item |
| Accumulation | 47 (BlockExecuted) | `data->'BlockExecuted'->'accumulate_costs'` → `jsonb_array_elements()` | gas_used, elapsed_ns per (service_id, cost) pair |

Returns: per-phase `count`, `total_gas`, `avg_gas_per_wp`, `avg_time_ns`. Also `recent_events[]` (last 100). The `by_service` field is **always empty** — comment at line 2627: "no service_id in the costs structure, so skip the by_service breakdown for now."

### What event_services has vs what's missing

`event_services` table (`migrations/007_event_services.sql`): `(timestamp, node_id, event_type, service_id, gas_used)`.

| Data point | event_services (7d) | Raw JSONB (1h) |
|---|---|---|
| gas_used | Yes | Yes |
| elapsed_ns | **NO** | Yes |
| load_ns | **NO** | Yes |
| per-operation breakdown | **NO** | Yes |
| per-service timing | **NO** | Yes (type 47 only — accumulate_costs is service-keyed) |

**The critical gap is timing data.** Only `gas_used` is extracted at ingestion time. `elapsed_ns` and `load_ns` are only in raw JSONB.

### Per-service timing availability by event type

- **Authorized (95)**: Cost is per-WP, not per-service. Service comes from enrichment. No per-service timing possible.
- **Refined (101)**: Costs are per work-item (array). No service_id on the cost struct. Cannot split by service.
- **BlockExecuted (47)**: `accumulate_costs: Vec<(ServiceId, AccumulateCost)>` — **service-keyed natively**. Per-service timing IS available.

### Recommendation

**Extract timing at ingestion** — add columns to `event_services`:

```sql
ALTER TABLE event_services ADD COLUMN elapsed_ns BIGINT;
ALTER TABLE event_services ADD COLUMN load_ns BIGINT;
```

Update `batch_writer.rs` (line ~578-600) to extract from event data:
- Type 95: `cost.total.elapsed_ns`, `cost.load_ns`
- Type 101: `SUM(costs[].total.elapsed_ns)`, `SUM(costs[].load_ns)`
- Type 47: per-service `cost.total.elapsed_ns`, `cost.load_ns` (already iterated per service)

Update `service_stats_1m` continuous aggregate to include `SUM(elapsed_ns)`, `AVG(elapsed_ns)`.

New grafana endpoint: `GET /api/grafana/execution` queries `service_stats_1m` for gas + timing aggregates by phase and service. Per-service breakdown only meaningful for accumulation (type 47).

**Why not raw retention?** 1h retention means you can't show "refine time over the last 7 days." Extracting at ingestion gives 7-day history matching gas retention.

**Effort revision**: 4h (was 3h backend + 1h UI) — need migration + batch_writer change + continuous aggregate update + new endpoint.

---

## 4. DA Enhanced Features

### What the legacy endpoints answer

**`/api/da/stats`** (`store.rs:1410`): **"What is the current DA inventory state?"**
- Per-node: num_shards, shard_size_bytes, num_preimages, preimages_size_bytes
- Aggregate: totals, averages, node_count
- Preimage activity: announced/forgotten/transferred counts (1h window)
- Source: Status events (type 10) JSONB extraction, 1h window

**`/api/da/stats/enhanced`** (`store.rs:4057`): **"What is the DA operational health?"**
- `shard_distribution[]` — per-shard request/receive counts + node coverage
- `availability_rate` — `transfers / requests` ratio
- `node_health[]` — per-node health classification (healthy/degraded/unhealthy) based on transfer rate
- `recent_operations[]` — **always empty** (not implemented)
- Source: O(n^2) self-join on raw events for latency correlation, heavy JSONB extraction

### What grafana already covers

| Feature | Grafana endpoint | Table |
|---|---|---|
| Per-node shard ops | `/grafana/da-stats` | `da_node_stats` |
| Per-node preimage ops | `/grafana/da-stats` | `da_node_stats` |
| Weighted avg latency | `/grafana/da-stats` | `da_node_stats` |
| Latency percentiles (p50/p75/p95/p99) | `/grafana/shard-latency` | `shard_latency_hist` |
| Per-node active shard count | `/grafana/da-stats` | `da_node_stats` |

### Gap analysis per feature

**`availability_rate`** — **Computable from existing data.** `SUM(shards_transferred) / SUM(shard_requests_sent)` from `/grafana/da-stats` aggregate. Can compute server-side (30 LOC in grafana_store.rs) or client-side in Grafana panel. **No new tables needed.**

**`node_health`** — **Computable from existing data.** Derive from `/grafana/da-stats` per-node row:
```
transfer_rate >= 0.9 && activity > 0 → "healthy"
transfer_rate >= 0.5 || activity > 0 → "degraded"
else → "unhealthy"
```
Add as computed field to `/grafana/da-stats` response. **30 LOC, no schema change.**

**`shard_distribution`** — **Missing.** Requires per-shard-index breakdown of requests/receives/nodes. `da_node_stats` has no `shard_index` column. Options:
- **Option A** (recommended): Extend `da_tracker.rs` to track shard_index from SendingShardRequest(120) and ShardRequestReceived(121). New table `da_shard_stats(ts, shard_index, requests_sent, requests_received, nodes_count)`. ~200 LOC + migration.
- **Option B** (fallback): Query raw events with 1h retention for shard_index JSONB extraction. Only useful for recent data.

**`recent_operations`** — **Not implemented in legacy either** (returns `[]`). Defer.

**Inventory data** (num_shards, shard_size_bytes, num_preimages, preimages_size_bytes) — Lives in `node_stats` table, extracted from Status events at ingestion. Already queryable via `/grafana/node-stats`. Just needs a dedicated response that formats it as inventory.

### Recommendation

1. Add `availability_rate` and `node_health` as computed fields to existing `/grafana/da-stats` response (1h)
2. Add `/grafana/da-inventory` that queries `node_stats` for shard/preimage inventory per node (2h)
3. For `shard_distribution`: extend `da_tracker.rs` + new `da_shard_stats` table + new endpoint (4h)
4. Skip `recent_operations` — never implemented

**Effort revision**: 7h (was "4h backend + 2h UI"). The shard_distribution feature is the heavy part.

---

## 5. Parallel Running / Rollback Strategy

### Current architecture supports it already

Both endpoint families run in the **same Axum router** (`api.rs:194`):
```
Router::new()
    .nest("/api/grafana", crate::grafana::router())   // always included
    [if !disable_legacy_endpoints] {
        .route("/api/stats", ...)                       // conditionally included
        ...
    }
```

The `--disable-legacy-endpoints` CLI flag (`main.rs:54`) already exists. Shared `ApiState` is consumed by both families with no conflicts.

### Migration strategy

**Phase A — Run both (already the case):**
No code changes. Both `/api/stats` and `/api/grafana/stats` serve simultaneously.

**Phase B — Verify parity (1-2 weeks):**
For each endpoint pair, verify response equivalence:
- P0 (exact match): `/api/stats` vs `/grafana/stats`, `/api/nodes` vs `/grafana/nodes`, `/api/metrics/timeseries` vs `/grafana/timeseries`
- P1 (within 5%): `/api/events/search` vs `/grafana/events`, `/api/cores/status` vs `/grafana/cores`

Verification approach: integration tests that call both endpoints, normalize responses (unwrap envelopes, rename fields), compare. Add to existing test suite.

**Phase C — Switch UI endpoint-by-endpoint (2-3 weeks):**
Update `lib/api.ts` hooks one at a time. Each PR switches one endpoint family. Prerequisite: Phase 0 from migration plan (ISO timestamps, pagination).

**Phase D — Disable legacy:**
Set `--disable-legacy-endpoints`. Monitor for 404s. Remove dead code.

### Key prerequisite

The UI sends `?duration=1h` (preset string). Grafana endpoints need `?start=ISO&end=ISO`. The `withDuration()` helper in `lib/api.ts` must be updated first. This is Phase 0 work (1h).

### No shim/adapter needed

The migration plan already calls for UI changes (field renames, envelope unwrapping). A compatibility shim would add complexity and hide divergence. Better to surface problems early with direct endpoint switches.

---

## 6. Count Table Timing Gap (Types 90-105)

### The "gap" doesn't exist

The ingestion pipeline in `server.rs` is **synchronous**:

```
1. TCP decode
2. Rate limit check
3. enricher.process() [SYNCHRONOUS — returns EnrichedFields immediately]
4. Slot/WP tracker updates [uses enriched fields]
5. event_counter.record_event() [receives &enriched — dimensions available]
6. WS broadcast
7. Queue to DB batch [includes enriched fields]
```

The counter runs **after** enrichment. There is no async gap. `record_event()` at `event_counter.rs:101` receives `&EnrichedFields` directly.

### But types 90-105 shouldn't use count tables anyway

**Reason 1**: They're not in `PRE_AGGREGATED_TYPES` (only 87 high-volume types are).

**Reason 2**: Types 90-91 (WorkPackageSubmission, WorkPackageBeingShared) and 103 (WorkReportSignatureSent) have **no enrichment path** — no `submission_or_share_id` to look up core/service_ids.

**Reason 3**: Enrichment depends on a prior WorkPackageReceived event populating the `submissions` HashMap. Lookup can fail if:
- Event arrives out-of-order
- Submission context expired (TTL=60s, `enricher.rs:19`)
- Different node received the WP (enricher is per-node)

**Reason 4**: Types 90-105 are **low-volume** WP pipeline events. Count tables are designed for high-volume events (3M+/s). These don't need the same treatment.

### What to use instead

`wp_tracking` is the right data sink for types 90-105. It already:
- Has enriched `core`, `service_ids`, `wp_hash`
- Has per-stage timestamps (received_at through distributed_at)
- Supports `time_bucket()` aggregation

For timeseries queries: grafana endpoints `/bottlenecks-timeseries` and `/wp-funnel-timeseries` query `wp_tracking` with time bucketing.

For event counts: `all_event_stats_*` UNION views already include types 90-105 via `event_stats_30s` (the raw continuous aggregate). No action needed for counts.

### Revised plan for Phase 6

**Do NOT add types 90-105 to count tables.** Instead, the 5 new count tables from the migration plan should cover only the remaining raw-event types that are genuinely high-enough volume AND don't need enrichment:

| New table | Event types | Notes |
|---|---|---|
| `status_counts` | 0, 10-13 | Native fields only (slot from BestBlock/Finalized) |
| `connection_counts` | 20-28 | Native fields only (reason from failures) |
| `block_counts` | 40-47 | Native fields (slot from Authored/Importing) |
| `ticket_low_counts` | 80-82 | Native fields (reason from failures) |

**Types 90-105 stay in `ingested_raw_events`** and are served via `event_stats_30s/1m/1h` continuous aggregates + `wp_tracking` for enriched queries. With 1h raw retention, the continuous aggregates are the persistent store.

This reduces Phase 6 from 5 new tables to 4, and avoids the enrichment-dependency problem entirely.

---

## 7. UNION View Scalability

### Current state: 10 branches per view

`all_event_stats_30s/1m/1h` each UNION ALL 10 sources: `event_stats_*` + 9 count tables. `all_core_stats_1m` has 3 sources. Defined in `migrations/016_count_tables.sql:499-544`.

All count tables have CHECK constraints: `CHECK (event_type BETWEEN X AND Y)` with non-overlapping ranges. This enables PostgreSQL's **constraint exclusion**.

### How queries use these views

`grafana_store.rs:175-194` — queries always include:
- `bucket >= $1 AND bucket < $2` (time range)
- Usually `event_type = ANY($X)` (event type filter)
- Often `node_id = $X` (node filter)

The `event_type` filter is critical — it lets the planner skip UNION branches whose CHECK constraints don't match.

### Expansion to 14 branches: moderate risk

Adding 4 new count tables (status, connection, block, ticket_low) takes each view from 10 to 14 branches. PostgreSQL handles 10-50 UNION branches efficiently with constraint exclusion. 14 is well within safe range.

**The real risk is queries WITHOUT event_type filter.** If a query only filters on time range, all 14 branches are scanned. This happens for aggregate dashboard queries ("total events in last hour").

### Unvalidated assumption

The codebase has **no EXPLAIN ANALYZE output** for these views. `review-plan-05.txt` item [10] flags this: "Verify CHECK constraint exclusion works with EXPLAIN ANALYZE after deploy."

### Recommendation

**Before expanding:**

1. Run EXPLAIN ANALYZE on current 10-branch views with representative queries:
```sql
-- With event_type filter (should prune)
EXPLAIN ANALYZE SELECT time_bucket('1m', bucket) AS ts, SUM(event_count)
FROM all_event_stats_1m
WHERE bucket >= NOW() - '24h'::interval AND bucket < NOW()
  AND event_type = ANY(ARRAY[106, 107, 108])
GROUP BY ts ORDER BY ts;

-- Without event_type filter (full scan)
EXPLAIN ANALYZE SELECT time_bucket('1m', bucket) AS ts, SUM(event_count)
FROM all_event_stats_1m
WHERE bucket >= NOW() - '24h'::interval AND bucket < NOW()
GROUP BY ts ORDER BY ts;
```

2. Verify `SHOW constraint_exclusion;` returns `partition` or `on`.

3. If pruning works: proceed with 4 new tables confidently.

4. If pruning doesn't work: implement **query-time table routing** in `grafana_store.rs` — map `event_types` to specific tables instead of querying the UNION view:
```rust
fn source_tables_for(event_types: &[i16]) -> Vec<&str> {
    // Map event type ranges to their source tables
    // Build targeted UNION ALL only with needed tables
}
```

**For 20+ branches (future):** Consider materialized view staging — group related count tables into intermediate materialized views, reducing the top-level UNION to 5-6 branches.

---

## 8. `/api/workpackages/active` → `/grafana/wp-active`

### What the legacy endpoint does

`GET /api/workpackages/active` (`store.rs:1873`) answers: **"What work packages are currently in-flight?"**

It's a 7-CTE JSONB monster (~200 lines of SQL):
1. Maps `submission_or_share_id` → `wp_hash` via event 94 JSONB
2. Joins all WP pipeline events (92, 94, 95, 101, 102, 105, 109) via share_id
3. Maps `work_report_hash` from event 102 to correlate events 112-113
4. Computes "post-guarantee" stages from GuaranteeDiscarded (113) — `included` (reason=PackageReportedOnChain), `discarded` (other reasons)
5. Falls back to per-node core lookup from raw events
6. Second query: matches erasure_root from shard events (120/124) for "available_at"

Returns `{work_packages:[], summary:{}, reached:{}, failure_breakdown:{}, stage_duration_percentiles:{}}`.

### Why this is massively simpler with `wp_tracking`

`wp_tracking` already has all guarantor pipeline stages (received_at → distributed_at, failed_at) plus core, service_ids, received_by, guaranteed_by. The `/grafana/cores/:core_id` endpoint already returns `WpTrackingRow` from this table.

The "post-guarantee" stages in the legacy endpoint were fabricated:
- **`included`** = GuaranteeDiscarded (113) with reason `PackageReportedOnChain`. This means a node dropped the guarantee from its local pool because it appeared on-chain — it's a **cleanup event**, not a pipeline stage.
- **`discarded`** = GuaranteeDiscarded (113) with other reasons (ReplacedByBetter, TooManyGuarantees, etc.) — also a pool cleanup event.
- **`available_at`** = first shard event (120/124) matching the erasure_root — a DA event, not a WP pipeline stage.

After `distributed`, the WP's fate is on-chain (block inclusion, accumulation). Telemetry doesn't cover that — it's in blocks.

### New endpoint: `/grafana/wp-active`

**Query:**
### Step 1: Endpoint (2h) — no schema changes needed

**WP list query:**
```sql
SELECT wp_hash, core, service_ids, stage,
       received_at, authorized_at, refined_at, report_built_at,
       guarantee_built_at, distributed_at, failed_at,
       received_by, guaranteed_by, first_seen, last_updated
FROM wp_tracking
WHERE distributed_at IS NULL AND failed_at IS NULL
  AND first_seen >= $1 AND first_seen < $2
ORDER BY first_seen DESC
LIMIT 200
```

**Aggregate query** (same WHERE, same table):
```sql
SELECT
    -- summary: per-stage counts
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE stage = 0) AS at_received,
    COUNT(*) FILTER (WHERE stage = 1) AS at_authorized,
    ...
    -- reached: cumulative funnel
    COUNT(*) FILTER (WHERE received_at IS NOT NULL) AS reached_received,
    COUNT(*) FILTER (WHERE authorized_at IS NOT NULL) AS reached_authorized,
    ...
    -- stage_duration_percentiles
    percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (authorized_at - received_at)) * 1000)
        FILTER (WHERE authorized_at IS NOT NULL AND received_at IS NOT NULL) AS authorize_p50_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (authorized_at - received_at)) * 1000)
        FILTER (WHERE authorized_at IS NOT NULL AND received_at IS NOT NULL) AS authorize_p95_ms,
    ...
FROM wp_tracking
WHERE distributed_at IS NULL AND failed_at IS NULL
  AND first_seen >= $1 AND first_seen < $2
```

One table, no joins, no JSONB. Reuses existing `WpTrackingRow` type for the WP list. Response envelope matches legacy shape (work_packages, summary, reached, stage_duration_percentiles).

**UI consumers:** ImportPipelineTab, ExportPipelineTab, WorkPackagesView, DashboardView.

**What's missing without Step 2:** `failure_reason`, `discard_reason`, and `failure_breakdown` are null/empty. WorkPackagesView's FailureAnalysis panel and per-WP failure column won't show data. Everything else works.

### Step 2: Reason columns (2h) — migration + ingestion

**Migration:**
```sql
ALTER TABLE wp_tracking ADD COLUMN failure_reason TEXT;
ALTER TABLE wp_tracking ADD COLUMN discard_reason TEXT;
```

**Ingestion — failure_reason (event 92, WorkPackageFailed):**
- Event 92 carries `reason` field
- Enricher already resolves `submission_or_share_id` → wp context
- In `server.rs`: extract reason, set `wp_tracking.failure_reason`
- ~20 LOC

**Ingestion — discard_reason (event 113, GuaranteeDiscarded):**
- Event 113 carries `outline.work_report_hash` + `reason` (enum: PackageReportedOnChain, ReplacedByBetter, CannotReportOnChain, TooManyGuarantees, Other)
- `guarantee_convergence` table already maps `work_report_hash → wp_hash` (built by convergence tracker)
- In `server.rs`: look up `work_report_hash` in convergence tracker's in-memory state to get `wp_hash`, then update `wp_tracking.discard_reason`
- ~30 LOC

**Enables:** `failure_breakdown` in response envelope (GROUP BY failure_reason) + per-WP `failure_reason`/`discard_reason` fields in WorkPackagesView.

### `/api/cores/status` — no new endpoint needed

The legacy `/api/cores/status` can be replaced by composing 3 existing endpoints client-side:

| Data | Endpoint (exists) |
|---|---|
| Telemetry counts (WPs, guarantees, failures per core) | `/grafana/cores` |
| On-chain activity (gas_used, da_load → active/idle/stale) | `/grafana/onchain/cores` |
| In-flight WP count per core | `/grafana/wp-active` (new, Phase 4) |

UI joins by `core` field. No backend work beyond `/grafana/wp-active`.

---

## 9. Revised Raw Events + Count Table Architecture

### Problem

The current architecture has a split data path:
- 74 high-volume event types → count tables only (skip `ingested_raw_events`)
- 41 lower-volume types → `ingested_raw_events` → continuous aggregates (`event_stats_30s → 1m → 1h`)

This creates two problems:
1. **`/grafana/events` rejects pre-aggregated types** (lines 1122-1134 in `grafana_store.rs`). The UI event browser can't search 74 of 115 event types (shards, guarantees, bundles, segments, preimages, etc.).
2. **Dual aggregation paths** — count tables and continuous aggregates serve the same purpose but with different schemas, creating complexity in UNION views.

### Revised architecture

**All 115 event types get dual-written:**

1. **Count tables (long-term aggregation)** — expand to cover all 115 types. In-memory DashMap pre-aggregation → COPY BINARY flush every 5s. This is the single source for all `all_event_stats_*` queries.

2. **`ingested_raw_events` (short-term browsing)** — all 115 types written with 1h retention. Hot columns (`slot`, `core`, `submission_id`) populated from enricher for all types. Existing indexes (`node_id`, `event_type`, `core`, `slot`, `submission_id`) enable fast filtered queries.

3. **Drop continuous aggregates** — `event_stats_30s`, `event_stats_1m`, `event_stats_1h`, `core_stats_1m` are no longer needed. Count tables replace them entirely. UNION views reference only count tables.

4. **`/grafana/events` unrestricted** — remove the `is_pre_aggregated` rejection check. All 115 types browsable within the 1h window.

### Why this works

- **Continuous aggregates only look back 5 minutes** (`start_offset => INTERVAL '5 minutes'`). 1h retention was always more than enough for them. But with count tables covering everything, they're redundant entirely.
- **Pre-aggregated types were skipping raw to save write volume.** With 1h retention + TimescaleDB chunk dropping, the steady-state disk cost is bounded. High-volume events (3M/sec) produce ~10.8B rows/hour, but chunks older than 1h are dropped continuously.
- **No duplication in aggregation** — count tables are the single source. `ingested_raw_events` is purely for individual event inspection.

### What this means for count tables (Phase 6)

Expand from 9 to 13+ count tables to cover all 115 types:

**Existing 9 tables** (74 types): block_distribution, tickets, guarantee_sending, guarantee_receiving, shards, assurances, bundles, segments, preimages.

**New tables** (41 types):

| New table | Event types | Extra dimensions |
|---|---|---|
| `status_counts` | 0 (Dropped), 10-13 (Status/BestBlock/Finalized/SyncState) | slot |
| `connection_counts` | 20-28 (PeerConnected/Disconnected/streams/handshake) | reason |
| `block_counts` | 40-47 (BlockAuthored/Importing/Imported/Executed) | slot, reason |
| `ticket_low_counts` | 80-82 (TicketGenerated/Failed/Submitted) | reason |
| `wp_pipeline_counts` | 90-105 (WorkPackageSubmission through GuaranteeBuilt) | core (nullable — enrichment may fail for types 90, 91, 103), reason |

**Note on types 90-105:** These now go to count tables despite enrichment concerns. `core` will be NULL for types 90, 91, 103 (no enrichment path) and occasionally NULL for other types when enricher lookup fails (60s TTL). This is acceptable — count tables handle NULL dimensions, you just lose per-core breakdown for those rows. Total counts remain accurate.

### What gets removed

- `event_stats_30s` continuous aggregate
- `event_stats_1m` continuous aggregate (hierarchical on 30s)
- `event_stats_1h` continuous aggregate (hierarchical on 1m)
- `core_stats_1m` continuous aggregate
- The `event_stats_*` branches from UNION views (`all_event_stats_30s/1m/1h`, `all_core_stats_1m`)

### Impact on `/grafana/events`

Before: rejects 74 pre-aggregated types with error.
After: queries `ingested_raw_events` for all 115 types. Supports `node_id`, `core`, `event_type` filters via existing indexes.

**Grafana dashboards**: only `tart-grafana-services.json` uses `/grafana/events` (types 92, 99 for failure browsing). Unaffected — those types are in raw either way.

**UI ForensicsView**: gains ability to browse all 115 types within 1h window.

### Implementation order

1. **Phase 0**: Remove `is_pre_aggregated` rejection from `/grafana/events`. Add `node` and `core` filter params. (Already scoped.)
2. **Phase 6 (revised)**: Add 5 new count tables (not 4). Remove `is_pre_aggregated` skip from `batch_writer.rs` — all types write to raw. Drop continuous aggregates. Update UNION views to reference only count tables. Set 1h retention on `ingested_raw_events`.

---

## Summary: Revised Effort Estimates

| Area | Original estimate | Revised | Delta | Key change |
|---|---|---|---|---|
| Bottleneck profiling (Phase 5) | 4h backend | 2h backend + 2h UI rework | 0h | Deprecate legacy, rework UI for stage profiling |
| WP audit-progress (Phase 4) | 2h | **Phase 1: 6-8h, Phase 3: 8-12h** | **+12-18h** | Legacy endpoint is fundamentally wrong — audit is a separate pipeline, needs new `audit_tracker.rs` |
| Execution metrics (Phase 5) | 4h | 4h | 0h | Extract timing at ingestion confirmed as right approach |
| DA enhanced (Phase 5) | 6h | 7h | +1h | shard_distribution needs new table + da_tracker extension |
| Parallel running (Phase 0) | Undefined | 1h test setup + 2 weeks running | New | Integration test parity checks |
| Count table expansion (Phase 6) | 13h | 12h | -1h | 5 new count tables (all 115 types covered). Drop continuous aggregates (`event_stats_30s/1m/1h`, `core_stats_1m`). Write all types to raw with 1h retention for browsing. UNION views reference only count tables. See Section 9. |
| UNION view validation | Undefined | 2h | New | EXPLAIN ANALYZE before expanding. With continuous aggregates dropped, views are count-tables-only — cleaner planner behavior. |
| `/api/workpackages/active` Step 1 (Phase 4) | 2h | 2h | 0h | New endpoint with full envelope (WP list + summary + reached + stage_duration_percentiles) from existing `wp_tracking` columns. No schema changes. Unblocks UI migration. |
| `/api/workpackages/active` Step 2 (Phase 4) | — | 2h | +2h | Add `failure_reason`/`discard_reason` columns + ingestion. Enables `failure_breakdown` + per-WP failure display. Can ship independently after Step 1. |
| `/api/cores/status` (Phase 2) | 3h (1.5h+1.5h) | 2h UI only | -1h | No new endpoint. UI composes `/grafana/cores` + `/grafana/onchain/cores` + `/grafana/wp-active`. |
