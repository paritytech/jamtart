//! Typed response structs for the Grafana HTTP API.
//!
//! Each struct documents its **data source pipeline** — how the data is collected,
//! aggregated, or enriched before being served. All structs derive `Serialize` for
//! JSON responses, `ToSchema` for OpenAPI documentation, and `FromRow` where the
//! SQL result maps directly to the struct fields.

use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

// ── /api/grafana/stats ──────────────────────────────────────────────────

/// Dashboard summary counters.
///
/// **Data source:** Event counts from the `event_stats_1m` continuous aggregate
/// (TimescaleDB rollup of the raw `events` hypertable, 1-minute buckets).
/// Queries specific event types: 42 (BlockAuthored) for slot events,
/// 105 (GuaranteeBuilt) for guarantees, 92 (WorkPackageFailed) for failures,
/// 94 (WorkPackageReceived) for WP events. Connected node count comes from
/// the `nodes` table (updated on TCP connect/disconnect).
///
/// The handler overlays real-time fields from in-memory `LiveCounters`:
/// events/blocks per second (10s rolling average), best/finalized slot numbers,
/// and active TCP connection count.
#[derive(Debug, Serialize, ToSchema)]
pub struct StatsResponse {
    /// Number of currently connected nodes (from `nodes` table)
    pub connected_nodes: i32,
    /// Max BlockAuthored event count in range
    pub slot_events: i64,
    /// Total GuaranteeBuilt events in range
    pub guarantees: i64,
    /// Total WorkPackageFailed events in range
    pub failures: i64,
    /// Total WorkPackageReceived events in range
    pub wp_events: i64,
    /// Events per second, 10s rolling average (from LiveCounters, may be absent)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events_per_sec_10s: Option<f64>,
    /// Blocks per second, 10s rolling average (from LiveCounters, may be absent)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocks_per_sec_10s: Option<f64>,
    /// Latest best slot number (from LiveCounters, may be absent)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub best_slot: Option<u32>,
    /// Latest finalized slot number (from LiveCounters, may be absent)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalized_slot: Option<u32>,
    /// Currently active TCP connections (from LiveCounters, may be absent)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active_nodes: Option<usize>,
}

// ── /api/grafana/timeseries ─────────────────────────────────────────────

/// Time-bucketed event count row.
///
/// **Data source:** Auto-selected TimescaleDB continuous aggregate based on
/// requested interval: `event_stats_30s` (intervals < 60s), `event_stats_1m`
/// (< 3600s), `event_stats_1h` (>= 3600s), or `core_stats_1m` (when
/// group_by=core). These aggregates roll up the raw `events` hypertable.
///
/// Exactly one of `event_type`, `core`, or `node_id` will be populated,
/// depending on the `group_by` parameter.
#[derive(Debug, Serialize, ToSchema)]
pub struct TimeseriesRow {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Aggregated event count for this bucket + group
    pub count: i64,
    /// Numeric event type code as defined in JIP-3 (present only when group_by=event_type)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_type: Option<i16>,
    /// Human-readable event type name, resolved from event_type_meta (present only when group_by=event_type)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_type_name: Option<&'static str>,
    /// Core index (present only when group_by=core)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub core: Option<i16>,
    /// Node identifier (present only when group_by=node_id)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
}

// ── /api/grafana/cores ──────────────────────────────────────────────────

/// Per-core activity summary.
///
/// **Data source:** `core_stats_1m` continuous aggregate. Counts filtered by
/// event type: 94 (WorkPackageReceived), 105 (GuaranteeBuilt),
/// 92 (WorkPackageFailed).
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct CoreSummary {
    /// Core index
    pub core: i16,
    /// WorkPackageReceived events (type 94 as defined in JIP-3)
    pub work_packages: i64,
    /// GuaranteeBuilt events (type 105 as defined in JIP-3)
    pub guarantees: i64,
    /// WorkPackageFailed events (type 92 as defined in JIP-3)
    pub failures: i64,
}

/// Single core detail with recent work packages.
///
/// **Data source:** Same as `CoreSummary` for counters. The `recent_work_packages`
/// come from the `wp_tracking` table, which is populated by the enricher
/// (`src/enricher.rs`) correlating WP pipeline events (types 90–109 as defined
/// in JIP-3) across nodes — tracking each work package from submission through
/// authorization, refinement, report building, guarantee building, distribution,
/// or failure.
#[derive(Debug, Serialize, ToSchema)]
pub struct CoreDetail {
    /// Core index
    pub core: i16,
    /// WorkPackageReceived events (type 94 as defined in JIP-3)
    pub work_packages: i64,
    /// GuaranteeBuilt events (type 105 as defined in JIP-3)
    pub guarantees: i64,
    /// WorkPackageFailed events (type 92 as defined in JIP-3)
    pub failures: i64,
    /// Up to 100 most recent work packages for this core
    pub recent_work_packages: Vec<WpTrackingRow>,
}

/// A work package lifecycle record from `wp_tracking`.
///
/// **Data source:** `wp_tracking` hypertable, populated by the `wp_tracker`
/// module which correlates WP pipeline events (as defined in JIP-3) across
/// multiple nodes: 94 (WorkPackageReceived), 95 (Authorized), 101 (Refined),
/// 102 (WorkReportBuilt), 105 (GuaranteeBuilt), 109 (GuaranteeDistributed),
/// 92 (WorkPackageFailed). Each row tracks one work package through its entire
/// lifecycle with timestamps for each pipeline stage.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpTrackingRow {
    /// Hex-encoded work package hash
    pub wp_hash: String,
    /// When this WP was first seen by any node
    pub first_seen: DateTime<Utc>,
    /// Last time any stage was updated
    pub last_updated: DateTime<Utc>,
    /// Current pipeline stage (numeric)
    pub stage: i16,
    /// Node that first received this WP
    pub received_by: i16,
    /// Node that built the guarantee
    pub guaranteed_by: i16,
    /// Service IDs involved in this WP
    pub service_ids: Vec<i32>,
    /// Timestamp when received
    pub received_at: Option<DateTime<Utc>>,
    /// Timestamp when authorization completed
    pub authorized_at: Option<DateTime<Utc>>,
    /// Timestamp when refinement completed
    pub refined_at: Option<DateTime<Utc>>,
    /// Timestamp when work report was built
    pub report_built_at: Option<DateTime<Utc>>,
    /// Timestamp when guarantee was built
    pub guarantee_built_at: Option<DateTime<Utc>>,
    /// Timestamp when guarantee was distributed
    pub distributed_at: Option<DateTime<Utc>>,
    /// Timestamp when WP failed (null if successful)
    pub failed_at: Option<DateTime<Utc>>,
}

// ── /api/grafana/blocks/convergence ─────────────────────────────────────

/// Block propagation convergence percentiles per slot.
///
/// **Data source:** `slot_convergence` table, populated by the enricher which
/// measures the time between block authoring (on the author node) and reception
/// across all other nodes. Percentiles (p50/p99/p100) represent network-wide
/// propagation latency in milliseconds.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct BlockConvergenceRow {
    /// Slot number
    pub slot: i32,
    /// Event type used for convergence measurement
    pub event_type: i16,
    /// Human-readable event type name
    #[sqlx(skip)]
    pub event_type_name: &'static str,
    /// Number of nodes that reported this event
    pub node_count: i16,
    /// 50th percentile propagation delay (ms)
    pub p50_ms: i32,
    /// 99th percentile propagation delay (ms)
    pub p99_ms: i32,
    /// Maximum propagation delay (ms)
    pub p100_ms: i32,
    /// When the block was authored
    pub authored_at: DateTime<Utc>,
}

// ── /api/grafana/blocks/contents ────────────────────────────────────────

/// Block contents extracted from BlockAuthored events.
///
/// **Data source:** Raw `events` hypertable, filtered to event_type=42
/// (BlockAuthored). The extrinsic breakdown (guarantees, assurances, etc.)
/// is extracted from the JSONB `data` column → `Authored.outline` fields.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct BlockContentsRow {
    /// Slot number
    pub slot: i32,
    /// When the block was authored
    pub timestamp: DateTime<Utc>,
    /// Node that authored the block
    pub node_id: String,
    /// Number of guarantees in the block
    pub num_guarantees: Option<i32>,
    /// Number of assurances in the block
    pub num_assurances: Option<i32>,
    /// Number of preimages in the block
    pub num_preimages: Option<i32>,
    /// Number of tickets in the block
    pub num_tickets: Option<i32>,
    /// Number of dispute verdicts in the block
    pub num_disputes: Option<i32>,
    /// Total extrinsic size in bytes
    pub extrinsic_size: Option<i32>,
}

// ── /api/grafana/services ───────────────────────────────────────────────

/// Per-service activity and gas usage totals.
///
/// **Data source:** `service_stats_1m` continuous aggregate over the
/// `event_services` join table. This aggregate tracks per-service event counts
/// and gas consumption. Event types: 94 (WorkPackageReceived) for WP counts,
/// 101 (Refined) for refinement gas, 95 (Authorized) for authorization gas,
/// 47 (BlockExecuted) for execution gas.
#[derive(Debug, Serialize, ToSchema)]
pub struct ServiceRow {
    /// Service ID, hex-encoded (e.g. "0xff"). JAM uses u32 service IDs; stored as i32 in PostgreSQL.
    pub service_id: String,
    /// Total WorkPackageReceived events
    pub work_packages: i64,
    /// Total Refined events
    pub refinements: i64,
    /// Total refinement gas consumed
    pub refinement_gas: i64,
    /// Total Authorized events
    pub authorizations: i64,
    /// Total authorization gas consumed
    pub authorization_gas: i64,
    /// Total BlockExecuted events
    pub executions: i64,
    /// Total execution gas consumed
    pub execution_gas: i64,
}

// ── /api/grafana/services/timeseries ────────────────────────────────────

/// Time-bucketed per-service metrics.
///
/// **Data source:** Same `service_stats_1m` continuous aggregate as `/services`,
/// re-bucketed via `time_bucket()` to the requested interval.
#[derive(Debug, Serialize, ToSchema)]
pub struct ServiceTimeseriesRow {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Service ID, hex-encoded (same encoding as ServiceRow)
    pub service_id: String,
    /// WorkPackageReceived count in bucket
    pub work_packages: i64,
    /// Authorization gas in bucket
    pub authorization_gas: i64,
    /// Refinement gas in bucket
    pub refinement_gas: i64,
    /// Execution gas in bucket
    pub execution_gas: i64,
}

// ── /api/grafana/nodes ──────────────────────────────────────────────────

/// Node metadata record.
///
/// **Data source:** `nodes` table, updated on TCP connect/disconnect and
/// Status events (type 10 as defined in JIP-3). `total_event_count` is computed
/// as `event_count` (current session) + `total_events` (historical, accumulated
/// across reconnects).
#[derive(Debug, Serialize, ToSchema)]
pub struct NodeRow {
    /// Unique node identifier (64-char hex)
    pub node_id: String,
    /// libp2p peer ID
    pub peer_id: String,
    /// Implementation name (e.g. "polkajam", "jamtart")
    pub implementation_name: String,
    /// Implementation version
    pub implementation_version: String,
    /// Additional node metadata (JSONB)
    pub node_info: serde_json::Value,
    /// When the node first connected
    pub connected_at: DateTime<Utc>,
    /// When the node disconnected (null if still connected)
    pub disconnected_at: Option<DateTime<Utc>>,
    /// Most recent activity timestamp
    pub last_seen_at: DateTime<Utc>,
    /// Whether the node is currently connected
    pub is_connected: bool,
    /// Total events received across all sessions (event_count + total_events)
    pub total_event_count: i64,
    /// TCP address
    pub address: Option<String>,
}

// ── /api/grafana/node-stats ─────────────────────────────────────────────

/// Raw node status snapshot (~2s granularity).
///
/// **Data source:** `node_stats` hypertable, inserted from Status events
/// (type 10) which each node sends periodically. Contains peer counts,
/// shard/preimage storage metrics, and guarantee distribution across cores.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct NodeStatsRow {
    /// Timestamp of the status report
    pub timestamp: DateTime<Utc>,
    /// Node that reported this status
    pub node_id: String,
    /// Total connected peers
    pub num_peers: i32,
    /// Validator peers
    pub num_val_peers: i32,
    /// Sync peers
    pub num_sync_peers: i32,
    /// Number of DA shards held
    pub num_shards: i32,
    /// Total shard storage in bytes
    pub shards_size: i64,
    /// Number of preimages held
    pub num_preimages: i32,
    /// Total preimage storage in bytes
    pub preimages_size: i32,
    /// Minimum guarantees across cores
    pub min_guarantees: i16,
    /// Maximum guarantees across cores
    pub max_guarantees: i16,
    /// Average guarantees per core
    pub avg_guarantees: f32,
    /// Number of cores with zero guarantees
    pub zero_guarantee_cores: i16,
}

// ── /api/grafana/node-stats-aggregate ───────────────────────────────────

/// 1-minute aggregated node stats.
///
/// **Data source:** `node_stats_1m` continuous aggregate (1-minute rollup of
/// `node_stats`). In **network-wide** mode (no node filter), values are
/// aggregated across all nodes: `avg_*` = AVG of per-node averages,
/// `min_*` = global MIN, `max_*` = global MAX per bucket. In **per-node**
/// mode, returns the raw per-node aggregate rows with `node_id` populated.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct NodeStatsAggregateRow {
    /// Bucket start timestamp
    pub bucket: DateTime<Utc>,
    /// Node ID (present only when filtering by specific nodes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node_id: Option<String>,
    /// Average total peers
    pub avg_peers: i32,
    /// Minimum total peers
    pub min_peers: i32,
    /// Maximum total peers
    pub max_peers: i32,
    /// Average validator peers
    pub avg_val_peers: i32,
    /// Minimum validator peers
    pub min_val_peers: i32,
    /// Maximum validator peers
    pub max_val_peers: i32,
    /// Average sync peers
    pub avg_sync_peers: i32,
    /// Minimum sync peers
    pub min_sync_peers: i32,
    /// Maximum sync peers
    pub max_sync_peers: i32,
    /// Average shards
    pub avg_shards: i32,
    /// Minimum shards
    pub min_shards: i32,
    /// Maximum shards
    pub max_shards: i32,
    /// Average shard storage (bytes)
    pub avg_shards_size: i64,
    /// Maximum shard storage (bytes)
    pub max_shards_size: i64,
    /// Average preimages
    pub avg_preimages: i32,
    /// Maximum preimages
    pub max_preimages: i32,
    /// Average preimage storage (bytes)
    pub avg_preimages_size: i32,
    /// Maximum preimage storage (bytes)
    pub max_preimages_size: i32,
    /// Average guarantees per core
    pub avg_guarantees: f64,
    /// Minimum guarantees across cores
    pub min_guarantees: i16,
    /// Maximum guarantees across cores
    pub max_guarantees: i16,
    /// Maximum cores with zero guarantees
    pub max_zero_guarantee_cores: i16,
    /// Number of status reports in this bucket
    pub status_count: i64,
}

// ── /api/grafana/db-stats ───────────────────────────────────────────────

/// TimescaleDB metadata: table sizes, row counts, compression stats.
///
/// **Data source:** TimescaleDB internal functions:
/// `hypertable_detailed_size()` for table/index/toast byte breakdown,
/// `approximate_row_count()` for fast row estimates,
/// `hypertable_compression_stats()` for compression ratios.
#[derive(Debug, Serialize, ToSchema)]
pub struct DbStatsResponse {
    /// Per-table size breakdown
    pub tables: Vec<TableSize>,
    /// Approximate row counts
    pub row_counts: Vec<RowCount>,
    /// Compression statistics
    pub compression: Vec<CompressionInfo>,
}

/// Byte breakdown for a single hypertable.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct TableSize {
    pub table_name: String,
    pub total_bytes: i64,
    pub table_bytes: i64,
    pub index_bytes: i64,
    pub toast_bytes: i64,
}

/// Approximate row count for a table.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct RowCount {
    pub table_name: String,
    pub row_count: i64,
}

/// Compression statistics for a hypertable.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct CompressionInfo {
    pub table_name: String,
    pub compressed_chunks: i64,
    pub before_compression_bytes: i64,
    pub after_compression_bytes: i64,
}

// ── /api/grafana/bottlenecks ────────────────────────────────────────────

/// Work package pipeline bottleneck analysis.
///
/// **Data source:** `wp_tracking` table, populated by the `wp_tracker` module
/// correlating JIP-3 events 94→95→101→102→105→109 (and 92 for failures).
/// Stage timings are computed via `percentile_cont(0.5/0.95)` on the
/// inter-stage timestamp deltas (received→authorized→refined→report_built→
/// guarantee_built→distributed). The pipeline_total measures received_at to
/// distributed_at (or last_updated for incomplete WPs). Failure rate is the
/// ratio of WPs with `failed_at` set.
#[derive(Debug, Serialize, ToSchema)]
pub struct BottlenecksResponse {
    /// Percentile timings for each pipeline stage
    pub stage_timing: StageTiming,
    /// Fraction of WPs that failed (0.0 to 1.0)
    pub failure_rate: f64,
    /// Total work packages analyzed
    pub total_wps: i64,
    /// Number of failed work packages
    pub failed_wps: i64,
    /// Average total pipeline time in milliseconds
    pub avg_pipeline_ms: Option<f64>,
}

/// Percentile timing pair for a single pipeline stage.
#[derive(Debug, Serialize, ToSchema)]
pub struct Percentiles {
    /// 50th percentile (median) in milliseconds
    pub p50_ms: Option<f64>,
    /// 95th percentile in milliseconds
    pub p95_ms: Option<f64>,
}

/// All pipeline stage timings.
#[derive(Debug, Serialize, ToSchema)]
pub struct StageTiming {
    /// received_at → authorized_at
    pub authorize: Percentiles,
    /// authorized_at → refined_at
    pub refine: Percentiles,
    /// refined_at → report_built_at
    pub report: Percentiles,
    /// report_built_at → guarantee_built_at
    pub guarantee: Percentiles,
    /// guarantee_built_at → distributed_at
    pub distribute: Percentiles,
    /// received_at → distributed_at (or last_updated)
    pub pipeline_total: Percentiles,
}

// ── /api/grafana/wp-funnel ──────────────────────────────────────────────

/// Work package pipeline funnel — how many WPs reached each stage.
///
/// **Data source:** `wp_tracking` table, populated by the `wp_tracker` module
/// which correlates WP pipeline events (as defined in JIP-3) across nodes:
/// 94 (WorkPackageReceived) → received, 95 (Authorized) → authorized,
/// 101 (Refined) → refined, 102 (WorkReportBuilt) → report_built,
/// 105 (GuaranteeBuilt) → guarantee_built, 109 (GuaranteeDistributed) →
/// distributed, 92 (WorkPackageFailed) → failed. Each count represents WPs
/// that have a non-null timestamp for that stage.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct WpFunnelResponse {
    /// Total work packages in range
    pub total: i64,
    /// WPs that were received
    pub received: i64,
    /// WPs that passed authorization
    pub authorized: i64,
    /// WPs that completed refinement
    pub refined: i64,
    /// WPs with work report built
    pub report_built: i64,
    /// WPs with guarantee built
    pub guarantee_built: i64,
    /// WPs fully distributed
    pub distributed: i64,
    /// WPs that hit a failure at any stage
    pub failed: i64,
}

// ── /api/grafana/events ─────────────────────────────────────────────────

/// Raw event record from the events hypertable.
///
/// **Data source:** `events` hypertable directly (not aggregated). This is
/// the raw telemetry data as received from nodes via the TCP binary protocol
/// (JIP-3). The `data` field contains the full event-specific JSONB payload
/// which varies by event type.
#[derive(Debug, Serialize, ToSchema)]
pub struct EventRow {
    /// Event timestamp
    pub ts: DateTime<Utc>,
    /// Node that reported this event
    pub node_id: String,
    /// Event type code as defined in JIP-3
    pub event_type: i16,
    /// Full event payload (structure varies by event type)
    pub data: serde_json::Value,
}
