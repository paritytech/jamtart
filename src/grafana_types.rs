//! Typed response structs for the Grafana HTTP API.
//!
//! Each struct documents its **data source pipeline** — how the data is collected,
//! aggregated, or enriched before being served. All structs derive `Serialize` for
//! JSON responses, `ToSchema` for OpenAPI documentation, and `FromRow` where the
//! SQL result maps directly to the struct fields.

use chrono::{DateTime, Utc};
use serde::{Serialize, Serializer};
use std::fmt;
use utoipa::ToSchema;

// ── DbServiceId ─────────────────────────────────────────────────────────

/// Service ID as stored in PostgreSQL (signed i32, bitwise equivalent to JAM's u32).
///
/// JAM uses u32 service IDs but PostgreSQL `INT` is signed, so we store them as i32.
/// This type centralises the i32↔hex conversion:
///   - Serializes to JSON as zero-padded hex: `"0x0000000a"`.
///   - Parses from decimal (`"10"`) or hex (`"0xa"`) input.
///   - Handles Grafana multi-select `{a,b}` wrapper.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(transparent)]
pub struct DbServiceId(pub i32);

impl DbServiceId {
    /// Parse a single service ID from decimal or `0x` hex.
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if let Some(hex) = s.strip_prefix("0x") {
            u32::from_str_radix(hex, 16).ok().map(|n| Self(n as i32))
        } else {
            s.parse::<i32>().ok().map(Self)
        }
    }

    /// Parse comma-separated service IDs (decimal or `0x` hex).
    /// Strips Grafana's `{a,b}` wrapper if present.
    pub fn parse_list(s: &str) -> Vec<Self> {
        let s = s
            .strip_prefix('{')
            .and_then(|s| s.strip_suffix('}'))
            .unwrap_or(s);
        s.split(',').filter_map(Self::parse).collect()
    }

    /// Convert a slice of `DbServiceId` to `Vec<i32>` for sqlx `= ANY($N)` binds.
    pub fn as_i32_vec(ids: &[Self]) -> Vec<i32> {
        ids.iter().map(|id| id.0).collect()
    }
}

impl fmt::Display for DbServiceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "0x{:08x}", self.0 as u32)
    }
}

impl Serialize for DbServiceId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl utoipa::ToSchema for DbServiceId {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("DbServiceId")
    }
}

impl utoipa::PartialSchema for DbServiceId {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::schema::ObjectBuilder::new()
            .schema_type(utoipa::openapi::schema::SchemaType::new(
                utoipa::openapi::schema::Type::String,
            ))
            .description(Some(
                "Service ID in zero-padded hex, e.g. \"0x0000000a\"",
            ))
            .into()
    }
}

impl From<i32> for DbServiceId {
    fn from(v: i32) -> Self {
        Self(v)
    }
}

impl From<u32> for DbServiceId {
    fn from(v: u32) -> Self {
        Self(v as i32)
    }
}

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
/// **Data source:** `all_core_stats_1m` UNION view (backed by count tables after
/// migration 020). Counts filtered by event type: 94 (WorkPackageReceived),
/// 105 (GuaranteeBuilt), 92 (WorkPackageFailed). `last_activity` from correlated
/// subquery on `wp_tracking` table.
#[derive(Debug, Serialize, ToSchema)]
pub struct CoreSummary {
    /// Core index
    pub core: i16,
    /// WorkPackageReceived events (type 94 as defined in JIP-3)
    pub work_packages: i64,
    /// GuaranteeBuilt events (type 105 as defined in JIP-3)
    pub guarantees: i64,
    /// WorkPackageFailed events (type 92 as defined in JIP-3)
    pub failures: i64,
    /// When the last work package was seen on this core (from wp_tracking.first_seen).
    /// NULL for cores with no WP activity in the queried time range.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_activity: Option<DateTime<Utc>>,
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
    /// Service IDs involved in this WP (hex-formatted)
    pub service_ids: Vec<DbServiceId>,
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
    /// 75th percentile propagation delay (ms)
    pub p75_ms: Option<i32>,
    /// 95th percentile propagation delay (ms)
    pub p95_ms: Option<i32>,
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
    /// Service ID (hex-formatted, e.g. "0x000000ff")
    pub service_id: DbServiceId,
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
    /// Service ID (hex-formatted, same encoding as ServiceRow)
    pub service_id: DbServiceId,
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

// ── /api/grafana/guarantee-convergence ───────────────────────────────────

/// Per-slot guarantee convergence summary (overview).
///
/// **Data source:** `guarantee_convergence_slots` table, populated by the
/// convergence_tracker flush. Aggregates all guarantees in a slot: flattens
/// received_timestamps across all work_report_hashes for that slot and computes
/// true cross-core percentiles of (received - built_at) latency.
#[derive(Debug, Serialize, ToSchema)]
pub struct GuaranteeConvergenceSlotRow {
    /// Slot number
    pub slot: i32,
    /// Slot timestamp (for Grafana X-axis)
    pub slot_timestamp: DateTime<Utc>,
    /// Number of guarantees in this slot
    pub guarantee_count: i16,
    /// Minimum receiver count across guarantees
    pub node_count: i16,
    /// p50 propagation latency (ms)
    pub p50_ms: Option<i32>,
    /// p75 propagation latency (ms)
    pub p75_ms: Option<i32>,
    /// p95 propagation latency (ms)
    pub p95_ms: Option<i32>,
    /// p99 propagation latency (ms)
    pub p99_ms: Option<i32>,
    /// p100 propagation latency (ms)
    pub p100_ms: Option<i32>,
    /// Earliest built_at across guarantees in slot
    pub built_at: DateTime<Utc>,
}

// ── /api/grafana/guarantee-convergence/detail ────────────────────────────

/// Per-guarantee convergence detail (drill-down by core or wp_hash).
///
/// **Data source:** `guarantee_convergence` table, one row per work_report_hash.
/// Measures: GuaranteeBuilt(105) anchor → GuaranteeReceived(112) reception
/// latency percentiles across all receiving validators.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct GuaranteeConvergenceDetailRow {
    /// Work report hash (hex-encoded)
    pub work_report_hash: String,
    /// Slot number
    pub slot: i32,
    /// Core index (NULL if guarantor not connected)
    pub core: Option<i16>,
    /// Work package hash (hex-encoded, NULL if guarantor not connected)
    pub wp_hash: Option<String>,
    /// Node ID of the guarantor that built this guarantee
    pub builder_node_id: Option<String>,
    /// Number of receiving validators
    pub node_count: i16,
    /// p50 propagation latency (ms)
    pub p50_ms: i32,
    /// p75 propagation latency (ms)
    pub p75_ms: Option<i32>,
    /// p95 propagation latency (ms)
    pub p95_ms: Option<i32>,
    /// p99 propagation latency (ms)
    pub p99_ms: i32,
    /// p100 propagation latency (ms)
    pub p100_ms: i32,
    /// When the guarantee was built
    pub built_at: DateTime<Utc>,
}

// ── /api/grafana/assurance-convergence ───────────────────────────────────

/// Per-anchor assurance convergence summary.
///
/// **Data source:** `assurance_convergence` table, populated by the
/// convergence_tracker flush. Each row represents one block anchor,
/// aggregating all senders' assurance propagation for that block.
///
/// Reception convergence: DistributingAssurance(126) → AssuranceReceived(131)
/// deltas, clamped to max(0, delta).
///
/// Distribution start spread: how quickly validators begin distributing
/// assurances (relative to the first distributor for this anchor).
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct AssuranceConvergenceRow {
    /// Block anchor (hex-encoded HeaderHash)
    pub anchor: String,
    /// Slot number
    pub slot: Option<i32>,
    /// Slot timestamp (for Grafana X-axis)
    pub slot_timestamp: Option<DateTime<Utc>>,
    /// Number of senders (validators distributing assurances)
    pub sender_count: i16,
    /// Total receiver count across all senders
    pub receiver_count: i32,
    /// p50 reception convergence (ms)
    pub p50_ms: i32,
    /// p75 reception convergence (ms)
    pub p75_ms: Option<i32>,
    /// p95 reception convergence (ms)
    pub p95_ms: Option<i32>,
    /// p99 reception convergence (ms)
    pub p99_ms: i32,
    /// p100 reception convergence (ms)
    pub p100_ms: i32,
    /// Distribution start spread p50 (ms)
    pub dist_start_p50_ms: Option<i32>,
    /// Distribution start spread p95 (ms)
    pub dist_start_p95_ms: Option<i32>,
    /// Distribution start spread p99 (ms)
    pub dist_start_p99_ms: Option<i32>,
    /// Distribution start spread p100 (ms)
    pub dist_start_p100_ms: Option<i32>,
    /// Earliest distribution start
    pub first_distributed_at: Option<DateTime<Utc>>,
    /// Latest distribution start
    pub last_distributed_at: Option<DateTime<Utc>>,
}

// ── /api/grafana/assurance-convergence/senders ──────────────────────────

/// Per-sender assurance convergence detail (for debugging individual nodes).
///
/// **Data source:** `assurance_convergence_senders` hypertable.
/// One row per (anchor, sender). Shows how quickly this sender's assurance
/// propagated to receiving validators.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct AssuranceConvergenceSenderRow {
    /// Block anchor (hex-encoded)
    pub anchor: String,
    /// Slot number
    pub slot: Option<i32>,
    /// Sender node ID
    pub sender_node_id: String,
    /// Number of receiving validators
    pub node_count: i16,
    /// p50 propagation latency (ms)
    pub p50_ms: i32,
    /// p75 propagation latency (ms)
    pub p75_ms: Option<i32>,
    /// p95 propagation latency (ms)
    pub p95_ms: Option<i32>,
    /// p99 propagation latency (ms)
    pub p99_ms: i32,
    /// p100 propagation latency (ms)
    pub p100_ms: i32,
    /// When this sender started distributing
    pub distributed_at: DateTime<Utc>,
}

// ── /api/grafana/convergence-timeseries ──────────────────────────────────

/// Convergence percentile timeseries row (from merged histograms).
///
/// **Data source:** Histogram columns on `guarantee_convergence`,
/// `assurance_convergence`, or `assurance_convergence_senders` tables,
/// SUMmed per time_bucket and converted to percentiles in Rust.
#[derive(Debug, Serialize, ToSchema)]
pub struct ConvergenceTimeseriesRow {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// p50 propagation latency (ms)
    pub p50_ms: Option<i32>,
    /// p75 propagation latency (ms)
    pub p75_ms: Option<i32>,
    /// p95 propagation latency (ms)
    pub p95_ms: Option<i32>,
    /// p99 propagation latency (ms)
    pub p99_ms: Option<i32>,
    /// p100 propagation latency (ms)
    pub p100_ms: Option<i32>,
    /// Total samples in this bucket
    pub sample_count: i32,
}

// ── /api/grafana/da-stats ────────────────────────────────────────────────

/// Per-node DA operational stats aggregated over a time range.
///
/// **Data source:** `da_node_stats` hypertable, populated by da_tracker flush.
/// One row per node with summed event counts, weighted avg latency, and
/// max active shard count.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct DaStatsRow {
    pub node_id: String,
    pub shard_requests_sent: i64,
    pub shard_requests_received: i64,
    pub shard_sent_confirmed: i64,
    pub shard_received_confirmed: i64,
    pub shards_transferred: i64,
    pub shard_failures: i64,
    pub preimage_ann_failures: i64,
    pub preimages_announced: i64,
    pub preimages_forgotten: i64,
    pub assurer_avg_latency_ms: Option<f32>,
    pub assurer_latency_samples: i64,
    pub guarantor_avg_latency_ms: Option<f32>,
    pub guarantor_latency_samples: i64,
    pub active_shards: i32,
}

// ── /api/grafana/shard-latency ──────────────────────────────────────────

/// Shard latency percentiles per time bucket (computed from merged histograms).
///
/// **Data source:** `shard_latency_hist` hypertable. Histograms are summed
/// across nodes per time bucket, then percentiles are interpolated from the
/// cumulative distribution in Rust.
#[derive(Debug, Serialize, ToSchema)]
pub struct ShardLatencyRow {
    pub ts: DateTime<Utc>,
    pub assurer_p50: Option<i32>,
    pub assurer_p75: Option<i32>,
    pub assurer_p95: Option<i32>,
    pub assurer_p99: Option<i32>,
    pub assurer_p100: Option<i32>,
    pub assurer_samples: i32,
    pub guarantor_p50: Option<i32>,
    pub guarantor_p75: Option<i32>,
    pub guarantor_p95: Option<i32>,
    pub guarantor_p99: Option<i32>,
    pub guarantor_p100: Option<i32>,
    pub guarantor_samples: i32,
    pub failed_count: i32,
}

// ── /api/grafana/wp-funnel-timeseries ────────────────────────────────────

/// Work package pipeline funnel bucketed over time — how many WPs reached
/// each stage per time bucket.
///
/// **Data source:** `wp_tracking` table, same as `/wp-funnel` but with
/// `time_bucket` grouping. Each row represents one time bucket containing
/// the count of WPs whose `first_seen` falls in that bucket, broken down
/// by pipeline stage (non-null stage timestamps).
///
/// Events: 94 (WorkPackageReceived) → received, 95 (Authorized) → authorized,
/// 101 (Refined) → refined, 102 (WorkReportBuilt) → report_built,
/// 105 (GuaranteeBuilt) → guarantee_built, 109 (GuaranteeDistributed) →
/// distributed, 92 (WorkPackageFailed) → failed.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct WpFunnelTimeseriesRow {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Total work packages in bucket
    pub total: i64,
    /// WPs that were received (WorkPackageReceived)
    pub received: i64,
    /// WPs that passed authorization (Authorized)
    pub authorized: i64,
    /// WPs that completed refinement (Refined)
    pub refined: i64,
    /// WPs with work report built (WorkReportBuilt)
    pub report_built: i64,
    /// WPs with guarantee built (GuaranteeBuilt)
    pub guarantee_built: i64,
    /// WPs fully distributed (GuaranteesDistributed)
    pub distributed: i64,
    /// WPs that hit a failure at any stage (WorkPackageFailed)
    pub failed: i64,
}

// ── /api/grafana/bottlenecks-timeseries ─────────────────────────────────

/// Work package pipeline bottleneck analysis bucketed over time.
///
/// **Data source:** `wp_tracking` table, same as `/bottlenecks` but with
/// `time_bucket` grouping. Per bucket: `percentile_cont(0.5)` and
/// `percentile_cont(0.95)` on inter-stage timestamp deltas.
///
/// Stages: authorize (received→authorized), refine (authorized→refined),
/// report (refined→report_built), guarantee (report_built→guarantee_built),
/// distribute (guarantee_built→distributed), pipeline_total
/// (received→COALESCE(distributed, last_updated)).
///
/// NULL stage timestamps are ignored by `percentile_cont`, so columns
/// may be NULL if no WPs in the bucket reached that stage.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct BottlenecksTimeseriesRow {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// received → authorized p50 (ms)
    pub authorize_p50: Option<f64>,
    /// received → authorized p95 (ms)
    pub authorize_p95: Option<f64>,
    /// authorized → refined p50 (ms)
    pub refine_p50: Option<f64>,
    /// authorized → refined p95 (ms)
    pub refine_p95: Option<f64>,
    /// refined → report_built p50 (ms)
    pub report_p50: Option<f64>,
    /// refined → report_built p95 (ms)
    pub report_p95: Option<f64>,
    /// report_built → guarantee_built p50 (ms)
    pub guarantee_p50: Option<f64>,
    /// report_built → guarantee_built p95 (ms)
    pub guarantee_p95: Option<f64>,
    /// guarantee_built → distributed p50 (ms)
    pub distribute_p50: Option<f64>,
    /// guarantee_built → distributed p95 (ms)
    pub distribute_p95: Option<f64>,
    /// received → distributed (or last_updated) p50 (ms)
    pub pipeline_p50: Option<f64>,
    /// received → distributed (or last_updated) p95 (ms)
    pub pipeline_p95: Option<f64>,
    /// Total WPs in bucket (with received_at IS NOT NULL)
    pub total_wps: i64,
    /// Failed WPs in bucket
    pub failed_wps: i64,
}

// ── /api/grafana/guarantee-discards ──────────────────────────────────────

/// Time-bucketed guarantee discard counts grouped by reason.
///
/// **Data source:** `guarantee_receiving_counts` table (pre-aggregated at
/// ingestion). Queries event_type=113 (GuaranteeDiscarded) rows where
/// reason IS NOT NULL, grouped by (bucket, reason).
#[derive(Debug, Serialize, ToSchema)]
pub struct GuaranteeDiscardRow {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Discard reason (e.g. "ReplacedByBetter(1)", "TooManyGuarantees(3)")
    pub reason: String,
    /// Number of discards in this bucket with this reason
    pub count: i64,
}

// ── /api/grafana/events ─────────────────────────────────────────────────

/// Raw event record from `ingested_raw_events` (1h retention browsing store).
///
/// **Data source:** `ingested_raw_events` hypertable. All 115 event types are
/// written to this table at ingestion time (after migration 020). The `data`
/// field contains the full event-specific JSONB payload which varies by type.
/// Hot columns (`slot`, `core`, `submission_id`, `wp_hash`) enable fast filtered
/// queries without JSONB extraction.
#[derive(Debug, Serialize, ToSchema)]
pub struct EventRow {
    /// Event timestamp (when the event occurred on the node)
    pub ts: DateTime<Utc>,
    /// Node that reported this event
    pub node_id: String,
    /// Event type code as defined in JIP-3
    pub event_type: i16,
    /// Full event payload (structure varies by event type)
    pub data: serde_json::Value,
    /// When the event was ingested into the database
    pub created_at: DateTime<Utc>,
}

/// Paginated event search response.
///
/// **Data source:** `ingested_raw_events` hypertable (1h retention). Supports
/// filtering by event type, node, core, and wp_hash. All 115 event types are
/// browsable after migration 020.
#[derive(Debug, Serialize, ToSchema)]
pub struct EventsSearchResponse {
    /// Event records matching the query
    pub events: Vec<EventRow>,
    /// Pagination metadata
    pub pagination: PaginationMeta,
}

/// Pagination metadata for paginated endpoints.
#[derive(Debug, Serialize, ToSchema)]
pub struct PaginationMeta {
    /// Current offset (0-based)
    pub offset: i64,
    /// Number of results requested
    pub limit: i64,
    /// Total number of matching records
    pub total: i64,
    /// Whether more records exist beyond offset + limit
    pub has_more: bool,
}

// ── Phase 3 response types ──────────────────────────────────────────────

// ── /api/grafana/failure-rates ──────────────────────────────────────────

/// Network failure rates with per-category and per-node breakdown.
///
/// **Data source:** `all_event_stats_1m` UNION view for aggregate failure
/// counts across 6 categories. `ingested_raw_events` (1h retention) for
/// recent failure details with reason text from JSONB.
#[derive(Debug, Serialize, ToSchema)]
pub struct FailureRatesResponse {
    /// Overall failure rate across all categories
    pub overall: FailureOverall,
    /// Per-category breakdown: block_authoring, tickets, work_packages, guarantees, shards, assurances
    pub by_category: Vec<FailureCategory>,
    /// Top 20 nodes by failure count
    pub by_node: Vec<FailureByNode>,
    /// Last 20 failure events from past 5 minutes with reason text
    pub recent_failures: Vec<RecentFailure>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct FailureOverall {
    /// Total events (successes + failures) across all monitored types
    pub total_events: i64,
    /// Failed events count
    pub failed_events: i64,
    /// Failure rate: failed_events / total_events (0.0 to 1.0)
    pub failure_rate: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct FailureCategory {
    /// Category name: block_authoring, tickets, work_packages, guarantees, shards, assurances
    pub category: String,
    /// Total attempts (successes + failures) in this category
    pub attempts: i64,
    /// Failed event count in this category
    pub failures: i64,
    /// Failure rate: failures / attempts (0.0 to 1.0)
    pub rate: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct FailureByNode {
    /// Node identifier (Ed25519 public key hex)
    pub node_id: String,
    /// Total events from this node across all monitored types
    pub total_events: i64,
    /// Failed events from this node
    pub failures: i64,
    /// Per-node failure rate (0.0 to 1.0)
    pub failure_rate: f64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RecentFailure {
    /// JIP-3 event type code
    pub event_type: i16,
    /// Human-readable event name (e.g. "WorkPackageFailed")
    pub event_name: String,
    /// Node that reported this failure
    pub node_id: String,
    /// When the failure occurred
    pub timestamp: DateTime<Utc>,
    /// Failure reason extracted from event JSONB (may be null if not present)
    pub reason: Option<String>,
}

// ── /api/grafana/sync-timeline ──────────────────────────────────────────

/// Network sync status over time — how many nodes are synced vs behind.
///
/// **Data source:** `status_counts` table for BestBlockChanged events with
/// `slot` dimension. Network slot = MAX(slot) per bucket across all nodes.
/// A node is "synced" if its max slot is within 2 of the network max.
#[derive(Debug, Serialize, ToSchema)]
pub struct SyncTimelineRow {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Total distinct nodes that reported BestBlockChanged in this bucket
    pub total_nodes: i64,
    /// Nodes whose max slot is within 2 of the network max slot
    pub synced_nodes: i64,
    /// Nodes whose max slot is more than 2 behind the network max
    pub behind_nodes: i64,
    /// Sync percentage: synced_nodes / total_nodes * 100 (0.0 to 100.0)
    pub sync_percentage: f64,
    /// Highest slot reported by any node in this bucket
    pub network_slot: i32,
}

// ── /api/grafana/connections-timeline ────────────────────────────────────

/// Network connection activity over time.
///
/// **Data source:** `all_event_stats_30s` for types 23 (ConnectedIn),
/// 26 (ConnectedOut), 27 (Disconnected). `nodes` table for per-node
/// uptime and health stats (maintained by batch_writer).
#[derive(Debug, Serialize, ToSchema)]
pub struct ConnectionsTimelineResponse {
    /// Time-bucketed connection counts
    pub timeline: Vec<ConnectionsBucket>,
    /// Overall connection health stats
    pub health_stats: ConnectionHealthStats,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ConnectionsBucket {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// ConnectedIn + ConnectedOut events in this bucket
    pub connections: i64,
    /// Disconnected events in this bucket
    pub disconnections: i64,
    /// Distinct nodes with any connection event in this bucket
    pub active_nodes: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ConnectionHealthStats {
    /// Total nodes ever seen in the `nodes` table
    pub total_nodes_seen: i64,
    /// Nodes currently connected (is_connected = true)
    pub currently_connected: i64,
}

// ── /api/grafana/guarantees ─────────────────────────────────────────────

/// Guarantee pipeline totals and success rates.
///
/// **Data source:** `all_event_stats_1m` UNION view for types 105-113.
/// Count tables provide correct data for types 106-113 (which were
/// previously returning 0 from raw events — this fixes a legacy bug).
#[derive(Debug, Serialize, ToSchema)]
pub struct GuaranteesResponse {
    /// Per-type event counts
    pub totals: GuaranteeTotals,
    /// Send and receive success rates
    pub success_rates: GuaranteeSuccessRates,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GuaranteeTotals {
    /// GuaranteeBuilt — guarantor built a guarantee proof for a work report
    pub built: i64,
    /// SendingGuarantee — guarantor is sending the guarantee to peers
    pub sending: i64,
    /// GuaranteeSendFailed — guarantee send attempt failed
    pub send_failed: i64,
    /// GuaranteeSent — guarantee successfully sent to a peer
    pub sent: i64,
    /// GuaranteesDistributed — all guarantees for a WP distributed to all peers
    pub distributed: i64,
    /// ReceivingGuarantee — node receiving a guarantee from a peer
    pub receiving: i64,
    /// GuaranteeReceiveFailed — guarantee receive failed (invalid, etc.)
    pub receive_failed: i64,
    /// GuaranteeReceived — guarantee received and validated
    pub received: i64,
    /// GuaranteeDiscarded — guarantee removed from local pool (various reasons)
    pub discarded: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GuaranteeSuccessRates {
    /// Sent / (Sending + SendFailed + Sent). 1.0 when no sending activity.
    pub send_success_rate: f64,
    /// Received / (Receiving + ReceiveFailed + Received). 1.0 when no receiving activity.
    pub receive_success_rate: f64,
}

// ── /api/grafana/guarantees/by-guarantor ────────────────────────────────

/// Per-guarantor breakdown with node→core mapping.
///
/// **Data source:** `guarantee_convergence` table for observed node→core
/// mapping (builder_node_id + core, 90d retention). `all_event_stats_1m`
/// for per-node success rates (types 105, 107, 109).
///
/// **Caveat:** Node→core mapping reflects observed guarantee behavior, not
/// protocol-level validator→core assignment. Telemetry does not transmit
/// `validator_index`. See deep-dive Section 5.
#[derive(Debug, Serialize, ToSchema)]
pub struct GuarantorBreakdownResponse {
    pub guarantors: Vec<GuarantorRow>,
    pub total_guarantors: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct GuarantorRow {
    /// Node identifier (Ed25519 public key hex)
    pub node_id: String,
    /// Core this node most frequently guarantees for (by count)
    pub primary_core: Option<i16>,
    /// Total guarantees built by this node in the time range
    pub guarantee_count: i64,
    /// Timestamp of this node's most recent guarantee
    pub last_guarantee: Option<DateTime<Utc>>,
    /// All cores this node has guaranteed for (sorted, deduplicated)
    pub cores_active: Vec<i16>,
}

// ── /api/grafana/wp-stats ───────────────────────────────────────────────

/// Work package pipeline summary — counts per stage, by core.
///
/// **Data source:** `wp_tracking` table for pipeline stage counts
/// (received → distributed/failed) + by-core breakdown. `all_event_stats_1m`
/// for pre-pipeline event counts (types 90 submissions, 91 being_shared,
/// 93 duplicates).
#[derive(Debug, Serialize, ToSchema)]
pub struct WpStatsResponse {
    pub totals: WpStageTotals,
    pub by_core: Vec<WpCoreCount>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WpStageTotals {
    /// WorkPackageSubmission events (pre-pipeline, from aggregates)
    pub submissions: i64,
    /// WorkPackageBeingShared events (pre-pipeline, from aggregates)
    pub being_shared: i64,
    /// DuplicateWorkPackage events (pre-pipeline, from aggregates)
    pub duplicates: i64,
    /// WPs that reached "received" stage (WorkPackageReceived, from wp_tracking)
    pub received: i64,
    /// WPs that reached "authorized" stage (Authorized, from wp_tracking)
    pub authorized: i64,
    /// WPs that reached "refined" stage (Refined, from wp_tracking)
    pub refined: i64,
    /// WPs that reached "report_built" stage (WorkReportBuilt, from wp_tracking)
    pub report_built: i64,
    /// WPs that reached "guarantee_built" stage (GuaranteeBuilt, from wp_tracking)
    pub guarantee_built: i64,
    /// WPs that completed pipeline (GuaranteesDistributed, from wp_tracking)
    pub distributed: i64,
    /// WPs that failed at any stage (WorkPackageFailed, from wp_tracking)
    pub failed: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct WpCoreCount {
    /// Core index
    pub core: i16,
    /// Total WPs on this core in the time range
    pub count: i64,
}

// ── /api/grafana/validators/cores ───────────────────────────────────────

/// Node→core mapping based on observed guarantee behavior.
///
/// **Data source:** `guarantee_convergence` table (builder_node_id + core).
/// Shares `node_core_mapping()` helper with `/guarantees/by-guarantor`.
///
/// **Caveat:** This mapping reflects observed guarantee behavior, not
/// protocol-level validator→core assignment. Telemetry does not transmit
/// `validator_index` — there is no way to map node_id → validator_index
/// without upstream JIP-3 changes.
#[derive(Debug, Serialize, ToSchema)]
pub struct ValidatorCoreRow {
    /// Node identifier (Ed25519 public key hex)
    pub node_id: String,
    /// Core this node most frequently guarantees for (by count). NULL if no core data.
    pub primary_core: Option<i16>,
    /// Total guarantees built by this node in the time range
    pub guarantee_count: i64,
}

// ── /api/grafana/network-health ─────────────────────────────────────────

/// Multi-signal network health score with per-component breakdown.
///
/// **Data source:** `all_event_stats_1m` UNION view for 5-component health
/// scoring (connectivity, block production, DA, work packages, throughput).
/// LiveCounters for real-time throughput overlay. `node_stats` for peer counts.
/// Scoring logic (~200 LOC) ported from legacy `store.rs`.
#[derive(Debug, Serialize, ToSchema)]
pub struct NetworkHealthResponse {
    pub health_score: f64,
    pub overall_health: String,
    pub components: Vec<HealthComponent>,
    pub alerts: Vec<HealthAlert>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HealthComponent {
    /// Component name: block_production, work_packages, data_availability, connectivity, event_throughput
    pub name: String,
    /// Component health score (0.0 to 100.0)
    pub score: f64,
    /// Component status: healthy (>= 95/90%), degraded (>= 80/70%), unhealthy (below)
    pub status: String,
    /// Specific issues detected (empty when healthy)
    pub issues: Vec<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HealthAlert {
    /// Alert severity: "warning" or "error"
    pub severity: String,
    /// Human-readable alert message
    pub message: String,
    /// Which health component generated this alert
    pub component: String,
}

// ── Shared: node_core_mapping helper row ────────────────────────────────

/// Row from the node→core mapping query on guarantee_convergence.
/// Used by /guarantees/by-guarantor and /validators/cores.
#[derive(Debug)]
pub struct NodeCoreRow {
    pub node_id: String,
    pub core: i16,
    pub guarantee_count: i64,
    pub last_guarantee: DateTime<Utc>,
}
