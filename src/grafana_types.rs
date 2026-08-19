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
            .description(Some("Service ID in zero-padded hex, e.g. \"0x0000000a\""))
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

/// One core's work-package activity over the time range.
#[derive(Debug, Serialize, ToSchema)]
pub struct CoreSummary {
    /// Core index
    pub core: i16,
    /// WorkPackageReceived(94) reports for this core — one per guarantor that
    /// received a work package, so higher than the number of distinct work packages
    pub work_packages: i64,
    /// GuaranteeBuilt(105) reports for this core, one per guarantor per work report
    pub guarantees: i64,
    /// WorkPackageFailed(92) reports for this core
    pub failures: i64,
    /// When the newest work package on this core was first observed. Counts every
    /// work package seen since the range start, so it can lie beyond the range end.
    /// Null for a core with no work-package activity since the range start.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_activity: Option<DateTime<Utc>>,
}

/// One core's work-package activity, with the pipeline timelines of its most
/// recent work packages.
#[derive(Debug, Serialize, ToSchema)]
pub struct CoreDetail {
    /// Core index
    pub core: i16,
    /// WorkPackageReceived(94) reports for this core — one per guarantor that
    /// received a work package, so higher than the number of distinct work packages
    pub work_packages: i64,
    /// GuaranteeBuilt(105) reports for this core, one per guarantor per work report
    pub guarantees: i64,
    /// WorkPackageFailed(92) reports for this core
    pub failures: i64,
    /// Up to 100 work packages first observed on this core in the range, newest
    /// first, each with its pipeline timeline
    pub recent_work_packages: Vec<WpTrackingRow>,
}

/// One work package's pipeline timeline, from reception to guarantee distribution.
///
/// Each stage timestamp is the first time any guarantor reported that stage for
/// this work package: WorkPackageReceived(94), Authorized(95), Refined(101),
/// WorkReportBuilt(102), GuaranteeBuilt(105), GuaranteesDistributed(109), and
/// WorkPackageFailed(92) if it failed. The first four are reported by both the
/// primary and the secondary guarantors, so the timeline follows whichever
/// guarantor reached each stage first.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpTrackingRow {
    /// Hex-encoded work package hash
    pub wp_hash: String,
    /// When this work package was first reported by any node
    pub first_seen: DateTime<Utc>,
    /// When the most recent pipeline event for this work package arrived
    pub last_updated: DateTime<Utc>,
    /// Furthest pipeline stage reached: 0 received, 1 authorized, 2 refined,
    /// 3 work report built, 4 guarantee built, 5 guarantees distributed
    pub stage: i16,
    /// How many distinct guarantors reported WorkPackageReceived(94) for it
    pub received_by: i16,
    /// How many distinct guarantors reported GuaranteeBuilt(105) for it
    pub guaranteed_by: i16,
    /// Services whose work items this work package carries (hex-formatted)
    pub service_ids: Vec<DbServiceId>,
    /// When the first guarantor reported WorkPackageReceived(94)
    pub received_at: Option<DateTime<Utc>>,
    /// When the first guarantor reported Authorized(95)
    pub authorized_at: Option<DateTime<Utc>>,
    /// When the first guarantor reported Refined(101)
    pub refined_at: Option<DateTime<Utc>>,
    /// When the first guarantor reported WorkReportBuilt(102)
    pub report_built_at: Option<DateTime<Utc>>,
    /// When the primary guarantor reported GuaranteeBuilt(105)
    pub guarantee_built_at: Option<DateTime<Utc>>,
    /// When the primary guarantor reported GuaranteesDistributed(109), having
    /// finished sending the guarantee to the other validators
    pub distributed_at: Option<DateTime<Utc>>,
    /// When WorkPackageFailed(92) was reported (null if no failure was reported)
    pub failed_at: Option<DateTime<Utc>>,
}

// ── /api/grafana/blocks/convergence ─────────────────────────────────────

/// How one step of a block's lifecycle spread across the network for one slot.
///
/// All offsets are measured from Authored(42) on the block's author to the same
/// slot's event on each reporting node, pooled over nodes.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct BlockConvergenceRow {
    /// Slot of the block these offsets refer to
    pub slot: i32,
    /// JIP-3 event id whose spread this row measures — BestBlockChanged(11),
    /// FinalizedBlockChanged(12), Authoring(40), Authored(42) or Importing(43)
    pub event_type: i16,
    /// Canonical JIP-3 name of that event
    #[sqlx(skip)]
    pub event_type_name: &'static str,
    /// How many node reports the percentiles were computed from
    pub node_count: i16,
    /// Median offset from Authored(42) on the author to this event on the
    /// reporting nodes, in milliseconds
    pub p50_ms: i32,
    /// 75th-percentile offset in milliseconds
    pub p75_ms: Option<i32>,
    /// 95th-percentile offset in milliseconds
    pub p95_ms: Option<i32>,
    /// 99th-percentile offset in milliseconds
    pub p99_ms: i32,
    /// Largest offset in milliseconds — the slowest reporting node
    pub p100_ms: i32,
    /// When the author reported Authored(42); the reference point for all offsets
    pub authored_at: DateTime<Utc>,
}

// ── /api/grafana/blocks/contents ────────────────────────────────────────

/// The contents of one authored block, as reported in the block outline of
/// Authored(42).
///
/// Every count is null if the author's report did not carry that field.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct BlockContentsRow {
    /// Slot the block was authored for
    pub slot: i32,
    /// When the author reported Authored(42)
    pub timestamp: DateTime<Utc>,
    /// Node that authored the block
    pub node_id: String,
    /// Guarantees included in the block
    pub num_guarantees: Option<i32>,
    /// Availability assurances included in the block
    pub num_assurances: Option<i32>,
    /// Preimages included in the block
    pub num_preimages: Option<i32>,
    /// Safrole tickets included in the block
    pub num_tickets: Option<i32>,
    /// Dispute verdicts included in the block
    pub num_disputes: Option<i32>,
    /// Size of the block in bytes, as reported in the block outline
    pub extrinsic_size: Option<i32>,
}

// ── /api/grafana/services ───────────────────────────────────────────────

/// One service's activity and gas usage over the whole queried range.
///
/// Every counter counts event attributions rather than distinct work packages
/// or blocks: each guarantor and each block-executing node reports for itself,
/// so the same work item or accumulated service is counted once per reporting
/// node.
#[derive(Debug, Serialize, ToSchema)]
pub struct ServiceRow {
    /// Service ID (hex-formatted, e.g. "0x000000ff")
    pub service_id: DbServiceId,
    /// WorkPackageReceived(94) reports naming this service
    pub work_packages: i64,
    /// Work items of this service refined, from Refined(101)
    pub refinements: i64,
    /// Gas used by those refine calls
    pub refinement_gas: i64,
    /// Authorized(95) reports naming this service
    pub authorizations: i64,
    /// Gas used by the is-authorized calls of Authorized(95). The cost is
    /// reported once per work package and booked to the first service of the
    /// package, so it is not spread across the package's other services.
    pub authorization_gas: i64,
    /// Times this service was accumulated, from the per-service accumulate
    /// entries of BlockExecuted(47)
    pub executions: i64,
    /// Gas used by those accumulate calls
    pub execution_gas: i64,
}

// ── /api/grafana/services/timeseries ────────────────────────────────────

/// One service's activity and gas usage inside one time bucket.
///
/// Same per-service attribution as `ServiceRow`, restricted to the bucket.
#[derive(Debug, Serialize, ToSchema)]
pub struct ServiceTimeseriesRow {
    /// Start of the time bucket
    pub ts: DateTime<Utc>,
    /// Service ID (hex-formatted, same encoding as ServiceRow)
    pub service_id: DbServiceId,
    /// WorkPackageReceived(94) reports naming this service in the bucket
    pub work_packages: i64,
    /// Gas used by the service's is-authorized calls, from Authorized(95)
    pub authorization_gas: i64,
    /// Gas used by the service's refine calls, from Refined(101)
    pub refinement_gas: i64,
    /// Gas used by the service's accumulate calls, from BlockExecuted(47)
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

/// Stage-by-stage latency of the guarantor work-package pipeline over a time range.
///
/// Covers the work packages first observed in the range that reached at least
/// WorkPackageReceived(94). Stage latencies are percentiles of the per-work-package
/// durations between consecutive pipeline events, so a slow stage stands out
/// directly.
#[derive(Debug, Serialize, ToSchema)]
pub struct BottlenecksResponse {
    /// Latency percentiles for each pipeline stage
    pub stage_timing: StageTiming,
    /// Share of the work packages for which WorkPackageFailed(92) was reported (0.0 to 1.0)
    pub failure_rate: f64,
    /// Work packages the percentiles were taken over
    pub total_wps: i64,
    /// How many of them reported WorkPackageFailed(92)
    pub failed_wps: i64,
    /// Mean reception-to-distribution duration in milliseconds
    pub avg_pipeline_ms: Option<f64>,
}

/// Median and 95th-percentile duration of one pipeline stage.
#[derive(Debug, Serialize, ToSchema)]
pub struct Percentiles {
    /// 50th percentile (median) in milliseconds
    pub p50_ms: Option<f64>,
    /// 95th percentile in milliseconds
    pub p95_ms: Option<f64>,
}

/// Latency of every stage of the guarantor work-package pipeline, in milliseconds.
#[derive(Debug, Serialize, ToSchema)]
pub struct StageTiming {
    /// WorkPackageReceived(94) → Authorized(95)
    pub authorize: Percentiles,
    /// Authorized(95) → Refined(101)
    pub refine: Percentiles,
    /// Refined(101) → WorkReportBuilt(102)
    pub report: Percentiles,
    /// WorkReportBuilt(102) → GuaranteeBuilt(105)
    pub guarantee: Percentiles,
    /// GuaranteeBuilt(105) → GuaranteesDistributed(109)
    pub distribute: Percentiles,
    /// WorkPackageReceived(94) → GuaranteesDistributed(109), or to the last
    /// pipeline event seen for work packages that never got distributed
    pub pipeline_total: Percentiles,
}

// ── /api/grafana/wp-funnel ──────────────────────────────────────────────

/// How many work packages reached each stage of the guarantor pipeline.
///
/// Counted over the work packages first observed in the time range. A work package
/// counts towards a stage once any of its guarantors reported the corresponding
/// event, so the drop between two consecutive counts is the number that stopped
/// progressing there.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct WpFunnelResponse {
    /// Work packages first observed in the range
    pub total: i64,
    /// Reached reception — WorkPackageReceived(94)
    pub received: i64,
    /// Passed the authorization check — Authorized(95)
    pub authorized: i64,
    /// Were refined — Refined(101)
    pub refined: i64,
    /// Had a work report built — WorkReportBuilt(102)
    pub report_built: i64,
    /// Had a guarantee built — GuaranteeBuilt(105)
    pub guarantee_built: i64,
    /// Had their guarantee distributed to the other validators — GuaranteesDistributed(109)
    pub distributed: i64,
    /// Failed at any point in the pipeline — WorkPackageFailed(92)
    pub failed: i64,
}

// ── /api/grafana/guarantee-convergence ───────────────────────────────────

/// Guarantee propagation for one slot, pooled across every guarantee built in it.
///
/// Percentiles are taken over the individual GuaranteeBuilt(105) →
/// GuaranteeReceived(112) latencies of all guarantees in the slot, so they are
/// true cross-core percentiles rather than an average of per-guarantee ones.
#[derive(Debug, Serialize, ToSchema)]
pub struct GuaranteeConvergenceSlotRow {
    /// Slot the guarantees were built in
    pub slot: i32,
    /// Wall-clock start of the slot, derived from the slot number
    pub slot_timestamp: DateTime<Utc>,
    /// Guarantees built in this slot that were seen received by at least one validator
    pub guarantee_count: i16,
    /// Fewest receiving validators any one guarantee in this slot reached — the
    /// worst-covered guarantee, not the slot average
    pub node_count: i16,
    /// Median milliseconds from GuaranteeBuilt(105) to GuaranteeReceived(112)
    pub p50_ms: Option<i32>,
    /// 75th-percentile milliseconds from GuaranteeBuilt(105) to GuaranteeReceived(112)
    pub p75_ms: Option<i32>,
    /// 95th-percentile milliseconds from GuaranteeBuilt(105) to GuaranteeReceived(112)
    pub p95_ms: Option<i32>,
    /// 99th-percentile milliseconds from GuaranteeBuilt(105) to GuaranteeReceived(112)
    pub p99_ms: Option<i32>,
    /// Slowest observed milliseconds from GuaranteeBuilt(105) to GuaranteeReceived(112)
    pub p100_ms: Option<i32>,
    /// Earliest GuaranteeBuilt(105) time among the guarantees in this slot
    pub built_at: DateTime<Utc>,
}

// ── /api/grafana/guarantee-convergence/detail ────────────────────────────

/// Propagation of one work report's guarantee across the validators that received it.
///
/// Percentiles are taken over the GuaranteeBuilt(105) → GuaranteeReceived(112)
/// latencies reported by each receiving validator for this one work report.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct GuaranteeConvergenceDetailRow {
    /// Hash of the guaranteed work report (hex-encoded)
    pub work_report_hash: String,
    /// Slot the guarantee was built in
    pub slot: i32,
    /// Core the work report was built for; null when the guarantor is not
    /// reporting telemetry, so its core could not be attributed
    pub core: Option<i16>,
    /// Hash of the work package behind the report (hex-encoded); null when the
    /// guarantor is not reporting telemetry
    pub wp_hash: Option<String>,
    /// Node that emitted GuaranteeBuilt(105) for this report; null when that
    /// guarantor is not reporting telemetry
    pub builder_node_id: Option<String>,
    /// Validators that reported GuaranteeReceived(112) for this report
    pub node_count: i16,
    /// Median milliseconds from GuaranteeBuilt(105) to GuaranteeReceived(112)
    pub p50_ms: i32,
    /// 75th-percentile milliseconds from GuaranteeBuilt(105) to GuaranteeReceived(112)
    pub p75_ms: Option<i32>,
    /// 95th-percentile milliseconds from GuaranteeBuilt(105) to GuaranteeReceived(112)
    pub p95_ms: Option<i32>,
    /// 99th-percentile milliseconds from GuaranteeBuilt(105) to GuaranteeReceived(112)
    pub p99_ms: i32,
    /// Slowest observed milliseconds from GuaranteeBuilt(105) to GuaranteeReceived(112)
    pub p100_ms: i32,
    /// When the guarantor emitted GuaranteeBuilt(105)
    pub built_at: DateTime<Utc>,
}

// ── /api/grafana/assurance-convergence ───────────────────────────────────

/// One assurance anchor: how quickly assurances for that block reached the
/// validator set.
///
/// The reception percentiles pool the DistributingAssurance(126) →
/// AssuranceReceived(131) latencies of every sender for this anchor (negative
/// deltas from clock skew are treated as zero). The `dist_start_*` percentiles
/// describe something different: when validators *started* distributing, relative
/// to the first validator to do so for this anchor.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct AssuranceConvergenceRow {
    /// Assurance anchor — hex-encoded header hash of the block the availability statement refers to
    pub anchor: String,
    /// Slot of the anchored block, when known
    pub slot: Option<i32>,
    /// Wall-clock start of that slot, for plotting on a time axis
    pub slot_timestamp: Option<DateTime<Utc>>,
    /// Validators seen distributing an assurance for this anchor
    pub sender_count: i16,
    /// Reception measurements pooled here — one per receiving validator per sender
    pub receiver_count: i32,
    /// Median milliseconds from DistributingAssurance(126) to AssuranceReceived(131)
    pub p50_ms: i32,
    /// 75th-percentile milliseconds from DistributingAssurance(126) to AssuranceReceived(131)
    pub p75_ms: Option<i32>,
    /// 95th-percentile milliseconds from DistributingAssurance(126) to AssuranceReceived(131)
    pub p95_ms: Option<i32>,
    /// 99th-percentile milliseconds from DistributingAssurance(126) to AssuranceReceived(131)
    pub p99_ms: i32,
    /// Slowest observed milliseconds from DistributingAssurance(126) to AssuranceReceived(131)
    pub p100_ms: i32,
    /// Median milliseconds by which a validator started distributing later than the first one to do so
    pub dist_start_p50_ms: Option<i32>,
    /// 95th-percentile lateness in starting to distribute, in milliseconds
    pub dist_start_p95_ms: Option<i32>,
    /// 99th-percentile lateness in starting to distribute, in milliseconds
    pub dist_start_p99_ms: Option<i32>,
    /// Lateness of the last validator to start distributing, in milliseconds
    pub dist_start_p100_ms: Option<i32>,
    /// When the first validator emitted DistributingAssurance(126) for this anchor
    pub first_distributed_at: Option<DateTime<Utc>>,
    /// When the last validator emitted DistributingAssurance(126) for this anchor
    pub last_distributed_at: Option<DateTime<Utc>>,
}

// ── /api/grafana/assurance-convergence/senders ──────────────────────────

/// One (anchor, sender) pair: how quickly a single validator's assurance reached
/// the validators that received it.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct AssuranceConvergenceSenderRow {
    /// Assurance anchor — hex-encoded header hash of the block the availability statement refers to
    pub anchor: String,
    /// Slot of the anchored block, when known
    pub slot: Option<i32>,
    /// Validator that emitted DistributingAssurance(126) for this anchor
    pub sender_node_id: String,
    /// Validators whose AssuranceReceived(131) was matched to this sender's assurance
    pub node_count: i16,
    /// Median milliseconds from this sender's DistributingAssurance(126) to AssuranceReceived(131)
    pub p50_ms: i32,
    /// 75th-percentile milliseconds until AssuranceReceived(131) on the receiving validators
    pub p75_ms: Option<i32>,
    /// 95th-percentile milliseconds until AssuranceReceived(131) on the receiving validators
    pub p95_ms: Option<i32>,
    /// 99th-percentile milliseconds until AssuranceReceived(131) on the receiving validators
    pub p99_ms: i32,
    /// Slowest observed milliseconds until AssuranceReceived(131) on a receiving validator
    pub p100_ms: i32,
    /// When this sender emitted DistributingAssurance(126) for this anchor
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

/// One node's data-availability activity over the requested time range.
///
/// Counts are totals over the range. The two average latencies are weighted by
/// sample count and include requests that ended in ShardRequestFailed(122),
/// measured up to the failure.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct DaStatsRow {
    /// Node that reported the events
    pub node_id: String,
    /// Shard requests this node issued as an assurer — SendingShardRequest(120)
    pub shard_requests_sent: i64,
    /// Shard requests this node was asked to serve — ReceivingShardRequest(121)
    pub shard_requests_received: i64,
    /// Shard requests fully sent to the guarantor — ShardRequestSent(123)
    pub shard_sent_confirmed: i64,
    /// Shard requests fully taken in as a guarantor — ShardRequestReceived(124)
    pub shard_received_confirmed: i64,
    /// Completed shard transfers — ShardsTransferred(125)
    pub shards_transferred: i64,
    /// Shard requests that failed — ShardRequestFailed(122)
    pub shard_failures: i64,
    /// Preimage announcements that failed — PreimageAnnouncementFailed(190)
    pub preimage_ann_failures: i64,
    /// Preimages announced to peers — PreimageAnnounced(191)
    pub preimages_announced: i64,
    /// Announced preimages later dropped — AnnouncedPreimageForgotten(192)
    pub preimages_forgotten: i64,
    /// Mean milliseconds from SendingShardRequest(120) to ShardsTransferred(125)
    pub assurer_avg_latency_ms: Option<f32>,
    /// Measurements behind the assurer-side average
    pub assurer_latency_samples: i64,
    /// Mean milliseconds from ReceivingShardRequest(121) to ShardRequestReceived(124)
    pub guarantor_avg_latency_ms: Option<f32>,
    /// Measurements behind the guarantor-side average
    pub guarantor_latency_samples: i64,
    /// Peak number of distinct shard indices this node served within a single sampling window
    pub active_shards: i32,
}

// ── /api/grafana/shard-latency ──────────────────────────────────────────

/// Shard transfer latency percentiles for one time bucket, pooled across all
/// reporting nodes. All percentiles are milliseconds.
///
/// The `assurer_*` fields measure the requesting side end to end,
/// SendingShardRequest(120) → ShardsTransferred(125); the `guarantor_*` fields
/// measure how long the serving side took to take the request in,
/// ReceivingShardRequest(121) → ShardRequestReceived(124). Values are
/// approximate: they are rounded up to a latency-bucket edge and saturate at 5 s.
/// Requests that ended in ShardRequestFailed(122) are part of the percentiles,
/// measured up to the failure, and `failed_count` says how many of the pooled
/// measurements those were.
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

/// One time bucket of the work-package pipeline funnel.
///
/// Each work package is counted in the bucket in which it was first observed, with
/// all the pipeline stages it eventually reached, so its later stages land in that
/// same bucket even when they happened afterwards.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct WpFunnelTimeseriesRow {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Work packages first observed in this bucket
    pub total: i64,
    /// Reached reception — WorkPackageReceived(94)
    pub received: i64,
    /// Passed the authorization check — Authorized(95)
    pub authorized: i64,
    /// Were refined — Refined(101)
    pub refined: i64,
    /// Had a work report built — WorkReportBuilt(102)
    pub report_built: i64,
    /// Had a guarantee built — GuaranteeBuilt(105)
    pub guarantee_built: i64,
    /// Had their guarantee distributed to the other validators — GuaranteesDistributed(109)
    pub distributed: i64,
    /// Failed at any point in the pipeline — WorkPackageFailed(92)
    pub failed: i64,
}

// ── /api/grafana/bottlenecks-timeseries ─────────────────────────────────

/// One time bucket of stage-by-stage work-package pipeline latency.
///
/// Each work package is attributed to the bucket in which it was first observed,
/// and only those that reached WorkPackageReceived(94) are counted. A stage's
/// percentiles are null when no work package in the bucket reached that stage.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct BottlenecksTimeseriesRow {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// WorkPackageReceived(94) → Authorized(95), p50 in milliseconds
    pub authorize_p50: Option<f64>,
    /// WorkPackageReceived(94) → Authorized(95), p95 in milliseconds
    pub authorize_p95: Option<f64>,
    /// Authorized(95) → Refined(101), p50 in milliseconds
    pub refine_p50: Option<f64>,
    /// Authorized(95) → Refined(101), p95 in milliseconds
    pub refine_p95: Option<f64>,
    /// Refined(101) → WorkReportBuilt(102), p50 in milliseconds
    pub report_p50: Option<f64>,
    /// Refined(101) → WorkReportBuilt(102), p95 in milliseconds
    pub report_p95: Option<f64>,
    /// WorkReportBuilt(102) → GuaranteeBuilt(105), p50 in milliseconds
    pub guarantee_p50: Option<f64>,
    /// WorkReportBuilt(102) → GuaranteeBuilt(105), p95 in milliseconds
    pub guarantee_p95: Option<f64>,
    /// GuaranteeBuilt(105) → GuaranteesDistributed(109), p50 in milliseconds
    pub distribute_p50: Option<f64>,
    /// GuaranteeBuilt(105) → GuaranteesDistributed(109), p95 in milliseconds
    pub distribute_p95: Option<f64>,
    /// Reception to distribution (or to the last pipeline event seen for work
    /// packages that never got distributed), p50 in milliseconds
    pub pipeline_p50: Option<f64>,
    /// Reception to distribution (or to the last pipeline event seen for work
    /// packages that never got distributed), p95 in milliseconds
    pub pipeline_p95: Option<f64>,
    /// Work packages in this bucket that reached WorkPackageReceived(94)
    pub total_wps: i64,
    /// How many of them reported WorkPackageFailed(92)
    pub failed_wps: i64,
}

// ── /api/grafana/validator-profiling ────────────────────────────────────

/// Work-package pipeline performance of every guarantor, with the network-wide
/// baseline to compare each of them against.
#[derive(Debug, Serialize, ToSchema)]
pub struct ValidatorProfilingResponse {
    /// Unweighted mean of the per-guarantor `avg_total_ms` values, in milliseconds
    /// — the baseline `slowdown_factor` is measured against. Null when no work
    /// package reached distribution in the range.
    pub network_avg_total_ms: Option<f64>,
    /// One row per guarantor, sorted by `avg_total_ms` (slowest first by default,
    /// fastest first with `sort=asc`), guarantors with no distributed work package
    /// last. `limit` truncates this list only; `network_avg_total_ms` still covers
    /// every guarantor.
    pub nodes: Vec<ValidatorProfilingRow>,
}

/// One guarantor's average progress through the work-package pipeline.
///
/// The work packages counted here are those attributed to this guarantor — it was
/// the first to report WorkPackageReceived(94) for them — that finished, meaning
/// they reached GuaranteesDistributed(109) or WorkPackageFailed(92).
#[derive(Debug, Serialize, ToSchema)]
pub struct ValidatorProfilingRow {
    /// Node identifier (hex-encoded 32-byte public key)
    pub node_id: String,
    /// Finished work packages attributed to this guarantor in the range
    pub wp_count: i64,
    /// How many of them reported WorkPackageFailed(92)
    pub failures: i64,
    /// `failures` / `wp_count`, between 0.0 and 1.0
    pub failure_rate: f64,
    /// Average WorkPackageReceived(94) → Authorized(95) duration in milliseconds,
    /// over the work packages that reached distribution
    pub avg_authorize_ms: Option<f64>,
    /// Average Authorized(95) → Refined(101) duration in milliseconds
    pub avg_refine_ms: Option<f64>,
    /// Average Refined(101) → WorkReportBuilt(102) duration in milliseconds
    pub avg_report_ms: Option<f64>,
    /// Average WorkReportBuilt(102) → GuaranteeBuilt(105) duration in milliseconds
    pub avg_guarantee_ms: Option<f64>,
    /// Average GuaranteeBuilt(105) → GuaranteesDistributed(109) duration in
    /// milliseconds
    pub avg_distribute_ms: Option<f64>,
    /// Average WorkPackageReceived(94) → GuaranteesDistributed(109) duration in
    /// milliseconds. Null when none of this guarantor's work packages reached
    /// distribution — failed ones do not contribute.
    pub avg_total_ms: Option<f64>,
    /// This guarantor's `avg_total_ms` divided by `network_avg_total_ms`; above
    /// roughly 1.5 the guarantor is an outlier
    pub slowdown_factor: Option<f64>,
}

/// One guarantor's average pipeline progress within one time bucket.
///
/// Work packages are placed in the bucket in which they were first observed, and
/// attributed to the guarantor that first reported WorkPackageReceived(94) for
/// them.
#[derive(Debug, Serialize, ToSchema)]
pub struct ValidatorProfilingTimeseriesRow {
    /// Start of the time bucket
    pub ts: DateTime<Utc>,
    /// Node identifier (hex-encoded 32-byte public key)
    pub node_id: String,
    /// Work packages attributed to this guarantor in this bucket, including ones
    /// still in flight
    pub wp_count: i64,
    /// How many of them reported WorkPackageFailed(92)
    pub failures: i64,
    /// Average WorkPackageReceived(94) → Authorized(95) duration in milliseconds,
    /// over the bucket's work packages that reached distribution
    pub avg_authorize_ms: Option<f64>,
    /// Average Authorized(95) → Refined(101) duration in milliseconds
    pub avg_refine_ms: Option<f64>,
    /// Average Refined(101) → WorkReportBuilt(102) duration in milliseconds
    pub avg_report_ms: Option<f64>,
    /// Average WorkReportBuilt(102) → GuaranteeBuilt(105) duration in milliseconds
    pub avg_guarantee_ms: Option<f64>,
    /// Average GuaranteeBuilt(105) → GuaranteesDistributed(109) duration in
    /// milliseconds
    pub avg_distribute_ms: Option<f64>,
    /// Average WorkPackageReceived(94) → GuaranteesDistributed(109) duration in
    /// milliseconds. Null in buckets where none of this guarantor's work packages
    /// reached distribution.
    pub avg_total_ms: Option<f64>,
}

// ── /api/grafana/guarantee-discards ──────────────────────────────────────

/// GuaranteeDiscarded(113) events for one time bucket and one discard reason.
#[derive(Debug, Serialize, ToSchema)]
pub struct GuaranteeDiscardRow {
    /// Start of the time bucket
    pub ts: DateTime<Utc>,
    /// JIP-3 discard reason, name and code (e.g. "ReplacedByBetter(1)",
    /// "TooManyGuarantees(3)")
    pub reason: String,
    /// Guarantees discarded for this reason within this bucket, across all validators
    pub count: i64,
}

// ── /api/grafana/events ─────────────────────────────────────────────────

/// One raw telemetry event exactly as a node reported it.
///
/// Every JIP-3 event type can appear here, and the payload carries the fields
/// that JIP-3 defines for that type. Raw events are retained for about an hour,
/// so only recent activity can be browsed this way.
#[derive(Debug, Serialize, ToSchema)]
pub struct EventRow {
    /// When the event occurred on the reporting node
    pub ts: DateTime<Utc>,
    /// Node that reported this event
    pub node_id: String,
    /// Event type code as defined in JIP-3
    pub event_type: i16,
    /// Full event payload; its fields depend on the event type
    pub data: serde_json::Value,
    /// When the event reached the telemetry backend
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

/// One time bucket's view of how much of the network was at the chain tip,
/// derived from the best-block slots nodes reported in BestBlockChanged(11).
#[derive(Debug, Serialize, ToSchema)]
pub struct SyncTimelineRow {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Nodes that reported BestBlockChanged(11) in this bucket
    pub total_nodes: i64,
    /// Nodes whose best slot was within 2 slots of the network's highest
    pub synced_nodes: i64,
    /// Nodes whose best slot was more than 2 slots behind the network's highest
    pub behind_nodes: i64,
    /// Share of reporting nodes that were synced, in percent (0.0 to 100.0)
    pub sync_percentage: f64,
    /// Highest best-block slot any node reported in this bucket
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

/// Network-wide guaranteeing activity over the requested time range.
#[derive(Debug, Serialize, ToSchema)]
pub struct GuaranteesResponse {
    /// One count per guaranteeing event type
    pub totals: GuaranteeTotals,
    /// Fraction of guarantee transfers that succeeded on each side
    pub success_rates: GuaranteeSuccessRates,
}

/// Counts of each guaranteeing event across all reporting nodes.
#[derive(Debug, Serialize, ToSchema)]
pub struct GuaranteeTotals {
    /// GuaranteeBuilt(105) — a primary guarantor assembled a guarantee for a work report
    pub built: i64,
    /// SendingGuarantee(106) — a guarantor started sending a guarantee to another validator
    pub sending: i64,
    /// GuaranteeSendFailed(107) — sending a guarantee to another validator failed
    pub send_failed: i64,
    /// GuaranteeSent(108) — a guarantee was successfully sent to another validator
    pub sent: i64,
    /// GuaranteesDistributed(109) — a primary guarantor finished distributing a
    /// work report's guarantees, successfully or not
    pub distributed: i64,
    /// ReceivingGuarantee(110) — a validator started receiving a guarantee
    pub receiving: i64,
    /// GuaranteeReceiveFailed(111) — receiving a guarantee failed
    pub receive_failed: i64,
    /// GuaranteeReceived(112) — a guarantee was fully received, before validity checks
    pub received: i64,
    /// GuaranteeDiscarded(113) — a guarantee was dropped from a validator's local pool
    pub discarded: i64,
}

/// Share of guarantee transfers that completed, on each side of the transfer.
#[derive(Debug, Serialize, ToSchema)]
pub struct GuaranteeSuccessRates {
    /// GuaranteeSent(108) as a fraction of all send attempts (106 + 107 + 108).
    /// 1.0 when nothing was sent in the range.
    pub send_success_rate: f64,
    /// GuaranteeReceived(112) as a fraction of all receive attempts (110 + 111 + 112).
    /// 1.0 when nothing was received in the range.
    pub receive_success_rate: f64,
}

// ── /api/grafana/guarantees/by-guarantor ────────────────────────────────

/// The set of nodes seen building guarantees in the requested time range.
///
/// The core association per node is observed guaranteeing behaviour, not the
/// protocol's validator→core assignment, which rotates every 10 slots and
/// reshuffles each epoch.
#[derive(Debug, Serialize, ToSchema)]
pub struct GuarantorBreakdownResponse {
    /// One row per guarantor node, most guarantees built first
    pub guarantors: Vec<GuarantorRow>,
    /// Number of rows in `guarantors`
    pub total_guarantors: i64,
}

/// One node's guaranteeing activity and the cores it was seen guaranteeing for.
#[derive(Debug, Serialize, ToSchema)]
pub struct GuarantorRow {
    /// Node identifier (Ed25519 public key, hex-encoded)
    pub node_id: String,
    /// Lowest core index in `cores_active`, used as a stable label for the node
    pub primary_core: Option<i16>,
    /// GuaranteeBuilt(105) events this node emitted in the time range
    pub guarantee_count: i64,
    /// When this node last emitted GuaranteeBuilt(105)
    pub last_guarantee: Option<DateTime<Utc>>,
    /// Every core this node built a guarantee for, ascending and deduplicated
    pub cores_active: Vec<i16>,
}

// ── /api/grafana/wp-stats ───────────────────────────────────────────────

/// Work-package traffic and pipeline progress over a time range, plus its spread
/// across cores.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpStatsResponse {
    pub totals: WpStageTotals,
    pub by_core: Vec<WpCoreCount>,
}

/// Work-package counts before and along the guarantor pipeline.
///
/// The first three are counts of events reported by all nodes, not of distinct work
/// packages; the rest count distinct work packages that reached each stage.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpStageTotals {
    /// WorkPackageSubmission(90) events — builders opening a submission stream to a guarantor
    pub submissions: i64,
    /// WorkPackageBeingShared(91) events — secondary guarantors accepting a share from a primary
    pub being_shared: i64,
    /// DuplicateWorkPackage(93) events — a work package already seen was offered again;
    /// it is reported instead of a reception, so it never enters the stage counts below
    pub duplicates: i64,
    /// Work packages that reached reception — WorkPackageReceived(94)
    pub received: i64,
    /// Passed the authorization check — Authorized(95)
    pub authorized: i64,
    /// Were refined — Refined(101)
    pub refined: i64,
    /// Had a work report built — WorkReportBuilt(102)
    pub report_built: i64,
    /// Had a guarantee built — GuaranteeBuilt(105)
    pub guarantee_built: i64,
    /// Had their guarantee distributed to the other validators — GuaranteesDistributed(109)
    pub distributed: i64,
    /// Failed at any point in the pipeline — WorkPackageFailed(92)
    pub failed: i64,
}

/// Work packages assigned to one core.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpCoreCount {
    /// Core index
    pub core: i16,
    /// Work packages first observed in the time range on this core
    pub count: i64,
}

// ── /api/grafana/validators/cores ───────────────────────────────────────

/// One guaranteeing node and a core it was seen guaranteeing for.
///
/// The association is observed from GuaranteeBuilt(105) reports, not the
/// protocol's validator→core assignment, which telemetry does not carry — JIP-3
/// events identify a node by its public key and never by validator index.
#[derive(Debug, Serialize, ToSchema)]
pub struct ValidatorCoreRow {
    /// Node identifier (Ed25519 public key hex)
    pub node_id: String,
    /// A core this node built guarantees for in the range. Only one core is
    /// reported even when the node guaranteed for several, so treat it as
    /// indicative unless the range is shorter than one 10-slot core rotation. Null
    /// when no core could be determined.
    pub primary_core: Option<i16>,
    /// Guarantees this node built in the range, across all cores
    pub guarantee_count: i64,
}

// ── /api/grafana/cores/{id}/validators ───────────────────────────────────

/// The nodes observed guaranteeing for one core.
///
/// Membership comes from GuaranteeBuilt(105) reports, so a validator assigned to
/// the core that built no guarantee in the range is absent; this is the observed
/// guarantor set, not the protocol's validator→core assignment.
#[derive(Debug, Serialize, ToSchema)]
pub struct CoreValidatorsResponse {
    /// Core index
    pub core: i16,
    /// One row per node that built guarantees for this core, most guarantees first
    pub validators: Vec<CoreValidatorRow>,
    /// Number of rows in `validators`
    pub total_active: i64,
}

/// One node's guaranteeing activity for a single core, with its node metadata.
#[derive(Debug, Serialize, ToSchema)]
pub struct CoreValidatorRow {
    /// Node identifier (Ed25519 public key hex)
    pub node_id: String,
    /// GuaranteeBuilt(105) reports from this node for this core in the range
    pub guarantee_count: i64,
    /// When this node last built a guarantee for this core
    pub last_guarantee: Option<DateTime<Utc>>,
    /// Node implementation name as announced at handshake (e.g. "polkajam"). Null
    /// for a node that has not connected to telemetry.
    pub implementation_name: Option<String>,
    /// Node implementation version as announced at handshake
    pub implementation_version: Option<String>,
    /// Whether the node's telemetry connection is open right now
    pub is_connected: Option<bool>,
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

// ── Phase 4 response types ──────────────────────────────────────────────

// ── /api/grafana/wp-active ──────────────────────────────────────────────

/// Recent work packages plus pipeline health for the whole time range.
///
/// The listed work packages are only the most recent ones; every summary beside
/// them covers all work packages first observed in the range, whether they are
/// still progressing, distributed or failed. The telemetry-visible pipeline ends at
/// GuaranteesDistributed(109) — what happens to the work report on chain afterwards
/// is not part of these stages.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpActiveResponse {
    /// The most recently started work packages of the range (at most 200, newest first)
    pub work_packages: Vec<WpActiveRow>,
    /// How many work packages stopped at each stage
    pub summary: WpActiveSummary,
    /// How many work packages ever reached each stage
    pub reached: WpReachedCounts,
    /// Median and p95 duration of each pipeline stage, in milliseconds
    pub stage_duration_percentiles: WpStageDurations,
    /// Failures grouped by the reason reported in WorkPackageFailed(92)
    pub failure_breakdown: Vec<FailureBreakdownEntry>,
}

/// One work package and how far it got through the guarantor pipeline.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpActiveRow {
    /// Hex-encoded work package hash
    pub wp_hash: String,
    /// Core this work package was submitted for
    pub core: i16,
    /// First guarantor that reported WorkPackageReceived(94) for it
    pub node_id: Option<String>,
    /// Services whose work items this work package carries (hex-formatted)
    pub service_ids: Vec<DbServiceId>,
    /// Furthest pipeline stage reached: 0 received, 1 authorized, 2 refined,
    /// 3 work report built, 4 guarantee built, 5 guarantees distributed
    pub stage: i16,
    /// Gas the work items consumed during refinement, as reported by Refined(101)
    pub refine_gas_used: Option<i64>,
    /// Reason reported in WorkPackageFailed(92), if it failed
    pub failure_reason: Option<String>,
    /// When this work package was first reported by any node
    pub first_seen: DateTime<Utc>,
    /// When the most recent pipeline event for it arrived
    pub last_updated: DateTime<Utc>,
    /// Pipeline stage timestamps: reception, authorization, refinement, work report,
    /// guarantee, distribution and failure (null = the stage was never reported)
    pub received_at: Option<DateTime<Utc>>,
    pub authorized_at: Option<DateTime<Utc>>,
    pub refined_at: Option<DateTime<Utc>>,
    pub report_built_at: Option<DateTime<Utc>>,
    pub guarantee_built_at: Option<DateTime<Utc>>,
    pub distributed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    /// How many distinct guarantors reported WorkPackageReceived(94) for it
    pub received_by: i16,
    /// How many distinct guarantors reported GuaranteeBuilt(105) for it
    pub guaranteed_by: i16,
    /// Milliseconds from the first to the most recent pipeline event for it
    pub elapsed_ms: f64,
}

/// Where work packages stopped: counts of those whose furthest reached stage is
/// each stage, so anything still progressing or stuck shows up here while
/// distributed ones do not.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpActiveSummary {
    /// Work packages first observed in the range
    pub total: i64,
    /// Got no further than reception — WorkPackageReceived(94)
    pub at_received: i64,
    /// Got no further than authorization — Authorized(95)
    pub at_authorized: i64,
    /// Got no further than refinement — Refined(101)
    pub at_refined: i64,
    /// Got no further than the work report — WorkReportBuilt(102)
    pub at_report_built: i64,
    /// Got no further than the guarantee — GuaranteeBuilt(105)
    pub at_guarantee_built: i64,
}

/// How many work packages ever reached each stage; a work package counts for every
/// stage it passed, so these counts overlap.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpReachedCounts {
    /// Reached reception — WorkPackageReceived(94)
    pub received: i64,
    /// Passed the authorization check — Authorized(95)
    pub authorized: i64,
    /// Were refined — Refined(101)
    pub refined: i64,
    /// Had a work report built — WorkReportBuilt(102)
    pub report_built: i64,
    /// Had a guarantee built — GuaranteeBuilt(105)
    pub guarantee_built: i64,
    /// Had their guarantee distributed — GuaranteesDistributed(109)
    pub distributed: i64,
    /// Failed at any point in the pipeline — WorkPackageFailed(92)
    pub failed: i64,
}

/// Duration of each guarantor pipeline stage, in milliseconds.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpStageDurations {
    /// WorkPackageReceived(94) → Authorized(95), p50
    pub authorize_p50_ms: Option<f64>,
    /// WorkPackageReceived(94) → Authorized(95), p95
    pub authorize_p95_ms: Option<f64>,
    /// Authorized(95) → Refined(101), p50
    pub refine_p50_ms: Option<f64>,
    /// Authorized(95) → Refined(101), p95
    pub refine_p95_ms: Option<f64>,
    /// Refined(101) → WorkReportBuilt(102), p50
    pub report_p50_ms: Option<f64>,
    /// Refined(101) → WorkReportBuilt(102), p95
    pub report_p95_ms: Option<f64>,
    /// WorkReportBuilt(102) → GuaranteeBuilt(105), p50
    pub guarantee_p50_ms: Option<f64>,
    /// WorkReportBuilt(102) → GuaranteeBuilt(105), p95
    pub guarantee_p95_ms: Option<f64>,
}

/// One distinct failure reason and how often it occurred.
#[derive(Debug, Serialize, ToSchema)]
pub struct FailureBreakdownEntry {
    /// Reason text as reported in WorkPackageFailed(92)
    pub reason: String,
    /// Work packages that failed for this reason
    pub count: i64,
}

// ── /api/grafana/wp/{hash} ──────────────────────────────────────────────

/// Everything known about one work package: its pipeline timeline and, while they
/// are still retained, the raw telemetry events behind it.
#[derive(Debug, Serialize, ToSchema)]
pub struct WpDetailResponse {
    /// This work package's pipeline timeline; null if the hash is unknown
    pub summary: Option<WpTrackingRow>,
    /// Every raw event reported for this work package, oldest first; empty once the
    /// events have aged out of the roughly one-hour retention window
    pub events: Vec<EventRow>,
}

// ── /api/grafana/blocks/summary ─────────────────────────────────────────

/// Block production and import health over the queried range, plus the chain
/// tips as of the request.
#[derive(Debug, Serialize, ToSchema)]
pub struct BlocksSummaryResponse {
    /// Network-wide counts of the block lifecycle events in the range
    pub totals: BlockTotals,
    /// Best and finalized slot right now, not over the range
    pub chain: ChainState,
    /// The most active block authors in the range
    pub authoring_by_node: Vec<AuthoringByNode>,
}

/// Network-wide counts of each block lifecycle event in the range. The
/// import-side counts are reported once per node per block, while `authored` is
/// reported once per block by its author.
#[derive(Debug, Serialize, ToSchema)]
pub struct BlockTotals {
    /// Authoring(40) — block authoring attempts started
    pub authoring_started: i64,
    /// AuthoringFailed(41) — authoring attempts that failed
    pub authoring_failed: i64,
    /// Authored(42) — blocks successfully authored
    pub authored: i64,
    /// Importing(43) — block imports started by non-authoring nodes
    pub importing: i64,
    /// BlockVerificationFailed(44) — imported blocks that failed verification
    pub verification_failed: i64,
    /// BlockVerified(45) — imported blocks that passed verification
    pub verified: i64,
    /// BlockExecutionFailed(46) — blocks whose execution failed
    pub execution_failed: i64,
    /// BlockExecuted(47) — blocks executed successfully
    pub executed: i64,
    /// BestBlockChanged(11) — best-block changes reported by nodes
    pub best_block_changes: i64,
    /// FinalizedBlockChanged(12) — finalized-block changes reported by nodes
    pub finalized_block_changes: i64,
}

/// The chain tips as currently observed across the network.
#[derive(Debug, Serialize, ToSchema)]
pub struct ChainState {
    /// Highest best-block slot currently seen; null if live tracking is off
    pub best_slot: Option<i32>,
    /// Highest finalized slot currently seen; null if live tracking is off
    pub finalized_slot: Option<i32>,
}

/// How many blocks one node authored in the range.
#[derive(Debug, Serialize, ToSchema)]
pub struct AuthoringByNode {
    /// Node identifier
    pub node_id: String,
    /// Authored(42) reports from this node
    pub blocks_authored: i64,
}

// ── /api/grafana/cores/{id}/metrics ─────────────────────────────────────

/// One core's work-package throughput, pipeline latency and gas usage.
#[derive(Debug, Serialize, ToSchema)]
pub struct CoreMetricsResponse {
    /// Core index
    pub core: i16,
    /// Share of Refined(101) among Refined(101) plus WorkPackageFailed(92) reports
    /// on this core, as a percentage. 100 when neither was reported.
    pub processing_efficiency_pct: f64,
    /// Median WorkPackageReceived(94) → GuaranteesDistributed(109) duration in
    /// milliseconds, over the work packages first observed on this core in the
    /// range. Ones that never got distributed contribute the time up to their last
    /// observed pipeline event.
    pub p50_latency_ms: Option<f64>,
    /// The same measurement at the 95th percentile, in milliseconds
    pub p95_latency_ms: Option<f64>,
    /// Average WorkPackageReceived(94) → GuaranteesDistributed(109) duration in
    /// milliseconds, over only the work packages that reached distribution
    pub average_completion_time_ms: Option<f64>,
    /// Total refine gas reported in Refined(101) for this core's work packages
    pub total_gas_used: i64,
    /// WorkPackageReceived(94) reports for this core — one per guarantor, so higher
    /// than the number of distinct work packages
    pub work_packages_processed: i64,
}

// ── Phase 5 response types ──────────────────────────────────────────────

// ── /api/grafana/execution ──────────────────────────────────────────────

/// Gas and timing for the three phases in which service code runs, plus the
/// heaviest services in each.
///
/// The phases are the is-authorized call of a work package, the refine call of
/// each of its work items, and the accumulate call of each service touched by
/// a block.
#[derive(Debug, Serialize, ToSchema)]
pub struct ExecutionMetricsResponse {
    /// Authorization phase, as reported by Authorized(95)
    pub authorization: ExecutionPhaseStats,
    /// Refinement phase, as reported by Refined(101)
    pub refinement: ExecutionPhaseStats,
    /// Accumulation phase, as reported by BlockExecuted(47)
    pub accumulation: ExecutionPhaseStats,
    /// The 50 service-and-phase combinations with the highest total gas,
    /// heaviest first
    pub by_service: Vec<ServiceExecutionRow>,
}

/// Gas and timing summary for one execution phase.
///
/// The averages cover only the executions that came with a cost figure. For
/// authorization the cost is reported once per work package, so `count` can
/// exceed the number of executions the averages are taken over.
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct ExecutionPhaseStats {
    /// Executions reported in this phase, counted once per reporting node
    pub count: i64,
    /// Total gas used by those executions
    pub total_gas: i64,
    /// Mean gas per execution
    pub avg_gas: f64,
    /// Mean wall-clock execution time in nanoseconds
    pub avg_time_ns: f64,
    /// Mean time to load and compile the service code, in nanoseconds
    pub avg_load_ns: f64,
}

/// One service's gas and timing in one execution phase.
#[derive(Debug, Serialize, ToSchema)]
pub struct ServiceExecutionRow {
    /// Service ID as a plain signed number, not the hex encoding used
    /// elsewhere. IDs from 2^31 up therefore appear negative — including
    /// 0xffffffff, which BlockExecuted(47) uses to report the combined cost of
    /// the lowest-gas services when a block accumulates more than 500 of them.
    pub service_id: i32,
    /// Execution phase: "authorization", "refinement", or "accumulation"
    pub phase: String,
    /// Total gas used by this service in this phase
    pub total_gas: i64,
    /// Executions reported for this service in this phase, counted once per
    /// reporting node
    pub count: i64,
    /// Mean wall-clock execution time in nanoseconds
    pub avg_time_ns: f64,
    /// Mean time to load and compile the service code, in nanoseconds
    pub avg_load_ns: f64,
}

// ── /api/grafana/bundle-latency ─────────────────────────────────────────

/// Audit bundle recovery latency percentiles for one time bucket.
///
/// Tracks audit data recovery: auditors fetch erasure-coded shards from assurers
/// to reconstruct the original bundle. Six measurement sides:
/// - **shard_req**: Requestor round-trip: SendingBundleShardRequest(140) → BundleShardTransferred(145)
/// - **shard_resp**: Responder local work: ReceivingBundleShardRequest(141) → BundleShardTransferred(145)
/// - **full_req**: Requestor round-trip for full bundle: SendingBundleRequest(148) → BundleTransferred(153)
/// - **full_resp**: Responder local work for full bundle: ReceivingBundleRequest(149) → BundleTransferred(153)
/// - **reconstruct**: Local reconstruction work: ReconstructingBundle(146) → BundleReconstructed(147)
/// - **e2e**: End-to-end recovery per audit: first SendingBundleShardRequest(140) → BundleReconstructed(147)
///
/// Latencies from all reporting nodes are pooled per time bucket. Values are
/// milliseconds and approximate: rounded up to a latency-bucket edge, saturating
/// at 120 s. Measurements that ended in a failure event — BundleShardRequestFailed(142)
/// or BundleRequestFailed(150) — are included, measured up to the failure;
/// `failed_count` totals them across all sides of the bucket.
#[derive(Debug, Serialize, ToSchema)]
pub struct BundleLatencyRow {
    pub ts: DateTime<Utc>,
    pub shard_req_p50: Option<i32>,
    pub shard_req_p95: Option<i32>,
    pub shard_req_p99: Option<i32>,
    pub shard_req_samples: i32,
    pub shard_resp_p50: Option<i32>,
    pub shard_resp_p95: Option<i32>,
    pub shard_resp_p99: Option<i32>,
    pub shard_resp_samples: i32,
    pub full_req_p50: Option<i32>,
    pub full_req_p95: Option<i32>,
    pub full_req_p99: Option<i32>,
    pub full_req_samples: i32,
    pub full_resp_p50: Option<i32>,
    pub full_resp_p95: Option<i32>,
    pub full_resp_p99: Option<i32>,
    pub full_resp_samples: i32,
    pub reconstruct_p50: Option<i32>,
    pub reconstruct_p95: Option<i32>,
    pub reconstruct_p99: Option<i32>,
    pub reconstruct_samples: i32,
    pub e2e_p50: Option<i32>,
    pub e2e_p95: Option<i32>,
    pub e2e_p99: Option<i32>,
    pub e2e_p100: Option<i32>,
    pub e2e_samples: i32,
    pub failed_count: i32,
}

// ── /api/grafana/segment-latency ────────────────────────────────────────

/// Import segment fetching latency percentiles for one time bucket.
///
/// Tracks the import segments a guarantor fetches while processing a work package,
/// before refinement. Five measurement sides:
/// - **shard_req**: Requestor round-trip: SendingSegmentShardRequest(162) → SegmentShardsTransferred(167)
/// - **shard_resp**: Responder local work: ReceivingSegmentShardRequest(163) → SegmentShardsTransferred(167)
/// - **full_req**: Requestor round-trip: SendingSegmentRequest(173) → SegmentsTransferred(178)
/// - **full_resp**: Responder local work: ReceivingSegmentRequest(174) → SegmentsTransferred(178)
/// - **reconstruct**: Local reconstruction work: ReconstructingSegments(168) → SegmentsReconstructed(170)
///
/// Latencies from all reporting nodes are pooled per time bucket. Values are
/// milliseconds and approximate: rounded up to a latency-bucket edge, saturating
/// at 120 s. Measurements that ended in SegmentShardRequestFailed(164),
/// SegmentRequestFailed(175) or SegmentReconstructionFailed(169) are included,
/// measured up to the failure; `failed_count` totals them across all sides of the
/// bucket.
#[derive(Debug, Serialize, ToSchema)]
pub struct SegmentLatencyRow {
    pub ts: DateTime<Utc>,
    pub shard_req_p50: Option<i32>,
    pub shard_req_p95: Option<i32>,
    pub shard_req_p99: Option<i32>,
    pub shard_req_samples: i32,
    pub shard_resp_p50: Option<i32>,
    pub shard_resp_p95: Option<i32>,
    pub shard_resp_p99: Option<i32>,
    pub shard_resp_samples: i32,
    pub full_req_p50: Option<i32>,
    pub full_req_p95: Option<i32>,
    pub full_req_p99: Option<i32>,
    pub full_req_samples: i32,
    pub full_resp_p50: Option<i32>,
    pub full_resp_p95: Option<i32>,
    pub full_resp_p99: Option<i32>,
    pub full_resp_samples: i32,
    pub reconstruct_p50: Option<i32>,
    pub reconstruct_p95: Option<i32>,
    pub reconstruct_p99: Option<i32>,
    pub reconstruct_samples: i32,
    pub failed_count: i32,
}

// ── /api/grafana/preimage-latency ───────────────────────────────────────

/// Preimage transfer latency percentiles for one time bucket.
///
/// Tracks preimage (service blob) fetching. Two measurement sides:
/// - **req**: Requestor round-trip: SendingPreimageRequest(193) → PreimageTransferred(198)
/// - **resp**: Responder local work: ReceivingPreimageRequest(194) → PreimageTransferred(198)
///
/// Latencies from all reporting nodes are pooled per time bucket. Values are
/// milliseconds and approximate: rounded up to a latency-bucket edge, saturating
/// at 120 s. Transfers that ended in PreimageRequestFailed(195) are included,
/// measured up to the failure, and counted in `failed_count`.
#[derive(Debug, Serialize, ToSchema)]
pub struct PreimageLatencyRow {
    pub ts: DateTime<Utc>,
    pub req_p50: Option<i32>,
    pub req_p95: Option<i32>,
    pub req_p99: Option<i32>,
    pub req_samples: i32,
    pub resp_p50: Option<i32>,
    pub resp_p95: Option<i32>,
    pub resp_p99: Option<i32>,
    pub resp_samples: i32,
    pub failed_count: i32,
}
