//! Grafana integration endpoints. Exposes time-series and aggregate query routes
//! consumed by Grafana dashboards for network-wide telemetry visualization.
//!
//! OpenAPI spec is auto-generated from these annotations and served at
//! `GET /api/docs/openapi.json`.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use utoipa::{IntoParams, OpenApi};

use crate::api::ApiState;
use crate::grafana_types::*;
use crate::onchain_types::*;

#[derive(OpenApi)]
#[openapi(
    paths(
        timeseries,
        stats,
        cores_summary,
        core_detail,
        blocks_convergence,
        blocks_contents,
        services,
        services_timeseries,
        nodes,
        node_stats,
        node_stats_aggregate,
        db_stats,
        bottlenecks,
        wp_funnel,
        guarantee_convergence,
        guarantee_convergence_detail,
        assurance_convergence,
        assurance_convergence_senders,
        wp_funnel_timeseries,
        bottlenecks_timeseries,
        event_types,
        events,
        guarantee_discards,
        onchain_cores_summary,
        onchain_cores_timeseries,
        onchain_core_detail,
        onchain_services_summary,
        onchain_services_timeseries,
        onchain_service_detail,
        onchain_validators_summary,
        onchain_validators_timeseries,
        onchain_validator_detail,
    ),
    components(schemas(
        TimeseriesRow,
        StatsResponse,
        CoreSummary,
        CoreDetail,
        WpTrackingRow,
        BlockConvergenceRow,
        BlockContentsRow,
        ServiceRow,
        ServiceTimeseriesRow,
        NodeRow,
        NodeStatsRow,
        NodeStatsAggregateRow,
        DbStatsResponse,
        TableSize,
        RowCount,
        CompressionInfo,
        BottlenecksResponse,
        StageTiming,
        Percentiles,
        WpFunnelResponse,
        GuaranteeConvergenceSlotRow,
        GuaranteeConvergenceDetailRow,
        AssuranceConvergenceRow,
        AssuranceConvergenceSenderRow,
        WpFunnelTimeseriesRow,
        BottlenecksTimeseriesRow,
        EventRow,
        GuaranteeDiscardRow,
        crate::event_type_meta::EventTypeMeta,
        OnchainCoreSummary,
        OnchainCoreTimeseries,
        OnchainCoreDetail,
        OnchainServiceSummary,
        OnchainServiceTimeseries,
        OnchainServiceDetail,
        OnchainValidatorSummary,
        OnchainValidatorTimeseries,
        OnchainValidatorDetail,
    )),
    tags(
        (name = "grafana", description = "Grafana dashboard API — time-series, aggregates, and metadata"),
        (name = "onchain", description = "On-chain statistics API — per-block data from JAM RPC statistics()")
    )
)]
pub struct GrafanaApiDoc;

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/timeseries", get(timeseries))
        .route("/stats", get(stats))
        .route("/cores", get(cores_summary))
        .route("/cores/:core_id", get(core_detail))
        .route("/blocks/convergence", get(blocks_convergence))
        .route("/blocks/contents", get(blocks_contents))
        .route("/services", get(services))
        .route("/services/timeseries", get(services_timeseries))
        .route("/nodes", get(nodes))
        .route("/node-stats", get(node_stats))
        .route("/node-stats-aggregate", get(node_stats_aggregate))
        .route("/db-stats", get(db_stats))
        .route("/bottlenecks", get(bottlenecks))
        .route("/wp-funnel", get(wp_funnel))
        .route("/guarantee-convergence", get(guarantee_convergence))
        .route("/guarantee-convergence/detail", get(guarantee_convergence_detail))
        .route("/assurance-convergence", get(assurance_convergence))
        .route("/assurance-convergence/senders", get(assurance_convergence_senders))
        .route("/wp-funnel-timeseries", get(wp_funnel_timeseries))
        .route("/bottlenecks-timeseries", get(bottlenecks_timeseries))
        .route("/event-types", get(event_types))
        .route("/events", get(events))
        .route("/guarantee-discards", get(guarantee_discards))
        .nest("/onchain", onchain_router())
}

fn onchain_router() -> Router<ApiState> {
    Router::new()
        .route("/cores", get(onchain_cores_summary))
        .route("/cores/timeseries", get(onchain_cores_timeseries))
        .route("/cores/:core_id", get(onchain_core_detail))
        .route("/services", get(onchain_services_summary))
        .route("/services/timeseries", get(onchain_services_timeseries))
        .route("/services/:service_id", get(onchain_service_detail))
        .route("/validators", get(onchain_validators_summary))
        .route("/validators/timeseries", get(onchain_validators_timeseries))
        .route("/validators/:validator_idx", get(onchain_validator_detail))
}

/// Map sqlx errors to appropriate HTTP status codes.
/// Protocol errors (used for validation) → 400, everything else → 500.
fn map_sqlx_error(context: &str, e: sqlx::Error) -> StatusCode {
    if matches!(&e, sqlx::Error::Protocol(_)) {
        tracing::warn!("{context} bad request: {e}");
        StatusCode::BAD_REQUEST
    } else {
        tracing::error!("{context} error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

// ── Query parameter structs ────────────────────────────────────────────

/// Parameters for time-series queries with flexible grouping and filtering.
#[derive(Deserialize, IntoParams)]
pub struct TimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Grouping column. Allowed: node_id, event_type, core
    pub group_by: Option<String>,
    /// Filter to a single node_id
    pub node: Option<String>,
    /// Comma-separated event type codes, group names, or event names. Supports Grafana {a,b} syntax.
    pub event_types: Option<String>,
    /// Filter to a single core index
    pub core: Option<i16>,
}

/// Common time range + optional filters used by most endpoints.
#[derive(Deserialize, IntoParams)]
pub struct TimeRangeQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Filter to a single node_id (or comma-separated list with Grafana {a,b} support)
    pub node: Option<String>,
    /// Filter to a single core index
    pub core: Option<i16>,
    /// Filter to a single event type code
    pub event_type: Option<i16>,
}

/// Parameters for service endpoints.
#[derive(Deserialize, IntoParams)]
pub struct ServiceQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Comma-separated service IDs (decimal or 0x hex). Supports Grafana {a,b} syntax.
    pub service: Option<String>,
}

/// Parameters for service timeseries endpoint.
#[derive(Deserialize, IntoParams)]
pub struct ServiceTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width (same values as /timeseries)
    pub interval: Option<String>,
    /// Comma-separated service IDs (decimal or 0x hex). Supports Grafana {a,b} syntax.
    pub service: Option<String>,
}

/// Parameters for event-types metadata endpoint.
#[derive(Deserialize, IntoParams)]
pub struct EventTypesParams {
    /// Filter to a single event group name (e.g. "blocks", "wp_pipeline", "failures")
    pub group: Option<String>,
}

/// Parameters for raw events query.
#[derive(Deserialize, IntoParams)]
pub struct EventsQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Comma-separated event type codes, group names, or event names (required)
    pub event_types: String,
    /// Maximum number of events to return (default: 500, max: 2000)
    pub limit: Option<i64>,
}

/// Parameters for assurance convergence senders query.
#[derive(Deserialize, IntoParams)]
pub struct AssuranceConvergenceSendersQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Filter to a single block anchor (hex-encoded)
    pub anchor: Option<String>,
    /// Filter to a single sender node_id
    pub node: Option<String>,
}

/// Parameters for guarantee convergence detail query.
#[derive(Deserialize, IntoParams)]
pub struct GuaranteeConvergenceDetailQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Filter to a single core index
    pub core: Option<i16>,
    /// Filter to a single work package hash (hex-encoded)
    pub wp_hash: Option<String>,
}

/// Parameters for WP pipeline timeseries queries (funnel + bottlenecks).
#[derive(Deserialize, IntoParams)]
pub struct WpTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Filter to a single core index
    pub core: Option<i16>,
}

/// Parameters for guarantee discards query.
#[derive(Deserialize, IntoParams)]
pub struct GuaranteeDiscardsQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width (same values as /timeseries)
    pub interval: Option<String>,
}

// ── Helper functions ───────────────────────────────────────────────────

/// Strip Grafana multi-select curly-brace wrapper: `{a,b}` → `a,b`.
fn strip_grafana_braces(s: &str) -> &str {
    s.strip_prefix('{').and_then(|s| s.strip_suffix('}')).unwrap_or(s)
}

/// Parse comma-separated validator indices, stripping Grafana curly-brace wrapper.
fn parse_validator_indices(s: &str) -> Vec<i16> {
    strip_grafana_braces(s)
        .split(',')
        .filter_map(|v| v.trim().parse().ok())
        .collect()
}

/// Parse comma-separated node names, stripping Grafana curly-brace wrapper.
fn parse_node_list(s: &str) -> Vec<String> {
    strip_grafana_braces(s)
        .split(',')
        .map(|s| s.trim().to_string())
        .collect()
}

// ── Handlers ───────────────────────────────────────────────────────────

/// Time-series event counts with automatic aggregate table selection.
///
/// Queries TimescaleDB continuous aggregates, auto-selecting by interval:
/// `event_stats_30s` (< 60 s), `event_stats_1m` (< 1 h), `event_stats_1h` (>= 1 h),
/// or `core_stats_1m` when `group_by=core`. Aggregation uses
/// `time_bucket(interval, bucket)` with `SUM(event_count)`.
///
/// Exactly one grouping column is populated per row — `event_type`, `core`, or
/// `node_id` — depending on the `group_by` parameter (default: `event_type`).
/// Event type IDs follow the JIP-3 telemetry specification; the `event_types`
/// parameter accepts numeric codes, group names (e.g. `wp_pipeline`), or event
/// names, and supports Grafana `{a,b}` multi-select syntax.
#[utoipa::path(
    get,
    path = "/api/grafana/timeseries",
    params(TimeseriesQuery),
    responses(
        (status = 200, description = "Time-bucketed event counts", body = [TimeseriesRow]),
        (status = 400, description = "Invalid interval or group_by"),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn timeseries(
    Query(q): Query<TimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    let event_types: Option<Vec<i16>> = q.event_types.map(|s| {
        crate::event_type_meta::expand_event_types(&s)
    }).filter(|v| !v.is_empty());

    state
        .store
        .grafana_timeseries(
            q.start,
            q.end,
            interval,
            q.group_by.as_deref(),
            q.node.as_deref(),
            event_types.as_deref(),
            q.core,
        )
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/timeseries", e))
}

/// Dashboard summary counters for the given time range.
///
/// Database counters from `event_stats_1m`: slot events (BlockAuthored, type 42),
/// guarantees (GuaranteeBuilt, 105), failures (WorkPackageFailed, 92), WP events
/// (WorkPackageReceived, 94). Connected nodes from the `nodes` table.
/// Event type IDs as defined in JIP-3.
///
/// Real-time fields are overlaid from in-memory `LiveCounters`: events/blocks per
/// second (10 s rolling average), best and finalized slot numbers, and active TCP
/// connection count. These fields are absent when the metrics tracker is disabled.
#[utoipa::path(
    get,
    path = "/api/grafana/stats",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Dashboard stats", body = StatsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn stats(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut result = state
        .store
        .grafana_stats(q.start, q.end)
        .await
        .map_err(|e| map_sqlx_error("grafana/stats", e))?;

    // Merge real-time data from LiveCounters (events_per_sec, blocks_per_sec, slots)
    if let Some(ref tracker) = state.metrics_tracker {
        let lc = tracker.live_counters();
        let last_10s = lc.sum_last_n_seconds(10);
        let active_nodes = state.telemetry_server.connection_count();
        result.events_per_sec_10s = Some(last_10s.events as f64 / 10.0);
        result.blocks_per_sec_10s = Some(last_10s.blocks as f64 / 10.0);
        result.best_slot = Some(lc.latest_slot());
        result.finalized_slot = Some(lc.finalized_slot());
        result.active_nodes = Some(active_nodes);
    }

    Ok(Json(result))
}

/// Per-core activity summary: work packages, guarantees, and failures.
///
/// Queries `core_stats_1m` continuous aggregate using `SUM(event_count) FILTER`
/// for three event types as defined in JIP-3: WorkPackageReceived (94),
/// GuaranteeBuilt (105), WorkPackageFailed (92). Grouped by core index.
#[utoipa::path(
    get,
    path = "/api/grafana/cores",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Per-core summary", body = [CoreSummary]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn cores_summary(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_cores_summary(q.start, q.end, q.core)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/cores", e))
}

/// Single core detail with recent work packages from the enricher pipeline.
///
/// Returns the same summary counters as `/cores` (from `core_stats_1m`) plus
/// the 100 most recent work packages from `wp_tracking` for this core. The
/// `wp_tracking` table is populated by the enricher, which correlates WP
/// pipeline events (types 90–109 as defined in JIP-3) across nodes, tracking
/// each work package from submission through distribution or failure.
#[utoipa::path(
    get,
    path = "/api/grafana/cores/{core_id}",
    params(
        ("core_id" = i16, Path, description = "Core index (0-based)"),
        TimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Core detail with recent WPs", body = CoreDetail),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn core_detail(
    Path(core_id): Path<i16>,
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_core_detail(q.start, q.end, core_id)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/cores/detail", e))
}

/// Block propagation convergence percentiles per slot.
///
/// Reads the `slot_convergence` table, populated by the enricher which measures
/// the time between block authoring on the author node and reception across all
/// other nodes. Returns pre-computed p50, p99, and p100 propagation delays in
/// milliseconds, along with the node count that reported each event type.
/// Use the `event_type` filter to select BestBlock (11), Finalized (12), or
/// Importing (43) convergence — event types as defined in JIP-3.
#[utoipa::path(
    get,
    path = "/api/grafana/blocks/convergence",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Convergence percentiles per slot", body = [BlockConvergenceRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn blocks_convergence(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_blocks_convergence(q.start, q.end, q.event_type)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/blocks/convergence", e))
}

/// Block contents extracted from BlockAuthored events.
///
/// Queries the raw `events` hypertable for BlockAuthored events (type 42 as
/// defined in JIP-3), extracting extrinsic breakdown from the JSONB `data`
/// column via `data->'Authored'->'outline'` — counts of guarantees, assurances,
/// preimages, tickets, dispute verdicts, and total extrinsic size in bytes.
#[utoipa::path(
    get,
    path = "/api/grafana/blocks/contents",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Block contents per slot", body = [BlockContentsRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn blocks_contents(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_blocks_contents(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/blocks/contents", e))
}

/// Per-service activity and gas usage totals.
///
/// Queries `service_stats_1m` continuous aggregate (rollup of `event_services`
/// join table). Counts and gas are computed via `SUM FILTER` for event types
/// as defined in JIP-3: WorkPackageReceived (94), Authorized (95),
/// Refined (101), BlockExecuted (47). Service IDs are hex-encoded in the
/// response (JAM uses u32 service IDs, stored as signed i32 in PostgreSQL).
/// The `service` parameter accepts decimal or `0x` hex IDs with Grafana
/// `{a,b}` multi-select syntax.
#[utoipa::path(
    get,
    path = "/api/grafana/services",
    params(ServiceQuery),
    responses(
        (status = 200, description = "Per-service totals", body = [ServiceRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn services(
    Query(q): Query<ServiceQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let services: Option<Vec<DbServiceId>> = q.service.map(|s| DbServiceId::parse_list(&s));
    state
        .store
        .grafana_services(q.start, q.end, services.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/services", e))
}

/// Time-bucketed per-service metrics (WP counts and gas usage).
///
/// Same `service_stats_1m` aggregate as `/services`, re-bucketed via
/// `time_bucket()` to the requested interval (default 1 m). Returns per-bucket
/// work package counts and gas consumed split by type: authorization (95),
/// refinement (101), execution (47) — event types as defined in JIP-3.
/// Service IDs are hex-encoded. Supports Grafana `{a,b}` multi-select.
#[utoipa::path(
    get,
    path = "/api/grafana/services/timeseries",
    params(ServiceTimeseriesQuery),
    responses(
        (status = 200, description = "Service time-series", body = [ServiceTimeseriesRow]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn services_timeseries(
    Query(q): Query<ServiceTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    let services: Option<Vec<DbServiceId>> = q.service.map(|s| DbServiceId::parse_list(&s));

    state
        .store
        .grafana_services_timeseries(
            q.start,
            q.end,
            interval,
            services.as_deref(),
        )
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/services/timeseries", e))
}

/// All known nodes with metadata.
///
/// Returns every node that has ever connected, from the `nodes` table (updated
/// on TCP connect/disconnect and status events). Sorted by `is_connected DESC,
/// last_seen_at DESC` (connected nodes first). `total_event_count` is the sum
/// of the current-session counter and the historical total across reconnects.
/// No time range required.
#[utoipa::path(
    get,
    path = "/api/grafana/nodes",
    responses(
        (status = 200, description = "All known nodes", body = [NodeRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn nodes(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_nodes()
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/nodes", e))
}

/// Raw node status rows at ~2 s granularity.
///
/// Reads the `node_stats` hypertable directly (not an aggregate). Each row is
/// inserted from a Status event (type 10, as defined in JIP-3) that nodes send
/// periodically. Contains peer counts, DA shard/preimage storage metrics, and
/// guarantee distribution across cores. The `node` parameter accepts a
/// comma-separated list with Grafana `{a,b}` multi-select syntax.
#[utoipa::path(
    get,
    path = "/api/grafana/node-stats",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Raw node status snapshots", body = [NodeStatsRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn node_stats(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let nodes: Option<Vec<String>> = q.node.map(|n| parse_node_list(&n));
    state
        .store
        .grafana_node_stats(q.start, q.end, nodes.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/node-stats", e))
}

/// 1-minute aggregated node stats from `node_stats_1m`.
///
/// Without a node filter, returns **network-wide** aggregates per 1-minute
/// bucket: AVG/MIN/MAX across all nodes for each metric (peers, shards,
/// preimages, guarantees). With a node filter, returns per-node aggregate
/// rows. The `node` parameter accepts comma-separated IDs with Grafana
/// `{a,b}` multi-select syntax.
#[utoipa::path(
    get,
    path = "/api/grafana/node-stats-aggregate",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Aggregated node stats (network-wide or per-node)", body = [NodeStatsAggregateRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn node_stats_aggregate(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let nodes: Option<Vec<String>> = q.node.map(|n| parse_node_list(&n));
    state
        .store
        .grafana_node_stats_aggregate(q.start, q.end, nodes.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/node-stats-aggregate", e))
}

/// TimescaleDB internal metadata: table sizes, row counts, compression.
///
/// Queries three TimescaleDB internal functions: `hypertable_detailed_size()`
/// for table/index/toast byte breakdown, `approximate_row_count()` for fast
/// row estimates on hypertables (exact `COUNT(*)` for smaller tables like
/// `wp_tracking`, `slot_convergence`, `nodes`), and `chunk_compression_stats()`
/// for compression ratios. No parameters required.
#[utoipa::path(
    get,
    path = "/api/grafana/db-stats",
    responses(
        (status = 200, description = "TimescaleDB metadata", body = DbStatsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn db_stats(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_db_stats()
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/db-stats", e))
}

/// Work package pipeline bottleneck analysis with percentile timings.
///
/// Queries `wp_tracking` table using `percentile_cont(0.5)` and
/// `percentile_cont(0.95)` on the inter-stage timestamp deltas for each
/// pipeline stage: authorize (received→authorized), refine (authorized→refined),
/// report (refined→report_built), guarantee (report_built→guarantee_built),
/// distribute (guarantee_built→distributed), and pipeline_total
/// (received→distributed or last_updated). Failure rate is the ratio of WPs
/// with `failed_at IS NOT NULL`. Optional `core` filter narrows to a single core.
#[utoipa::path(
    get,
    path = "/api/grafana/bottlenecks",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Pipeline bottleneck analysis", body = [BottlenecksResponse]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn bottlenecks(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_bottlenecks(q.start, q.end, q.core)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/bottlenecks", e))
}

/// Work package pipeline funnel — counts how many WPs reached each stage.
///
/// Queries `wp_tracking` with `COUNT(*) FILTER (WHERE stage_timestamp IS NOT NULL)`
/// for each pipeline stage: received, authorized, refined, report_built,
/// guarantee_built, distributed, and failed. A WP counted as "distributed" has
/// successfully completed the entire pipeline. "failed" counts WPs with
/// `failed_at` set at any stage.
#[utoipa::path(
    get,
    path = "/api/grafana/wp-funnel",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Pipeline funnel counts", body = WpFunnelResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_funnel(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_wp_funnel(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp-funnel", e))
}

/// Guarantee convergence overview — per-slot summary.
///
/// Aggregates all guarantees per slot: flattens received_timestamps across all
/// work_report_hashes for a slot and computes true cross-core percentiles of
/// (GuaranteeReceived - GuaranteeBuilt) propagation latency. One row per slot.
#[utoipa::path(
    get,
    path = "/api/grafana/guarantee-convergence",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Per-slot guarantee convergence summary", body = [GuaranteeConvergenceSlotRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantee_convergence(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_guarantee_convergence(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/guarantee-convergence", e))
}

/// Guarantee convergence detail — per-guarantee rows for drill-down.
///
/// Returns one row per work_report_hash, filtered by optional core or wp_hash.
/// Each row shows how quickly GuaranteeReceived(112) propagated across the
/// validator network after GuaranteeBuilt(105).
#[utoipa::path(
    get,
    path = "/api/grafana/guarantee-convergence/detail",
    params(GuaranteeConvergenceDetailQuery),
    responses(
        (status = 200, description = "Per-guarantee convergence detail", body = [GuaranteeConvergenceDetailRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantee_convergence_detail(
    Query(q): Query<GuaranteeConvergenceDetailQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let wp_hash_bytes = q.wp_hash.as_deref().and_then(|h| hex::decode(h).ok());
    state
        .store
        .grafana_guarantee_convergence_detail(
            q.start,
            q.end,
            q.core,
            wp_hash_bytes.as_deref(),
        )
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/guarantee-convergence/detail", e))
}

/// Assurance convergence overview — per-anchor summary.
///
/// Each row represents one block anchor, showing how quickly assurances
/// from all senders propagated to receiving validators. Also includes
/// distribution start spread (how quickly validators begin distributing).
///
/// Anchor: DistributingAssurance(126) per sender.
/// Measured: AssuranceReceived(131) on receiving validators.
/// Availability window: 5 slots (30 seconds).
#[utoipa::path(
    get,
    path = "/api/grafana/assurance-convergence",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Per-anchor assurance convergence summary", body = [AssuranceConvergenceRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn assurance_convergence(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_assurance_convergence(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/assurance-convergence", e))
}

/// Assurance convergence per-sender detail — for debugging individual node propagation.
///
/// Returns one row per (anchor, sender), showing how quickly this sender's
/// assurance reached other validators. Filter by anchor or node for drill-down.
#[utoipa::path(
    get,
    path = "/api/grafana/assurance-convergence/senders",
    params(AssuranceConvergenceSendersQuery),
    responses(
        (status = 200, description = "Per-sender assurance convergence detail", body = [AssuranceConvergenceSenderRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn assurance_convergence_senders(
    Query(q): Query<AssuranceConvergenceSendersQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let anchor_bytes = q.anchor.as_deref().and_then(|h| hex::decode(h).ok());
    state
        .store
        .grafana_assurance_convergence_senders(
            q.start,
            q.end,
            anchor_bytes.as_deref(),
            q.node.as_deref(),
        )
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/assurance-convergence/senders", e))
}

/// Work package pipeline funnel bucketed over time.
///
/// Same data as `/wp-funnel` but bucketed by `time_bucket` on `first_seen`.
/// Each row contains per-stage counts for WPs whose `first_seen` falls in
/// that bucket. Optional `core` filter narrows to a single core.
#[utoipa::path(
    get,
    path = "/api/grafana/wp-funnel-timeseries",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Pipeline funnel counts per time bucket", body = [WpFunnelTimeseriesRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_funnel_timeseries(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_wp_funnel_timeseries(q.start, q.end, interval, q.core)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp-funnel-timeseries", e))
}

/// Work package pipeline bottleneck analysis bucketed over time.
///
/// Same data as `/bottlenecks` but bucketed by `time_bucket` on `first_seen`.
/// Per bucket: `percentile_cont(0.5)` and `percentile_cont(0.95)` on
/// inter-stage timestamp deltas for each pipeline stage. Optional `core`
/// filter narrows to a single core.
#[utoipa::path(
    get,
    path = "/api/grafana/bottlenecks-timeseries",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Pipeline bottleneck percentiles per time bucket", body = [BottlenecksTimeseriesRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn bottlenecks_timeseries(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_bottlenecks_timeseries(q.start, q.end, interval, q.core)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/bottlenecks-timeseries", e))
}

/// Static metadata for all telemetry event types (as defined in JIP-3).
///
/// Returns in-memory metadata for all 115 event types — no database query.
/// Each entry includes the numeric ID, human-readable name, and group. Use
/// the `group` parameter to filter by group name (e.g. `blocks`, `wp_pipeline`,
/// `failures`). The `failures` group is a virtual group spanning all
/// Failed/Discarded/Duplicate events across categories.
#[utoipa::path(
    get,
    path = "/api/grafana/event-types",
    params(EventTypesParams),
    responses(
        (status = 200, description = "Event type metadata", body = [crate::event_type_meta::EventTypeMeta]),
    ),
    tag = "grafana"
)]
async fn event_types(Query(params): Query<EventTypesParams>) -> impl IntoResponse {
    let all = crate::event_type_meta::event_type_metadata();
    if let Some(ref group) = params.group {
        let ids = crate::event_type_meta::expand_event_types(group);
        let filtered: Vec<_> = all.iter().filter(|m| ids.contains(&m.id)).cloned().collect();
        Json(filtered)
    } else {
        Json(all.to_vec())
    }
}

/// Raw events from the `events` hypertable, filtered by event type.
///
/// Returns the most recent events matching the given event types, ordered by
/// timestamp DESC. The `event_types` parameter is required and accepts numeric
/// IDs (as defined in JIP-3), group names (e.g. `wp_pipeline`, `failures`),
/// or event names (e.g. `Authored`), expanded server-side via
/// `expand_event_types()`. Default limit is 500, capped at 2000. The `data`
/// field contains the full event-specific JSONB payload which varies by type.
#[utoipa::path(
    get,
    path = "/api/grafana/events",
    params(EventsQuery),
    responses(
        (status = 200, description = "Raw event records", body = [EventRow]),
        (status = 400, description = "Missing event_types parameter"),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn events(
    Query(q): Query<EventsQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let event_types = crate::event_type_meta::expand_event_types(&q.event_types);
    let limit = q.limit.unwrap_or(500);

    state
        .store
        .grafana_events(q.start, q.end, &event_types, limit)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/events", e))
}

/// Time-bucketed guarantee discard counts grouped by reason.
///
/// Queries the pre-aggregated `guarantee_receiving_counts` table for
/// GuaranteeDiscarded events (type 113), grouped by discard reason.
/// Reasons are enum variants: PackageReportedOnChain(0), ReplacedByBetter(1),
/// CannotReportOnChain(2), TooManyGuarantees(3), Other(4).
#[utoipa::path(
    get,
    path = "/api/grafana/guarantee-discards",
    params(GuaranteeDiscardsQuery),
    responses(
        (status = 200, description = "Guarantee discards by reason", body = [GuaranteeDiscardRow]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantee_discards(
    Query(q): Query<GuaranteeDiscardsQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("30s");
    state
        .store
        .grafana_guarantee_discards(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/guarantee-discards", e))
}

// ── On-chain statistics query params ─────────────────────────────────────

/// Parameters for on-chain time range queries.
#[derive(Deserialize, IntoParams)]
pub struct OnchainTimeRangeQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
}

/// Parameters for on-chain timeseries queries.
#[derive(Deserialize, IntoParams)]
pub struct OnchainTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
}

/// Parameters for on-chain validator timeseries queries (with optional validator filter).
#[derive(Deserialize, IntoParams)]
pub struct OnchainValidatorTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Comma-separated validator indices. Supports Grafana {a,b} syntax.
    pub validator: Option<String>,
}

/// Parameters for on-chain service queries (with optional service filter).
#[derive(Deserialize, IntoParams)]
pub struct OnchainServiceQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Comma-separated service IDs (decimal or 0x hex). Supports Grafana {a,b} syntax.
    pub service: Option<String>,
}

/// Parameters for on-chain service timeseries queries (with optional service filter).
#[derive(Deserialize, IntoParams)]
pub struct OnchainServiceTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Comma-separated service IDs (decimal or 0x hex). Supports Grafana {a,b} syntax.
    pub service: Option<String>,
}

// ── On-chain cores ──────────────────────────────────────────────────────

/// Per-core on-chain activity summary (all 341 cores).
///
/// Fields from Gray Paper `CoreActivityRecord`, SUMmed over range except
/// popularity (AVG). Data source: `onchain_core_stats` hypertable.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/cores",
    params(OnchainTimeRangeQuery),
    responses(
        (status = 200, description = "Per-core on-chain activity summary (all 341 cores). \
            Fields from Gray Paper CoreActivityRecord, SUMmed over range except popularity (AVG).",
            body = [OnchainCoreSummary]),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_cores_summary(
    Query(q): Query<OnchainTimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .onchain_cores_summary(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/cores", e))
}

/// Time-bucketed per-core on-chain stats.
///
/// Each row = one (bucket, core) pair with SUMmed fields.
/// Data source: `onchain_core_stats` with `time_bucket()` aggregation.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/cores/timeseries",
    params(OnchainTimeseriesQuery),
    responses(
        (status = 200, description = "Time-bucketed per-core on-chain stats. \
            Each row = one (bucket, core) pair with SUMmed fields.",
            body = [OnchainCoreTimeseries]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_cores_timeseries(
    Query(q): Query<OnchainTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .onchain_cores_timeseries(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/cores/timeseries", e))
}

/// Raw per-block on-chain stats for a single core.
///
/// No aggregation — each row is one block. Max 1000 rows, newest first.
/// Data source: `onchain_core_stats` filtered by core.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/cores/{core_id}",
    params(
        ("core_id" = i16, Path, description = "Core index (0–340)"),
        OnchainTimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Raw per-block on-chain stats for a single core. \
            No aggregation — each row is one block. Max 1000 rows, newest first.",
            body = [OnchainCoreDetail]),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_core_detail(
    Path(core_id): Path<i16>,
    Query(q): Query<OnchainTimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .onchain_core_detail(core_id, q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/cores/{id}", e))
}

// ── On-chain services ───────────────────────────────────────────────────

/// Per-service on-chain activity summary.
///
/// Only services with non-zero activity are returned.
/// Fields from Gray Paper `ServiceActivityRecord`, all SUMmed.
/// Data source: `onchain_service_stats` hypertable.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/services",
    params(OnchainServiceQuery),
    responses(
        (status = 200, description = "Per-service on-chain activity summary. \
            Only services with non-zero activity are returned. \
            Fields from Gray Paper ServiceActivityRecord, all SUMmed.",
            body = [OnchainServiceSummary]),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_services_summary(
    Query(q): Query<OnchainServiceQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let services: Option<Vec<DbServiceId>> = q.service.map(|s| DbServiceId::parse_list(&s));
    state
        .store
        .onchain_services_summary(q.start, q.end, services.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/services", e))
}

/// Time-bucketed per-service on-chain stats.
///
/// Data source: `onchain_service_stats` with `time_bucket()` aggregation.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/services/timeseries",
    params(OnchainServiceTimeseriesQuery),
    responses(
        (status = 200, description = "Time-bucketed per-service on-chain stats.",
            body = [OnchainServiceTimeseries]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_services_timeseries(
    Query(q): Query<OnchainServiceTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    let services: Option<Vec<DbServiceId>> = q.service.map(|s| DbServiceId::parse_list(&s));
    state
        .store
        .onchain_services_timeseries(q.start, q.end, interval, services.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/services/timeseries", e))
}

/// Raw per-block on-chain stats for a single service.
///
/// Max 1000 rows, newest first.
/// Data source: `onchain_service_stats` filtered by service_id.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/services/{service_id}",
    params(
        ("service_id" = String, Path, description = "Service ID (decimal or 0x hex)"),
        OnchainTimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Raw per-block on-chain stats for a single service. \
            Max 1000 rows, newest first.",
            body = [OnchainServiceDetail]),
        (status = 400, description = "Invalid service ID"),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_service_detail(
    Path(service_id): Path<String>,
    Query(q): Query<OnchainTimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let ids = DbServiceId::parse_list(&service_id);
    let id = ids.first().copied().ok_or(StatusCode::BAD_REQUEST)?;
    state
        .store
        .onchain_service_detail(id, q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/services/{id}", e))
}

// ── On-chain validators ─────────────────────────────────────────────────

/// Per-validator on-chain stats (all 1024 validators).
///
/// Fields from Gray Paper `ValActivityRecord`. Values are epoch-cumulative —
/// MAX is used to get peak value in the requested range.
/// Data source: `onchain_validator_stats` hypertable.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/validators",
    params(OnchainTimeRangeQuery),
    responses(
        (status = 200, description = "Per-validator on-chain stats (all 1024 validators). \
            Fields from Gray Paper ValActivityRecord. Values are epoch-cumulative — \
            MAX is used to get peak value in the requested range.",
            body = [OnchainValidatorSummary]),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_validators_summary(
    Query(q): Query<OnchainTimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .onchain_validators_summary(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/validators", e))
}

/// Time-bucketed per-validator on-chain stats.
///
/// MAX aggregation (epoch-cumulative values).
/// Data source: `onchain_validator_stats` with `time_bucket()` aggregation.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/validators/timeseries",
    params(OnchainValidatorTimeseriesQuery),
    responses(
        (status = 200, description = "Time-bucketed per-validator on-chain stats. \
            MAX aggregation (epoch-cumulative values).",
            body = [OnchainValidatorTimeseries]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_validators_timeseries(
    Query(q): Query<OnchainValidatorTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    let validators: Option<Vec<i16>> = q.validator.map(|s| parse_validator_indices(&s));
    state
        .store
        .onchain_validators_timeseries(q.start, q.end, interval, validators.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/validators/timeseries", e))
}

/// Raw per-block on-chain stats for a single validator.
///
/// Shows epoch-cumulative values growing block by block. Max 1000 rows.
/// Data source: `onchain_validator_stats` filtered by validator_index.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/validators/{validator_idx}",
    params(
        ("validator_idx" = i16, Path, description = "Validator index (0–1023)"),
        OnchainTimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Raw per-block on-chain stats for a single validator. \
            Shows epoch-cumulative values growing block by block. Max 1000 rows.",
            body = [OnchainValidatorDetail]),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_validator_detail(
    Path(validator_idx): Path<i16>,
    Query(q): Query<OnchainTimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .onchain_validator_detail(validator_idx, q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/validators/{idx}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_service_ids_basic() {
        assert_eq!(
            DbServiceId::parse_list("10,20"),
            vec![DbServiceId(10), DbServiceId(20)]
        );
    }

    #[test]
    fn test_parse_service_ids_hex() {
        assert_eq!(
            DbServiceId::parse_list("0xa,0x14"),
            vec![DbServiceId(10), DbServiceId(20)]
        );
    }

    #[test]
    fn test_parse_service_ids_hex_overflow() {
        // 0xea9f727c = 3936318076 as u32, wraps to -358649220 as i32
        let result = DbServiceId::parse_list("0xea9f727c");
        assert_eq!(result, vec![DbServiceId(0xea9f727c_u32 as i32)]);
    }

    #[test]
    fn test_parse_service_ids_curly_braces() {
        assert_eq!(
            DbServiceId::parse_list("{0xa,0x14}"),
            vec![DbServiceId(10), DbServiceId(20)]
        );
    }

    #[test]
    fn test_parse_service_ids_mixed() {
        assert_eq!(
            DbServiceId::parse_list("10,0x14"),
            vec![DbServiceId(10), DbServiceId(20)]
        );
    }

    #[test]
    fn test_db_service_id_display() {
        assert_eq!(DbServiceId(10).to_string(), "0x0000000a");
        assert_eq!(DbServiceId(0xea9f727c_u32 as i32).to_string(), "0xea9f727c");
    }

    #[test]
    fn test_db_service_id_serialize() {
        let id = DbServiceId(255);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"0x000000ff\"");
    }

    #[test]
    fn test_parse_node_list_basic() {
        assert_eq!(parse_node_list("node1,node2"), vec!["node1", "node2"]);
    }

    #[test]
    fn test_parse_node_list_curly_braces() {
        assert_eq!(parse_node_list("{node1,node2}"), vec!["node1", "node2"]);
    }
}
