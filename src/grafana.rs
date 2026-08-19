//! Grafana integration endpoints. Exposes time-series and aggregate query routes
//! consumed by Grafana dashboards for network-wide telemetry visualization.
//!
//! OpenAPI spec is auto-generated from these annotations and served at
//! `GET /api/docs/openapi.json`.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
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
        da_stats,
        shard_latency,
        wp_funnel_timeseries,
        bottlenecks_timeseries,
        event_types,
        events,
        guarantee_discards,
        // Phase 3
        failure_rates,
        sync_timeline,
        connections_timeline,
        guarantees,
        guarantees_by_guarantor,
        wp_stats,
        validators_cores,
        network_health,
        // Phase 4
        wp_active,
        wp_detail,
        wp_batch,
        blocks_summary,
        core_metrics,
        core_validators,
        // Phase 5
        execution_metrics,
        onchain_cores_summary,
        onchain_cores_timeseries,
        onchain_core_detail,
        onchain_services_summary,
        onchain_services_timeseries,
        onchain_service_detail,
        onchain_validators_summary,
        onchain_validators_timeseries,
        onchain_validator_detail,
        // Validator profiling
        validator_profiling,
        validator_profiling_timeseries,
        // DA latency
        bundle_latency,
        segment_latency,
        preimage_latency,
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
        ConvergenceTimeseriesRow,
        DaStatsRow,
        ShardLatencyRow,
        WpFunnelTimeseriesRow,
        BottlenecksTimeseriesRow,
        EventRow,
        GuaranteeDiscardRow,
        crate::event_type_meta::EventTypeMeta,
        OnchainCoreSummary,
        OnchainCoreTimeseries,
        OnchainCoreTimeseriesAgg,
        OnchainCoreDetail,
        OnchainServiceSummary,
        OnchainServiceTimeseries,
        OnchainServiceDetail,
        OnchainValidatorSummary,
        OnchainValidatorTimeseries,
        OnchainValidatorTimeseriesAgg,
        OnchainValidatorDetail,
        // Phase 0
        EventsSearchResponse,
        PaginationMeta,
        // Phase 3
        FailureRatesResponse,
        FailureOverall,
        FailureCategory,
        FailureByNode,
        RecentFailure,
        SyncTimelineRow,
        ConnectionsTimelineResponse,
        ConnectionsBucket,
        ConnectionHealthStats,
        GuaranteesResponse,
        GuaranteeTotals,
        GuaranteeSuccessRates,
        GuarantorBreakdownResponse,
        GuarantorRow,
        WpStatsResponse,
        WpStageTotals,
        WpCoreCount,
        ValidatorCoreRow,
        NetworkHealthResponse,
        HealthComponent,
        HealthAlert,
        // Phase 4
        WpActiveResponse,
        WpActiveRow,
        WpActiveSummary,
        WpReachedCounts,
        WpStageDurations,
        FailureBreakdownEntry,
        WpDetailResponse,
        BlocksSummaryResponse,
        BlockTotals,
        ChainState,
        AuthoringByNode,
        CoreMetricsResponse,
        CoreValidatorsResponse,
        CoreValidatorRow,
        // Phase 5
        ExecutionMetricsResponse,
        ExecutionPhaseStats,
        ServiceExecutionRow,
        // Validator profiling
        ValidatorProfilingResponse,
        ValidatorProfilingRow,
        ValidatorProfilingTimeseriesRow,
        // DA latency
        BundleLatencyRow,
        SegmentLatencyRow,
        PreimageLatencyRow,
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
        .route(
            "/guarantee-convergence/detail",
            get(guarantee_convergence_detail),
        )
        .route("/assurance-convergence", get(assurance_convergence))
        .route(
            "/assurance-convergence/senders",
            get(assurance_convergence_senders),
        )
        .route("/da-stats", get(da_stats))
        .route("/shard-latency", get(shard_latency))
        .route("/bundle-latency", get(bundle_latency))
        .route("/segment-latency", get(segment_latency))
        .route("/preimage-latency", get(preimage_latency))
        .route("/wp-funnel-timeseries", get(wp_funnel_timeseries))
        .route("/bottlenecks-timeseries", get(bottlenecks_timeseries))
        .route("/event-types", get(event_types))
        .route("/events", get(events))
        .route("/guarantee-discards", get(guarantee_discards))
        // Phase 3: new endpoints
        .route("/failure-rates", get(failure_rates))
        .route("/sync-timeline", get(sync_timeline))
        .route("/connections-timeline", get(connections_timeline))
        .route("/guarantees", get(guarantees))
        .route("/guarantees/by-guarantor", get(guarantees_by_guarantor))
        .route("/wp-stats", get(wp_stats))
        .route("/validators/cores", get(validators_cores))
        .route("/network-health", get(network_health))
        // Phase 4: moderate endpoints
        .route("/wp-active", get(wp_active))
        .route("/wp/:wp_hash", get(wp_detail))
        .route("/wp/batch", post(wp_batch))
        .route("/blocks/summary", get(blocks_summary))
        .route("/cores/:core_id/metrics", get(core_metrics))
        .route("/cores/:core_id/validators", get(core_validators))
        // Validator profiling
        .route("/validator-profiling", get(validator_profiling))
        .route(
            "/validator-profiling-timeseries",
            get(validator_profiling_timeseries),
        )
        // Phase 5
        .route("/execution", get(execution_metrics))
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

/// Parameters for raw events query with optional filtering and pagination.
#[derive(Deserialize, IntoParams)]
pub struct EventsQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Comma-separated event type codes, group names, or event names. Optional — if omitted, returns all types.
    pub event_types: Option<String>,
    /// Maximum number of events to return (default: 500, max: 2000)
    pub limit: Option<i64>,
    /// Offset for pagination (default: 0)
    pub offset: Option<i64>,
    /// Filter to a single node_id
    pub node: Option<String>,
    /// Filter to a single core index (uses hot column)
    pub core: Option<i16>,
    /// Filter to a single work package hash (hex-encoded, uses hot column)
    pub wp_hash: Option<String>,
}

/// Parameters for convergence endpoints that support both per-slot and histogram modes.
#[derive(Deserialize, IntoParams)]
pub struct ConvergenceQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width for histogram mode. When present, returns percentile timeseries from merged histograms instead of per-slot rows.
    pub interval: Option<String>,
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
    /// Bucket width. When present, the response becomes one percentile row per bucket instead of one row per sender, and the anchor filter is ignored.
    pub interval: Option<String>,
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

/// Parameters for validator profiling summary query.
#[derive(Deserialize, IntoParams)]
pub struct ValidatorProfilingQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Filter to a single core index
    pub core: Option<i16>,
    /// Maximum number of nodes to return.
    /// `network_avg_total_ms` still reflects all nodes regardless of limit.
    pub limit: Option<u32>,
    /// Sort order for `avg_total_ms`: `desc` (slowest first, default) or `asc` (fastest first).
    pub sort: Option<String>,
}

/// Parameters for validator profiling timeseries query.
#[derive(Deserialize, IntoParams)]
pub struct ValidatorProfilingTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Filter to a single core index
    pub core: Option<i16>,
    /// Filter to a single node (hex-encoded 32-byte public key). When provided,
    /// returns per-bucket timeseries for that node only. When omitted, returns
    /// top ~20 slowest nodes.
    pub node: Option<String>,
}

// ── Helper functions ───────────────────────────────────────────────────

/// Strip Grafana multi-select curly-brace wrapper: `{a,b}` → `a,b`.
fn strip_grafana_braces(s: &str) -> &str {
    s.strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(s)
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
    let event_types: Option<Vec<i16>> = q
        .event_types
        .map(|s| crate::event_type_meta::expand_event_types(&s))
        .filter(|v| !v.is_empty());

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

/// How fast a newly authored block reaches the rest of the network, per slot.
///
/// One row per slot and per block event. Every offset is measured from
/// Authored(42) on the block's author to the same slot's event on each other
/// node, and the offsets of all reporting nodes are pooled before the
/// percentiles are taken, so they are percentiles over nodes. The `event_type`
/// filter picks which step to look at — BestBlockChanged(11),
/// FinalizedBlockChanged(12), Authoring(40), Authored(42) or Importing(43);
/// without it every step observed for the slot is returned. Authoring(40) and
/// Authored(42) come from the author itself, so their offsets sit at or below
/// zero. A slot whose author never reported Authored(42) has no rows at all,
/// and a slot's rows keep being refined while late reports for it arrive.
///
/// Answers: how quickly does a new block propagate across the network, and in
/// which slots does propagation degrade?
#[utoipa::path(
    get,
    path = "/api/grafana/blocks/convergence",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array of per-slot rows, ascending by slot and then by event type, each carrying the propagation-offset percentiles across the nodes that reported that block event for the slot.", body = [BlockConvergenceRow]),
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

/// What each authored block contained, one row per authored block.
///
/// Each row is one Authored(42) report from the node that authored the block:
/// the slot, the author, and the block outline it reported — how many
/// guarantees, assurances, preimages, tickets and dispute verdicts the block
/// carried, plus the block's size in bytes. Built from recent raw events, which
/// are retained for about an hour, so older parts of a range come back empty.
///
/// Answers: how full are the blocks being authored, and which extrinsic types
/// are actually making it on chain?
#[utoipa::path(
    get,
    path = "/api/grafana/blocks/contents",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array of rows, one per authored block, ascending by slot, each with the author and the per-extrinsic-type counts from the block outline. Only covers the ~1 hour raw-event retention window.", body = [BlockContentsRow]),
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
        .grafana_services_timeseries(q.start, q.end, interval, services.as_deref())
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
async fn nodes(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
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
async fn db_stats(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_db_stats()
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/db-stats", e))
}

/// Where time goes inside the guarantor work-package pipeline.
///
/// Median and 95th-percentile durations of each stage a work package passes
/// through on its guarantors: authorize (WorkPackageReceived(94) →
/// Authorized(95)), refine (→ Refined(101)), report (→ WorkReportBuilt(102)),
/// guarantee (→ GuaranteeBuilt(105)), distribute (→ GuaranteesDistributed(109)),
/// plus the total from reception to distribution. Work packages are selected by
/// when they were first observed anywhere in the network; one that never reached
/// distribution contributes the time up to its last observed pipeline event.
/// `failure_rate` is the share of them for which a WorkPackageFailed(92) was
/// reported. The optional `core` filter narrows to the work packages assigned to
/// one core.
///
/// Answers: which pipeline stage dominates work-package latency, and how often do
/// work packages fail outright?
#[utoipa::path(
    get,
    path = "/api/grafana/bottlenecks",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array holding a single object: per-stage median and p95 durations in milliseconds, the number of work packages considered, how many of them failed, and the resulting failure rate.", body = [BottlenecksResponse]),
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

/// How many work packages reached each stage of the guarantor pipeline.
///
/// Counts over the work packages first observed in the range: received
/// (WorkPackageReceived(94)), authorized (Authorized(95)), refined (Refined(101)),
/// report_built (WorkReportBuilt(102)), guarantee_built (GuaranteeBuilt(105)),
/// distributed (GuaranteesDistributed(109)) and failed (WorkPackageFailed(92)).
/// A work package counts towards a stage as soon as any of its guarantors reported
/// that stage, so the gap between two consecutive counts is the number that
/// stopped progressing there. `distributed` means the primary guarantor finished
/// sending the guarantee out.
///
/// Answers: at which pipeline stage do work packages get stuck or lost?
#[utoipa::path(
    get,
    path = "/api/grafana/wp-funnel",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with one work-package count per pipeline stage for the whole range.", body = WpFunnelResponse),
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

/// How fast guarantees reach the validator set, summarised per slot.
///
/// One row per slot. The latency measured is GuaranteeBuilt(105) on the
/// guarantor until GuaranteeReceived(112) on each validator that received the
/// guarantee. Latencies for every guarantee built in the slot are pooled before
/// the percentiles are taken, so they are true cross-core percentiles and not an
/// average of per-guarantee ones.
///
/// Answers: how quickly does a guarantee propagate to the rest of the validator
/// set, and in which slots does propagation degrade?
#[utoipa::path(
    get,
    path = "/api/grafana/guarantee-convergence",
    params(ConvergenceQuery),
    responses(
        (status = 200, description = "Array of per-slot rows, ascending by slot, each carrying the propagation-latency percentiles for that slot. With `interval`: one row per time bucket instead, with percentiles taken over the latencies of all guarantees in the bucket.", body = [GuaranteeConvergenceSlotRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantee_convergence(
    Query(q): Query<ConvergenceQuery>,
    State(state): State<ApiState>,
) -> Result<Response, StatusCode> {
    if let Some(interval) = &q.interval {
        state
            .store
            .grafana_guarantee_convergence_hist(q.start, q.end, interval)
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/guarantee-convergence", e))
    } else {
        state
            .store
            .grafana_guarantee_convergence(q.start, q.end)
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/guarantee-convergence", e))
    }
}

/// Per-work-report guarantee propagation — drill-down behind the per-slot summary.
///
/// One row per work report, identified by its work-report hash. Each row names
/// the guarantor that emitted GuaranteeBuilt(105) and the core the report was
/// built for, and gives the spread of latencies until GuaranteeReceived(112) on
/// the validators that received it. Optional `core` and `wp_hash` filters narrow
/// the drill-down.
///
/// Answers: which individual guarantees propagated slowly, and which guarantor
/// and core produced them?
#[utoipa::path(
    get,
    path = "/api/grafana/guarantee-convergence/detail",
    params(GuaranteeConvergenceDetailQuery),
    responses(
        (status = 200, description = "Array of rows, one per work report, ascending by slot, each with its guarantor, core and GuaranteeBuilt(105) → GuaranteeReceived(112) latency percentiles. Narrowed by the optional core and work-package-hash filters.", body = [GuaranteeConvergenceDetailRow]),
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
        .grafana_guarantee_convergence_detail(q.start, q.end, q.core, wp_hash_bytes.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/guarantee-convergence/detail", e))
}

/// How fast assurances reach the validator set, summarised per assurance anchor.
///
/// One row per anchor — the header hash of the block an availability statement
/// refers to. Reception latency runs from DistributingAssurance(126) on a sender
/// to AssuranceReceived(131) on each validator that received that sender's
/// assurance; the latencies of every sender for the anchor are pooled before the
/// percentiles are taken. A second set of percentiles gives the distribution
/// start spread — how much later the remaining validators began distributing for
/// this anchor than the first one did. Assurances only count towards
/// availability while the report is still pending, a 5-slot (30 s) window.
///
/// Answers: do assurances for a block reach the validator set well inside the
/// 5-slot availability window, and which anchors converge slowly?
#[utoipa::path(
    get,
    path = "/api/grafana/assurance-convergence",
    params(ConvergenceQuery),
    responses(
        (status = 200, description = "Array of per-anchor rows, ascending by slot, each with the pooled DistributingAssurance(126) → AssuranceReceived(131) percentiles and the distribution start spread for that anchor. With `interval`: one row per time bucket instead, with percentiles over the latencies of all assurances in the bucket (approximate, latency-histogram based).", body = [AssuranceConvergenceRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn assurance_convergence(
    Query(q): Query<ConvergenceQuery>,
    State(state): State<ApiState>,
) -> Result<Response, StatusCode> {
    if let Some(interval) = &q.interval {
        state
            .store
            .grafana_assurance_convergence_hist(q.start, q.end, interval)
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/assurance-convergence", e))
    } else {
        state
            .store
            .grafana_assurance_convergence(q.start, q.end)
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/assurance-convergence", e))
    }
}

/// Per-sender assurance propagation — drill-down behind the per-anchor summary.
///
/// One row per (anchor, sender): the validator that emitted
/// DistributingAssurance(126) for that anchor, how many validators received its
/// assurance, and the spread of latencies until AssuranceReceived(131) on them.
/// Optional `anchor` and `node` filters isolate a single block or a single
/// suspect validator.
///
/// Answers: which validator's assurances propagate slowly, and to how many of
/// its peers?
#[utoipa::path(
    get,
    path = "/api/grafana/assurance-convergence/senders",
    params(AssuranceConvergenceSendersQuery),
    responses(
        (status = 200, description = "Array of rows, one per anchor and sender, ascending by the time that sender started distributing, each with its receiving-validator count and DistributingAssurance(126) → AssuranceReceived(131) latency percentiles. With `interval`: one row per time bucket instead, with percentiles over all sender latencies in the bucket, and the anchor filter no longer applies.", body = [AssuranceConvergenceSenderRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn assurance_convergence_senders(
    Query(q): Query<AssuranceConvergenceSendersQuery>,
    State(state): State<ApiState>,
) -> Result<Response, StatusCode> {
    if let Some(interval) = &q.interval {
        state
            .store
            .grafana_assurance_convergence_senders_hist(q.start, q.end, interval, q.node.as_deref())
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/assurance-convergence/senders", e))
    } else {
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
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/assurance-convergence/senders", e))
    }
}

/// Per-node data-availability activity: shard request counts, average shard
/// latency and shard inventory over the requested time range.
///
/// One row per node, totalled over the range. Shard work is counted from both
/// ends: requests the node made as an assurer fetching its shards
/// (SendingShardRequest(120), ShardRequestSent(123), ShardsTransferred(125)) and
/// requests it took in as a guarantor holding them (ReceivingShardRequest(121),
/// ShardRequestReceived(124)), alongside failures (ShardRequestFailed(122)) and
/// preimage announcement activity (PreimageAnnouncementFailed(190),
/// PreimageAnnounced(191), AnnouncedPreimageForgotten(192)). The two average
/// latencies cover the same two perspectives and include requests that ended in
/// failure, measured up to the failure.
///
/// Answers: which nodes carry the data-availability load, and which are slow or
/// failing at serving shards?
#[utoipa::path(
    get,
    path = "/api/grafana/da-stats",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array of per-node rows, busiest shard transferrer first, each with its shard and preimage event totals, sample-weighted average latency per perspective in milliseconds, and peak distinct-shard count.", body = [DaStatsRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn da_stats(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_da_stats(q.start, q.end, q.node.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/da-stats", e))
}

/// Shard transfer latency percentiles over time, seen from both ends of the
/// transfer.
///
/// One row per time bucket, with the two perspectives measured separately: the
/// assurer's round-trip from SendingShardRequest(120) to ShardsTransferred(125),
/// and the guarantor's time to take the request in, ReceivingShardRequest(121) to
/// ShardRequestReceived(124). Latencies from all reporting nodes are pooled per
/// bucket, so the percentiles are network-wide and approximate: values are
/// rounded up to a latency-bucket edge and saturate at 5 s. Requests that ended
/// in ShardRequestFailed(122) are included, measured up to the failure, and also
/// reported as a separate count.
///
/// Answers: is shard fetching slow because assurers are waiting on the network,
/// or because guarantors are slow to serve their shards?
#[utoipa::path(
    get,
    path = "/api/grafana/shard-latency",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket, each with approximate p50/p75/p95/p99/p100 latencies in milliseconds for the assurer round-trip and the guarantor request-intake side, their sample counts, and how many measurements ended in failure.", body = [ShardLatencyRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn shard_latency(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_shard_latency_raw(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/shard-latency", e))
}

/// Audit bundle recovery latency percentiles over time.
///
/// One row per time bucket. Auditors fetch erasure-coded shards from assurers
/// and reconstruct the original work-package bundle; each leg of that recovery is
/// reported separately — requesting and serving shards
/// (SendingBundleShardRequest(140) / ReceivingBundleShardRequest(141) →
/// BundleShardTransferred(145)), requesting and serving a whole bundle
/// (SendingBundleRequest(148) / ReceivingBundleRequest(149) →
/// BundleTransferred(153)), local reconstruction (ReconstructingBundle(146) →
/// BundleReconstructed(147)) and end-to-end recovery per audit. Latencies from
/// all reporting nodes are pooled per bucket; see BundleLatencyRow for the exact
/// event pairing behind each field.
///
/// Answers: how long does recovering an audit bundle take, and which leg of the
/// recovery dominates?
#[utoipa::path(
    get,
    path = "/api/grafana/bundle-latency",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket, each with approximate percentiles in milliseconds for shard request and serve, full-bundle request and serve, reconstruction and end-to-end recovery, their sample counts, and how many measurements ended in failure.", body = [BundleLatencyRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn bundle_latency(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_bundle_latency(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/bundle-latency", e))
}

/// Import segment fetching latency percentiles over time.
///
/// One row per time bucket. A guarantor fetches a work package's import segments
/// before refinement, so these latencies sit directly in the work-package
/// pipeline. Each leg is reported separately — requesting and serving segment
/// shards (SendingSegmentShardRequest(162) / ReceivingSegmentShardRequest(163) →
/// SegmentShardsTransferred(167)), requesting and serving whole segments
/// (SendingSegmentRequest(173) / ReceivingSegmentRequest(174) →
/// SegmentsTransferred(178)) and local reconstruction
/// (ReconstructingSegments(168) → SegmentsReconstructed(170)). Latencies from all
/// reporting nodes are pooled per bucket.
///
/// Answers: is import segment fetching delaying refinement, and is the delay in
/// the network or in reconstruction?
#[utoipa::path(
    get,
    path = "/api/grafana/segment-latency",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket, each with approximate percentiles in milliseconds for segment shard request and serve, whole-segment request and serve, and reconstruction, their sample counts, and how many measurements ended in failure.", body = [SegmentLatencyRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn segment_latency(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_segment_latency(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/segment-latency", e))
}

/// Preimage transfer latency percentiles over time.
///
/// One row per time bucket, split by role: the requestor's round-trip from
/// SendingPreimageRequest(193) to PreimageTransferred(198), and the responder's
/// local handling from ReceivingPreimageRequest(194) to PreimageTransferred(198).
/// Latencies from all reporting nodes are pooled per bucket.
///
/// Answers: how quickly do nodes obtain the preimages a service needs, and is a
/// slow transfer the requestor's or the responder's problem?
#[utoipa::path(
    get,
    path = "/api/grafana/preimage-latency",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket, each with approximate requestor-side and responder-side percentiles in milliseconds, their sample counts, and how many transfers ended in failure.", body = [PreimageLatencyRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn preimage_latency(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_preimage_latency(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/preimage-latency", e))
}

/// Work-package pipeline funnel over time, one row per time bucket.
///
/// The same per-stage counts as `/wp-funnel` — WorkPackageReceived(94),
/// Authorized(95), Refined(101), WorkReportBuilt(102), GuaranteeBuilt(105),
/// GuaranteesDistributed(109) and WorkPackageFailed(92) — with each work package
/// attributed to the bucket in which it was first observed, so its later stages
/// are counted in that same bucket even if they happened afterwards. The optional
/// `core` filter narrows to one core.
///
/// Answers: when did the pipeline start losing work packages, and at which stage?
#[utoipa::path(
    get,
    path = "/api/grafana/wp-funnel-timeseries",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket in ascending order, each with the per-stage work-package counts for that bucket. Narrowed by the optional core filter.", body = [WpFunnelTimeseriesRow]),
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

/// Per-stage work-package pipeline durations over time, one row per time bucket.
///
/// The same stage measurements as `/bottlenecks` — authorize
/// (WorkPackageReceived(94) → Authorized(95)) through distribute
/// (GuaranteeBuilt(105) → GuaranteesDistributed(109)), plus the total from
/// reception to distribution — with each work package attributed to the bucket in
/// which it was first observed. A stage's percentiles are null in buckets where no
/// work package reached that stage. The optional `core` filter narrows to one core.
///
/// Answers: when did a pipeline stage start slowing down, and which one?
#[utoipa::path(
    get,
    path = "/api/grafana/bottlenecks-timeseries",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket in ascending order, each with the per-stage median and p95 durations in milliseconds plus the bucket's work-package and failure counts. Narrowed by the optional core filter.", body = [BottlenecksTimeseriesRow]),
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

/// Per-guarantor pipeline performance — which guarantors are slow or failing?
///
/// Profiles the guarantor-side work-package pipeline: from receiving a WP
/// through authorization, refinement, report building, guarantee building,
/// to distribution. All stages execute on the same guarantor node. Only WPs
/// that completed the pipeline (distributed or failed) are included —
/// in-flight WPs are excluded.
///
/// | Pipeline stage  | Source event              | Type ID | Ordinal | wp_tracking column    |
/// |-----------------|---------------------------|---------|---------|-----------------------|
/// | Received        | WorkPackageReceived       | 94      | 0       | `received_at`         |
/// | Authorized      | WorkPackageAuthorized     | 95      | 1       | `authorized_at`       |
/// | Refined         | Refined                   | 101     | 2       | `refined_at`          |
/// | Report built    | WorkReportBuilt           | 102     | 3       | `report_built_at`     |
/// | Guarantee built | GuaranteeBuilt            | 105     | 4       | `guarantee_built_at`  |
/// | Distributed     | GuaranteesDistributed     | 109     | 5       | `distributed_at`      |
/// | Failed          | WorkPackageFailed         | 92      | —       | `failed_at`           |
///
/// `node_id` identifies the guarantor — set from WorkPackageReceived (94).
/// All subsequent stages execute on the same node.
///
/// `slowdown_factor` = `node_avg_total_ms / network_avg_total_ms` (>1.5 = underperformer).
/// Guarantors rotate across cores, so `core` is an optional drill-down filter.
#[utoipa::path(
    get,
    path = "/api/grafana/validator-profiling",
    params(ValidatorProfilingQuery),
    responses(
        (status = 200, description = "Per-guarantor pipeline performance. `nodes` sorted by avg_total_ms (slowest first by default). Only guarantors with completed WPs (distributed or failed) are included. `network_avg_total_ms` reflects all completed guarantors regardless of `limit`.", body = ValidatorProfilingResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn validator_profiling(
    Query(q): Query<ValidatorProfilingQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let ascending = q.sort.as_deref() == Some("asc");
    state
        .store
        .grafana_validator_profiling(q.start, q.end, q.core, q.limit, ascending)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/validator-profiling", e))
}

/// Per-guarantor pipeline performance over time.
///
/// Time-bucketed variant of `/api/grafana/validator-profiling`. Same guarantor
/// pipeline stages (see that endpoint's docs for the full event→column mapping).
/// Results are grouped by `time_bucket(interval, first_seen)` and `node_id`.
///
/// When `node` is provided, returns per-bucket averages for that single guarantor
/// (suitable for sparklines / per-node detail charts). When omitted, returns
/// only the top ~20 slowest guarantors per bucket to avoid 1024×N result explosion.
#[utoipa::path(
    get,
    path = "/api/grafana/validator-profiling-timeseries",
    params(ValidatorProfilingTimeseriesQuery),
    responses(
        (status = 200, description = "Per-guarantor pipeline performance per time bucket. When node is provided: one row per bucket for that guarantor. When omitted: top ~20 slowest guarantors per bucket.", body = [ValidatorProfilingTimeseriesRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn validator_profiling_timeseries(
    Query(q): Query<ValidatorProfilingTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_validator_profiling_timeseries(q.start, q.end, interval, q.core, q.node.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/validator-profiling-timeseries", e))
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
        let filtered: Vec<_> = all
            .iter()
            .filter(|m| ids.contains(&m.id))
            .cloned()
            .collect();
        Json(filtered)
    } else {
        Json(all.to_vec())
    }
}

/// Search raw events from `ingested_raw_events` with filtering and pagination.
///
/// Returns events matching the given filters, ordered by timestamp DESC. All 115
/// event types are browsable (1h retention after migration 020). Supports filtering
/// by event type, node, core (hot column), and wp_hash (hot column). Returns
/// paginated response with total count for UI pagination controls.
///
/// The `event_types` parameter is optional — if omitted, returns all types. When
/// provided, accepts numeric IDs (as defined in JIP-3), group names (e.g.
/// `wp_pipeline`, `failures`), or event names (e.g. `Authored`), expanded
/// server-side via `expand_event_types()`.
#[utoipa::path(
    get,
    path = "/api/grafana/events",
    params(EventsQuery),
    responses(
        (status = 200, description = "Paginated event records with total count", body = EventsSearchResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn events(
    Query(q): Query<EventsQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let event_types = q
        .event_types
        .as_deref()
        .map(crate::event_type_meta::expand_event_types)
        .unwrap_or_default();
    let limit = q.limit.unwrap_or(500);
    let offset = q.offset.unwrap_or(0);

    // Parse wp_hash from hex string to bytes
    let wp_hash_bytes = q.wp_hash.as_deref().and_then(|h| {
        let h = h.strip_prefix("0x").unwrap_or(h);
        hex::decode(h).ok()
    });

    state
        .store
        .grafana_events(
            q.start,
            q.end,
            &event_types,
            limit,
            offset,
            q.node.as_deref(),
            q.core,
            wp_hash_bytes.as_deref(),
        )
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/events", e))
}

/// Guarantees dropped from validators' local guarantee pools over time, split by reason.
///
/// Counts GuaranteeDiscarded(113) per time bucket and per reason. The reasons are
/// the JIP-3 discard reasons: PackageReportedOnChain(0) — the work package was
/// already reported on-chain, ReplacedByBetter(1), CannotReportOnChain(2),
/// TooManyGuarantees(3), Other(4). Only reason 0 means the guarantee reached its
/// intended end; the others mean guarantor work was thrown away.
///
/// Answers: why are guarantees leaving the pool without being reported on-chain,
/// and is that getting worse over time?
#[utoipa::path(
    get,
    path = "/api/grafana/guarantee-discards",
    params(GuaranteeDiscardsQuery),
    responses(
        (status = 200, description = "Array of (bucket timestamp, discard reason, count) rows, ordered by bucket then reason; one row per reason seen in each bucket.", body = [GuaranteeDiscardRow]),
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

// ── Phase 3: New grafana endpoints ───────────────────────────────────────

/// Network failure rates with per-category, per-node breakdown and recent failures.
///
/// **Question answered:** "What's failing across the network, how badly, and where?"
///
/// **Data source:** `all_event_stats_1m` UNION view for aggregate counts.
/// `ingested_raw_events` (1h retention) for recent failure details with reason
/// text extracted from JSONB.
///
/// **Categories and their failure event types (JIP-3):**
/// - block_authoring: AuthoringFailed, BlockVerificationFailed, BlockExecutionFailed
/// - tickets: TicketGenerationFailed, TicketTransferFailed
/// - work_packages: WorkPackageFailed, WorkPackageSharingFailed
/// - guarantees: GuaranteeSendFailed, GuaranteeReceiveFailed, GuaranteeDiscarded
/// - shards: ShardRequestFailed
/// - assurances: AssuranceSendFailed
///
/// Each category's rate = failures / (successes + failures). Overall rate spans
/// all categories. `by_node` returns the top 20 nodes by failure count.
/// `recent_failures` returns the last 20 failure events from the past 5 minutes
/// with reason text from JSONB and human-readable event name.
#[utoipa::path(
    get,
    path = "/api/grafana/failure-rates",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Failure rates with breakdown", body = FailureRatesResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn failure_rates(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_failure_rates(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/failure-rates", e))
}

/// How much of the network is keeping up with the chain tip, over time.
///
/// One row per time bucket. Each node's highest best-block slot in the bucket
/// comes from its BestBlockChanged(11) reports; `network_slot` is the highest
/// slot any node reported, and a node counts as synced when its own best slot is
/// within 2 slots (about 12 s) of it. Only nodes that reported
/// BestBlockChanged(11) in the bucket are counted, so a node that stops
/// reporting disappears from the row instead of showing up as behind. This is an
/// observed measure and is independent of the node's own subjective
/// SyncStatusChanged(13) flag. `interval` defaults to 5m and is snapped to a
/// supported bucket width.
///
/// Answers: is the network in sync, and how many nodes are lagging behind the
/// chain tip?
#[utoipa::path(
    get,
    path = "/api/grafana/sync-timeline",
    params(TimeseriesQuery),
    responses(
        (status = 200, description = "Array of per-bucket rows, ascending by time, each with the highest best-block slot seen in the network and how many reporting nodes were at the tip versus behind it.", body = [SyncTimelineRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn sync_timeline(
    Query(q): Query<TimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("5m");
    state
        .store
        .grafana_sync_timeline(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/sync-timeline", e))
}

/// Network connection activity over time.
///
/// **Question answered:** "How are node connections changing over time?"
///
/// **Data source:** `all_event_stats_30s` for ConnectedIn, ConnectedOut, and
/// Disconnected events. `nodes` table for overall health stats (maintained by
/// batch_writer on connect/disconnect).
///
/// Timeline shows per-bucket: connections (ConnectedIn + ConnectedOut),
/// disconnections (Disconnected), and active_nodes (distinct node_ids).
/// Health stats show total_nodes_seen and currently_connected from the nodes table.
#[utoipa::path(
    get,
    path = "/api/grafana/connections-timeline",
    params(TimeseriesQuery),
    responses(
        (status = 200, description = "Connection activity timeline", body = ConnectionsTimelineResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn connections_timeline(
    Query(q): Query<TimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("5m");
    state
        .store
        .grafana_connections_timeline(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/connections-timeline", e))
}

/// Network-wide guarantee counts for every stage of guarantee distribution, plus success rates.
///
/// Counts each guaranteeing event over the time range: GuaranteeBuilt(105),
/// SendingGuarantee(106), GuaranteeSendFailed(107), GuaranteeSent(108),
/// GuaranteesDistributed(109) on the guarantor side, and ReceivingGuarantee(110),
/// GuaranteeReceiveFailed(111), GuaranteeReceived(112), GuaranteeDiscarded(113)
/// on the receiving side. Send success is GuaranteeSent(108) over all send
/// attempts (106 + 107 + 108), receive success is GuaranteeReceived(112) over all
/// receive attempts (110 + 111 + 112); both are 1.0 when there was no activity.
///
/// Answers: how much guaranteeing traffic is the network carrying, and what
/// fraction of guarantee transfers succeeds?
#[utoipa::path(
    get,
    path = "/api/grafana/guarantees",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with a per-event-type count block and a send/receive success-rate block for the whole time range.", body = GuaranteesResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantees(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_guarantees(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/guarantees", e))
}

/// Guaranteeing activity per guarantor node, with the cores each one was seen guaranteeing for.
///
/// One row per node that emitted GuaranteeBuilt(105) in the time range, with how
/// many guarantees it built, when it last built one, and the set of cores those
/// guarantees were for. Nodes that built no guarantees do not appear. Guarantee
/// propagation records are retained for 90 days, which bounds how far back the
/// time range can reach.
///
/// **Caveat:** the node→core association is what was observed, not the protocol's
/// validator→core assignment. JAM rotates core assignments every 10 slots and
/// reshuffles them each epoch, so a node legitimately appears on several cores
/// over any range longer than one rotation.
///
/// Answers: which nodes are actually guaranteeing, for which cores, and how
/// evenly is guaranteeing work spread across them?
#[utoipa::path(
    get,
    path = "/api/grafana/guarantees/by-guarantor",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object holding the guarantor count and an array of per-node rows, ordered by guarantees built, most active first.", body = GuarantorBreakdownResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantees_by_guarantor(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_guarantees_by_guarantor(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/guarantees/by-guarantor", e))
}

/// Work-package pipeline totals for the range, with a per-core breakdown.
///
/// `totals` combines two views. The stage counts say how many work packages
/// reached each guarantor pipeline stage, from received (WorkPackageReceived(94))
/// through distributed (GuaranteesDistributed(109)) and failed
/// (WorkPackageFailed(92)). The pre-pipeline figures count the
/// WorkPackageSubmission(90), WorkPackageBeingShared(91) and
/// DuplicateWorkPackage(93) events reported by all nodes — these are event counts,
/// not distinct work packages, and a duplicate is reported instead of a reception,
/// so duplicates never appear in the stage counts. `by_core` counts the work
/// packages first observed in the range per core they were assigned to.
///
/// Answers: how much work-package traffic did the network handle, how far did it
/// get, and how is it spread across cores?
#[utoipa::path(
    get,
    path = "/api/grafana/wp-stats",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with the pre-pipeline and per-stage totals for the range plus one count per core.", body = WpStatsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_stats(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_wp_stats(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp-stats", e))
}

/// Node→core mapping based on observed guarantee behavior.
///
/// **Question answered:** "Which node is active on which core?"
///
/// **Data source:** `guarantee_convergence` table (builder_node_id + core, 90d
/// retention). Returns primary core (most guarantees built) per node, plus total
/// guarantee count. Shares `node_core_mapping()` helper with
/// `/guarantees/by-guarantor`.
///
/// **Caveat:** Reflects observed guarantee behavior, not protocol-level
/// validator→core assignment. Nodes that haven't built any guarantees in the
/// time range won't appear. See `/guarantees/by-guarantor` for the same caveat.
#[utoipa::path(
    get,
    path = "/api/grafana/validators/cores",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Node to core mapping", body = [ValidatorCoreRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn validators_cores(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_validators_cores(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/validators/cores", e))
}

/// Multi-signal network health score with per-component breakdown.
///
/// **Question answered:** "Is the network healthy? Which subsystems are degraded?"
///
/// **Data source:** `all_event_stats_1m` for event counts. `nodes` table for
/// connectivity. Each component scored 0-100:
///
/// - **block_production:** Authored / (Authored + AuthoringFailed +
///   BlockVerificationFailed + BlockExecutionFailed). Healthy >= 95%.
/// - **work_packages:** WorkPackageReceived / (Received + WorkPackageFailed +
///   WorkPackageSharingFailed). Healthy >= 95%.
/// - **data_availability:** ShardsTransferred / (Transferred + ShardRequestFailed).
///   Healthy >= 95%.
/// - **connectivity:** connected_nodes / total_nodes from `nodes` table.
///   Healthy >= 90%.
/// - **event_throughput:** non-zero total events = 100, zero = 0.
///
/// Overall health_score = average of 5 component scores. Status: healthy >= 90,
/// degraded >= 70, unhealthy < 70.
#[utoipa::path(
    get,
    path = "/api/grafana/network-health",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Network health score and breakdown", body = NetworkHealthResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn network_health(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_network_health(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/network-health", e))
}

// ── Phase 4: Moderate endpoints ──────────────────────────────────────────

/// Recent work packages, with a pipeline health summary of the whole range.
///
/// `work_packages` lists the most recently started work packages of the range (at
/// most 200, newest first), each with its stage timestamps from
/// WorkPackageReceived(94) through GuaranteesDistributed(109), the gas its
/// Refined(101) reported, how many guarantors received it and how many built a
/// guarantee for it, and the reason from WorkPackageFailed(92) where one was
/// reported. The summaries alongside cover every work package first observed in the
/// range, not only the listed ones: `summary` counts the work packages whose
/// furthest reached stage is each stage, so it shows where work stalled; `reached`
/// counts those that ever reached each stage; `stage_duration_percentiles` gives
/// per-stage median and p95 durations; `failure_breakdown` groups failures by
/// reported reason.
///
/// Answers: which work packages ran recently, where are they stalling, and why are
/// they failing?
#[utoipa::path(
    get,
    path = "/api/grafana/wp-active",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with the recent work-package list and the range-wide stage counts, per-stage duration percentiles and failure-reason breakdown.", body = WpActiveResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_active(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_wp_active(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp-active", e))
}

/// Everything known about one work package, looked up by its hash.
///
/// `summary` is that work package's pipeline timeline — the timestamps of
/// WorkPackageReceived(94), Authorized(95), Refined(101), WorkReportBuilt(102),
/// GuaranteeBuilt(105), GuaranteesDistributed(109) and WorkPackageFailed(92), the
/// services it touched, and how many guarantors received and guaranteed it — and
/// stays available long after the work package finished. `events` is the raw
/// telemetry timeline for the same work package in emission order; raw events are
/// retained for about an hour, so for older work packages this array is empty while
/// the summary remains. The hash is hex-encoded, with or without a `0x` prefix.
///
/// Answers: what exactly happened to this one work package, and where did it stop?
#[utoipa::path(
    get,
    path = "/api/grafana/wp/{wp_hash}",
    params(
        ("wp_hash" = String, Path, description = "Work package hash (hex-encoded)")
    ),
    responses(
        (status = 200, description = "Single object with the work package's pipeline timeline (null if the hash is unknown) and its raw event timeline, oldest event first.", body = WpDetailResponse),
        (status = 400, description = "Work package hash is not valid hex"),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_detail(
    Path(wp_hash): Path<String>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let hash_str = wp_hash.strip_prefix("0x").unwrap_or(&wp_hash);
    let hash_bytes = hex::decode(hash_str).map_err(|_| StatusCode::BAD_REQUEST)?;

    state
        .store
        .grafana_wp_detail(&hash_bytes)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp/{hash}", e))
}

/// Pipeline timelines for several work packages in one request.
///
/// The request body is a JSON array of hex-encoded work-package hashes, with or
/// without `0x` prefixes. Each response row is one work package's pipeline
/// timeline, the same summary `/wp/{wp_hash}` returns: the stage timestamps from
/// WorkPackageReceived(94) through GuaranteesDistributed(109), WorkPackageFailed(92)
/// where reported, and the guarantor counts. Hashes that are unknown or not valid
/// hex are simply absent, so the response can be shorter than the request.
///
/// Answers: how far through the pipeline did each of these specific work packages
/// get?
#[utoipa::path(
    post,
    path = "/api/grafana/wp/batch",
    request_body = Vec<String>,
    responses(
        (status = 200, description = "Array of pipeline timelines, one per work package that was found, newest first.", body = [WpTrackingRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_batch(
    State(state): State<ApiState>,
    Json(hashes): Json<Vec<String>>,
) -> Result<impl IntoResponse, StatusCode> {
    let hash_bytes: Vec<Vec<u8>> = hashes
        .iter()
        .filter_map(|h| {
            let h = h.strip_prefix("0x").unwrap_or(h);
            hex::decode(h).ok()
        })
        .collect();

    state
        .store
        .grafana_wp_batch(&hash_bytes)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp/batch", e))
}

/// Block production and import health for the range, plus the current chain tips.
///
/// `totals` counts the block lifecycle events reported by the whole network in
/// the range: Authoring(40), AuthoringFailed(41), Authored(42), Importing(43),
/// BlockVerificationFailed(44), BlockVerified(45), BlockExecutionFailed(46),
/// BlockExecuted(47), plus BestBlockChanged(11) and FinalizedBlockChanged(12).
/// Every node reports its own import of a block, so the import-side counts scale
/// with the number of nodes, while `authored` is one per block. `chain` gives the
/// current best and finalized slot as of the request, not over the range, and is
/// null when live chain tracking is unavailable. `authoring_by_node` ranks the 50
/// most active authors by Authored(42) count.
///
/// Answers: are blocks being produced, verified and executed successfully, and
/// which nodes are authoring them?
#[utoipa::path(
    get,
    path = "/api/grafana/blocks/summary",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with the range-wide block-event totals, the current best and finalized slot, and the 50 most active authoring nodes.", body = BlocksSummaryResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn blocks_summary(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut result = state
        .store
        .grafana_blocks_summary(q.start, q.end)
        .await
        .map_err(|e| map_sqlx_error("grafana/blocks/summary", e))?;

    // Overlay LiveCounters for current slot numbers
    if let Some(ref tracker) = state.metrics_tracker {
        let lc = tracker.live_counters();
        result.chain.best_slot = Some(lc.latest_slot() as i32);
        result.chain.finalized_slot = Some(lc.finalized_slot() as i32);
    }

    Ok(Json(result))
}

/// Core performance metrics — efficiency, latency, throughput, gas.
///
/// **Question answered:** "How is this core performing?"
///
/// **Data source:** `all_core_stats_1m` for event counts (processing efficiency).
/// `wp_tracking` for pipeline latency percentiles and gas totals.
#[utoipa::path(
    get,
    path = "/api/grafana/cores/{core_id}/metrics",
    params(
        ("core_id" = i16, Path, description = "Core index (0–340)"),
        TimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Core performance metrics", body = CoreMetricsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn core_metrics(
    Path(core_id): Path<i16>,
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_core_metrics(q.start, q.end, core_id)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/cores/{id}/metrics", e))
}

/// Per-core validator (guarantor) list with node metadata.
///
/// **Question answered:** "Which validators are active guarantors for this core?"
///
/// **Data source:** `guarantee_convergence` table filtered by core, JOINed with
/// `nodes` table for implementation details. Only includes validators who actually
/// built guarantees — inactive validators don't appear. Shares `node_core_mapping()`
/// infrastructure.
///
/// Also replaces legacy `/api/cores/{id}/guarantors` and `/guarantors/enhanced`.
/// DA metrics per guarantor available separately from `/api/grafana/da-stats?node=X`.
#[utoipa::path(
    get,
    path = "/api/grafana/cores/{core_id}/validators",
    params(
        ("core_id" = i16, Path, description = "Core index (0–340)"),
        TimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Validators active on this core", body = CoreValidatorsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn core_validators(
    Path(core_id): Path<i16>,
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_core_validators(q.start, q.end, core_id)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/cores/{id}/validators", e))
}

// ── Phase 5: Hard rewrites ──────────────────────────────────────────────

/// Execution performance metrics — gas and timing per processing phase.
///
/// **Question answered:** "How much gas and time does each execution phase use?"
///
/// **Data source:** `event_services` table (7-day retention) with pre-extracted
/// gas and timing columns from three event types:
/// - Authorized (type 95): `is_authorized` PVM call cost
/// - Refined (type 101): per-item `refine` PVM call costs
/// - BlockExecuted (type 47): per-service `accumulate` PVM call costs
///
/// Per-service gas/timing breakdown available for all three phases.
/// Each `by_service` entry includes a `phase` field.
#[utoipa::path(
    get,
    path = "/api/grafana/execution",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Execution performance by phase", body = ExecutionMetricsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn execution_metrics(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_execution(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/execution", e))
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

/// Parameters for on-chain core timeseries queries (with optional core filter).
#[derive(Deserialize, IntoParams)]
pub struct OnchainCoreTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Core index to filter by. Without this, returns network-wide aggregates.
    pub core: Option<i16>,
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

/// Time-bucketed on-chain core stats.
///
/// Without `core` filter: network-wide aggregate — one row per time bucket
/// with SUMmed fields across all cores (AVG for popularity).
///
/// With `core` filter: per-core timeseries — one row per time bucket for
/// the specified core.
///
/// Data source: `onchain_core_stats` with `time_bucket()` aggregation.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/cores/timeseries",
    params(OnchainCoreTimeseriesQuery),
    responses(
        (status = 200, description = "Without core filter: network-wide aggregate — one row \
            per time bucket with SUMmed fields across all cores. \
            With core filter: per-core timeseries for the specified core.",
            body = [OnchainCoreTimeseriesAgg]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_cores_timeseries(
    Query(q): Query<OnchainCoreTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    if let Some(core) = q.core {
        let rows = state
            .store
            .onchain_cores_timeseries(q.start, q.end, interval, core)
            .await
            .map_err(|e| map_sqlx_error("onchain/cores/timeseries", e))?;
        Ok(Json(serde_json::to_value(rows).unwrap()))
    } else {
        let rows = state
            .store
            .onchain_cores_timeseries_agg(q.start, q.end, interval)
            .await
            .map_err(|e| map_sqlx_error("onchain/cores/timeseries", e))?;
        Ok(Json(serde_json::to_value(rows).unwrap()))
    }
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

/// Time-bucketed on-chain validator stats.
///
/// Without `validator` filter: network-wide aggregate — one row per time bucket
/// with SUMmed fields across all validators.
///
/// With `validator` filter: per-validator timeseries — one row per time bucket
/// for the specified validator(s). MAX aggregation (epoch-cumulative values).
///
/// Data source: `onchain_validator_stats` with `time_bucket()` aggregation.
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/validators/timeseries",
    params(OnchainValidatorTimeseriesQuery),
    responses(
        (status = 200, description = "Without validator filter: network-wide aggregate — one row \
            per time bucket with SUMmed fields across all validators. \
            With validator filter: per-validator timeseries with MAX aggregation (epoch-cumulative).",
            body = [OnchainValidatorTimeseriesAgg]),
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
    if let Some(ref validator_str) = q.validator {
        let validators = parse_validator_indices(validator_str);
        let rows = state
            .store
            .onchain_validators_timeseries(q.start, q.end, interval, &validators)
            .await
            .map_err(|e| map_sqlx_error("onchain/validators/timeseries", e))?;
        Ok(Json(serde_json::to_value(rows).unwrap()))
    } else {
        let rows = state
            .store
            .onchain_validators_timeseries_agg(q.start, q.end, interval)
            .await
            .map_err(|e| map_sqlx_error("onchain/validators/timeseries", e))?;
        Ok(Json(serde_json::to_value(rows).unwrap()))
    }
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
