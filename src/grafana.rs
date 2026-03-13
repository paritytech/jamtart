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
        event_types,
        events,
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
        EventRow,
        crate::event_type_meta::EventTypeMeta,
    )),
    tags(
        (name = "grafana", description = "Grafana dashboard API — time-series, aggregates, and metadata")
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
        .route("/event-types", get(event_types))
        .route("/events", get(events))
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
    /// Bucket width. Allowed: 10s, 15s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h, 2h, 4h, 6h, 12h, 1d
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

// ── Helper functions ───────────────────────────────────────────────────

/// Strip Grafana multi-select curly-brace wrapper: `{a,b}` → `a,b`.
fn strip_grafana_braces(s: &str) -> &str {
    s.strip_prefix('{').and_then(|s| s.strip_suffix('}')).unwrap_or(s)
}

/// Parse comma-separated service IDs (supports both decimal and 0x hex).
/// Hex values are parsed as u32 then cast to i32 to match the DB representation
/// (service IDs are u32 in JAM but stored as PostgreSQL INT which is signed).
fn parse_service_ids(s: &str) -> Vec<i32> {
    strip_grafana_braces(s)
        .split(',')
        .filter_map(|v| {
            let v = v.trim();
            if let Some(hex) = v.strip_prefix("0x") {
                u32::from_str_radix(hex, 16).ok().map(|n| n as i32)
            } else {
                v.parse().ok()
            }
        })
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
#[utoipa::path(
    get,
    path = "/api/grafana/timeseries",
    params(TimeseriesQuery),
    responses(
        (status = 200, description = "Time-bucketed event counts", body = Vec<TimeseriesRow>),
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
#[utoipa::path(
    get,
    path = "/api/grafana/cores",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Per-core summary", body = Vec<CoreSummary>),
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
#[utoipa::path(
    get,
    path = "/api/grafana/cores/{core_id}",
    params(
        ("core_id" = i16, Path, description = "Core index"),
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
#[utoipa::path(
    get,
    path = "/api/grafana/blocks/convergence",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Convergence percentiles", body = Vec<BlockConvergenceRow>),
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
#[utoipa::path(
    get,
    path = "/api/grafana/blocks/contents",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Block contents", body = Vec<BlockContentsRow>),
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
#[utoipa::path(
    get,
    path = "/api/grafana/services",
    params(ServiceQuery),
    responses(
        (status = 200, description = "Service totals", body = Vec<ServiceRow>),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn services(
    Query(q): Query<ServiceQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let services: Option<Vec<i32>> = q.service.map(|s| parse_service_ids(&s));
    state
        .store
        .grafana_services(q.start, q.end, services.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/services", e))
}

/// Time-bucketed per-service metrics (WP counts and gas usage).
#[utoipa::path(
    get,
    path = "/api/grafana/services/timeseries",
    params(ServiceTimeseriesQuery),
    responses(
        (status = 200, description = "Service time-series", body = Vec<ServiceTimeseriesRow>),
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
    let services: Option<Vec<i32>> = q.service.map(|s| parse_service_ids(&s));

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
#[utoipa::path(
    get,
    path = "/api/grafana/nodes",
    responses(
        (status = 200, description = "Node list", body = Vec<NodeRow>),
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

/// Raw node status rows at ~2s granularity.
#[utoipa::path(
    get,
    path = "/api/grafana/node-stats",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Node stats", body = Vec<NodeStatsRow>),
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

/// 1-minute aggregated node stats. Network-wide without node filter, per-node with.
#[utoipa::path(
    get,
    path = "/api/grafana/node-stats-aggregate",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Aggregated node stats", body = Vec<NodeStatsAggregateRow>),
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
#[utoipa::path(
    get,
    path = "/api/grafana/db-stats",
    responses(
        (status = 200, description = "Database stats", body = DbStatsResponse),
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
#[utoipa::path(
    get,
    path = "/api/grafana/bottlenecks",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Pipeline bottlenecks", body = Vec<BottlenecksResponse>),
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
#[utoipa::path(
    get,
    path = "/api/grafana/wp-funnel",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Pipeline funnel", body = WpFunnelResponse),
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

/// Static metadata for all telemetry event types, optionally filtered by group.
#[utoipa::path(
    get,
    path = "/api/grafana/event-types",
    params(EventTypesParams),
    responses(
        (status = 200, description = "Event type metadata", body = Vec<crate::event_type_meta::EventTypeMeta>),
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

/// Raw events from the events hypertable, filtered by event type.
#[utoipa::path(
    get,
    path = "/api/grafana/events",
    params(EventsQuery),
    responses(
        (status = 200, description = "Raw events", body = Vec<EventRow>),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_service_ids_basic() {
        assert_eq!(parse_service_ids("10,20"), vec![10, 20]);
    }

    #[test]
    fn test_parse_service_ids_hex() {
        assert_eq!(parse_service_ids("0xa,0x14"), vec![10, 20]);
    }

    #[test]
    fn test_parse_service_ids_hex_overflow() {
        // 0xea9f727c = 3936318076 as u32, wraps to -358649220 as i32
        let result = parse_service_ids("0xea9f727c");
        assert_eq!(result, vec![0xea9f727c_u32 as i32]);
    }

    #[test]
    fn test_parse_service_ids_curly_braces() {
        assert_eq!(parse_service_ids("{0xa,0x14}"), vec![10, 20]);
    }

    #[test]
    fn test_parse_service_ids_mixed() {
        assert_eq!(parse_service_ids("10,0x14"), vec![10, 20]);
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
