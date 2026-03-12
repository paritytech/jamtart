//! Grafana integration endpoints. Exposes time-series and aggregate query routes
//! consumed by Grafana dashboards for network-wide telemetry visualization.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;

use crate::api::ApiState;

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/timeseries", get(timeseries))
        .route("/stats", get(stats))
        .route("/cores", get(cores))
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

#[derive(Deserialize)]
struct TimeseriesQuery {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    interval: Option<String>,
    group_by: Option<String>,
    node: Option<String>,
    event_types: Option<String>,
}

#[derive(Deserialize)]
struct TimeRangeQuery {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    node: Option<String>,
    core: Option<i16>,
    event_type: Option<i16>,
}

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
        )
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/timeseries", e))
}

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
        if let Some(obj) = result.as_object_mut() {
            obj.insert(
                "events_per_sec_10s".into(),
                serde_json::json!(last_10s.events as f64 / 10.0),
            );
            obj.insert(
                "blocks_per_sec_10s".into(),
                serde_json::json!(last_10s.blocks as f64 / 10.0),
            );
            obj.insert("best_slot".into(), serde_json::json!(lc.latest_slot()));
            obj.insert(
                "finalized_slot".into(),
                serde_json::json!(lc.finalized_slot()),
            );
            obj.insert("active_nodes".into(), serde_json::json!(active_nodes));
        }
    }

    Ok(Json(result))
}

async fn cores(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_cores(q.start, q.end, q.core)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/cores", e))
}

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

async fn services(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_services(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/services", e))
}

#[derive(Deserialize)]
struct ServiceTimeseriesQuery {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    interval: Option<String>,
    service: Option<String>,
    event_types: Option<String>,
}

async fn services_timeseries(
    Query(q): Query<ServiceTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    let services: Option<Vec<i32>> = q.service.map(|s| {
        s.split(',')
            .filter_map(|v| v.trim().parse().ok())
            .collect()
    });
    let event_types: Option<Vec<i16>> = q
        .event_types
        .map(|s| crate::event_type_meta::expand_event_types(&s))
        .filter(|v| !v.is_empty());

    state
        .store
        .grafana_services_timeseries(
            q.start,
            q.end,
            interval,
            services.as_deref(),
            event_types.as_deref(),
        )
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/services/timeseries", e))
}

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

async fn node_stats(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let nodes: Option<Vec<String>> = q.node.map(|n| {
        n.split(',').map(|s| s.trim().to_string()).collect()
    });
    state
        .store
        .grafana_node_stats(q.start, q.end, nodes.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/node-stats", e))
}

async fn node_stats_aggregate(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let nodes: Option<Vec<String>> = q.node.map(|n| {
        n.split(',').map(|s| s.trim().to_string()).collect()
    });
    state
        .store
        .grafana_node_stats_aggregate(q.start, q.end, nodes.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/node-stats-aggregate", e))
}

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

#[derive(Deserialize)]
struct EventTypesParams {
    group: Option<String>,
}

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
