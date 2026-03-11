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
        .route("/nodes", get(nodes))
        .route("/node-stats", get(node_stats))
        .route("/node-stats-aggregate", get(node_stats_aggregate))
        .route("/db-stats", get(db_stats))
        .route("/bottlenecks", get(bottlenecks))
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
}

async fn timeseries(
    Query(q): Query<TimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    let event_types: Option<Vec<i16>> = q.event_types.map(|s| {
        s.split(',')
            .filter_map(|v| v.trim().parse().ok())
            .collect()
    });

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
        .map_err(|e| {
            tracing::error!("grafana/timeseries error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn stats(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_stats(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| {
            tracing::error!("grafana/stats error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
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
        .map_err(|e| {
            tracing::error!("grafana/cores error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn blocks_convergence(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_blocks_convergence(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| {
            tracing::error!("grafana/blocks/convergence error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
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
        .map_err(|e| {
            tracing::error!("grafana/blocks/contents error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
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
        .map_err(|e| {
            tracing::error!("grafana/services error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn nodes(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_nodes()
        .await
        .map(Json)
        .map_err(|e| {
            tracing::error!("grafana/nodes error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
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
        .map_err(|e| {
            tracing::error!("grafana/node-stats error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
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
        .map_err(|e| {
            tracing::error!("grafana/node-stats-aggregate error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
}

async fn db_stats(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_db_stats()
        .await
        .map(Json)
        .map_err(|e| {
            tracing::error!("grafana/db-stats error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
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
        .map_err(|e| {
            tracing::error!("grafana/bottlenecks error: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
}
