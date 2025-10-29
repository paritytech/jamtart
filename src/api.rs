use crate::event_broadcaster::EventBroadcaster;
use crate::health::HealthMonitor;
use crate::server::TelemetryServer;
use crate::store::EventStore;
use axum::extract::ws::{Message, WebSocket};
use axum::{
    extract::{Path, Query, State, WebSocketUpgrade},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::CorsLayer;
use tracing::{error, info, warn};

/// Validates that a node_id is a valid 64-character hexadecimal string (32 bytes encoded).
fn is_valid_node_id(node_id: &str) -> bool {
    node_id.len() == 64 && node_id.chars().all(|c| c.is_ascii_hexdigit())
}

const MAX_QUERY_LIMIT: i64 = 1000;

/// Helper function to safely serialize responses for WebSocket messages.
/// Returns None if serialization fails, allowing graceful error handling.
fn serialize_ws_message<T: Serialize>(data: &T) -> Option<String> {
    match serde_json::to_string(data) {
        Ok(msg) => Some(msg),
        Err(e) => {
            error!("Failed to serialize WebSocket message: {}", e);
            None
        }
    }
}

#[derive(Clone)]
pub struct ApiState {
    pub store: Arc<EventStore>,
    pub telemetry_server: Arc<TelemetryServer>,
    pub broadcaster: Arc<EventBroadcaster>,
    pub health_monitor: Arc<HealthMonitor>,
}

pub fn create_api_router(state: ApiState) -> Router {
    Router::new()
        .route("/api/health", get(health_check))
        .route("/api/health/detailed", get(detailed_health_check))
        .route("/api/stats", get(get_stats))
        .route("/api/nodes", get(get_nodes))
        .route("/api/nodes/:node_id", get(get_node_details))
        .route("/api/nodes/:node_id/events", get(get_node_events))
        .route("/api/events", get(get_recent_events))
        .route("/api/ws", get(websocket_handler))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn health_check() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "service": "tart-backend",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

async fn detailed_health_check(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let health_report = state.health_monitor.get_health().await;

    let status_code = match health_report.status {
        crate::health::HealthStatus::Healthy => StatusCode::OK,
        crate::health::HealthStatus::Degraded => StatusCode::OK, // Still returning 200 for degraded
        crate::health::HealthStatus::Unhealthy => StatusCode::SERVICE_UNAVAILABLE,
    };

    Ok((status_code, Json(health_report)))
}

async fn get_stats(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    match state.store.get_stats().await {
        Ok(stats) => Ok(Json(stats)),
        Err(e) => {
            error!("Failed to get stats: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_nodes(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    match state.store.get_nodes().await {
        Ok(nodes) => Ok(Json(serde_json::json!({ "nodes": nodes }))),
        Err(e) => {
            error!("Failed to get nodes: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_node_details(
    Path(node_id): Path<String>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    // Validate node_id format
    if !is_valid_node_id(&node_id) {
        warn!("Invalid node_id format: {}", node_id);
        return Err(StatusCode::BAD_REQUEST);
    }

    match state.store.get_nodes().await {
        Ok(nodes) => {
            if let Some(node) = nodes.into_iter().find(|n| n["node_id"] == node_id) {
                Ok(Json(node))
            } else {
                Err(StatusCode::NOT_FOUND)
            }
        }
        Err(e) => {
            error!("Failed to get node details: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

#[derive(Deserialize)]
struct EventsQuery {
    limit: Option<i64>,
    #[allow(dead_code)]
    offset: Option<i64>,
}

async fn get_node_events(
    Path(node_id): Path<String>,
    Query(query): Query<EventsQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    // Validate node_id format
    if !is_valid_node_id(&node_id) {
        warn!("Invalid node_id format: {}", node_id);
        return Err(StatusCode::BAD_REQUEST);
    }

    let limit = query.limit.unwrap_or(50).clamp(1, MAX_QUERY_LIMIT);

    // Use database-level filtering for optimal performance (avoids N+1 query pattern)
    match state.store.get_recent_events_by_node(&node_id, limit).await {
        Ok(events) => {
            let has_more = events.len() as i64 == limit;
            Ok(Json(serde_json::json!({
                "events": events,
                "has_more": has_more
            })))
        }
        Err(e) => {
            error!("Failed to get node events for {}: {}", node_id, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

async fn get_recent_events(
    Query(query): Query<EventsQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let limit = query.limit.unwrap_or(50).clamp(1, MAX_QUERY_LIMIT);

    match state.store.get_recent_events(limit).await {
        Ok(events) => {
            let has_more = events.len() as i64 == limit;
            Ok(Json(serde_json::json!({
                "events": events,
                "has_more": has_more
            })))
        }
        Err(e) => {
            error!("Failed to get recent events: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// WebSocket message types for client communication
#[derive(Deserialize)]
#[serde(tag = "type")]
enum WebSocketRequest {
    Subscribe { filter: SubscriptionFilter },
    Unsubscribe,
    GetRecentEvents { limit: Option<usize> },
    Ping,
}

#[derive(Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
enum SubscriptionFilter {
    All,
    Node { node_id: String },
    EventType { event_type: u8 },
    EventTypeRange { start: u8, end: u8 },
}

#[derive(Serialize)]
struct WebSocketResponse<T> {
    r#type: String,
    data: T,
    timestamp: chrono::DateTime<chrono::Utc>,
}

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| websocket_connection(socket, state))
}

async fn websocket_connection(mut socket: WebSocket, state: ApiState) {
    info!("WebSocket connection established");

    // Send initial connection confirmation with recent events
    let recent_events = state.broadcaster.get_recent_events(Some(20)).await;
    let broadcaster_stats = state.broadcaster.get_stats().await;

    let initial_state = WebSocketResponse {
        r#type: "connected".to_string(),
        data: serde_json::json!({
            "message": "Connected to TART telemetry (1024-node scale)",
            "recent_events": recent_events.len(),
            "total_nodes": state.telemetry_server.connection_count(),
            "broadcaster_stats": broadcaster_stats,
            "recent_event_samples": recent_events.iter().take(5).map(|e| {
                serde_json::json!({
                    "id": e.id,
                    "node_id": e.node_id,
                    "event_type": e.event_type,
                    "timestamp": e.timestamp
                })
            }).collect::<Vec<_>>()
        }),
        timestamp: chrono::Utc::now(),
    };

    if let Some(msg) = serialize_ws_message(&initial_state) {
        if socket.send(Message::Text(msg)).await.is_err() {
            return;
        }
    } else {
        // Failed to serialize, close connection
        return;
    }

    // Default to subscribing to all events
    let mut event_receiver = state.broadcaster.subscribe_all();
    let mut current_filter = SubscriptionFilter::All;

    // Stats update interval (5 seconds)
    let mut stats_interval = tokio::time::interval(std::time::Duration::from_secs(5));

    // Track performance metrics
    let mut events_received = 0u64;
    let mut last_event_time = chrono::Utc::now();

    loop {
        tokio::select! {
            // Real-time event streaming from broadcaster
            Ok(event) = event_receiver.recv() => {
                events_received += 1;

                // Calculate latency
                let latency_ms = (chrono::Utc::now() - event.timestamp).num_milliseconds();

                let response = WebSocketResponse {
                    r#type: "event".to_string(),
                    data: serde_json::json!({
                        "id": event.id,
                        "node_id": event.node_id,
                        "event_type": event.event_type,
                        "latency_ms": latency_ms,
                        "event": event.event,
                    }),
                    timestamp: chrono::Utc::now(),
                };

                if let Some(msg) = serialize_ws_message(&response) {
                    if socket.send(Message::Text(msg)).await.is_err() {
                        break;
                    }
                } else {
                    // Failed to serialize, close connection
                    break;
                }

                last_event_time = chrono::Utc::now();
            }

            // Handle client messages
            Some(msg) = socket.recv() => {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Ok(request) = serde_json::from_str::<WebSocketRequest>(&text) {
                            match request {
                                WebSocketRequest::Subscribe { filter } => {
                                    // Update subscription based on filter
                                    event_receiver = match &filter {
                                        SubscriptionFilter::All => {
                                            state.broadcaster.subscribe_all()
                                        }
                                        SubscriptionFilter::Node { node_id } => {
                                            state.broadcaster.subscribe_node(node_id).await
                                        }
                                        SubscriptionFilter::EventType { event_type: _ } => {
                                            // For now, use main channel and client-side filtering
                                            state.broadcaster.subscribe_all()
                                        }
                                        SubscriptionFilter::EventTypeRange { start: _, end: _ } => {
                                            // For now, use main channel and client-side filtering
                                            state.broadcaster.subscribe_all()
                                        }
                                    };
                                    current_filter = filter.clone();

                                    let response = WebSocketResponse {
                                        r#type: "subscribed".to_string(),
                                        data: serde_json::json!({
                                            "filter": current_filter,
                                            "message": "Subscription updated"
                                        }),
                                        timestamp: chrono::Utc::now(),
                                    };

                                    if let Some(msg) = serialize_ws_message(&response) {
                                        let _ = socket.send(Message::Text(msg)).await;
                                    }
                                }
                                WebSocketRequest::Unsubscribe => {
                                    // Reset to all events
                                    event_receiver = state.broadcaster.subscribe_all();
                                    current_filter = SubscriptionFilter::All;

                                    let response = WebSocketResponse {
                                        r#type: "unsubscribed".to_string(),
                                        data: serde_json::json!({"message": "Reset to all events"}),
                                        timestamp: chrono::Utc::now(),
                                    };

                                    if let Some(msg) = serialize_ws_message(&response) {
                                        let _ = socket.send(Message::Text(msg)).await;
                                    }
                                }
                                WebSocketRequest::GetRecentEvents { limit } => {
                                    let events = state.broadcaster.get_recent_events(limit).await;

                                    let response = WebSocketResponse {
                                        r#type: "recent_events".to_string(),
                                        data: serde_json::json!({
                                            "count": events.len(),
                                            "events": events.iter().map(|e| {
                                                serde_json::json!({
                                                    "id": e.id,
                                                    "node_id": e.node_id,
                                                    "event_type": e.event_type,
                                                    "timestamp": e.timestamp,
                                                    "event": e.event
                                                })
                                            }).collect::<Vec<_>>()
                                        }),
                                        timestamp: chrono::Utc::now(),
                                    };

                                    if let Some(msg) = serialize_ws_message(&response) {
                                        let _ = socket.send(Message::Text(msg)).await;
                                    }
                                }
                                WebSocketRequest::Ping => {
                                    let response = WebSocketResponse {
                                        r#type: "pong".to_string(),
                                        data: serde_json::json!({
                                            "events_received": events_received,
                                            "uptime_ms": (chrono::Utc::now() - last_event_time).num_milliseconds()
                                        }),
                                        timestamp: chrono::Utc::now(),
                                    };

                                    if let Some(msg) = serialize_ws_message(&response) {
                                        let _ = socket.send(Message::Text(msg)).await;
                                    }
                                }
                            }
                        }
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(Message::Ping(data)) => {
                        if socket.send(Message::Pong(data)).await.is_err() {
                            break;
                        }
                    }
                    _ => {}
                }
            }

            // Periodic stats updates
            _ = stats_interval.tick() => {
                if let Ok(db_stats) = state.store.get_stats().await {
                    let broadcaster_stats = state.broadcaster.get_stats().await;
                    let node_ids = state.telemetry_server.get_connection_ids();

                    let response = WebSocketResponse {
                        r#type: "stats".to_string(),
                        data: serde_json::json!({
                            "database": db_stats,
                            "broadcaster": broadcaster_stats,
                            "connections": {
                                "total": node_ids.len(),
                                "nodes": node_ids
                            },
                            "websocket": {
                                "events_received": events_received,
                                "current_filter": current_filter
                            }
                        }),
                        timestamp: chrono::Utc::now(),
                    };

                    if let Some(msg) = serialize_ws_message(&response) {
                        if socket.send(Message::Text(msg)).await.is_err() {
                            break;
                        }
                    } else {
                        // Failed to serialize, close connection
                        break;
                    }
                }
            }
        }
    }

    info!(
        "WebSocket connection closed (received {} events)",
        events_received
    );
}
