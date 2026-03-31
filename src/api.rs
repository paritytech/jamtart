//! HTTP and WebSocket API layer. Defines route handlers, shared application state,
//! and extraction middleware for the telemetry dashboard and Grafana integration.

use crate::cache::TtlCache;
use crate::event_broadcaster::EventBroadcaster;
use crate::health::HealthMonitor;
use crate::jam_rpc::JamRpcClient;
use crate::server::TelemetryServer;
use crate::store::EventStore;
use axum::extract::ws::{Message, WebSocket};
use axum::{
    extract::{DefaultBodyLimit, Path, Query, State, WebSocketUpgrade},
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use utoipa::OpenApi;
use std::sync::Arc;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::timeout::TimeoutLayer;
use tracing::{debug, error, info, warn};

/// Validates that a node_id is a valid 64-character hexadecimal string (32 bytes encoded).
fn is_valid_node_id(node_id: &str) -> bool {
    node_id.len() == 64 && node_id.chars().all(|c| c.is_ascii_hexdigit())
}

/// Typed response struct for /api/health (avoids serde_json::json! overhead on hot path)
#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    service: &'static str,
    version: &'static str,
}

const MAX_QUERY_LIMIT: i64 = 1000;

/// No-cache headers for real-time endpoints polled at high frequency.
fn no_cache_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-cache, no-store, must-revalidate"),
    );
    headers
}

/// Cache-friendly headers for endpoints backed by the TTL cache.
fn cache_headers(max_age: u32) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::try_from(format!("public, max-age={}", max_age)).unwrap(),
    );
    headers
}

/// Cache-or-compute helper with stampede prevention.
/// Checks the cache first; on miss, uses register_inflight to ensure only one
/// concurrent request computes the value while others wait.
async fn cache_or_compute<F, Fut>(
    cache: &TtlCache,
    key: &str,
    compute: F,
) -> Result<Arc<serde_json::Value>, StatusCode>
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<serde_json::Value, StatusCode>>,
{
    // Fast path: cache hit
    if let Some(cached) = cache.get(key) {
        return Ok(cached);
    }

    // Check if another request is already computing this key
    if let Some(notify) = cache.register_inflight(key) {
        // Wait for the other request to finish
        notify.notified().await;
        // Re-check cache after being notified
        if let Some(cached) = cache.get(key) {
            return Ok(cached);
        }
        // Fallthrough: compute ourselves if cache still empty (rare edge case)
    }

    // We're the one computing
    let result = compute().await;
    match result {
        Ok(value) => {
            let value = Arc::new(value);
            cache.insert_arc(key.to_string(), Arc::clone(&value));
            cache.clear_inflight(key);
            Ok(value)
        }
        Err(e) => {
            cache.clear_inflight(key);
            Err(e)
        }
    }
}

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
    pub jam_rpc: Option<Arc<JamRpcClient>>,
    /// In-memory TTL cache for expensive analytics queries
    pub cache: Arc<TtlCache>,
    /// In-memory metrics tracker (replaces self-JOIN SQL queries)
    pub metrics_tracker: Option<Arc<crate::metrics_tracker::MetricsTracker>>,
}

pub fn create_api_router(state: ApiState) -> Router {
    Router::new()
        .route("/api/health", get(health_check))
        .route("/api/health/detailed", get(detailed_health_check))
        // Grafana-optimized API endpoints
        .nest("/api/grafana", crate::grafana::router())
        // OpenAPI spec (auto-generated from utoipa annotations)
        .route("/api/docs/openapi.json", get(openapi_spec))
        .route("/api/ws", get(websocket_handler))
        .route("/api/network", get(get_network_info))
        .route("/api/nodes/:node_id", get(get_node_details))
        .route("/api/nodes/:node_id/status", get(get_node_status))
        .route("/api/nodes/:node_id/peers", get(get_node_peers))
        .route(
            "/api/nodes/:node_id/status/enhanced",
            get(get_node_status_enhanced),
        )
        .route("/api/nodes/:node_id/timeline", get(get_node_timeline))
        .route("/api/network/topology", get(get_peer_topology))
        .route("/api/metrics/realtime", get(get_realtime_metrics))
        .route("/api/metrics/live", get(get_live_counters))
        .route("/api/metrics/stream", get(metrics_sse_handler))
        .route("/api/slots/:slot", get(get_slot_events))
        .route("/api/jam/stats", get(get_jam_stats))
        .route("/api/jam/services", get(get_jam_services))
        .route("/api/jam/cores", get(get_jam_cores))
        // Middleware layers wrap bottom-up: last .layer() is outermost.
        // Order (outermost first): CORS → Compression → Headers → Body limit → Timeout
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(30))) // Innermost: timeout on handler
        .layer(DefaultBodyLimit::max(256 * 1024)) // Body limit before handler
        .layer(CompressionLayer::new())
        .layer(tower_http::trace::TraceLayer::new_for_http()
            .make_span_with(tower_http::trace::DefaultMakeSpan::new().level(tracing::Level::DEBUG))
            .on_response(tower_http::trace::DefaultOnResponse::new().level(tracing::Level::DEBUG)))
        .layer(CorsLayer::permissive()) // Outermost: cheap CORS preflight
        .with_state(state)
}

async fn openapi_spec() -> impl IntoResponse {
    Json(crate::grafana::GrafanaApiDoc::openapi())
}

async fn health_check() -> impl IntoResponse {
    (
        no_cache_headers(),
        Json(HealthResponse {
            status: "ok",
            service: "tart-backend",
            version: env!("CARGO_PKG_VERSION"),
        }),
    )
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


/// Returns network topology information gleaned from connected nodes.
/// This includes core count, validator count, and other protocol parameters.
async fn get_network_info(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    match state.store.get_network_info().await {
        Ok(info) => Ok(Json(info)),
        Err(e) => {
            error!("Failed to get network info: {}", e);
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

    match state.store.get_node_by_id(&node_id).await {
        Ok(Some(mut node)) => {
            // Enrich with live connection info from telemetry server
            if let Some(conn) = state
                .telemetry_server
                .get_connections()
                .into_iter()
                .find(|c| c.id == node_id)
            {
                let duration = chrono::Utc::now()
                    .signed_duration_since(conn.connected_at)
                    .num_seconds();
                node["connection_info"] = serde_json::json!({
                    "address": conn.address.to_string(),
                    "event_count": conn.event_count,
                    "connected_duration_secs": duration,
                });
            }
            Ok(Json(node))
        }
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(e) => {
            error!("Failed to get node details: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get per-node status including best/finalized block heights
async fn get_node_status(
    Path(node_id): Path<String>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    // Validate node_id format
    if !is_valid_node_id(&node_id) {
        warn!("Invalid node_id format: {}", node_id);
        return Err(StatusCode::BAD_REQUEST);
    }

    match state.store.get_node_status(&node_id).await {
        Ok(status) => Ok(Json(status)),
        Err(e) => {
            error!("Failed to get node status for {}: {}", node_id, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}


/// Get peer/connection metrics for a specific node
async fn get_node_peers(
    Path(node_id): Path<String>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    if !is_valid_node_id(&node_id) {
        warn!("Invalid node_id format: {}", node_id);
        return Err(StatusCode::BAD_REQUEST);
    }

    match state.store.get_node_peers(&node_id).await {
        Ok(peers) => Ok(Json(peers)),
        Err(e) => {
            error!("Failed to get node peers for {}: {}", node_id, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}


/// Get peer topology and network traffic patterns
async fn get_peer_topology(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    match state.store.get_peer_topology().await {
        Ok(topology) => Ok(Json(topology)),
        Err(e) => {
            error!("Failed to get peer topology: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get enhanced node status with core assignment
async fn get_node_status_enhanced(
    Path(node_id): Path<String>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    if !is_valid_node_id(&node_id) {
        warn!("Invalid node_id format: {}", node_id);
        return Err(StatusCode::BAD_REQUEST);
    }

    match state.store.get_node_status_enhanced(&node_id).await {
        Ok(status) => Ok(Json(status)),
        Err(e) => {
            error!("Failed to get enhanced node status for {}: {}", node_id, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// ============================================================================
// Real-time Metrics Endpoints
// ============================================================================

/// Query parameters for real-time metrics
#[derive(Deserialize)]
struct RealtimeMetricsQuery {
    /// Number of seconds to look back (10-300, default 60)
    seconds: Option<i32>,
}

/// Get real-time rolling window metrics with per-second granularity.
/// Returns counts per second for the last N seconds (default 60).
async fn get_realtime_metrics(
    Query(params): Query<RealtimeMetricsQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let seconds = params.seconds.unwrap_or(60).clamp(10, 300);

    if let Some(ref tracker) = state.metrics_tracker {
        let lc = tracker.live_counters();
        let per_second = lc.per_second_history(seconds as u64);
        let active_nodes = state.telemetry_server.connection_count();
        let snapshot = lc.build_realtime_snapshot(seconds, &per_second, active_nodes);
        return Ok((no_cache_headers(), Json(Arc::new(snapshot))));
    }

    // Fallback to SQL
    let key = format!("realtime_{}", seconds);
    let result = cache_or_compute(&state.cache, &key, || async {
        state
            .store
            .get_realtime_metrics(seconds)
            .await
            .map_err(|e| {
                error!("Failed to get realtime metrics: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((no_cache_headers(), Json(result)))
}

/// Get live counters - ultra-lightweight for high-frequency polling.
/// Returns current slot, active nodes, and rate calculations.
/// Served from in-memory LiveCounters (no SQL).
async fn get_live_counters(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    if let Some(ref tracker) = state.metrics_tracker {
        let lc = tracker.live_counters();
        let last_10s = lc.sum_last_n_seconds(10);
        let last_1m = lc.sum_last_n_seconds(60);
        let active_nodes = state.telemetry_server.connection_count();
        let snapshot = lc.build_live_snapshot(&last_10s, &last_1m, active_nodes);
        return Ok((no_cache_headers(), Json(Arc::new(snapshot))));
    }
    // Fallback to SQL
    let result = cache_or_compute(&state.cache, "live_counters", || async {
        state.store.get_live_counters().await.map_err(|e| {
            error!("Failed to get live counters: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
    })
    .await?;
    Ok((no_cache_headers(), Json(result)))
}

/// SSE (Server-Sent Events) handler for real-time metrics streaming.
/// Pushes updates every second without client polling overhead.
async fn metrics_sse_handler(
    State(state): State<ApiState>,
) -> axum::response::Sse<
    impl futures::Stream<Item = Result<axum::response::sse::Event, std::convert::Infallible>>,
> {
    use axum::response::sse::{Event, KeepAlive};
    use futures::stream;
    use std::time::Duration;

    let cache = state.cache.clone();

    // Read from cache instead of hitting DB per-connection per-second.
    // The cache is warmed every 2s by the background task.
    let stream = stream::unfold(cache, |cache| async move {
        // Wait 1 second between updates
        tokio::time::sleep(Duration::from_secs(1)).await;

        let data = match cache.get("live_counters") {
            Some(cached) => (*cached).clone(),
            None => serde_json::json!({"error": "Cache not available"}),
        };

        let event = Event::default().data(data.to_string()).event("metrics");

        Some((Ok(event), cache))
    });

    axum::response::Sse::new(stream).keep_alive(KeepAlive::default())
}

#[derive(Deserialize)]
struct SlotQuery {
    include_events: Option<bool>,
}

/// Get all events for a specific slot, grouped by node
async fn get_slot_events(
    Path(slot): Path<i64>,
    Query(query): Query<SlotQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let include_events = query.include_events.unwrap_or(false);
    let cache_key = format!("slot_events_{}_{}", slot, include_events);

    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_slot_events(slot, include_events)
            .await
            .map_err(|e| {
                error!("Failed to get slot {} events: {}", slot, e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;

    Ok((cache_headers(2), Json(result)))
}

#[derive(Deserialize)]
struct NodeTimelineQuery {
    start_time: Option<String>,
    end_time: Option<String>,
    categories: Option<String>, // comma-separated
    limit: Option<i64>,
}

/// Get validator activity timeline with time range and category filtering
async fn get_node_timeline(
    Path(node_id): Path<String>,
    Query(query): Query<NodeTimelineQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    if !is_valid_node_id(&node_id) {
        warn!("Invalid node_id format in timeline: {}", node_id);
        return Err(StatusCode::BAD_REQUEST);
    }

    let limit = query.limit.unwrap_or(200).clamp(1, MAX_QUERY_LIMIT);

    let start_time = query
        .start_time
        .as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    let end_time = query
        .end_time
        .as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    let categories: Option<Vec<String>> = query
        .categories
        .as_ref()
        .map(|s| s.split(',').map(|c| c.trim().to_string()).collect());

    match state
        .store
        .get_node_timeline(&node_id, start_time, end_time, categories.as_deref(), limit)
        .await
    {
        Ok(timeline) => Ok(Json(timeline)),
        Err(e) => {
            error!("Failed to get node {} timeline: {}", node_id, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// ============================================================================
// JAM RPC Endpoints - Live data from JAM node
// ============================================================================

/// Get full JAM network statistics including services and cores
async fn get_jam_stats(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    let jam_rpc = state.jam_rpc.as_ref().ok_or_else(|| {
        warn!("JAM RPC not configured (set JAM_RPC_URL)");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    // Try cached stats first, fall back to fetch
    if let Some(stats) = jam_rpc.get_stats().await {
        return Ok(Json(serde_json::json!(stats)));
    }

    // Fetch fresh stats
    match jam_rpc.fetch_stats().await {
        Ok(stats) => Ok(Json(serde_json::json!(stats))),
        Err(e) => {
            error!("Failed to fetch JAM stats: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get list of services from the JAM network
async fn get_jam_services(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    let jam_rpc = state.jam_rpc.as_ref().ok_or_else(|| {
        warn!("JAM RPC not configured (set JAM_RPC_URL)");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    // Try cached stats first
    if let Some(stats) = jam_rpc.get_stats().await {
        return Ok(Json(serde_json::json!({
            "services": stats.services,
            "totals": {
                "total": stats.totals.total_services,
                "active": stats.totals.active_services,
                "refining": stats.totals.refining_services,
                "accumulating": stats.totals.accumulating_services,
            }
        })));
    }

    // Fetch fresh stats
    match jam_rpc.fetch_stats().await {
        Ok(stats) => Ok(Json(serde_json::json!({
            "services": stats.services,
            "totals": {
                "total": stats.totals.total_services,
                "active": stats.totals.active_services,
                "refining": stats.totals.refining_services,
                "accumulating": stats.totals.accumulating_services,
            }
        }))),
        Err(e) => {
            error!("Failed to fetch JAM services: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get core activity statistics from the JAM network
async fn get_jam_cores(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    let jam_rpc = state.jam_rpc.as_ref().ok_or_else(|| {
        warn!("JAM RPC not configured (set JAM_RPC_URL)");
        StatusCode::SERVICE_UNAVAILABLE
    })?;

    // Try cached stats first
    if let Some(stats) = jam_rpc.get_stats().await {
        let params = jam_rpc.get_params().await;
        return Ok(Json(serde_json::json!({
            "cores": stats.cores,
            "core_count": stats.core_count,
            "params": params,
        })));
    }

    // Fetch fresh stats
    match jam_rpc.fetch_stats().await {
        Ok(stats) => {
            let params = jam_rpc.get_params().await;
            Ok(Json(serde_json::json!({
                "cores": stats.cores,
                "core_count": stats.core_count,
                "params": params,
            })))
        }
        Err(e) => {
            error!("Failed to fetch JAM cores: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// WebSocket message types for client communication
#[derive(Deserialize)]
#[serde(tag = "type")]
enum WebSocketRequest {
    Subscribe {
        filter: SubscriptionFilter,
    },
    Unsubscribe,
    GetRecentEvents {
        limit: Option<usize>,
    },
    Ping,
    /// Subscribe to aggregated metrics channel (pushes every interval_ms)
    SubscribeMetrics {
        interval_ms: Option<u64>,
    },
    /// Unsubscribe from metrics channel
    UnsubscribeMetrics,
    /// Subscribe to alerts channel
    SubscribeAlerts,
    /// Unsubscribe from alerts channel
    UnsubscribeAlerts,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(tag = "type")]
enum SubscriptionFilter {
    All,
    Nodes { node_ids: Vec<String> },
    EventType { event_type: u8 },
    EventTypeRange { start: u8, end: u8 },
}

#[derive(Serialize)]
struct WebSocketResponse<T> {
    r#type: String,
    data: T,
    timestamp: chrono::DateTime<chrono::Utc>,
}

/// Maximum concurrent WebSocket connections
const MAX_WS_CONNECTIONS: usize = 5000;

/// Active WebSocket connection counter
static ACTIVE_WS_CONNECTIONS: std::sync::atomic::AtomicUsize =
    std::sync::atomic::AtomicUsize::new(0);

async fn websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let current = ACTIVE_WS_CONNECTIONS.load(std::sync::atomic::Ordering::Relaxed);
    if current >= MAX_WS_CONNECTIONS {
        warn!(
            "WebSocket connection limit reached ({})",
            MAX_WS_CONNECTIONS
        );
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(ws.on_upgrade(move |socket| {
        let store = Some(state.store.clone());
        let cache = Some(state.cache.clone());
        websocket_connection(
            socket,
            state.broadcaster,
            state.telemetry_server,
            store,
            cache,
        )
    }))
}

/// Send timeout for WebSocket messages - prevents slow clients from blocking
const WS_SEND_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

/// Max events per WS message. Opportunistic batching: after receiving one event,
/// drain up to this many more via try_recv() (non-blocking). At batch=100,
/// ws-synth measured 1.6M events/s vs 158K unbatched. Larger batches help
/// drain broadcast_lag spikes faster (observed up to 524K with batch=10).
const WS_BATCH_SIZE: usize = 100;

use crate::event_broadcaster::BroadcastEvent;

/// Dual receive mode for WS event streaming.
/// `All` uses the main broadcast channel (firehose).
/// `Filtered` uses a StreamMap over per-node broadcast channels.
enum EventSource {
    All(tokio::sync::broadcast::Receiver<Arc<BroadcastEvent>>),
    Filtered(
        tokio_stream::StreamMap<
            String,
            tokio_stream::wrappers::BroadcastStream<Arc<BroadcastEvent>>,
        >,
    ),
}

impl EventSource {
    async fn recv(
        &mut self,
    ) -> Result<Arc<BroadcastEvent>, tokio::sync::broadcast::error::RecvError> {
        match self {
            EventSource::All(rx) => rx.recv().await,
            EventSource::Filtered(map) => {
                use tokio_stream::StreamExt;
                match map.next().await {
                    Some((_key, Ok(event))) => Ok(event),
                    Some((_key, Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)))) => {
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(n))
                    }
                    None => Err(tokio::sync::broadcast::error::RecvError::Closed),
                }
            }
        }
    }

    fn try_recv(
        &mut self,
    ) -> Result<Arc<BroadcastEvent>, tokio::sync::broadcast::error::TryRecvError> {
        match self {
            EventSource::All(rx) => rx.try_recv(),
            EventSource::Filtered(map) => {
                // Poll StreamMap synchronously for batching support.
                // Without this, a client subscribing to 512 nodes (~500K events/s)
                // would send each event individually — as bad as the firehose.
                use futures::Stream;
                let waker = futures::task::noop_waker();
                let mut cx = std::task::Context::from_waker(&waker);
                match std::pin::Pin::new(map).poll_next(&mut cx) {
                    std::task::Poll::Ready(Some((_key, Ok(event)))) => Ok(event),
                    std::task::Poll::Ready(Some((_key, Err(tokio_stream::wrappers::errors::BroadcastStreamRecvError::Lagged(n)))) ) => {
                        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n))
                    }
                    _ => Err(tokio::sync::broadcast::error::TryRecvError::Empty),
                }
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            EventSource::All(rx) => rx.len(),
            EventSource::Filtered(_) => 0,
        }
    }
}

async fn websocket_connection(
    mut socket: WebSocket,
    broadcaster: Arc<EventBroadcaster>,
    telemetry_server: Arc<TelemetryServer>,
    store: Option<Arc<EventStore>>,
    cache: Option<Arc<TtlCache>>,
) {
    ACTIVE_WS_CONNECTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    info!("WebSocket connection established");

    // Send initial connection confirmation with recent events
    let recent_events = broadcaster.get_recent_events(Some(200));
    let broadcaster_stats = broadcaster.get_stats();
    let has_db = store.is_some();

    let initial_state = WebSocketResponse {
        r#type: "connected".to_string(),
        data: serde_json::json!({
            "message": "Connected to TART telemetry (1024-node scale)",
            "recent_events": recent_events.len(),
            "total_nodes": telemetry_server.connection_count(),
            "broadcaster_stats": broadcaster_stats,
            "recent_event_samples": recent_events.iter().take(200).map(|e| {
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

    // Default to subscribing to all events via main broadcast channel
    let mut event_source = EventSource::All(broadcaster.subscribe_all());
    let mut current_filter = SubscriptionFilter::All;

    // Stats update interval (5 seconds)
    let mut stats_interval = tokio::time::interval(std::time::Duration::from_secs(5));

    // Metrics channel state
    let mut metrics_subscribed = false;
    let mut metrics_interval = tokio::time::interval(std::time::Duration::from_secs(1));

    // Alerts channel state
    let mut alerts_subscribed = false;
    let mut alerts_interval = tokio::time::interval(std::time::Duration::from_secs(10));
    let mut last_alerts: serde_json::Value = serde_json::json!(null);

    // Track performance metrics
    let mut events_received = 0u64;
    let mut last_event_time = chrono::Utc::now();

    // Debug: track WS loop health
    let mut debug_events_since_log = 0u64;
    let mut debug_last_log = tokio::time::Instant::now();
    let mut debug_send_time_us = 0u64;
    let mut debug_sends_since_log = 0u64;
    let mut debug_lagged_total = 0u64;

    loop {
        tokio::select! {
            // Real-time event streaming from broadcaster
            result = event_source.recv() => {
            match result {
            Ok(event) => {
                // Opportunistic batching: collect first event, then drain more via try_recv()
                let mut batch: Vec<std::sync::Arc<str>> = Vec::with_capacity(WS_BATCH_SIZE);

                if let Some(ref json) = event.serialized_json {
                    batch.push(std::sync::Arc::clone(json));
                } else {
                    // Fallback: serialize on the fly
                    let response = WebSocketResponse {
                        r#type: "event".to_string(),
                        data: serde_json::json!({
                            "id": event.id,
                            "node_id": event.node_id,
                            "event_type": event.event_type,
                            "event": event.event,
                        }),
                        timestamp: chrono::Utc::now(),
                    };
                    if let Some(m) = serialize_ws_message(&response) {
                        batch.push(std::sync::Arc::from(m));
                    }
                }

                // Drain more events without blocking
                while batch.len() < WS_BATCH_SIZE {
                    match event_source.try_recv() {
                        Ok(ev) => {
                            if let Some(ref json) = ev.serialized_json {
                                batch.push(std::sync::Arc::clone(json));
                            }
                        }
                        Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                            debug_lagged_total += n;
                        }
                        Err(tokio::sync::broadcast::error::TryRecvError::Empty
                            | tokio::sync::broadcast::error::TryRecvError::Closed) => break,
                    }
                }

                if batch.is_empty() {
                    continue;
                }

                let batch_len = batch.len() as u64;
                events_received += batch_len;
                debug_events_since_log += batch_len;

                // Build message: single event as-is, multiple events in batch envelope
                let msg = if batch.len() == 1 {
                    batch[0].to_string()
                } else {
                    let total_len: usize = batch.iter().map(|s| s.len()).sum::<usize>() + batch.len() + 80;
                    let mut buf = String::with_capacity(total_len);
                    buf.push_str(r#"{"type":"batch","data":["#);
                    for (i, json) in batch.iter().enumerate() {
                        if i > 0 { buf.push(','); }
                        buf.push_str(json);
                    }
                    buf.push_str(r#"],"timestamp":""#);
                    buf.push_str(&chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true));
                    buf.push_str(r#""}"#);
                    buf
                };

                // Send with timeout to prevent slow clients from blocking
                let send_start = tokio::time::Instant::now();
                match tokio::time::timeout(WS_SEND_TIMEOUT, socket.send(Message::Text(msg))).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => {
                        warn!("WebSocket send error after {} events: {}", events_received, e);
                        break;
                    }
                    Err(_) => {
                        warn!("WebSocket send timeout after {} events", events_received);
                        break;
                    }
                }
                debug_send_time_us += send_start.elapsed().as_micros() as u64;
                debug_sends_since_log += 1;

                // Periodic debug log every 2 seconds
                if debug_last_log.elapsed() >= std::time::Duration::from_secs(2) {
                    let elapsed = debug_last_log.elapsed().as_secs_f64();
                    let avg_send_us = if debug_sends_since_log > 0 { debug_send_time_us / debug_sends_since_log } else { 0 };
                    let filter_desc = match &current_filter {
                        SubscriptionFilter::All => "all".to_string(),
                        SubscriptionFilter::Nodes { node_ids } => {
                            if node_ids.len() == 1 && node_ids[0] == "*" {
                                "nodes(*)".to_string()
                            } else {
                                format!("nodes({})", node_ids.len())
                            }
                        }
                        SubscriptionFilter::EventType { event_type } => format!("etype({})", event_type),
                        SubscriptionFilter::EventTypeRange { start, end } => format!("etype({}-{})", start, end),
                    };
                    debug!(
                        "WS [{}]: {:.0} ev/s, avg_send={}us, lagged={}, lag={}, batch_avg={:.0}",
                        filter_desc,
                        debug_events_since_log as f64 / elapsed,
                        avg_send_us,
                        debug_lagged_total,
                        event_source.len(),
                        if debug_sends_since_log > 0 { debug_events_since_log as f64 / debug_sends_since_log as f64 } else { 0.0 },
                    );
                    debug_events_since_log = 0;
                    debug_send_time_us = 0;
                    debug_sends_since_log = 0;
                    debug_last_log = tokio::time::Instant::now();
                }

                last_event_time = chrono::Utc::now();
            }
            Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                debug_lagged_total += n;
            }
            Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                warn!("Broadcast channel closed, ending WS connection");
                break;
            }
            } // match result
            }

            // Handle client messages
            Some(msg) = socket.recv() => {
                match msg {
                    Ok(Message::Text(text)) => {
                        if let Ok(request) = serde_json::from_str::<WebSocketRequest>(&text) {
                            match request {
                                WebSocketRequest::Subscribe { filter } => {
                                    // Update subscription based on filter
                                    event_source = match &filter {
                                        SubscriptionFilter::All => {
                                            EventSource::All(broadcaster.subscribe_all())
                                        }
                                        SubscriptionFilter::Nodes { node_ids } => {
                                            let mut map = tokio_stream::StreamMap::new();
                                            let subs = if node_ids.len() == 1 && node_ids[0] == "*" {
                                                // Wildcard: subscribe to all node channels via StreamMap.
                                                // Intentionally uses StreamMap (not main channel) to benchmark
                                                // StreamMap performance vs the firehose.
                                                broadcaster.subscribe_all_nodes().await
                                            } else {
                                                broadcaster.subscribe_nodes(node_ids).await
                                            };
                                            debug!(
                                                "WS building StreamMap: requested={}, found={}",
                                                if node_ids.len() == 1 && node_ids[0] == "*" { "all".to_string() } else { node_ids.len().to_string() },
                                                subs.len(),
                                            );
                                            for (id, rx) in subs {
                                                map.insert(id, tokio_stream::wrappers::BroadcastStream::new(rx));
                                            }
                                            EventSource::Filtered(map)
                                        }
                                        SubscriptionFilter::EventType { event_type: _ } => {
                                            // Use main channel + client-side filtering
                                            EventSource::All(broadcaster.subscribe_all())
                                        }
                                        SubscriptionFilter::EventTypeRange { start: _, end: _ } => {
                                            // Use main channel + client-side filtering
                                            EventSource::All(broadcaster.subscribe_all())
                                        }
                                    };
                                    current_filter = filter.clone();
                                    // Reset per-subscription stats
                                    debug_lagged_total = 0;
                                    debug_events_since_log = 0;
                                    debug_send_time_us = 0;
                                    debug_sends_since_log = 0;
                                    debug_last_log = tokio::time::Instant::now();

                                    let sub_desc = match &current_filter {
                                        SubscriptionFilter::All => "All".to_string(),
                                        SubscriptionFilter::Nodes { node_ids } => {
                                            if node_ids.len() == 1 && node_ids[0] == "*" {
                                                "Nodes(* → all channels)".to_string()
                                            } else {
                                                format!("Nodes({})", node_ids.len())
                                            }
                                        }
                                        SubscriptionFilter::EventType { event_type } => format!("EventType({})", event_type),
                                        SubscriptionFilter::EventTypeRange { start, end } => format!("EventTypeRange({}-{})", start, end),
                                    };
                                    info!("WS subscribed: {}", sub_desc);

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
                                    event_source = EventSource::All(broadcaster.subscribe_all());
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
                                    let events = broadcaster.get_recent_events(limit);

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
                                WebSocketRequest::SubscribeMetrics { interval_ms } => {
                                    metrics_subscribed = true;
                                    let interval = interval_ms.unwrap_or(1000).clamp(500, 10000);
                                    metrics_interval = tokio::time::interval(
                                        std::time::Duration::from_millis(interval)
                                    );

                                    let response = WebSocketResponse {
                                        r#type: "metrics_subscribed".to_string(),
                                        data: serde_json::json!({
                                            "message": "Subscribed to metrics channel",
                                            "interval_ms": interval
                                        }),
                                        timestamp: chrono::Utc::now(),
                                    };

                                    if let Some(msg) = serialize_ws_message(&response) {
                                        let _ = socket.send(Message::Text(msg)).await;
                                    }
                                }
                                WebSocketRequest::UnsubscribeMetrics => {
                                    metrics_subscribed = false;

                                    let response = WebSocketResponse {
                                        r#type: "metrics_unsubscribed".to_string(),
                                        data: serde_json::json!({"message": "Unsubscribed from metrics channel"}),
                                        timestamp: chrono::Utc::now(),
                                    };

                                    if let Some(msg) = serialize_ws_message(&response) {
                                        let _ = socket.send(Message::Text(msg)).await;
                                    }
                                }
                                WebSocketRequest::SubscribeAlerts => {
                                    alerts_subscribed = true;

                                    let response = WebSocketResponse {
                                        r#type: "alerts_subscribed".to_string(),
                                        data: serde_json::json!({"message": "Subscribed to alerts channel"}),
                                        timestamp: chrono::Utc::now(),
                                    };

                                    if let Some(msg) = serialize_ws_message(&response) {
                                        let _ = socket.send(Message::Text(msg)).await;
                                    }
                                }
                                WebSocketRequest::UnsubscribeAlerts => {
                                    alerts_subscribed = false;

                                    let response = WebSocketResponse {
                                        r#type: "alerts_unsubscribed".to_string(),
                                        data: serde_json::json!({"message": "Unsubscribed from alerts channel"}),
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
                let broadcaster_stats = broadcaster.get_stats();
                let node_ids = telemetry_server.get_connection_ids();

                let response = WebSocketResponse {
                    r#type: "stats".to_string(),
                    data: serde_json::json!({
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

            // Metrics channel updates (if subscribed, read from cache)
            _ = metrics_interval.tick(), if metrics_subscribed && has_db => {
                let metrics_result = cache_or_compute(cache.as_ref().unwrap(), "aggregated_metrics", || {
                    let store = store.clone().unwrap();
                    async move { store.get_aggregated_metrics().await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR) }
                }).await;

                if let Ok(metrics) = metrics_result {
                    // Enrich with core status from cache (pre-warmed every 2s)
                    let mut data = (*metrics).clone();
                    if let Some(cores) = cache.as_ref().unwrap().get("cores_status") {
                        data["cores"] = (*cores).clone();
                    }

                    let response = WebSocketResponse {
                        r#type: "metrics".to_string(),
                        data,
                        timestamp: chrono::Utc::now(),
                    };

                    if let Some(msg) = serialize_ws_message(&response) {
                        if socket.send(Message::Text(msg)).await.is_err() {
                            break;
                        }
                    }
                }
            }

            // Alerts channel updates (if subscribed) - read from cache instead of hitting DB
            _ = alerts_interval.tick(), if alerts_subscribed && has_db => {
                if let Some(cached_anomalies) = cache.as_ref().unwrap().get("anomalies") {
                    // Only send if there are new alerts and they differ from last sent
                    if *cached_anomalies != last_alerts {
                        let alerts_array = cached_anomalies.get("alerts").cloned().unwrap_or(serde_json::json!([]));
                        let count = alerts_array.as_array().map(|a| a.len()).unwrap_or(0);

                        if count > 0 {
                            let response = WebSocketResponse {
                                r#type: "alert".to_string(),
                                data: serde_json::json!({
                                    "alerts": alerts_array,
                                    "count": count
                                }),
                                timestamp: chrono::Utc::now(),
                            };

                            if let Some(msg) = serialize_ws_message(&response) {
                                if socket.send(Message::Text(msg)).await.is_err() {
                                    break;
                                }
                            }
                        }

                        last_alerts = (*cached_anomalies).clone();
                    }
                }
            }
        }
    }

    ACTIVE_WS_CONNECTIONS.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    info!(
        "WS closed: {} events received, lagged_total={}, filter={:?}",
        events_received, debug_lagged_total, current_filter
    );
}

// ---------------------------------------------------------------------------
// Minimal API for --no-database mode (WebSocket + health only, no DB routes)
// ---------------------------------------------------------------------------

/// Lightweight state for --no-database mode. Only carries the components needed
/// for WebSocket event streaming and health checks — no DB store or cache.
#[derive(Clone)]
pub struct MinimalApiState {
    pub telemetry_server: Arc<TelemetryServer>,
    pub broadcaster: Arc<EventBroadcaster>,
}

/// Create a minimal router for --no-database mode.
/// Only registers WebSocket and health endpoints — no DB-backed routes.
pub fn create_minimal_router(state: MinimalApiState) -> Router {
    Router::new()
        .route("/api/health", get(health_check))
        .route("/api/ws", get(minimal_websocket_handler))
        .layer(TimeoutLayer::new(std::time::Duration::from_secs(30)))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

async fn minimal_websocket_handler(
    ws: WebSocketUpgrade,
    State(state): State<MinimalApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let current = ACTIVE_WS_CONNECTIONS.load(std::sync::atomic::Ordering::Relaxed);
    if current >= MAX_WS_CONNECTIONS {
        warn!(
            "WebSocket connection limit reached ({})",
            MAX_WS_CONNECTIONS
        );
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }
    Ok(ws.on_upgrade(move |socket| {
        websocket_connection(
            socket,
            state.broadcaster,
            state.telemetry_server,
            None,
            None,
        )
    }))
}
