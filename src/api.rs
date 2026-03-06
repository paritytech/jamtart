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

/// Supported time range durations for telemetry queries.
/// Only preset values are accepted to keep cache cardinality bounded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
pub enum DurationPreset {
    #[serde(rename = "5m")]
    FiveMin,
    #[serde(rename = "15m")]
    FifteenMin,
    #[serde(rename = "1h")]
    OneHour,
    #[serde(rename = "6h")]
    SixHours,
    #[serde(rename = "24h")]
    TwentyFourHours,
}

impl DurationPreset {
    /// Returns the PostgreSQL interval string for this duration.
    pub fn as_pg_interval(&self) -> &'static str {
        match self {
            Self::FiveMin => "5 minutes",
            Self::FifteenMin => "15 minutes",
            Self::OneHour => "1 hour",
            Self::SixHours => "6 hours",
            Self::TwentyFourHours => "24 hours",
        }
    }

    /// Returns a short suffix for cache key construction.
    pub fn cache_suffix(&self) -> &'static str {
        match self {
            Self::FiveMin => "5m",
            Self::FifteenMin => "15m",
            Self::OneHour => "1h",
            Self::SixHours => "6h",
            Self::TwentyFourHours => "24h",
        }
    }

    /// Returns a secondary (longer) window for dual-interval queries.
    /// 5m→1h, 15m→3h, 1h→24h, 6h→24h, 24h→24h
    pub fn secondary_interval(&self) -> &'static str {
        match self {
            Self::FiveMin => "1 hour",
            Self::FifteenMin => "3 hours",
            Self::OneHour => "24 hours",
            Self::SixHours => "24 hours",
            Self::TwentyFourHours => "24 hours",
        }
    }
}

impl std::fmt::Display for DurationPreset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.cache_suffix())
    }
}

/// Query parameter for duration-aware endpoints.
#[derive(Debug, Deserialize)]
pub struct DurationQuery {
    pub duration: Option<DurationPreset>,
}

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
        .route("/api/stats", get(get_stats))
        .route("/api/network", get(get_network_info))
        .route("/api/nodes", get(get_nodes))
        .route("/api/nodes/:node_id", get(get_node_details))
        .route("/api/nodes/:node_id/events", get(get_node_events))
        .route("/api/nodes/:node_id/status", get(get_node_status))
        .route("/api/nodes/:node_id/peers", get(get_node_peers))
        .route("/api/events", get(get_recent_events))
        // Aggregation endpoints
        .route("/api/workpackages", get(get_workpackage_stats))
        .route("/api/workpackages/active", get(get_active_workpackages))
        .route(
            "/api/workpackages/:hash/journey",
            get(get_workpackage_journey),
        )
        .route("/api/blocks", get(get_block_stats))
        .route("/api/guarantees", get(get_guarantee_stats))
        // Data availability endpoints
        .route("/api/da/stats", get(get_da_stats))
        // Core status endpoints
        .route("/api/cores/status", get(get_cores_status))
        .route(
            "/api/cores/:core_index/guarantees",
            get(get_core_guarantees),
        )
        // Execution metrics
        .route("/api/metrics/execution", get(get_execution_metrics))
        .route("/api/metrics/timeseries", get(get_timeseries_metrics))
        // Validator/Core mapping
        .route("/api/validators/cores", get(get_validator_core_mapping))
        // Network topology
        .route("/api/network/topology", get(get_peer_topology))
        // Enhanced node status with core assignment
        .route(
            "/api/nodes/:node_id/status/enhanced",
            get(get_node_status_enhanced),
        )
        // Real-time metrics endpoints
        .route("/api/metrics/realtime", get(get_realtime_metrics))
        .route("/api/metrics/live", get(get_live_counters))
        .route("/api/metrics/stream", get(metrics_sse_handler))
        // HIGH PRIORITY: Frontend team requested endpoints
        .route(
            "/api/cores/:core_index/guarantors",
            get(get_core_guarantors),
        )
        .route(
            "/api/cores/:core_index/work-packages",
            get(get_core_work_packages),
        )
        .route(
            "/api/workpackages/:hash/journey/enhanced",
            get(get_workpackage_journey_enhanced),
        )
        .route("/api/da/stats/enhanced", get(get_da_stats_enhanced))
        // MEDIUM PRIORITY: Analytics endpoints
        .route("/api/analytics/failure-rates", get(get_failure_rates))
        .route(
            "/api/analytics/block-propagation",
            get(get_block_propagation),
        )
        .route("/api/analytics/network-health", get(get_network_health))
        .route(
            "/api/guarantees/by-guarantor",
            get(get_guarantees_by_guarantor),
        )
        // LOW PRIORITY: Timeline & grouped analytics
        .route(
            "/api/metrics/timeseries/grouped",
            get(get_timeseries_grouped),
        )
        .route(
            "/api/analytics/sync-status/timeline",
            get(get_sync_status_timeline),
        )
        .route(
            "/api/analytics/connections/timeline",
            get(get_connections_timeline),
        )
        // JAM RPC endpoints (requires JAM_RPC_URL to be set)
        .route("/api/jam/stats", get(get_jam_stats))
        .route("/api/jam/services", get(get_jam_services))
        .route("/api/jam/cores", get(get_jam_cores))
        // DASHBOARD: Mock data replacement endpoints
        .route(
            "/api/cores/:core_index/validators",
            get(get_core_validators),
        )
        .route("/api/cores/:core_index/metrics", get(get_core_metrics))
        .route(
            "/api/cores/:core_index/bottlenecks",
            get(get_core_bottlenecks),
        )
        .route(
            "/api/cores/:core_index/guarantors/enhanced",
            get(get_core_guarantors_enhanced),
        )
        .route(
            "/api/workpackages/:hash/audit-progress",
            get(get_workpackage_audit_progress),
        )
        // Frontend search & explorer endpoints
        .route("/api/events/search", get(search_events))
        .route("/api/slots/:slot", get(get_slot_events))
        .route("/api/nodes/:node_id/timeline", get(get_node_timeline))
        .route(
            "/api/workpackages/batch/journey",
            axum::routing::post(batch_workpackage_journeys),
        )
        .route("/api/ws", get(websocket_handler))
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

async fn get_stats(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let cache_key = format!("stats_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_stats(duration.as_pg_interval(), duration.secondary_interval())
            .await
            .map_err(|e| {
                error!("Failed to get stats: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
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

#[derive(Deserialize)]
struct EventsQuery {
    limit: Option<i64>,
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
    let offset = query.offset.unwrap_or(0).max(0);

    match state.store.get_recent_events(limit, offset).await {
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

// ============================================================================
// Aggregation Endpoints - Statistics derived from telemetry events
// ============================================================================

/// Get work package statistics aggregated from telemetry events
async fn get_workpackage_stats(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::TwentyFourHours);
    let cache_key = format!("workpackage_stats_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_workpackage_stats(duration.as_pg_interval())
            .await
            .map_err(|e| {
                error!("Failed to get workpackage stats: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get block statistics aggregated from telemetry events
async fn get_block_stats(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let cache_key = format!("block_stats_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_block_stats(duration.as_pg_interval())
            .await
            .map_err(|e| {
                error!("Failed to get block stats: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
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

/// Get guarantee distribution statistics
async fn get_guarantee_stats(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::TwentyFourHours);
    let cache_key = format!("guarantee_stats_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_guarantee_stats(duration.as_pg_interval(), duration.secondary_interval())
            .await
            .map_err(|e| {
                error!("Failed to get guarantee stats: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
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

/// Get data availability (shard/preimage) statistics
async fn get_da_stats(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    let result = cache_or_compute(&state.cache, "da_stats", || async {
        state.store.get_da_stats().await.map_err(|e| {
            error!("Failed to get DA stats: {}", e);
            StatusCode::INTERNAL_SERVER_ERROR
        })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get work package journey/pipeline tracking
async fn get_workpackage_journey(
    Path(hash): Path<String>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    // Basic validation - hash should be hex
    if hash.is_empty() || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        warn!("Invalid work package hash format: {}", hash);
        return Err(StatusCode::BAD_REQUEST);
    }

    match state.store.get_workpackage_journey(&hash).await {
        Ok(journey) => Ok(Json(journey)),
        Err(e) => {
            error!("Failed to get workpackage journey for {}: {}", hash, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get list of active work packages
async fn get_active_workpackages(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let cache_key = format!("active_wps_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_active_workpackages(duration.as_pg_interval(), duration.secondary_interval())
            .await
            .map_err(|e| {
                error!("Failed to get active workpackages: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get core status aggregation.
///
/// Uses rolling-window accumulated per-core data from JAM RPC (real pi_C
/// stats: gas_used, da_load, popularity, etc.) combined with aggregate
/// telemetry from the DB (guarantee/WP pipeline counts).
async fn get_cores_status(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let cache_key = format!("cores_status_{}", duration.cache_suffix());
    let interval = duration.as_pg_interval();
    let secondary = duration.secondary_interval();
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        // Per-core accumulated stats from JAM RPC rolling window
        let (window_stats, core_count, current_slot) = if let Some(ref jam_rpc) = state.jam_rpc {
            let ws = jam_rpc.get_core_window_stats().await;
            let params = jam_rpc.get_params().await;
            let count = params.as_ref().map(|p| p.core_count).unwrap_or(0);
            let slot = jam_rpc.get_stats().await.map(|s| s.slot).unwrap_or(0);
            (ws, count, slot)
        } else {
            (vec![], 0u16, 0u32)
        };

        // Fallback: get core_count from connected node params if JAM RPC unavailable
        let core_count = if core_count > 0 {
            core_count
        } else {
            state
                .telemetry_server
                .get_connections()
                .first()
                .map(|c| c.info.params.core_count)
                .unwrap_or(0)
        };

        // Fallback: get core_count from DB nodes table if no live connections
        let core_count = if core_count > 0 {
            core_count
        } else {
            let row: Option<(Option<serde_json::Value>,)> =
                sqlx::query_as("SELECT node_info->'params' FROM nodes LIMIT 1")
                    .fetch_optional(state.store.pool())
                    .await
                    .unwrap_or(None);

            row.and_then(|(params,)| params?.get("core_count")?.as_u64().map(|v| v as u16))
                .unwrap_or(0)
        };

        // Aggregate telemetry from DB
        let telemetry = state
            .store
            .get_cores_telemetry_agg(interval, secondary)
            .await
            .map_err(|e| {
                error!("Failed to get cores telemetry: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        let guarantees_last_hour = telemetry["guarantees_last_hour"].as_i64().unwrap_or(0);
        let wp_last_hour = telemetry["work_packages_last_hour"].as_i64().unwrap_or(0);
        let last_activity = telemetry.get("last_activity").cloned();

        // Build per-core entries
        let mut cores = Vec::new();
        let mut active_count = 0i64;
        let mut idle_count = 0i64;
        let has_window_stats = !window_stats.is_empty();

        if has_window_stats {
            // JAM RPC available: use accumulated per-core window stats
            let total_active_blocks: u32 = (0..core_count)
                .map(|i| {
                    window_stats
                        .iter()
                        .find(|w| w.core_index == i)
                        .map(|w| w.active_blocks)
                        .unwrap_or(0)
                })
                .sum();

            for i in 0..core_count {
                let ws = window_stats.iter().find(|w| w.core_index == i);
                let gas = ws.map(|w| w.gas_used).unwrap_or(0);
                let active_blocks = ws.map(|w| w.active_blocks).unwrap_or(0);
                let window_blocks = ws.map(|w| w.window_blocks).unwrap_or(0);
                let last_active = ws.map(|w| w.last_active_slot).unwrap_or(0);

                // Distribute aggregate WP/guarantee counts proportionally
                let (core_wp, core_guar) = if total_active_blocks > 0 && active_blocks > 0 {
                    let share = active_blocks as f64 / total_active_blocks as f64;
                    (
                        (wp_last_hour as f64 * share).round() as i64,
                        (guarantees_last_hour as f64 * share).round() as i64,
                    )
                } else {
                    (0, 0)
                };

                let is_active = gas > 0;
                if is_active {
                    active_count += 1;
                } else {
                    idle_count += 1;
                }

                cores.push(serde_json::json!({
                    "core_index": i,
                    "status": if is_active { "active" } else { "idle" },
                    "work_packages_last_hour": core_wp,
                    "guarantees_last_hour": core_guar,
                    "gas_used": gas,
                    "da_load": ws.map(|w| w.da_load).unwrap_or(0),
                    "popularity_avg": ws.map(|w| w.popularity_avg).unwrap_or(0.0),
                    "imports": ws.map(|w| w.imports).unwrap_or(0),
                    "extrinsic_count": ws.map(|w| w.extrinsic_count).unwrap_or(0),
                    "extrinsic_size": ws.map(|w| w.extrinsic_size).unwrap_or(0),
                    "active_blocks": active_blocks,
                    "window_blocks": window_blocks,
                    "utilization_pct": if window_blocks > 0 {
                        (active_blocks as f64 / window_blocks as f64) * 100.0
                    } else { 0.0 },
                    "last_active_slot": last_active,
                    "slots_since_active": if last_active > 0 {
                        current_slot.saturating_sub(last_active)
                    } else { 0 },
                }));
            }
        } else {
            // No JAM RPC: build cores from telemetry only.
            // Count per-core WPs from events that have a core field (93, 94).
            let per_core_wp: Vec<(Option<i64>, i64)> = sqlx::query_as(&format!(
                r#"
                SELECT
                    COALESCE(
                        (data->'WorkPackageReceived'->>'core')::bigint,
                        (data->'DuplicateWorkPackage'->>'core')::bigint
                    ) as core_idx,
                    COUNT(*) as cnt
                FROM events
                WHERE event_type IN (93, 94)
                AND timestamp > NOW() - INTERVAL '{}'
                GROUP BY core_idx
                ORDER BY core_idx
                "#,
                interval
            ))
            .fetch_all(state.store.pool())
            .await
            .unwrap_or_default();

            let mut core_wp_map = std::collections::HashMap::<u16, i64>::new();
            for (core_idx, cnt) in &per_core_wp {
                if let Some(c) = core_idx {
                    if *c >= 0 && *c < core_count as i64 {
                        core_wp_map.insert(*c as u16, *cnt);
                    }
                }
            }

            let has_activity = wp_last_hour > 0 || guarantees_last_hour > 0;

            for i in 0..core_count {
                let core_wp_events = core_wp_map.get(&i).copied().unwrap_or(0);

                // If per-core event data exists, use it. Otherwise distribute
                // aggregate telemetry evenly so the dashboard shows something.
                let (core_wp, core_guar, is_active) = if !core_wp_map.is_empty() {
                    let active = core_wp_events > 0;
                    let n_active = core_wp_map.len().max(1) as i64;
                    let guar = if active {
                        guarantees_last_hour / n_active
                    } else {
                        0
                    };
                    (core_wp_events, guar, active)
                } else if has_activity && core_count > 0 {
                    // No per-core data; distribute evenly
                    let wp = wp_last_hour / core_count as i64;
                    let guar = guarantees_last_hour / core_count as i64;
                    (wp, guar, wp > 0)
                } else {
                    (0, 0, false)
                };

                if is_active {
                    active_count += 1;
                } else {
                    idle_count += 1;
                }

                cores.push(serde_json::json!({
                    "core_index": i,
                    "status": if is_active { "active" } else { "idle" },
                    "work_packages_last_hour": core_wp,
                    "guarantees_last_hour": core_guar,
                    "gas_used": 0,
                    "da_load": 0,
                    "popularity_avg": 0.0,
                    "imports": 0,
                    "extrinsic_count": 0,
                    "extrinsic_size": 0,
                    "active_blocks": 0,
                    "window_blocks": 0,
                    "utilization_pct": 0.0,
                    "last_active_slot": 0,
                    "slots_since_active": 0,
                }));
            }
        }

        Ok(serde_json::json!({
            "cores": cores,
            "summary": {
                "total_cores": core_count,
                "active_cores": active_count,
                "idle_cores": idle_count,
            },
            "telemetry": {
                "guarantees_last_hour": guarantees_last_hour,
                "work_packages_last_hour": wp_last_hour,
                "last_activity": last_activity,
            },
        }))
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get guarantee distribution for a specific core
async fn get_core_guarantees(
    Path(core_index): Path<i32>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    if core_index < 0 {
        warn!("Invalid core_index: {}", core_index);
        return Err(StatusCode::BAD_REQUEST);
    }

    let key = format!("core_guarantees_{}", core_index);
    let result = cache_or_compute(&state.cache, &key, || async {
        state
            .store
            .get_core_guarantees(core_index)
            .await
            .map_err(|e| {
                error!("Failed to get core guarantees for {}: {}", core_index, e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get execution cost metrics
async fn get_execution_metrics(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let cache_key = format!("execution_metrics_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_execution_metrics(duration.as_pg_interval())
            .await
            .map_err(|e| {
                error!("Failed to get execution metrics: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

#[derive(Deserialize)]
struct TimeseriesQuery {
    metric: Option<String>,
    interval: Option<i32>,
    duration: Option<i32>,
}

/// Get time-series metrics with configurable interval and duration
async fn get_timeseries_metrics(
    Query(query): Query<TimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let metric = query.metric.as_deref().unwrap_or("throughput").to_string();
    let interval = query.interval.unwrap_or(5).clamp(1, 60); // 1-60 minutes
    let duration = query.duration.unwrap_or(1).clamp(1, 24); // 1-24 hours

    let cache_key = format!("timeseries_{}_{}_{}", metric, interval, duration);
    let result = cache_or_compute(&state.cache, &cache_key, || {
        let store = state.store.clone();
        let metric = metric.clone();
        async move {
            store
                .get_timeseries_metrics(&metric, interval, duration)
                .await
                .map_err(|e| {
                    error!("Failed to get timeseries metrics: {}", e);
                    StatusCode::INTERNAL_SERVER_ERROR
                })
        }
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get validator-to-core mapping derived from guarantee and ticket events
async fn get_validator_core_mapping(
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    match state.store.get_validator_core_mapping().await {
        Ok(mapping) => Ok(Json(mapping)),
        Err(e) => {
            error!("Failed to get validator core mapping: {}", e);
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

// ============================================================================
// HIGH PRIORITY: Frontend Team Requested Endpoints
// ============================================================================

/// Get guarantor information for a specific core with DA usage metrics.
async fn get_core_guarantors(
    Path(core_index): Path<i32>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let key = format!("core_guarantors_{}", core_index);
    let result = cache_or_compute(&state.cache, &key, || async {
        state
            .store
            .get_core_guarantors(core_index)
            .await
            .map_err(|e| {
                error!("Failed to get core {} guarantors: {}", core_index, e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get work packages currently being processed on a specific core.
async fn get_core_work_packages(
    Path(core_index): Path<i32>,
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let key = format!(
        "core_work_packages_{}_{}",
        core_index,
        duration.cache_suffix()
    );
    let result = cache_or_compute(&state.cache, &key, || async {
        state
            .store
            .get_core_work_packages(core_index, duration.as_pg_interval())
            .await
            .map_err(|e| {
                error!("Failed to get core {} work packages: {}", core_index, e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;

    // Overwrite self-JOIN fields with in-memory stats if available
    if let Some(ref tracker) = state.metrics_tracker {
        if let Some(processing) = tracker.get_core_processing_snapshot(core_index as u16) {
            let mut val = result.as_ref().clone();
            if let Some(obj) = val.as_object_mut() {
                if let Some(avg) = processing.get("avg_processing_time_ms") {
                    obj.insert("avg_processing_time_ms".into(), avg.clone());
                }
                if let Some(count) = processing.get("completed_last_hour") {
                    obj.insert("completed_last_hour".into(), count.clone());
                }
            }
            return Ok((cache_headers(2), Json(Arc::new(val))));
        }
    }

    Ok((cache_headers(2), Json(result)))
}

/// Get enhanced work package journey with node info, timing, and error details.
async fn get_workpackage_journey_enhanced(
    Path(hash): Path<String>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    match state.store.get_workpackage_journey_enhanced(&hash).await {
        Ok(journey) => Ok(Json(journey)),
        Err(e) => {
            error!("Failed to get enhanced WP journey for {}: {}", hash, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get enhanced DA stats with read/write operations and latency metrics.
async fn get_da_stats_enhanced(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let cache_key = format!("da_stats_enhanced_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_da_stats_enhanced(duration.as_pg_interval(), duration.secondary_interval())
            .await
            .map_err(|e| {
                error!("Failed to get enhanced DA stats: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

// ============================================================================
// MEDIUM PRIORITY: Analytics Endpoints
// ============================================================================

/// Get failure rate analytics across the system.
async fn get_failure_rates(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let cache_key = format!("failure_rates_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_failure_rates(duration.as_pg_interval())
            .await
            .map_err(|e| {
                error!("Failed to get failure rates: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get block propagation analytics.
async fn get_block_propagation(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    // Try in-memory tracker first (instant, replaces 28s+ SQL self-JOIN)
    if let Some(ref tracker) = state.metrics_tracker {
        let snapshot = tracker.get_block_propagation_snapshot();
        if !snapshot.is_null() {
            return Ok((cache_headers(2), Json(snapshot)));
        }
    }

    // Fallback to SQL
    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let cache_key = format!("block_propagation_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_block_propagation(duration.as_pg_interval())
            .await
            .map_err(|e| {
                error!("Failed to get block propagation: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get network health and congestion metrics.
async fn get_network_health(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let cache_key = format!("network_health_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_network_health(duration.as_pg_interval(), duration.secondary_interval())
            .await
            .map_err(|e| {
                error!("Failed to get network health: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get guarantor statistics aggregated by guarantor.
async fn get_guarantees_by_guarantor(
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = dq.duration.unwrap_or(DurationPreset::TwentyFourHours);
    let cache_key = format!("guarantees_by_guarantor_{}", duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &cache_key, || async {
        state
            .store
            .get_guarantees_by_guarantor(duration.as_pg_interval(), duration.secondary_interval())
            .await
            .map_err(|e| {
                error!("Failed to get guarantees by guarantor: {}", e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

// ============================================================================
// LOW PRIORITY: Timeline & Grouped Analytics Endpoints
// ============================================================================

/// Query parameters for grouped timeseries
#[derive(Deserialize)]
struct TimeseriesGroupedQuery {
    /// Metric type: events, guarantees, failures
    metric: String,
    /// Group by: node, event_type, core, category
    group_by: String,
    /// Interval in minutes (default 5)
    interval: Option<i32>,
    /// Duration in hours (default 1)
    duration: Option<i32>,
}

/// Get time-series metrics grouped by node, event_type, core, etc.
async fn get_timeseries_grouped(
    Query(params): Query<TimeseriesGroupedQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = params.interval.unwrap_or(5);
    let duration = params.duration.unwrap_or(1);

    match state
        .store
        .get_timeseries_grouped(&params.metric, &params.group_by, interval, duration)
        .await
    {
        Ok(data) => Ok(Json(data)),
        Err(e) => {
            error!("Failed to get grouped timeseries: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Query parameters for timeline endpoints
#[derive(Deserialize)]
struct TimelineQuery {
    /// Duration in hours (default 1)
    duration: Option<i32>,
}

/// Get sync status timeline - synced vs out-of-sync nodes over time.
async fn get_sync_status_timeline(
    Query(params): Query<TimelineQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = params.duration.unwrap_or(1);

    match state.store.get_sync_status_timeline(duration).await {
        Ok(timeline) => Ok(Json(timeline)),
        Err(e) => {
            error!("Failed to get sync status timeline: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

/// Get connection health timeline - connections/disconnections over time.
async fn get_connections_timeline(
    Query(params): Query<TimelineQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let duration = params.duration.unwrap_or(1);

    match state.store.get_connections_timeline(duration).await {
        Ok(timeline) => Ok(Json(timeline)),
        Err(e) => {
            error!("Failed to get connections timeline: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// ============================================================================
// DASHBOARD: Mock Data Replacement Endpoints
// ============================================================================

/// Get validators assigned to a specific core with node IDs and client info.
async fn get_core_validators(
    Path(core_index): Path<i32>,
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    if core_index < 0 {
        warn!("Invalid core_index: {}", core_index);
        return Err(StatusCode::BAD_REQUEST);
    }

    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let key = format!("core_validators_{}_{}", core_index, duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &key, || async {
        state
            .store
            .get_core_validators(core_index, duration.as_pg_interval())
            .await
            .map_err(|e| {
                error!("Failed to get core {} validators: {}", core_index, e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get real-time performance metrics for a specific core.
async fn get_core_metrics(
    Path(core_index): Path<i32>,
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    if core_index < 0 {
        warn!("Invalid core_index: {}", core_index);
        return Err(StatusCode::BAD_REQUEST);
    }

    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let key = format!("core_metrics_{}_{}", core_index, duration.cache_suffix());
    let result = cache_or_compute(&state.cache, &key, || async {
        state
            .store
            .get_core_metrics(core_index, duration.as_pg_interval())
            .await
            .map_err(|e| {
                error!("Failed to get core {} metrics: {}", core_index, e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;

    // Overwrite self-JOIN latency fields with in-memory stats if available
    if let Some(ref tracker) = state.metrics_tracker {
        if let Some(processing) = tracker.get_core_processing_snapshot(core_index as u16) {
            let mut val = result.as_ref().clone();
            if let Some(obj) = val.as_object_mut() {
                if let Some(avg) = processing.get("avg_completion_ms") {
                    obj.insert("network_latency_ms".into(), avg.clone());
                    obj.insert("average_completion_time_ms".into(), avg.clone());
                }
                if let Some(p95) = processing.get("p95_completion_ms") {
                    obj.insert("p95_latency_ms".into(), p95.clone());
                }
                if let Some(count) = processing.get("sample_count") {
                    obj.insert("sample_count".into(), count.clone());
                }
            }
            return Ok((cache_headers(2), Json(Arc::new(val))));
        }
    }

    Ok((cache_headers(2), Json(result)))
}

/// Get bottleneck analysis for a specific core.
async fn get_core_bottlenecks(
    Path(core_index): Path<i32>,
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    if core_index < 0 {
        warn!("Invalid core_index: {}", core_index);
        return Err(StatusCode::BAD_REQUEST);
    }

    let duration = dq.duration.unwrap_or(DurationPreset::OneHour);
    let key = format!(
        "core_bottlenecks_{}_{}",
        core_index,
        duration.cache_suffix()
    );
    let result = cache_or_compute(&state.cache, &key, || async {
        state
            .store
            .get_core_bottlenecks(core_index, duration.as_pg_interval())
            .await
            .map_err(|e| {
                error!("Failed to get core {} bottlenecks: {}", core_index, e);
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get guarantors for a core with import sharing data.
async fn get_core_guarantors_enhanced(
    Path(core_index): Path<i32>,
    Query(dq): Query<DurationQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    if core_index < 0 {
        warn!("Invalid core_index: {}", core_index);
        return Err(StatusCode::BAD_REQUEST);
    }

    let duration = dq.duration.unwrap_or(DurationPreset::TwentyFourHours);
    let key = format!(
        "core_guarantors_enhanced_{}_{}",
        core_index,
        duration.cache_suffix()
    );
    let result = cache_or_compute(&state.cache, &key, || async {
        state
            .store
            .get_core_guarantors_with_sharing(core_index)
            .await
            .map_err(|e| {
                error!(
                    "Failed to get core {} guarantors with sharing: {}",
                    core_index, e
                );
                StatusCode::INTERNAL_SERVER_ERROR
            })
    })
    .await?;
    Ok((cache_headers(2), Json(result)))
}

/// Get audit progress for a specific work package.
async fn get_workpackage_audit_progress(
    Path(hash): Path<String>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    // Basic validation - hash should be hex
    if hash.is_empty() || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        warn!("Invalid work package hash format: {}", hash);
        return Err(StatusCode::BAD_REQUEST);
    }

    match state.store.get_workpackage_audit_progress(&hash).await {
        Ok(progress) => Ok(Json(progress)),
        Err(e) => {
            error!("Failed to get WP {} audit progress: {}", hash, e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
}

// ============================================================================
// Frontend Search & Explorer Endpoints
// ============================================================================

#[derive(Deserialize)]
struct EventSearchQuery {
    event_types: Option<String>, // comma-separated list of event type integers
    node_id: Option<String>,
    core_index: Option<i32>,
    wp_hash: Option<String>,
    start_time: Option<String>, // ISO 8601
    end_time: Option<String>,   // ISO 8601
    limit: Option<i64>,
    offset: Option<i64>,
}

/// Multi-criteria event search with pagination
async fn search_events(
    Query(query): Query<EventSearchQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let limit = query.limit.unwrap_or(50).clamp(1, MAX_QUERY_LIMIT);
    let offset = query.offset.unwrap_or(0).max(0);

    // Parse event_types from comma-separated string
    let event_types: Option<Vec<i32>> = query.event_types.as_ref().map(|s| {
        s.split(',')
            .filter_map(|t| t.trim().parse::<i32>().ok())
            .collect()
    });

    // Parse timestamps; default to last 1 hour if no start_time to prevent full table scans
    let start_time = query
        .start_time
        .as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|| Some(chrono::Utc::now() - chrono::Duration::minutes(1)));

    let end_time = query
        .end_time
        .as_ref()
        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
        .map(|dt| dt.with_timezone(&chrono::Utc));

    // Validate node_id if provided
    if let Some(ref nid) = query.node_id {
        if !is_valid_node_id(nid) {
            warn!("Invalid node_id format in search: {}", nid);
            return Err(StatusCode::BAD_REQUEST);
        }
    }

    match state
        .store
        .search_events(
            event_types.as_deref(),
            query.node_id.as_deref(),
            query.core_index,
            query.wp_hash.as_deref(),
            start_time,
            end_time,
            limit,
            offset,
        )
        .await
    {
        Ok(results) => Ok((no_cache_headers(), Json(results))),
        Err(e) => {
            error!("Failed to search events: {}", e);
            Err(StatusCode::INTERNAL_SERVER_ERROR)
        }
    }
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

#[derive(Deserialize)]
struct BatchJourneyRequest {
    hashes: Vec<String>,
}

/// Batch work package journey lookup
async fn batch_workpackage_journeys(
    State(state): State<ApiState>,
    Json(body): Json<BatchJourneyRequest>,
) -> Result<impl IntoResponse, StatusCode> {
    if body.hashes.is_empty() {
        return Ok(Json(serde_json::json!({
            "journeys": [],
            "timestamp": chrono::Utc::now(),
        })));
    }

    if body.hashes.len() > 50 {
        warn!(
            "Batch journey request too large: {} hashes",
            body.hashes.len()
        );
        return Err(StatusCode::BAD_REQUEST);
    }

    // Validate all hashes
    for hash in &body.hashes {
        if hash.is_empty() || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
            warn!("Invalid work package hash in batch: {}", hash);
            return Err(StatusCode::BAD_REQUEST);
        }
    }

    match state.store.batch_workpackage_journeys(&body.hashes).await {
        Ok(journeys) => Ok(Json(journeys)),
        Err(e) => {
            error!("Failed to get batch WP journeys: {}", e);
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

            // Periodic stats updates (read from cache to avoid N*DB queries for N WS clients)
            _ = stats_interval.tick(), if has_db => {
                let stats_result = cache_or_compute(cache.as_ref().unwrap(), "stats", || {
                    let store = store.clone().unwrap();
                    async move { store.get_stats("1 hour", "24 hours").await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR) }
                }).await;

                if let Ok(db_stats) = stats_result {
                    let broadcaster_stats = broadcaster.get_stats();
                    let node_ids = telemetry_server.get_connection_ids();

                    let response = WebSocketResponse {
                        r#type: "stats".to_string(),
                        data: serde_json::json!({
                            "database": (*db_stats),
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
