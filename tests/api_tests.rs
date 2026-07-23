mod common;

use axum::http::StatusCode;
use axum_test::TestServer;
use serde_json::Value;
use std::sync::Arc;
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::encoding::encode_message;
use tart_backend::types::*;
use tart_backend::{EventStore, TelemetryServer};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

async fn setup_test_api() -> (TestServer, Arc<TelemetryServer>, u16) {
    let database_url = common::test_database_url();

    let store = Arc::new(
        EventStore::new(&database_url)
            .await
            .expect("Failed to connect to database"),
    );

    store
        .cleanup_test_data()
        .await
        .expect("Failed to cleanup test data");

    let telemetry_server = Arc::new(
        TelemetryServer::with_options("127.0.0.1:0", Some(Arc::clone(&store)), true, 0)
            .await
            .unwrap(),
    );
    let telemetry_port = telemetry_server.local_addr().unwrap().port();

    // Start telemetry server
    let telemetry_server_clone = Arc::clone(&telemetry_server);
    tokio::spawn(async move {
        telemetry_server_clone.run().await.unwrap();
    });

    // Get the broadcaster from telemetry server for API WebSocket connections
    let broadcaster = telemetry_server.get_broadcaster();

    // Create health monitor
    let health_monitor = Arc::new(tart_backend::health::HealthMonitor::new());

    // Create API state and router
    let api_state = ApiState {
        store,
        telemetry_server: Arc::clone(&telemetry_server),
        broadcaster,
        health_monitor,
        jam_rpc: None,
        cache: Arc::new(tart_backend::cache::TtlCache::new(
            std::time::Duration::ZERO,
        )),
        metrics_tracker: None,
    };

    let app = create_api_router(api_state);
    let test_server = TestServer::new(app).unwrap();

    (test_server, telemetry_server, telemetry_port)
}

async fn connect_test_node_with_server(
    port: u16,
    node_id: u8,
    telemetry_server: &Arc<TelemetryServer>,
) -> TcpStream {
    let expected = telemetry_server.connection_count() + 1;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let mut node_info = common::test_node_info([node_id; 32]);
    node_info.implementation_name = BoundedString::new(&format!("test-node-{}", node_id)).unwrap();

    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    telemetry_server.wait_for_connections(expected).await;
    common::flush_and_wait(telemetry_server).await;

    stream
}

#[tokio::test]
async fn test_health_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/health").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["status"], "ok");
    assert_eq!(json["service"], "tart-backend");
    assert!(json.get("version").is_some());
}

#[tokio::test]
async fn test_node_details_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Connect a node (includes flush)
    let _stream = connect_test_node_with_server(telemetry_port, 3, &telemetry_server).await;

    // Get node ID (hex encoded peer_id)
    let node_id = hex::encode([3u8; 32]);

    let response = server.get(&format!("/api/nodes/{}", node_id)).await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["node_id"], node_id);
    assert_eq!(json["implementation_name"], "test-node-3");
    assert_eq!(json["is_connected"], true);
    assert!(json["connection_info"].is_object());

    let conn_info = &json["connection_info"];
    assert!(conn_info["address"].is_string());
    assert_eq!(conn_info["event_count"], 0);
    assert!(conn_info["connected_duration_secs"].is_number());
}

#[tokio::test]
async fn test_node_details_not_found() {
    let (server, _, _) = setup_test_api().await;

    // Use a valid hex node ID that doesn't exist (64 hex chars = 32 bytes)
    let nonexistent_node_id = "0000000000000000000000000000000000000000000000000000000000000000";
    let response = server
        .get(&format!("/api/nodes/{}", nonexistent_node_id))
        .await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
}

// --- Metrics endpoints ---

#[tokio::test]
async fn test_realtime_metrics() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/metrics/realtime?seconds=60").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_live_counters() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/metrics/live").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

// --- Other endpoints ---

#[tokio::test]
async fn test_network_info() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/network").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_detailed_health() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/health/detailed").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
    assert!(json.get("status").is_some());
    assert!(json.get("components").is_some());
    assert!(json.get("uptime_seconds").is_some());
}

#[tokio::test]
async fn test_peer_topology() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/network/topology").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_slot_events() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/slots/1").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object() || json.is_array());
}

// --- Node-specific endpoints (require connected node) ---

#[tokio::test]
async fn test_node_status() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let _stream = connect_test_node_with_server(telemetry_port, 10, &telemetry_server).await;
    let node_id = hex::encode([10u8; 32]);

    let response = server.get(&format!("/api/nodes/{}/status", node_id)).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_node_status_enhanced() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let _stream = connect_test_node_with_server(telemetry_port, 11, &telemetry_server).await;
    let node_id = hex::encode([11u8; 32]);

    let response = server
        .get(&format!("/api/nodes/{}/status/enhanced", node_id))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_node_peers() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let _stream = connect_test_node_with_server(telemetry_port, 12, &telemetry_server).await;
    let node_id = hex::encode([12u8; 32]);

    let response = server.get(&format!("/api/nodes/{}/peers", node_id)).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_node_timeline() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let _stream = connect_test_node_with_server(telemetry_port, 13, &telemetry_server).await;
    let node_id = hex::encode([13u8; 32]);

    let response = server
        .get(&format!("/api/nodes/{}/timeline", node_id))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object() || json.is_array());
}

// --- JAM RPC endpoints (no RPC configured → 503) ---

#[tokio::test]
async fn test_jam_stats_no_rpc() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/jam/stats").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_jam_services_no_rpc() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/jam/services").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_jam_cores_no_rpc() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/jam/cores").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);
}
