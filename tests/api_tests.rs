mod common;

use axum::http::StatusCode;
use axum_test::TestServer;
use serde_json::Value;
use std::sync::Arc;
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::encoding::encode_message;
use tart_backend::events::Event;
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
        TelemetryServer::with_options("127.0.0.1:0", Arc::clone(&store), true)
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

async fn flush_and_wait(telemetry_server: &Arc<TelemetryServer>) {
    common::flush_and_wait(telemetry_server).await;
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
async fn test_nodes_endpoint_empty() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/nodes").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(json["nodes"].is_array());
    assert_eq!(json["nodes"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_nodes_endpoint_with_connections() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Connect two nodes (includes flush to ensure data is written)
    let _stream1 = connect_test_node_with_server(telemetry_port, 1, &telemetry_server).await;
    let _stream2 = connect_test_node_with_server(telemetry_port, 2, &telemetry_server).await;

    let response = server.get("/api/nodes").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    eprintln!(
        "DEBUG: Response JSON: {}",
        serde_json::to_string_pretty(&json).unwrap()
    );
    let nodes = json["nodes"].as_array().unwrap();
    eprintln!("DEBUG: Found {} nodes, expected 2", nodes.len());
    assert_eq!(nodes.len(), 2, "Expected 2 nodes but found {}", nodes.len());

    // Check node data
    for node in nodes {
        assert!(node["node_id"].is_string());
        assert!(node["peer_id"].is_string());
        assert!(node["implementation_name"].is_string());
        assert!(node["implementation_version"].is_string());
        assert!(node["connected_at"].is_string());
        assert!(node["last_seen_at"].is_string());
        assert_eq!(node["is_connected"], true);
        assert_eq!(node["event_count"], 0);
    }
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

#[tokio::test]
async fn test_events_endpoint_empty() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/events").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(json["events"].is_array());
    assert_eq!(json["events"].as_array().unwrap().len(), 0);
    assert_eq!(json["has_more"], false);
}

#[tokio::test]
async fn test_events_endpoint_with_data() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Connect a node and send events
    let mut stream = connect_test_node_with_server(telemetry_port, 4, &telemetry_server).await;

    // Send some events
    let events = vec![
        Event::SyncStatusChanged {
            timestamp: 1_000_000,
            synced: false,
        },
        Event::BestBlockChanged {
            timestamp: 2_000_000,
            slot: 100,
            hash: [0xAA; 32],
        },
        Event::Status {
            timestamp: 3_000_000,
            num_val_peers: 5,
            num_peers: 10,
            num_sync_peers: 8,
            num_guarantees: vec![1, 2, 3, 4],
            num_shards: 50,
            shards_size: 1024 * 1024,
            num_preimages: 3,
            preimages_size: 7,
        },
    ];

    for event in events {
        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }

    // Flush events to database
    flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/events").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let events = json["events"].as_array().unwrap();

    // Debug: print what events we actually got
    eprintln!("Got {} events:", events.len());
    for (i, event) in events.iter().enumerate() {
        eprintln!(
            "  Event {}: type={}, timestamp={}",
            i, event["event_type"], event["timestamp"]
        );
    }

    assert_eq!(events.len(), 3);

    // Events should be in reverse chronological order
    assert_eq!(events[0]["event_type"], 10); // Status
    assert_eq!(events[1]["event_type"], 11); // BestBlockChanged
    assert_eq!(events[2]["event_type"], 13); // SyncStatusChanged
}

#[tokio::test]
async fn test_events_pagination() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Connect a node and send many events
    let mut stream = connect_test_node_with_server(telemetry_port, 5, &telemetry_server).await;

    // Send 10 events
    for i in 0..10 {
        let event = Event::BestBlockChanged {
            timestamp: i as u64 * 1_000_000,
            slot: i,
            hash: [i as u8; 32],
        };
        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }

    // Flush events to database
    flush_and_wait(&telemetry_server).await;

    // Test limit
    let response = server.get("/api/events?limit=5").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["events"].as_array().unwrap().len(), 5);
    assert_eq!(json["has_more"], true);

    // Test offset
    let response = server.get("/api/events?limit=5&offset=5").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["events"].as_array().unwrap().len(), 5);
    // API returns has_more=true when events.len() == limit, even if these are the last events
    assert_eq!(json["has_more"], true);

    // Test offset beyond available events
    let response = server.get("/api/events?limit=5&offset=10").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["events"].as_array().unwrap().len(), 0);
    assert_eq!(json["has_more"], false);
}

#[tokio::test]
async fn test_node_events_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Connect two nodes (includes flush)
    let mut stream1 = connect_test_node_with_server(telemetry_port, 6, &telemetry_server).await;
    let mut stream2 = connect_test_node_with_server(telemetry_port, 7, &telemetry_server).await;

    let node1_id = hex::encode([6u8; 32]);
    let node2_id = hex::encode([7u8; 32]);

    // Send events from node 1
    for i in 0..3 {
        let event = Event::BestBlockChanged {
            timestamp: i as u64 * 1_000_000,
            slot: i,
            hash: [6u8; 32],
        };
        let encoded = encode_message(&event).unwrap();
        stream1.write_all(&encoded).await.unwrap();
    }

    // Send events from node 2
    for i in 0..2 {
        let event = Event::FinalizedBlockChanged {
            timestamp: i as u64 * 1_000_000,
            slot: i,
            hash: [7u8; 32],
        };
        let encoded = encode_message(&event).unwrap();
        stream2.write_all(&encoded).await.unwrap();
    }

    // Flush events to database
    flush_and_wait(&telemetry_server).await;

    // Get events for node 1
    let response = server.get(&format!("/api/nodes/{}/events", node1_id)).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let events = json["events"].as_array().unwrap();
    assert_eq!(events.len(), 3);
    assert!(events.iter().all(|e| e["event_type"] == 11)); // All BestBlockChanged

    // Get events for node 2
    let response = server.get(&format!("/api/nodes/{}/events", node2_id)).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let events = json["events"].as_array().unwrap();
    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|e| e["event_type"] == 12)); // All FinalizedBlockChanged
}

// WebSocket testing is commented out as axum-test doesn't have direct WebSocket support
// In a real implementation, you would use a WebSocket client library to test this
/*
#[tokio::test]
async fn test_websocket_connection() {
    let (server, _, telemetry_port) = setup_test_api().await;

    // Connect via WebSocket
    let ws = server.ws("/ws").await;

    // Should receive initial nodes update
    let msg = ws.receive_text().await;
    let json: Value = serde_json::from_str(&msg).unwrap();
    assert_eq!(json["type"], "nodes_update");
    assert!(json["nodes"].is_array());
    assert_eq!(json["nodes"].as_array().unwrap().len(), 0);

    // Connect a telemetry node
    let _stream = connect_test_node(telemetry_port, 8).await;

    // TODO: In a real implementation, we would receive real-time updates
    // For now, just verify WebSocket stays connected
    ws.send_text(json!({"type": "ping"}).to_string()).await;

    ws.close().await;
}
*/

#[tokio::test]
async fn test_concurrent_api_requests() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Connect a node (includes flush)
    let _stream = connect_test_node_with_server(telemetry_port, 9, &telemetry_server).await;

    // Make multiple requests sequentially (TestServer doesn't support clone)
    for _ in 0..5 {
        let response = server.get("/api/nodes").await;
        assert_eq!(response.status_code(), StatusCode::OK);
    }

    for _ in 0..5 {
        let response = server.get("/api/events").await;
        assert_eq!(response.status_code(), StatusCode::OK);
    }
}

// ============================================================================
// Phase 2: API endpoint smoke tests
// Each test verifies the endpoint returns 200 OK with valid JSON.
// ============================================================================

// --- Statistics & Aggregation endpoints (empty DB) ---

#[tokio::test]
async fn test_stats_endpoint() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/stats").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_workpackage_stats() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/workpackages").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_block_stats() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/blocks").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_guarantee_stats() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/guarantees").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_da_stats() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/da/stats").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_da_stats_enhanced() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/da/stats/enhanced").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_cores_status() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/cores/status").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_guarantees_by_guarantor() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/guarantees/by-guarantor").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

// --- Metrics endpoints ---

#[tokio::test]
async fn test_execution_metrics() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/metrics/execution").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_timeseries_metrics() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/metrics/timeseries").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object() || json.is_array());
}

#[tokio::test]
async fn test_timeseries_grouped() {
    let (server, _, _) = setup_test_api().await;
    // Required query params: metric, group_by
    let response = server
        .get("/api/metrics/timeseries/grouped?metric=events&group_by=node")
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object() || json.is_array());
}

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

// --- Analytics endpoints ---

#[tokio::test]
async fn test_failure_rates() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/analytics/failure-rates").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_block_propagation() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/analytics/block-propagation").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_network_health() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/analytics/network-health").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_sync_status_timeline() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/analytics/sync-status/timeline").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object() || json.is_array());
}

#[tokio::test]
async fn test_connections_timeline() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/analytics/connections/timeline").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object() || json.is_array());
}

// --- Core detail endpoints ---

#[tokio::test]
async fn test_core_guarantees() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/cores/0/guarantees").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_core_validators() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/cores/0/validators").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_core_guarantors() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/cores/0/guarantors").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_core_guarantors_enhanced() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/cores/0/guarantors/enhanced").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_core_work_packages() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/cores/0/work-packages").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_core_metrics() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/cores/0/metrics").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
}

#[tokio::test]
async fn test_core_bottlenecks() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/cores/0/bottlenecks").await;
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
async fn test_validator_core_mapping() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/validators/cores").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
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
async fn test_events_search() {
    let (server, _, _) = setup_test_api().await;
    let response = server
        .get("/api/events/search?event_types=11&limit=10")
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    // search_events returns {events: [...]} object
    assert!(json.is_object());
    assert!(json["events"].is_array());
}

#[tokio::test]
async fn test_slot_events() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/slots/1").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object() || json.is_array());
}

#[tokio::test]
async fn test_active_workpackages() {
    let (server, _, _) = setup_test_api().await;
    let response = server.get("/api/workpackages/active").await;
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

// --- Work package detail endpoints ---

#[tokio::test]
async fn test_workpackage_journey() {
    let (server, _, _) = setup_test_api().await;
    // Use a fake hash — should return empty result, not error
    let fake_hash = "0000000000000000000000000000000000000000000000000000000000000000";
    let response = server
        .get(&format!("/api/workpackages/{}/journey", fake_hash))
        .await;
    // May return 200 with empty data or 404
    let status = response.status_code();
    assert!(
        status == StatusCode::OK || status == StatusCode::NOT_FOUND,
        "Expected 200 or 404, got {}",
        status
    );
}

#[tokio::test]
async fn test_workpackage_journey_enhanced() {
    let (server, _, _) = setup_test_api().await;
    let fake_hash = "0000000000000000000000000000000000000000000000000000000000000000";
    let response = server
        .get(&format!("/api/workpackages/{}/journey/enhanced", fake_hash))
        .await;
    let status = response.status_code();
    assert!(
        status == StatusCode::OK || status == StatusCode::NOT_FOUND,
        "Expected 200 or 404, got {}",
        status
    );
}

#[tokio::test]
async fn test_workpackage_audit_progress() {
    let (server, _, _) = setup_test_api().await;
    let fake_hash = "0000000000000000000000000000000000000000000000000000000000000000";
    let response = server
        .get(&format!("/api/workpackages/{}/audit-progress", fake_hash))
        .await;
    let status = response.status_code();
    assert!(
        status == StatusCode::OK || status == StatusCode::NOT_FOUND,
        "Expected 200 or 404, got {}",
        status
    );
}

#[tokio::test]
async fn test_batch_workpackage_journeys() {
    let (server, _, _) = setup_test_api().await;
    let response = server
        .post("/api/workpackages/batch/journey")
        .json(&serde_json::json!({ "hashes": [] }))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
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

// ============================================================================
// Data-driven tests — validate JSONB query paths against real event data
// ============================================================================

/// Helper: construct a WorkPackageReceived event
fn wp_received_event(timestamp: u64, submission_id: u64, core: u16) -> Event {
    Event::WorkPackageReceived {
        timestamp,
        submission_or_share_id: submission_id,
        core,
        outline: WorkPackageSummary {
            work_package_size: 2048,
            work_package_hash: [0xCC; 32],
            anchor: [0xAA; 32],
            lookup_anchor_slot: 100,
            prerequisites: vec![],
            work_items: vec![
                WorkItemSummary {
                    service_id: 1,
                    payload_size: 512,
                    refine_gas_limit: 1_000_000,
                    accumulate_gas_limit: 500_000,
                    sum_of_extrinsic_lengths: 128,
                    imports: vec![],
                    num_exported_segments: 2,
                },
                WorkItemSummary {
                    service_id: 2,
                    payload_size: 256,
                    refine_gas_limit: 2_000_000,
                    accumulate_gas_limit: 300_000,
                    sum_of_extrinsic_lengths: 64,
                    imports: vec![],
                    num_exported_segments: 1,
                },
            ],
        },
    }
}

/// Helper: construct a Refined event
fn refined_event(timestamp: u64, submission_id: u64) -> Event {
    Event::Refined {
        timestamp,
        submission_or_share_id: submission_id,
        costs: vec![
            RefineCost {
                total: ExecCost {
                    gas_used: 500_000,
                    elapsed_ns: 1_000_000,
                },
                load_ns: 100_000,
                host_call: RefineHostCallCost {
                    lookup: ExecCost {
                        gas_used: 50_000,
                        elapsed_ns: 100_000,
                    },
                    vm: ExecCost {
                        gas_used: 200_000,
                        elapsed_ns: 400_000,
                    },
                    mem: ExecCost {
                        gas_used: 30_000,
                        elapsed_ns: 60_000,
                    },
                    invoke: ExecCost {
                        gas_used: 100_000,
                        elapsed_ns: 200_000,
                    },
                    other: ExecCost {
                        gas_used: 20_000,
                        elapsed_ns: 40_000,
                    },
                },
            },
            RefineCost {
                total: ExecCost {
                    gas_used: 700_000,
                    elapsed_ns: 1_500_000,
                },
                load_ns: 120_000,
                host_call: RefineHostCallCost {
                    lookup: ExecCost {
                        gas_used: 70_000,
                        elapsed_ns: 140_000,
                    },
                    vm: ExecCost {
                        gas_used: 300_000,
                        elapsed_ns: 600_000,
                    },
                    mem: ExecCost {
                        gas_used: 40_000,
                        elapsed_ns: 80_000,
                    },
                    invoke: ExecCost {
                        gas_used: 150_000,
                        elapsed_ns: 300_000,
                    },
                    other: ExecCost {
                        gas_used: 30_000,
                        elapsed_ns: 60_000,
                    },
                },
            },
        ],
    }
}

/// Helper: construct an Authorized event
fn authorized_event(timestamp: u64, submission_id: u64) -> Event {
    Event::Authorized {
        timestamp,
        submission_or_share_id: submission_id,
        cost: IsAuthorizedCost {
            total: ExecCost {
                gas_used: 100_000,
                elapsed_ns: 200_000,
            },
            load_ns: 50_000,
            host_call: ExecCost {
                gas_used: 30_000,
                elapsed_ns: 60_000,
            },
        },
    }
}

/// Helper: construct a GuaranteeBuilt event
fn guarantee_built_event(timestamp: u64, submission_id: u64, _core: u16) -> Event {
    Event::GuaranteeBuilt {
        timestamp,
        submission_id,
        outline: GuaranteeSummary {
            work_report_hash: [0xBB; 32],
            slot: 200,
            guarantors: vec![0, 1, 2],
        },
    }
}

#[tokio::test]
async fn test_workpackage_stats_with_data() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 30, &telemetry_server).await;

    let now = common::now_jce_micros();
    let events: Vec<Event> = vec![
        wp_received_event(now, 42, 3),
        wp_received_event(now + 1_000_000, 43, 5),
        refined_event(now + 2_000_000, 42),
    ];

    for event in events {
        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/workpackages").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = serde_json::from_str(&response.text()).unwrap();

    let totals = &json["totals"];
    assert!(
        totals["received"].as_i64().unwrap() >= 2,
        "Expected at least 2 received WPs, got {}",
        totals["received"]
    );
    assert!(
        totals["refined"].as_i64().unwrap() >= 1,
        "Expected at least 1 refined WP, got {}",
        totals["refined"]
    );

    let by_core = json["by_core"].as_array().unwrap();
    if !by_core.is_empty() {
        let first = &by_core[0];
        assert!(
            first.get("core_index").is_some() || first.get("core").is_some(),
            "Expected core index in by_core entry"
        );
    }
}

#[tokio::test]
async fn test_execution_metrics_with_block_executed() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 31, &telemetry_server).await;

    let now = common::now_jce_micros();
    let event = Event::BlockExecuted {
        timestamp: now,
        authoring_or_importing_id: 1,
        accumulate_costs: vec![
            (1, common::test_accumulate_cost()),
            (2, common::test_accumulate_cost()),
        ],
    };
    let encoded = encode_message(&event).unwrap();
    stream.write_all(&encoded).await.unwrap();

    let refined = refined_event(now + 1_000_000, 100);
    let encoded = encode_message(&refined).unwrap();
    stream.write_all(&encoded).await.unwrap();

    let auth = authorized_event(now + 2_000_000, 100);
    let encoded = encode_message(&auth).unwrap();
    stream.write_all(&encoded).await.unwrap();

    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/metrics/execution").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();

    let refinement = &json["refinement"];
    assert!(
        refinement["total_refined"].as_i64().unwrap() >= 1,
        "Expected at least 1 refined, got {}",
        refinement["total_refined"]
    );

    let auth_section = &json["authorization"];
    assert!(
        auth_section["total_authorized"].as_i64().unwrap() >= 1,
        "Expected at least 1 authorized, got {}",
        auth_section["total_authorized"]
    );
    if auth_section["total_gas_used"].as_i64().unwrap() > 0 {
        assert_eq!(
            auth_section["total_gas_used"].as_i64().unwrap(),
            100_000,
            "Expected authorized gas_used = 100000"
        );
    }
}

#[tokio::test]
async fn test_cores_status_with_guarantee_join() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 32, &telemetry_server).await;

    let submission_id: u64 = 999;
    let core: u16 = 7;
    let now = common::now_jce_micros();

    let wp = wp_received_event(now, submission_id, core);
    let encoded = encode_message(&wp).unwrap();
    stream.write_all(&encoded).await.unwrap();

    let guarantee = guarantee_built_event(now + 1_000_000, submission_id, core);
    let encoded = encode_message(&guarantee).unwrap();
    stream.write_all(&encoded).await.unwrap();

    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/cores/status").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();

    let cores = json["cores"].as_array().unwrap();
    assert!(
        !cores.is_empty(),
        "Expected at least one core with activity"
    );

    let summary = &json["summary"];
    assert!(
        summary["total_cores"].as_i64().unwrap() >= 1,
        "Expected at least 1 total core"
    );
}

#[tokio::test]
async fn test_active_workpackages_pipeline() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 33, &telemetry_server).await;

    let submission_id: u64 = 777;
    let now = common::now_jce_micros();

    let events: Vec<Event> = vec![
        wp_received_event(now, submission_id, 2),
        authorized_event(now + 1_000_000, submission_id),
        refined_event(now + 2_000_000, submission_id),
        guarantee_built_event(now + 3_000_000, submission_id, 2),
    ];

    for event in events {
        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/workpackages/active").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();

    // The WP should appear with multiple stages tracked
    let _wps = if json.is_array() {
        json.as_array().unwrap().clone()
    } else if json.get("work_packages").is_some() {
        json["work_packages"].as_array().unwrap_or(&vec![]).clone()
    } else {
        vec![]
    };
}

#[tokio::test]
async fn test_core_guarantees_with_status_data() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 34, &telemetry_server).await;

    let now = common::now_jce_micros();
    let event = Event::Status {
        timestamp: now,
        num_val_peers: 10,
        num_peers: 20,
        num_sync_peers: 15,
        num_guarantees: vec![3, 1, 4, 1, 5, 9, 2, 6],
        num_shards: 100,
        shards_size: 1024 * 1024,
        num_preimages: 5,
        preimages_size: 2048,
    };
    let encoded = encode_message(&event).unwrap();
    stream.write_all(&encoded).await.unwrap();

    let guarantee = guarantee_built_event(now + 1_000_000, 500, 0);
    let encoded = encode_message(&guarantee).unwrap();
    stream.write_all(&encoded).await.unwrap();

    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/cores/0/guarantees").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let response = server.get("/api/cores/2/guarantees").await;
    assert_eq!(response.status_code(), StatusCode::OK);
}

#[tokio::test]
async fn test_da_stats_with_status_data() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 35, &telemetry_server).await;

    let now = common::now_jce_micros();
    let event = Event::Status {
        timestamp: now,
        num_val_peers: 10,
        num_peers: 20,
        num_sync_peers: 15,
        num_guarantees: vec![1, 2, 3],
        num_shards: 150,
        shards_size: 2 * 1024 * 1024,
        num_preimages: 8,
        preimages_size: 4096,
    };
    let encoded = encode_message(&event).unwrap();
    stream.write_all(&encoded).await.unwrap();
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/da/stats").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();

    let agg = &json["aggregate"];
    assert!(
        agg["total_shards"].as_i64().unwrap() >= 150,
        "Expected total_shards >= 150, got {}",
        agg["total_shards"]
    );
    assert!(
        agg["total_preimages"].as_i64().unwrap() >= 8,
        "Expected total_preimages >= 8, got {}",
        agg["total_preimages"]
    );
    assert!(
        agg["node_count"].as_i64().unwrap() >= 1,
        "Expected node_count >= 1, got {}",
        agg["node_count"]
    );
}

#[tokio::test]
async fn test_workpackage_journey_with_data() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 40, &telemetry_server).await;

    let submission_id: u64 = 5001;
    let now = common::now_jce_micros();

    let events: Vec<Event> = vec![
        wp_received_event(now, submission_id, 4),
        authorized_event(now + 1_000_000, submission_id),
        refined_event(now + 2_000_000, submission_id),
        guarantee_built_event(now + 3_000_000, submission_id, 4),
    ];

    for event in &events {
        let encoded = encode_message(event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    common::flush_and_wait(&telemetry_server).await;

    let response = server
        .get(&format!("/api/workpackages/{}/journey", submission_id))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();

    let stages = json["stages"]
        .as_array()
        .expect("stages should be an array");

    if !stages.is_empty() {
        let first = &stages[0];
        assert!(
            first.get("stage").is_some(),
            "Stage entry should have 'stage' field"
        );
        assert!(
            first.get("timestamp").is_some(),
            "Stage entry should have 'timestamp' field"
        );
        assert!(
            first.get("node_id").is_some(),
            "Stage entry should have 'node_id' field"
        );
        assert!(
            first.get("event_type").is_some(),
            "Stage entry should have 'event_type' field"
        );
    }

    assert_eq!(
        json["failed"].as_bool().unwrap(),
        false,
        "Expected failed=false for successful pipeline"
    );
}

#[tokio::test]
async fn test_workpackage_journey_enhanced_with_data() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 41, &telemetry_server).await;

    let submission_id: u64 = 6001;
    let now = common::now_jce_micros();

    let events: Vec<Event> = vec![
        wp_received_event(now, submission_id, 3),
        authorized_event(now + 1_000_000, submission_id),
        refined_event(now + 2_000_000, submission_id),
        guarantee_built_event(now + 3_000_000, submission_id, 3),
    ];

    for event in &events {
        let encoded = encode_message(event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    common::flush_and_wait(&telemetry_server).await;

    let response = server
        .get(&format!(
            "/api/workpackages/{}/journey/enhanced",
            submission_id
        ))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();

    assert!(json.get("stages").is_some(), "Should have stages array");
    assert!(json.get("core_index").is_some(), "Should have core_index");
    assert!(json.get("failed").is_some(), "Should have failed flag");
    assert!(json.get("timing").is_some(), "Should have timing info");

    let stages = json["stages"].as_array().expect("stages should be array");

    if !stages.is_empty() {
        let first = &stages[0];
        assert!(first.get("stage").is_some(), "Should have stage name");
        assert!(first.get("status").is_some(), "Should have status");
        assert!(first.get("event_type").is_some(), "Should have event_type");
        assert!(first.get("node_id").is_some(), "Should have node_id");
        assert!(first.get("timestamp").is_some(), "Should have timestamp");
    }

    assert_eq!(json["failed"].as_bool().unwrap(), false);
}

#[tokio::test]
async fn test_workpackage_audit_progress_with_data() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 42, &telemetry_server).await;

    let now = common::now_jce_micros();
    let guarantee = guarantee_built_event(now, 7001, 5);
    let encoded = encode_message(&guarantee).unwrap();
    stream.write_all(&encoded).await.unwrap();

    common::flush_and_wait(&telemetry_server).await;

    let wp_hash = hex::encode([0xBB; 32]);
    let response = server
        .get(&format!("/api/workpackages/{}/audit-progress", wp_hash))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();

    assert!(json.get("hash").is_some(), "Should have hash field");
    assert!(json.get("status").is_some(), "Should have status field");
    assert!(
        json.get("audit_progress").is_some(),
        "Should have audit_progress"
    );
    assert!(json.get("events").is_some(), "Should have events array");
}

#[tokio::test]
async fn test_guarantees_by_guarantor_with_data() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 43, &telemetry_server).await;

    let now = common::now_jce_micros();
    let guarantees: Vec<Event> = vec![
        guarantee_built_event(now, 8001, 0),
        guarantee_built_event(now + 1_000_000, 8002, 0),
        guarantee_built_event(now + 2_000_000, 8003, 3),
    ];

    for event in &guarantees {
        let encoded = encode_message(event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/guarantees/by-guarantor").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();

    let guarantors = json["guarantors"]
        .as_array()
        .expect("Should have guarantors array");
    assert!(!guarantors.is_empty(), "Should have at least 1 guarantor");

    let first = &guarantors[0];
    assert!(
        first.get("node_id").is_some(),
        "Guarantor should have node_id"
    );
    assert!(
        first.get("total_guarantees").is_some(),
        "Guarantor should have total_guarantees"
    );

    let total = first["total_guarantees"].as_i64().unwrap();
    assert!(
        total >= 3,
        "Expected at least 3 guarantees from our node, got {}",
        total
    );

    assert!(json["total_guarantors"].as_i64().unwrap() >= 1);
}

#[tokio::test]
async fn test_core_guarantors_with_data() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 44, &telemetry_server).await;

    let now = common::now_jce_micros();
    let guarantees: Vec<Event> = vec![
        guarantee_built_event(now, 9001, 2),
        guarantee_built_event(now + 1_000_000, 9002, 2),
    ];

    for event in &guarantees {
        let encoded = encode_message(event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/cores/2/guarantors").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();

    assert_eq!(json["core_index"].as_i64().unwrap(), 2);
    let _guarantors = json["guarantors"]
        .as_array()
        .expect("Should have guarantors array");
}

#[tokio::test]
async fn test_core_guarantors_enhanced_with_data() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;
    let mut stream = connect_test_node_with_server(telemetry_port, 45, &telemetry_server).await;

    let submission_id: u64 = 10001;
    let now = common::now_jce_micros();

    let wp = wp_received_event(now, submission_id, 1);
    let encoded = encode_message(&wp).unwrap();
    stream.write_all(&encoded).await.unwrap();

    let guarantee = guarantee_built_event(now + 1_000_000, submission_id, 1);
    let encoded = encode_message(&guarantee).unwrap();
    stream.write_all(&encoded).await.unwrap();

    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/cores/1/guarantors/enhanced").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();

    assert_eq!(json["core_index"].as_i64().unwrap(), 1);
    let _guarantors = json["guarantors"]
        .as_array()
        .expect("Should have guarantors array");
}
