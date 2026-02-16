mod common;

use axum::http::StatusCode;
use axum_test::TestServer;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::encoding::encode_message;
use tart_backend::events::Event;
use tart_backend::types::*;
use tart_backend::{EventStore, TelemetryServer};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::sleep;

async fn setup_test_api() -> (TestServer, Arc<TelemetryServer>, u16) {
    // Setup test database (PostgreSQL)
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://tart:tart_password@localhost:5432/tart_test".to_string());

    eprintln!("DEBUG: Connecting to database: {}", database_url);
    let store = Arc::new(
        EventStore::new(&database_url)
            .await
            .expect("Failed to connect to database"),
    );

    eprintln!("DEBUG: Cleaning test data...");
    store
        .cleanup_test_data()
        .await
        .expect("Failed to cleanup test data");
    eprintln!("DEBUG: Cleanup completed");

    // Small delay to ensure cleanup completes
    sleep(Duration::from_millis(50)).await;

    // Find available port for telemetry
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let telemetry_port = listener.local_addr().unwrap().port();
    drop(listener);

    let telemetry_bind = format!("127.0.0.1:{}", telemetry_port);
    let telemetry_server = Arc::new(
        TelemetryServer::new(&telemetry_bind, Arc::clone(&store))
            .await
            .unwrap(),
    );

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
            std::time::Duration::from_secs(5),
        )),
    };

    let app = create_api_router(api_state);
    let test_server = TestServer::new(app).unwrap();

    // Give servers time to start
    sleep(Duration::from_millis(100)).await;

    (test_server, telemetry_server, telemetry_port)
}

async fn connect_test_node_with_server(
    port: u16,
    node_id: u8,
    telemetry_server: &Arc<TelemetryServer>,
) -> TcpStream {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let mut node_info = common::test_node_info([node_id; 32]);
    node_info.implementation_name = BoundedString::new(&format!("test-node-{}", node_id)).unwrap();

    eprintln!("DEBUG: Sending node info for node {}", node_id);
    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    // Wait for server to receive, decode, queue AND write to database
    // The batch writer automatically flushes NodeConnected immediately
    sleep(Duration::from_millis(50)).await;

    // Verify the node was actually written by flushing
    eprintln!("DEBUG: Flushing after node {} connection", node_id);
    flush_and_wait(telemetry_server).await;
    eprintln!("DEBUG: Node {} connection confirmed in database", node_id);

    stream
}

async fn flush_and_wait(telemetry_server: &Arc<TelemetryServer>) {
    // Flush batch writer and wait for completion
    // This ensures all queued writes have been processed and written to PostgreSQL
    // before we query the database in tests
    eprintln!("DEBUG: Calling flush_writes()");
    match telemetry_server.flush_writes().await {
        Ok(_) => eprintln!("DEBUG: Flush completed successfully"),
        Err(e) => {
            eprintln!("ERROR: Flush failed: {}", e);
            panic!("Flush failed: {}", e);
        }
    }
    // Delay to ensure PostgreSQL commit completes and data is visible to other connections
    sleep(Duration::from_millis(100)).await;
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
    let response = server.get("/api/events/search?event_types=11&limit=10").await;
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

    let response = server
        .get(&format!("/api/nodes/{}/peers", node_id))
        .await;
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
