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
    // Setup test database (PostgreSQL)
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
        TelemetryServer::new("127.0.0.1:0", Some(Arc::clone(&store)))
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
        store: Some(store),
        telemetry_server: Arc::clone(&telemetry_server),
        broadcaster,
        health_monitor,
        jam_rpc: None,
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
    let nodes = json["nodes"].as_array().unwrap();
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
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/events").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let events = json["events"].as_array().unwrap();

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
    common::flush_and_wait(&telemetry_server).await;

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
    common::flush_and_wait(&telemetry_server).await;

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
// Tier 1 — Critical endpoint tests (query DB with potentially renamed columns)
// ============================================================================

#[tokio::test]
async fn test_network_info_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Empty DB
    let response = server.get("/api/network").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/network empty response: {}", serde_json::to_string_pretty(&json).unwrap());

    // With connected nodes
    let _stream1 = connect_test_node_with_server(telemetry_port, 10, &telemetry_server).await;
    let _stream2 = connect_test_node_with_server(telemetry_port, 11, &telemetry_server).await;

    let response = server.get("/api/network").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/network with nodes: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_workpackage_stats_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/workpackages").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/workpackages response: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_block_stats_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Empty DB
    let response = server.get("/api/blocks").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/blocks empty: {}", serde_json::to_string_pretty(&json).unwrap());

    // With block events
    let mut stream = connect_test_node_with_server(telemetry_port, 12, &telemetry_server).await;

    let events = vec![
        Event::BestBlockChanged {
            timestamp: 1_000_000,
            slot: 100,
            hash: [0xBB; 32],
        },
        Event::BlockExecuted {
            timestamp: 2_000_000,
            authoring_or_importing_id: 1,
            accumulate_costs: vec![(1, common::test_accumulate_cost())],
        },
    ];

    for event in events {
        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/blocks").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/blocks with data: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_guarantee_stats_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/guarantees").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/guarantees response: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_cores_status_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/cores/status").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/cores/status response: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_node_status_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Invalid node_id → 400
    let response = server.get("/api/nodes/invalid/status").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);

    // Connect a node and send BestBlockChanged
    let mut stream = connect_test_node_with_server(telemetry_port, 13, &telemetry_server).await;
    let node_id = hex::encode([13u8; 32]);

    let event = Event::BestBlockChanged {
        timestamp: 5_000_000,
        slot: 200,
        hash: [0xCC; 32],
    };
    let encoded = encode_message(&event).unwrap();
    stream.write_all(&encoded).await.unwrap();
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get(&format!("/api/nodes/{}/status", node_id)).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/nodes/:id/status: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_node_peers_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Invalid node_id → 400
    let response = server.get("/api/nodes/short/peers").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);

    // Connect a node
    let _stream = connect_test_node_with_server(telemetry_port, 14, &telemetry_server).await;
    let node_id = hex::encode([14u8; 32]);

    let response = server.get(&format!("/api/nodes/{}/peers", node_id)).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/nodes/:id/peers: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_da_stats_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/da/stats").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/da/stats response: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_execution_metrics_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Empty DB
    let response = server.get("/api/metrics/execution").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/metrics/execution empty: {}", serde_json::to_string_pretty(&json).unwrap());

    // With BlockExecuted events
    let mut stream = connect_test_node_with_server(telemetry_port, 15, &telemetry_server).await;

    let event = Event::BlockExecuted {
        timestamp: 3_000_000,
        authoring_or_importing_id: 1,
        accumulate_costs: vec![
            (1, common::test_accumulate_cost()),
            (2, common::test_accumulate_cost()),
        ],
    };
    let encoded = encode_message(&event).unwrap();
    stream.write_all(&encoded).await.unwrap();
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/metrics/execution").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/metrics/execution with data: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_timeseries_metrics_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // With events
    let mut stream = connect_test_node_with_server(telemetry_port, 16, &telemetry_server).await;

    for i in 0..5 {
        let event = Event::BestBlockChanged {
            timestamp: i as u64 * 1_000_000,
            slot: i,
            hash: [i as u8; 32],
        };
        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    common::flush_and_wait(&telemetry_server).await;

    // Default params
    let response = server.get("/api/metrics/timeseries").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/metrics/timeseries default: {}", serde_json::to_string_pretty(&json).unwrap());

    // With query params
    let response = server.get("/api/metrics/timeseries?metric=throughput&interval=5&duration=1").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/metrics/timeseries params: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_search_events_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Connect node and send diverse events
    let mut stream = connect_test_node_with_server(telemetry_port, 17, &telemetry_server).await;
    let node_id = hex::encode([17u8; 32]);

    let events = vec![
        Event::BestBlockChanged {
            timestamp: 1_000_000,
            slot: 50,
            hash: [0xDD; 32],
        },
        Event::SyncStatusChanged {
            timestamp: 2_000_000,
            synced: true,
        },
        Event::FinalizedBlockChanged {
            timestamp: 3_000_000,
            slot: 49,
            hash: [0xEE; 32],
        },
    ];

    for event in events {
        let encoded = encode_message(&event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
    common::flush_and_wait(&telemetry_server).await;

    // No filters → returns recent events
    let response = server.get("/api/events/search").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    // Filter by event_types (11 = BestBlockChanged)
    let response = server.get("/api/events/search?event_types=11").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: search by event_type=11: {}", serde_json::to_string_pretty(&json).unwrap());

    // Filter by node_id
    let response = server.get(&format!("/api/events/search?node_id={}", node_id)).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    // Invalid node_id → 400
    let response = server.get("/api/events/search?node_id=invalid").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_slot_events_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    let mut stream = connect_test_node_with_server(telemetry_port, 18, &telemetry_server).await;

    // Send events at slot 999
    let event = Event::BestBlockChanged {
        timestamp: 1_000_000,
        slot: 999,
        hash: [0xFF; 32],
    };
    let encoded = encode_message(&event).unwrap();
    stream.write_all(&encoded).await.unwrap();
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get("/api/slots/999").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/slots/999: {}", serde_json::to_string_pretty(&json).unwrap());

    // Slot with no events
    let response = server.get("/api/slots/0").await;
    assert_eq!(response.status_code(), StatusCode::OK);
}

// ============================================================================
// Tier 2 — Important endpoint tests
// ============================================================================

#[tokio::test]
async fn test_workpackage_journey_endpoint() {
    let (server, _, _) = setup_test_api().await;

    // Invalid hash → 400
    let response = server.get("/api/workpackages/not-hex/journey").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);

    // Valid hex hash with no data
    let hash = "aa".repeat(32); // 64 hex chars
    let response = server.get(&format!("/api/workpackages/{}/journey", hash)).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/workpackages/:hash/journey empty: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_active_workpackages_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/workpackages/active").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/workpackages/active: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_core_guarantees_endpoint() {
    let (server, _, _) = setup_test_api().await;

    // Valid core_index
    let response = server.get("/api/cores/0/guarantees").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/cores/0/guarantees: {}", serde_json::to_string_pretty(&json).unwrap());

    // Negative core_index → 400 (axum parses i32, -1 is valid i32 but handler rejects)
    let response = server.get("/api/cores/-1/guarantees").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_validator_core_mapping_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/validators/cores").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/validators/cores: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_peer_topology_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/network/topology").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/network/topology: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_realtime_metrics_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/metrics/realtime").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/metrics/realtime: {}", serde_json::to_string_pretty(&json).unwrap());

    // With seconds param
    let response = server.get("/api/metrics/realtime?seconds=30").await;
    assert_eq!(response.status_code(), StatusCode::OK);
}

#[tokio::test]
async fn test_live_counters_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/metrics/live").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/metrics/live: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_failure_rates_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/analytics/failure-rates").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/analytics/failure-rates: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_network_health_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/analytics/network-health").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/analytics/network-health: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_block_propagation_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/analytics/block-propagation").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/analytics/block-propagation: {}", serde_json::to_string_pretty(&json).unwrap());
}

// ============================================================================
// Tier 3 — JAM RPC endpoints (503 when not configured)
// ============================================================================

#[tokio::test]
async fn test_jam_endpoints_without_rpc() {
    let (server, _, _) = setup_test_api().await;

    // All JAM RPC endpoints should return 503 when jam_rpc is None
    let response = server.get("/api/jam/stats").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);

    let response = server.get("/api/jam/services").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);

    let response = server.get("/api/jam/cores").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);
}

// ============================================================================
// Tier 4 — Additional endpoint coverage
// ============================================================================

#[tokio::test]
async fn test_core_validators_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/cores/0/validators").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/cores/0/validators: {}", serde_json::to_string_pretty(&json).unwrap());

    // Negative core_index → 400
    let response = server.get("/api/cores/-1/validators").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_core_metrics_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/cores/0/metrics").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/cores/0/metrics: {}", serde_json::to_string_pretty(&json).unwrap());

    let response = server.get("/api/cores/-1/metrics").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_core_bottlenecks_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/cores/0/bottlenecks").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/cores/0/bottlenecks: {}", serde_json::to_string_pretty(&json).unwrap());

    let response = server.get("/api/cores/-1/bottlenecks").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_da_stats_enhanced_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/da/stats/enhanced").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/da/stats/enhanced: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_node_timeline_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Invalid node_id → 400
    let response = server.get("/api/nodes/bad/timeline").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);

    // Connect a node and send events
    let mut stream = connect_test_node_with_server(telemetry_port, 19, &telemetry_server).await;
    let node_id = hex::encode([19u8; 32]);

    let event = Event::BestBlockChanged {
        timestamp: 1_000_000,
        slot: 42,
        hash: [0xAA; 32],
    };
    let encoded = encode_message(&event).unwrap();
    stream.write_all(&encoded).await.unwrap();
    common::flush_and_wait(&telemetry_server).await;

    let response = server.get(&format!("/api/nodes/{}/timeline", node_id)).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/nodes/:id/timeline: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_sync_status_timeline_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/analytics/sync-status/timeline").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/analytics/sync-status/timeline: {}", serde_json::to_string_pretty(&json).unwrap());

    // With duration param
    let response = server.get("/api/analytics/sync-status/timeline?duration=2").await;
    assert_eq!(response.status_code(), StatusCode::OK);
}

#[tokio::test]
async fn test_connections_timeline_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/analytics/connections/timeline").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/analytics/connections/timeline: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_batch_workpackage_journeys() {
    let (server, _, _) = setup_test_api().await;

    // Empty hashes → returns empty journeys
    let response = server
        .post("/api/workpackages/batch/journey")
        .json(&serde_json::json!({"hashes": []}))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json["journeys"].is_array());
    assert_eq!(json["journeys"].as_array().unwrap().len(), 0);

    // Valid hashes with no data
    let hash = "bb".repeat(32);
    let response = server
        .post("/api/workpackages/batch/journey")
        .json(&serde_json::json!({"hashes": [hash]}))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);

    // Too many hashes → 400
    let hashes: Vec<String> = (0..51).map(|i| format!("{:064x}", i)).collect();
    let response = server
        .post("/api/workpackages/batch/journey")
        .json(&serde_json::json!({"hashes": hashes}))
        .await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);

    // Invalid hash in batch → 400
    let response = server
        .post("/api/workpackages/batch/journey")
        .json(&serde_json::json!({"hashes": ["not-hex"]}))
        .await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_timeseries_grouped_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/metrics/timeseries/grouped?metric=events&group_by=event_type").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/metrics/timeseries/grouped: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_detailed_health_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/health/detailed").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/health/detailed: {}", serde_json::to_string_pretty(&json).unwrap());
    assert!(json["status"].is_string());
    assert!(json["timestamp"].is_string());
    assert!(json["components"].is_object());
    assert!(json["version"].is_string());
    assert!(json["uptime_seconds"].is_number());
    assert!(json["summary"].is_object());
}

#[tokio::test]
async fn test_stats_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/stats").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/stats: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_node_status_enhanced_endpoint() {
    let (server, telemetry_server, telemetry_port) = setup_test_api().await;

    // Invalid node_id → 400
    let response = server.get("/api/nodes/bad/status/enhanced").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);

    // Connect a node
    let _stream = connect_test_node_with_server(telemetry_port, 20, &telemetry_server).await;
    let node_id = hex::encode([20u8; 32]);

    let response = server.get(&format!("/api/nodes/{}/status/enhanced", node_id)).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/nodes/:id/status/enhanced: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_core_guarantors_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/cores/0/guarantors").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/cores/0/guarantors: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_core_work_packages_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/cores/0/work-packages").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/cores/0/work-packages: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_workpackage_journey_enhanced_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let hash = "cc".repeat(32);
    let response = server.get(&format!("/api/workpackages/{}/journey/enhanced", hash)).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/workpackages/:hash/journey/enhanced: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_workpackage_audit_progress_endpoint() {
    let (server, _, _) = setup_test_api().await;

    // Invalid hash → 400
    let response = server.get("/api/workpackages/not-hex/audit-progress").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);

    // Valid hash
    let hash = "dd".repeat(32);
    let response = server.get(&format!("/api/workpackages/{}/audit-progress", hash)).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/workpackages/:hash/audit-progress: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_guarantees_by_guarantor_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/guarantees/by-guarantor").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/guarantees/by-guarantor: {}", serde_json::to_string_pretty(&json).unwrap());
}

#[tokio::test]
async fn test_core_guarantors_enhanced_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/api/cores/0/guarantors/enhanced").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    eprintln!("DEBUG: /api/cores/0/guarantors/enhanced: {}", serde_json::to_string_pretty(&json).unwrap());

    let response = server.get("/api/cores/-1/guarantors/enhanced").await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}
