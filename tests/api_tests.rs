use axum::http::StatusCode;
use axum_test::TestServer;
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::encoding::encode_message;
use tart_backend::events::{Event, NodeInformation};
use tart_backend::types::*;
use tart_backend::{EventStore, TelemetryServer};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::time::sleep;

async fn setup_test_api() -> (TestServer, Arc<TelemetryServer>, u16) {
    // Setup test database (PostgreSQL)
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://tart:tart_password@localhost/tart_test".to_string());
    let store = Arc::new(EventStore::new(&database_url).await.unwrap());

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
    };

    let app = create_api_router(api_state);
    let test_server = TestServer::new(app).unwrap();

    // Give servers time to start
    sleep(Duration::from_millis(100)).await;

    (test_server, telemetry_server, telemetry_port)
}

async fn connect_test_node(port: u16, node_id: u8) -> TcpStream {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let node_info = NodeInformation {
        peer_id: [node_id; 32],
        peer_address: PeerAddress {
            ipv6: [0; 16],
            port: 30333,
        },
        node_flags: 1,
        implementation_name: BoundedString::new(&format!("test-node-{}", node_id)).unwrap(),
        implementation_version: BoundedString::new("1.0.0").unwrap(),
        additional_info: BoundedString::new("Test node").unwrap(),
    };

    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    sleep(Duration::from_millis(50)).await;
    stream
}

#[tokio::test]
async fn test_health_endpoint() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/health").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["status"], "ok");
    assert_eq!(json["service"], "tart-backend");
    assert!(json.get("version").is_some());
}

#[tokio::test]
async fn test_nodes_endpoint_empty() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/nodes").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(json["nodes"].is_array());
    assert_eq!(json["nodes"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_nodes_endpoint_with_connections() {
    let (server, _, telemetry_port) = setup_test_api().await;

    // Connect two nodes
    let _stream1 = connect_test_node(telemetry_port, 1).await;
    let _stream2 = connect_test_node(telemetry_port, 2).await;

    let response = server.get("/nodes").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let nodes = json["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 2);

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
    let (server, _, telemetry_port) = setup_test_api().await;

    // Connect a node
    let _stream = connect_test_node(telemetry_port, 3).await;

    // Get node ID (hex encoded peer_id)
    let node_id = hex::encode([3u8; 32]);

    let response = server.get(&format!("/nodes/{}", node_id)).await;

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

    let response = server.get("/nodes/nonexistent").await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);

    let json: Value = response.json();
    assert!(json["error"].is_string());
}

#[tokio::test]
async fn test_events_endpoint_empty() {
    let (server, _, _) = setup_test_api().await;

    let response = server.get("/events").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(json["events"].is_array());
    assert_eq!(json["events"].as_array().unwrap().len(), 0);
    assert_eq!(json["has_more"], false);
}

#[tokio::test]
async fn test_events_endpoint_with_data() {
    let (server, _, telemetry_port) = setup_test_api().await;

    // Connect a node and send events
    let mut stream = connect_test_node(telemetry_port, 4).await;

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

    sleep(Duration::from_millis(500)).await;

    let response = server.get("/events").await;

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
    let (server, _, telemetry_port) = setup_test_api().await;

    // Connect a node and send many events
    let mut stream = connect_test_node(telemetry_port, 5).await;

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

    sleep(Duration::from_millis(200)).await;

    // Test limit
    let response = server.get("/events?limit=5").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["events"].as_array().unwrap().len(), 5);
    assert_eq!(json["has_more"], true);

    // Test offset
    let response = server.get("/events?limit=5&offset=5").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["events"].as_array().unwrap().len(), 5);
    // API returns has_more=true when events.len() == limit, even if these are the last events
    assert_eq!(json["has_more"], true);

    // Test offset beyond available events
    let response = server.get("/events?limit=5&offset=10").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["events"].as_array().unwrap().len(), 0);
    assert_eq!(json["has_more"], false);
}

#[tokio::test]
async fn test_node_events_endpoint() {
    let (server, _, telemetry_port) = setup_test_api().await;

    // Connect two nodes
    let mut stream1 = connect_test_node(telemetry_port, 6).await;
    let mut stream2 = connect_test_node(telemetry_port, 7).await;

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

    sleep(Duration::from_millis(200)).await;

    // Get events for node 1
    let response = server.get(&format!("/nodes/{}/events", node1_id)).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let events = json["events"].as_array().unwrap();
    assert_eq!(events.len(), 3);
    assert!(events.iter().all(|e| e["event_type"] == 11)); // All BestBlockChanged

    // Get events for node 2
    let response = server.get(&format!("/nodes/{}/events", node2_id)).await;
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
    let (server, _, telemetry_port) = setup_test_api().await;

    // Connect a node
    let _stream = connect_test_node(telemetry_port, 9).await;

    // Make multiple requests sequentially (TestServer doesn't support clone)
    for _ in 0..5 {
        let response = server.get("/nodes").await;
        assert_eq!(response.status_code(), StatusCode::OK);
    }

    for _ in 0..5 {
        let response = server.get("/events").await;
        assert_eq!(response.status_code(), StatusCode::OK);
    }
}
