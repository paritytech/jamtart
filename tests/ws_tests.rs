mod common;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::{EventStore, TelemetryServer};
use tokio::time::sleep;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

/// Starts a real HTTP server (not axum-test) so WebSocket upgrades work.
/// Returns (server_url, telemetry_server, telemetry_port).
async fn setup_ws_test() -> (String, Arc<TelemetryServer>, u16) {
    let database_url = std::env::var("TEST_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://tart:tart_password@localhost:5432/tart_test".to_string());

    let store = Arc::new(
        EventStore::new(&database_url)
            .await
            .expect("Failed to connect to database"),
    );
    store
        .cleanup_test_data()
        .await
        .expect("Failed to cleanup test data");
    sleep(Duration::from_millis(50)).await;

    // Start telemetry server on random port
    let tl = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let telemetry_port = tl.local_addr().unwrap().port();
    drop(tl);

    let telemetry_bind = format!("127.0.0.1:{}", telemetry_port);
    let telemetry_server = Arc::new(
        TelemetryServer::new(&telemetry_bind, Arc::clone(&store))
            .await
            .unwrap(),
    );

    let ts_clone = Arc::clone(&telemetry_server);
    tokio::spawn(async move {
        ts_clone.run().await.unwrap();
    });

    let broadcaster = telemetry_server.get_broadcaster();
    let health_monitor = Arc::new(tart_backend::health::HealthMonitor::new());

    let api_state = ApiState {
        store,
        telemetry_server: Arc::clone(&telemetry_server),
        broadcaster,
        health_monitor,
        jam_rpc: None,
        cache: Arc::new(tart_backend::cache::TtlCache::new(Duration::from_secs(5))),
    };

    let app = create_api_router(api_state);

    // Bind to random port for the HTTP server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    sleep(Duration::from_millis(100)).await;

    let server_url = format!("http://127.0.0.1:{}", addr.port());
    (server_url, telemetry_server, telemetry_port)
}

#[tokio::test]
async fn test_ws_connect() {
    let (server_url, _, _) = setup_ws_test().await;
    let ws_url = server_url.replace("http://", "ws://") + "/api/ws";

    let (mut ws, _) = connect_async(&ws_url).await.expect("Failed to connect");

    // Should receive initial "connected" message
    let msg = tokio::time::timeout(Duration::from_secs(5), ws.next())
        .await
        .expect("Timeout waiting for connected message")
        .expect("Stream ended")
        .expect("WS error");

    if let Message::Text(text) = msg {
        let json: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["type"], "connected");
        assert!(json["data"]["message"].is_string());
    } else {
        panic!("Expected text message, got {:?}", msg);
    }

    ws.close(None).await.ok();
}

#[tokio::test]
async fn test_ws_ping_pong() {
    let (server_url, _, _) = setup_ws_test().await;
    let ws_url = server_url.replace("http://", "ws://") + "/api/ws";

    let (mut ws, _) = connect_async(&ws_url).await.expect("Failed to connect");

    // Consume the initial "connected" message
    let _ = tokio::time::timeout(Duration::from_secs(5), ws.next()).await;

    // Send a Ping message
    let ping_msg = serde_json::json!({"type": "Ping"});
    ws.send(Message::Text(ping_msg.to_string()))
        .await
        .unwrap();

    // Read messages until we find "pong" (stats may arrive first)
    let mut found_pong = false;
    for _ in 0..5 {
        let msg = tokio::time::timeout(Duration::from_secs(6), ws.next())
            .await
            .expect("Timeout waiting for pong")
            .expect("Stream ended")
            .expect("WS error");

        if let Message::Text(text) = msg {
            let json: Value = serde_json::from_str(&text).unwrap();
            if json["type"] == "pong" {
                found_pong = true;
                break;
            }
        }
    }
    assert!(found_pong, "Never received 'pong' response");

    ws.close(None).await.ok();
}

#[tokio::test]
async fn test_ws_subscribe_all() {
    let (server_url, _, _) = setup_ws_test().await;
    let ws_url = server_url.replace("http://", "ws://") + "/api/ws";

    let (mut ws, _) = connect_async(&ws_url).await.expect("Failed to connect");

    // Consume the initial "connected" message
    let _ = tokio::time::timeout(Duration::from_secs(5), ws.next()).await;

    // Send Subscribe{All}
    let sub_msg = serde_json::json!({"type": "Subscribe", "filter": {"type": "All"}});
    ws.send(Message::Text(sub_msg.to_string())).await.unwrap();

    // Read messages until we find the "subscribed" confirmation
    // (stats updates may arrive first due to async timing)
    let mut found_subscribed = false;
    for _ in 0..5 {
        let msg = tokio::time::timeout(Duration::from_secs(6), ws.next())
            .await
            .expect("Timeout waiting for subscribed")
            .expect("Stream ended")
            .expect("WS error");

        if let Message::Text(text) = msg {
            let json: Value = serde_json::from_str(&text).unwrap();
            if json["type"] == "subscribed" {
                found_subscribed = true;
                break;
            }
        }
    }
    assert!(found_subscribed, "Never received 'subscribed' confirmation");

    ws.close(None).await.ok();
}

#[tokio::test]
async fn test_ws_get_recent_events() {
    let (server_url, _, _) = setup_ws_test().await;
    let ws_url = server_url.replace("http://", "ws://") + "/api/ws";

    let (mut ws, _) = connect_async(&ws_url).await.expect("Failed to connect");

    // Consume the initial "connected" message
    let _ = tokio::time::timeout(Duration::from_secs(5), ws.next()).await;

    // Request recent events
    let req = serde_json::json!({"type": "GetRecentEvents", "limit": 10});
    ws.send(Message::Text(req.to_string())).await.unwrap();

    // Read messages until we find "recent_events" (stats may arrive first)
    let mut found = false;
    for _ in 0..5 {
        let msg = tokio::time::timeout(Duration::from_secs(6), ws.next())
            .await
            .expect("Timeout waiting for recent_events")
            .expect("Stream ended")
            .expect("WS error");

        if let Message::Text(text) = msg {
            let json: Value = serde_json::from_str(&text).unwrap();
            if json["type"] == "recent_events" {
                assert!(json["data"]["events"].is_array());
                found = true;
                break;
            }
        }
    }
    assert!(found, "Never received 'recent_events' response");

    ws.close(None).await.ok();
}

#[tokio::test]
async fn test_ws_stats_updates() {
    let (server_url, _, _) = setup_ws_test().await;
    let ws_url = server_url.replace("http://", "ws://") + "/api/ws";

    let (mut ws, _) = connect_async(&ws_url).await.expect("Failed to connect");

    // Consume the initial "connected" message
    let _ = tokio::time::timeout(Duration::from_secs(5), ws.next()).await;

    // Wait for the periodic stats update (every 5 seconds)
    let msg = tokio::time::timeout(Duration::from_secs(7), ws.next())
        .await
        .expect("Timeout waiting for stats update")
        .expect("Stream ended")
        .expect("WS error");

    if let Message::Text(text) = msg {
        let json: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(json["type"], "stats");
        assert!(json["data"].is_object());
    } else {
        panic!("Expected stats update, got {:?}", msg);
    }

    ws.close(None).await.ok();
}
