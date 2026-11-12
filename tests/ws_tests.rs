mod common;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::events::Event;
use tart_backend::{EventStore, TelemetryServer};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const SLOT_MICROS: u64 = 6_000_000;
const MICROS_PER_MILLI: u64 = 1_000;

/// Read the next text frame as JSON.
async fn next_json(ws: &mut WsStream) -> Value {
    let msg = tokio::time::timeout(Duration::from_secs(8), ws.next())
        .await
        .expect("Timeout waiting for WS frame")
        .expect("Stream ended")
        .expect("WS error");
    match msg {
        Message::Text(text) => serde_json::from_str(&text).unwrap(),
        other => panic!("Expected text frame, got {:?}", other),
    }
}

/// Read frames until one satisfies `predicate`, skipping the others.
async fn read_until(ws: &mut WsStream, predicate: impl Fn(&Value) -> bool) -> Value {
    for _ in 0..50 {
        let json = next_json(ws).await;
        if predicate(&json) {
            return json;
        }
    }
    panic!("No matching frame received");
}

/// The per-event envelopes (`id`, `node_id`, `event_type`, `event`) carried by
/// an `event` or `batch` frame; none for any other frame.
fn event_envelopes(frame: &Value) -> Vec<Value> {
    match frame["type"].as_str() {
        Some("event") => vec![frame["data"].clone()],
        Some("batch") => frame["data"]
            .as_array()
            .unwrap()
            .iter()
            .map(|event| event["data"].clone())
            .collect(),
        _ => Vec::new(),
    }
}

/// Collect every event envelope delivered until one matches `is_marker`,
/// so a test sees everything that arrived before its marker event.
async fn read_events_until(ws: &mut WsStream, is_marker: impl Fn(&Value) -> bool) -> Vec<Value> {
    let mut envelopes = Vec::new();
    for _ in 0..50 {
        let frame = next_json(ws).await;
        for envelope in event_envelopes(&frame) {
            let found = is_marker(&envelope);
            envelopes.push(envelope);
            if found {
                return envelopes;
            }
        }
    }
    panic!("Marker event never arrived, got {envelopes:?}");
}

async fn open_ws(server_url: &str) -> WsStream {
    let ws_url = server_url.replace("http://", "ws://") + "/api/ws";
    let (mut ws, _) = connect_async(&ws_url).await.expect("Failed to connect");
    assert_eq!(next_json(&mut ws).await["type"], "connected");
    ws
}

async fn subscribe(ws: &mut WsStream, filter: Value) {
    let request = serde_json::json!({"type": "Subscribe", "filter": filter});
    ws.send(Message::Text(request.to_string())).await.unwrap();
    read_until(ws, |json| json["type"] == "subscribed").await;
}

fn best_block(ts: u64, slot: u32) -> Event {
    common::best_block_event_with_hash(ts, slot, common::block_hash(slot))
}

fn is_best_block(slot: u32) -> impl Fn(&Value) -> bool {
    move |envelope| {
        envelope["event_type"] == 11 && envelope["event"]["BestBlockChanged"]["slot"] == slot
    }
}

fn event_types(envelopes: &[Value]) -> Vec<u64> {
    envelopes
        .iter()
        .map(|envelope| envelope["event_type"].as_u64().unwrap())
        .collect()
}

fn node_ids(envelopes: &[Value]) -> Vec<&str> {
    envelopes
        .iter()
        .map(|envelope| envelope["node_id"].as_str().unwrap())
        .collect()
}

/// Starts a real HTTP server (not axum-test) so WebSocket upgrades work.
/// Returns (server_url, telemetry_server, telemetry_port).
async fn setup_ws_test() -> (String, Arc<TelemetryServer>, u16) {
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
        metrics_tracker: None,
    };

    let app = create_api_router(api_state);

    // Bind to random port for the HTTP server
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

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
    ws.send(Message::Text(ping_msg.to_string())).await.unwrap();

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

/// A subscriber to all events receives each event as it arrives, in order,
/// with the payload the node sent.
#[tokio::test]
async fn test_ws_subscribe_all() {
    let (server_url, telemetry_server, telemetry_port) = setup_ws_test().await;
    let mut ws = open_ws(&server_url).await;
    let mut node = common::connect_test_node(telemetry_port, 1, &telemetry_server).await;
    subscribe(&mut ws, serde_json::json!({"type": "All"})).await;

    let slot_1_at = common::now_jce_micros() - SLOT_MICROS;
    let sent = [
        best_block(slot_1_at, 1),
        best_block(slot_1_at + SLOT_MICROS, 2),
    ];
    common::send_events(&mut node, &sent).await;

    let received = read_events_until(&mut ws, is_best_block(2)).await;
    let node_id = common::node_id_hex(1);
    assert_eq!(node_ids(&received), [node_id.as_str(), node_id.as_str()]);
    let payloads: Vec<&Value> = received.iter().map(|envelope| &envelope["event"]).collect();
    let expected: Vec<Value> = sent
        .iter()
        .map(|event| serde_json::to_value(event).unwrap())
        .collect();
    assert_eq!(payloads, expected.iter().collect::<Vec<_>>());

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

/// An event-type subscription delivers only events of that type.
#[tokio::test]
#[ignore = "bug: WebSocket EventType filter delivers every event"]
async fn test_ws_subscribe_eventtype_filters() {
    let (server_url, telemetry_server, telemetry_port) = setup_ws_test().await;
    let mut ws = open_ws(&server_url).await;
    let mut node = common::connect_test_node(telemetry_port, 1, &telemetry_server).await;
    subscribe(
        &mut ws,
        serde_json::json!({"type": "EventType", "event_type": 11}),
    )
    .await;

    let status_at = common::now_jce_micros() - SLOT_MICROS;
    common::send_events(
        &mut node,
        &[
            common::status_event(status_at),
            best_block(status_at + 500 * MICROS_PER_MILLI, 1),
        ],
    )
    .await;

    let received = read_events_until(&mut ws, is_best_block(1)).await;
    assert_eq!(event_types(&received), [11]);
    ws.close(None).await.ok();
}

/// An event-type range subscription delivers only events whose type is in the range.
#[tokio::test]
#[ignore = "bug: WebSocket EventTypeRange filter delivers every event"]
async fn test_ws_subscribe_eventtype_range_filters() {
    let (server_url, telemetry_server, telemetry_port) = setup_ws_test().await;
    let mut ws = open_ws(&server_url).await;
    let mut node = common::connect_test_node(telemetry_port, 1, &telemetry_server).await;
    subscribe(
        &mut ws,
        serde_json::json!({"type": "EventTypeRange", "start": 90, "end": 113}),
    )
    .await;

    let slot_1_at = common::now_jce_micros() - SLOT_MICROS;
    common::send_events(
        &mut node,
        &[
            best_block(slot_1_at, 1),
            common::status_event(slot_1_at + 900 * MICROS_PER_MILLI),
            common::wp_submission_event(slot_1_at + 1_500 * MICROS_PER_MILLI, [0x50; 32]),
        ],
    )
    .await;

    let received = read_events_until(&mut ws, |envelope| envelope["event_type"] == 90).await;
    assert_eq!(event_types(&received), [90]);
    ws.close(None).await.ok();
}

/// The metrics channel reports the last 10 seconds of events by event time:
/// older blocks fall out of the window, healthy work-package events are not
/// failures, and the latest slot is the highest best block of the last minute.
#[tokio::test]
async fn test_ws_metrics_payload_values() {
    let (server_url, telemetry_server, telemetry_port) = setup_ws_test().await;
    let mut ws = open_ws(&server_url).await;
    let mut author = common::connect_test_node(telemetry_port, 1, &telemetry_server).await;
    let mut importer = common::connect_test_node(telemetry_port, 2, &telemetry_server).await;

    let slot_102_at = common::now_jce_micros() - 1_000 * MICROS_PER_MILLI;
    common::send_events(
        &mut author,
        &[
            best_block(slot_102_at - 2 * SLOT_MICROS, 100),
            best_block(slot_102_at - SLOT_MICROS, 101),
            best_block(slot_102_at, 102),
        ],
    )
    .await;
    common::send_events(
        &mut importer,
        &[best_block(slot_102_at + 300 * MICROS_PER_MILLI, 102)],
    )
    .await;
    let submitted_at = slot_102_at - 2_000 * MICROS_PER_MILLI;
    let submission_id = common::send_events(
        &mut author,
        &[common::wp_submission_event(submitted_at, [0x50; 32])],
    )
    .await[0];
    common::send_events(
        &mut author,
        &[common::wp_received_event(
            submitted_at + 20 * MICROS_PER_MILLI,
            submission_id,
            3,
        )],
    )
    .await;
    common::flush_and_wait(&telemetry_server).await;

    let request = serde_json::json!({"type": "SubscribeMetrics", "interval_ms": 500});
    ws.send(Message::Text(request.to_string())).await.unwrap();
    let metrics = read_until(&mut ws, |json| json["type"] == "metrics").await;
    let data = &metrics["data"];
    assert_eq!(data["throughput"]["events_10s"], 5);
    assert_eq!(data["throughput"]["blocks_10s"], 3);
    assert_eq!(data["active_nodes"], 2);
    assert_eq!(data["active_work_packages"], 2);
    assert_eq!(data["failure_rate"], 0.0);
    assert_eq!(data["latest_slot"], 102);
    ws.close(None).await.ok();
}

/// A subscriber to one node receives none of the events other nodes send.
#[tokio::test]
async fn test_ws_nodes_filter_only_receives_subscribed_node() {
    let (server_url, telemetry_server, telemetry_port) = setup_ws_test().await;
    let mut ws = open_ws(&server_url).await;
    let mut watched = common::connect_test_node(telemetry_port, 1, &telemetry_server).await;
    let mut other = common::connect_test_node(telemetry_port, 2, &telemetry_server).await;
    let watched_id = common::node_id_hex(1);

    // Both nodes need a per-node channel, made on a node's first event, so a
    // filter that wrongly includes the other node would have it to subscribe to.
    let slot_1_at = common::now_jce_micros() - 2 * SLOT_MICROS;
    common::send_events(&mut watched, &[best_block(slot_1_at, 1)]).await;
    common::send_events(
        &mut other,
        &[best_block(slot_1_at + 200 * MICROS_PER_MILLI, 1)],
    )
    .await;
    subscribe(
        &mut ws,
        serde_json::json!({"type": "Nodes", "node_ids": [watched_id]}),
    )
    .await;

    let slot_2_at = slot_1_at + SLOT_MICROS;
    common::send_events(&mut other, &[best_block(slot_2_at, 2)]).await;
    common::send_events(
        &mut watched,
        &[best_block(slot_2_at + 250 * MICROS_PER_MILLI, 2)],
    )
    .await;

    let received = read_events_until(&mut ws, |envelope| {
        envelope["node_id"] == watched_id.as_str() && is_best_block(2)(envelope)
    })
    .await;
    assert_eq!(node_ids(&received), [watched_id.as_str()]);
    ws.close(None).await.ok();
}
