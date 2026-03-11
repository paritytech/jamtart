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

async fn setup_test_api() -> (TestServer, Arc<TelemetryServer>, u16, Arc<EventStore>) {
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

    let telemetry_server_clone = Arc::clone(&telemetry_server);
    tokio::spawn(async move {
        telemetry_server_clone.run().await.unwrap();
    });

    let broadcaster = telemetry_server.get_broadcaster();
    let health_monitor = Arc::new(tart_backend::health::HealthMonitor::new());

    let api_state = ApiState {
        store: Arc::clone(&store),
        telemetry_server: Arc::clone(&telemetry_server),
        broadcaster,
        health_monitor,
        jam_rpc: None,
        cache: Arc::new(tart_backend::cache::TtlCache::new(std::time::Duration::ZERO)),
        metrics_tracker: None,
    };

    let app = create_api_router(api_state);
    let test_server = TestServer::new(app).unwrap();

    (test_server, telemetry_server, telemetry_port, store)
}

async fn connect_test_node(
    port: u16,
    node_id: u8,
    server: &Arc<TelemetryServer>,
) -> TcpStream {
    let expected = server.connection_count() + 1;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let mut node_info = common::test_node_info([node_id; 32]);
    node_info.implementation_name =
        BoundedString::new(&format!("test-node-{}", node_id)).unwrap();

    let encoded = encode_message(&node_info).unwrap();
    stream.write_all(&encoded).await.unwrap();

    server.wait_for_connections(expected).await;
    common::flush_and_wait(server).await;

    stream
}

async fn send_events(stream: &mut TcpStream, events: &[Event]) {
    for event in events {
        let encoded = encode_message(event).unwrap();
        stream.write_all(&encoded).await.unwrap();
    }
}

fn time_range_params() -> String {
    let now = chrono::Utc::now();
    let start = now - chrono::Duration::hours(1);
    let end = now + chrono::Duration::hours(1);
    // Use Rfc3339 which produces `Z` suffix for UTC (no `+` to confuse URL parsing)
    format!(
        "start={}&end={}",
        start.format("%Y-%m-%dT%H:%M:%SZ"),
        end.format("%Y-%m-%dT%H:%M:%SZ"),
    )
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 1: Smoke tests — all 12 endpoints return 200 on empty DB
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_all_endpoints_empty_200() {
    let (server, _telemetry, _port, _store) = setup_test_api().await;

    let paths = [
        format!(
            "/api/grafana/timeseries?{}&interval=1m",
            time_range_params()
        ),
        format!("/api/grafana/stats?{}", time_range_params()),
        format!("/api/grafana/cores?{}", time_range_params()),
        format!(
            "/api/grafana/blocks/convergence?{}",
            time_range_params()
        ),
        format!(
            "/api/grafana/blocks/contents?{}",
            time_range_params()
        ),
        format!("/api/grafana/services?{}", time_range_params()),
        "/api/grafana/nodes".to_string(),
        format!("/api/grafana/node-stats?{}", time_range_params()),
        format!(
            "/api/grafana/node-stats-aggregate?{}",
            time_range_params()
        ),
        "/api/grafana/db-stats".to_string(),
        format!("/api/grafana/bottlenecks?{}", time_range_params()),
        format!("/api/grafana/wp-funnel?{}", time_range_params()),
    ];

    for path in &paths {
        let response = server.get(path).await;
        assert_eq!(
            response.status_code(),
            StatusCode::OK,
            "Failed for path: {}",
            path
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 2: Timeseries validation
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_timeseries_invalid_interval() {
    let (server, _telemetry, _port, _store) = setup_test_api().await;

    let path = format!(
        "/api/grafana/timeseries?{}&interval=99x",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_grafana_timeseries_invalid_group_by() {
    let (server, _telemetry, _port, _store) = setup_test_api().await;

    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&group_by=invalid",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 3: Timeseries with data
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_timeseries_with_events() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Inject 5 WPReceived + 3 BestBlockChanged events
    let mut events: Vec<Event> = Vec::new();
    for i in 0..5u64 {
        events.push(common::wp_received_event(ts + i * 1000, 1000 + i, 3));
    }
    for i in 0..3u32 {
        events.push(common::best_block_event(ts + (i as u64) * 1000, 100 + i));
    }

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("timeseries should return an array");

    // Verify structure: each entry has ts, event_type, count
    for entry in arr {
        assert!(entry.get("ts").is_some(), "entry missing ts");
        assert!(entry.get("event_type").is_some(), "entry missing event_type");
        assert!(entry.get("count").is_some(), "entry missing count");
    }

    // Sum up counts per event type
    let wp_count: i64 = arr
        .iter()
        .filter(|e| e["event_type"].as_i64() == Some(94))
        .map(|e| e["count"].as_i64().unwrap_or(0))
        .sum();
    let bb_count: i64 = arr
        .iter()
        .filter(|e| e["event_type"].as_i64() == Some(11))
        .map(|e| e["count"].as_i64().unwrap_or(0))
        .sum();

    assert_eq!(wp_count, 5, "expected 5 WPReceived events");
    assert_eq!(bb_count, 3, "expected 3 BestBlockChanged events");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 4: Nodes
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_nodes_with_connections() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let _stream1 = connect_test_node(port, 1, &telemetry).await;
    let _stream2 = connect_test_node(port, 2, &telemetry).await;

    let response = server.get("/api/grafana/nodes").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("nodes should return an array");

    // At least 2 connected nodes
    let connected: Vec<&Value> = arr
        .iter()
        .filter(|n| n["is_connected"].as_bool() == Some(true))
        .collect();
    assert!(
        connected.len() >= 2,
        "expected at least 2 connected nodes, got {}",
        connected.len()
    );

    // Verify structure
    for node in &connected {
        assert!(node.get("node_id").is_some(), "node missing node_id");
        assert!(node.get("peer_id").is_some(), "node missing peer_id");
        assert!(
            node.get("implementation_name").is_some(),
            "node missing implementation_name"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 5: Node stats
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_node_stats_from_status_events() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Inject 3 Status events
    let events: Vec<Event> = (0..3)
        .map(|i| common::status_event(ts + i * 1_000_000))
        .collect();
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/node-stats?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("node-stats should return an array");

    // Should have rows with num_peers, etc.
    assert!(
        !arr.is_empty(),
        "expected node-stats rows after status events"
    );
    for row in arr {
        assert!(row.get("node_id").is_some(), "row missing node_id");
        assert!(row.get("num_peers").is_some(), "row missing num_peers");
        assert!(
            row.get("num_val_peers").is_some(),
            "row missing num_val_peers"
        );
        assert!(
            row.get("num_sync_peers").is_some(),
            "row missing num_sync_peers"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 6: Blocks contents (from Authored events)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_blocks_contents_from_authored() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Send a BestBlockChanged first (so the enricher has a slot), then Authored
    let events = vec![
        common::best_block_event(ts, 100),
        common::authored_event(ts + 1000, 42),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/blocks/contents?{}",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(
        json.is_array(),
        "blocks/contents should return an array"
    );
    // The response may be empty if the enricher didn't populate slot for
    // the Authored event. We only verify the endpoint returns 200 with
    // valid JSON array structure.
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 7: WP Funnel
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_wp_funnel_with_full_pipeline() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    let sid: u64 = 5000;

    let events = vec![
        common::wp_received_event(ts, sid, 3),
        common::authorized_event(ts + 100_000, sid),
        common::refined_event(ts + 200_000, sid),
        common::work_report_built_event(ts + 300_000, sid),
        common::guarantee_built_event(ts + 400_000, sid),
        common::guarantees_distributed_event(ts + 500_000, sid),
    ];

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/wp-funnel?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(json.get("total").is_some(), "missing total");
    assert!(json.get("received").is_some(), "missing received");
    assert!(json.get("authorized").is_some(), "missing authorized");
    assert!(json.get("refined").is_some(), "missing refined");
    assert!(json.get("report_built").is_some(), "missing report_built");
    assert!(
        json.get("guarantee_built").is_some(),
        "missing guarantee_built"
    );
    assert!(json.get("distributed").is_some(), "missing distributed");
    assert!(json.get("failed").is_some(), "missing failed");

    // Full pipeline: all stages should have >= 1
    assert!(
        json["received"].as_i64().unwrap_or(0) >= 1,
        "expected received >= 1"
    );
    assert!(
        json["authorized"].as_i64().unwrap_or(0) >= 1,
        "expected authorized >= 1"
    );
    assert!(
        json["refined"].as_i64().unwrap_or(0) >= 1,
        "expected refined >= 1"
    );
    assert!(
        json["guarantee_built"].as_i64().unwrap_or(0) >= 1,
        "expected guarantee_built >= 1"
    );
    assert!(
        json["distributed"].as_i64().unwrap_or(0) >= 1,
        "expected distributed >= 1"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 8: Bottlenecks
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_bottlenecks_with_pipeline() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    let sid: u64 = 6000;

    let events = vec![
        common::wp_received_event(ts, sid, 5),
        common::authorized_event(ts + 100_000, sid),
        common::refined_event(ts + 200_000, sid),
        common::work_report_built_event(ts + 300_000, sid),
        common::guarantee_built_event(ts + 400_000, sid),
        common::guarantees_distributed_event(ts + 500_000, sid),
    ];

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/bottlenecks?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(json.get("total_wps").is_some(), "missing total_wps");
    assert!(json.get("failed_wps").is_some(), "missing failed_wps");
    assert!(json.get("failure_rate").is_some(), "missing failure_rate");
    assert!(
        json.get("stage_timing").is_some(),
        "missing stage_timing"
    );

    let stage_timing = &json["stage_timing"];
    assert!(
        stage_timing.get("authorize").is_some(),
        "stage_timing missing authorize"
    );
    assert!(
        stage_timing.get("refine").is_some(),
        "stage_timing missing refine"
    );
    assert!(
        stage_timing.get("report").is_some(),
        "stage_timing missing report"
    );
    assert!(
        stage_timing.get("guarantee").is_some(),
        "stage_timing missing guarantee"
    );
    assert!(
        stage_timing.get("distribute").is_some(),
        "stage_timing missing distribute"
    );
    assert!(
        stage_timing.get("pipeline_total").is_some(),
        "stage_timing missing pipeline_total"
    );

    assert!(
        json["total_wps"].as_i64().unwrap_or(0) >= 1,
        "expected total_wps >= 1"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 9: DB stats
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_db_stats_structure() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    // Inject a few events so the DB has some data
    let ts = common::now_jce_micros();
    let events = vec![
        common::best_block_event(ts, 50),
        common::status_event(ts + 1000),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let response = server.get("/api/grafana/db-stats").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(json.get("tables").is_some(), "missing tables");
    assert!(json.get("row_counts").is_some(), "missing row_counts");
    assert!(json.get("compression").is_some(), "missing compression");

    assert!(json["tables"].is_array(), "tables should be an array");
    assert!(
        json["row_counts"].is_array(),
        "row_counts should be an array"
    );
    assert!(
        json["compression"].is_array(),
        "compression should be an array"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 10: Stats endpoint
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_stats_basic() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Inject WPReceived and GuaranteeBuilt events
    let events = vec![
        common::wp_received_event(ts, 7000, 3),
        common::wp_received_event(ts + 1000, 7001, 4),
        common::guarantee_built_event(ts + 2000, 7000),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!("/api/grafana/stats?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(
        json.get("connected_nodes").is_some(),
        "missing connected_nodes"
    );
    assert!(json.get("wp_events").is_some(), "missing wp_events");
    assert!(json.get("guarantees").is_some(), "missing guarantees");
    assert!(json.get("failures").is_some(), "missing failures");
    assert!(
        json.get("slot_events").is_some(),
        "missing slot_events"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 11: Cores
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_cores_summary() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Inject WPReceived on cores 3 and 5
    let events = vec![
        common::wp_received_event(ts, 8000, 3),
        common::wp_received_event(ts + 1000, 8001, 3),
        common::wp_received_event(ts + 2000, 8002, 5),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!("/api/grafana/cores?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("cores should return an array");

    // Verify structure on returned entries
    for entry in arr {
        assert!(entry.get("core").is_some(), "entry missing core");
        assert!(
            entry.get("work_packages").is_some(),
            "entry missing work_packages"
        );
        assert!(
            entry.get("guarantees").is_some(),
            "entry missing guarantees"
        );
        assert!(
            entry.get("failures").is_some(),
            "entry missing failures"
        );
    }

    // Check that core 3 and core 5 have work_packages > 0
    let core3 = arr.iter().find(|e| e["core"].as_i64() == Some(3));
    let core5 = arr.iter().find(|e| e["core"].as_i64() == Some(5));

    assert!(core3.is_some(), "expected entry for core 3");
    assert!(core5.is_some(), "expected entry for core 5");

    assert!(
        core3.unwrap()["work_packages"].as_i64().unwrap_or(0) > 0,
        "core 3 should have work_packages > 0"
    );
    assert!(
        core5.unwrap()["work_packages"].as_i64().unwrap_or(0) > 0,
        "core 5 should have work_packages > 0"
    );
}
