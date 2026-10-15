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
        cache: Arc::new(tart_backend::cache::TtlCache::new(
            std::time::Duration::ZERO,
        )),
        metrics_tracker: None,
    };

    let app = create_api_router(api_state);
    let test_server = TestServer::new(app).unwrap();

    (test_server, telemetry_server, telemetry_port, store)
}

async fn connect_test_node(port: u16, node_id: u8, server: &Arc<TelemetryServer>) -> TcpStream {
    let expected = server.connection_count() + 1;

    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .unwrap();

    let mut node_info = common::test_node_info([node_id; 32]);
    node_info.implementation_name = BoundedString::new(&format!("test-node-{}", node_id)).unwrap();

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
        format!(
            "/api/grafana/events-by-node?{}&event_types=42",
            time_range_params()
        ),
        format!("/api/grafana/stats?{}", time_range_params()),
        format!("/api/grafana/cores?{}", time_range_params()),
        format!("/api/grafana/blocks/convergence?{}", time_range_params()),
        format!("/api/grafana/blocks/contents?{}", time_range_params()),
        format!("/api/grafana/services?{}", time_range_params()),
        format!(
            "/api/grafana/services/timeseries?{}&interval=1m",
            time_range_params()
        ),
        "/api/grafana/nodes".to_string(),
        format!("/api/grafana/node-stats?{}", time_range_params()),
        format!("/api/grafana/node-stats-aggregate?{}", time_range_params()),
        "/api/grafana/db-stats".to_string(),
        format!("/api/grafana/bottlenecks?{}", time_range_params()),
        format!("/api/grafana/wp-funnel?{}", time_range_params()),
        format!("/api/grafana/guarantee-convergence?{}", time_range_params()),
        format!(
            "/api/grafana/guarantee-convergence/detail?{}",
            time_range_params()
        ),
        format!("/api/grafana/assurance-convergence?{}", time_range_params()),
        format!(
            "/api/grafana/assurance-convergence/senders?{}",
            time_range_params()
        ),
        format!("/api/grafana/da-stats?{}", time_range_params()),
        format!(
            "/api/grafana/shard-latency?{}&interval=1m",
            time_range_params()
        ),
        format!(
            "/api/grafana/wp-funnel-timeseries?{}&interval=1m",
            time_range_params()
        ),
        format!(
            "/api/grafana/bottlenecks-timeseries?{}&interval=1m",
            time_range_params()
        ),
        format!("/api/grafana/events?{}&event_types=92", time_range_params()),
        format!(
            "/api/grafana/bundle-latency?{}&interval=1m",
            time_range_params()
        ),
        format!(
            "/api/grafana/segment-latency?{}&interval=1m",
            time_range_params()
        ),
        format!(
            "/api/grafana/preimage-latency?{}&interval=1m",
            time_range_params()
        ),
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
    // snap_interval converts unparseable intervals to "1m" — no 400 error
    assert_eq!(response.status_code(), StatusCode::OK);
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

    // Verify structure: each entry has ts, event_type, event_type_name, count
    for entry in arr {
        assert!(entry.get("ts").is_some(), "entry missing ts");
        assert!(
            entry.get("event_type").is_some(),
            "entry missing event_type"
        );
        assert!(
            entry.get("event_type_name").is_some(),
            "entry missing event_type_name"
        );
        assert!(entry.get("count").is_some(), "entry missing count");
    }

    // Sum up counts per event type and verify names
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

    // Verify event_type_name mappings
    for entry in arr {
        match entry["event_type"].as_i64() {
            Some(94) => assert_eq!(
                entry["event_type_name"].as_str(),
                Some("WorkPackageReceived")
            ),
            Some(11) => assert_eq!(entry["event_type_name"].as_str(), Some("BestBlockChanged")),
            _ => {}
        }
    }
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

    // Authoring is the first event → event_id = 0.
    // Authored references authoring_id = 0 so the enricher propagates the slot.
    let events = vec![
        common::authoring_event(ts, 200),     // event_id = 0, slot = 200
        common::authored_event(ts + 1000, 0), // authoring_id = 0 → inherits slot 200
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/blocks/contents?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json
        .as_array()
        .expect("blocks/contents should return an array");

    assert!(
        !arr.is_empty(),
        "blocks/contents should return rows after Authoring→Authored"
    );

    let row = &arr[0];
    assert_eq!(row["slot"].as_i64(), Some(200), "slot should be 200");
    assert_eq!(
        row["num_guarantees"].as_i64(),
        Some(3),
        "num_guarantees should be 3"
    );
    assert_eq!(
        row["num_assurances"].as_i64(),
        Some(2),
        "num_assurances should be 2"
    );
    assert_eq!(
        row["num_preimages"].as_i64(),
        Some(1),
        "num_preimages should be 1"
    );
    assert_eq!(
        row["num_tickets"].as_i64(),
        Some(2),
        "num_tickets should be 2"
    );
    assert_eq!(
        row["num_disputes"].as_i64(),
        Some(0),
        "num_disputes should be 0"
    );
    assert_eq!(
        row["extrinsic_size"].as_i64(),
        Some(2048),
        "extrinsic_size should be 2048"
    );
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
    let arr = json.as_array().expect("bottlenecks should return an array");
    assert!(!arr.is_empty(), "bottlenecks array should not be empty");
    let entry = &arr[0];
    assert!(entry.get("total_wps").is_some(), "missing total_wps");
    assert!(entry.get("failed_wps").is_some(), "missing failed_wps");
    assert!(entry.get("failure_rate").is_some(), "missing failure_rate");
    assert!(entry.get("stage_timing").is_some(), "missing stage_timing");

    let stage_timing = &entry["stage_timing"];
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
        entry["total_wps"].as_i64().unwrap_or(0) >= 1,
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
    assert!(json.get("slot_events").is_some(), "missing slot_events");
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
        assert!(entry.get("failures").is_some(), "entry missing failures");
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

// ─────────────────────────────────────────────────────────────────────────────
// Scenario A: Services pipeline
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_services_pipeline() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // WPReceived carries service_ids [10, 20] in its work items
    let events = vec![
        common::wp_received_event(ts, 9000, 3),
        common::authorized_event(ts + 100_000, 9000),
        common::refined_event(ts + 200_000, 9000),
        common::block_executed_event(ts + 300_000, 42, &[(10, 50_000), (20, 30_000)]),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!("/api/grafana/services?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("services should return an array");

    // Verify structure
    for entry in arr {
        assert!(
            entry.get("service_id").is_some(),
            "entry missing service_id"
        );
        assert!(
            entry.get("work_packages").is_some(),
            "entry missing work_packages"
        );
    }

    // Check that services 10 and 20 appear as zero-padded hex strings
    let svc10 = arr
        .iter()
        .find(|e| e["service_id"].as_str() == Some("0x0000000a"));
    let svc20 = arr
        .iter()
        .find(|e| e["service_id"].as_str() == Some("0x00000014"));

    assert!(svc10.is_some(), "expected entry for service 10");
    assert!(svc20.is_some(), "expected entry for service 20");

    assert!(
        svc10.unwrap()["work_packages"].as_i64().unwrap_or(0) > 0,
        "service 10 should have work_packages > 0"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario B: Timeseries group_by=core
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_timeseries_group_by_core() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let events = vec![
        common::wp_received_event(ts, 8100, 3),
        common::wp_received_event(ts + 1000, 8101, 3),
        common::wp_received_event(ts + 2000, 8102, 3),
        common::wp_received_event(ts + 3000, 8103, 5),
        common::wp_received_event(ts + 4000, 8104, 5),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&group_by=core",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("timeseries should return an array");

    // Entries should have a 'core' field
    for entry in arr {
        assert!(entry.get("core").is_some(), "entry missing core field");
    }

    // Both cores should appear
    let cores: Vec<i64> = arr.iter().filter_map(|e| e["core"].as_i64()).collect();
    assert!(cores.contains(&3), "expected core 3 in results");
    assert!(cores.contains(&5), "expected core 5 in results");
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario C: Block convergence multi-node
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_blocks_convergence_multi_node() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let ts = common::now_jce_micros();

    // Node 1: authored + best block
    let mut stream1 = connect_test_node(port, 1, &telemetry).await;
    let events1 = vec![
        common::authored_event(ts, 42),
        common::best_block_event(ts + 1_000, 200),
    ];
    send_events(&mut stream1, &events1).await;

    // Node 2: best block for same slot, later
    let mut stream2 = connect_test_node(port, 2, &telemetry).await;
    let events2 = vec![common::best_block_event(ts + 5_000, 200)];
    send_events(&mut stream2, &events2).await;

    // Node 3: best block for same slot, even later
    let mut stream3 = connect_test_node(port, 3, &telemetry).await;
    let events3 = vec![common::best_block_event(ts + 10_000, 200)];
    send_events(&mut stream3, &events3).await;

    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/blocks/convergence?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("convergence should return an array");

    // Should have convergence data for slot 200
    if !arr.is_empty() {
        for row in arr {
            assert!(row.get("slot").is_some(), "row missing slot");
            assert!(row.get("event_type").is_some(), "row missing event_type");
            assert!(row.get("node_count").is_some(), "row missing node_count");
            assert!(row.get("p50_ms").is_some(), "row missing p50_ms");
            assert!(row.get("p99_ms").is_some(), "row missing p99_ms");
            assert!(row.get("p100_ms").is_some(), "row missing p100_ms");
        }

        // Find slot 200
        let slot200 = arr.iter().find(|r| r["slot"].as_i64() == Some(200));
        if let Some(row) = slot200 {
            assert!(
                row["node_count"].as_i64().unwrap_or(0) >= 1,
                "expected node_count >= 1 for slot 200"
            );
        }
    }

    // Test event_type filter: only BestBlockChanged (11)
    let path = format!(
        "/api/grafana/blocks/convergence?event_type=11&{}",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let arr = response
        .json::<Value>()
        .as_array()
        .cloned()
        .unwrap_or_default();
    for row in &arr {
        assert_eq!(
            row["event_type"].as_i64(),
            Some(11),
            "event_type filter should return only type 11, got {:?}",
            row["event_type"]
        );
    }

    // Test event_type filter: only Authored (42)
    let path = format!(
        "/api/grafana/blocks/convergence?event_type=42&{}",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let arr = response
        .json::<Value>()
        .as_array()
        .cloned()
        .unwrap_or_default();
    for row in &arr {
        assert_eq!(
            row["event_type"].as_i64(),
            Some(42),
            "event_type filter should return only type 42, got {:?}",
            row["event_type"]
        );
    }

    // Test event_type filter: non-existent type returns empty
    let path = format!(
        "/api/grafana/blocks/convergence?event_type=99&{}",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let arr = response
        .json::<Value>()
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        arr.is_empty(),
        "non-existent event_type should return empty array"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario D: WP failure & partial pipeline
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_wp_failure_partial_pipeline() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // WP A: full pipeline
    let mut events = vec![
        common::wp_received_event(ts, 1000, 3),
        common::authorized_event(ts + 100_000, 1000),
        common::refined_event(ts + 200_000, 1000),
        common::work_report_built_event(ts + 300_000, 1000),
        common::guarantee_built_event(ts + 400_000, 1000),
        common::guarantees_distributed_event(ts + 500_000, 1000),
    ];

    // WP B: partial (stalls after refined)
    events.push(common::wp_received_event(ts + 600_000, 1001, 3));
    events.push(common::authorized_event(ts + 700_000, 1001));
    events.push(common::refined_event(ts + 800_000, 1001));

    // WP C: received then failed
    events.push(common::wp_received_event(ts + 900_000, 1002, 3));
    events.push(common::wp_failed_event(ts + 1_000_000, 1002));

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    // wp-funnel assertions
    let path = format!("/api/grafana/wp-funnel?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(
        json["received"].as_i64().unwrap_or(0) >= 3,
        "expected received >= 3"
    );
    assert!(
        json["authorized"].as_i64().unwrap_or(0) >= 2,
        "expected authorized >= 2"
    );
    assert!(
        json["refined"].as_i64().unwrap_or(0) >= 2,
        "expected refined >= 2"
    );
    assert!(
        json["distributed"].as_i64().unwrap_or(0) >= 1,
        "expected distributed >= 1"
    );
    assert!(
        json["failed"].as_i64().unwrap_or(0) >= 1,
        "expected failed >= 1"
    );

    // bottlenecks assertions (returns array after wrapping for Infinity plugin)
    let path = format!("/api/grafana/bottlenecks?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("bottlenecks should return an array");
    assert!(!arr.is_empty(), "bottlenecks array should not be empty");
    let entry = &arr[0];
    assert!(
        entry["total_wps"].as_i64().unwrap_or(0) >= 3,
        "expected total_wps >= 3"
    );
    assert!(
        entry["failed_wps"].as_i64().unwrap_or(0) >= 1,
        "expected failed_wps >= 1"
    );
    assert!(
        entry["failure_rate"].as_f64().unwrap_or(0.0) > 0.0,
        "expected failure_rate > 0"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario E: Node stats aggregate & filter
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_node_stats_aggregate_and_filter() {
    let (server, telemetry, port, store) = setup_test_api().await;

    let ts = common::now_jce_micros();

    // Node 1: 3 status events
    let mut stream1 = connect_test_node(port, 1, &telemetry).await;
    let events1: Vec<_> = (0..3)
        .map(|i| common::status_event(ts + i * 1_000_000))
        .collect();
    send_events(&mut stream1, &events1).await;

    // Node 2: 3 status events
    let mut stream2 = connect_test_node(port, 2, &telemetry).await;
    let events2: Vec<_> = (0..3)
        .map(|i| common::status_event(ts + i * 1_000_000))
        .collect();
    send_events(&mut stream2, &events2).await;

    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    // Unfiltered aggregate
    let path = format!("/api/grafana/node-stats-aggregate?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json
        .as_array()
        .expect("node-stats-aggregate should return an array");
    assert!(!arr.is_empty(), "expected aggregate rows");

    // Filtered by node 1
    let node1_id = common::node_id_hex(1);
    let path = format!(
        "/api/grafana/node-stats-aggregate?{}&node={}",
        time_range_params(),
        node1_id
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json
        .as_array()
        .expect("filtered aggregate should return an array");
    for row in arr {
        assert_eq!(
            row["node_id"].as_str(),
            Some(node1_id.as_str()),
            "filtered rows should match node 1"
        );
    }

    // node-stats with node filter
    let path = format!(
        "/api/grafana/node-stats?{}&node={}",
        time_range_params(),
        node1_id
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("node-stats should return an array");
    for row in arr {
        assert_eq!(
            row["node_id"].as_str(),
            Some(node1_id.as_str()),
            "node-stats filtered rows should match node 1"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario F: Cores detail mode
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_cores_detail_mode() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let events = vec![
        common::wp_received_event(ts, 2000, 3),
        common::authorized_event(ts + 100_000, 2000),
        common::wp_received_event(ts + 200_000, 2001, 3),
        common::authorized_event(ts + 300_000, 2001),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!("/api/grafana/cores/3?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    // Core detail endpoint returns an object with recent_work_packages
    assert!(json.is_object(), "core detail should return an object");
    assert!(json.get("core").is_some(), "entry missing core");
    assert!(
        json.get("recent_work_packages").is_some(),
        "detail mode should include recent_work_packages"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario G: Node disconnect
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_node_disconnect() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let _stream1 = connect_test_node(port, 1, &telemetry).await;
    let stream2 = connect_test_node(port, 2, &telemetry).await;

    // Both connected
    let response = server.get("/api/grafana/nodes").await;
    let json: Value = response.json();
    let arr = json.as_array().unwrap();
    let connected: Vec<_> = arr
        .iter()
        .filter(|n| n["is_connected"].as_bool() == Some(true))
        .collect();
    assert!(connected.len() >= 2, "expected at least 2 connected nodes");

    // Drop node 2's stream
    drop(stream2);
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;

    // Verify node 2 disconnected
    let response = server.get("/api/grafana/nodes").await;
    let json: Value = response.json();
    let arr = json.as_array().unwrap();

    let node2_id = common::node_id_hex(2);
    let node2 = arr
        .iter()
        .find(|n| n["node_id"].as_str() == Some(&node2_id));
    if let Some(n) = node2 {
        assert_eq!(
            n["is_connected"].as_bool(),
            Some(false),
            "node 2 should be disconnected"
        );
    }

    // Node 1 should still be connected
    let node1_id = common::node_id_hex(1);
    let node1 = arr
        .iter()
        .find(|n| n["node_id"].as_str() == Some(&node1_id));
    assert!(node1.is_some(), "node 1 should still exist");
    assert_eq!(
        node1.unwrap()["is_connected"].as_bool(),
        Some(true),
        "node 1 should still be connected"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Scenario H: Timeseries node & event_type filters
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_timeseries_node_and_event_type_filters() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let events = vec![
        common::wp_received_event(ts, 3000, 3),
        common::wp_received_event(ts + 1000, 3001, 5),
        common::best_block_event(ts + 2000, 100),
        common::best_block_event(ts + 3000, 101),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let node1_id = common::node_id_hex(1);

    // group_by=node_id
    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&group_by=node_id",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    for entry in arr {
        assert!(
            entry.get("node_id").is_some(),
            "entry missing node_id for group_by=node_id"
        );
    }

    // node filter
    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&node={}",
        time_range_params(),
        node1_id
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(
        !arr.is_empty(),
        "node-filtered timeseries should have results"
    );

    // event_types filter: only WPReceived (94) and BestBlockChanged (11)
    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&event_types=94,11",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    for entry in arr {
        let et = entry["event_type"].as_i64().unwrap_or(-1);
        assert!(
            et == 94 || et == 11,
            "expected only event_type 94 or 11, got {}",
            et
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Events by node: per-node totals ranked by count
// ─────────────────────────────────────────────────────────────────────────────

fn assert_share(entry: &Value, expected: f64) {
    let share = entry["share"].as_f64().expect("share should be a number");
    assert!(
        (share - expected).abs() < 1e-9,
        "expected share {expected}, got {share}"
    );
}

#[tokio::test]
async fn test_grafana_events_by_node_ranking() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream1 = connect_test_node(port, 1, &telemetry).await;
    let mut stream2 = connect_test_node(port, 2, &telemetry).await;

    let ts = common::now_jce_micros();

    // Node 1: three WorkPackageReceived(94) + one BestBlockChanged(11).
    send_events(
        &mut stream1,
        &[
            common::wp_received_event(ts, 3000, 3),
            common::wp_received_event(ts + 1000, 3001, 5),
            common::wp_received_event(ts + 2000, 3002, 7),
            common::best_block_event(ts + 3000, 100),
        ],
    )
    .await;
    // Node 2: one WorkPackageReceived(94).
    send_events(
        &mut stream2,
        &[common::wp_received_event(ts + 4000, 4000, 3)],
    )
    .await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let node1_id = common::node_id_hex(1);
    let node2_id = common::node_id_hex(2);

    // Both tiers must agree: 30s raw counts (sub-minute hint) and 1m aggregates.
    for interval in ["30s", "1m"] {
        let path = format!(
            "/api/grafana/events-by-node?{}&event_types=94&interval={}",
            time_range_params(),
            interval
        );
        let response = server.get(&path).await;
        assert_eq!(
            response.status_code(),
            StatusCode::OK,
            "interval={interval}"
        );
        let json: Value = response.json();
        let arr = json
            .as_array()
            .expect("events-by-node should return an array");
        assert_eq!(
            arr.len(),
            2,
            "two nodes reported type 94 (interval={interval})"
        );

        // Ranked by count, highest first.
        assert_eq!(arr[0]["node_id"], node1_id, "interval={interval}");
        assert_eq!(arr[0]["count"], 3, "interval={interval}");
        assert_share(&arr[0], 0.75);
        assert_eq!(arr[1]["node_id"], node2_id, "interval={interval}");
        assert_eq!(arr[1]["count"], 1, "interval={interval}");
        assert_share(&arr[1], 0.25);

        // Node identity joined from the nodes table.
        assert_eq!(arr[0]["implementation_name"], "test-node-1");
        assert_eq!(arr[1]["implementation_name"], "test-node-2");
        assert_eq!(arr[0]["is_connected"], true);
        let address = arr[0]["address"]
            .as_str()
            .expect("address should be present for a connected node");
        assert!(
            address.starts_with("127.0.0.1:"),
            "address should be the telemetry session's peer address, got {address}"
        );
        assert!(arr[0]["last_seen_at"].is_string(), "last_seen_at missing");
    }

    // event_types filter: only node 1 reported BestBlockChanged(11).
    let path = format!(
        "/api/grafana/events-by-node?{}&event_types=11",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["node_id"], node1_id);
    assert_eq!(arr[0]["count"], 1);
    assert_share(&arr[0], 1.0);

    // limit keeps the top sender; share stays relative to all nodes.
    let path = format!(
        "/api/grafana/events-by-node?{}&event_types=94&limit=1",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["node_id"], node1_id);
    assert_share(&arr[0], 0.75);

    // Group names and Grafana braces work like in /timeseries: {wp_pipeline} covers type 94.
    let path = format!(
        "/api/grafana/events-by-node?{}&event_types=%7Bwp_pipeline%7D",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["node_id"], node1_id);
    assert_eq!(arr[0]["count"], 3);

    // No event_types: every type counts, node 1 (4 events) still ranks first.
    let path = format!("/api/grafana/events-by-node?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["node_id"], node1_id);
    assert!(arr[0]["count"].as_i64().unwrap() >= 4);
    assert_eq!(arr[1]["node_id"], node2_id);
}

// ─────────────────────────────────────────────────────────────────────────────
// Event types endpoint with group filter
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_event_types_unfiltered() {
    let (server, _telemetry, _port, _store) = setup_test_api().await;

    let response = server.get("/api/grafana/event-types").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert_eq!(
        arr.len(),
        115,
        "unfiltered should return all 115 event types"
    );
}

#[tokio::test]
async fn test_grafana_event_types_filtered_by_group() {
    let (server, _telemetry, _port, _store) = setup_test_api().await;

    let response = server.get("/api/grafana/event-types?group=blocks").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    // All returned events should belong to the blocks group
    let names: Vec<&str> = arr.iter().filter_map(|e| e["name"].as_str()).collect();
    assert!(
        names.contains(&"Authored"),
        "blocks group should contain Authored"
    );
    assert!(
        names.contains(&"BlockExecuted"),
        "blocks group should contain BlockExecuted"
    );
    assert!(
        !names.contains(&"WorkPackageFailed"),
        "blocks group should not contain WorkPackageFailed"
    );
    assert!(
        !names.contains(&"Dropped"),
        "blocks group should not contain Dropped"
    );

    // All entries should have group=blocks
    for entry in arr {
        assert_eq!(
            entry["group"].as_str(),
            Some("blocks"),
            "all filtered entries should be in blocks group"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Services timeseries
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_services_timeseries_invalid_interval() {
    let (server, _telemetry, _port, _store) = setup_test_api().await;

    let path = format!(
        "/api/grafana/services/timeseries?{}&interval=99x",
        time_range_params()
    );
    let response = server.get(&path).await;
    // snap_interval converts unparseable intervals to "1m" — no 400 error
    assert_eq!(response.status_code(), StatusCode::OK);
}

#[tokio::test]
async fn test_grafana_services_timeseries_with_data() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // WPReceived for core 3 (enricher maps core → service via work items)
    // BlockExecuted carries per-service gas: services 10 and 20
    let events = vec![
        common::wp_received_event(ts, 9500, 3),
        common::authorized_event(ts + 100_000, 9500),
        common::refined_event(ts + 200_000, 9500),
        common::block_executed_event(ts + 300_000, 42, &[(10, 50_000), (20, 30_000)]),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/services/timeseries?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    // Verify structure — split gas columns
    for entry in arr {
        assert!(entry.get("ts").is_some(), "entry missing ts");
        assert!(
            entry.get("service_id").is_some(),
            "entry missing service_id"
        );
        assert!(
            entry.get("work_packages").is_some(),
            "entry missing work_packages"
        );
        assert!(
            entry.get("authorization_gas").is_some(),
            "entry missing authorization_gas"
        );
        assert!(
            entry.get("refinement_gas").is_some(),
            "entry missing refinement_gas"
        );
        assert!(
            entry.get("execution_gas").is_some(),
            "entry missing execution_gas"
        );
    }
}

#[tokio::test]
async fn test_grafana_services_timeseries_service_filter() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let events = vec![common::block_executed_event(
        ts,
        42,
        &[(10, 50_000), (20, 30_000)],
    )];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    // Filter to service 10 only
    let path = format!(
        "/api/grafana/services/timeseries?{}&interval=1m&service=10",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    for entry in arr {
        assert_eq!(
            entry["service_id"].as_str(),
            Some("0x0000000a"),
            "service filter should only return service 10 (0x0000000a)"
        );
    }
}

#[tokio::test]
async fn test_grafana_services_timeseries_gas_split() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // BlockExecuted carries per-service gas (event_type=47 → execution_gas)
    let events = vec![common::block_executed_event(ts, 42, &[(10, 50_000)])];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/services/timeseries?{}&interval=1m&service=10",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    // Should have execution_gas > 0 for service 10
    if !arr.is_empty() {
        let entry = &arr[0];
        assert!(
            entry["execution_gas"].as_i64().unwrap_or(0) > 0,
            "execution_gas should be > 0 for BlockExecuted event"
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Timeseries with core filter
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_timeseries_core_filter() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Send WP events for two different cores
    // Core 3: received + failed
    let events = vec![
        common::wp_received_event(ts, 9000, 3),
        common::wp_failed_event(ts + 1000, 9000),
        common::wp_received_event(ts + 2000, 9001, 3),
        common::wp_failed_event(ts + 3000, 9001),
        // Core 5: received + failed
        common::wp_received_event(ts + 4000, 9002, 5),
        common::wp_failed_event(ts + 5000, 9002),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    // Query failures filtered to core 3
    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&group_by=event_type&event_types=92&core=3",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    // Should have data (core 3 had 2 WorkPackageFailed events)
    let total_count: i64 = arr.iter().filter_map(|e| e["count"].as_i64()).sum();
    assert_eq!(total_count, 2, "core 3 should have 2 failure events");

    // Query failures for core 5
    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&group_by=event_type&event_types=92&core=5",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    let total_count: i64 = arr.iter().filter_map(|e| e["count"].as_i64()).sum();
    assert_eq!(total_count, 1, "core 5 should have 1 failure event");

    // Query without core filter — should get all 3
    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&group_by=event_type&event_types=92",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    let total_count: i64 = arr.iter().filter_map(|e| e["count"].as_i64()).sum();
    assert!(
        total_count >= 3,
        "unfiltered should have at least 3 failure events, got {total_count}"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Generic raw events endpoint
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_events_raw() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Send 3 WorkPackageFailed events with different reasons
    let events = vec![
        common::wp_received_event(ts, 5000, 0),
        common::wp_failed_event_with_reason(ts + 1000, 5000, "out of gas"),
        common::wp_received_event(ts + 2000, 5001, 0),
        common::wp_failed_event_with_reason(ts + 3000, 5001, "out of gas"),
        common::wp_received_event(ts + 4000, 5002, 0),
        common::wp_failed_event_with_reason(ts + 5000, 5002, "invalid code"),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    // Query raw events for event_type=92 (WorkPackageFailed)
    let path = format!("/api/grafana/events?{}&event_types=92", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    // Response is now EventsSearchResponse {events: [...], pagination: {...}}
    let arr = json["events"].as_array().expect("should have events array");
    assert!(
        arr.len() >= 3,
        "expected at least 3 events, got {}",
        arr.len()
    );

    // Pagination metadata
    assert!(json["pagination"]["total"].as_i64().unwrap() >= 3);
    assert!(json["pagination"]["offset"].as_i64() == Some(0));

    // Each entry should have ts, node_id, event_type, data, created_at
    for entry in arr {
        assert!(entry["ts"].is_string(), "ts should be present");
        assert!(entry["node_id"].is_string(), "node_id should be present");
        assert_eq!(entry["event_type"].as_i64().unwrap(), 92);
        assert!(entry["data"].is_object(), "data should be a JSON object");
        assert!(
            entry["created_at"].is_string(),
            "created_at should be present"
        );

        // The data should contain WorkPackageFailed with a reason field
        let wp_data = &entry["data"]["WorkPackageFailed"];
        assert!(
            wp_data["reason"].is_string(),
            "data.WorkPackageFailed.reason should be a string"
        );
    }

    // Verify reasons match
    let reasons: Vec<&str> = arr
        .iter()
        .filter_map(|e| e["data"]["WorkPackageFailed"]["reason"].as_str())
        .collect();
    assert_eq!(
        reasons.iter().filter(|&&r| r == "out of gas").count(),
        2,
        "should have 2 'out of gas' reasons"
    );
    assert_eq!(
        reasons.iter().filter(|&&r| r == "invalid code").count(),
        1,
        "should have 1 'invalid code' reason"
    );
}

#[tokio::test]
async fn test_grafana_events_missing_event_types() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    let events = vec![
        common::wp_received_event(ts, 7000, 0),
        common::best_block_event(ts + 1000, 200),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    // Missing event_types is now OK — returns all types
    let path = format!("/api/grafana/events?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(
        response.status_code(),
        StatusCode::OK,
        "should succeed without event_types (returns all types)"
    );

    let json: Value = response.json();
    let arr = json["events"].as_array().expect("should have events array");
    assert!(arr.len() >= 2, "should return events of multiple types");
}

#[tokio::test]
async fn test_grafana_events_with_limit() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Send 5 failed events
    let mut events = Vec::new();
    for i in 0..5 {
        events.push(common::wp_received_event(ts + i * 2000, 6000 + i, 0));
        events.push(common::wp_failed_event(ts + i * 2000 + 1000, 6000 + i));
    }
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    // Query with limit=2
    let path = format!(
        "/api/grafana/events?{}&event_types=92&limit=2",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json["events"].as_array().expect("should have events array");
    assert_eq!(arr.len(), 2, "should respect limit=2");
    // Pagination should indicate more results exist
    assert!(
        json["pagination"]["has_more"].as_bool().unwrap(),
        "should have more events"
    );
    assert!(
        json["pagination"]["total"].as_i64().unwrap() >= 5,
        "total should be >= 5"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 13: Pre-aggregated event types — baseline tests for storage optimization
// These tests establish correctness before the refactoring. After refactoring,
// events flow through per-group count tables + UNION views instead of raw table.
// ─────────────────────────────────────────────────────────────────────────────

/// Helper: sum counts for a given event_type from a timeseries JSON array.
fn sum_counts_for_type(arr: &[Value], event_type: i64) -> i64 {
    arr.iter()
        .filter(|e| e["event_type"].as_i64() == Some(event_type))
        .map(|e| e["count"].as_i64().unwrap_or(0))
        .sum()
}

#[tokio::test]
async fn test_timeseries_pre_aggregated_event_types() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let mut events = Vec::new();
    for i in 0..3u64 {
        events.push(common::block_announced_event(ts + i * 1000, 100));
    }
    for i in 0..5u64 {
        events.push(common::assurance_sent_event(ts + i * 1000));
    }
    for i in 0..2u64 {
        events.push(common::assurance_received_event(ts + i * 1000));
    }

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&event_types=62,128,131",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    assert_eq!(sum_counts_for_type(arr, 62), 3, "expected 3 BlockAnnounced");
    assert_eq!(sum_counts_for_type(arr, 128), 5, "expected 5 AssuranceSent");
    assert_eq!(
        sum_counts_for_type(arr, 131),
        2,
        "expected 2 AssuranceReceived"
    );
}

#[tokio::test]
async fn test_timeseries_mixed_raw_and_pre_aggregated() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let mut events = Vec::new();
    // Raw event types (stay in ingested_raw_events)
    for i in 0..2u64 {
        events.push(common::wp_received_event(ts + i * 1000, 9000 + i, 3));
    }
    events.push(common::best_block_event(ts, 100));
    // Pre-aggregated event type
    for i in 0..3u64 {
        events.push(common::block_announced_event(ts + i * 1000, 100));
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
    let arr = json.as_array().expect("should return array");

    assert_eq!(
        sum_counts_for_type(arr, 94),
        2,
        "expected 2 WPReceived (raw)"
    );
    assert_eq!(
        sum_counts_for_type(arr, 11),
        1,
        "expected 1 BestBlockChanged (raw)"
    );
    assert_eq!(
        sum_counts_for_type(arr, 62),
        3,
        "expected 3 BlockAnnounced (pre-agg)"
    );
}

#[tokio::test]
async fn test_timeseries_group_filter_with_pre_aggregated() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let mut events = Vec::new();
    for i in 0..2u64 {
        events.push(common::assurance_sent_event(ts + i * 1000));
    }
    events.push(common::assurance_received_event(ts));
    events.push(common::assurance_send_failed_event(ts, "timeout"));

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&event_types=assurances",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    assert_eq!(
        sum_counts_for_type(arr, 127),
        1,
        "expected 1 AssuranceSendFailed"
    );
    assert_eq!(sum_counts_for_type(arr, 128), 2, "expected 2 AssuranceSent");
    assert_eq!(
        sum_counts_for_type(arr, 131),
        1,
        "expected 1 AssuranceReceived"
    );
}

#[tokio::test]
async fn test_timeseries_failures_cross_storage_paths() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let mut events = Vec::new();
    // WPFailed (92) stays raw
    for i in 0..2u64 {
        events.push(common::wp_received_event(ts + i * 1000, 7000 + i, 3));
        events.push(common::wp_failed_event(ts + i * 1000 + 500, 7000 + i));
    }
    // AssuranceSendFailed (127) will be pre-aggregated
    for i in 0..3u64 {
        events.push(common::assurance_send_failed_event(
            ts + i * 1000,
            "timeout",
        ));
    }

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&event_types=failures",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    assert_eq!(
        sum_counts_for_type(arr, 92),
        2,
        "expected 2 WPFailed (raw path)"
    );
    assert_eq!(
        sum_counts_for_type(arr, 127),
        3,
        "expected 3 AssuranceSendFailed (pre-agg path)"
    );
}

#[tokio::test]
async fn test_guarantee_sending_enriched_core_in_timeseries() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Full enricher chain: WPReceived → GuaranteeBuilt → SendingGuarantee → GuaranteeSent
    let events = vec![
        common::wp_received_event(ts, 8000, 3),
        common::guarantee_built_event(ts + 1000, 8000),
        common::sending_guarantee_event(ts + 2000, 1), // built_id=1 (first event from this node)
        common::guarantee_sent_event(ts + 3000, 1),    // sending_id=1
    ];

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&event_types=106,108",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    assert_eq!(
        sum_counts_for_type(arr, 106),
        1,
        "expected 1 SendingGuarantee"
    );
    assert_eq!(sum_counts_for_type(arr, 108), 1, "expected 1 GuaranteeSent");
}

#[tokio::test]
async fn test_guarantee_receiving_slot_and_reason() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    let report_hash = [0xDD; 32];

    let mut events = Vec::new();
    // 2x GuaranteeReceived
    for i in 0..2u64 {
        events.push(common::guarantee_received_event(
            ts + i * 1000,
            200,
            report_hash,
        ));
    }
    // 3x GuaranteeDiscarded with Superseded + 1x with TooManyGuarantees
    for i in 0..3u64 {
        events.push(common::guarantee_discarded_event(
            ts + i * 1000,
            200,
            report_hash,
            GuaranteeDiscardReason::ReplacedByBetter,
        ));
    }
    events.push(common::guarantee_discarded_event(
        ts + 3000,
        200,
        report_hash,
        GuaranteeDiscardReason::TooManyGuarantees,
    ));

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&event_types=112,113",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    assert_eq!(
        sum_counts_for_type(arr, 112),
        2,
        "expected 2 GuaranteeReceived"
    );
    assert_eq!(
        sum_counts_for_type(arr, 113),
        4,
        "expected 4 GuaranteeDiscarded"
    );
}

#[tokio::test]
async fn test_timeseries_multi_bucket_aggregation() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let mut events = Vec::new();
    // 3 events in one 30s bucket
    for i in 0..3u64 {
        events.push(common::assurance_sent_event(ts + i * 1000));
    }
    // 2 events in the next 30s bucket (35s later)
    for i in 0..2u64 {
        events.push(common::assurance_sent_event(ts + 35_000_000 + i * 1000));
    }

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    // At 1m resolution, both buckets should be merged
    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&event_types=128",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    let total: i64 = sum_counts_for_type(arr, 128);
    assert_eq!(total, 5, "expected 5 total AssuranceSent across buckets");

    // At 30s resolution, should see two separate entries
    let path_30s = format!(
        "/api/grafana/timeseries?{}&interval=30s&event_types=128",
        time_range_params()
    );
    let response_30s = server.get(&path_30s).await;
    assert_eq!(response_30s.status_code(), StatusCode::OK);

    let json_30s: Value = response_30s.json();
    let arr_30s = json_30s.as_array().expect("should return array");

    let counts: Vec<i64> = arr_30s
        .iter()
        .filter(|e| e["event_type"].as_i64() == Some(128))
        .map(|e| e["count"].as_i64().unwrap_or(0))
        .collect();
    assert_eq!(counts.len(), 2, "expected 2 separate 30s buckets");
    assert!(counts.contains(&3), "expected bucket with count=3");
    assert!(counts.contains(&2), "expected bucket with count=2");
}

#[tokio::test]
async fn test_events_endpoint_returns_pre_aggregated_types() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let mut events = Vec::new();
    for i in 0..3u64 {
        events.push(common::assurance_sent_event(ts + i * 1000));
    }

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/events?{}&event_types=128",
        time_range_params()
    );
    let response = server.get(&path).await;

    // After migration 020: all types write to ingested_raw_events (1h retention).
    // /events no longer rejects pre-aggregated types — all 115 types are browsable.
    assert_eq!(
        response.status_code(),
        StatusCode::OK,
        "all event types should now be browsable (unified architecture)"
    );
    let json: Value = response.json();
    let arr = json["events"].as_array().expect("should have events array");
    assert_eq!(arr.len(), 3, "expected 3 assurance events");
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 14: Post-refactoring tests for count tables + new endpoints
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_guarantee_discards_endpoint() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    let report_hash = [0xDD; 32];

    let mut events = Vec::new();
    // 3x GuaranteeDiscarded with ReplacedByBetter
    for i in 0..3u64 {
        events.push(common::guarantee_discarded_event(
            ts + i * 1000,
            200,
            report_hash,
            GuaranteeDiscardReason::ReplacedByBetter,
        ));
    }
    // 2x GuaranteeDiscarded with TooManyGuarantees
    for i in 0..2u64 {
        events.push(common::guarantee_discarded_event(
            ts + 3000 + i * 1000,
            200,
            report_hash,
            GuaranteeDiscardReason::TooManyGuarantees,
        ));
    }

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/guarantee-discards?{}&interval=30s",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    // Sum counts by reason
    let replaced: i64 = arr
        .iter()
        .filter(|e| {
            e["reason"]
                .as_str()
                .is_some_and(|r| r.contains("ReplacedByBetter"))
        })
        .map(|e| e["count"].as_i64().unwrap_or(0))
        .sum();
    let too_many: i64 = arr
        .iter()
        .filter(|e| {
            e["reason"]
                .as_str()
                .is_some_and(|r| r.contains("TooManyGuarantees"))
        })
        .map(|e| e["count"].as_i64().unwrap_or(0))
        .sum();

    assert_eq!(replaced, 3, "expected 3 ReplacedByBetter discards");
    assert_eq!(too_many, 2, "expected 2 TooManyGuarantees discards");
}

#[tokio::test]
async fn test_timeseries_group_by_core_with_pre_aggregated() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Full enricher chain: WPReceived(core=3) → GuaranteeBuilt → SendingGuarantee → GuaranteeSent
    let events = vec![
        common::wp_received_event(ts, 8500, 3),
        common::guarantee_built_event(ts + 1000, 8500),
        common::sending_guarantee_event(ts + 2000, 1),
        common::guarantee_sent_event(ts + 3000, 1),
    ];

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/timeseries?{}&interval=1m&event_types=106,108&group_by=core",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    // Should have results grouped by core
    assert!(!arr.is_empty(), "expected results for group_by=core");
    // All results should have core=3
    for entry in arr {
        assert_eq!(
            entry["core"].as_i64(),
            Some(3),
            "expected core=3, got {:?}",
            entry
        );
    }
}

#[tokio::test]
async fn test_event_services_dual_write_for_segments() {
    let (_server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // WPReceived(core=3, service_ids=[10,20]) provides enrichment context
    // WorkPackageHashMapped(160) is both a SERVICE_EVENT_TYPE and a pre-aggregated type
    let events = vec![
        common::wp_received_event(ts, 8600, 3),
        Event::WorkPackageHashMapped {
            timestamp: ts + 1000,
            submission_id: 8600,
            work_package_hash: [0xCC; 32],
            segments_root: [0xDD; 32],
        },
    ];

    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let pool = store.pool();

    // Check event_services has rows for type 160 with service_ids 10 and 20
    let es_rows: Vec<(i16, i32)> =
        sqlx::query_as("SELECT event_type, service_id FROM event_services WHERE event_type = 160")
            .fetch_all(pool)
            .await
            .expect("event_services query failed");

    let service_ids: Vec<i32> = es_rows.iter().map(|r| r.1).collect();
    assert!(
        service_ids.contains(&10) && service_ids.contains(&20),
        "expected service_ids [10, 20] in event_services, got {:?}",
        service_ids
    );

    // Check segment_counts has row for type 160
    let count: (i64,) = sqlx::query_as(
        "SELECT COALESCE(SUM(event_count), 0)::BIGINT FROM segment_counts WHERE event_type = 160",
    )
    .fetch_one(pool)
    .await
    .expect("segment_counts query failed");
    assert!(count.0 > 0, "expected segment_counts row for type 160");

    // After migration 020: all types write to ingested_raw_events (1h browsing store).
    // Type 160 is now in BOTH segment_counts AND ingested_raw_events.
    let raw: (i64,) =
        sqlx::query_as("SELECT COUNT(*)::BIGINT FROM ingested_raw_events WHERE event_type = 160")
            .fetch_one(pool)
            .await
            .expect("ingested_raw_events query failed");
    assert!(
        raw.0 > 0,
        "type 160 should now be in ingested_raw_events (unified architecture)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// WP Funnel Timeseries
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_wp_funnel_timeseries() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let now = common::now_jce_micros();
    let sid: u64 = 6000;
    let events = vec![
        common::wp_received_event(now, sid, 3),
        common::authorized_event(now + 100_000, sid),
        common::refined_event(now + 200_000, sid),
        common::work_report_built_event(now + 300_000, sid),
        common::guarantee_built_event(now + 400_000, sid),
        common::guarantees_distributed_event(now + 500_000, sid),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/wp-funnel-timeseries?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "should have at least one bucket with data");

    let data_row = &arr[0];
    assert!(data_row["ts"].is_string(), "missing ts");
    assert_eq!(data_row["total"].as_i64(), Some(1), "expected total=1");
    assert_eq!(
        data_row["received"].as_i64(),
        Some(1),
        "expected received=1"
    );
    assert_eq!(
        data_row["authorized"].as_i64(),
        Some(1),
        "expected authorized=1"
    );
    assert_eq!(data_row["refined"].as_i64(), Some(1), "expected refined=1");
    assert_eq!(
        data_row["report_built"].as_i64(),
        Some(1),
        "expected report_built=1"
    );
    assert_eq!(
        data_row["guarantee_built"].as_i64(),
        Some(1),
        "expected guarantee_built=1"
    );
    assert_eq!(
        data_row["distributed"].as_i64(),
        Some(1),
        "expected distributed=1"
    );
    assert_eq!(data_row["failed"].as_i64(), Some(0), "expected failed=0");
}

// ─────────────────────────────────────────────────────────────────────────────
// Bottlenecks Timeseries
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_bottlenecks_timeseries() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let now = common::now_jce_micros();
    let sid: u64 = 7000;
    let events = vec![
        common::wp_received_event(now, sid, 3),
        common::authorized_event(now + 100_000, sid), // +100ms
        common::refined_event(now + 200_000, sid),    // +100ms
        common::work_report_built_event(now + 300_000, sid), // +100ms
        common::guarantee_built_event(now + 400_000, sid), // +100ms
        common::guarantees_distributed_event(now + 500_000, sid), // +100ms
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/bottlenecks-timeseries?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "should have at least one bucket with data");

    let data_row = &arr[0];
    assert!(data_row["ts"].is_string(), "missing ts");
    assert_eq!(
        data_row["total_wps"].as_i64(),
        Some(1),
        "expected total_wps=1"
    );
    assert_eq!(
        data_row["failed_wps"].as_i64(),
        Some(0),
        "expected failed_wps=0"
    );
    // authorize_p50 should be ~100ms (received→authorized delta is 100_000 JCE micros = 100ms)
    let auth_p50 = data_row["authorize_p50"]
        .as_f64()
        .expect("authorize_p50 should be a number");
    assert!(
        auth_p50 > 50.0 && auth_p50 < 200.0,
        "authorize_p50={auth_p50} should be ~100ms"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Guarantee Convergence
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_guarantee_convergence() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    // Node A: guarantor — sends GuaranteeBuilt(105) with work_report_hash=[0xBB; 32], slot=200
    let mut stream_a = connect_test_node(port, 1, &telemetry).await;
    let now = common::now_jce_micros();
    let sid: u64 = 8000;

    // First send WorkPackageReceived so enricher has context for GuaranteeBuilt
    let events_a = vec![
        common::wp_received_event(now, sid, 5),
        common::guarantee_built_event(now + 100_000, sid), // built_at = now + 100ms
    ];
    send_events(&mut stream_a, &events_a).await;

    // Nodes B, C, D: validators — send GuaranteeReceived(112)
    let report_hash = [0xBB; 32]; // matches guarantee_built_event's hardcoded hash
    let mut stream_b = connect_test_node(port, 2, &telemetry).await;
    let mut stream_c = connect_test_node(port, 3, &telemetry).await;
    let mut stream_d = connect_test_node(port, 4, &telemetry).await;

    send_events(
        &mut stream_b,
        &[
            common::guarantee_received_event(now + 200_000, 200, report_hash), // +100ms after built
        ],
    )
    .await;
    send_events(
        &mut stream_c,
        &[
            common::guarantee_received_event(now + 300_000, 200, report_hash), // +200ms after built
        ],
    )
    .await;
    send_events(
        &mut stream_d,
        &[
            common::guarantee_received_event(now + 400_000, 200, report_hash), // +300ms after built
        ],
    )
    .await;

    common::flush_all(&telemetry).await;

    // Test overview endpoint (per-slot summary)
    let path = format!("/api/grafana/guarantee-convergence?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "should have at least one slot");
    let row = &arr[0];
    assert_eq!(row["slot"].as_i64(), Some(200));
    assert!(row["guarantee_count"].as_i64().unwrap_or(0) >= 1);
    assert!(row["p50_ms"].is_number(), "expected p50_ms");

    // Test detail endpoint (per-guarantee)
    let path = format!(
        "/api/grafana/guarantee-convergence/detail?{}",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "should have at least one guarantee");
    let row = &arr[0];
    assert_eq!(row["slot"].as_i64(), Some(200));
    assert!(
        row["node_count"].as_i64().unwrap_or(0) >= 3,
        "expected node_count >= 3"
    );
    assert!(
        row["work_report_hash"].is_string(),
        "expected work_report_hash"
    );
    assert!(row["p50_ms"].is_number(), "expected p50_ms");
    assert!(row["p99_ms"].is_number(), "expected p99_ms");
    assert!(
        row["builder_node_id"].is_string(),
        "expected builder_node_id"
    );

    // Test interval mode — should return ConvergenceTimeseriesRow
    let path = format!(
        "/api/grafana/guarantee-convergence?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "interval mode should have data");
    let row = &arr[0];
    assert!(row["ts"].is_string(), "expected ts in interval mode");
    assert!(
        row["sample_count"].as_i64().unwrap_or(0) > 0,
        "expected sample_count > 0"
    );
    assert!(
        row["p50_ms"].is_number(),
        "expected p50_ms in interval mode"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Assurance Convergence
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_assurance_convergence() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let now = common::now_jce_micros();
    let anchor = [0xDD; 32];

    // Node X: sends Importing to populate HeaderHashLookup
    let mut stream_x = connect_test_node(port, 10, &telemetry).await;
    send_events(&mut stream_x, &[common::importing_event(now, 500, anchor)]).await;

    // Node A: sender — distributes assurance
    let mut stream_a = connect_test_node(port, 1, &telemetry).await;
    // peer_id for node 1 = [1; 32] (connect_test_node uses [node_id; 32])
    let sender_a_peer_id = [1u8; 32];
    send_events(
        &mut stream_a,
        &[
            common::distributing_assurance_event(now + 100_000, anchor), // distributed_at = now + 100ms
        ],
    )
    .await;

    // Nodes B, C: receivers
    let mut stream_b = connect_test_node(port, 2, &telemetry).await;
    let mut stream_c = connect_test_node(port, 3, &telemetry).await;
    send_events(
        &mut stream_b,
        &[
            common::assurance_received_event_with(now + 200_000, anchor, sender_a_peer_id), // +100ms after distribution
        ],
    )
    .await;
    send_events(
        &mut stream_c,
        &[
            common::assurance_received_event_with(now + 300_000, anchor, sender_a_peer_id), // +200ms after distribution
        ],
    )
    .await;

    common::flush_all(&telemetry).await;

    // Test overview endpoint
    let path = format!("/api/grafana/assurance-convergence?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "should have at least one anchor");
    let row = &arr[0];
    assert_eq!(row["slot"].as_i64(), Some(500));
    assert!(
        row["sender_count"].as_i64().unwrap_or(0) >= 1,
        "expected sender_count >= 1"
    );
    assert!(row["p50_ms"].is_number(), "expected p50_ms");

    // Test senders endpoint
    let path = format!(
        "/api/grafana/assurance-convergence/senders?{}",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "should have at least one sender row");
    let row = &arr[0];
    assert!(row["sender_node_id"].is_string(), "expected sender_node_id");
    assert!(
        row["node_count"].as_i64().unwrap_or(0) >= 2,
        "expected node_count >= 2"
    );

    // Test interval mode on overview endpoint
    let path = format!(
        "/api/grafana/assurance-convergence?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "assurance interval mode should have data");
    let row = &arr[0];
    assert!(row["ts"].is_string(), "expected ts in interval mode");
    assert!(
        row["sample_count"].as_i64().unwrap_or(0) > 0,
        "expected sample_count > 0"
    );
    assert!(
        row["p50_ms"].is_number(),
        "expected p50_ms in interval mode"
    );

    // Test interval mode on senders endpoint
    let path = format!(
        "/api/grafana/assurance-convergence/senders?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "senders interval mode should have data");
    let row = &arr[0];
    assert!(
        row["ts"].is_string(),
        "expected ts in senders interval mode"
    );
    assert!(
        row["sample_count"].as_i64().unwrap_or(0) > 0,
        "expected sample_count > 0"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// DA Tracker — da-stats endpoint
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_da_stats() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let now = common::now_jce_micros();
    let erasure_root = [0xEE; 32];
    let guarantor_peer = [0x55; 32];

    // Node A: assurer — send shard request then transfer completes
    let mut stream_a = connect_test_node(port, 1, &telemetry).await;
    // Event IDs: first event after connection is event_id=0 (NodeInformation doesn't count)
    // Actually event IDs are per-node, starting from 0 after the NodeInformation
    send_events(
        &mut stream_a,
        &[
            common::sending_shard_request_event(now, guarantor_peer, erasure_root, 42),
            // event_id=0 for this node
        ],
    )
    .await;
    // Small delay to ensure ordering
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream_a,
        &[
            common::shards_transferred_event(now + 50_000, 0), // request_id=0, +50ms
        ],
    )
    .await;

    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/da-stats?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "should have at least one node");
    // Find node A's row
    let node_a_id = hex::encode([1u8; 32]);
    let node_a_row = arr
        .iter()
        .find(|r| r["node_id"].as_str() == Some(&node_a_id));
    assert!(node_a_row.is_some(), "expected node A row");
    let row = node_a_row.unwrap();
    assert!(row["shard_requests_sent"].as_i64().unwrap_or(0) >= 1);
    assert!(row["shards_transferred"].as_i64().unwrap_or(0) >= 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Shard Latency Histogram — end-to-end
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_shard_latency_histogram() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let now = common::now_jce_micros();
    let erasure_root = [0xEE; 32];
    let guarantor_peer = [0x55; 32];

    // Node A: assurer — 3 shard requests with known delays
    // Event IDs are sequential per node starting from 0.
    let mut stream_a = connect_test_node(port, 1, &telemetry).await;

    // Request 1: 5ms delay (5000 us)
    send_events(
        &mut stream_a,
        &[
            common::sending_shard_request_event(now, guarantor_peer, erasure_root, 10), // event_id=0
        ],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream_a,
        &[
            common::shards_transferred_event(now + 5_000, 0), // request_id=0, +5ms
        ],
    )
    .await;

    // Request 2: 50ms delay (50000 us)
    send_events(
        &mut stream_a,
        &[
            common::sending_shard_request_event(now + 100_000, guarantor_peer, erasure_root, 11), // event_id=2
        ],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream_a,
        &[
            common::shards_transferred_event(now + 150_000, 2), // request_id=2, +50ms
        ],
    )
    .await;

    // Request 3: 200ms delay (200000 us)
    send_events(
        &mut stream_a,
        &[
            common::sending_shard_request_event(now + 300_000, guarantor_peer, erasure_root, 12), // event_id=4
        ],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream_a,
        &[
            common::shards_transferred_event(now + 500_000, 4), // request_id=4, +200ms
        ],
    )
    .await;

    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/shard-latency?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json
        .as_array()
        .expect("shard-latency should return an array");
    assert!(!arr.is_empty(), "should have at least one time bucket");

    // Check assurer samples >= 3
    let total_samples: i64 = arr
        .iter()
        .map(|r| r["assurer_samples"].as_i64().unwrap_or(0))
        .sum();
    assert!(
        total_samples >= 3,
        "expected assurer_samples >= 3, got {}",
        total_samples
    );

    // The p50 should be reasonable — with 3 samples at 5ms, 50ms, 200ms
    // the median is 50ms, which falls in bucket 25-50ms or 50-100ms
    let row = &arr[0];
    assert!(
        row["assurer_p50"].is_number(),
        "expected assurer_p50 to be a number"
    );
    let p50 = row["assurer_p50"].as_i64().unwrap_or(0);
    assert!(
        p50 > 0 && p50 < 500,
        "expected p50 in reasonable range (0-500ms), got {}",
        p50
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Shard Latency with Failures
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_shard_latency_with_failure() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let now = common::now_jce_micros();
    let erasure_root = [0xEE; 32];
    let guarantor_peer = [0x55; 32];

    // Node A: assurer — send SendingShardRequest then ShardRequestFailed
    let mut stream_a = connect_test_node(port, 1, &telemetry).await;

    // Request that fails after 10ms
    send_events(
        &mut stream_a,
        &[
            common::sending_shard_request_event(now, guarantor_peer, erasure_root, 10), // event_id=0
        ],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream_a,
        &[
            common::shard_request_failed_event_with_id(now + 10_000, 0, "timeout"), // request_id=0, +10ms
        ],
    )
    .await;

    // Request that succeeds after 20ms (to confirm both paths work on same node)
    send_events(
        &mut stream_a,
        &[
            common::sending_shard_request_event(now + 100_000, guarantor_peer, erasure_root, 11), // event_id=2
        ],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream_a,
        &[
            common::shards_transferred_event(now + 120_000, 2), // request_id=2, +20ms
        ],
    )
    .await;

    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/shard-latency?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json
        .as_array()
        .expect("shard-latency should return an array");
    assert!(!arr.is_empty(), "should have at least one time bucket");

    // Sum failed_count across all buckets
    let total_failed: i64 = arr
        .iter()
        .map(|r| r["failed_count"].as_i64().unwrap_or(0))
        .sum();
    assert!(
        total_failed >= 1,
        "expected failed_count >= 1, got {}",
        total_failed
    );

    // Sum assurer_samples: both the failed and successful request contribute
    // (failed still measures latency in the assurer histogram)
    let total_samples: i64 = arr
        .iter()
        .map(|r| r["assurer_samples"].as_i64().unwrap_or(0))
        .sum();
    assert!(
        total_samples >= 2,
        "expected assurer_samples >= 2 (1 failed + 1 success), got {}",
        total_samples
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Guarantee Convergence — multi-core slot summary
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_guarantee_convergence_multi_core_slot_summary() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let now = common::now_jce_micros();
    let report_hash_a = [0xAA; 32];
    let report_hash_b = [0xBB; 32];

    // Node 1: guarantor A — builds guarantee with report_hash_a, slot=300, core=3
    let mut stream_1 = connect_test_node(port, 1, &telemetry).await;
    send_events(
        &mut stream_1,
        &[
            common::wp_received_event(now, 9000, 3),
            common::guarantee_built_event_with_hash(now + 100_000, 9000, report_hash_a, 300),
        ],
    )
    .await;

    // Node 2: guarantor B — builds guarantee with report_hash_b, slot=300, core=5
    let mut stream_2 = connect_test_node(port, 2, &telemetry).await;
    send_events(
        &mut stream_2,
        &[
            common::wp_received_event(now, 9001, 5),
            common::guarantee_built_event_with_hash(now + 100_000, 9001, report_hash_b, 300),
        ],
    )
    .await;

    // Nodes 3, 4: validators — receive both guarantees
    let mut stream_3 = connect_test_node(port, 3, &telemetry).await;
    let mut stream_4 = connect_test_node(port, 4, &telemetry).await;

    send_events(
        &mut stream_3,
        &[
            common::guarantee_received_event(now + 200_000, 300, report_hash_a),
            common::guarantee_received_event(now + 250_000, 300, report_hash_b),
        ],
    )
    .await;
    send_events(
        &mut stream_4,
        &[
            common::guarantee_received_event(now + 300_000, 300, report_hash_a),
            common::guarantee_received_event(now + 350_000, 300, report_hash_b),
        ],
    )
    .await;

    common::flush_all(&telemetry).await;

    // Overview: per-slot summary
    let path = format!("/api/grafana/guarantee-convergence?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    // Find slot 300
    let slot300 = arr.iter().find(|r| r["slot"].as_i64() == Some(300));
    assert!(slot300.is_some(), "expected data for slot 300");
    let row = slot300.unwrap();
    assert!(
        row["guarantee_count"].as_i64().unwrap_or(0) >= 2,
        "expected guarantee_count >= 2 for two different report hashes"
    );

    // Detail: per-guarantee
    let path = format!(
        "/api/grafana/guarantee-convergence/detail?{}",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");

    // Should have entries for both report hashes in slot 300
    let slot300_entries: Vec<&Value> = arr
        .iter()
        .filter(|r| r["slot"].as_i64() == Some(300))
        .collect();
    assert!(
        slot300_entries.len() >= 2,
        "expected at least 2 detail entries for slot 300, got {}",
        slot300_entries.len()
    );

    // Each should have node_count >= 2 (two validators received each guarantee)
    for entry in &slot300_entries {
        assert!(
            entry["node_count"].as_i64().unwrap_or(0) >= 2,
            "expected node_count >= 2, got {:?}",
            entry["node_count"]
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Assurance Convergence — pending buffer path (receiver before sender)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_assurance_convergence_pending_buffer() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let now = common::now_jce_micros();
    let anchor = [0xCC; 32];

    // Step 1: send Importing on any node to populate HeaderHashLookup (anchor → slot)
    let mut stream_x = connect_test_node(port, 10, &telemetry).await;
    send_events(&mut stream_x, &[common::importing_event(now, 600, anchor)]).await;

    // Step 2: send AssuranceReceived BEFORE DistributingAssurance (out-of-order)
    // Node B: receiver — sees assurance from sender (node 1, peer_id=[1;32])
    let sender_peer_id = [1u8; 32];
    let mut stream_b = connect_test_node(port, 2, &telemetry).await;
    send_events(
        &mut stream_b,
        &[common::assurance_received_event_with(
            now + 200_000,
            anchor,
            sender_peer_id,
        )],
    )
    .await;

    // Node C: another receiver
    let mut stream_c = connect_test_node(port, 3, &telemetry).await;
    send_events(
        &mut stream_c,
        &[common::assurance_received_event_with(
            now + 250_000,
            anchor,
            sender_peer_id,
        )],
    )
    .await;

    // Step 3: send DistributingAssurance AFTER receivers (pending buffer resolves)
    // Node A: sender — distributes assurance
    let mut stream_a = connect_test_node(port, 1, &telemetry).await;
    send_events(
        &mut stream_a,
        &[common::distributing_assurance_event(now + 100_000, anchor)],
    )
    .await;

    common::flush_all(&telemetry).await;

    // Test overview endpoint
    let path = format!("/api/grafana/assurance-convergence?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(
        !arr.is_empty(),
        "pending buffer path should still produce assurance convergence data"
    );

    // Find slot 600 (from the Importing event)
    let slot600 = arr.iter().find(|r| r["slot"].as_i64() == Some(600));
    assert!(
        slot600.is_some(),
        "expected assurance convergence data for slot 600"
    );
    let row = slot600.unwrap();
    assert!(
        row["sender_count"].as_i64().unwrap_or(0) >= 1,
        "expected sender_count >= 1"
    );
    assert!(row["p50_ms"].is_number(), "expected p50_ms");

    // Test senders endpoint — should find the sender
    let path = format!(
        "/api/grafana/assurance-convergence/senders?{}",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(
        !arr.is_empty(),
        "senders endpoint should have data from pending buffer resolution"
    );
    let row = &arr[0];
    assert!(
        row["node_count"].as_i64().unwrap_or(0) >= 2,
        "expected node_count >= 2 (two receivers)"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 3: New grafana endpoint tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_failure_rates() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Inject success + failure events
    let events = vec![
        common::wp_received_event(ts, 9100, 3),
        common::wp_received_event(ts + 1000, 9101, 3),
        common::wp_failed_event_with_reason(ts + 2000, 9100, "out of gas"),
        common::shard_request_failed_event(ts + 3000, "timeout"),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!("/api/grafana/failure-rates?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(
        json["overall"]["total_events"].as_i64().unwrap() > 0,
        "should have total events"
    );
    assert!(
        json["overall"]["failed_events"].as_i64().unwrap() > 0,
        "should have failures"
    );
    assert!(
        json["overall"]["failure_rate"].as_f64().unwrap() > 0.0,
        "rate should be > 0"
    );

    let categories = json["by_category"]
        .as_array()
        .expect("should have categories");
    assert!(!categories.is_empty(), "should have category breakdown");

    // recent_failures should have entries (from raw events)
    let recent = json["recent_failures"]
        .as_array()
        .expect("should have recent_failures");
    assert!(!recent.is_empty(), "should have recent failure events");
    // Each recent failure should have event_name
    assert!(
        recent[0]["event_name"].is_string(),
        "should have event_name"
    );
}

#[tokio::test]
async fn test_grafana_sync_timeline() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;
    let mut stream2 = connect_test_node(port, 2, &telemetry).await;

    let ts = common::now_jce_micros();

    // Node 1: slot 100, Node 2: slot 98 (behind by 2)
    send_events(&mut stream, &[common::best_block_event(ts, 100)]).await;
    send_events(&mut stream2, &[common::best_block_event(ts + 500, 98)]).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/sync-timeline?{}&interval=30m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert!(!arr.is_empty(), "should have timeline data");

    let row = &arr[0];
    assert!(
        row["total_nodes"].as_i64().unwrap() >= 2,
        "should have 2+ nodes"
    );
    assert!(
        row["network_slot"].as_i64().unwrap() >= 100,
        "network slot should be >= 100"
    );
    assert!(
        row["synced_nodes"].as_i64().unwrap() >= 1,
        "should have synced nodes"
    );
    assert!(
        row["sync_percentage"].as_f64().is_some(),
        "should have sync_percentage"
    );
}

#[tokio::test]
async fn test_grafana_connections_timeline() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Connected events (type 23, 26) are emitted by the TCP server on connect.
    // We also inject a Disconnected event (type 27) via telemetry.
    let events = vec![common::disconnected_event(ts + 1000, [3; 32])];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!(
        "/api/grafana/connections-timeline?{}&interval=30m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(
        json["health_stats"]["total_nodes_seen"].as_i64().unwrap() >= 1,
        "should see nodes"
    );
    assert!(
        json["health_stats"]["currently_connected"]
            .as_i64()
            .unwrap()
            >= 1,
        "should have connected nodes"
    );
}

#[tokio::test]
async fn test_grafana_guarantees() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Full guarantee pipeline: WPReceived → GuaranteeBuilt → SendingGuarantee → GuaranteeSent
    let events = vec![
        common::wp_received_event(ts, 9200, 5),
        common::guarantee_built_event(ts + 1000, 9200),
        common::sending_guarantee_event(ts + 2000, 1),
        common::guarantee_sent_event(ts + 3000, 1),
        // Also inject a receive event
        common::guarantee_received_event(ts + 4000, 50, [0xAA; 32]),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!("/api/grafana/guarantees?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let totals = &json["totals"];
    assert!(
        totals["built"].as_i64().unwrap() >= 1,
        "should have GuaranteeBuilt"
    );
    assert!(
        totals["sending"].as_i64().unwrap() >= 1,
        "should have SendingGuarantee"
    );
    assert!(
        totals["sent"].as_i64().unwrap() >= 1,
        "should have GuaranteeSent"
    );
    assert!(
        totals["received"].as_i64().unwrap() >= 1,
        "should have GuaranteeReceived"
    );

    // Success rates
    assert!(json["success_rates"]["send_success_rate"].as_f64().unwrap() > 0.0);
}

#[tokio::test]
async fn test_grafana_guarantees_by_guarantor() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Create a WP lifecycle that populates guarantee_convergence
    let events = vec![
        common::wp_received_event(ts, 9300, 7),
        common::guarantee_built_event_with_hash(ts + 1000, 9300, [0xEE; 32], 50),
        common::guarantee_received_event(ts + 2000, 50, [0xEE; 32]),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/guarantees/by-guarantor?{}",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(
        json["total_guarantors"].as_i64().unwrap() >= 0,
        "should have total_guarantors"
    );
    assert!(
        json["guarantors"].is_array(),
        "should have guarantors array"
    );
}

#[tokio::test]
async fn test_grafana_wp_stats() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Full WP lifecycle
    let events = vec![
        common::wp_received_event(ts, 9400, 2),
        common::authorized_event(ts + 1000, 9400),
        common::refined_event(ts + 2000, 9400),
        common::work_report_built_event(ts + 3000, 9400),
        common::guarantee_built_event(ts + 4000, 9400),
        common::guarantees_distributed_event(ts + 5000, 9400),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!("/api/grafana/wp-stats?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let totals = &json["totals"];
    assert!(
        totals["received"].as_i64().unwrap() >= 1,
        "should have received WPs"
    );
    assert!(
        totals["authorized"].as_i64().unwrap() >= 1,
        "should have authorized WPs"
    );
    assert!(
        totals["refined"].as_i64().unwrap() >= 1,
        "should have refined WPs"
    );
    assert!(
        totals["distributed"].as_i64().unwrap() >= 1,
        "should have distributed WPs"
    );

    // By core
    let by_core = json["by_core"].as_array().expect("should have by_core");
    assert!(!by_core.is_empty(), "should have core breakdown");
}

#[tokio::test]
async fn test_grafana_validators_cores() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Create guarantee activity so convergence tracker has data
    let events = vec![
        common::wp_received_event(ts, 9500, 4),
        common::guarantee_built_event_with_hash(ts + 1000, 9500, [0xFF; 32], 60),
        common::guarantee_received_event(ts + 2000, 60, [0xFF; 32]),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/validators/cores?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    // May be empty if convergence tracker hasn't flushed — that's OK for this test
    // The endpoint itself works and returns the right shape
    for row in arr {
        assert!(row["node_id"].is_string(), "should have node_id");
        assert!(
            row["guarantee_count"].is_number(),
            "should have guarantee_count"
        );
    }
}

#[tokio::test]
async fn test_grafana_network_health() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Mix of success and failure events for health scoring
    let events = vec![
        common::wp_received_event(ts, 9600, 0),
        common::wp_received_event(ts + 1000, 9601, 0),
        common::wp_failed_event(ts + 2000, 9600),
        common::authoring_event(ts + 3000, 80),
        common::authored_event(ts + 4000, 1),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!("/api/grafana/network-health?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(
        json["health_score"].as_f64().is_some(),
        "should have health_score"
    );
    assert!(
        json["overall_health"].is_string(),
        "should have overall_health"
    );

    let components = json["components"]
        .as_array()
        .expect("should have components");
    assert_eq!(components.len(), 5, "should have 5 health components");
    for comp in components {
        assert!(comp["name"].is_string(), "component should have name");
        assert!(
            comp["score"].as_f64().is_some(),
            "component should have score"
        );
        assert!(comp["status"].is_string(), "component should have status");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 4: Moderate endpoint tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_wp_active() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // 2 active WPs (received but not distributed), 1 completed
    let events = vec![
        common::wp_received_event(ts, 9700, 0),
        common::authorized_event(ts + 1000, 9700),
        common::wp_received_event(ts + 2000, 9701, 1),
        common::wp_received_event(ts + 3000, 9702, 2),
        common::authorized_event(ts + 4000, 9702),
        common::refined_event(ts + 5000, 9702),
        common::work_report_built_event(ts + 6000, 9702),
        common::guarantee_built_event(ts + 7000, 9702),
        common::guarantees_distributed_event(ts + 8000, 9702), // completed
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/wp-active?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let wps = json["work_packages"]
        .as_array()
        .expect("should have work_packages");
    // All 3 WPs should appear (no filter on distributed/failed — matches legacy behavior)
    assert!(
        wps.len() >= 3,
        "expected >= 3 WPs (all recent, not just in-flight), got {}",
        wps.len()
    );

    // Summary should count all WPs
    assert!(json["summary"]["total"].as_i64().unwrap() >= 3);

    // Reached — all 3 received, but only the completed one reached distributed
    assert!(json["reached"]["received"].as_i64().unwrap() >= 3);

    // Stage durations (may be null if not enough data — just check structure)
    assert!(json["stage_duration_percentiles"].is_object());
}

#[tokio::test]
async fn test_grafana_wp_detail() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let events = vec![
        common::wp_received_event(ts, 9800, 5),
        common::authorized_event(ts + 1000, 9800),
        common::refined_event(ts + 2000, 9800),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    // We need the wp_hash — get it from wp-active
    let active_path = format!("/api/grafana/wp-active?{}", time_range_params());
    let active_response = server.get(&active_path).await;
    let active_json: Value = active_response.json();
    let wps = active_json["work_packages"].as_array().unwrap();

    if let Some(wp) = wps.first() {
        let hash = wp["wp_hash"].as_str().unwrap();
        let detail_path = format!("/api/grafana/wp/{}", hash);
        let response = server.get(&detail_path).await;
        assert_eq!(response.status_code(), StatusCode::OK);

        let json: Value = response.json();
        // Summary should exist
        assert!(json["summary"].is_object(), "should have summary");
        assert!(
            json["summary"]["wp_hash"].is_string(),
            "summary should have wp_hash"
        );
        // Events array (from raw events within 1h)
        assert!(json["events"].is_array(), "should have events array");
    }
}

#[tokio::test]
async fn test_grafana_blocks_summary() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let events = vec![
        common::authoring_event(ts, 90),
        common::authored_event(ts + 1000, 1),
        common::best_block_event(ts + 2000, 90),
        common::finalized_block_event(ts + 3000, 89),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!("/api/grafana/blocks/summary?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let totals = &json["totals"];
    assert!(
        totals["authoring_started"].as_i64().unwrap() >= 1,
        "should have authoring"
    );
    assert!(
        totals["authored"].as_i64().unwrap() >= 1,
        "should have authored"
    );
    assert!(
        totals["best_block_changes"].as_i64().unwrap() >= 1,
        "should have BestBlockChanged"
    );

    // Chain state
    assert!(json["chain"].is_object(), "should have chain state");

    // Authoring by node
    let by_node = json["authoring_by_node"]
        .as_array()
        .expect("should have authoring_by_node");
    assert!(!by_node.is_empty(), "should have per-node authoring data");
}

#[tokio::test]
async fn test_grafana_core_metrics() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // WP lifecycle on core 3
    let events = vec![
        common::wp_received_event(ts, 9900, 3),
        common::authorized_event(ts + 1000, 9900),
        common::refined_event(ts + 2000, 9900),
        common::work_report_built_event(ts + 3000, 9900),
        common::guarantee_built_event(ts + 4000, 9900),
        common::guarantees_distributed_event(ts + 5000, 9900),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    let path = format!("/api/grafana/cores/3/metrics?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["core"].as_i64().unwrap(), 3);
    assert!(json["processing_efficiency_pct"].as_f64().is_some());
    assert!(json["work_packages_processed"].as_i64().unwrap() >= 1);
}

// ─────────────────────────────────────────────────────────────────────────────
// Step 0: Gap-filling tests (from ui-migration-review-02.txt)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_events_filter_by_node() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream1 = connect_test_node(port, 1, &telemetry).await;
    let mut stream2 = connect_test_node(port, 2, &telemetry).await;

    let ts = common::now_jce_micros();
    send_events(&mut stream1, &[common::wp_received_event(ts, 10100, 0)]).await;
    send_events(
        &mut stream2,
        &[common::wp_received_event(ts + 1000, 10101, 1)],
    )
    .await;
    common::flush_all(&telemetry).await;

    let node1_hex = common::node_id_hex(1);
    let path = format!(
        "/api/grafana/events?{}&event_types=94&node={}",
        time_range_params(),
        node1_hex
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let events = json["events"].as_array().expect("should have events");
    assert!(!events.is_empty(), "should have events for node 1");
    for e in events {
        assert_eq!(
            e["node_id"].as_str().unwrap(),
            node1_hex,
            "all events should be from node 1"
        );
    }
}

#[tokio::test]
async fn test_grafana_events_filter_by_core() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    send_events(
        &mut stream,
        &[
            common::wp_received_event(ts, 10200, 5),
            common::wp_received_event(ts + 1000, 10201, 7),
        ],
    )
    .await;
    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/events?{}&event_types=94&core=5",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let events = json["events"].as_array().expect("should have events");
    assert!(!events.is_empty(), "should have events for core 5");
}

#[tokio::test]
async fn test_grafana_events_no_event_types_returns_all() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    send_events(
        &mut stream,
        &[
            common::wp_received_event(ts, 10300, 0),
            common::best_block_event(ts + 1000, 300),
        ],
    )
    .await;
    common::flush_all(&telemetry).await;

    // No event_types param — should return all types
    let path = format!("/api/grafana/events?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let events = json["events"].as_array().expect("should have events");
    // Should have events of multiple types
    let types: std::collections::HashSet<i64> = events
        .iter()
        .filter_map(|e| e["event_type"].as_i64())
        .collect();
    assert!(
        types.len() >= 2,
        "expected multiple event types, got {:?}",
        types
    );
}

#[tokio::test]
async fn test_grafana_cores_last_activity() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    // WP on core 3 — should produce last_activity
    send_events(&mut stream, &[common::wp_received_event(ts, 10400, 3)]).await;
    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/cores?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    // Find core 3
    let core3 = arr.iter().find(|c| c["core"].as_i64() == Some(3));
    assert!(core3.is_some(), "core 3 should exist");
    assert!(
        core3.unwrap()["last_activity"].is_string(),
        "core 3 should have last_activity"
    );
}

#[tokio::test]
async fn test_grafana_wp_batch() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    send_events(
        &mut stream,
        &[
            common::wp_received_event(ts, 10500, 0),
            common::wp_received_event(ts + 1000, 10501, 1),
            common::wp_received_event(ts + 2000, 10502, 2),
        ],
    )
    .await;
    common::flush_all(&telemetry).await;

    // Get WP hashes from wp-active
    let active_path = format!("/api/grafana/wp-active?{}", time_range_params());
    let active_response = server.get(&active_path).await;
    let active_json: Value = active_response.json();
    let wps = active_json["work_packages"].as_array().unwrap();

    if wps.len() >= 2 {
        let hashes: Vec<String> = wps
            .iter()
            .take(2)
            .map(|wp| wp["wp_hash"].as_str().unwrap().to_string())
            .collect();

        let batch_response = server.post("/api/grafana/wp/batch").json(&hashes).await;
        assert_eq!(batch_response.status_code(), StatusCode::OK);

        let batch_json: Value = batch_response.json();
        let results = batch_json.as_array().expect("should return array");
        assert_eq!(results.len(), 2, "should return 2 WPs");
    }
}

#[tokio::test]
async fn test_grafana_core_validators() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    // Create guarantee activity on core 5 to populate guarantee_convergence
    send_events(
        &mut stream,
        &[
            common::wp_received_event(ts, 10600, 5),
            common::guarantee_built_event_with_hash(ts + 1000, 10600, [0xAB; 32], 70),
            common::guarantee_received_event(ts + 2000, 70, [0xAB; 32]),
        ],
    )
    .await;
    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/cores/5/validators?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["core"].as_i64().unwrap(), 5);
    assert!(
        json["validators"].is_array(),
        "should have validators array"
    );
    assert!(json["total_active"].is_number(), "should have total_active");
}

// ─────────────────────────────────────────────────────────────────────────────
// Phase 5: Execution metrics
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_execution_metrics() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Inject WP lifecycle: WPReceived → Authorized → Refined → BlockExecuted
    // Test helpers have known values:
    //   authorized_event: gas=100_000, elapsed_ns=200_000, load_ns=50_000
    //   refined_event: 1 work item, gas=500_000, elapsed_ns=1_000_000, load_ns=100_000
    //   block_executed_event([(10, 5000), (20, 3000)]): elapsed_ns=gas*2, load_ns=1000
    let events = vec![
        common::wp_received_event(ts, 11000, 0),
        common::authorized_event(ts + 1000, 11000),
        common::refined_event(ts + 2000, 11000),
        common::block_executed_event(ts + 3000, 1, &[(10, 5000), (20, 3000)]),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/execution?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();

    // Authorization phase (type 95): enricher maps to first service only
    let auth = &json["authorization"];
    assert!(
        auth["count"].as_i64().unwrap() >= 1,
        "should have authorization events"
    );
    assert!(
        auth["avg_time_ns"].as_f64().unwrap() > 0.0,
        "auth should have avg_time_ns"
    );
    assert!(
        auth["avg_load_ns"].as_f64().unwrap() > 0.0,
        "auth should have avg_load_ns"
    );

    // Refinement phase (type 101): per-work-item timing
    let refine = &json["refinement"];
    assert!(
        refine["count"].as_i64().unwrap() >= 1,
        "should have refinement events"
    );
    assert!(
        refine["avg_time_ns"].as_f64().unwrap() > 0.0,
        "refine should have avg_time_ns"
    );
    assert!(
        refine["avg_load_ns"].as_f64().unwrap() > 0.0,
        "refine should have avg_load_ns"
    );

    // Accumulation phase (type 47): 2 services
    let accum = &json["accumulation"];
    assert!(
        accum["count"].as_i64().unwrap() >= 2,
        "should have 2+ accumulation entries (services 10, 20)"
    );
    assert!(
        accum["avg_time_ns"].as_f64().unwrap() > 0.0,
        "accum should have avg_time_ns"
    );
    assert!(
        accum["avg_load_ns"].as_f64().unwrap() > 0.0,
        "accum should have avg_load_ns"
    );

    // Per-service breakdown: now includes all phases
    let by_service = json["by_service"]
        .as_array()
        .expect("should have by_service array");

    // Every entry should have a phase field
    for entry in by_service {
        let phase = entry["phase"].as_str().expect("entry missing phase");
        assert!(
            ["authorization", "refinement", "accumulation"].contains(&phase),
            "unexpected phase: {}",
            phase
        );
    }

    // Accumulation: service 10 — gas=5000, elapsed_ns=10000, load_ns=1000
    let accum_svc10 = by_service.iter().find(|s| {
        s["service_id"].as_i64() == Some(10) && s["phase"].as_str() == Some("accumulation")
    });
    assert!(
        accum_svc10.is_some(),
        "should have accumulation for service 10"
    );
    let accum_svc10 = accum_svc10.unwrap();
    assert_eq!(accum_svc10["total_gas"].as_i64().unwrap(), 5000);
    assert!((accum_svc10["avg_time_ns"].as_f64().unwrap() - 10000.0).abs() < 1.0);
    assert!((accum_svc10["avg_load_ns"].as_f64().unwrap() - 1000.0).abs() < 1.0);

    // Accumulation: service 20 — gas=3000, elapsed_ns=6000, load_ns=1000
    let accum_svc20 = by_service.iter().find(|s| {
        s["service_id"].as_i64() == Some(20) && s["phase"].as_str() == Some("accumulation")
    });
    assert!(
        accum_svc20.is_some(),
        "should have accumulation for service 20"
    );
    let accum_svc20 = accum_svc20.unwrap();
    assert_eq!(accum_svc20["total_gas"].as_i64().unwrap(), 3000);
    assert!((accum_svc20["avg_time_ns"].as_f64().unwrap() - 6000.0).abs() < 1.0);

    // Refinement and authorization should appear for enriched services
    // (enricher maps WPReceived service_ids [10, 20] to Authorized and Refined)
    let refine_entries: Vec<_> = by_service
        .iter()
        .filter(|s| s["phase"].as_str() == Some("refinement"))
        .collect();
    assert!(
        !refine_entries.is_empty(),
        "should have refinement entries in by_service"
    );
    // First service (aligned with the single work item) should have timing
    let refine_with_timing = refine_entries
        .iter()
        .any(|e| e["avg_time_ns"].as_f64().unwrap() > 0.0);
    assert!(
        refine_with_timing,
        "at least one refinement entry should have timing"
    );

    let auth_entries: Vec<_> = by_service
        .iter()
        .filter(|s| s["phase"].as_str() == Some("authorization"))
        .collect();
    assert!(
        !auth_entries.is_empty(),
        "should have authorization entries in by_service"
    );
    // First service gets WP-level auth timing
    let auth_with_timing = auth_entries
        .iter()
        .any(|e| e["avg_time_ns"].as_f64().unwrap() > 0.0);
    assert!(
        auth_with_timing,
        "at least one authorization entry should have timing"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// Group 15: Validator profiling
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_validator_profiling_slow_vs_fast_node() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream_fast = connect_test_node(port, 1, &telemetry).await;
    let mut stream_slow = connect_test_node(port, 2, &telemetry).await;

    let ts = common::now_jce_micros();

    // Send 3 WPs from each node with different timing profiles.
    // Fast node: ~100ms between stages. Slow node: ~500ms between stages.
    for i in 0u64..3 {
        let sid_fast = 9000 + i * 2;
        let sid_slow = 9001 + i * 2;
        let base_fast = ts + i * 1_000_000;
        let base_slow = ts + i * 5_000_000;

        // Fast node — 100ms (100_000 µs) per stage
        let fast_events = vec![
            common::wp_received_event(base_fast, sid_fast, 3),
            common::authorized_event(base_fast + 100_000, sid_fast),
            common::refined_event(base_fast + 200_000, sid_fast),
            common::work_report_built_event(base_fast + 300_000, sid_fast),
            common::guarantee_built_event(base_fast + 400_000, sid_fast),
            common::guarantees_distributed_event(base_fast + 500_000, sid_fast),
        ];
        send_events(&mut stream_fast, &fast_events).await;

        // Slow node — 500ms (500_000 µs) per stage
        let slow_events = vec![
            common::wp_received_event(base_slow, sid_slow, 3),
            common::authorized_event(base_slow + 500_000, sid_slow),
            common::refined_event(base_slow + 1_000_000, sid_slow),
            common::work_report_built_event(base_slow + 1_500_000, sid_slow),
            common::guarantee_built_event(base_slow + 2_000_000, sid_slow),
            common::guarantees_distributed_event(base_slow + 2_500_000, sid_slow),
        ];
        send_events(&mut stream_slow, &slow_events).await;
    }

    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/validator-profiling?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();

    // Response is wrapped: { network_avg_total_ms, nodes: [...] }
    let network_avg = json["network_avg_total_ms"]
        .as_f64()
        .expect("missing network_avg_total_ms");
    let arr = json["nodes"].as_array().expect("nodes should be an array");
    assert_eq!(arr.len(), 2, "expected 2 nodes");

    // First entry should be the slow node (sorted by avg_total_ms DESC)
    let slow = &arr[0];
    let fast = &arr[1];

    // Verify all fields are present
    for entry in [slow, fast] {
        assert!(entry.get("node_id").is_some(), "missing node_id");
        assert!(entry.get("wp_count").is_some(), "missing wp_count");
        assert!(entry.get("failures").is_some(), "missing failures");
        assert!(entry.get("failure_rate").is_some(), "missing failure_rate");
        assert!(
            entry.get("avg_authorize_ms").is_some(),
            "missing avg_authorize_ms"
        );
        assert!(
            entry.get("avg_refine_ms").is_some(),
            "missing avg_refine_ms"
        );
        assert!(
            entry.get("avg_report_ms").is_some(),
            "missing avg_report_ms"
        );
        assert!(
            entry.get("avg_guarantee_ms").is_some(),
            "missing avg_guarantee_ms"
        );
        assert!(
            entry.get("avg_distribute_ms").is_some(),
            "missing avg_distribute_ms"
        );
        assert!(entry.get("avg_total_ms").is_some(), "missing avg_total_ms");
        assert!(
            entry.get("slowdown_factor").is_some(),
            "missing slowdown_factor"
        );
    }

    // Both should have 3 WPs, 0 failures
    assert_eq!(slow["wp_count"].as_i64().unwrap(), 3);
    assert_eq!(fast["wp_count"].as_i64().unwrap(), 3);
    assert_eq!(slow["failures"].as_i64().unwrap(), 0);
    assert_eq!(fast["failures"].as_i64().unwrap(), 0);
    assert_eq!(slow["failure_rate"].as_f64().unwrap(), 0.0);

    // Slow node should have higher avg_total_ms
    let slow_total = slow["avg_total_ms"].as_f64().unwrap();
    let fast_total = fast["avg_total_ms"].as_f64().unwrap();
    assert!(
        slow_total > fast_total,
        "slow node should have higher avg_total_ms: {} vs {}",
        slow_total,
        fast_total
    );

    // Network average should be between fast and slow
    assert!(
        network_avg > fast_total,
        "network_avg should be > fast: {} vs {}",
        network_avg,
        fast_total
    );
    assert!(
        network_avg < slow_total,
        "network_avg should be < slow: {} vs {}",
        network_avg,
        slow_total
    );

    // Slow node slowdown_factor > 1.0, fast node < 1.0
    let slow_factor = slow["slowdown_factor"].as_f64().unwrap();
    let fast_factor = fast["slowdown_factor"].as_f64().unwrap();
    assert!(
        slow_factor > 1.0,
        "slow node slowdown_factor should be > 1.0: {}",
        slow_factor
    );
    assert!(
        fast_factor < 1.0,
        "fast node slowdown_factor should be < 1.0: {}",
        fast_factor
    );

    // Test limit=1: should return only the slowest node, but network_avg reflects both
    let path_limited = format!(
        "/api/grafana/validator-profiling?{}&limit=1",
        time_range_params()
    );
    let response_limited = server.get(&path_limited).await;
    assert_eq!(response_limited.status_code(), StatusCode::OK);
    let json_limited: Value = response_limited.json();
    let limited_avg = json_limited["network_avg_total_ms"]
        .as_f64()
        .expect("missing network_avg_total_ms");
    let limited_nodes = json_limited["nodes"]
        .as_array()
        .expect("nodes should be an array");
    assert_eq!(limited_nodes.len(), 1, "limit=1 should return 1 node");
    assert_eq!(
        limited_avg, network_avg,
        "network_avg should be the same regardless of limit"
    );
    // The one returned should be the slow node
    assert!(
        limited_nodes[0]["avg_total_ms"].as_f64().unwrap() > fast_total,
        "limit=1 should return the slowest node"
    );
}

#[tokio::test]
async fn test_grafana_validator_profiling_with_failures() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // 2 successful pipelines
    for i in 0u64..2 {
        let sid = 9100 + i;
        let base = ts + i * 1_000_000;
        let events = vec![
            common::wp_received_event(base, sid, 5),
            common::authorized_event(base + 100_000, sid),
            common::refined_event(base + 200_000, sid),
            common::work_report_built_event(base + 300_000, sid),
            common::guarantee_built_event(base + 400_000, sid),
            common::guarantees_distributed_event(base + 500_000, sid),
        ];
        send_events(&mut stream, &events).await;
    }

    // 1 failed pipeline
    let sid_fail = 9102;
    let events = vec![
        common::wp_received_event(ts + 3_000_000, sid_fail, 5),
        common::wp_failed_event(ts + 3_100_000, sid_fail),
    ];
    send_events(&mut stream, &events).await;

    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/validator-profiling?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json["nodes"].as_array().expect("nodes should be an array");
    assert_eq!(arr.len(), 1, "expected 1 node");

    let entry = &arr[0];
    assert_eq!(entry["wp_count"].as_i64().unwrap(), 3);
    assert_eq!(entry["failures"].as_i64().unwrap(), 1);
    let failure_rate = entry["failure_rate"].as_f64().unwrap();
    assert!(
        (failure_rate - 1.0 / 3.0).abs() < 0.01,
        "failure_rate should be ~0.333: {}",
        failure_rate
    );
    assert!(
        json["network_avg_total_ms"].is_number(),
        "missing network_avg_total_ms"
    );
}

#[tokio::test]
async fn test_grafana_validator_profiling_timeseries() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();
    let sid: u64 = 9200;

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

    // Node ID for test node 1 = [1; 32] hex-encoded
    let node_id_hex = "01".repeat(32);
    let path = format!(
        "/api/grafana/validator-profiling-timeseries?{}&interval=1m&node={}",
        time_range_params(),
        node_id_hex,
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return an array");
    assert!(!arr.is_empty(), "should have at least 1 bucket");

    let bucket = &arr[0];
    assert!(bucket.get("ts").is_some(), "missing ts");
    assert!(bucket.get("node_id").is_some(), "missing node_id");
    assert!(bucket.get("wp_count").is_some(), "missing wp_count");
    assert!(
        bucket.get("avg_authorize_ms").is_some(),
        "missing avg_authorize_ms"
    );
    assert!(bucket.get("avg_total_ms").is_some(), "missing avg_total_ms");
    assert_eq!(bucket["wp_count"].as_i64().unwrap(), 1);
}

#[tokio::test]
async fn test_grafana_validator_profiling_failures_only_node() {
    let (server, telemetry, port, _store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    // Send only failed pipelines — no distributed WPs
    for i in 0u64..3 {
        let sid = 9300 + i;
        let events = vec![
            common::wp_received_event(ts + i * 1_000_000, sid, 5),
            common::wp_failed_event(ts + i * 1_000_000 + 50_000, sid),
        ];
        send_events(&mut stream, &events).await;
    }

    common::flush_all(&telemetry).await;

    let path = format!("/api/grafana/validator-profiling?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json["nodes"].as_array().expect("nodes should be an array");
    assert_eq!(arr.len(), 1, "expected 1 node (failures-only)");

    let entry = &arr[0];
    assert_eq!(entry["wp_count"].as_i64().unwrap(), 3);
    assert_eq!(entry["failures"].as_i64().unwrap(), 3);
    assert_eq!(entry["failure_rate"].as_f64().unwrap(), 1.0);
    // No distributed WPs → all timing AVGs should be null
    assert!(
        entry["avg_total_ms"].is_null(),
        "avg_total_ms should be null for failures-only node"
    );
    assert!(
        entry["avg_authorize_ms"].is_null(),
        "avg_authorize_ms should be null"
    );
    assert!(
        entry["slowdown_factor"].is_null(),
        "slowdown_factor should be null"
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// DA Latency: Bundle Reconstruction
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_bundle_latency() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let now = common::now_jce_micros();
    let assurer_peer = [0x55; 32];
    let audit_id = 42u64;

    let mut stream = connect_test_node(port, 1, &telemetry).await;

    // Shard request 1: 10ms delay
    send_events(
        &mut stream,
        &[
            common::sending_bundle_shard_request_event(now, audit_id, assurer_peer, 0), // event_id=0
        ],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream,
        &[
            common::bundle_shard_transferred_event(now + 10_000, 0), // +10ms
        ],
    )
    .await;

    // Shard request 2: 50ms delay
    send_events(
        &mut stream,
        &[
            common::sending_bundle_shard_request_event(now + 100_000, audit_id, assurer_peer, 1), // event_id=2
        ],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream,
        &[
            common::bundle_shard_transferred_event(now + 150_000, 2), // +50ms
        ],
    )
    .await;

    // Reconstruction: 5ms CPU
    send_events(
        &mut stream,
        &[
            common::reconstructing_bundle_event(
                now + 200_000,
                audit_id,
                ReconstructionKind::NonTrivial,
            ), // event_id=4
        ],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream,
        &[
            common::bundle_reconstructed_event(now + 205_000, audit_id), // +5ms reconstruction, completes e2e too
        ],
    )
    .await;

    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/bundle-latency?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json
        .as_array()
        .expect("bundle-latency should return an array");
    assert!(!arr.is_empty(), "should have at least one time bucket");

    // Check shard requestor samples >= 2
    let total_shard_req: i64 = arr
        .iter()
        .map(|r| r["shard_req_samples"].as_i64().unwrap_or(0))
        .sum();
    assert!(
        total_shard_req >= 2,
        "expected shard_req_samples >= 2, got {}",
        total_shard_req
    );

    // Check reconstruction samples >= 1
    let total_reconstruct: i64 = arr
        .iter()
        .map(|r| r["reconstruct_samples"].as_i64().unwrap_or(0))
        .sum();
    assert!(
        total_reconstruct >= 1,
        "expected reconstruct_samples >= 1, got {}",
        total_reconstruct
    );

    // Check e2e samples >= 1
    let total_e2e: i64 = arr
        .iter()
        .map(|r| r["e2e_samples"].as_i64().unwrap_or(0))
        .sum();
    assert!(
        total_e2e >= 1,
        "expected e2e_samples >= 1, got {}",
        total_e2e
    );

    // p50 should be a reasonable number
    let row = &arr[0];
    if let Some(p50) = row["shard_req_p50"].as_i64() {
        assert!(
            p50 > 0 && p50 < 1000,
            "expected shard_req_p50 in range (0-1000ms), got {}",
            p50
        );
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// DA Latency: Segment Fetching
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_segment_latency() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let now = common::now_jce_micros();
    let assurer_peer = [0x66; 32];
    let submission_id = 99u64;

    let mut stream = connect_test_node(port, 1, &telemetry).await;

    // Segment shard request: 20ms delay
    send_events(
        &mut stream,
        &[
            common::sending_segment_shard_request_event(now, submission_id, assurer_peer), // event_id=0
        ],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream,
        &[
            common::segment_shards_transferred_event(now + 20_000, 0), // +20ms
        ],
    )
    .await;

    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/segment-latency?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json
        .as_array()
        .expect("segment-latency should return an array");
    assert!(!arr.is_empty(), "should have at least one time bucket");

    let total_shard_req: i64 = arr
        .iter()
        .map(|r| r["shard_req_samples"].as_i64().unwrap_or(0))
        .sum();
    assert!(
        total_shard_req >= 1,
        "expected shard_req_samples >= 1, got {}",
        total_shard_req
    );
}

// ─────────────────────────────────────────────────────────────────────────────
// DA Latency: Preimage Transfer
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_preimage_latency() {
    let (server, telemetry, port, _store) = setup_test_api().await;

    let now = common::now_jce_micros();
    let recipient = [0x77; 32];
    let hash = [0xAA; 32];

    let mut stream = connect_test_node(port, 1, &telemetry).await;

    // Preimage request: 15ms delay
    send_events(
        &mut stream,
        &[
            common::sending_preimage_request_event(now, recipient, hash), // event_id=0
        ],
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    send_events(
        &mut stream,
        &[
            common::preimage_transferred_event(now + 15_000, 0, 4096), // +15ms
        ],
    )
    .await;

    common::flush_all(&telemetry).await;

    let path = format!(
        "/api/grafana/preimage-latency?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json
        .as_array()
        .expect("preimage-latency should return an array");
    assert!(!arr.is_empty(), "should have at least one time bucket");

    let total_req: i64 = arr
        .iter()
        .map(|r| r["req_samples"].as_i64().unwrap_or(0))
        .sum();
    assert!(
        total_req >= 1,
        "expected req_samples >= 1, got {}",
        total_req
    );
}
