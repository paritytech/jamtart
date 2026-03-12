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
        format!(
            "/api/grafana/services/timeseries?{}&interval=1m",
            time_range_params()
        ),
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

    // Verify structure: each entry has ts, event_type, event_type_name, count
    for entry in arr {
        assert!(entry.get("ts").is_some(), "entry missing ts");
        assert!(entry.get("event_type").is_some(), "entry missing event_type");
        assert!(entry.get("event_type_name").is_some(), "entry missing event_type_name");
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
            Some(94) => assert_eq!(entry["event_type_name"].as_str(), Some("WorkPackageReceived")),
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
        common::authoring_event(ts, 200),          // event_id = 0, slot = 200
        common::authored_event(ts + 1000, 0),       // authoring_id = 0 → inherits slot 200
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
    let arr = json.as_array().expect("blocks/contents should return an array");

    assert!(
        !arr.is_empty(),
        "blocks/contents should return rows after Authoring→Authored"
    );

    let row = &arr[0];
    assert_eq!(row["slot"].as_i64(), Some(200), "slot should be 200");
    assert_eq!(row["num_guarantees"].as_i64(), Some(3), "num_guarantees should be 3");
    assert_eq!(row["num_assurances"].as_i64(), Some(2), "num_assurances should be 2");
    assert_eq!(row["num_preimages"].as_i64(), Some(1), "num_preimages should be 1");
    assert_eq!(row["num_tickets"].as_i64(), Some(2), "num_tickets should be 2");
    assert_eq!(row["num_disputes"].as_i64(), Some(0), "num_disputes should be 0");
    assert_eq!(row["extrinsic_size"].as_i64(), Some(2048), "extrinsic_size should be 2048");
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
        assert!(entry.get("service_id").is_some(), "entry missing service_id");
        assert!(entry.get("work_packages").is_some(), "entry missing work_packages");
    }

    // Check that services 10 and 20 appear
    let svc10 = arr.iter().find(|e| e["service_id"].as_i64() == Some(10));
    let svc20 = arr.iter().find(|e| e["service_id"].as_i64() == Some(20));

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
    let cores: Vec<i64> = arr
        .iter()
        .filter_map(|e| e["core"].as_i64())
        .collect();
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
    let arr = response.json::<Value>().as_array().cloned().unwrap_or_default();
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
    let arr = response.json::<Value>().as_array().cloned().unwrap_or_default();
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
    let arr = response.json::<Value>().as_array().cloned().unwrap_or_default();
    assert!(arr.is_empty(), "non-existent event_type should return empty array");
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
    assert!(json["received"].as_i64().unwrap_or(0) >= 3, "expected received >= 3");
    assert!(json["authorized"].as_i64().unwrap_or(0) >= 2, "expected authorized >= 2");
    assert!(json["refined"].as_i64().unwrap_or(0) >= 2, "expected refined >= 2");
    assert!(json["distributed"].as_i64().unwrap_or(0) >= 1, "expected distributed >= 1");
    assert!(json["failed"].as_i64().unwrap_or(0) >= 1, "expected failed >= 1");

    // bottlenecks assertions
    let path = format!("/api/grafana/bottlenecks?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(json["total_wps"].as_i64().unwrap_or(0) >= 3, "expected total_wps >= 3");
    assert!(json["failed_wps"].as_i64().unwrap_or(0) >= 1, "expected failed_wps >= 1");
    assert!(
        json["failure_rate"].as_f64().unwrap_or(0.0) > 0.0,
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
    let arr = json.as_array().expect("node-stats-aggregate should return an array");
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
    let arr = json.as_array().expect("filtered aggregate should return an array");
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

    let path = format!("/api/grafana/cores?{}&core=3", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("cores detail should return an array");

    // In detail mode there should be entries with recent_work_packages
    if !arr.is_empty() {
        let entry = &arr[0];
        assert!(entry.get("core").is_some(), "entry missing core");
        assert!(
            entry.get("recent_work_packages").is_some(),
            "detail mode should include recent_work_packages"
        );
    }
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
    let node2 = arr.iter().find(|n| n["node_id"].as_str() == Some(&node2_id));
    if let Some(n) = node2 {
        assert_eq!(
            n["is_connected"].as_bool(),
            Some(false),
            "node 2 should be disconnected"
        );
    }

    // Node 1 should still be connected
    let node1_id = common::node_id_hex(1);
    let node1 = arr.iter().find(|n| n["node_id"].as_str() == Some(&node1_id));
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
        assert!(entry.get("node_id").is_some(), "entry missing node_id for group_by=node_id");
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
    assert!(!arr.is_empty(), "node-filtered timeseries should have results");

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
// Event types endpoint with group filter
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_grafana_event_types_unfiltered() {
    let (server, _telemetry, _port, _store) = setup_test_api().await;

    let response = server.get("/api/grafana/event-types").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    let arr = json.as_array().expect("should return array");
    assert_eq!(arr.len(), 115, "unfiltered should return all 115 event types");
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
    assert!(names.contains(&"Authored"), "blocks group should contain Authored");
    assert!(names.contains(&"BlockExecuted"), "blocks group should contain BlockExecuted");
    assert!(!names.iter().any(|n| *n == "WorkPackageFailed"), "blocks group should not contain WorkPackageFailed");
    assert!(!names.iter().any(|n| *n == "Dropped"), "blocks group should not contain Dropped");

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
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
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

    // Verify structure
    for entry in arr {
        assert!(entry.get("ts").is_some(), "entry missing ts");
        assert!(entry.get("service_id").is_some(), "entry missing service_id");
        assert!(entry.get("count").is_some(), "entry missing count");
        assert!(entry.get("gas").is_some(), "entry missing gas");
    }
}

#[tokio::test]
async fn test_grafana_services_timeseries_service_filter() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let events = vec![
        common::block_executed_event(ts, 42, &[(10, 50_000), (20, 30_000)]),
    ];
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
            entry["service_id"].as_i64(),
            Some(10),
            "service filter should only return service 10"
        );
    }
}

#[tokio::test]
async fn test_grafana_services_timeseries_event_type_filter() {
    let (server, telemetry, port, store) = setup_test_api().await;
    let mut stream = connect_test_node(port, 1, &telemetry).await;

    let ts = common::now_jce_micros();

    let events = vec![
        common::wp_received_event(ts, 9600, 3),
        common::authorized_event(ts + 100_000, 9600),
        common::refined_event(ts + 200_000, 9600),
        common::block_executed_event(ts + 300_000, 42, &[(10, 50_000)]),
    ];
    send_events(&mut stream, &events).await;
    common::flush_all(&telemetry).await;
    common::refresh_aggregates(store.pool()).await;

    // Filter by event_types using group name
    let path = format!(
        "/api/grafana/services/timeseries?{}&interval=1m&event_types=wp_pipeline",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert!(json.is_array(), "should return array");
}
