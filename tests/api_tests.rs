mod common;

use axum::http::StatusCode;
use axum_test::TestServer;
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::events::Event;
use tart_backend::feature_flags::FeatureFlags;
use tart_backend::metrics_tracker::{self, MetricsTracker};
use tart_backend::{EventStore, TelemetryServer};

const SLOT_MICROS: u64 = 6_000_000;
const MICROS_PER_MILLI: u64 = 1_000;

async fn setup_test_api() -> (TestServer, Arc<TelemetryServer>, u16, Arc<MetricsTracker>) {
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

    let metrics_tracker = Arc::new(MetricsTracker::new());
    let (metrics_tx, metrics_rx) = tokio::sync::mpsc::channel(50_000);
    tokio::spawn(metrics_tracker::run(
        Arc::clone(&metrics_tracker),
        metrics_rx,
    ));

    let telemetry_server = Arc::new(
        TelemetryServer::with_options_and_metrics(
            "127.0.0.1:0",
            Some(Arc::clone(&store)),
            true,
            0,
            Some(metrics_tx),
            FeatureFlags::default(),
        )
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
        store: Arc::clone(&store),
        telemetry_server: Arc::clone(&telemetry_server),
        broadcaster,
        health_monitor,
        jam_rpc: None,
        cache: Arc::new(tart_backend::cache::TtlCache::new(
            std::time::Duration::ZERO,
        )),
        metrics_tracker: Some(Arc::clone(&metrics_tracker)),
    };

    let app = create_api_router(api_state);
    let test_server = TestServer::new(app).unwrap();

    (
        test_server,
        telemetry_server,
        telemetry_port,
        metrics_tracker,
    )
}

/// Live counters report completed wall-clock seconds only.
async fn wait_for_current_second_to_end() {
    let into_second = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    let remaining =
        Duration::from_secs(1) - Duration::from_nanos(into_second.subsec_nanos().into());
    tokio::time::sleep(remaining).await;
}

async fn get_json(server: &TestServer, path: &str) -> Value {
    let response = server.get(path).await;
    assert_eq!(response.status_code(), StatusCode::OK, "GET {path}");
    response.json()
}

/// Best-block changes for consecutive slots, one slot period apart.
fn best_blocks(first_slot: u32, first_at: u64, count: u32) -> Vec<Event> {
    (0..count)
        .map(|offset| {
            let slot = first_slot + offset;
            common::best_block_event_with_hash(
                first_at + u64::from(offset) * SLOT_MICROS,
                slot,
                common::block_hash(slot),
            )
        })
        .collect()
}

fn status_with_peers(ts: u64, num_peers: u32, num_val_peers: u32, num_sync_peers: u32) -> Event {
    Event::Status {
        timestamp: ts,
        num_peers,
        num_val_peers,
        num_sync_peers,
        num_guarantees: vec![0; common::TEST_CORE_COUNT],
        num_shards: 100,
        shards_size: 50_000,
        num_preimages: 10,
        preimages_size: 4096,
    }
}

#[tokio::test]
async fn test_health_endpoint() {
    let (server, _, _, _) = setup_test_api().await;

    let response = server.get("/api/health").await;

    assert_eq!(response.status_code(), StatusCode::OK);

    let json: Value = response.json();
    assert_eq!(json["status"], "ok");
    assert_eq!(json["service"], "tart-backend");
    assert!(json.get("version").is_some());
}

/// A node is identified by its peer id, and stays without a disconnect time
/// while its connection is open.
#[tokio::test]
async fn test_node_details_endpoint() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let _node = common::connect_test_node(telemetry_port, 3, &telemetry_server).await;
    let node_id = common::node_id_hex(3);

    let json = get_json(&server, &format!("/api/nodes/{node_id}")).await;
    assert_eq!(json["node_id"], node_id);
    assert_eq!(json["peer_id"], node_id);
    assert_eq!(json["implementation_name"], "test-node-3");
    assert_eq!(json["is_connected"], true);
    assert_eq!(json["disconnected_at"], Value::Null);
    assert_eq!(json["connection_info"]["event_count"], 0);
}

/// A burst of events reaches the server over many socket reads and writer
/// batches; once flushed, the node's event count must include every one.
#[tokio::test]
async fn test_node_event_count_after_burst() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let mut node = common::connect_test_node(telemetry_port, 4, &telemetry_server).await;

    let first_slot_at = common::now_jce_micros() - 1000 * SLOT_MICROS;
    let events: Vec<Event> = (0..1000u32)
        .map(|slot| common::best_block_event(first_slot_at + u64::from(slot) * SLOT_MICROS, slot))
        .collect();
    common::send_events(&mut node, &events).await;
    common::flush_all(&telemetry_server).await;

    let response = server
        .get(&format!("/api/nodes/{}", common::node_id_hex(4)))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert_eq!(json["event_count"], 1000);
}

/// Events refer to earlier ones by the id their node gave them (JIP-3): ids start
/// at 0 after NodeInformation, and a Dropped event takes the ids of the events it
/// stands for. Tests build those references from what `send_events` returns.
#[tokio::test]
async fn test_send_events_returns_server_event_ids() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let mut node = common::connect_test_node(telemetry_port, 5, &telemetry_server).await;

    let slot_100_at = common::now_jce_micros() - 10 * SLOT_MICROS;
    let event_ids = common::send_events(
        &mut node,
        &[
            common::best_block_event(slot_100_at, 100),
            // Best-block changes for slots 101 and 102 did not fit the node's buffer.
            Event::Dropped {
                timestamp: slot_100_at + SLOT_MICROS,
                last_timestamp: slot_100_at + 2 * SLOT_MICROS,
                num: 2,
            },
            common::best_block_event(slot_100_at + 3 * SLOT_MICROS, 103),
        ],
    )
    .await;
    assert_eq!(event_ids, [0, 1, 3]);
    common::flush_all(&telemetry_server).await;

    let response = server
        .get(&format!("/api/nodes/{}/timeline", common::node_id_hex(5)))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    let stored_ids: Vec<u64> = json["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["event_id"].as_u64().unwrap())
        .collect();
    assert_eq!(stored_ids, [3, 1, 0], "newest first");
}

#[tokio::test]
async fn test_node_details_not_found() {
    let (server, _, _, _) = setup_test_api().await;

    // Use a valid hex node ID that doesn't exist (64 hex chars = 32 bytes)
    let nonexistent_node_id = "0000000000000000000000000000000000000000000000000000000000000000";
    let response = server
        .get(&format!("/api/nodes/{}", nonexistent_node_id))
        .await;

    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
}

// --- Metrics endpoints ---

/// The realtime window counts every event of every node, best-block changes
/// as blocks and finalizations separately, and reports the highest best slot
/// seen on the network.
#[tokio::test]
async fn test_realtime_metrics() {
    let (server, telemetry_server, telemetry_port, metrics_tracker) = setup_test_api().await;
    let mut author = common::connect_test_node(telemetry_port, 1, &telemetry_server).await;
    let mut importer = common::connect_test_node(telemetry_port, 2, &telemetry_server).await;

    let slot_100_at = common::now_jce_micros() - 3 * SLOT_MICROS;
    let mut author_events = best_blocks(100, slot_100_at, 3);
    author_events.push(common::finalized_block_event_with_hash(
        slot_100_at + 2 * SLOT_MICROS + 50 * MICROS_PER_MILLI,
        100,
        common::block_hash(100),
    ));
    common::send_events(&mut author, &author_events).await;
    common::send_events(
        &mut importer,
        &best_blocks(100, slot_100_at + 400 * MICROS_PER_MILLI, 3),
    )
    .await;
    metrics_tracker.wait_for_events(7).await;
    wait_for_current_second_to_end().await;

    let json = get_json(&server, "/api/metrics/realtime?seconds=60").await;
    assert_eq!(json["window_seconds"], 60);
    assert_eq!(json["data"].as_array().unwrap().len(), 60);
    assert_eq!(json["totals"]["events"], 7);
    assert_eq!(json["totals"]["best_blocks"], 6);
    assert_eq!(json["totals"]["finalized_blocks"], 1);
    assert_eq!(json["totals"]["latest_slot"], 102);
    assert_eq!(json["totals"]["active_nodes"], 2);
}

/// Live counters keep the events of a node that has since disconnected, while
/// active_nodes counts only the nodes connected now.
#[tokio::test]
async fn test_live_counters() {
    let (server, telemetry_server, telemetry_port, metrics_tracker) = setup_test_api().await;
    let mut staying = common::connect_test_node(telemetry_port, 1, &telemetry_server).await;
    let mut leaving = common::connect_test_node(telemetry_port, 2, &telemetry_server).await;

    let slot_200_at = common::now_jce_micros() - 4 * SLOT_MICROS;
    common::send_events(&mut staying, &best_blocks(200, slot_200_at, 4)).await;
    common::send_events(
        &mut leaving,
        &[
            status_with_peers(slot_200_at, 12, 9, 3),
            common::best_block_event_with_hash(
                slot_200_at + 300 * MICROS_PER_MILLI,
                200,
                common::block_hash(200),
            ),
        ],
    )
    .await;
    drop(leaving);
    telemetry_server.wait_for_connections(1).await;
    metrics_tracker.wait_for_events(6).await;
    wait_for_current_second_to_end().await;

    let json = get_json(&server, "/api/metrics/live").await;
    assert_eq!(json["last_10s"]["events"], 6);
    assert_eq!(json["last_10s"]["blocks"], 5);
    assert_eq!(json["last_1m"]["events"], 6);
    assert_eq!(json["last_1m"]["blocks"], 5);
    assert_eq!(json["latest_slot"], 203);
    assert_eq!(json["active_nodes"], 1);
}

// --- Other endpoints ---

/// Network parameters come from the connected nodes' NodeInformation, and the
/// implementation list names only nodes that are still connected.
#[tokio::test]
async fn test_network_info() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let _first = common::connect_test_node(telemetry_port, 1, &telemetry_server).await;
    let _second = common::connect_test_node(telemetry_port, 2, &telemetry_server).await;
    let gone = common::connect_test_node(telemetry_port, 3, &telemetry_server).await;
    drop(gone);
    telemetry_server.wait_for_connections(2).await;
    common::flush_and_wait(&telemetry_server).await;

    let json = get_json(&server, "/api/network").await;
    assert_eq!(json["network"]["connected_nodes"], 2);
    assert_eq!(json["network"]["total_nodes"], 3);
    assert_eq!(json["topology"]["core_count"], 16);
    assert_eq!(json["topology"]["validator_count"], 48);
    assert_eq!(json["topology"]["epoch_period"], 600);
    assert_eq!(
        json["implementations"],
        json!([
            {"name": "test-node-1", "version": "1.0.0"},
            {"name": "test-node-2", "version": "1.0.0"},
        ])
    );
}

#[tokio::test]
async fn test_detailed_health() {
    let (server, _, _, _) = setup_test_api().await;
    let response = server.get("/api/health/detailed").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let json: Value = response.json();
    assert!(json.is_object());
    assert!(json.get("status").is_some());
    assert!(json.get("components").is_some());
    assert!(json.get("uptime_seconds").is_some());
}

/// A topology link is a peer that announced blocks to a node more than once;
/// a peer heard from only once is not a link, but its announcement still counts.
#[tokio::test]
async fn test_peer_topology() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let mut node = common::connect_test_node(telemetry_port, 1, &telemetry_server).await;
    let regular_peer = [2u8; 32];
    let occasional_peer = [3u8; 32];

    let slot_100_at = common::now_jce_micros() - 4 * SLOT_MICROS;
    let announced_at = |slot: u32| slot_100_at + u64::from(slot - 100) * SLOT_MICROS;
    common::send_events(
        &mut node,
        &[
            common::block_announced_event_from(
                announced_at(100),
                regular_peer,
                100,
                common::block_hash(100),
            ),
            common::block_announced_event_from(
                announced_at(101),
                regular_peer,
                101,
                common::block_hash(101),
            ),
            common::block_announced_event_from(
                announced_at(102),
                regular_peer,
                102,
                common::block_hash(102),
            ),
            common::block_announced_event_from(
                announced_at(102) + 120 * MICROS_PER_MILLI,
                occasional_peer,
                102,
                common::block_hash(102),
            ),
        ],
    )
    .await;
    common::flush_all(&telemetry_server).await;

    let json = get_json(&server, "/api/network/topology").await;
    let connections = json["connections"].as_array().unwrap();
    assert_eq!(connections.len(), 1, "{connections:?}");
    assert_eq!(connections[0]["from_node"], common::node_id_hex(1));
    assert_eq!(connections[0]["to_node"], hex::encode(regular_peer));
    assert_eq!(connections[0]["message_count"], 3);
    assert_eq!(json["network_stats"]["total_connections"], 1);
    assert_eq!(json["network_stats"]["block_announcements_last_hour"], 4);
    assert_eq!(json["network_stats"]["active_nodes"], 1);
}

/// A block a node authored belongs to the slot of the Authoring event its
/// Authored event refers to.
#[tokio::test]
#[ignore = "bug: /api/slots links Authored to Authoring by created_at, not authoring_id"]
async fn test_slot_events() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let mut node = common::connect_test_node(telemetry_port, 1, &telemetry_server).await;

    let authoring_at = common::now_jce_micros() - SLOT_MICROS;
    let authoring_id =
        common::send_events(&mut node, &[common::authoring_event(authoring_at, 500)]).await[0];
    common::send_events(
        &mut node,
        &[common::authored_event(
            authoring_at + 300 * MICROS_PER_MILLI,
            authoring_id,
        )],
    )
    .await;
    common::flush_all(&telemetry_server).await;

    let json = get_json(&server, "/api/slots/500").await;
    assert_eq!(json["slot"], 500);
    assert_eq!(json["authoring_attempts"], 1);
    assert_eq!(json["authoring_failures"], 0);
    assert_eq!(json["blocks_authored"], 1);
    assert_eq!(json["total_events"], 2);
    assert_eq!(json["nodes_involved"], 1);

    let empty = get_json(&server, "/api/slots/501").await;
    assert_eq!(empty["slot"], 501);
    assert_eq!(empty["total_events"], 0);
}

// --- Node-specific endpoints (require connected node) ---

/// Chain status reports the node's highest best and finalized slots, the hash
/// of its latest best block, and per-type event counts.
#[tokio::test]
async fn test_node_status() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let mut node = common::connect_test_node(telemetry_port, 10, &telemetry_server).await;

    let slot_776_at = common::now_jce_micros() - 2 * SLOT_MICROS;
    let mut events = vec![status_with_peers(slot_776_at - MICROS_PER_MILLI, 12, 9, 3)];
    events.extend(best_blocks(776, slot_776_at, 2));
    events.push(common::finalized_block_event_with_hash(
        slot_776_at + SLOT_MICROS + 80 * MICROS_PER_MILLI,
        775,
        common::block_hash(775),
    ));
    common::send_events(&mut node, &events).await;
    common::flush_all(&telemetry_server).await;

    let json = get_json(
        &server,
        &format!("/api/nodes/{}/status", common::node_id_hex(10)),
    )
    .await;
    let chain = &json["chain_status"];
    assert_eq!(chain["best_slot"], 777);
    assert_eq!(chain["finalized_slot"], 775);
    assert_eq!(chain["best_hash"], hex::encode(common::block_hash(777)));
    assert_eq!(
        chain["finalized_hash"],
        hex::encode(common::block_hash(775))
    );
    assert_eq!(chain["best_block_events"], 2);
    assert_eq!(chain["finalized_block_events"], 1);
    assert_eq!(json["node"]["is_connected"], true);
    assert_eq!(json["node"]["event_count"], 4);

    let mut breakdown: Vec<(u64, u64)> = json["event_breakdown"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry["event_type"].as_u64().unwrap(),
                entry["count"].as_u64().unwrap(),
            )
        })
        .collect();
    breakdown.sort();
    assert_eq!(breakdown, [(10, 1), (11, 2), (12, 1)]);
}

/// Sends a work package's journey through a guarantor: submitted by a builder,
/// received for `core`, then either guaranteed or failed during authorization.
async fn send_work_package(
    node: &mut common::TestNode,
    submitted_at: u64,
    core: u16,
    guaranteed: bool,
) {
    let builder = [0x50; 32];
    let submission_id =
        common::send_events(node, &[common::wp_submission_event(submitted_at, builder)]).await[0];
    let step = |millis: u64| submitted_at + millis * MICROS_PER_MILLI;
    let mut events = vec![common::wp_received_event(step(20), submission_id, core)];
    if guaranteed {
        events.extend([
            common::authorized_event(step(60), submission_id),
            common::refined_event(step(900), submission_id),
            common::work_report_built_event(step(950), submission_id),
            common::guarantee_built_event(step(1000), submission_id),
        ]);
    } else {
        events.push(common::wp_failed_event_with_reason(
            step(60),
            submission_id,
            "authorization failed",
        ));
    }
    common::send_events(node, &events).await;
}

/// A guarantor's assigned core is the core it received most work packages for;
/// every core it worked on is listed, busiest first.
#[tokio::test]
async fn test_node_status_enhanced() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let mut node = common::connect_test_node(telemetry_port, 11, &telemetry_server).await;

    let first_at = common::now_jce_micros() - 60 * SLOT_MICROS;
    send_work_package(&mut node, first_at, 5, false).await;
    send_work_package(&mut node, first_at + 30 * SLOT_MICROS, 3, true).await;
    send_work_package(&mut node, first_at + 31 * SLOT_MICROS, 3, true).await;
    common::flush_all(&telemetry_server).await;

    let json = get_json(
        &server,
        &format!("/api/nodes/{}/status/enhanced", common::node_id_hex(11)),
    )
    .await;
    assert_eq!(json["assigned_core"], 3);
    assert_eq!(json["cores_active"], json!([3, 5]));
    assert_eq!(json["guarantee_activity"]["guarantees_built"], 2);
    assert_eq!(json["guarantee_activity"]["guarantees_signed"], 2);
    assert_eq!(json["guarantee_activity"]["primary_core"], 3);
    assert_eq!(json["ticket_activity"], Value::Null);
    assert_eq!(json["is_connected"], true);
}

/// Peer counts come from the node's latest Status event; non-validator peers
/// are all peers minus validator peers; history lists every Status, newest first.
#[tokio::test]
async fn test_node_peers() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let mut node = common::connect_test_node(telemetry_port, 12, &telemetry_server).await;

    let earlier_at = common::now_jce_micros() - SLOT_MICROS;
    common::send_events(
        &mut node,
        &[
            status_with_peers(earlier_at, 28, 19, 6),
            status_with_peers(earlier_at + 2_000 * MICROS_PER_MILLI, 30, 20, 4),
        ],
    )
    .await;
    common::flush_all(&telemetry_server).await;

    let json = get_json(
        &server,
        &format!("/api/nodes/{}/peers", common::node_id_hex(12)),
    )
    .await;
    assert_eq!(json["total_peers"], 30);
    assert_eq!(json["validator_peers"], 20);
    assert_eq!(json["non_validator_peers"], 10);
    assert_eq!(json["sync_peers"], 4);
    assert_eq!(json["block_announcement_peers"], 0);
    let history: Vec<(u64, u64, u64)> = json["history"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| {
            (
                entry["total_peers"].as_u64().unwrap(),
                entry["validator_peers"].as_u64().unwrap(),
                entry["sync_peers"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(history, [(30, 20, 4), (28, 19, 6)]);
}

fn event_ids(timeline: &Value) -> Vec<u64> {
    timeline["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| event["event_id"].as_u64().unwrap())
        .collect()
}

/// The timeline lists a node's events newest first, counts them per category,
/// and a category filter keeps only that category's events.
#[tokio::test]
async fn test_node_timeline() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let mut node = common::connect_test_node(telemetry_port, 13, &telemetry_server).await;

    let slot_900_at = common::now_jce_micros() - 2 * SLOT_MICROS;
    let step = |millis: u64| slot_900_at + millis * MICROS_PER_MILLI;
    common::send_events(
        &mut node,
        &[
            common::best_block_event_with_hash(step(0), 900, common::block_hash(900)),
            status_with_peers(step(1_500), 12, 9, 3),
        ],
    )
    .await;
    let submission_id = common::send_events(
        &mut node,
        &[common::wp_submission_event(step(2_000), [0x50; 32])],
    )
    .await[0];
    common::send_events(
        &mut node,
        &[
            common::wp_received_event(step(2_020), submission_id, 2),
            common::best_block_event_with_hash(step(6_000), 901, common::block_hash(901)),
        ],
    )
    .await;
    common::flush_all(&telemetry_server).await;

    let timeline_path = format!("/api/nodes/{}/timeline", common::node_id_hex(13));
    let json = get_json(&server, &timeline_path).await;
    assert_eq!(event_ids(&json), [4, 3, 2, 1, 0]);
    assert_eq!(json["event_count"], 5);
    assert_eq!(
        json["category_counts"],
        json!([
            {"category": "status", "count": 3},
            {"category": "workPackage", "count": 2},
        ])
    );

    let work_packages = get_json(&server, &format!("{timeline_path}?categories=workPackage")).await;
    assert_eq!(event_ids(&work_packages), [3, 2]);
}

// --- JAM RPC endpoints (no RPC configured → 503) ---

#[tokio::test]
async fn test_jam_stats_no_rpc() {
    let (server, _, _, _) = setup_test_api().await;
    let response = server.get("/api/jam/stats").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_jam_services_no_rpc() {
    let (server, _, _, _) = setup_test_api().await;
    let response = server.get("/api/jam/services").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn test_jam_cores_no_rpc() {
    let (server, _, _, _) = setup_test_api().await;
    let response = server.get("/api/jam/cores").await;
    assert_eq!(response.status_code(), StatusCode::SERVICE_UNAVAILABLE);
}

/// A node's event count survives a disconnect and keeps growing after it
/// reconnects, counting each event once.
#[tokio::test]
#[ignore = "bug: node event_count counted twice after disconnect"]
async fn test_node_event_count_reconnect_cycle() {
    let (server, telemetry_server, telemetry_port, _) = setup_test_api().await;
    let node_path = format!("/api/nodes/{}", common::node_id_hex(6));

    let slot_1_at = common::now_jce_micros() - 10 * SLOT_MICROS;
    let mut node = common::connect_test_node(telemetry_port, 6, &telemetry_server).await;
    common::send_events(&mut node, &best_blocks(1, slot_1_at, 4)).await;
    common::flush_all(&telemetry_server).await;
    assert_eq!(get_json(&server, &node_path).await["event_count"], 4);

    drop(node);
    telemetry_server.wait_for_connections(0).await;
    common::flush_and_wait(&telemetry_server).await;
    let disconnected = get_json(&server, &node_path).await;
    assert_eq!(disconnected["is_connected"], false);
    assert_eq!(disconnected["event_count"], 4);

    let mut node = common::connect_test_node(telemetry_port, 6, &telemetry_server).await;
    common::send_events(&mut node, &best_blocks(5, slot_1_at + 4 * SLOT_MICROS, 2)).await;
    common::flush_all(&telemetry_server).await;
    assert_eq!(get_json(&server, &node_path).await["event_count"], 6);
}
