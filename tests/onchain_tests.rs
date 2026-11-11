mod common;

use axum::http::StatusCode;
use axum_test::TestServer;
use chrono::{DateTime, Duration, DurationRound, Utc};
use common::mock_jam_rpc::{Block, MockJamRpc};
use jam_std_common::{CoreActivityRecord, ServiceActivityRecord, Statistics, ValActivityRecord};
use jam_types::{FixedVec, ServiceId, Slot, JAM_COMMON_ERA};
use serde_json::Value;
use std::sync::Arc;
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::onchain_stats::{ingest_until_closed, slot_to_timestamp};
use tart_backend::EventStore;

async fn setup_test_api() -> (TestServer, Arc<EventStore>) {
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
        tart_backend::TelemetryServer::with_options(
            "127.0.0.1:0",
            Some(Arc::clone(&store)),
            true,
            0,
        )
        .await
        .unwrap(),
    );

    let broadcaster = telemetry_server.get_broadcaster();
    let health_monitor = Arc::new(tart_backend::health::HealthMonitor::new());

    let api_state = ApiState {
        store: Arc::clone(&store),
        telemetry_server,
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

    (test_server, store)
}

fn time_range_params() -> String {
    let now = chrono::Utc::now();
    let start = now - chrono::Duration::hours(1);
    let end = now + chrono::Duration::hours(1);
    format!(
        "start={}&end={}",
        start.format("%Y-%m-%dT%H:%M:%SZ"),
        end.format("%Y-%m-%dT%H:%M:%SZ"),
    )
}

const SLOT_PERIOD_SECS: u16 = 6;
const ORPHAN_HASH: u8 = 0xEE;

fn core_count() -> i64 {
    jam_types::core_count() as i64
}

fn slot_at(timestamp: DateTime<Utc>) -> Slot {
    let seconds_since_era = timestamp.timestamp() as u64 - JAM_COMMON_ERA;
    let slot = (seconds_since_era / SLOT_PERIOD_SECS as u64) as Slot;
    assert_eq!(
        slot_to_timestamp(slot, SLOT_PERIOD_SECS),
        timestamp,
        "timestamp is not on a slot boundary"
    );
    slot
}

/// Two consecutive slots inside the previous complete 30m bucket, so
/// `interval=30m` groups them together no matter when the test runs.
fn slots_in_one_30m_bucket() -> (Slot, Slot) {
    let bucket = Duration::minutes(30);
    let bucket_start = Utc::now().duration_trunc(bucket).unwrap() - bucket;
    let slot = slot_at(bucket_start + Duration::minutes(10));
    (slot, slot + 1)
}

fn slot_minutes_ago(minutes: i64) -> Slot {
    let this_minute = Utc::now().duration_trunc(Duration::minutes(1)).unwrap();
    slot_at(this_minute - Duration::minutes(minutes))
}

/// Cores past the end of `cores` report zero activity.
fn block(
    header_hash: u8,
    slot: Slot,
    cores: Vec<CoreActivityRecord>,
    validators: Vec<ValActivityRecord>,
    services: Vec<(ServiceId, ServiceActivityRecord)>,
) -> Block {
    Block {
        header_hash: [header_hash; 32],
        slot,
        statistics: Statistics {
            vals_curr: validators.try_into().unwrap(),
            vals_last: Default::default(),
            cores: FixedVec::padded(&cores),
            services: services.into_iter().collect(),
        },
    }
}

fn cores_block(header_hash: u8, slot: Slot, cores: Vec<CoreActivityRecord>) -> Block {
    block(header_hash, slot, cores, vec![], vec![])
}

/// Validator tallies are running totals for the epoch. A block raises only its
/// author's `blocks`, tickets and preimages, while guarantees and assurances
/// can rise for many validators at once.
fn validators_block(header_hash: u8, slot: Slot, validators: Vec<ValActivityRecord>) -> Block {
    block(header_hash, slot, vec![], validators, vec![])
}

fn services_block(
    header_hash: u8,
    slot: Slot,
    services: Vec<(ServiceId, ServiceActivityRecord)>,
) -> Block {
    block(header_hash, slot, vec![], vec![], services)
}

fn core(gas_used: u64, da_load: u32) -> CoreActivityRecord {
    CoreActivityRecord {
        gas_used,
        da_load,
        popularity: 100,
        imports: 2,
        extrinsic_count: 1,
        extrinsic_size: 256,
        exports: 3,
        bundle_size: 512,
    }
}

fn validator(blocks: u32, guarantees: u32) -> ValActivityRecord {
    ValActivityRecord {
        blocks,
        guarantees,
        ..Default::default()
    }
}

fn service(
    provided_count: u16,
    refinement_count: u32,
    refinement_gas_used: u64,
    accumulate_gas_used: u64,
) -> ServiceActivityRecord {
    ServiceActivityRecord {
        provided_count,
        refinement_count,
        refinement_gas_used,
        accumulate_gas_used,
        ..Default::default()
    }
}

/// Feeds `blocks` through a mock JAM node and returns once all are stored.
async fn ingest(store: &EventStore, blocks: Vec<Block>) {
    let node = MockJamRpc::start(blocks).await;
    ingest_until_closed(node.url(), store.pool(), SLOT_PERIOD_SECS)
        .await
        .expect("ingestion failed");
}

// ─────────────────────────────────────────────────────────────────────────────
// Empty DB smoke tests
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_onchain_cores_timeseries_empty_200() {
    let (server, _store) = setup_test_api().await;

    // Aggregate (no filter)
    let path = format!(
        "/api/grafana/onchain/cores/timeseries?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();
    assert!(body.is_empty());

    // Filtered
    let path = format!(
        "/api/grafana/onchain/cores/timeseries?{}&interval=1m&core=0",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();
    assert!(body.is_empty());
}

#[tokio::test]
async fn test_onchain_validators_timeseries_empty_200() {
    let (server, _store) = setup_test_api().await;

    // Aggregate
    let path = format!(
        "/api/grafana/onchain/validators/timeseries?{}&interval=1m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();
    assert!(body.is_empty());

    // Filtered
    let path = format!(
        "/api/grafana/onchain/validators/timeseries?{}&interval=1m&validator=0",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();
    assert!(body.is_empty());
}

// ─────────────────────────────────────────────────────────────────────────────
// Core timeseries: aggregate vs filtered
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_onchain_cores_timeseries_aggregate() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2) = slots_in_one_30m_bucket();

    ingest(
        &store,
        vec![
            // Core 0: gas=1000, da=100 then gas=2000, da=200
            // Core 1: gas=3000, da=300 then gas=4000, da=400
            // Core 2: gas=5000, da=500 in the first block only
            cores_block(
                1,
                slot1,
                vec![core(1000, 100), core(3000, 300), core(5000, 500)],
            ),
            cores_block(2, slot2, vec![core(2000, 200), core(4000, 400)]),
        ],
    )
    .await;

    // Aggregate — should SUM across all cores per bucket, no `core` field
    let path = format!(
        "/api/grafana/onchain/cores/timeseries?{}&interval=30m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    // With 30m interval, all data falls in one bucket
    assert_eq!(
        body.len(),
        1,
        "Expected 1 aggregate bucket, got {}",
        body.len()
    );
    let row = &body[0];

    // No `core` field in aggregate response
    assert!(
        row.get("core").is_none(),
        "Aggregate should not have `core` field"
    );

    // gas_used = 1000 + 2000 + 3000 + 4000 + 5000 = 15000
    assert_eq!(row["gas_used"].as_i64().unwrap(), 15000);
    // da_load = 100 + 200 + 300 + 400 + 500 = 1500
    assert_eq!(row["da_load"].as_i64().unwrap(), 1500);
}

#[tokio::test]
async fn test_onchain_cores_timeseries_filtered() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2) = slots_in_one_30m_bucket();

    ingest(
        &store,
        vec![
            cores_block(1, slot1, vec![core(1000, 100), core(3000, 300)]),
            cores_block(2, slot2, vec![core(2000, 200), core(4000, 400)]),
        ],
    )
    .await;

    // Filtered to core 1, 30m bucket
    let path = format!(
        "/api/grafana/onchain/cores/timeseries?{}&interval=30m&core=1",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    assert_eq!(body.len(), 1, "Expected 1 bucket for core 1");
    let row = &body[0];

    // Has `core` field
    assert_eq!(row["core"].as_i64().unwrap(), 1);
    // gas_used = 3000 + 4000 = 7000 (only core 1)
    assert_eq!(row["gas_used"].as_i64().unwrap(), 7000);
    // da_load = 300 + 400 = 700
    assert_eq!(row["da_load"].as_i64().unwrap(), 700);
}

// ─────────────────────────────────────────────────────────────────────────────
// Validator timeseries: aggregate vs filtered
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_onchain_validators_timeseries_aggregate() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2) = slots_in_one_30m_bucket();

    ingest(
        &store,
        vec![
            // Authored by validator 2.
            validators_block(
                1,
                slot1,
                vec![validator(1, 5), validator(0, 3), validator(3, 10)],
            ),
            // Authored by validator 1; the network has produced 5 blocks this epoch.
            validators_block(
                2,
                slot2,
                vec![validator(1, 7), validator(1, 6), validator(3, 10)],
            ),
        ],
    )
    .await;

    // Aggregate — SUM across all validators per bucket, no `validator_index` field
    let path = format!(
        "/api/grafana/onchain/validators/timeseries?{}&interval=30m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    assert_eq!(body.len(), 1, "Expected 1 aggregate bucket");
    let row = &body[0];

    // No `validator_index` field
    assert!(
        row.get("validator_index").is_none(),
        "Aggregate should not have `validator_index` field"
    );

    // blocks_produced = (1 + 0 + 3) + (1 + 1 + 3) = 9
    assert_eq!(row["blocks_produced"].as_i64().unwrap(), 9);
    // guarantees = (5 + 3 + 10) + (7 + 6 + 10) = 41
    assert_eq!(row["guarantees"].as_i64().unwrap(), 41);
}

#[tokio::test]
async fn test_onchain_validators_timeseries_filtered() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2) = slots_in_one_30m_bucket();

    ingest(
        &store,
        vec![
            // Authored by validator 1.
            validators_block(1, slot1, vec![validator(1, 5), validator(1, 3)]),
            // Authored by validator 0.
            validators_block(2, slot2, vec![validator(2, 8), validator(1, 6)]),
        ],
    )
    .await;

    // Filtered to validator 0, 30m bucket
    let path = format!(
        "/api/grafana/onchain/validators/timeseries?{}&interval=30m&validator=0",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    assert_eq!(body.len(), 1, "Expected 1 bucket for validator 0");
    let row = &body[0];

    // Has `validator_index` field
    assert_eq!(row["validator_index"].as_i64().unwrap(), 0);
    // MAX aggregation: blocks_produced = max(1, 2) = 2
    assert_eq!(row["blocks_produced"].as_i64().unwrap(), 2);
    // MAX aggregation: guarantees = max(5, 8) = 8
    assert_eq!(row["guarantees"].as_i64().unwrap(), 8);
}

// ─────────────────────────────────────────────────────────────────────────────
// Core timeseries: extra columns
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_onchain_cores_timeseries_aggregate_extra_columns() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2) = slots_in_one_30m_bucket();

    // Five active core rows with the `core()` defaults: popularity=100,
    // imports=2, extrinsic_count=1, extrinsic_size=256, exports=3,
    // bundle_size=512; core 0 of the first block has popularity=964.
    ingest(
        &store,
        vec![
            cores_block(
                1,
                slot1,
                vec![
                    CoreActivityRecord {
                        popularity: 964,
                        ..core(1000, 100)
                    },
                    core(3000, 300),
                    core(5000, 500),
                ],
            ),
            cores_block(2, slot2, vec![core(2000, 200), core(4000, 400)]),
        ],
    )
    .await;

    let path = format!(
        "/api/grafana/onchain/cores/timeseries?{}&interval=30m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();
    assert_eq!(body.len(), 1);
    let row = &body[0];

    // AVG over every core of both blocks: (964 + 4 x 100) / (2 x 341) = 2
    assert_eq!(row["popularity"].as_i64().unwrap(), 2);
    // imports=2 x 5 = 10
    assert_eq!(row["imports"].as_i64().unwrap(), 10);
    // extrinsic_count=1 x 5 = 5
    assert_eq!(row["extrinsic_count"].as_i64().unwrap(), 5);
    // extrinsic_size=256 x 5 = 1280
    assert_eq!(row["extrinsic_size"].as_i64().unwrap(), 1280);
    // exports=3 x 5 = 15
    assert_eq!(row["exports"].as_i64().unwrap(), 15);
    // bundle_size=512 x 5 = 2560
    assert_eq!(row["bundle_size"].as_i64().unwrap(), 2560);
}

#[tokio::test]
async fn test_onchain_cores_timeseries_filtered_extra_columns() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2) = slots_in_one_30m_bucket();

    ingest(
        &store,
        vec![
            cores_block(1, slot1, vec![core(1000, 100), core(3000, 300)]),
            cores_block(2, slot2, vec![core(2000, 200), core(4000, 400)]),
        ],
    )
    .await;

    let path = format!(
        "/api/grafana/onchain/cores/timeseries?{}&interval=30m&core=1",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();
    assert_eq!(body.len(), 1);
    let row = &body[0];

    assert_eq!(row["core"].as_i64().unwrap(), 1);
    // core 1: 2 rows -> imports=4, extrinsic_count=2, extrinsic_size=512,
    // exports=6, bundle_size=1024.
    assert_eq!(row["imports"].as_i64().unwrap(), 4);
    assert_eq!(row["extrinsic_count"].as_i64().unwrap(), 2);
    assert_eq!(row["extrinsic_size"].as_i64().unwrap(), 512);
    assert_eq!(row["exports"].as_i64().unwrap(), 6);
    assert_eq!(row["bundle_size"].as_i64().unwrap(), 1024);
}

// ─────────────────────────────────────────────────────────────────────────────
// Summary and detail endpoints
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_onchain_cores_summary() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2) = (slot_minutes_ago(10), slot_minutes_ago(5));

    ingest(
        &store,
        vec![
            cores_block(
                ORPHAN_HASH,
                slot1,
                vec![CoreActivityRecord {
                    popularity: 400,
                    ..core(9000, 900)
                }],
            ),
            cores_block(1, slot1, vec![core(1000, 100), core(3000, 300)]),
            cores_block(2, slot2, vec![core(2000, 200), core(4000, 400)]),
        ],
    )
    .await;

    let path = format!("/api/grafana/onchain/cores?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    // Every block reports every core; one row per core, ascending by core index.
    let cores: Vec<i64> = body
        .iter()
        .map(|row| row["core"].as_i64().unwrap())
        .collect();
    assert_eq!(cores, (0..core_count()).collect::<Vec<_>>());
    // Orphaned block excluded. Core 0: gas = 1000 + 2000 = 3000, da = 300.
    assert_eq!(body[0]["gas_used"].as_i64().unwrap(), 3000);
    assert_eq!(body[0]["da_load"].as_i64().unwrap(), 300);
    assert_eq!(body[0]["popularity_avg"].as_i64().unwrap(), 100);
    // Core 1: gas = 3000 + 4000 = 7000, da = 700.
    assert_eq!(body[1]["gas_used"].as_i64().unwrap(), 7000);
    assert_eq!(body[1]["da_load"].as_i64().unwrap(), 700);
    assert_eq!(body[2]["gas_used"].as_i64().unwrap(), 0);
}

#[tokio::test]
async fn test_onchain_core_detail() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2, slot3) = (
        slot_minutes_ago(10),
        slot_minutes_ago(5),
        slot_minutes_ago(0),
    );

    // Only the three best-chain core-0 rows should come back.
    ingest(
        &store,
        vec![
            cores_block(ORPHAN_HASH, slot1, vec![core(9000, 900)]),
            cores_block(1, slot1, vec![core(1000, 100), core(5000, 500)]),
            cores_block(2, slot2, vec![core(2000, 200)]),
            cores_block(3, slot3, vec![core(3000, 300)]),
        ],
    )
    .await;

    let path = format!("/api/grafana/onchain/cores/0?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    assert_eq!(body.len(), 3, "expected 3 rows, got {}", body.len());
    // Newest first.
    assert_eq!(body[0]["slot"].as_i64().unwrap(), slot3 as i64);
    assert_eq!(body[1]["slot"].as_i64().unwrap(), slot2 as i64);
    assert_eq!(body[2]["slot"].as_i64().unwrap(), slot1 as i64);
    assert_eq!(body[0]["gas_used"].as_i64().unwrap(), 3000);
    assert_eq!(body[2]["gas_used"].as_i64().unwrap(), 1000);
    assert_eq!(body[0]["core"].as_i64().unwrap(), 0);
}

#[tokio::test]
async fn test_onchain_services_summary() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2) = (slot_minutes_ago(10), slot_minutes_ago(5));

    ingest(
        &store,
        vec![
            services_block(ORPHAN_HASH, slot1, vec![(10, service(9, 9, 900, 9))]),
            services_block(
                1,
                slot1,
                vec![(10, service(1, 2, 50, 70)), (20, service(1, 1, 10, 9))],
            ),
            services_block(2, slot2, vec![(10, service(1, 2, 50, 70))]),
        ],
    )
    .await;

    let path = format!("/api/grafana/onchain/services?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    assert_eq!(body.len(), 2, "expected 2 services, got {}", body.len());
    // Ascending by service_id.
    assert_eq!(body[0]["service_id"].as_str().unwrap(), "0x0000000a");
    assert_eq!(body[1]["service_id"].as_str().unwrap(), "0x00000014");
    // Service 10: provided_count=2, refinement_count=4, refinement_gas=100.
    assert_eq!(body[0]["provided_count"].as_i64().unwrap(), 2);
    assert_eq!(body[0]["refinement_count"].as_i64().unwrap(), 4);
    assert_eq!(body[0]["refinement_gas"].as_i64().unwrap(), 100);
    assert_eq!(body[0]["accumulate_gas"].as_i64().unwrap(), 140);
}

#[tokio::test]
async fn test_onchain_services_timeseries() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2) = slots_in_one_30m_bucket();

    ingest(
        &store,
        vec![
            services_block(
                1,
                slot1,
                vec![(10, service(0, 0, 50, 70)), (20, service(0, 0, 10, 9))],
            ),
            services_block(2, slot2, vec![(10, service(0, 0, 50, 70))]),
        ],
    )
    .await;

    let path = format!(
        "/api/grafana/onchain/services/timeseries?{}&interval=30m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    // Grouped by ts + service_id -> 2 rows in one bucket.
    assert_eq!(body.len(), 2, "expected 2 rows, got {}", body.len());
    assert_eq!(body[0]["service_id"].as_str().unwrap(), "0x0000000a");
    assert_eq!(body[1]["service_id"].as_str().unwrap(), "0x00000014");
    assert_eq!(body[0]["refinement_gas"].as_i64().unwrap(), 100);
    assert_eq!(body[0]["accumulate_gas"].as_i64().unwrap(), 140);
}

#[tokio::test]
async fn test_onchain_service_detail() {
    let (server, store) = setup_test_api().await;

    let (slot1, slot2, slot3) = (
        slot_minutes_ago(10),
        slot_minutes_ago(5),
        slot_minutes_ago(0),
    );

    // Only the three best-chain service-10 rows should come back.
    ingest(
        &store,
        vec![
            services_block(ORPHAN_HASH, slot1, vec![(10, service(0, 0, 900, 0))]),
            services_block(
                1,
                slot1,
                vec![(10, service(0, 0, 50, 0)), (20, service(0, 0, 10, 0))],
            ),
            services_block(2, slot2, vec![(10, service(0, 0, 60, 0))]),
            services_block(3, slot3, vec![(10, service(0, 0, 70, 0))]),
        ],
    )
    .await;

    let path = format!(
        "/api/grafana/onchain/services/0x0000000a?{}",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    assert_eq!(body.len(), 3, "expected 3 rows, got {}", body.len());
    // Newest first.
    assert_eq!(body[0]["slot"].as_i64().unwrap(), slot3 as i64);
    assert_eq!(body[1]["slot"].as_i64().unwrap(), slot2 as i64);
    assert_eq!(body[2]["slot"].as_i64().unwrap(), slot1 as i64);
    assert_eq!(body[0]["refinement_gas"].as_i64().unwrap(), 70);
    assert_eq!(body[2]["refinement_gas"].as_i64().unwrap(), 50);
    assert_eq!(body[0]["service_id"].as_str().unwrap(), "0x0000000a");

    // Decimal form must resolve to the same service.
    let path = format!("/api/grafana/onchain/services/10?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body2: Vec<Value> = response.json();
    assert_eq!(body2.len(), 3);

    // Invalid service id -> BAD_REQUEST.
    let response = server
        .get(&format!(
            "/api/grafana/onchain/services/zzz?{}",
            time_range_params()
        ))
        .await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_onchain_validators_summary() {
    let (server, store) = setup_test_api().await;

    let slot1 = slot_minutes_ago(10);
    let slot2 = slot1 + 1;

    let tallies =
        |blocks, tickets, preimages, preimages_size, guarantees, assurances| ValActivityRecord {
            blocks,
            tickets,
            preimages,
            preimages_size,
            guarantees,
            assurances,
        };
    ingest(
        &store,
        vec![
            // Fork authored by validator 1.
            validators_block(
                ORPHAN_HASH,
                slot1,
                vec![tallies(0, 0, 0, 0, 4, 6), tallies(3, 5, 6, 60, 5, 3)],
            ),
            // Authored by validator 0, on the same parent as the fork.
            validators_block(
                1,
                slot1,
                vec![tallies(1, 2, 3, 30, 5, 6), tallies(2, 1, 1, 10, 3, 2)],
            ),
            // Authored by validator 0.
            validators_block(
                2,
                slot2,
                vec![tallies(2, 5, 6, 60, 8, 9), tallies(2, 1, 1, 10, 3, 2)],
            ),
        ],
    )
    .await;

    let path = format!("/api/grafana/onchain/validators?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    assert_eq!(body.len(), 2, "expected 2 validators, got {}", body.len());
    assert_eq!(body[0]["validator_index"].as_i64().unwrap(), 0);
    assert_eq!(body[1]["validator_index"].as_i64().unwrap(), 1);
    // MAX semantics: validator 0 blocks=max(1,2)=2, tickets=max(2,5)=5,
    // preimages=max(3,6)=6, preimages_size=max(30,60)=60, guarantees=max(5,8)=8,
    // assurances=max(6,9)=9.
    assert_eq!(body[0]["blocks_produced"].as_i64().unwrap(), 2);
    assert_eq!(body[0]["tickets"].as_i64().unwrap(), 5);
    assert_eq!(body[0]["preimages"].as_i64().unwrap(), 6);
    assert_eq!(body[0]["preimages_size"].as_i64().unwrap(), 60);
    assert_eq!(body[0]["guarantees"].as_i64().unwrap(), 8);
    assert_eq!(body[0]["assurances"].as_i64().unwrap(), 9);
    // The fork's tallies for validator 1 (3 blocks, 5 tickets, 5 guarantees) are excluded.
    assert_eq!(body[1]["blocks_produced"].as_i64().unwrap(), 2);
    assert_eq!(body[1]["tickets"].as_i64().unwrap(), 1);
    assert_eq!(body[1]["guarantees"].as_i64().unwrap(), 3);
}

#[tokio::test]
async fn test_onchain_validator_detail() {
    let (server, store) = setup_test_api().await;

    let slot1 = slot_minutes_ago(10);
    let (slot2, slot3) = (slot1 + 1, slot1 + 2);

    let tallies = |blocks, tickets, assurances| ValActivityRecord {
        blocks,
        tickets,
        assurances,
        ..Default::default()
    };
    // Only the three best-chain validator-0 rows should come back.
    ingest(
        &store,
        vec![
            // Authors: fork validator 1, then validators 0, 1, 0.
            validators_block(ORPHAN_HASH, slot1, vec![tallies(0, 0, 6), tallies(3, 3, 3)]),
            validators_block(1, slot1, vec![tallies(1, 2, 6), tallies(2, 1, 3)]),
            validators_block(2, slot2, vec![tallies(1, 2, 7), tallies(3, 3, 4)]),
            validators_block(3, slot3, vec![tallies(2, 5, 8), tallies(3, 3, 5)]),
        ],
    )
    .await;

    let path = format!("/api/grafana/onchain/validators/0?{}", time_range_params());
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    assert_eq!(body.len(), 3, "expected 3 rows, got {}", body.len());
    // Newest first.
    assert_eq!(body[0]["slot"].as_i64().unwrap(), slot3 as i64);
    assert_eq!(body[1]["slot"].as_i64().unwrap(), slot2 as i64);
    assert_eq!(body[2]["slot"].as_i64().unwrap(), slot1 as i64);
    assert_eq!(body[0]["blocks_produced"].as_i64().unwrap(), 2);
    assert_eq!(body[0]["tickets"].as_i64().unwrap(), 5);
    assert_eq!(body[0]["assurances"].as_i64().unwrap(), 8);
    assert_eq!(body[2]["blocks_produced"].as_i64().unwrap(), 1);
}
