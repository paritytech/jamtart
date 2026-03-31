mod common;

use axum::http::StatusCode;
use axum_test::TestServer;
use serde_json::Value;
use std::sync::Arc;
use tart_backend::api::{create_api_router, ApiState};
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
        tart_backend::TelemetryServer::with_options("127.0.0.1:0", Some(Arc::clone(&store)), true, 0)
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
        cache: Arc::new(tart_backend::cache::TtlCache::new(std::time::Duration::ZERO)),
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

/// Insert test rows into onchain_core_stats.
async fn insert_core_stats(pool: &sqlx::PgPool, ts: &str, slot: i32, core: i16, gas_used: i64, da_load: i32) {
    sqlx::query(
        "INSERT INTO onchain_core_stats (timestamp, slot, header_hash, core, gas_used, da_load, popularity, imports, extrinsic_count, extrinsic_size, exports, bundle_size, on_best_chain) \
         VALUES ($1::timestamptz, $2, '\\x0000000000000000000000000000000000000000000000000000000000000000', $3, $4, $5, 100, 2, 1, 256, 3, 512, true)",
    )
    .bind(ts)
    .bind(slot)
    .bind(core)
    .bind(gas_used)
    .bind(da_load)
    .execute(pool)
    .await
    .expect("Failed to insert core stats");
}

/// Insert test rows into onchain_validator_stats.
async fn insert_validator_stats(pool: &sqlx::PgPool, ts: &str, slot: i32, validator_index: i16, blocks_produced: i32, guarantees: i32) {
    sqlx::query(
        "INSERT INTO onchain_validator_stats (timestamp, slot, header_hash, validator_index, blocks_produced, tickets, preimages, preimages_size, guarantees, assurances, on_best_chain) \
         VALUES ($1::timestamptz, $2, '\\x0000000000000000000000000000000000000000000000000000000000000000', $3, $4, 0, 0, 0, $5, 0, true)",
    )
    .bind(ts)
    .bind(slot)
    .bind(validator_index)
    .bind(blocks_produced)
    .bind(guarantees)
    .execute(pool)
    .await
    .expect("Failed to insert validator stats");
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
    let pool = store.pool();

    let now = chrono::Utc::now();
    let t1 = (now - chrono::Duration::minutes(10)).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let t2 = (now - chrono::Duration::minutes(5)).format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // Core 0: gas=1000, da=100 at t1 and t2
    insert_core_stats(pool, &t1, 100, 0, 1000, 100).await;
    insert_core_stats(pool, &t2, 101, 0, 2000, 200).await;
    // Core 1: gas=3000, da=300 at t1 and t2
    insert_core_stats(pool, &t1, 100, 1, 3000, 300).await;
    insert_core_stats(pool, &t2, 101, 1, 4000, 400).await;
    // Core 2: gas=5000, da=500 at t1 only
    insert_core_stats(pool, &t1, 100, 2, 5000, 500).await;

    // Aggregate — should SUM across all cores per bucket, no `core` field
    let path = format!(
        "/api/grafana/onchain/cores/timeseries?{}&interval=30m",
        time_range_params()
    );
    let response = server.get(&path).await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: Vec<Value> = response.json();

    // With 30m interval, all data falls in one bucket
    assert_eq!(body.len(), 1, "Expected 1 aggregate bucket, got {}", body.len());
    let row = &body[0];

    // No `core` field in aggregate response
    assert!(row.get("core").is_none(), "Aggregate should not have `core` field");

    // gas_used = 1000 + 2000 + 3000 + 4000 + 5000 = 15000
    assert_eq!(row["gas_used"].as_i64().unwrap(), 15000);
    // da_load = 100 + 200 + 300 + 400 + 500 = 1500
    assert_eq!(row["da_load"].as_i64().unwrap(), 1500);
}

#[tokio::test]
async fn test_onchain_cores_timeseries_filtered() {
    let (server, store) = setup_test_api().await;
    let pool = store.pool();

    let now = chrono::Utc::now();
    let t1 = (now - chrono::Duration::minutes(10)).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let t2 = (now - chrono::Duration::minutes(5)).format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // Core 0
    insert_core_stats(pool, &t1, 100, 0, 1000, 100).await;
    insert_core_stats(pool, &t2, 101, 0, 2000, 200).await;
    // Core 1
    insert_core_stats(pool, &t1, 100, 1, 3000, 300).await;
    insert_core_stats(pool, &t2, 101, 1, 4000, 400).await;

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
    let pool = store.pool();

    let now = chrono::Utc::now();
    let t1 = (now - chrono::Duration::minutes(10)).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let t2 = (now - chrono::Duration::minutes(5)).format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // Validator 0: blocks_produced=1, guarantees=5 at t1; blocks=2, guarantees=8 at t2
    insert_validator_stats(pool, &t1, 100, 0, 1, 5).await;
    insert_validator_stats(pool, &t2, 101, 0, 2, 8).await;
    // Validator 1: blocks_produced=0, guarantees=3 at t1; blocks=1, guarantees=6 at t2
    insert_validator_stats(pool, &t1, 100, 1, 0, 3).await;
    insert_validator_stats(pool, &t2, 101, 1, 1, 6).await;
    // Validator 2: blocks_produced=3, guarantees=10 at t1 only
    insert_validator_stats(pool, &t1, 100, 2, 3, 10).await;

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
    assert!(row.get("validator_index").is_none(), "Aggregate should not have `validator_index` field");

    // blocks_produced = 1 + 2 + 0 + 1 + 3 = 7
    assert_eq!(row["blocks_produced"].as_i64().unwrap(), 7);
    // guarantees = 5 + 8 + 3 + 6 + 10 = 32
    assert_eq!(row["guarantees"].as_i64().unwrap(), 32);
}

#[tokio::test]
async fn test_onchain_validators_timeseries_filtered() {
    let (server, store) = setup_test_api().await;
    let pool = store.pool();

    let now = chrono::Utc::now();
    let t1 = (now - chrono::Duration::minutes(10)).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let t2 = (now - chrono::Duration::minutes(5)).format("%Y-%m-%dT%H:%M:%SZ").to_string();

    // Validator 0
    insert_validator_stats(pool, &t1, 100, 0, 1, 5).await;
    insert_validator_stats(pool, &t2, 101, 0, 2, 8).await;
    // Validator 1
    insert_validator_stats(pool, &t1, 100, 1, 0, 3).await;
    insert_validator_stats(pool, &t2, 101, 1, 1, 6).await;

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
