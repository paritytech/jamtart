// Integration tests for the incremental raw-events retention job
// (migrations/008_incremental_retention.sql).
//
// Require PostgreSQL/TimescaleDB — see tests/README.md. Run serially:
//   cargo test --test retention_tests -- --test-threads=1

mod common;

use std::sync::Arc;
use tart_backend::EventStore;

/// Connect, run migrations, wipe data, and pause the scheduled retention job
/// so tests can CALL the procedure deterministically.
///
/// Each test re-enables the job on success; a panicked run leaves it paused,
/// which the next setup() simply repeats — harmless on a test database.
async fn setup() -> (Arc<EventStore>, sqlx::PgPool) {
    let database_url = common::test_database_url();

    let store = Arc::new(
        EventStore::new(&database_url)
            .await
            .expect("DB connection failed"),
    );
    store.cleanup_test_data().await.expect("Cleanup failed");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(3)
        .connect(&database_url)
        .await
        .expect("Pool connection failed");

    sqlx::query(
        "SELECT alter_job(job_id, scheduled => false) FROM timescaledb_information.jobs \
         WHERE proc_name = 'tart_incremental_retention'",
    )
    .execute(&pool)
    .await
    .expect("Failed to pause retention job");

    (store, pool)
}

async fn reenable_job(pool: &sqlx::PgPool) {
    sqlx::query(
        "SELECT alter_job(job_id, scheduled => true) FROM timescaledb_information.jobs \
         WHERE proc_name = 'tart_incremental_retention'",
    )
    .execute(pool)
    .await
    .expect("Failed to re-enable retention job");
}

/// Insert `count` raw events aged `age` (an interval like '3 hours'), spread
/// across 8 node ids so several space partitions get chunks.
async fn insert_events(pool: &sqlx::PgPool, age: &str, count: i64, tag: &str) {
    sqlx::query(
        "INSERT INTO ingested_raw_events (timestamp, node_id, event_id, event_type, data) \
         SELECT now() - $1::interval, $2 || (i % 8)::text, i, 10, '{}'::jsonb \
         FROM generate_series(1, $3) i",
    )
    .bind(age)
    .bind(tag)
    .bind(count)
    .execute(pool)
    .await
    .expect("Failed to insert test events");
}

/// Chunks of ingested_raw_events eligible for the 1-hour retention window.
async fn eligible_chunks(pool: &sqlx::PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT format('%I.%I', chunk_schema, chunk_name) \
         FROM timescaledb_information.chunks \
         WHERE hypertable_name = 'ingested_raw_events' \
           AND range_end <= now() - interval '1 hour' \
         ORDER BY range_end, chunk_name",
    )
    .fetch_all(pool)
    .await
    .expect("Failed to list chunks")
}

/// CALL the retention procedure the way the scheduler would.
/// raw_sql uses the simple query protocol, which permits the procedure's
/// internal COMMITs (no surrounding transaction).
async fn run_retention(pool: &sqlx::PgPool, lock_timeout: &str) {
    let call = format!(
        "CALL tart_incremental_retention(0, '{{\
            \"hypertable\": \"ingested_raw_events\", \
            \"drop_after\": \"1 hour\", \
            \"lock_timeout\": \"{lock_timeout}\", \
            \"max_runtime\": \"1 minute\"}}'::jsonb)"
    );
    sqlx::raw_sql(&call)
        .execute(pool)
        .await
        .expect("CALL tart_incremental_retention failed");
}

#[tokio::test]
async fn incremental_retention_replaces_policy_and_drops_only_old_chunks() {
    let (_store, pool) = setup().await;

    // Migration 008 must have removed the built-in policy...
    let builtin: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM timescaledb_information.jobs \
         WHERE proc_name = 'policy_retention' AND hypertable_name = 'ingested_raw_events'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(builtin, 0, "built-in retention policy should be removed");

    // Compression is unnecessary with a one-hour retention window. Migration
    // 008 must remove the old policy and disable the hypertable setting.
    let compression_jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM timescaledb_information.jobs \
         WHERE proc_name = 'policy_compression' \
           AND hypertable_name = 'ingested_raw_events'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(compression_jobs, 0, "compression policy should be removed");

    let compression_enabled: bool = sqlx::query_scalar(
        "SELECT compression_enabled FROM timescaledb_information.hypertables \
         WHERE hypertable_schema = 'public' \
           AND hypertable_name = 'ingested_raw_events'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !compression_enabled,
        "compression should be disabled on the raw-events hypertable"
    );

    // The migration must also register the custom job with the same window.
    let custom: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM timescaledb_information.jobs \
         WHERE proc_name = 'tart_incremental_retention' \
           AND config ->> 'hypertable' = 'ingested_raw_events' \
           AND config ->> 'drop_after' = '1 hour'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(custom, 1, "incremental retention job should be registered");

    insert_events(&pool, "3 hours", 800, "ret-old-").await;
    insert_events(&pool, "0 seconds", 800, "ret-new-").await;

    assert!(
        !eligible_chunks(&pool).await.is_empty(),
        "old inserts should create retention-eligible chunks"
    );

    run_retention(&pool, "2s").await;

    assert!(
        eligible_chunks(&pool).await.is_empty(),
        "all eligible chunks should be dropped"
    );
    let old_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingested_raw_events WHERE timestamp <= now() - interval '1 hour'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        old_rows, 0,
        "no rows older than the retention window remain"
    );
    let new_rows: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM ingested_raw_events WHERE node_id LIKE 'ret-new-%'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(new_rows, 800, "fresh rows must be untouched");

    reenable_job(&pool).await;
}

#[tokio::test]
async fn incremental_retention_skips_locked_chunk_and_recovers() {
    let (_store, pool) = setup().await;

    // Two distinct time slices → guaranteed distinct chunks, oldest tried first.
    insert_events(&pool, "5 hours", 400, "ret-lock-").await;
    insert_events(&pool, "3 hours", 400, "ret-old-").await;

    let before = eligible_chunks(&pool).await;
    assert!(
        before.len() > 1,
        "need several eligible chunks, got {before:?}"
    );

    // Hold an AccessShareLock on one 5h-old chunk in an open transaction,
    // simulating a long-running browsing query.
    let locked_chunk: String = sqlx::query_scalar(
        "SELECT format('%I.%I', chunk_schema, chunk_name) \
         FROM timescaledb_information.chunks \
         WHERE hypertable_name = 'ingested_raw_events' \
           AND range_end <= now() - interval '4 hours' \
         ORDER BY range_end, chunk_name LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .expect("expected a 5h-old chunk");

    let mut tx = pool.begin().await.unwrap();
    sqlx::query(&format!("SELECT count(*) FROM {locked_chunk}"))
        .fetch_one(&mut *tx)
        .await
        .unwrap();

    // Short lock_timeout keeps the test fast; the job must skip the locked
    // chunk, drop everything else, and complete without error.
    run_retention(&pool, "500ms").await;

    let remaining = eligible_chunks(&pool).await;
    assert_eq!(
        remaining,
        vec![locked_chunk.clone()],
        "only the locked chunk should survive"
    );

    // Lock released ("the storm is over") → the next run finishes the job.
    tx.rollback().await.unwrap();
    run_retention(&pool, "500ms").await;

    assert!(
        eligible_chunks(&pool).await.is_empty(),
        "retention should self-heal once the lock is gone"
    );

    reenable_job(&pool).await;
}
