//! TimescaleDB-backed event store. Handles schema migrations, bulk inserts of
//! telemetry event records, and time-range queries for the API and Grafana layers.

use crate::batch_writer::EventRecord;
use crate::types::JCE_EPOCH_UNIX_MICROS;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::{postgres::PgPoolOptions, Executor, PgPool, Row};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

/// Shared string type for node IDs (matches batch_writer::NodeId).
type NodeId = Arc<str>;

/// TimescaleDB-backed event store for high-throughput telemetry data.
///
/// Optimized for handling 3,000,000+ events/second from 1024+ concurrent nodes.
/// Features include:
/// - Batch event insertion using PostgreSQL QueryBuilder
/// - TimescaleDB hypertable with automatic chunking (1-hour intervals)
/// - Continuous aggregates for efficient time-series analytics
/// - Automatic compression and retention policies
/// - JSONB storage for flexible event data
///
/// # Example
/// ```no_run
/// use tart_backend::EventStore;
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), sqlx::Error> {
/// let store = Arc::new(EventStore::new("postgres://localhost/tart").await?);
/// store.ping().await?;
/// # Ok(())
/// # }
/// ```
/// event_services row: (unix_micros, node_id, event_type, service_id, gas_used, elapsed_ns, load_ns)
pub type EventServiceRow<'a> = (
    i64,
    &'a str,
    i16,
    i32,
    Option<i64>,
    Option<i64>,
    Option<i64>,
);

/// node_stats row extracted from Status events (unix_micros, node_id, then the
/// numeric Status fields in `node_stats` column order).
pub type NodeStatsRow<'a> = (
    i64,
    &'a str,
    i32,
    i32,
    i32,
    i32,
    i64,
    i32,
    i32,
    i16,
    i16,
    f32,
    i16,
);

/// True if a migration failed as a deadlock victim (SQLSTATE 40P01), e.g. by
/// colliding with a TimescaleDB background policy job.
fn is_deadlock(e: &sqlx::migrate::MigrateError) -> bool {
    use sqlx::migrate::MigrateError;
    match e {
        MigrateError::Execute(sqlx::Error::Database(db_err))
        | MigrateError::ExecuteMigration(sqlx::Error::Database(db_err), _) => {
            db_err.code().as_deref() == Some("40P01")
        }
        _ => false,
    }
}

pub struct EventStore {
    pool: PgPool,       // read pool — API queries + cache warmer
    write_pool: PgPool, // write pool — batch writer, node updates
}

impl EventStore {
    /// Creates a new event store connected to TimescaleDB.
    ///
    /// Automatically runs database migrations on startup.
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string (e.g., "postgres://user:pass@host/db")
    ///
    /// # Errors
    /// Returns `sqlx::Error` if connection fails or migrations cannot be applied.
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        // Read pool: smaller, with statement timeout to kill runaway queries.
        // Serves API queries + cache warmer (15 concurrent queries + HTTP handlers).
        let pool = PgPoolOptions::new()
            .max_connections(30)
            .min_connections(5)
            .acquire_timeout(Duration::from_secs(3))
            .idle_timeout(Duration::from_secs(300))
            .max_lifetime(Duration::from_secs(600))
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    conn.execute("SET statement_timeout = '8s'").await?;
                    Ok(())
                })
            })
            .connect(database_url)
            .await?;

        info!("Read pool connected (30 conns, 8s statement_timeout)");

        // Write pool: larger, no statement timeout (COPY operations take variable time).
        // Serves batch writer workers + node upserts.
        let write_pool = PgPoolOptions::new()
            .max_connections(200)
            .min_connections(20)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(300))
            .max_lifetime(Duration::from_secs(600))
            .connect(database_url)
            .await?;

        info!("Write pool connected (200 conns, no statement_timeout)");

        // Run migrations (using write pool — no timeout constraint).
        // Retry on deadlock: TimescaleDB background jobs (continuous aggregate
        // refresh/retention policies created by earlier migrations) can collide
        // with DROP MATERIALIZED VIEW in later migrations on a fresh database.
        let mut attempt = 0;
        loop {
            match sqlx::migrate!("./migrations").run(&write_pool).await {
                Ok(()) => break,
                Err(e) if attempt < 5 && is_deadlock(&e) => {
                    attempt += 1;
                    warn!(
                        "Migration deadlocked with a TimescaleDB background job, retrying ({}/5): {}",
                        attempt, e
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
                Err(e) => return Err(e.into()),
            }
        }

        info!("Migrations applied successfully");

        Ok(Self { pool, write_pool })
    }

    /// Expose the connection pool for raw queries in API handlers.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Batch insert/update multiple node connections in a single query.
    /// Uses PostgreSQL unnest() for efficient multi-row upsert.
    pub async fn store_nodes_connected_batch(
        &self,
        nodes: &[(NodeId, crate::events::NodeInformation, String)],
    ) -> Result<(), sqlx::Error> {
        if nodes.is_empty() {
            return Ok(());
        }

        let now = Utc::now();

        // Prepare arrays for unnest
        let node_ids: Vec<&str> = nodes.iter().map(|(id, _, _)| &**id).collect();
        let peer_ids: Vec<String> = nodes
            .iter()
            .map(|(_, info, _)| hex::encode(info.details.peer_id))
            .collect();
        let impl_names: Vec<&str> = nodes
            .iter()
            .map(|(_, info, _)| info.implementation_name.as_str().unwrap_or("unknown"))
            .collect();
        let impl_versions: Vec<&str> = nodes
            .iter()
            .map(|(_, info, _)| info.implementation_version.as_str().unwrap_or("unknown"))
            .collect();
        let node_infos: Vec<serde_json::Value> = nodes
            .iter()
            .map(|(_, info, _)| {
                serde_json::to_value(info).unwrap_or_else(|_| serde_json::json!({}))
            })
            .collect();
        let addresses: Vec<&str> = nodes.iter().map(|(_, _, addr)| addr.as_str()).collect();

        sqlx::query(
            r#"
            INSERT INTO nodes (node_id, peer_id, implementation_name, implementation_version,
                             node_info, connected_at, last_seen_at, is_connected, event_count, address)
            SELECT * FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::jsonb[],
                                 $6::timestamptz[], $7::timestamptz[], $8::bool[], $9::bigint[], $10::text[])
            ON CONFLICT(node_id) DO UPDATE SET
                implementation_name = EXCLUDED.implementation_name,
                implementation_version = EXCLUDED.implementation_version,
                node_info = EXCLUDED.node_info,
                last_seen_at = EXCLUDED.last_seen_at,
                is_connected = true,
                address = EXCLUDED.address
            "#,
        )
        .bind(&node_ids)
        .bind(&peer_ids)
        .bind(&impl_names)
        .bind(&impl_versions)
        .bind(&node_infos)
        .bind(vec![now; nodes.len()])
        .bind(vec![now; nodes.len()])
        .bind(vec![true; nodes.len()])
        .bind(vec![0i64; nodes.len()])
        .bind(&addresses)
        .execute(&self.write_pool)
        .await?;

        tracing::trace!("Batch inserted/updated {} node connections", nodes.len());
        Ok(())
    }

    /// Batch update multiple node disconnections in a single query.
    pub async fn store_nodes_disconnected_batch(
        &self,
        node_ids: &[NodeId],
    ) -> Result<(), sqlx::Error> {
        if node_ids.is_empty() {
            return Ok(());
        }

        let now = Utc::now();
        let ids: Vec<&str> = node_ids.iter().map(|s| &**s).collect();

        sqlx::query(
            r#"
            UPDATE nodes
            SET is_connected = false,
                disconnected_at = $1,
                total_events = total_events + event_count
            WHERE node_id = ANY($2::text[])
            "#,
        )
        .bind(now)
        .bind(&ids)
        .execute(&self.write_pool)
        .await?;

        tracing::trace!("Batch disconnected {} nodes", node_ids.len());
        Ok(())
    }

    /// Store events using PostgreSQL COPY BINARY for maximum throughput.
    /// COPY bypasses SQL parsing, and binary format eliminates CSV encoding/parsing
    /// overhead on both client and server side.
    pub async fn store_events_batch(&self, events: Vec<EventRecord>) -> Result<(), sqlx::Error> {
        if events.is_empty() {
            return Ok(());
        }

        // For very small batches, use simple INSERT (COPY has overhead for small batches)
        if events.len() <= 10 {
            return self.store_events_simple(events).await;
        }

        // PostgreSQL epoch: 2000-01-01 00:00:00 UTC in Unix microseconds
        const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;
        const FIELD_COUNT: i16 = 9; // +1 for wp_hash

        // Build binary COPY payload
        let mut buf: Vec<u8> = Vec::with_capacity(19 + events.len() * 280 + 2);

        // Header: 11-byte magic + flags (i32) + header extension length (i32)
        buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        buf.extend_from_slice(&0i32.to_be_bytes()); // flags
        buf.extend_from_slice(&0i32.to_be_bytes()); // header extension length

        for record in &events {
            // Field count
            buf.extend_from_slice(&FIELD_COUNT.to_be_bytes());

            // Column 1: timestamp (TIMESTAMPTZ) — i64 microseconds since PG epoch
            let unix_micros = JCE_EPOCH_UNIX_MICROS + record.event.timestamp() as i64;
            let pg_micros = unix_micros - PG_EPOCH_UNIX_MICROS;
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&pg_micros.to_be_bytes());

            // Column 2: node_id (TEXT) — length + UTF-8 bytes
            let node_bytes = record.node_id.as_bytes();
            buf.extend_from_slice(&(node_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(node_bytes);

            // Column 3: event_id (BIGINT) — i64 big-endian
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&(record.event_id as i64).to_be_bytes());

            // Column 4: event_type (SMALLINT) — i16 big-endian
            let event_type = record.event.event_type() as i16;
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&event_type.to_be_bytes());

            // Column 5: data (JSONB) — version byte (0x01) + pre-serialized JSON bytes
            buf.extend_from_slice(&(record.event_json.len() as i32 + 1).to_be_bytes());
            buf.push(1u8); // JSONB version 1
            buf.extend_from_slice(&record.event_json);

            // Column 6: slot (INT, nullable)
            match record.enriched.slot {
                Some(s) => {
                    buf.extend_from_slice(&4i32.to_be_bytes());
                    buf.extend_from_slice(&(s as i32).to_be_bytes());
                }
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()), // NULL
            }

            // Column 7: core (SMALLINT, nullable)
            match record.enriched.core {
                Some(c) => {
                    buf.extend_from_slice(&2i32.to_be_bytes());
                    buf.extend_from_slice(&(c as i16).to_be_bytes());
                }
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()), // NULL
            }

            // Column 8: submission_id (BIGINT, nullable)
            match record.enriched.submission_id {
                Some(sid) => {
                    buf.extend_from_slice(&8i32.to_be_bytes());
                    buf.extend_from_slice(&(sid as i64).to_be_bytes());
                }
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()), // NULL
            }

            // Column 9: wp_hash (BYTEA, nullable) — 32-byte work package hash from enricher
            match record.enriched.wp_hash {
                Some(ref hash) => {
                    buf.extend_from_slice(&32i32.to_be_bytes());
                    buf.extend_from_slice(hash);
                }
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()), // NULL
            }
        }

        // Trailer: -1 as i16
        buf.extend_from_slice(&(-1i16).to_be_bytes());

        // Send binary payload via COPY
        let mut conn = self.write_pool.acquire().await?;
        let mut copy_in = conn
            .copy_in_raw(
                "COPY ingested_raw_events (timestamp, node_id, event_id, event_type, data, slot, core, submission_id, wp_hash) FROM STDIN WITH (FORMAT binary)",
            )
            .await?;

        copy_in.send(buf.as_slice()).await?;
        let rows_affected = copy_in.finish().await?;

        tracing::trace!(
            "COPY completed: {} events ({} rows affected)",
            events.len(),
            rows_affected
        );
        Ok(())
    }

    /// Simple batch insert for small batches using individual INSERTs in a transaction.
    async fn store_events_simple(&self, events: Vec<EventRecord>) -> Result<(), sqlx::Error> {
        let mut tx = self.write_pool.begin().await?;
        let event_count = events.len();

        for record in events {
            let event_type = record.event.event_type() as i16;
            let unix_timestamp_micros = JCE_EPOCH_UNIX_MICROS + record.event.timestamp() as i64;
            let timestamp =
                DateTime::from_timestamp_micros(unix_timestamp_micros).unwrap_or_else(|| {
                    tracing::warn!(
                        "Invalid event timestamp for node {}: {} (unix micros: {})",
                        record.node_id,
                        record.event.timestamp(),
                        unix_timestamp_micros
                    );
                    Utc::now()
                });
            let event_json: serde_json::Value = serde_json::from_slice(&record.event_json)
                .map_err(|e| sqlx::Error::Encode(Box::new(e)))?;
            let slot = record.enriched.slot.map(|s| s as i32);
            let core = record.enriched.core.map(|c| c as i16);
            let submission_id = record.enriched.submission_id.map(|s| s as i64);

            let wp_hash = record.enriched.wp_hash.map(|h| h.to_vec());

            sqlx::query(
                r#"
                INSERT INTO ingested_raw_events (timestamp, node_id, event_id, event_type, data, slot, core, submission_id, wp_hash)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                "#,
            )
            .bind(timestamp)
            .bind(&*record.node_id)
            .bind(record.event_id as i64)
            .bind(event_type)
            .bind(event_json)
            .bind(slot)
            .bind(core)
            .bind(submission_id)
            .bind(wp_hash)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                tracing::error!(
                    "Failed to insert event in simple batch, rolling back: {}",
                    e
                );
                e
            })?;
        }

        match tx.commit().await {
            Ok(_) => {
                tracing::trace!(
                    "Successfully committed simple batch of {} events",
                    event_count
                );
                Ok(())
            }
            Err(e) => {
                tracing::error!(
                    "Failed to commit simple batch transaction for {} events, rolling back: {}",
                    event_count,
                    e
                );
                Err(e)
            }
        }
    }

    /// Store event_services rows using PostgreSQL COPY for service junction table.
    /// Only called for the 21 low-volume pipeline event types + BlockExecuted.
    pub async fn store_event_services_batch(
        &self,
        rows: &[EventServiceRow<'_>],
    ) -> Result<(), sqlx::Error> {
        if rows.is_empty() {
            return Ok(());
        }

        const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;
        const FIELD_COUNT: i16 = 7;

        let mut buf: Vec<u8> = Vec::with_capacity(19 + rows.len() * 80 + 2);
        buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        buf.extend_from_slice(&0i32.to_be_bytes());
        buf.extend_from_slice(&0i32.to_be_bytes());

        for (unix_micros, node_id, event_type, service_id, gas_used, elapsed_ns, load_ns) in rows {
            buf.extend_from_slice(&FIELD_COUNT.to_be_bytes());

            // timestamp (TIMESTAMPTZ)
            let ts = *unix_micros - PG_EPOCH_UNIX_MICROS;
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&ts.to_be_bytes());

            // node_id (TEXT)
            let node_bytes = node_id.as_bytes();
            buf.extend_from_slice(&(node_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(node_bytes);

            // event_type (SMALLINT)
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&event_type.to_be_bytes());

            // service_id (INT)
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&service_id.to_be_bytes());

            // gas_used (BIGINT, nullable)
            match gas_used {
                Some(g) => {
                    buf.extend_from_slice(&8i32.to_be_bytes());
                    buf.extend_from_slice(&g.to_be_bytes());
                }
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
            }

            // elapsed_ns (BIGINT, nullable)
            match elapsed_ns {
                Some(e) => {
                    buf.extend_from_slice(&8i32.to_be_bytes());
                    buf.extend_from_slice(&e.to_be_bytes());
                }
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
            }

            // load_ns (BIGINT, nullable)
            match load_ns {
                Some(l) => {
                    buf.extend_from_slice(&8i32.to_be_bytes());
                    buf.extend_from_slice(&l.to_be_bytes());
                }
                None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }

        buf.extend_from_slice(&(-1i16).to_be_bytes());

        let mut conn = self.write_pool.acquire().await?;
        let mut copy_in = conn
            .copy_in_raw(
                "COPY event_services (timestamp, node_id, event_type, service_id, gas_used, elapsed_ns, load_ns) FROM STDIN WITH (FORMAT binary)",
            )
            .await?;
        copy_in.send(buf.as_slice()).await?;
        copy_in.finish().await?;
        Ok(())
    }

    /// Store node_stats rows using PostgreSQL COPY for Status event extraction.
    pub async fn store_node_stats_batch(
        &self,
        rows: &[NodeStatsRow<'_>],
    ) -> Result<(), sqlx::Error> {
        if rows.is_empty() {
            return Ok(());
        }

        const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;
        const FIELD_COUNT: i16 = 13;

        let mut buf: Vec<u8> = Vec::with_capacity(19 + rows.len() * 80 + 2);
        buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        buf.extend_from_slice(&0i32.to_be_bytes());
        buf.extend_from_slice(&0i32.to_be_bytes());

        for (
            unix_micros,
            node_id,
            num_peers,
            num_val_peers,
            num_sync_peers,
            num_shards,
            shards_size,
            num_preimages,
            preimages_size,
            min_guarantees,
            max_guarantees,
            avg_guarantees,
            zero_guarantee_cores,
        ) in rows
        {
            buf.extend_from_slice(&FIELD_COUNT.to_be_bytes());

            // timestamp
            let ts = *unix_micros - PG_EPOCH_UNIX_MICROS;
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&ts.to_be_bytes());

            // node_id
            let node_bytes = node_id.as_bytes();
            buf.extend_from_slice(&(node_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(node_bytes);

            // num_peers (INT)
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&num_peers.to_be_bytes());

            // num_val_peers (INT)
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&num_val_peers.to_be_bytes());

            // num_sync_peers (INT)
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&num_sync_peers.to_be_bytes());

            // num_shards (INT)
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&num_shards.to_be_bytes());

            // shards_size (BIGINT)
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&shards_size.to_be_bytes());

            // num_preimages (INT)
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&num_preimages.to_be_bytes());

            // preimages_size (INT)
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&preimages_size.to_be_bytes());

            // min_guarantees (SMALLINT)
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&min_guarantees.to_be_bytes());

            // max_guarantees (SMALLINT)
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&max_guarantees.to_be_bytes());

            // avg_guarantees (REAL / float4)
            buf.extend_from_slice(&4i32.to_be_bytes());
            buf.extend_from_slice(&avg_guarantees.to_be_bytes());

            // zero_guarantee_cores (SMALLINT)
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&zero_guarantee_cores.to_be_bytes());
        }

        buf.extend_from_slice(&(-1i16).to_be_bytes());

        let mut conn = self.write_pool.acquire().await?;
        let mut copy_in = conn
            .copy_in_raw(
                "COPY node_stats (timestamp, node_id, num_peers, num_val_peers, num_sync_peers, num_shards, shards_size, num_preimages, preimages_size, min_guarantees, max_guarantees, avg_guarantees, zero_guarantee_cores) FROM STDIN WITH (FORMAT binary)",
            )
            .await?;
        copy_in.send(buf.as_slice()).await?;
        copy_in.finish().await?;
        Ok(())
    }

    /// Batch update node statistics from application-level counters.
    ///
    /// Replaces the per-row database trigger (which is catastrophic at 3M events/s)
    /// with periodic batch updates from the writer workers.
    /// Multiple concurrent callers are safe since updates are additive.
    pub async fn update_node_stats(
        &self,
        node_counts: &HashMap<NodeId, u64>,
    ) -> Result<(), sqlx::Error> {
        if node_counts.is_empty() {
            return Ok(());
        }

        let now = Utc::now();
        let node_ids: Vec<&str> = node_counts.keys().map(|s| &**s).collect();
        let counts: Vec<i64> = node_counts.values().map(|&c| c as i64).collect();

        // Single UPDATE with unnest() acquires all row locks atomically,
        // preventing deadlocks when multiple writer workers call concurrently.
        sqlx::query(
            r#"
            UPDATE nodes
            SET last_seen_at = $1,
                event_count = event_count + data.cnt
            FROM unnest($2::text[], $3::bigint[]) AS data(nid, cnt)
            WHERE nodes.node_id = data.nid
            "#,
        )
        .bind(now)
        .bind(&node_ids)
        .bind(&counts)
        .execute(&self.write_pool)
        .await?;

        Ok(())
    }

    /// Health metrics for monitoring.
    /// Uses TimescaleDB approximate_row_count() for O(1) event counting.
    pub async fn get_health_metrics(
        &self,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, sqlx::Error> {
        let mut metrics = std::collections::HashMap::new();

        let node_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM nodes WHERE is_connected = true")
                .fetch_one(&self.pool)
                .await?;

        // Use approximate_row_count for O(1) instead of full table scan
        let event_count =
            sqlx::query_scalar::<_, i64>("SELECT GREATEST(approximate_row_count('events'), 0)")
                .fetch_one(&self.pool)
                .await
                .unwrap_or(0);

        // Use continuous aggregate for recent event count
        let recent_events = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(SUM(event_count), 0)::BIGINT FROM all_event_stats_1m WHERE bucket > NOW() - INTERVAL '1 hour'",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        let db_size = sqlx::query_scalar::<_, i64>("SELECT pg_database_size(current_database())")
            .fetch_one(&self.pool)
            .await?;

        metrics.insert(
            "connected_nodes".to_string(),
            serde_json::Value::Number(serde_json::Number::from(node_count)),
        );
        metrics.insert(
            "total_events".to_string(),
            serde_json::Value::Number(serde_json::Number::from(event_count)),
        );
        metrics.insert(
            "events_last_hour".to_string(),
            serde_json::Value::Number(serde_json::Number::from(recent_events)),
        );
        metrics.insert(
            "size_bytes".to_string(),
            serde_json::Value::Number(serde_json::Number::from(db_size)),
        );

        Ok(metrics)
    }

    /// Cleanup test data by truncating all tables.
    ///
    /// **DANGER**: Deletes ALL data. Only use in test/dev environments.
    pub async fn cleanup_test_data(&self) -> Result<(), sqlx::Error> {
        // Use DO block with DELETE instead of TRUNCATE — TimescaleDB compressed
        // chunks may not be cleared by TRUNCATE, causing test data leaks.
        sqlx::query(
            r#"
            DO $$ BEGIN
                DELETE FROM ingested_raw_events;
                DELETE FROM nodes;
                DELETE FROM stats_cache;
                DELETE FROM node_stats;
                DELETE FROM event_services;
                DELETE FROM wp_tracking;
                DELETE FROM slot_convergence;
                DELETE FROM guarantee_convergence;
                DELETE FROM guarantee_convergence_slots;
                DELETE FROM assurance_convergence;
                DELETE FROM assurance_convergence_senders;
                DELETE FROM da_node_stats;
                DELETE FROM shard_latency_hist;
                DELETE FROM block_distribution_counts;
                DELETE FROM ticket_counts;
                DELETE FROM guarantee_sending_counts;
                DELETE FROM guarantee_receiving_counts;
                DELETE FROM shard_counts;
                DELETE FROM assurance_counts;
                DELETE FROM bundle_counts;
                DELETE FROM segment_counts;
                DELETE FROM preimage_counts;
                DELETE FROM status_counts;
                DELETE FROM connection_counts;
                DELETE FROM block_counts;
                DELETE FROM ticket_low_counts;
                DELETE FROM wp_pipeline_counts;
                DELETE FROM onchain_core_stats;
                DELETE FROM onchain_validator_stats;
                DELETE FROM onchain_service_stats;
            END $$
        "#,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Lightweight database connectivity check (SELECT 1).
    pub async fn ping(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Get a single node by ID, returning None if not found.
    pub async fn get_node_by_id(
        &self,
        node_id: &str,
    ) -> Result<Option<serde_json::Value>, sqlx::Error> {
        let row = sqlx::query(
            r#"
            SELECT
                node_id, peer_id, implementation_name, implementation_version,
                node_info, connected_at, disconnected_at, last_seen_at,
                is_connected, event_count, total_events
            FROM nodes
            WHERE node_id = $1
            "#,
        )
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(row.map(|row| {
            let event_count: i64 =
                row.get::<i64, _>("event_count") + row.get::<i64, _>("total_events");
            let node_info: serde_json::Value = row.get("node_info");
            serde_json::json!({
                "node_id": row.get::<String, _>("node_id"),
                "peer_id": row.get::<String, _>("peer_id"),
                "implementation_name": row.get::<String, _>("implementation_name"),
                "implementation_version": row.get::<String, _>("implementation_version"),
                "node_info": node_info,
                "connected_at": row.get::<DateTime<Utc>, _>("connected_at"),
                "disconnected_at": row.get::<Option<DateTime<Utc>>, _>("disconnected_at"),
                "last_seen_at": row.get::<DateTime<Utc>, _>("last_seen_at"),
                "is_connected": row.get::<bool, _>("is_connected"),
                "event_count": event_count,
            })
        }))
    }

    // ======================================================================
    // Analytics query methods (ported from v0.2.0 with column renames)
    // ======================================================================

    pub async fn get_network_info(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Get protocol parameters from connected nodes
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE is_connected) as connected_nodes,
                COUNT(*) as total_nodes,
                -- Extract protocol parameters from a connected node (they should all be the same)
                (SELECT node_info->'params' FROM nodes WHERE is_connected LIMIT 1) as params,
                -- Get genesis hash from a connected node
                (SELECT node_info->'genesis' FROM nodes WHERE is_connected LIMIT 1) as genesis,
                -- Get implementation info
                (SELECT jsonb_agg(DISTINCT jsonb_build_object(
                    'name', implementation_name,
                    'version', implementation_version
                )) FROM nodes WHERE is_connected) as implementations
            FROM nodes
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let connected_nodes: i64 = row.get("connected_nodes");
        let total_nodes: i64 = row.get("total_nodes");
        let params: Option<serde_json::Value> = row.get("params");
        let genesis: Option<serde_json::Value> = row.get("genesis");
        let implementations: Option<serde_json::Value> = row.get("implementations");

        // Extract key network parameters
        let (core_count, val_count, epoch_period, slot_period_sec) = if let Some(ref p) = params {
            (
                p.get("core_count").and_then(|v| v.as_u64()).unwrap_or(0) as u16,
                p.get("val_count").and_then(|v| v.as_u64()).unwrap_or(0) as u16,
                p.get("epoch_period").and_then(|v| v.as_u64()).unwrap_or(0) as u32,
                p.get("slot_period_sec")
                    .and_then(|v| v.as_u64())
                    .unwrap_or(6) as u16,
            )
        } else {
            (0, 0, 0, 6)
        };

        Ok(serde_json::json!({
            "network": {
                "connected_nodes": connected_nodes,
                "total_nodes": total_nodes,
                "genesis": genesis,
            },
            "topology": {
                "core_count": core_count,
                "validator_count": val_count,
                "epoch_period": epoch_period,
                "slot_period_sec": slot_period_sec,
            },
            "protocol_params": params,
            "implementations": implementations,
        }))
    }

    /// Get per-node status including best/finalized block heights.
    pub async fn get_node_status(&self, node_id: &str) -> Result<serde_json::Value, sqlx::Error> {
        // Get node info
        let node_info = sqlx::query(
            r#"
            SELECT
                node_id, peer_id, implementation_name, implementation_version,
                is_connected, last_seen_at, event_count, connected_at
            FROM nodes
            WHERE node_id = $1
            "#,
        )
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;

        let node = match node_info {
            Some(row) => row,
            None => return Ok(serde_json::json!({"error": "Node not found"})),
        };

        // Get best and finalized slots for this node (including hashes)
        let slots = sqlx::query(
            r#"
            SELECT
                MAX(CAST(data->'BestBlockChanged'->>'slot' AS BIGINT)) FILTER (WHERE event_type = 11) as best_slot,
                MAX(CAST(data->'FinalizedBlockChanged'->>'slot' AS BIGINT)) FILTER (WHERE event_type = 12) as finalized_slot,
                COUNT(*) FILTER (WHERE event_type = 11) as best_block_events,
                COUNT(*) FILTER (WHERE event_type = 12) as finalized_block_events,
                MAX(created_at) as last_updated
            FROM events
            WHERE node_id = $1 AND event_type IN (11, 12)
            "#,
        )
        .bind(node_id)
        .fetch_one(&self.pool)
        .await?;

        // Get latest best block hash (hex-encoded from JSON byte array)
        let best_hash: Option<String> = sqlx::query_scalar(
            r#"
            SELECT (
                SELECT string_agg(lpad(to_hex(elem::int), 2, '0'), '')
                FROM jsonb_array_elements_text(data->'BestBlockChanged'->'hash') elem
            )
            FROM events
            WHERE node_id = $1 AND event_type = 11
            ORDER BY timestamp DESC LIMIT 1
            "#,
        )
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        // Get latest finalized block hash
        let finalized_hash: Option<String> = sqlx::query_scalar(
            r#"
            SELECT (
                SELECT string_agg(lpad(to_hex(elem::int), 2, '0'), '')
                FROM jsonb_array_elements_text(data->'FinalizedBlockChanged'->'hash') elem
            )
            FROM events
            WHERE node_id = $1 AND event_type = 12
            ORDER BY timestamp DESC LIMIT 1
            "#,
        )
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        // Get latest status event for this node
        let latest_status: Option<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT data->'Status'
            FROM events
            WHERE node_id = $1 AND event_type = 10
            ORDER BY timestamp DESC
            LIMIT 1
            "#,
        )
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;

        // Get sync status
        let sync_status: Option<bool> = sqlx::query_scalar(
            r#"
            SELECT CAST(data->'SyncStatusChanged'->>'synced' AS BOOLEAN)
            FROM events
            WHERE node_id = $1 AND event_type = 13
            ORDER BY timestamp DESC
            LIMIT 1
            "#,
        )
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;

        // Get event type breakdown for this node
        let event_breakdown: Vec<(i16, i64)> = sqlx::query_as(
            r#"
            SELECT event_type, COUNT(*) as count
            FROM events
            WHERE node_id = $1
            GROUP BY event_type
            ORDER BY count DESC
            "#,
        )
        .bind(node_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "node": {
                "node_id": node.get::<String, _>("node_id"),
                "peer_id": node.get::<String, _>("peer_id"),
                "implementation_name": node.get::<String, _>("implementation_name"),
                "implementation_version": node.get::<String, _>("implementation_version"),
                "is_connected": node.get::<bool, _>("is_connected"),
                "connected_at": node.get::<Option<chrono::DateTime<chrono::Utc>>, _>("connected_at"),
                "last_seen_at": node.get::<chrono::DateTime<chrono::Utc>, _>("last_seen_at"),
                "event_count": node.get::<i64, _>("event_count"),
            },
            "chain_status": {
                "best_slot": slots.get::<Option<i64>, _>("best_slot").unwrap_or(0),
                "finalized_slot": slots.get::<Option<i64>, _>("finalized_slot").unwrap_or(0),
                "best_hash": best_hash,
                "finalized_hash": finalized_hash,
                "best_block_events": slots.get::<i64, _>("best_block_events"),
                "finalized_block_events": slots.get::<i64, _>("finalized_block_events"),
                "synced": sync_status,
                "last_updated": slots.get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_updated"),
            },
            "latest_status": latest_status,
            "event_breakdown": event_breakdown.into_iter().map(|(event_type, count)| {
                serde_json::json!({"event_type": event_type, "count": count})
            }).collect::<Vec<_>>(),
        }))
    }

    /// Get peer/connection metrics for a specific node from Status events.
    pub async fn get_node_peers(&self, node_id: &str) -> Result<serde_json::Value, sqlx::Error> {
        // Get latest status for this node
        let latest_status = sqlx::query(
            r#"
            SELECT
                CAST(data->'Status'->>'num_peers' AS INTEGER) as total_peers,
                CAST(data->'Status'->>'num_val_peers' AS INTEGER) as validator_peers,
                CAST(data->'Status'->>'num_sync_peers' AS INTEGER) as sync_peers,
                timestamp
            FROM events
            WHERE node_id = $1 AND event_type = 10
            ORDER BY timestamp DESC
            LIMIT 1
            "#,
        )
        .bind(node_id)
        .fetch_optional(&self.pool)
        .await?;

        let (total_peers, validator_peers, sync_peers) = match &latest_status {
            Some(row) => (
                row.get::<Option<i32>, _>("total_peers").unwrap_or(0),
                row.get::<Option<i32>, _>("validator_peers").unwrap_or(0),
                row.get::<Option<i32>, _>("sync_peers").unwrap_or(0),
            ),
            None => (0, 0, 0),
        };

        // Non-validator peers = total - validator peers
        let non_validator_peers = (total_peers - validator_peers).max(0);

        // Get peer history (last 100 status events)
        let history: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'timestamp', timestamp,
                'total_peers', CAST(data->'Status'->>'num_peers' AS INTEGER),
                'validator_peers', CAST(data->'Status'->>'num_val_peers' AS INTEGER),
                'sync_peers', CAST(data->'Status'->>'num_sync_peers' AS INTEGER)
            )
            FROM events
            WHERE node_id = $1 AND event_type = 10
            ORDER BY timestamp DESC
            LIMIT 100
            "#,
        )
        .bind(node_id)
        .fetch_all(&self.pool)
        .await?;

        // Get block announcement stream peers (count of opened streams)
        let announcement_peers: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(DISTINCT data->'BlockAnnouncementStreamOpened'->>'peer')
            FROM events
            WHERE node_id = $1 AND event_type = 60
            AND timestamp > NOW() - INTERVAL '1 hour'
            "#,
        )
        .bind(node_id)
        .fetch_one(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "validator_peers": validator_peers,
            "non_validator_peers": non_validator_peers,
            "total_peers": total_peers,
            "sync_peers": sync_peers,
            "block_announcement_peers": announcement_peers,
            "history": history,
        }))
    }

    /// Get real-time rolling metrics for the last N seconds.
    /// Returns per-second event counts for immediate display.
    pub async fn get_realtime_metrics(
        &self,
        seconds: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let seconds = seconds.clamp(10, 300); // 10s to 5min

        // Per-second counts from raw events — bounded to last 13s (260k rows at 20k/s)
        let per_second: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'timestamp', date_trunc('second', timestamp),
                'events', COUNT(*),
                'nodes', COUNT(DISTINCT node_id),
                'blocks', COUNT(*) FILTER (WHERE event_type = 11),
                'finalized', COUNT(*) FILTER (WHERE event_type = 12),
                'announcements', COUNT(*) FILTER (WHERE event_type = 62),
                'tickets', COUNT(*) FILTER (WHERE event_type IN (80, 82, 84))
            )
            FROM events
            WHERE timestamp > NOW() - INTERVAL '13 seconds'
            GROUP BY date_trunc('second', timestamp)
            ORDER BY date_trunc('second', timestamp) DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Totals from continuous aggregate
        let totals = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(event_count), 0)::BIGINT as total_events,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 11), 0)::BIGINT as best_blocks,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 12), 0)::BIGINT as finalized_blocks,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 42), 0)::BIGINT as authored,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 62), 0)::BIGINT as announcements,
                COUNT(DISTINCT node_id) as active_nodes
            FROM all_event_stats_1m
            WHERE bucket > NOW() - make_interval(secs => $1)
            "#,
        )
        .bind(seconds)
        .fetch_one(&self.pool)
        .await?;

        // Latest slot from raw events — bounded to 10 seconds (tiny scan)
        let slot_row = sqlx::query(
            r#"
            SELECT
                MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER)) FILTER (WHERE event_type = 11) as latest_slot
            FROM events
            WHERE event_type IN (11, 12)
            AND timestamp > NOW() - INTERVAL '10 seconds'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Calculate rates
        let total_events: i64 = totals.get("total_events");
        let events_per_second = total_events as f64 / seconds as f64;
        let blocks_per_second = totals.get::<i64, _>("best_blocks") as f64 / seconds as f64;

        Ok(serde_json::json!({
            "window_seconds": seconds,
            "timestamp": chrono::Utc::now(),
            "totals": {
                "events": total_events,
                "best_blocks": totals.get::<i64, _>("best_blocks"),
                "finalized_blocks": totals.get::<i64, _>("finalized_blocks"),
                "authored_blocks": totals.get::<i64, _>("authored"),
                "announcements": totals.get::<i64, _>("announcements"),
                "active_nodes": totals.get::<i64, _>("active_nodes"),
                "latest_slot": slot_row.get::<Option<i32>, _>("latest_slot"),
            },
            "rates": {
                "events_per_second": events_per_second,
                "blocks_per_second": blocks_per_second,
            },
            "data": per_second,
        }))
    }

    /// Get live counters - lightweight query for frequent polling.
    /// Returns just the essential counts without historical data.
    pub async fn get_live_counters(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Ultra-fast query: just counts from last 10 seconds
        let counters = sqlx::query(
            r#"
            SELECT
                COUNT(*) as events_10s,
                COUNT(*) FILTER (WHERE event_type = 11) as blocks_10s,
                COUNT(*) FILTER (WHERE event_type = 12) as finalized_10s,
                COUNT(DISTINCT node_id) as nodes_10s,
                MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER)) FILTER (WHERE event_type = 11) as latest_slot,
                MAX(CAST(data->'FinalizedBlockChanged'->>'slot' AS INTEGER)) FILTER (WHERE event_type = 12) as finalized_slot
            FROM events
            WHERE timestamp > NOW() - INTERVAL '10 seconds'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get 1-minute rates from continuous aggregate (real-time mode)
        let minute_counters = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(event_count), 0)::BIGINT as events_1m,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 11), 0)::BIGINT as blocks_1m
            FROM all_event_stats_1m
            WHERE bucket > NOW() - INTERVAL '1 minute'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let events_10s: i64 = counters.get("events_10s");
        let blocks_10s: i64 = counters.get("blocks_10s");
        let events_1m: i64 = minute_counters.get("events_1m");
        let blocks_1m: i64 = minute_counters.get("blocks_1m");

        Ok(serde_json::json!({
            "timestamp": chrono::Utc::now(),
            "latest_slot": counters.get::<Option<i32>, _>("latest_slot"),
            "finalized_slot": counters.get::<Option<i32>, _>("finalized_slot"),
            "active_nodes": counters.get::<i64, _>("nodes_10s"),
            "last_10s": {
                "events": events_10s,
                "blocks": blocks_10s,
                "finalized": counters.get::<i64, _>("finalized_10s"),
                "events_per_second": events_10s as f64 / 10.0,
                "blocks_per_second": blocks_10s as f64 / 10.0,
            },
            "last_1m": {
                "events": events_1m,
                "blocks": blocks_1m,
                "events_per_second": events_1m as f64 / 60.0,
                "blocks_per_second": blocks_1m as f64 / 60.0,
            },
        }))
    }

    /// Get peer topology and traffic patterns from block announcements and transfers.
    pub async fn get_peer_topology(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Get peer connections from block announcements (who announces to whom)
        // Peer field is a JSON array of bytes — convert to hex string
        let connections: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH block_ann AS (
                SELECT
                    node_id,
                    (SELECT string_agg(lpad(to_hex(elem::int), 2, '0'), '')
                     FROM jsonb_array_elements_text(data->'BlockAnnounced'->'peer') elem
                    ) AS peer_hex,
                    timestamp
                FROM events
                WHERE event_type = 62
                AND data->'BlockAnnounced'->'peer' IS NOT NULL
                AND jsonb_typeof(data->'BlockAnnounced'->'peer') = 'array'
                AND timestamp > NOW() - INTERVAL '1 hour'
            )
            SELECT jsonb_build_object(
                'from_node', node_id,
                'to_node', peer_hex,
                'message_count', COUNT(*),
                'connection_type', 'validator',
                'last_seen', MAX(timestamp)
            )
            FROM block_ann
            WHERE peer_hex IS NOT NULL
            GROUP BY node_id, peer_hex
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC
            LIMIT 5000
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Get block transfer activity
        let transfers: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'node_id', node_id,
                'blocks_transferred', COUNT(*),
                'unique_slots', COUNT(DISTINCT data->'BlockTransferred'->>'slot'),
                'last_transfer', MAX(timestamp)
            )
            FROM events
            WHERE event_type = 68
            AND timestamp > NOW() - INTERVAL '1 hour'
            GROUP BY node_id
            ORDER BY COUNT(*) DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Get ticket transfer topology
        let ticket_transfers: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH ticket_tx AS (
                SELECT
                    node_id,
                    (SELECT string_agg(lpad(to_hex(elem::int), 2, '0'), '')
                     FROM jsonb_array_elements_text(data->'TicketTransferred'->'peer') elem
                    ) AS peer_hex,
                    timestamp
                FROM events
                WHERE event_type = 84
                AND data->'TicketTransferred'->'peer' IS NOT NULL
                AND jsonb_typeof(data->'TicketTransferred'->'peer') = 'array'
                AND timestamp > NOW() - INTERVAL '1 hour'
            )
            SELECT jsonb_build_object(
                'from_node', node_id,
                'to_node', peer_hex,
                'message_count', COUNT(*),
                'last_seen', MAX(timestamp)
            )
            FROM ticket_tx
            WHERE peer_hex IS NOT NULL
            GROUP BY node_id, peer_hex
            ORDER BY COUNT(*) DESC
            LIMIT 5000
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Network-wide statistics
        let network_stats = sqlx::query(
            r#"
            SELECT
                COUNT(DISTINCT node_id) as active_nodes,
                COUNT(*) FILTER (WHERE event_type = 62) as total_announcements,
                COUNT(*) FILTER (WHERE event_type = 68) as total_transfers,
                COUNT(*) FILTER (WHERE event_type = 84) as total_ticket_transfers
            FROM events
            WHERE timestamp > NOW() - INTERVAL '1 hour'
            AND event_type IN (62, 68, 84)
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let active_nodes = network_stats.get::<i64, _>("active_nodes");
        let num_connections = connections.len() as i64;

        Ok(serde_json::json!({
            "connections": connections,
            "block_transfers": transfers,
            "ticket_transfers": ticket_transfers,
            "network_stats": {
                "total_connections": num_connections,
                "block_announcements_last_hour": network_stats.get::<i64, _>("total_announcements"),
                "block_transfers_last_hour": network_stats.get::<i64, _>("total_transfers"),
                "ticket_transfers_last_hour": network_stats.get::<i64, _>("total_ticket_transfers"),
                "average_peers_per_node": if active_nodes > 0 { num_connections / active_nodes } else { 0 },
                "active_nodes": active_nodes,
            }
        }))
    }

    /// Get enhanced node status with core assignment derived from recent activity.
    /// Returns fields matching ApiNodeStatusEnhanced:
    ///   assigned_core, cores_active, guarantee_activity, ticket_activity, chain_status, ...
    pub async fn get_node_status_enhanced(
        &self,
        node_id: &str,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get basic status (reuse existing query)
        let basic_status = self.get_node_status(node_id).await?;

        // Core assignments from WorkPackageReceived (94) — only event with core field
        let core_rows: Vec<(i32, i64)> = sqlx::query_as(
            r#"
            SELECT
                CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) as core_index,
                COUNT(*) as cnt
            FROM events
            WHERE node_id = $1
            AND event_type = 94
            AND data->'WorkPackageReceived'->>'core' IS NOT NULL
            AND timestamp > NOW() - INTERVAL '24 hours'
            GROUP BY CAST(data->'WorkPackageReceived'->>'core' AS INTEGER)
            ORDER BY cnt DESC
            "#,
        )
        .bind(node_id)
        .fetch_all(&self.pool)
        .await?;

        let cores_active: Vec<i32> = core_rows.iter().map(|(c, _)| *c).collect();
        let assigned_core = core_rows.first().map(|(c, _)| *c);

        // Guarantee activity from GuaranteeBuilt (105)
        let guarantee_row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as guarantees_built,
                COUNT(*) FILTER (WHERE jsonb_array_length(data->'GuaranteeBuilt'->'outline'->'guarantors') > 0) as guarantees_signed,
                MAX(timestamp) as last_guarantee_at
            FROM events
            WHERE node_id = $1
            AND event_type = 105
            AND timestamp > NOW() - INTERVAL '24 hours'
            "#,
        )
        .bind(node_id)
        .fetch_one(&self.pool)
        .await?;

        let guarantees_built = guarantee_row.get::<i64, _>("guarantees_built");
        let guarantee_activity = if guarantees_built > 0 {
            serde_json::json!({
                "guarantees_built": guarantees_built,
                "guarantees_signed": guarantee_row.get::<i64, _>("guarantees_signed"),
                "primary_core": assigned_core,
                "last_guarantee_at": guarantee_row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_guarantee_at"),
            })
        } else {
            serde_json::Value::Null
        };

        // Ticket activity: GeneratingTickets (80) + TicketSealed (82)
        let ticket_row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE event_type = 80) as tickets_generated,
                COUNT(*) FILTER (WHERE event_type = 82) as tickets_sealed,
                MAX(timestamp) FILTER (WHERE event_type IN (80, 82)) as last_ticket_at
            FROM events
            WHERE node_id = $1
            AND event_type IN (80, 82)
            AND timestamp > NOW() - INTERVAL '24 hours'
            "#,
        )
        .bind(node_id)
        .fetch_one(&self.pool)
        .await?;

        let tickets_generated = ticket_row.get::<i64, _>("tickets_generated");
        let ticket_activity = if tickets_generated > 0 {
            serde_json::json!({
                "tickets_generated": tickets_generated,
                "tickets_sealed": ticket_row.get::<i64, _>("tickets_sealed"),
                "last_ticket_at": ticket_row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_ticket_at"),
            })
        } else {
            serde_json::Value::Null
        };

        // Merge: flatten basic_status and add enhanced fields
        // The frontend expects top-level: node_id, is_connected, connected_at, last_seen_at,
        // event_count, chain_status, assigned_core, cores_active, guarantee_activity, ticket_activity
        let mut enhanced = basic_status;
        if let Some(obj) = enhanced.as_object_mut() {
            // Flatten node info to top level
            if let Some(node_obj) = obj.remove("node").and_then(|v| v.as_object().cloned()) {
                for (k, v) in node_obj {
                    obj.entry(k).or_insert(v);
                }
            }

            obj.insert(
                "assigned_core".to_string(),
                assigned_core
                    .map(|c| serde_json::json!(c))
                    .unwrap_or(serde_json::Value::Null),
            );
            obj.insert("cores_active".to_string(), serde_json::json!(cores_active));
            obj.insert("guarantee_activity".to_string(), guarantee_activity);
            obj.insert("ticket_activity".to_string(), ticket_activity);
        }

        Ok(enhanced)
    }

    /// Get aggregated metrics for WebSocket streaming.
    /// Lightweight query designed for frequent polling (1-second intervals).
    pub async fn get_aggregated_metrics(&self) -> Result<serde_json::Value, sqlx::Error> {
        let metrics = sqlx::query(
            r#"
            SELECT
                -- Last second
                COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '1 second') as events_1s,
                COUNT(*) FILTER (WHERE event_type = 11 AND timestamp > NOW() - INTERVAL '1 second') as blocks_1s,
                -- Last 10 seconds
                COUNT(*) FILTER (WHERE timestamp > NOW() - INTERVAL '10 seconds') as events_10s,
                COUNT(*) FILTER (WHERE event_type = 11 AND timestamp > NOW() - INTERVAL '10 seconds') as blocks_10s,
                COUNT(*) FILTER (WHERE event_type = 12 AND timestamp > NOW() - INTERVAL '10 seconds') as finalized_10s,
                COUNT(DISTINCT node_id) FILTER (WHERE timestamp > NOW() - INTERVAL '10 seconds') as nodes_10s,
                -- Failures last minute
                COUNT(*) FILTER (WHERE event_type IN (41, 44, 46, 81, 83, 92, 99, 107, 111, 113, 122, 127)
                    AND timestamp > NOW() - INTERVAL '1 minute') as failures_1m,
                -- Work packages last minute
                COUNT(*) FILTER (WHERE event_type BETWEEN 90 AND 113
                    AND timestamp > NOW() - INTERVAL '1 minute') as wp_events_1m,
                -- Latest slots
                MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER)) FILTER (WHERE event_type = 11) as latest_slot,
                MAX(CAST(data->'FinalizedBlockChanged'->>'slot' AS INTEGER)) FILTER (WHERE event_type = 12) as finalized_slot
            FROM events
            WHERE timestamp > NOW() - INTERVAL '1 minute'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let events_10s: i64 = metrics.get("events_10s");
        let blocks_10s: i64 = metrics.get("blocks_10s");
        let events_1s: i64 = metrics.get("events_1s");
        let nodes_10s: i64 = metrics.get("nodes_10s");
        let wp_events_1m: i64 = metrics.get("wp_events_1m");
        let failures_1m: i64 = metrics.get("failures_1m");
        let finalized_10s: i64 = metrics.get("finalized_10s");
        let latest_slot: Option<i32> = metrics.get("latest_slot");
        let finalized_slot: Option<i32> = metrics.get("finalized_slot");

        let failure_rate = if wp_events_1m > 0 {
            failures_1m as f64 / wp_events_1m as f64
        } else {
            0.0
        };

        Ok(serde_json::json!({
            "events_per_second": events_1s,
            "blocks_per_second": blocks_10s as f64 / 10.0,
            "active_nodes": nodes_10s,
            "active_work_packages": wp_events_1m,
            "failure_rate": failure_rate,
            "latest_slot": latest_slot,
            "finalized_slot": finalized_slot,
            "throughput": {
                "events_10s": events_10s,
                "blocks_10s": blocks_10s,
                "finalized_10s": finalized_10s,
            },
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Detect anomalies for alert generation.
    /// Returns a list of current alerts/warnings.
    pub async fn detect_anomalies(&self) -> Result<Vec<serde_json::Value>, sqlx::Error> {
        let mut alerts = Vec::new();

        // Check for high failure rates — from continuous aggregate
        let failure_check = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(event_count), 0)::BIGINT as total_events,
                COALESCE(SUM(event_count) FILTER (WHERE event_type IN (41, 44, 46, 81, 94, 97, 99, 102, 109, 110, 112, 113)), 0)::BIGINT as failures
            FROM all_event_stats_1m
            WHERE bucket > NOW() - INTERVAL '5 minutes'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let total: i64 = failure_check.get("total_events");
        let failures: i64 = failure_check.get("failures");
        if total > 100 && failures as f64 / total as f64 > 0.05 {
            alerts.push(serde_json::json!({
                "severity": "warning",
                "type": "high_failure_rate",
                "message": format!("High failure rate detected: {:.1}% ({} failures)",
                    failures as f64 / total as f64 * 100.0, failures),
                "details": {
                    "total_events": total,
                    "failures": failures,
                    "rate": failures as f64 / total as f64
                },
                "timestamp": chrono::Utc::now()
            }));
        }

        // Check for dropped events — from continuous aggregate
        let dropped_check = sqlx::query(
            r#"
            SELECT
                node_id,
                SUM(event_count)::BIGINT as dropped
            FROM all_event_stats_1m
            WHERE event_type = 0
            AND bucket > NOW() - INTERVAL '5 minutes'
            GROUP BY node_id
            HAVING SUM(event_count) > 10
            ORDER BY SUM(event_count) DESC
            LIMIT 5
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        for row in dropped_check {
            let node_id: String = row.get("node_id");
            let dropped: i64 = row.get("dropped");
            alerts.push(serde_json::json!({
                "severity": "warning",
                "type": "dropped_events",
                "message": format!("Node {} dropped {} events", &node_id[..16], dropped),
                "node_id": node_id,
                "details": {
                    "dropped_count": dropped
                },
                "timestamp": chrono::Utc::now()
            }));
        }

        // Check for nodes falling behind — narrow to 30s to keep scan small
        let sync_check: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH recent_slots AS (
                SELECT
                    node_id,
                    MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER)) as slot
                FROM events
                WHERE event_type = 11
                AND timestamp > NOW() - INTERVAL '30 seconds'
                GROUP BY node_id
            ),
            network_max AS (
                SELECT MAX(slot) as max_slot FROM recent_slots
            )
            SELECT jsonb_build_object(
                'node_id', rs.node_id,
                'slot', rs.slot,
                'slots_behind', nm.max_slot - rs.slot
            )
            FROM recent_slots rs
            CROSS JOIN network_max nm
            WHERE nm.max_slot - rs.slot > 10
            ORDER BY nm.max_slot - rs.slot DESC
            LIMIT 5
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        for node_info in sync_check {
            let node_id = node_info
                .get("node_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let slots_behind = node_info
                .get("slots_behind")
                .and_then(|v| v.as_i64())
                .unwrap_or(0);
            alerts.push(serde_json::json!({
                "severity": "warning",
                "type": "node_behind",
                "message": format!("Node {} is {} slots behind", &node_id[..16.min(node_id.len())], slots_behind),
                "node_id": node_id,
                "details": {
                    "slots_behind": slots_behind
                },
                "timestamp": chrono::Utc::now()
            }));
        }

        // Check for inactive nodes (were active but stopped) — narrow to 10 min
        let inactive_check = sqlx::query(
            r#"
            SELECT
                node_id,
                MAX(timestamp) as last_seen
            FROM events
            WHERE timestamp > NOW() - INTERVAL '10 minutes'
            GROUP BY node_id
            HAVING MAX(timestamp) < NOW() - INTERVAL '5 minutes'
            LIMIT 5
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        for row in inactive_check {
            let node_id: String = row.get("node_id");
            alerts.push(serde_json::json!({
                "severity": "info",
                "type": "node_inactive",
                "message": format!("Node {} became inactive", &node_id[..16]),
                "node_id": node_id,
                "timestamp": chrono::Utc::now()
            }));
        }

        Ok(alerts)
    }

    /// Get all events for a specific slot, grouped by node.
    pub async fn get_slot_events(
        &self,
        slot: i64,
        include_events: bool,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get slot summary - blocks authored, events, status
        // Uses CTE to resolve Authored (42) and AuthoringFailed (41) events
        // which don't have a slot field — their slot comes from the nearest
        // prior Authoring (40) event for the same node.
        let summary: Option<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH direct_slot_events AS (
                SELECT event_id, event_type, node_id, created_at, data
                FROM events
                WHERE COALESCE(
                    CAST(data->'Authoring'->>'slot' AS BIGINT),
                    CAST(data->'BestBlockChanged'->>'slot' AS BIGINT),
                    CAST(data->'FinalizedBlockChanged'->>'slot' AS BIGINT),
                    CAST(data->'Importing'->>'slot' AS BIGINT),
                    CAST(data->'BlockAnnounced'->>'slot' AS BIGINT),
                    CAST(data->'BlockTransferred'->>'slot' AS BIGINT)
                ) = $1
                AND event_type IN (11, 12, 40, 43, 62, 68)
                AND timestamp > NOW() - INTERVAL '7 days'
            ),
            slot_authoring AS (
                SELECT event_id, node_id, created_at
                FROM direct_slot_events
                WHERE event_type = 40
            ),
            linked_events AS (
                SELECT next_evt.event_id, next_evt.event_type, next_evt.node_id, next_evt.created_at, next_evt.data
                FROM slot_authoring sa
                CROSS JOIN LATERAL (
                    SELECT e.event_id, e.event_type, e.node_id, e.created_at, e.data
                    FROM events e
                    WHERE e.node_id = sa.node_id
                    AND e.event_type IN (41, 42)
                    AND e.created_at > sa.created_at
                    ORDER BY e.created_at ASC
                    LIMIT 1
                ) next_evt
            ),
            all_slot_events AS (
                SELECT * FROM direct_slot_events
                UNION ALL
                SELECT * FROM linked_events
            )
            SELECT jsonb_build_object(
                'slot', $1,
                'blocks_authored', COUNT(*) FILTER (WHERE event_type = 42),
                'blocks_announced', COUNT(*) FILTER (WHERE event_type = 62),
                'blocks_transferred', COUNT(*) FILTER (WHERE event_type = 68),
                'total_events', COUNT(*),
                'authoring_attempts', COUNT(*) FILTER (WHERE event_type = 40),
                'authoring_failures', COUNT(*) FILTER (WHERE event_type = 41),
                'nodes_involved', COUNT(DISTINCT node_id),
                'first_event', MIN(created_at),
                'last_event', MAX(created_at)
            )
            FROM all_slot_events
            "#,
        )
        .bind(slot)
        .fetch_optional(&self.pool)
        .await?;

        let mut result = summary.unwrap_or(serde_json::json!({
            "slot": slot,
            "blocks_authored": 0,
            "total_events": 0,
            "nodes_involved": 0,
        }));

        if include_events {
            // Get events grouped by node (same CTE approach for linked events)
            let events_by_node: Vec<serde_json::Value> = sqlx::query_scalar(
                r#"
                WITH direct_slot_events AS (
                    SELECT event_id, event_type, node_id, timestamp, data
                    FROM events
                    WHERE COALESCE(
                        CAST(data->'Authoring'->>'slot' AS BIGINT),
                        CAST(data->'BestBlockChanged'->>'slot' AS BIGINT),
                        CAST(data->'FinalizedBlockChanged'->>'slot' AS BIGINT),
                        CAST(data->'Importing'->>'slot' AS BIGINT),
                        CAST(data->'BlockAnnounced'->>'slot' AS BIGINT),
                        CAST(data->'BlockTransferred'->>'slot' AS BIGINT)
                    ) = $1
                    AND event_type IN (11, 12, 40, 43, 62, 68)
                    AND timestamp > NOW() - INTERVAL '7 days'
                ),
                slot_authoring AS (
                    SELECT event_id, node_id, timestamp
                    FROM direct_slot_events
                    WHERE event_type = 40
                ),
                linked_events AS (
                    SELECT next_evt.event_id, next_evt.event_type, next_evt.node_id, next_evt.timestamp, next_evt.data
                    FROM slot_authoring sa
                    CROSS JOIN LATERAL (
                        SELECT e.event_id, e.event_type, e.node_id, e.timestamp, e.data
                        FROM events e
                        WHERE e.node_id = sa.node_id
                        AND e.event_type IN (41, 42)
                        AND e.timestamp > sa.timestamp
                        ORDER BY e.timestamp ASC
                        LIMIT 1
                    ) next_evt
                ),
                all_slot_events AS (
                    SELECT * FROM direct_slot_events
                    UNION ALL
                    SELECT * FROM linked_events
                )
                SELECT jsonb_build_object(
                    'node_id', node_id,
                    'events', jsonb_agg(
                        jsonb_build_object(
                            'event_id', event_id,
                            'event_type', event_type,
                            'timestamp', timestamp,
                            'data', data
                        ) ORDER BY timestamp
                    )
                )
                FROM all_slot_events
                GROUP BY node_id
                ORDER BY node_id
                "#,
            )
            .bind(slot)
            .fetch_all(&self.pool)
            .await?;

            result["events_by_node"] = serde_json::json!(events_by_node);
        }

        Ok(result)
    }

    /// Get validator activity timeline with timestamp range and category filtering.
    pub async fn get_node_timeline(
        &self,
        node_id: &str,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        categories: Option<&[String]>,
        limit: i64,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Map category names to event type ranges
        // categories: status, connection, blockAuthoring, blockAnnouncement, tickets, workPackage, guarantee, shard, assurance, bundleShard, segment, preimage
        let events: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'event_id', event_id,
                'event_type', event_type,
                'timestamp', timestamp,
                'created_at', created_at,
                'category', CASE
                    WHEN event_type BETWEEN 10 AND 13 THEN 'status'
                    WHEN event_type BETWEEN 20 AND 28 THEN 'connection'
                    WHEN event_type BETWEEN 40 AND 47 THEN 'blockAuthoring'
                    WHEN event_type BETWEEN 60 AND 68 THEN 'blockAnnouncement'
                    WHEN event_type BETWEEN 80 AND 84 THEN 'tickets'
                    WHEN event_type BETWEEN 90 AND 101 THEN 'workPackage'
                    WHEN event_type BETWEEN 102 AND 113 THEN 'guarantee'
                    WHEN event_type BETWEEN 120 AND 125 THEN 'shard'
                    WHEN event_type BETWEEN 126 AND 131 THEN 'assurance'
                    WHEN event_type BETWEEN 140 AND 153 THEN 'bundleShard'
                    WHEN event_type BETWEEN 160 AND 178 THEN 'segment'
                    WHEN event_type BETWEEN 190 AND 199 THEN 'preimage'
                    ELSE 'other'
                END,
                'data', data
            )
            FROM events
            WHERE node_id = $1
            AND ($2::timestamptz IS NULL OR timestamp >= $2)
            AND ($3::timestamptz IS NULL OR timestamp <= $3)
            AND ($4::text[] IS NULL OR CASE
                WHEN event_type BETWEEN 10 AND 13 THEN 'status'
                WHEN event_type BETWEEN 20 AND 28 THEN 'connection'
                WHEN event_type BETWEEN 40 AND 47 THEN 'blockAuthoring'
                WHEN event_type BETWEEN 60 AND 68 THEN 'blockAnnouncement'
                WHEN event_type BETWEEN 80 AND 84 THEN 'tickets'
                WHEN event_type BETWEEN 90 AND 101 THEN 'workPackage'
                WHEN event_type BETWEEN 102 AND 113 THEN 'guarantee'
                WHEN event_type BETWEEN 120 AND 125 THEN 'shard'
                WHEN event_type BETWEEN 126 AND 131 THEN 'assurance'
                WHEN event_type BETWEEN 140 AND 153 THEN 'bundleShard'
                WHEN event_type BETWEEN 160 AND 178 THEN 'segment'
                WHEN event_type BETWEEN 190 AND 199 THEN 'preimage'
                ELSE 'other'
            END = ANY($4))
            ORDER BY timestamp DESC
            LIMIT $5
            "#,
        )
        .bind(node_id)
        .bind(start_time)
        .bind(end_time)
        .bind(categories)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        // Get category summary counts
        let category_counts: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'category', CASE
                    WHEN event_type BETWEEN 10 AND 13 THEN 'status'
                    WHEN event_type BETWEEN 20 AND 28 THEN 'connection'
                    WHEN event_type BETWEEN 40 AND 47 THEN 'blockAuthoring'
                    WHEN event_type BETWEEN 60 AND 68 THEN 'blockAnnouncement'
                    WHEN event_type BETWEEN 80 AND 84 THEN 'tickets'
                    WHEN event_type BETWEEN 90 AND 101 THEN 'workPackage'
                    WHEN event_type BETWEEN 102 AND 113 THEN 'guarantee'
                    WHEN event_type BETWEEN 120 AND 125 THEN 'shard'
                    WHEN event_type BETWEEN 126 AND 131 THEN 'assurance'
                    WHEN event_type BETWEEN 140 AND 153 THEN 'bundleShard'
                    WHEN event_type BETWEEN 160 AND 178 THEN 'segment'
                    WHEN event_type BETWEEN 190 AND 199 THEN 'preimage'
                    ELSE 'other'
                END,
                'count', COUNT(*)
            )
            FROM events
            WHERE node_id = $1
            AND ($2::timestamptz IS NULL OR timestamp >= $2)
            AND ($3::timestamptz IS NULL OR timestamp <= $3)
            GROUP BY CASE
                WHEN event_type BETWEEN 10 AND 13 THEN 'status'
                WHEN event_type BETWEEN 20 AND 28 THEN 'connection'
                WHEN event_type BETWEEN 40 AND 47 THEN 'blockAuthoring'
                WHEN event_type BETWEEN 60 AND 68 THEN 'blockAnnouncement'
                WHEN event_type BETWEEN 80 AND 84 THEN 'tickets'
                WHEN event_type BETWEEN 90 AND 101 THEN 'workPackage'
                WHEN event_type BETWEEN 102 AND 113 THEN 'guarantee'
                WHEN event_type BETWEEN 120 AND 125 THEN 'shard'
                WHEN event_type BETWEEN 126 AND 131 THEN 'assurance'
                WHEN event_type BETWEEN 140 AND 153 THEN 'bundleShard'
                WHEN event_type BETWEEN 160 AND 178 THEN 'segment'
                WHEN event_type BETWEEN 190 AND 199 THEN 'preimage'
                ELSE 'other'
            END
            ORDER BY COUNT(*) DESC
            "#,
        )
        .bind(node_id)
        .bind(start_time)
        .bind(end_time)
        .fetch_all(&self.pool)
        .await?;

        // Derive time_range from actual events returned
        let (range_start, range_end) = if events.is_empty() {
            (serde_json::Value::Null, serde_json::Value::Null)
        } else {
            // Events are ordered DESC, so last is earliest, first is latest
            let earliest = events.last().and_then(|e| e.get("created_at").cloned());
            let latest = events.first().and_then(|e| e.get("created_at").cloned());
            (
                earliest.unwrap_or(serde_json::Value::Null),
                latest.unwrap_or(serde_json::Value::Null),
            )
        };

        Ok(serde_json::json!({
            "node_id": node_id,
            "events": events,
            "event_count": events.len(),
            "category_counts": category_counts,
            "time_range": {
                "start": range_start,
                "end": range_end,
            },
            "timestamp": chrono::Utc::now(),
        }))
    }
}
