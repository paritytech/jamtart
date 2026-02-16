use crate::events::Event;
use crate::types::JCE_EPOCH_UNIX_MICROS;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::collections::HashMap;
use std::time::Duration;
use tracing::info;

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
/// let stats = store.get_stats().await?;
/// # Ok(())
/// # }
/// ```
pub struct EventStore {
    pool: PgPool,
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
        let pool = PgPoolOptions::new()
            .max_connections(200)
            .min_connections(20)
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(300))
            .max_lifetime(Duration::from_secs(600))
            .connect(database_url)
            .await?;

        info!("Connected to TimescaleDB database");

        // Run migrations
        sqlx::migrate!("./migrations").run(&pool).await?;

        info!("Migrations applied successfully");

        Ok(Self { pool })
    }

    /// Batch insert/update multiple node connections in a single query.
    /// Uses PostgreSQL unnest() for efficient multi-row upsert.
    pub async fn store_nodes_connected_batch(
        &self,
        nodes: &[(String, crate::events::NodeInformation, String)],
    ) -> Result<(), sqlx::Error> {
        if nodes.is_empty() {
            return Ok(());
        }

        let now = Utc::now();

        // Prepare arrays for unnest
        let node_ids: Vec<&str> = nodes.iter().map(|(id, _, _)| id.as_str()).collect();
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
            .map(|(_, info, _)| serde_json::to_value(info).unwrap_or_else(|_| serde_json::json!({})))
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
        .execute(&self.pool)
        .await?;

        tracing::debug!("Batch inserted/updated {} node connections", nodes.len());
        Ok(())
    }

    /// Batch update multiple node disconnections in a single query.
    pub async fn store_nodes_disconnected_batch(
        &self,
        node_ids: &[String],
    ) -> Result<(), sqlx::Error> {
        if node_ids.is_empty() {
            return Ok(());
        }

        let now = Utc::now();
        let ids: Vec<&str> = node_ids.iter().map(|s| s.as_str()).collect();

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
        .execute(&self.pool)
        .await?;

        tracing::debug!("Batch disconnected {} nodes", node_ids.len());
        Ok(())
    }

    pub async fn get_nodes(&self) -> Result<Vec<serde_json::Value>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                node_id,
                peer_id,
                implementation_name,
                implementation_version,
                node_info,
                connected_at,
                disconnected_at,
                last_seen_at,
                is_connected,
                event_count,
                total_events,
                address
            FROM nodes
            ORDER BY is_connected DESC, last_seen_at DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let nodes: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
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
                    "address": row.get::<Option<String>, _>("address"),
                })
            })
            .collect();

        Ok(nodes)
    }

    pub async fn get_recent_events(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<serde_json::Value>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                e.time,
                e.node_id,
                e.event_id,
                e.event_type,
                e.data,
                n.implementation_name,
                n.implementation_version
            FROM events e
            JOIN nodes n ON e.node_id = n.node_id
            ORDER BY e.time DESC
            LIMIT $1
            OFFSET $2
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        let events: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let event_data: serde_json::Value = row.get("data");
                serde_json::json!({
                    "node_id": row.get::<String, _>("node_id"),
                    "event_id": row.get::<i64, _>("event_id"),
                    "event_type": row.get::<i16, _>("event_type"),
                    "timestamp": row.get::<DateTime<Utc>, _>("time"),
                    "data": event_data,
                    "node_name": row.get::<String, _>("implementation_name"),
                    "node_version": row.get::<String, _>("implementation_version"),
                })
            })
            .collect();

        Ok(events)
    }

    /// Get recent events for a specific node, filtered at the database level.
    ///
    /// Uses the idx_events_node_time index for optimal performance on the TimescaleDB hypertable.
    pub async fn get_recent_events_by_node(
        &self,
        node_id: &str,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                e.time,
                e.node_id,
                e.event_id,
                e.event_type,
                e.data,
                n.implementation_name,
                n.implementation_version
            FROM events e
            JOIN nodes n ON e.node_id = n.node_id
            WHERE e.node_id = $1
            ORDER BY e.time DESC
            LIMIT $2
            "#,
        )
        .bind(node_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let events: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let event_data: serde_json::Value = row.get("data");
                serde_json::json!({
                    "node_id": row.get::<String, _>("node_id"),
                    "event_id": row.get::<i64, _>("event_id"),
                    "event_type": row.get::<i16, _>("event_type"),
                    "timestamp": row.get::<DateTime<Utc>, _>("time"),
                    "data": event_data,
                    "node_name": row.get::<String, _>("implementation_name"),
                    "node_version": row.get::<String, _>("implementation_version"),
                })
            })
            .collect();

        Ok(events)
    }

    /// Get blockchain statistics with caching to avoid expensive queries.
    ///
    /// Uses the stats_cache table with a 5-second TTL. Stats for total blocks
    /// are sourced from continuous aggregates for efficiency.
    pub async fn get_stats(&self) -> Result<serde_json::Value, sqlx::Error> {
        const CACHE_TTL_SECONDS: i64 = 5;
        const CACHE_KEY: &str = "system_stats";

        // Try to get cached stats first
        let cached = sqlx::query_scalar::<_, serde_json::Value>(
            r#"
            SELECT value
            FROM stats_cache
            WHERE key = $1
            AND updated_at > NOW() - INTERVAL '1 second' * $2
            "#,
        )
        .bind(CACHE_KEY)
        .bind(CACHE_TTL_SECONDS)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(stats) = cached {
            return Ok(stats);
        }

        // Cache miss or expired - recompute stats
        // Use continuous aggregates for total blocks (avoids full table scan)
        let (total_blocks, best_block_opt, finalized_block_opt) = tokio::try_join!(
            sqlx::query_scalar::<_, i64>(
                "SELECT COALESCE(SUM(event_count), 0) FROM event_stats_1h WHERE event_type = 42"
            )
            .fetch_one(&self.pool),
            sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(CAST(data->'BestBlockChanged'->>'slot' AS BIGINT))
                FROM events
                WHERE event_type = 11 AND time > NOW() - INTERVAL '1 hour'
                "#
            )
            .fetch_one(&self.pool),
            sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(CAST(data->'FinalizedBlockChanged'->>'slot' AS BIGINT))
                FROM events
                WHERE event_type = 12 AND time > NOW() - INTERVAL '1 hour'
                "#
            )
            .fetch_one(&self.pool)
        )?;

        let stats = serde_json::json!({
            "total_blocks_authored": total_blocks,
            "best_block": best_block_opt.unwrap_or(0),
            "finalized_block": finalized_block_opt.unwrap_or(0),
        });

        // Update cache asynchronously (fire-and-forget to not block response)
        let pool = self.pool.clone();
        let stats_clone = stats.clone();
        tokio::spawn(async move {
            match sqlx::query(
                r#"
                INSERT INTO stats_cache (key, value, updated_at)
                VALUES ($1, $2, CURRENT_TIMESTAMP)
                ON CONFLICT (key) DO UPDATE SET
                    value = EXCLUDED.value,
                    updated_at = CURRENT_TIMESTAMP
                "#,
            )
            .bind(CACHE_KEY)
            .bind(stats_clone)
            .execute(&pool)
            .await
            {
                Ok(_) => {
                    tracing::debug!("Stats cache updated successfully");
                }
                Err(e) => {
                    tracing::warn!("Failed to update stats cache: {}", e);
                }
            }
        });

        Ok(stats)
    }

    /// Store events using PostgreSQL COPY BINARY for maximum throughput.
    /// COPY bypasses SQL parsing, and binary format eliminates CSV encoding/parsing
    /// overhead on both client and server side.
    pub async fn store_events_batch(
        &self,
        events: Vec<(String, u64, Event)>,
    ) -> Result<(), sqlx::Error> {
        if events.is_empty() {
            return Ok(());
        }

        // For very small batches, use simple INSERT (COPY has overhead for small batches)
        if events.len() <= 10 {
            return self.store_events_simple(events).await;
        }

        // PostgreSQL epoch: 2000-01-01 00:00:00 UTC in Unix microseconds
        const PG_EPOCH_UNIX_MICROS: i64 = 946_684_800_000_000;
        const FIELD_COUNT: i16 = 5;

        // Build binary COPY payload
        let mut buf: Vec<u8> = Vec::with_capacity(19 + events.len() * 250 + 2);

        // Header: 11-byte magic + flags (i32) + header extension length (i32)
        buf.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
        buf.extend_from_slice(&0i32.to_be_bytes()); // flags
        buf.extend_from_slice(&0i32.to_be_bytes()); // header extension length

        for (node_id, event_id, event) in &events {
            // Field count
            buf.extend_from_slice(&FIELD_COUNT.to_be_bytes());

            // Column 1: time (TIMESTAMPTZ) — i64 microseconds since PG epoch
            let unix_micros = JCE_EPOCH_UNIX_MICROS + event.timestamp() as i64;
            let pg_micros = unix_micros - PG_EPOCH_UNIX_MICROS;
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&pg_micros.to_be_bytes());

            // Column 2: node_id (TEXT) — length + UTF-8 bytes
            let node_bytes = node_id.as_bytes();
            buf.extend_from_slice(&(node_bytes.len() as i32).to_be_bytes());
            buf.extend_from_slice(node_bytes);

            // Column 3: event_id (BIGINT) — i64 big-endian
            buf.extend_from_slice(&8i32.to_be_bytes());
            buf.extend_from_slice(&(*event_id as i64).to_be_bytes());

            // Column 4: event_type (SMALLINT) — i16 big-endian
            let event_type = event.event_type() as i16;
            buf.extend_from_slice(&2i32.to_be_bytes());
            buf.extend_from_slice(&event_type.to_be_bytes());

            // Column 5: data (JSONB) — version byte (0x01) + JSON UTF-8 bytes
            let event_json = serde_json::to_string(event).unwrap_or_else(|_| "{}".to_string());
            let json_bytes = event_json.as_bytes();
            buf.extend_from_slice(&(json_bytes.len() as i32 + 1).to_be_bytes()); // +1 for version byte
            buf.push(1u8); // JSONB version 1
            buf.extend_from_slice(json_bytes);
        }

        // Trailer: -1 as i16
        buf.extend_from_slice(&(-1i16).to_be_bytes());

        // Send binary payload via COPY
        let mut conn = self.pool.acquire().await?;
        let mut copy_in = conn
            .copy_in_raw(
                "COPY events (time, node_id, event_id, event_type, data) FROM STDIN WITH (FORMAT binary)",
            )
            .await?;

        copy_in.send(buf.as_slice()).await?;
        let rows_affected = copy_in.finish().await?;

        tracing::debug!(
            "COPY completed: {} events ({} rows affected)",
            events.len(),
            rows_affected
        );
        Ok(())
    }

    /// Simple batch insert for small batches using individual INSERTs in a transaction.
    async fn store_events_simple(
        &self,
        events: Vec<(String, u64, Event)>,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let event_count = events.len();

        for (node_id, event_id, event) in events {
            let event_type = event.event_type() as i16;
            let unix_timestamp_micros = JCE_EPOCH_UNIX_MICROS + event.timestamp() as i64;
            let timestamp =
                DateTime::from_timestamp_micros(unix_timestamp_micros).unwrap_or_else(|| {
                    tracing::warn!(
                        "Invalid event timestamp for node {}: {} (unix micros: {})",
                        node_id,
                        event.timestamp(),
                        unix_timestamp_micros
                    );
                    Utc::now()
                });
            let event_json =
                serde_json::to_value(&event).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;

            sqlx::query(
                r#"
                INSERT INTO events (time, node_id, event_id, event_type, data)
                VALUES ($1, $2, $3, $4, $5)
                "#,
            )
            .bind(timestamp)
            .bind(&node_id)
            .bind(event_id as i64)
            .bind(event_type)
            .bind(event_json)
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
                tracing::debug!(
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

    /// Batch update node statistics from application-level counters.
    ///
    /// Replaces the per-row database trigger (which is catastrophic at 3M events/s)
    /// with periodic batch updates from the writer workers.
    /// Multiple concurrent callers are safe since updates are additive.
    pub async fn update_node_stats(
        &self,
        node_counts: &HashMap<String, u64>,
    ) -> Result<(), sqlx::Error> {
        if node_counts.is_empty() {
            return Ok(());
        }

        let now = Utc::now();
        let node_ids: Vec<&str> = node_counts.keys().map(|s| s.as_str()).collect();
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
        .execute(&self.pool)
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
        let event_count = sqlx::query_scalar::<_, i64>(
            "SELECT GREATEST(approximate_row_count('events'), 0)",
        )
        .fetch_one(&self.pool)
        .await
        .unwrap_or(0);

        // Use continuous aggregate for recent event count
        let recent_events = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(SUM(event_count), 0) FROM event_stats_1m WHERE bucket > NOW() - INTERVAL '1 hour'",
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
        sqlx::query("TRUNCATE TABLE events, nodes, stats_cache CASCADE")
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}
