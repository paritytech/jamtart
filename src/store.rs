use crate::events::{Event, NodeInformation};
use crate::types::JCE_EPOCH_UNIX_MICROS;
use chrono::{DateTime, Utc};
use serde_json;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::time::Duration;
use tracing::info;

/// PostgreSQL-backed event store for high-throughput telemetry data.
///
/// Optimized for handling 10,000+ events/second from 1024+ concurrent nodes.
/// Features include:
/// - Batch event insertion using PostgreSQL QueryBuilder
/// - Time-based partitioning for efficient archival
/// - JSONB storage for flexible event data
/// - Automatic node stat updates via database triggers
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
    /// Creates a new event store connected to PostgreSQL.
    ///
    /// Automatically runs database migrations on startup.
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string (e.g., "postgres://user:pass@host/db")
    ///
    /// # Errors
    /// Returns `sqlx::Error` if connection fails or migrations cannot be applied.
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        // Optimized connection pool for PostgreSQL high concurrency
        // Increased to 200 to support 1024 concurrent nodes (each node may need 1-2 connections)
        let pool = PgPoolOptions::new()
            .max_connections(200) // Increased for 1024+ node support
            .min_connections(20) // Keep more connections ready
            .acquire_timeout(Duration::from_secs(5))
            .idle_timeout(Duration::from_secs(300))
            .max_lifetime(Duration::from_secs(600)) // Recycle connections every 10 minutes
            .connect(database_url)
            .await?;

        info!("Connected to PostgreSQL database");

        // Run migrations
        sqlx::migrate!("./migrations").run(&pool).await?;

        info!("Migrations applied successfully");

        Ok(Self { pool })
    }

    pub async fn store_node_connected(
        &self,
        node_id: &str,
        info: &NodeInformation,
    ) -> Result<(), sqlx::Error> {
        let now = Utc::now();
        let info_json = serde_json::to_value(info).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;

        sqlx::query(
            r#"
            INSERT INTO nodes (node_id, peer_id, implementation_name, implementation_version,
                             node_info, connected_at, last_seen_at, is_connected, event_count)
            VALUES ($1, $2, $3, $4, $5, $6, $7, true, 0)
            ON CONFLICT(node_id) DO UPDATE SET
                implementation_name = EXCLUDED.implementation_name,
                implementation_version = EXCLUDED.implementation_version,
                node_info = EXCLUDED.node_info,
                last_seen_at = EXCLUDED.last_seen_at,
                is_connected = true
            "#,
        )
        .bind(node_id)
        .bind(hex::encode(info.details.peer_id))
        .bind(info.implementation_name.as_str().unwrap_or("unknown"))
        .bind(info.implementation_version.as_str().unwrap_or("unknown"))
        .bind(info_json)
        .bind(now)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn store_node_disconnected(&self, node_id: &str) -> Result<(), sqlx::Error> {
        let now = Utc::now();

        sqlx::query(
            r#"
            UPDATE nodes
            SET is_connected = false,
                disconnected_at = $1,
                total_events = total_events + event_count
            WHERE node_id = $2
            "#,
        )
        .bind(now)
        .bind(node_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn store_event(
        &self,
        node_id: &str,
        event_id: u64,
        event: &Event,
    ) -> Result<(), sqlx::Error> {
        let event_type = event.event_type() as i32;
        let unix_timestamp_micros = JCE_EPOCH_UNIX_MICROS + event.timestamp() as i64;
        let timestamp =
            DateTime::from_timestamp_micros(unix_timestamp_micros).unwrap_or_else(|| {
                tracing::warn!(
                    "Invalid event timestamp for node {}: {} (unix micros: {}), using current time",
                    node_id,
                    event.timestamp(),
                    unix_timestamp_micros
                );
                Utc::now()
            });
        let event_json =
            serde_json::to_value(event).map_err(|e| sqlx::Error::Encode(Box::new(e)))?;

        // Use ON CONFLICT for ephemeral testnets (replace existing events)
        sqlx::query(
            r#"
            INSERT INTO events (node_id, event_id, event_type, timestamp, data, created_at)
            VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
            ON CONFLICT (node_id, event_id, created_at) DO UPDATE SET
                event_type = EXCLUDED.event_type,
                timestamp = EXCLUDED.timestamp,
                data = EXCLUDED.data
            "#,
        )
        .bind(node_id)
        .bind(event_id as i64)
        .bind(event_type)
        .bind(timestamp)
        .bind(event_json)
        .execute(&self.pool)
        .await?;

        // Note: Node stats (event_count, last_seen_at) are updated automatically
        // via database trigger trg_update_node_last_seen

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
                total_events
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
                })
            })
            .collect();

        Ok(nodes)
    }

    pub async fn get_recent_events(
        &self,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                e.id,
                e.node_id,
                e.event_id,
                e.event_type,
                e.timestamp,
                e.data,
                n.implementation_name,
                n.implementation_version
            FROM events e
            JOIN nodes n ON e.node_id = n.node_id
            ORDER BY e.id DESC
            LIMIT $1
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        let events: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let event_data: serde_json::Value = row.get("data");
                serde_json::json!({
                    "id": row.get::<i64, _>("id"),
                    "node_id": row.get::<String, _>("node_id"),
                    "event_id": row.get::<i64, _>("event_id"),
                    "event_type": row.get::<i32, _>("event_type"),
                    "timestamp": row.get::<DateTime<Utc>, _>("timestamp"),
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
    /// This avoids the N+1 query pattern by filtering in PostgreSQL rather than application code.
    /// Uses the idx_events_node_created_at index for optimal performance.
    ///
    /// # Arguments
    /// * `node_id` - The node identifier to filter events for
    /// * `limit` - Maximum number of events to return
    pub async fn get_recent_events_by_node(
        &self,
        node_id: &str,
        limit: i64,
    ) -> Result<Vec<serde_json::Value>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                e.id,
                e.node_id,
                e.event_id,
                e.event_type,
                e.timestamp,
                e.data,
                n.implementation_name,
                n.implementation_version
            FROM events e
            JOIN nodes n ON e.node_id = n.node_id
            WHERE e.node_id = $1
            ORDER BY e.created_at DESC
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
                    "id": row.get::<i64, _>("id"),
                    "node_id": row.get::<String, _>("node_id"),
                    "event_id": row.get::<i64, _>("event_id"),
                    "event_type": row.get::<i32, _>("event_type"),
                    "timestamp": row.get::<DateTime<Utc>, _>("timestamp"),
                    "data": event_data,
                    "node_name": row.get::<String, _>("implementation_name"),
                    "node_version": row.get::<String, _>("implementation_version"),
                })
            })
            .collect();

        Ok(events)
    }

    /// Get blockchain statistics with caching to avoid expensive full table scans.
    ///
    /// Uses the stats_cache table with a 5-second TTL to dramatically reduce database load.
    /// Stats are computed once and cached, then reused for subsequent requests within the TTL window.
    ///
    /// Performance impact: Reduces database load from 3 queries every request to 1 lightweight
    /// cache check per request, with full recompute only every 5 seconds.
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
        let (total_blocks, best_block_opt, finalized_block_opt) = tokio::try_join!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events WHERE event_type = 42")
                .fetch_one(&self.pool),
            sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(CAST(data->'BestBlockChanged'->>'slot' AS BIGINT))
                FROM events
                WHERE event_type = 11
                "#
            )
            .fetch_one(&self.pool),
            sqlx::query_scalar::<_, Option<i64>>(
                r#"
                SELECT MAX(CAST(data->'FinalizedBlockChanged'->>'slot' AS BIGINT))
                FROM events
                WHERE event_type = 12
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
                    // Cache update failure is non-critical - we'll recompute on next request
                }
            }
        });

        Ok(stats)
    }

    /// Optimized batch insert using PostgreSQL QueryBuilder for true bulk INSERT.
    /// This provides 10-50x better performance than individual INSERTs.
    pub async fn store_events_batch(
        &self,
        events: Vec<(String, u64, Event)>,
    ) -> Result<(), sqlx::Error> {
        if events.is_empty() {
            return Ok(());
        }

        // For very small batches, use simple approach
        if events.len() <= 5 {
            return self.store_events_simple(events).await;
        }

        // Use PostgreSQL QueryBuilder for true multi-row INSERT
        // Process in chunks to respect PostgreSQL parameter limit (~65535)
        const PARAMS_PER_ROW: usize = 5;
        const MAX_PARAMS: usize = 32000; // Conservative limit
        const CHUNK_SIZE: usize = MAX_PARAMS / PARAMS_PER_ROW; // ~6400 rows

        let mut tx = self.pool.begin().await?;

        for chunk in events.chunks(CHUNK_SIZE) {
            let mut query_builder = sqlx::QueryBuilder::new(
                "INSERT INTO events (node_id, event_id, event_type, timestamp, data, created_at) ",
            );

            query_builder.push_values(chunk.iter(), |mut b, (node_id, event_id, event)| {
                let event_type = event.event_type() as i32;
                let unix_timestamp_micros = JCE_EPOCH_UNIX_MICROS + event.timestamp() as i64;
                let timestamp = DateTime::from_timestamp_micros(unix_timestamp_micros)
                    .unwrap_or_else(|| {
                        tracing::debug!(
                            "Invalid event timestamp in batch: {} (unix micros: {})",
                            event.timestamp(),
                            unix_timestamp_micros
                        );
                        Utc::now()
                    });
                let event_json = serde_json::to_value(event).unwrap_or_else(|e| {
                    tracing::warn!("Failed to serialize event in batch: {}", e);
                    serde_json::json!({})
                });

                b.push_bind(node_id)
                    .push_bind(*event_id as i64)
                    .push_bind(event_type)
                    .push_bind(timestamp)
                    .push_bind(event_json)
                    .push("CURRENT_TIMESTAMP");
            });

            query_builder.push(
                " ON CONFLICT (node_id, event_id, created_at) DO UPDATE SET \
                 event_type = EXCLUDED.event_type, \
                 timestamp = EXCLUDED.timestamp, \
                 data = EXCLUDED.data",
            );

            query_builder.build().execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Simple batch insert for small batches using individual INSERTs in a transaction.
    async fn store_events_simple(
        &self,
        events: Vec<(String, u64, Event)>,
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        for (node_id, event_id, event) in events {
            let event_type = event.event_type() as i32;
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
                INSERT INTO events (node_id, event_id, event_type, timestamp, data, created_at)
                VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP)
                ON CONFLICT (node_id, event_id, created_at) DO UPDATE SET
                    event_type = EXCLUDED.event_type,
                    timestamp = EXCLUDED.timestamp,
                    data = EXCLUDED.data
                "#,
            )
            .bind(&node_id)
            .bind(event_id as i64)
            .bind(event_type)
            .bind(timestamp)
            .bind(event_json)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    // Health metrics method for monitoring
    pub async fn get_health_metrics(
        &self,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, sqlx::Error> {
        let mut metrics = std::collections::HashMap::new();

        // Get basic stats
        let node_count =
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM nodes WHERE is_connected = true")
                .fetch_one(&self.pool)
                .await?;

        let event_count = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events")
            .fetch_one(&self.pool)
            .await?;

        let recent_events = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM events WHERE created_at > NOW() - INTERVAL '1 hour'",
        )
        .fetch_one(&self.pool)
        .await?;

        // Get PostgreSQL database size
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
}
