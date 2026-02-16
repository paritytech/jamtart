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
                "SELECT COALESCE(SUM(event_count), 0)::BIGINT FROM event_stats_1h WHERE event_type = 42"
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
            "SELECT COALESCE(SUM(event_count), 0)::BIGINT FROM event_stats_1m WHERE bucket > NOW() - INTERVAL '1 hour'",
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

    pub async fn get_workpackage_stats(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Work package event types:
        // 90 = WorkPackageSubmission, 91 = WorkPackageBeingShared, 92 = WorkPackageFailed
        // 93 = DuplicateWorkPackage, 94 = WorkPackageReceived, 101 = Refined
        // 102 = WorkReportBuilt, 105 = GuaranteeBuilt
        // Time-bounded to 24h for performance (avoids full-table scan of 8M+ events)
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE event_type = 90) as submissions,
                COUNT(*) FILTER (WHERE event_type = 91) as being_shared,
                COUNT(*) FILTER (WHERE event_type = 92) as failed,
                COUNT(*) FILTER (WHERE event_type = 93) as duplicates,
                COUNT(*) FILTER (WHERE event_type = 94) as received,
                COUNT(*) FILTER (WHERE event_type = 101) as refined,
                COUNT(*) FILTER (WHERE event_type = 102) as work_reports_built,
                COUNT(*) FILTER (WHERE event_type = 105) as guarantees_built
            FROM events
            WHERE event_type IN (90, 91, 92, 93, 94, 101, 102, 105)
            AND received_at > NOW() - INTERVAL '24 hours'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get per-core work package stats (last 24h)
        let core_stats: Vec<(i32, i64)> = sqlx::query_as(
            r#"
            SELECT
                CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) as core,
                COUNT(*) as count
            FROM events
            WHERE event_type = 94
            AND received_at > NOW() - INTERVAL '24 hours'
            AND data->'WorkPackageReceived'->>'core' IS NOT NULL
            GROUP BY core
            ORDER BY core
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Get recent work packages (last 100)
        let recent: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'node_id', node_id,
                'time', time,
                'core', data->'WorkPackageReceived'->>'core',
                'work_package_size', data->'WorkPackageReceived'->'outline'->>'work_package_size'
            )
            FROM events
            WHERE event_type = 94
            ORDER BY received_at DESC
            LIMIT 100
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "totals": {
                "submissions": row.get::<i64, _>("submissions"),
                "being_shared": row.get::<i64, _>("being_shared"),
                "failed": row.get::<i64, _>("failed"),
                "duplicates": row.get::<i64, _>("duplicates"),
                "received": row.get::<i64, _>("received"),
                "refined": row.get::<i64, _>("refined"),
                "work_reports_built": row.get::<i64, _>("work_reports_built"),
                "guarantees_built": row.get::<i64, _>("guarantees_built"),
            },
            "by_core": core_stats.into_iter().map(|(core, count)| {
                serde_json::json!({"core": core, "count": count})
            }).collect::<Vec<_>>(),
            "recent": recent,
        }))
    }

    /// Get block statistics aggregated from telemetry events.
    pub async fn get_block_stats(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Block event types:
        // 40 = Authoring, 41 = AuthoringFailed, 42 = Authored
        // 43 = Importing, 44 = BlockVerificationFailed, 45 = BlockVerified
        // 46 = BlockExecutionFailed, 47 = BlockExecuted
        // 11 = BestBlockChanged, 12 = FinalizedBlockChanged
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE event_type = 40) as authoring_started,
                COUNT(*) FILTER (WHERE event_type = 41) as authoring_failed,
                COUNT(*) FILTER (WHERE event_type = 42) as authored,
                COUNT(*) FILTER (WHERE event_type = 43) as importing,
                COUNT(*) FILTER (WHERE event_type = 44) as verification_failed,
                COUNT(*) FILTER (WHERE event_type = 45) as verified,
                COUNT(*) FILTER (WHERE event_type = 46) as execution_failed,
                COUNT(*) FILTER (WHERE event_type = 47) as executed,
                COUNT(*) FILTER (WHERE event_type = 11) as best_block_changes,
                COUNT(*) FILTER (WHERE event_type = 12) as finalized_block_changes,
                MAX(CAST(data->'BestBlockChanged'->>'slot' AS BIGINT)) FILTER (WHERE event_type = 11) as best_slot,
                MAX(CAST(data->'FinalizedBlockChanged'->>'slot' AS BIGINT)) FILTER (WHERE event_type = 12) as finalized_slot
            FROM events
            WHERE event_type IN (40, 41, 42, 43, 44, 45, 46, 47, 11, 12)
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get per-node authoring stats
        let authoring_by_node: Vec<(String, i64)> = sqlx::query_as(
            r#"
            SELECT node_id, COUNT(*) as blocks_authored
            FROM events
            WHERE event_type = 42
            GROUP BY node_id
            ORDER BY blocks_authored DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Get recent blocks (last 50 authored)
        let recent_authored: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'node_id', node_id,
                'time', time,
                'slot', data->'Authored'->'outline'->>'slot',
                'hash', data->'Authored'->'outline'->>'hash'
            )
            FROM events
            WHERE event_type = 42
            ORDER BY received_at DESC
            LIMIT 50
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "totals": {
                "authoring_started": row.get::<i64, _>("authoring_started"),
                "authoring_failed": row.get::<i64, _>("authoring_failed"),
                "authored": row.get::<i64, _>("authored"),
                "importing": row.get::<i64, _>("importing"),
                "verification_failed": row.get::<i64, _>("verification_failed"),
                "verified": row.get::<i64, _>("verified"),
                "execution_failed": row.get::<i64, _>("execution_failed"),
                "executed": row.get::<i64, _>("executed"),
                "best_block_changes": row.get::<i64, _>("best_block_changes"),
                "finalized_block_changes": row.get::<i64, _>("finalized_block_changes"),
            },
            "chain": {
                "best_slot": row.get::<Option<i64>, _>("best_slot").unwrap_or(0),
                "finalized_slot": row.get::<Option<i64>, _>("finalized_slot").unwrap_or(0),
            },
            "authoring_by_node": authoring_by_node.into_iter().map(|(node_id, count)| {
                serde_json::json!({"node_id": node_id, "blocks_authored": count})
            }).collect::<Vec<_>>(),
            "recent_authored": recent_authored,
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
                MAX(received_at) as last_updated
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
            ORDER BY received_at DESC LIMIT 1
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
            ORDER BY received_at DESC LIMIT 1
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
            ORDER BY received_at DESC
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
            ORDER BY received_at DESC
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
                time
            FROM events
            WHERE node_id = $1 AND event_type = 10
            ORDER BY received_at DESC
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
                'time', time,
                'total_peers', CAST(data->'Status'->>'num_peers' AS INTEGER),
                'validator_peers', CAST(data->'Status'->>'num_val_peers' AS INTEGER),
                'sync_peers', CAST(data->'Status'->>'num_sync_peers' AS INTEGER)
            )
            FROM events
            WHERE node_id = $1 AND event_type = 10
            ORDER BY received_at DESC
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
            AND received_at > NOW() - INTERVAL '1 hour'
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

    /// Get data availability (shard/preimage) statistics aggregated across all nodes.
    pub async fn get_da_stats(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Get aggregate stats from latest status events for each node
        let aggregate = sqlx::query(
            r#"
            WITH latest_status AS (
                SELECT DISTINCT ON (node_id)
                    node_id,
                    CAST(data->'Status'->>'num_shards' AS INTEGER) as num_shards,
                    CAST(data->'Status'->>'shards_size' AS BIGINT) as shards_size,
                    CAST(data->'Status'->>'num_preimages' AS INTEGER) as num_preimages,
                    CAST(data->'Status'->>'preimages_size' AS INTEGER) as preimages_size
                FROM events
                WHERE event_type = 10
                ORDER BY node_id, received_at DESC
            )
            SELECT
                COUNT(*) as node_count,
                COALESCE(SUM(num_shards), 0)::BIGINT as total_shards,
                COALESCE(SUM(shards_size), 0)::BIGINT as total_shard_size,
                COALESCE(SUM(num_preimages), 0)::BIGINT as total_preimages,
                COALESCE(SUM(preimages_size), 0)::BIGINT as total_preimages_size,
                COALESCE(AVG(num_shards), 0)::FLOAT8 as avg_shards_per_node
            FROM latest_status
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get per-node DA stats
        let by_node: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH latest_status AS (
                SELECT DISTINCT ON (node_id)
                    node_id,
                    CAST(data->'Status'->>'num_shards' AS INTEGER) as num_shards,
                    CAST(data->'Status'->>'shards_size' AS BIGINT) as shards_size,
                    CAST(data->'Status'->>'num_preimages' AS INTEGER) as num_preimages,
                    CAST(data->'Status'->>'preimages_size' AS INTEGER) as preimages_size,
                    time
                FROM events
                WHERE event_type = 10
                ORDER BY node_id, received_at DESC
            )
            SELECT jsonb_build_object(
                'node_id', node_id,
                'num_shards', COALESCE(num_shards, 0),
                'shard_size_bytes', COALESCE(shards_size, 0),
                'num_preimages', COALESCE(num_preimages, 0),
                'preimages_size_bytes', COALESCE(preimages_size, 0),
                'last_update', time
            )
            FROM latest_status
            ORDER BY num_shards DESC NULLS LAST
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Get preimage announcement stats
        let preimage_stats = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE event_type = 191) as announced,
                COUNT(*) FILTER (WHERE event_type = 192) as forgotten,
                COUNT(*) FILTER (WHERE event_type = 198) as transferred,
                COUNT(*) FILTER (WHERE event_type = 199) as discarded
            FROM events
            WHERE event_type IN (191, 192, 198, 199)
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "aggregate": {
                "total_shards": aggregate.get::<i64, _>("total_shards"),
                "total_shard_size_bytes": aggregate.get::<i64, _>("total_shard_size"),
                "total_preimages": aggregate.get::<i64, _>("total_preimages"),
                "total_preimages_size_bytes": aggregate.get::<i64, _>("total_preimages_size"),
                "average_shards_per_node": aggregate.get::<f64, _>("avg_shards_per_node"),
                "node_count": aggregate.get::<i64, _>("node_count"),
            },
            "preimage_activity": {
                "announced": preimage_stats.get::<i64, _>("announced"),
                "forgotten": preimage_stats.get::<i64, _>("forgotten"),
                "transferred": preimage_stats.get::<i64, _>("transferred"),
                "discarded": preimage_stats.get::<i64, _>("discarded"),
            },
            "by_node": by_node,
        }))
    }

    /// Get work package journey/pipeline tracking for a specific work package hash.
    pub async fn get_workpackage_journey(
        &self,
        wp_hash: &str,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Work package pipeline events (types 90-113):
        // 90: Submission, 94: Received, 95: Authorized, 101: Refined
        // 102: WorkReportBuilt, 105: GuaranteeBuilt
        // Also track failures: 92: Failed, 113: GuaranteeDiscarded

        // Search for events containing this work package hash
        // The hash might be in different fields depending on event type
        let events: Vec<(i32, DateTime<Utc>, String, serde_json::Value)> = sqlx::query_as(
            r#"
            SELECT event_type, time, node_id, data
            FROM events
            WHERE event_type IN (90, 91, 92, 93, 94, 95, 96, 97, 101, 102, 105, 106, 108, 109, 112, 113)
            AND (
                data::text LIKE $1
                OR data->'WorkPackageReceived'->'outline'->>'hash' = $2
                OR data->'DuplicateWorkPackage'->>'hash' = $2
                OR data->'WorkPackageHashMapped'->>'work_package_hash' = $2
            )
            ORDER BY time ASC
            LIMIT 100
            "#,
        )
        .bind(format!("%{}%", wp_hash))
        .bind(wp_hash)
        .fetch_all(&self.pool)
        .await?;

        // Map event types to stage names
        let stages: Vec<serde_json::Value> = events
            .iter()
            .map(|(event_type, time, node_id, data)| {
                let stage = match *event_type {
                    90 => "submitted",
                    91 => "being_shared",
                    92 => "failed",
                    93 => "duplicate",
                    94 => "received",
                    95 => "authorized",
                    96 => "extrinsic_received",
                    97 => "imports_received",
                    101 => "refined",
                    102 => "report_built",
                    105 => "guarantee_built",
                    106 => "guarantee_sending",
                    108 => "guarantee_sent",
                    109 => "guarantees_distributed",
                    112 => "guarantee_received",
                    113 => "guarantee_discarded",
                    _ => "unknown",
                };
                serde_json::json!({
                    "stage": stage,
                    "timestamp": time,
                    "node_id": node_id,
                    "event_type": event_type,
                    "data": data,
                })
            })
            .collect();

        // Determine current stage and failure status
        let current_stage = stages
            .last()
            .map(|s| s["stage"].as_str().unwrap_or("unknown"));
        let failed = stages.iter().any(|s| {
            matches!(
                s["stage"].as_str(),
                Some("failed") | Some("duplicate") | Some("guarantee_discarded")
            )
        });
        let failure_reason = stages
            .iter()
            .find(|s| s["stage"].as_str() == Some("failed"))
            .and_then(|s| s["data"].get("WorkPackageFailed"))
            .and_then(|d| d.get("reason"))
            .cloned();

        // Try to extract core_index from received event
        let core_index = stages
            .iter()
            .find(|s| s["stage"].as_str() == Some("received"))
            .and_then(|s| s["data"].get("WorkPackageReceived"))
            .and_then(|d| d.get("core"))
            .and_then(|c| c.as_i64())
            .map(|c| c as i32);

        Ok(serde_json::json!({
            "work_package_hash": wp_hash,
            "core_index": core_index,
            "stages": stages,
            "current_stage": current_stage,
            "failed": failed,
            "failure_reason": failure_reason,
        }))
    }

    /// Get list of active work packages (in progress through the pipeline).
    pub async fn get_active_workpackages(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Unified single-CTE query: anchors on ALL WP-related events, not just event 94.
        // Event 94 (WorkPackageReceived) is rarely emitted — anchoring only on it causes
        // empty results when none exist in the time window.
        // Uses submission_or_share_id (events 92, 94, 95, 101, 102) and submission_id
        // (events 105, 109) to group all stages by work package.
        let work_packages: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH all_wp_events AS (
                -- Collect ALL work-package-related events, extract a unified wp_id
                SELECT
                    event_type, time, node_id, data,
                    COALESCE(
                        data->'WorkPackageReceived'->>'submission_or_share_id',
                        data->'Authorized'->>'submission_or_share_id',
                        data->'Refined'->>'submission_or_share_id',
                        data->'WorkReportBuilt'->>'submission_or_share_id',
                        data->'WorkPackageFailed'->>'submission_or_share_id',
                        data->'GuaranteeBuilt'->>'submission_id',
                        data->'GuaranteesDistributed'->>'submission_id'
                    ) as wp_id
                FROM events
                WHERE event_type IN (92, 94, 95, 101, 102, 105, 109)
                AND received_at > NOW() - INTERVAL '2 hours'
            ),
            -- Fallback: each node's core from their most recent WorkPackageReceived
            node_cores AS (
                SELECT DISTINCT ON (node_id)
                    node_id,
                    CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) as core_index
                FROM events
                WHERE event_type = 94
                AND received_at > NOW() - INTERVAL '7 days'
                AND data->'WorkPackageReceived'->>'core' IS NOT NULL
                ORDER BY node_id, received_at DESC
            ),
            wp_raw AS (
                SELECT
                    wp_id,
                    MAX(CAST(COALESCE(
                        data->'WorkPackageReceived'->>'core',
                        data->'WorkPackageReceived'->>'core_index'
                    ) AS INTEGER)) FILTER (WHERE event_type = 94) as direct_core,
                    -- Use earliest node as the primary node
                    (array_agg(node_id ORDER BY time ASC))[1] as node_id,
                    -- Timestamps per stage
                    MIN(time) FILTER (WHERE event_type = 94) as received_at,
                    MIN(time) FILTER (WHERE event_type = 95) as authorized_at,
                    MIN(time) FILTER (WHERE event_type = 101) as refined_at,
                    MIN(time) FILTER (WHERE event_type = 102) as report_built_at,
                    MIN(time) FILTER (WHERE event_type = 105) as guarantee_built_at,
                    MIN(time) FILTER (WHERE event_type = 109) as distributed_at,
                    MIN(time) FILTER (WHERE event_type = 92) as failed_at,
                    -- Earliest event = when we first saw this WP
                    MIN(time) as first_seen_at,
                    MAX(time) as last_event_at,
                    COUNT(DISTINCT node_id) as nodes_involved,
                    (array_agg(data->'WorkPackageFailed'->>'reason')
                     FILTER (WHERE event_type = 92))[1] as failure_reason
                FROM all_wp_events
                WHERE wp_id IS NOT NULL
                GROUP BY wp_id
            ),
            wp_stages AS (
                SELECT
                    r.wp_id, COALESCE(r.direct_core, nc.core_index) as core_index,
                    r.node_id, r.received_at, r.authorized_at, r.refined_at,
                    r.report_built_at, r.guarantee_built_at, r.distributed_at,
                    r.failed_at, r.first_seen_at, r.last_event_at,
                    r.nodes_involved, r.failure_reason
                FROM wp_raw r
                LEFT JOIN node_cores nc ON nc.node_id = r.node_id
            )
            SELECT jsonb_build_object(
                'hash', wp_id,
                'core_index', core_index,
                'node_id', node_id,
                'submitted_at', COALESCE(received_at, first_seen_at),
                'last_update', last_event_at,
                'current_stage', CASE
                    WHEN failed_at IS NOT NULL THEN 'failed'
                    WHEN distributed_at IS NOT NULL THEN 'distributed'
                    WHEN guarantee_built_at IS NOT NULL THEN 'guarantee_built'
                    WHEN report_built_at IS NOT NULL THEN 'report_built'
                    WHEN refined_at IS NOT NULL THEN 'refined'
                    WHEN authorized_at IS NOT NULL THEN 'authorized'
                    WHEN received_at IS NOT NULL THEN 'received'
                    ELSE 'submitted'
                END,
                'stages', jsonb_build_object(
                    'received', received_at,
                    'authorized', authorized_at,
                    'refined', refined_at,
                    'report_built', report_built_at,
                    'guarantee_built', guarantee_built_at,
                    'distributed', distributed_at,
                    'failed', failed_at
                ),
                'failure_reason', failure_reason,
                'nodes_involved', nodes_involved,
                'elapsed_ms', EXTRACT(EPOCH FROM (
                    last_event_at - COALESCE(received_at, first_seen_at)
                )) * 1000
            )
            FROM wp_stages
            ORDER BY COALESCE(received_at, first_seen_at) DESC
            LIMIT 100
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Compute summary from the result set for guaranteed accuracy
        let mut total = 0i64;
        let mut submitted = 0i64;
        let mut received = 0i64;
        let mut authorized = 0i64;
        let mut refined = 0i64;
        let mut reports_built = 0i64;
        let mut guarantees_built = 0i64;
        let mut distributed = 0i64;
        let mut failed = 0i64;

        for wp in &work_packages {
            total += 1;
            match wp.get("current_stage").and_then(|v| v.as_str()) {
                Some("submitted") => submitted += 1,
                Some("received") => received += 1,
                Some("authorized") => authorized += 1,
                Some("refined") => refined += 1,
                Some("report_built") => reports_built += 1,
                Some("guarantee_built") => guarantees_built += 1,
                Some("distributed") => distributed += 1,
                Some("failed") => failed += 1,
                _ => {}
            }
        }

        Ok(serde_json::json!({
            "work_packages": work_packages,
            "summary": {
                "total": total,
                "submitted": submitted,
                "received": received,
                "authorized": authorized,
                "refined": refined,
                "reports_built": reports_built,
                "guarantees_built": guarantees_built,
                "distributed": distributed,
                "failed": failed,
            },
        }))
    }

    /// Get core status aggregation - activity per core.
    pub async fn get_cores_status(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Get work package and guarantee activity per core.
        // Uses time-bounded scans on both sides to avoid full-table JSONB joins.
        let cores: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH core_activity AS (
                SELECT
                    CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) as core_index,
                    COUNT(*) as wp_count,
                    COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '1 hour') as wp_last_hour,
                    MAX(time) as last_activity
                FROM events
                WHERE event_type = 94
                AND received_at > NOW() - INTERVAL '24 hours'
                AND data->'WorkPackageReceived'->>'core' IS NOT NULL
                GROUP BY CAST(data->'WorkPackageReceived'->>'core' AS INTEGER)
            ),
            guarantee_activity AS (
                SELECT
                    CAST(wr.data->'WorkPackageReceived'->>'core' AS INTEGER) as core_index,
                    COUNT(*) as guarantees_last_hour
                FROM events g
                INNER JOIN events wr ON wr.event_type = 94
                    AND wr.received_at > NOW() - INTERVAL '24 hours'
                    AND wr.data->'WorkPackageReceived'->>'submission_or_share_id' = g.data->'GuaranteeBuilt'->>'submission_id'
                WHERE g.event_type = 105
                AND g.received_at > NOW() - INTERVAL '1 hour'
                AND wr.data->'WorkPackageReceived'->>'core' IS NOT NULL
                GROUP BY CAST(wr.data->'WorkPackageReceived'->>'core' AS INTEGER)
            )
            SELECT jsonb_build_object(
                'core_index', COALESCE(ca.core_index, ga.core_index),
                'active_work_packages', COALESCE(ca.wp_count, 0),
                'work_packages_last_hour', COALESCE(ca.wp_last_hour, 0),
                'guarantees_last_hour', COALESCE(ga.guarantees_last_hour, 0),
                'last_activity', ca.last_activity,
                'status', CASE
                    WHEN ca.wp_last_hour > 0 OR ga.guarantees_last_hour > 0 THEN 'active'
                    WHEN ca.last_activity > NOW() - INTERVAL '1 day' THEN 'idle'
                    ELSE 'stale'
                END
            )
            FROM core_activity ca
            FULL OUTER JOIN guarantee_activity ga ON ca.core_index = ga.core_index
            ORDER BY COALESCE(ca.core_index, ga.core_index)
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Calculate summary
        let mut active_count = 0;
        let mut idle_count = 0;
        let mut stale_count = 0;
        for core in &cores {
            match core.get("status").and_then(|s| s.as_str()) {
                Some("active") => active_count += 1,
                Some("idle") => idle_count += 1,
                Some("stale") => stale_count += 1,
                _ => {}
            }
        }

        Ok(serde_json::json!({
            "cores": cores,
            "summary": {
                "total_cores": cores.len(),
                "active_cores": active_count,
                "idle_cores": idle_count,
                "stale_cores": stale_count,
            },
        }))
    }

    /// Get guarantee distribution for a specific core.
    pub async fn get_core_guarantees(
        &self,
        core_index: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get current guarantee count from latest status events
        // Status events contain num_guarantees as Vec<u8> indexed by core
        let current_guarantees: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH latest_status AS (
                SELECT DISTINCT ON (node_id)
                    node_id,
                    data->'Status'->'num_guarantees' as num_guarantees,
                    time
                FROM events
                WHERE event_type = 10
                ORDER BY node_id, received_at DESC
            )
            SELECT jsonb_build_object(
                'node_id', node_id,
                'guarantees', COALESCE(num_guarantees->$1, '0'),
                'time', time
            )
            FROM latest_status
            WHERE num_guarantees IS NOT NULL
            AND jsonb_array_length(num_guarantees) > $1
            "#,
        )
        .bind(core_index)
        .fetch_all(&self.pool)
        .await?;

        // Get guarantee history for this core (last 24 hours, sampled)
        let history: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'time', date_trunc('hour', time),
                'count', COUNT(*)
            )
            FROM events
            WHERE event_type = 105
            AND CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER) = $1
            AND received_at > NOW() - INTERVAL '24 hours'
            GROUP BY date_trunc('hour', time)
            ORDER BY date_trunc('hour', time) DESC
            LIMIT 24
            "#,
        )
        .bind(core_index)
        .fetch_all(&self.pool)
        .await?;

        // Get recent guarantors for this core
        let recent_guarantors: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'node_id', node_id,
                'time', time,
                'work_report_hash', data->'GuaranteeBuilt'->'outline'->>'work_report_hash'
            )
            FROM events
            WHERE event_type = 105
            AND CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER) = $1
            ORDER BY received_at DESC
            LIMIT 50
            "#,
        )
        .bind(core_index)
        .fetch_all(&self.pool)
        .await?;

        // Calculate current total
        let current_total: i64 = current_guarantees
            .iter()
            .filter_map(|g| g.get("guarantees").and_then(|v| v.as_i64()))
            .sum();

        Ok(serde_json::json!({
            "core_index": core_index,
            "current_guarantees": current_total,
            "by_node": current_guarantees,
            "guarantee_history": history,
            "recent_guarantors": recent_guarantors,
        }))
    }

    /// Get execution cost metrics from Refined/Executed/Authorized events.
    pub async fn get_execution_metrics(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Get refinement stats from event type 101 (Refined)
        // costs is a JSON array; each element has total.gas_used and total.elapsed_ns
        let refinement = sqlx::query(
            r#"
            SELECT
                COUNT(*) as count,
                COALESCE(SUM(CAST(c->'total'->>'gas_used' AS BIGINT)), 0)::BIGINT as total_gas,
                COALESCE(AVG(CAST(c->'total'->>'gas_used' AS BIGINT)), 0)::FLOAT8 as avg_gas,
                COALESCE(AVG(CAST(c->'total'->>'elapsed_ns' AS BIGINT)), 0)::FLOAT8 as avg_time_ns
            FROM events e, jsonb_array_elements(e.data->'Refined'->'costs') c
            WHERE e.event_type = 101
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get authorization stats from event type 95 (Authorized)
        // cost is a single object with total.gas_used and total.elapsed_ns
        let authorization = sqlx::query(
            r#"
            SELECT
                COUNT(*) as count,
                COALESCE(SUM(CAST(data->'Authorized'->'cost'->'total'->>'gas_used' AS BIGINT)), 0)::BIGINT as total_gas,
                COALESCE(AVG(CAST(data->'Authorized'->'cost'->'total'->>'gas_used' AS BIGINT)), 0)::FLOAT8 as avg_gas,
                COALESCE(AVG(CAST(data->'Authorized'->'cost'->'total'->>'elapsed_ns' AS BIGINT)), 0)::FLOAT8 as avg_time_ns
            FROM events
            WHERE event_type = 95
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get accumulation stats from event type 47 (BlockExecuted)
        // accumulate_costs is a JSON array of [service_id, cost_object] pairs
        // cost_object has total.gas_used and total.elapsed_ns
        let accumulation = sqlx::query(
            r#"
            SELECT
                COUNT(*) as count,
                COALESCE(SUM(CAST(pair->1->'total'->>'gas_used' AS BIGINT)), 0)::BIGINT as total_gas,
                COALESCE(AVG(CAST(pair->1->'total'->>'gas_used' AS BIGINT)), 0)::FLOAT8 as avg_gas,
                COALESCE(AVG(CAST(pair->1->'total'->>'elapsed_ns' AS BIGINT)), 0)::FLOAT8 as avg_time_ns
            FROM events e, jsonb_array_elements(e.data->'BlockExecuted'->'accumulate_costs') pair
            WHERE e.event_type = 47
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get refinement stats by service — no service_id in the costs structure,
        // so skip the by_service breakdown for now
        let by_service: Vec<serde_json::Value> = vec![];

        // Get recent execution events with correct JSON paths
        let recent: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'time', time,
                'event_type', event_type,
                'node_id', node_id,
                'gas_used', CASE event_type
                    WHEN 95 THEN data->'Authorized'->'cost'->'total'->>'gas_used'
                    WHEN 101 THEN (
                        SELECT SUM(CAST(c->'total'->>'gas_used' AS BIGINT))::TEXT
                        FROM jsonb_array_elements(data->'Refined'->'costs') c
                    )
                    ELSE NULL
                END,
                'elapsed_ns', CASE event_type
                    WHEN 95 THEN data->'Authorized'->'cost'->'total'->>'elapsed_ns'
                    WHEN 101 THEN (
                        SELECT SUM(CAST(c->'total'->>'elapsed_ns' AS BIGINT))::TEXT
                        FROM jsonb_array_elements(data->'Refined'->'costs') c
                    )
                    ELSE NULL
                END
            )
            FROM events
            WHERE event_type IN (95, 101, 47)
            ORDER BY received_at DESC
            LIMIT 100
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "refinement": {
                "total_refined": refinement.get::<i64, _>("count"),
                "total_gas_used": refinement.get::<i64, _>("total_gas"),
                "average_gas_per_wp": refinement.get::<f64, _>("avg_gas"),
                "average_time_ns": refinement.get::<f64, _>("avg_time_ns"),
            },
            "authorization": {
                "total_authorized": authorization.get::<i64, _>("count"),
                "total_gas_used": authorization.get::<i64, _>("total_gas"),
                "average_gas": authorization.get::<f64, _>("avg_gas"),
                "average_time_ns": authorization.get::<f64, _>("avg_time_ns"),
            },
            "accumulation": {
                "total_accumulated": accumulation.get::<i64, _>("count"),
                "total_gas_used": accumulation.get::<i64, _>("total_gas"),
                "average_gas": accumulation.get::<f64, _>("avg_gas"),
                "average_time_ns": accumulation.get::<f64, _>("avg_time_ns"),
            },
            "by_service": by_service,
            "recent": recent,
        }))
    }

    /// Get guarantee distribution statistics.
    pub async fn get_guarantee_stats(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Guarantee event types:
        // 105 = GuaranteeBuilt, 106 = SendingGuarantee, 107 = GuaranteeSendFailed
        // 108 = GuaranteeSent, 109 = GuaranteesDistributed
        // 110 = ReceivingGuarantee, 111 = GuaranteeReceiveFailed, 112 = GuaranteeReceived
        // 113 = GuaranteeDiscarded
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE event_type = 105) as built,
                COUNT(*) FILTER (WHERE event_type = 106) as sending,
                COUNT(*) FILTER (WHERE event_type = 107) as send_failed,
                COUNT(*) FILTER (WHERE event_type = 108) as sent,
                COUNT(*) FILTER (WHERE event_type = 109) as distributed,
                COUNT(*) FILTER (WHERE event_type = 110) as receiving,
                COUNT(*) FILTER (WHERE event_type = 111) as receive_failed,
                COUNT(*) FILTER (WHERE event_type = 112) as received,
                COUNT(*) FILTER (WHERE event_type = 113) as discarded
            FROM events
            WHERE event_type IN (105, 106, 107, 108, 109, 110, 111, 112, 113)
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get guarantees by node (who is building them)
        let by_node: Vec<(String, i64)> = sqlx::query_as(
            r#"
            SELECT node_id, COUNT(*) as guarantees_built
            FROM events
            WHERE event_type = 105
            GROUP BY node_id
            ORDER BY guarantees_built DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Get recent guarantee events
        let recent: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'node_id', node_id,
                'event_type', event_type,
                'time', time,
                'data', data
            )
            FROM events
            WHERE event_type IN (105, 112, 113)
            ORDER BY received_at DESC
            LIMIT 50
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Calculate success rate
        let sent: i64 = row.get("sent");
        let send_failed: i64 = row.get("send_failed");
        let send_success_rate = if sent + send_failed > 0 {
            (sent as f64 / (sent + send_failed) as f64) * 100.0
        } else {
            100.0
        };

        let received: i64 = row.get("received");
        let receive_failed: i64 = row.get("receive_failed");
        let receive_success_rate = if received + receive_failed > 0 {
            (received as f64 / (received + receive_failed) as f64) * 100.0
        } else {
            100.0
        };

        Ok(serde_json::json!({
            "totals": {
                "built": row.get::<i64, _>("built"),
                "sending": row.get::<i64, _>("sending"),
                "send_failed": send_failed,
                "sent": sent,
                "distributed": row.get::<i64, _>("distributed"),
                "receiving": row.get::<i64, _>("receiving"),
                "receive_failed": receive_failed,
                "received": received,
                "discarded": row.get::<i64, _>("discarded"),
            },
            "success_rates": {
                "send_success_rate": format!("{:.2}%", send_success_rate),
                "receive_success_rate": format!("{:.2}%", receive_success_rate),
            },
            "by_node": by_node.into_iter().map(|(node_id, count)| {
                serde_json::json!({"node_id": node_id, "guarantees_built": count})
            }).collect::<Vec<_>>(),
            "recent": recent,
        }))
    }

    /// Get real-time rolling metrics for the last N seconds.
    /// Returns per-second event counts for immediate display.
    pub async fn get_realtime_metrics(
        &self,
        seconds: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let seconds = seconds.clamp(10, 300); // 10s to 5min

        // Get per-second event counts
        let per_second: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'time', date_trunc('second', received_at),
                'events', COUNT(*),
                'nodes', COUNT(DISTINCT node_id),
                'blocks', COUNT(*) FILTER (WHERE event_type = 11),
                'finalized', COUNT(*) FILTER (WHERE event_type = 12),
                'announcements', COUNT(*) FILTER (WHERE event_type = 62),
                'tickets', COUNT(*) FILTER (WHERE event_type IN (80, 82, 84))
            )
            FROM events
            WHERE received_at > NOW() - make_interval(secs => $1)
            GROUP BY date_trunc('second', received_at)
            ORDER BY date_trunc('second', received_at) DESC
            "#,
        )
        .bind(seconds)
        .fetch_all(&self.pool)
        .await?;

        // Get current totals
        let totals = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_events,
                COUNT(*) FILTER (WHERE event_type = 11) as best_blocks,
                COUNT(*) FILTER (WHERE event_type = 12) as finalized_blocks,
                COUNT(*) FILTER (WHERE event_type = 42) as authored,
                COUNT(*) FILTER (WHERE event_type = 62) as announcements,
                COUNT(DISTINCT node_id) as active_nodes,
                MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER)) FILTER (WHERE event_type = 11) as latest_slot
            FROM events
            WHERE received_at > NOW() - make_interval(secs => $1)
            "#,
        )
        .bind(seconds)
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
                "latest_slot": totals.get::<Option<i32>, _>("latest_slot"),
            },
            "rates": {
                "events_per_second": events_per_second,
                "blocks_per_second": blocks_per_second,
            },
            "per_second": per_second,
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
            WHERE received_at > NOW() - INTERVAL '10 seconds'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Get 1-minute rates for comparison
        let minute_counters = sqlx::query(
            r#"
            SELECT
                COUNT(*) as events_1m,
                COUNT(*) FILTER (WHERE event_type = 11) as blocks_1m
            FROM events
            WHERE received_at > NOW() - INTERVAL '1 minute'
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

    /// Get time-series metrics with configurable interval and duration.
    /// Supports: blocks, events, throughput
    pub async fn get_timeseries_metrics(
        &self,
        metric: &str,
        interval_minutes: i32,
        duration_hours: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let interval = format!("{} minutes", interval_minutes);
        let duration = format!("{} hours", duration_hours);

        match metric {
            "blocks" => {
                // Block production rate over time
                let data: Vec<serde_json::Value> = sqlx::query_scalar(
                    r#"
                    WITH time_series AS (
                        SELECT generate_series(
                            date_trunc('minute', NOW() - $1::interval),
                            date_trunc('minute', NOW()),
                            $2::interval
                        ) AS bucket
                    ),
                    block_counts AS (
                        SELECT
                            date_trunc('minute', time) -
                            (EXTRACT(MINUTE FROM time)::int % $3) * interval '1 minute' AS bucket,
                            COUNT(*) as blocks,
                            COUNT(DISTINCT node_id) as authoring_nodes,
                            MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER)) as max_slot
                        FROM events
                        WHERE event_type = 11
                        AND received_at > NOW() - $1::interval
                        GROUP BY 1
                    )
                    SELECT jsonb_build_object(
                        'time', ts.bucket,
                        'blocks', COALESCE(bc.blocks, 0),
                        'authoring_nodes', COALESCE(bc.authoring_nodes, 0),
                        'max_slot', bc.max_slot
                    )
                    FROM time_series ts
                    LEFT JOIN block_counts bc ON ts.bucket = bc.bucket
                    ORDER BY ts.bucket DESC
                    LIMIT 500
                    "#,
                )
                .bind(&duration)
                .bind(&interval)
                .bind(interval_minutes)
                .fetch_all(&self.pool)
                .await?;

                Ok(serde_json::json!({
                    "metric": "blocks",
                    "interval_minutes": interval_minutes,
                    "duration_hours": duration_hours,
                    "data": data,
                }))
            }
            "events" => {
                // Event throughput over time
                let data: Vec<serde_json::Value> = sqlx::query_scalar(
                    r#"
                    WITH time_series AS (
                        SELECT generate_series(
                            date_trunc('minute', NOW() - $1::interval),
                            date_trunc('minute', NOW()),
                            $2::interval
                        ) AS bucket
                    ),
                    event_counts AS (
                        SELECT
                            date_trunc('minute', received_at) -
                            (EXTRACT(MINUTE FROM received_at)::int % $3) * interval '1 minute' AS bucket,
                            COUNT(*) as total_events,
                            COUNT(DISTINCT node_id) as active_nodes,
                            COUNT(DISTINCT event_type) as event_types
                        FROM events
                        WHERE received_at > NOW() - $1::interval
                        GROUP BY 1
                    )
                    SELECT jsonb_build_object(
                        'time', ts.bucket,
                        'total_events', COALESCE(ec.total_events, 0),
                        'active_nodes', COALESCE(ec.active_nodes, 0),
                        'event_types', COALESCE(ec.event_types, 0),
                        'events_per_second', COALESCE(ec.total_events::float / ($3 * 60), 0)
                    )
                    FROM time_series ts
                    LEFT JOIN event_counts ec ON ts.bucket = ec.bucket
                    ORDER BY ts.bucket DESC
                    LIMIT 500
                    "#,
                )
                .bind(&duration)
                .bind(&interval)
                .bind(interval_minutes)
                .fetch_all(&self.pool)
                .await?;

                Ok(serde_json::json!({
                    "metric": "events",
                    "interval_minutes": interval_minutes,
                    "duration_hours": duration_hours,
                    "data": data,
                }))
            }
            "throughput" => {
                // Combined throughput metrics
                let data: Vec<serde_json::Value> = sqlx::query_scalar(
                    r#"
                    WITH time_series AS (
                        SELECT generate_series(
                            date_trunc('minute', NOW() - $1::interval),
                            date_trunc('minute', NOW()),
                            $2::interval
                        ) AS bucket
                    ),
                    metrics AS (
                        SELECT
                            date_trunc('minute', received_at) -
                            (EXTRACT(MINUTE FROM received_at)::int % $3) * interval '1 minute' AS bucket,
                            COUNT(*) FILTER (WHERE event_type = 11) as best_blocks,
                            COUNT(*) FILTER (WHERE event_type = 12) as finalized_blocks,
                            COUNT(*) FILTER (WHERE event_type = 42) as authored_blocks,
                            COUNT(*) FILTER (WHERE event_type = 62) as block_announcements,
                            COUNT(*) FILTER (WHERE event_type BETWEEN 90 AND 113) as wp_events,
                            COUNT(*) FILTER (WHERE event_type BETWEEN 105 AND 113) as guarantee_events
                        FROM events
                        WHERE received_at > NOW() - $1::interval
                        GROUP BY 1
                    )
                    SELECT jsonb_build_object(
                        'time', ts.bucket,
                        'best_blocks', COALESCE(m.best_blocks, 0),
                        'finalized_blocks', COALESCE(m.finalized_blocks, 0),
                        'authored_blocks', COALESCE(m.authored_blocks, 0),
                        'block_announcements', COALESCE(m.block_announcements, 0),
                        'work_package_events', COALESCE(m.wp_events, 0),
                        'guarantee_events', COALESCE(m.guarantee_events, 0)
                    )
                    FROM time_series ts
                    LEFT JOIN metrics m ON ts.bucket = m.bucket
                    ORDER BY ts.bucket DESC
                    LIMIT 500
                    "#,
                )
                .bind(&duration)
                .bind(&interval)
                .bind(interval_minutes)
                .fetch_all(&self.pool)
                .await?;

                Ok(serde_json::json!({
                    "metric": "throughput",
                    "interval_minutes": interval_minutes,
                    "duration_hours": duration_hours,
                    "data": data,
                }))
            }
            _ => Ok(serde_json::json!({
                "error": "Unknown metric type. Supported: blocks, events, throughput"
            })),
        }
    }

    /// Get validator-to-core mapping derived from guarantee events and ticket generation.
    /// In JAM, validators are assigned to cores via SAFROLE ticket sealing.
    pub async fn get_validator_core_mapping(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Get recent guarantee building activity per node/core
        let guarantee_mapping: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'node_id', node_id,
                'core_index', CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER),
                'last_guarantee', MAX(time),
                'guarantee_count', COUNT(*)
            )
            FROM events
            WHERE event_type = 105
            AND data->'GuaranteeBuilt'->'outline'->>'core' IS NOT NULL
            AND received_at > NOW() - INTERVAL '24 hours'
            GROUP BY node_id, CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER)
            ORDER BY COUNT(*) DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Get ticket generation activity per node
        let ticket_activity: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'node_id', node_id,
                'epoch', data->'GeneratingTickets'->>'epoch',
                'ticket_count', COUNT(*),
                'last_generated', MAX(time)
            )
            FROM events
            WHERE event_type = 80
            AND received_at > NOW() - INTERVAL '24 hours'
            GROUP BY node_id, data->'GeneratingTickets'->>'epoch'
            ORDER BY MAX(time) DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Get per-core activity summary
        let core_summary: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'core_index', CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER),
                'active_validators', COUNT(DISTINCT node_id),
                'total_guarantees', COUNT(*),
                'last_activity', MAX(time)
            )
            FROM events
            WHERE event_type = 105
            AND data->'GuaranteeBuilt'->'outline'->>'core' IS NOT NULL
            AND received_at > NOW() - INTERVAL '1 hour'
            GROUP BY CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER)
            ORDER BY CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER)
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Build node -> primary core mapping (most frequently used core)
        let node_core_mapping: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH ranked AS (
                SELECT
                    node_id,
                    CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER) as core_index,
                    COUNT(*) as cnt,
                    ROW_NUMBER() OVER (PARTITION BY node_id ORDER BY COUNT(*) DESC) as rn
                FROM events
                WHERE event_type = 105
                AND data->'GuaranteeBuilt'->'outline'->>'core' IS NOT NULL
                AND received_at > NOW() - INTERVAL '24 hours'
                GROUP BY node_id, CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER)
            )
            SELECT jsonb_build_object(
                'node_id', node_id,
                'primary_core', core_index,
                'guarantee_count', cnt
            )
            FROM ranked
            WHERE rn = 1
            ORDER BY core_index, cnt DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "node_core_mapping": node_core_mapping,
            "guarantee_activity": guarantee_mapping,
            "ticket_activity": ticket_activity,
            "core_summary": core_summary,
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
                    time
                FROM events
                WHERE event_type = 62
                AND data->'BlockAnnounced'->'peer' IS NOT NULL
                AND jsonb_typeof(data->'BlockAnnounced'->'peer') = 'array'
                AND received_at > NOW() - INTERVAL '1 hour'
            )
            SELECT jsonb_build_object(
                'from_node', node_id,
                'to_node', peer_hex,
                'message_count', COUNT(*),
                'connection_type', 'validator',
                'last_seen', MAX(time)
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
                'last_transfer', MAX(time)
            )
            FROM events
            WHERE event_type = 68
            AND received_at > NOW() - INTERVAL '1 hour'
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
                    time
                FROM events
                WHERE event_type = 84
                AND data->'TicketTransferred'->'peer' IS NOT NULL
                AND jsonb_typeof(data->'TicketTransferred'->'peer') = 'array'
                AND received_at > NOW() - INTERVAL '1 hour'
            )
            SELECT jsonb_build_object(
                'from_node', node_id,
                'to_node', peer_hex,
                'message_count', COUNT(*),
                'last_seen', MAX(time)
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
            WHERE received_at > NOW() - INTERVAL '1 hour'
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
            AND received_at > NOW() - INTERVAL '24 hours'
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
                MAX(time) as last_guarantee_at
            FROM events
            WHERE node_id = $1
            AND event_type = 105
            AND received_at > NOW() - INTERVAL '24 hours'
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
                MAX(time) FILTER (WHERE event_type IN (80, 82)) as last_ticket_at
            FROM events
            WHERE node_id = $1
            AND event_type IN (80, 82)
            AND received_at > NOW() - INTERVAL '24 hours'
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

    /// Optimized batch insert using PostgreSQL QueryBuilder for true bulk INSERT.
    /// This provides 10-50x better performance than individual INSERTs.
    ///
    /// Includes automatic partition recovery: if a "no partition found" error occurs,
    /// this method will attempt to create the missing partition and retry once.
    pub async fn get_core_guarantors(
        &self,
        core_index: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get guarantors who have built guarantees for this core
        let guarantors: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH guarantee_activity AS (
                SELECT
                    node_id,
                    COUNT(*) as guarantee_count,
                    MAX(time) as last_guarantee
                FROM events
                WHERE event_type = 105
                AND CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '24 hours'
                GROUP BY node_id
            ),
            shard_requests AS (
                SELECT
                    node_id,
                    COUNT(*) as shards_requested,
                    SUM(COALESCE(CAST(data->'ShardRequested'->>'size' AS BIGINT), 0)) as bytes_requested
                FROM events
                WHERE event_type = 121
                AND received_at > NOW() - INTERVAL '24 hours'
                GROUP BY node_id
            ),
            shard_transfers AS (
                SELECT
                    node_id,
                    COUNT(*) as shards_transferred,
                    SUM(COALESCE(CAST(data->'ShardTransferred'->>'size' AS BIGINT), 0)) as bytes_transferred
                FROM events
                WHERE event_type = 124
                AND received_at > NOW() - INTERVAL '24 hours'
                GROUP BY node_id
            ),
            shard_stored AS (
                SELECT
                    node_id,
                    COUNT(*) as shards_stored,
                    SUM(COALESCE(CAST(data->'ShardStored'->>'size' AS BIGINT), 0)) as bytes_stored
                FROM events
                WHERE event_type = 123
                AND received_at > NOW() - INTERVAL '24 hours'
                GROUP BY node_id
            )
            SELECT jsonb_build_object(
                'node_id', ga.node_id,
                'guarantee_count', ga.guarantee_count,
                'last_activity', ga.last_guarantee,
                'da_usage_bytes', COALESCE(ss.bytes_stored, 0),
                'shards_stored', COALESCE(ss.shards_stored, 0),
                'shards_served', COALESCE(st.shards_transferred, 0),
                'bytes_served', COALESCE(st.bytes_transferred, 0),
                'import_efficiency', CASE
                    WHEN COALESCE(sr.bytes_requested, 0) > 0
                    THEN ROUND((COALESCE(ss.bytes_stored, 0)::numeric / sr.bytes_requested)::numeric, 3)
                    ELSE 1.0
                END,
                'export_efficiency', CASE
                    WHEN COALESCE(sr.shards_requested, 0) > 0
                    THEN ROUND((COALESCE(st.shards_transferred, 0)::numeric / sr.shards_requested)::numeric, 3)
                    ELSE 1.0
                END
            )
            FROM guarantee_activity ga
            LEFT JOIN shard_requests sr ON ga.node_id = sr.node_id
            LEFT JOIN shard_transfers st ON ga.node_id = st.node_id
            LEFT JOIN shard_stored ss ON ga.node_id = ss.node_id
            ORDER BY ga.guarantee_count DESC
            "#,
        )
        .bind(core_index)
        .fetch_all(&self.pool)
        .await?;

        // Get total DA bytes for this core
        let core_totals = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(CAST(data->'ShardStored'->>'size' AS BIGINT)), 0)::BIGINT as total_da_bytes,
                COUNT(DISTINCT node_id) as active_guarantors
            FROM events
            WHERE event_type = 123
            AND received_at > NOW() - INTERVAL '24 hours'
            AND node_id IN (
                SELECT DISTINCT node_id FROM events
                WHERE event_type = 105
                AND CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '24 hours'
            )
            "#,
        )
        .bind(core_index)
        .fetch_one(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "core_index": core_index,
            "guarantors": guarantors,
            "core_total_da_bytes": core_totals.get::<i64, _>("total_da_bytes"),
            "active_guarantor_count": core_totals.get::<i64, _>("active_guarantors"),
        }))
    }

    /// Get enhanced work package journey with node info, timing, and error details.
    pub async fn get_workpackage_journey_enhanced(
        &self,
        wp_id: &str,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get all events for this work package, linked by submission_or_share_id / submission_id.
        // The wp_id parameter is the submission_or_share_id from WorkPackageReceived.
        // Per JIP-3: events 90-102 use submission_or_share_id, events 105/109 use submission_id.
        let stages: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH wp_events AS (
                SELECT
                    event_type,
                    node_id,
                    time,
                    data,
                    LAG(time) OVER (ORDER BY time) as prev_timestamp
                FROM events
                WHERE received_at > NOW() - INTERVAL '24 hours'
                AND (
                    (event_type IN (90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102) AND (
                        data->'WorkPackageSubmission'->>'submission_or_share_id' = $1
                        OR data->'WorkPackageBeingShared'->>'submission_or_share_id' = $1
                        OR data->'WorkPackageFailed'->>'submission_or_share_id' = $1
                        OR data->'DuplicateWorkPackage'->>'submission_or_share_id' = $1
                        OR data->'WorkPackageReceived'->>'submission_or_share_id' = $1
                        OR data->'Authorized'->>'submission_or_share_id' = $1
                        OR data->'ExtrinsicDataReceived'->>'submission_or_share_id' = $1
                        OR data->'ImportsReceived'->>'submission_or_share_id' = $1
                        OR data->'SharingWorkPackage'->>'submission_or_share_id' = $1
                        OR data->'WorkPackageSharingFailed'->>'submission_or_share_id' = $1
                        OR data->'BundleSent'->>'submission_or_share_id' = $1
                        OR data->'Refined'->>'submission_or_share_id' = $1
                        OR data->'WorkReportBuilt'->>'submission_or_share_id' = $1
                    ))
                    OR (event_type IN (105, 109) AND (
                        data->'GuaranteeBuilt'->>'submission_id' = $1
                        OR data->'GuaranteesDistributed'->>'submission_id' = $1
                    ))
                )
                ORDER BY time
            )
            SELECT jsonb_build_object(
                'stage', CASE event_type
                    WHEN 90 THEN 'submitted'
                    WHEN 91 THEN 'being_shared'
                    WHEN 92 THEN 'failed'
                    WHEN 93 THEN 'duplicate'
                    WHEN 94 THEN 'received'
                    WHEN 95 THEN 'authorized'
                    WHEN 96 THEN 'extrinsic_data_received'
                    WHEN 97 THEN 'imports_received'
                    WHEN 98 THEN 'sharing'
                    WHEN 99 THEN 'sharing_failed'
                    WHEN 100 THEN 'bundle_sent'
                    WHEN 101 THEN 'refined'
                    WHEN 102 THEN 'report_built'
                    WHEN 105 THEN 'guarantee_built'
                    WHEN 109 THEN 'guaranteed'
                    ELSE 'unknown'
                END,
                'status', CASE
                    WHEN event_type IN (92, 99) THEN 'failed'
                    WHEN event_type IN (91, 96, 97, 98, 100) THEN 'in_progress'
                    ELSE 'completed'
                END,
                'event_type', event_type,
                'node_id', node_id,
                'time', time,
                'duration_ms', CASE
                    WHEN prev_timestamp IS NOT NULL
                    THEN EXTRACT(EPOCH FROM (time - prev_timestamp)) * 1000
                    ELSE NULL
                END,
                'error_code', CASE
                    WHEN event_type = 92 THEN data->'WorkPackageFailed'->>'reason'
                    WHEN event_type = 99 THEN data->'WorkPackageSharingFailed'->>'reason'
                    ELSE NULL
                END,
                'data', CASE
                    WHEN event_type = 94 THEN jsonb_build_object(
                        'core', data->'WorkPackageReceived'->>'core',
                        'work_package_size', data->'WorkPackageReceived'->'outline'->>'work_package_size'
                    )
                    WHEN event_type = 101 THEN jsonb_build_object(
                        'refine_costs', data->'Refined'->'refine_costs'
                    )
                    WHEN event_type = 105 THEN jsonb_build_object(
                        'slot', data->'GuaranteeBuilt'->'outline'->>'slot',
                        'guarantors', data->'GuaranteeBuilt'->'outline'->'guarantors'
                    )
                    ELSE '{}'::jsonb
                END
            )
            FROM wp_events
            ORDER BY time
            "#,
        )
        .bind(wp_id)
        .fetch_all(&self.pool)
        .await?;

        // Extract core_index from the WorkPackageReceived stage if present
        let core_index: Option<i32> = stages.iter().find_map(|s| {
            if s.get("event_type").and_then(|v| v.as_i64()) == Some(94) {
                s.get("data")
                    .and_then(|d| d.get("core"))
                    .and_then(|c| c.as_str())
                    .and_then(|c| c.parse().ok())
            } else {
                None
            }
        });

        // Fallback: if event 94 wasn't present for this WP, look up the core
        // from the processing node's most recent event 94. Nodes are assigned
        // to cores, so their recent WorkPackageReceived events reveal the core.
        let core_index = if core_index.is_some() {
            core_index
        } else {
            let node_id = stages
                .first()
                .and_then(|s| s.get("node_id").and_then(|v| v.as_str()));
            if let Some(nid) = node_id {
                sqlx::query_scalar::<_, Option<i32>>(
                    r#"
                    SELECT CAST(data->'WorkPackageReceived'->>'core' AS INTEGER)
                    FROM events
                    WHERE event_type = 94
                    AND node_id = $1
                    ORDER BY received_at DESC
                    LIMIT 1
                    "#,
                )
                .bind(nid)
                .fetch_optional(&self.pool)
                .await?
                .flatten()
            } else {
                None
            }
        };

        let has_failed = stages.iter().any(|s| {
            s.get("status")
                .and_then(|v| v.as_str())
                .map(|s| s == "failed")
                .unwrap_or(false)
        });

        let failure_reason = stages.iter().find_map(|s| {
            if s.get("status").and_then(|v| v.as_str()) == Some("failed") {
                s.get("error_code")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            } else {
                None
            }
        });

        let started_at = stages.first().and_then(|s| s.get("timestamp").cloned());
        let completed_at = stages.last().and_then(|s| s.get("timestamp").cloned());

        // Calculate total duration from first to last event
        let total_duration_ms: Option<f64> = if stages.len() >= 2 {
            stages
                .iter()
                .filter_map(|s| s.get("duration_ms").and_then(|v| v.as_f64()))
                .sum::<f64>()
                .into()
        } else {
            None
        };

        // Build stage durations map
        let mut stage_durations = serde_json::Map::new();
        for stage in &stages {
            if let (Some(name), Some(dur)) = (
                stage.get("stage").and_then(|v| v.as_str()),
                stage.get("duration_ms").and_then(|v| v.as_f64()),
            ) {
                stage_durations.insert(name.to_string(), serde_json::json!(dur));
            }
        }

        let current_stage = stages
            .last()
            .and_then(|s| s.get("stage").cloned())
            .unwrap_or(serde_json::json!("submitted"));

        Ok(serde_json::json!({
            "hash": wp_id,
            "work_package_hash": wp_id,
            "core_index": core_index,
            "stages": stages,
            "stage_count": stages.len(),
            "current_stage": current_stage,
            "current_status": stages.last().and_then(|s| s.get("status")),
            "failed": has_failed,
            "failure_reason": failure_reason,
            "started_at": started_at,
            "completed_at": completed_at,
            "has_errors": has_failed,
            "execution_details": null,
            "guarantor_info": stages.iter()
                .filter(|s| s.get("event_type").and_then(|v| v.as_i64()) == Some(105))
                .map(|s| serde_json::json!({
                    "guarantor_index": 0,
                    "node_id": s.get("node_id"),
                    "signed_at": s.get("timestamp"),
                    "is_local": false,
                }))
                .collect::<Vec<_>>(),
            "timing": {
                "total_duration_ms": total_duration_ms,
                "submission_to_guarantee_ms": null,
                "stage_durations": stage_durations,
            },
            "errors": stages.iter()
                .filter(|s| s.get("status").and_then(|v| v.as_str()) == Some("failed"))
                .map(|s| serde_json::json!({
                    "stage": s.get("stage"),
                    "error_code": s.get("error_code"),
                    "message": s.get("error_code"),
                    "time": s.get("timestamp"),
                    "node_id": s.get("node_id"),
                }))
                .collect::<Vec<_>>(),
        }))
    }

    /// Get enhanced DA stats with read/write operations and latency metrics.
    pub async fn get_da_stats_enhanced(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Get per-node DA operation metrics
        let by_node: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH shard_ops AS (
                SELECT
                    node_id,
                    COUNT(*) FILTER (WHERE event_type = 121) as read_requests,
                    COUNT(*) FILTER (WHERE event_type = 123) as write_ops,
                    COUNT(*) FILTER (WHERE event_type = 124) as transfers_out,
                    SUM(CASE WHEN event_type = 123 THEN COALESCE(CAST(data->'ShardStored'->>'size' AS BIGINT), 0) ELSE 0 END) as bytes_written,
                    SUM(CASE WHEN event_type = 124 THEN COALESCE(CAST(data->'ShardTransferred'->>'size' AS BIGINT), 0) ELSE 0 END) as bytes_transferred
                FROM events
                WHERE event_type IN (121, 123, 124)
                AND received_at > NOW() - INTERVAL '1 hour'
                GROUP BY node_id
            ),
            preimage_ops AS (
                SELECT
                    node_id,
                    COUNT(*) FILTER (WHERE event_type = 190) as preimages_announced,
                    COUNT(*) FILTER (WHERE event_type = 191) as preimages_received,
                    COUNT(*) FILTER (WHERE event_type = 192) as preimages_in_pool
                FROM events
                WHERE event_type IN (190, 191, 192)
                AND received_at > NOW() - INTERVAL '1 hour'
                GROUP BY node_id
            ),
            latency_calc AS (
                -- Calculate latency between request and transfer
                SELECT
                    r.node_id,
                    AVG(EXTRACT(EPOCH FROM (t.time - r.time)) * 1000) as avg_latency_ms
                FROM events r
                JOIN events t ON r.node_id = t.node_id
                    AND t.event_type = 124
                    AND t.time > r.time
                    AND t.time < r.time + INTERVAL '10 seconds'
                WHERE r.event_type = 121
                AND r.received_at > NOW() - INTERVAL '1 hour'
                GROUP BY r.node_id
            ),
            current_shards AS (
                SELECT
                    node_id,
                    COUNT(DISTINCT data->'ShardStored'->>'shard_index') as num_shards,
                    SUM(COALESCE(CAST(data->'ShardStored'->>'size' AS BIGINT), 0)) as shard_size_bytes
                FROM events
                WHERE event_type = 123
                AND received_at > NOW() - INTERVAL '24 hours'
                GROUP BY node_id
            )
            SELECT jsonb_build_object(
                'node_id', COALESCE(so.node_id, po.node_id, cs.node_id),
                'num_shards', COALESCE(cs.num_shards, 0),
                'shard_size_bytes', COALESCE(cs.shard_size_bytes, 0),
                'preimages_announced', COALESCE(po.preimages_announced, 0),
                'preimages_in_pool', COALESCE(po.preimages_in_pool, 0),
                'read_ops_last_hour', COALESCE(so.read_requests, 0),
                'write_ops_last_hour', COALESCE(so.write_ops, 0),
                'avg_read_latency_ms', COALESCE(lc.avg_latency_ms, 0),
                'avg_write_latency_ms', 0,  -- Would need write request/confirm events
                'transfer_success_rate', CASE
                    WHEN COALESCE(so.read_requests, 0) > 0
                    THEN ROUND((COALESCE(so.transfers_out, 0)::numeric / so.read_requests)::numeric, 3)
                    ELSE 1.0
                END,
                'bytes_written', COALESCE(so.bytes_written, 0),
                'bytes_transferred', COALESCE(so.bytes_transferred, 0)
            )
            FROM shard_ops so
            FULL OUTER JOIN preimage_ops po ON so.node_id = po.node_id
            FULL OUTER JOIN latency_calc lc ON COALESCE(so.node_id, po.node_id) = lc.node_id
            FULL OUTER JOIN current_shards cs ON COALESCE(so.node_id, po.node_id) = cs.node_id
            WHERE COALESCE(so.node_id, po.node_id, cs.node_id) IS NOT NULL
            ORDER BY COALESCE(cs.num_shards, 0) DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Get network-wide totals
        let totals = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE event_type = 121) as total_read_ops,
                COUNT(*) FILTER (WHERE event_type = 123) as total_write_ops,
                COUNT(*) FILTER (WHERE event_type = 124) as total_transfers,
                COUNT(DISTINCT node_id) as active_da_nodes,
                COALESCE(SUM(CASE WHEN event_type = 123 THEN COALESCE(CAST(data->'ShardStored'->>'size' AS BIGINT), 0) ELSE 0 END), 0)::BIGINT as total_bytes_stored,
                COALESCE(SUM(CASE WHEN event_type = 124 THEN COALESCE(CAST(data->'ShardTransferred'->>'size' AS BIGINT), 0) ELSE 0 END), 0)::BIGINT as total_bytes_transferred
            FROM events
            WHERE event_type IN (121, 123, 124)
            AND received_at > NOW() - INTERVAL '1 hour'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let total_read_ops = totals.get::<i64, _>("total_read_ops");
        let total_transfers = totals.get::<i64, _>("total_transfers");
        let active_da_nodes = totals.get::<i64, _>("active_da_nodes");
        let total_write_ops = totals.get::<i64, _>("total_write_ops");

        // Derive node_health from by_node data
        let node_health: Vec<serde_json::Value> = by_node.iter().map(|n| {
            let read_ops = n.get("read_ops_last_hour").and_then(|v| v.as_i64()).unwrap_or(0);
            let write_ops = n.get("write_ops_last_hour").and_then(|v| v.as_i64()).unwrap_or(0);
            let num_shards = n.get("num_shards").and_then(|v| v.as_i64()).unwrap_or(0);
            let transfer_rate = n.get("transfer_success_rate").and_then(|v| v.as_f64()).unwrap_or(1.0);

            let status = if transfer_rate >= 0.9 && (read_ops + write_ops) > 0 {
                "healthy"
            } else if transfer_rate >= 0.5 || (read_ops + write_ops) > 0 {
                "degraded"
            } else {
                "unhealthy"
            };

            serde_json::json!({
                "node_id": n.get("node_id").and_then(|v| v.as_str()).unwrap_or(""),
                "status": status,
                "shards_stored": num_shards,
                "storage_used_pct": n.get("shard_size_bytes").and_then(|v| v.as_f64()).unwrap_or(0.0) / 1_073_741_824.0 * 100.0,
                "last_activity": chrono::Utc::now(),
                "issues": serde_json::json!([]),
            })
        }).collect();

        // Per-shard activity distribution
        let shard_distribution: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'shard_index', shard_idx,
                'count', total_ops,
                'requests_sent', requests_sent,
                'requests_received', requests_received,
                'nodes', node_count
            )
            FROM (
                SELECT
                    COALESCE(
                        CAST(data->'SendingShardRequest'->>'shard' AS INT),
                        CAST(data->'ShardRequestReceived'->>'shard' AS INT)
                    ) as shard_idx,
                    COUNT(*) as total_ops,
                    COUNT(*) FILTER (WHERE event_type = 120) as requests_sent,
                    COUNT(*) FILTER (WHERE event_type = 124) as requests_received,
                    COUNT(DISTINCT node_id) as node_count
                FROM events
                WHERE event_type IN (120, 124)
                AND received_at > NOW() - INTERVAL '1 hour'
                GROUP BY shard_idx
                ORDER BY shard_idx
            ) sub
            WHERE shard_idx IS NOT NULL
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Calculate availability_rate from transfer success
        let availability_rate = if total_read_ops > 0 {
            total_transfers as f64 / total_read_ops as f64
        } else {
            1.0
        };

        // Total shards and preimages across all nodes
        let total_shards: i64 = by_node
            .iter()
            .filter_map(|n| n.get("num_shards").and_then(|v| v.as_i64()))
            .sum();
        let total_preimages: i64 = by_node
            .iter()
            .filter_map(|n| n.get("preimages_in_pool").and_then(|v| v.as_i64()))
            .sum();

        Ok(serde_json::json!({
            "aggregate": {
                "total_shards": total_shards,
                "total_shard_size_bytes": totals.get::<i64, _>("total_bytes_stored"),
                "total_preimages": total_preimages,
                "total_preimage_size_bytes": 0,
                "average_shards_per_node": if active_da_nodes > 0 { total_shards / active_da_nodes } else { 0 },
                "average_shard_size_per_node": 0,
                "nodes_reporting": active_da_nodes,
            },
            "availability_rate": availability_rate,
            "shard_distribution": shard_distribution,
            "node_health": node_health,
            "by_node": by_node,
            "preimage_activity": {
                "announced_last_hour": by_node.iter()
                    .filter_map(|n| n.get("preimages_announced").and_then(|v| v.as_i64()))
                    .sum::<i64>(),
                "requested_last_hour": total_read_ops,
                "received_last_hour": total_write_ops,
            },
            "recent_operations": [],
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Get work packages currently being processed on a specific core.
    pub async fn get_core_work_packages(
        &self,
        core_index: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get active work packages for this core.
        // Only WorkPackageReceived (94) carries the core field, so we start there
        // and join downstream events via submission_or_share_id.
        let work_packages: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH received AS (
                SELECT
                    node_id,
                    time as submitted_at,
                    (data->'WorkPackageReceived'->>'submission_or_share_id') as wp_id
                FROM events
                WHERE event_type = 94
                AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '1 hour'
            ),
            latest_stage AS (
                SELECT DISTINCT ON (r.wp_id)
                    r.wp_id,
                    r.submitted_at,
                    r.node_id,
                    COALESCE(e.event_type, 94) as event_type,
                    COALESCE(e.time, r.submitted_at) as last_update,
                    e.data as stage_data
                FROM received r
                LEFT JOIN LATERAL (
                    SELECT event_type, time, data
                    FROM events
                    WHERE received_at > NOW() - INTERVAL '1 hour'
                    AND event_type IN (95, 101, 102, 105, 109, 92)
                    AND (
                        data->'Authorized'->>'submission_or_share_id' = r.wp_id
                        OR data->'Refined'->>'submission_or_share_id' = r.wp_id
                        OR data->'WorkReportBuilt'->>'submission_or_share_id' = r.wp_id
                        OR data->'WorkPackageFailed'->>'submission_or_share_id' = r.wp_id
                        OR data->'GuaranteeBuilt'->>'submission_id' = r.wp_id
                        OR data->'GuaranteesDistributed'->>'submission_id' = r.wp_id
                    )
                    ORDER BY time DESC
                    LIMIT 1
                ) e ON true
                ORDER BY r.wp_id, COALESCE(e.time, r.submitted_at) DESC
            )
            SELECT jsonb_build_object(
                'hash', wp_id,
                'stage', CASE event_type
                    WHEN 94 THEN 'received'
                    WHEN 95 THEN 'authorized'
                    WHEN 101 THEN 'refined'
                    WHEN 102 THEN 'report_built'
                    WHEN 105 THEN 'guarantee_built'
                    WHEN 109 THEN 'distributed'
                    WHEN 92 THEN 'failed'
                    ELSE 'processing'
                END,
                'is_active', event_type NOT IN (109, 92),
                'last_update', last_update,
                'submitted_at', submitted_at,
                'submitting_node', node_id,
                'gas_used', CASE WHEN stage_data IS NOT NULL
                    THEN COALESCE(
                        CAST(stage_data->'Refined'->>'gas_used' AS BIGINT),
                        NULL
                    )
                    ELSE NULL
                END,
                'elapsed_ms', EXTRACT(EPOCH FROM (last_update - submitted_at)) * 1000
            )
            FROM latest_stage
            WHERE wp_id IS NOT NULL
            ORDER BY submitted_at DESC
            LIMIT 50
            "#,
        )
        .bind(core_index)
        .fetch_all(&self.pool)
        .await?;

        // Get historical processing time for this core
        let processing_stats = sqlx::query(
            r#"
            WITH received AS (
                SELECT
                    (data->'WorkPackageReceived'->>'submission_or_share_id') as wp_id,
                    time as start_time
                FROM events
                WHERE event_type = 94
                AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '1 hour'
            ),
            wp_times AS (
                SELECT
                    r.wp_id,
                    r.start_time,
                    MAX(e.time) as end_time
                FROM received r
                INNER JOIN events e ON (
                    e.received_at > NOW() - INTERVAL '1 hour'
                    AND e.event_type IN (95, 101, 102, 105, 109)
                    AND (
                        e.data->'Authorized'->>'submission_or_share_id' = r.wp_id
                        OR e.data->'Refined'->>'submission_or_share_id' = r.wp_id
                        OR e.data->'WorkReportBuilt'->>'submission_or_share_id' = r.wp_id
                        OR e.data->'GuaranteeBuilt'->>'submission_id' = r.wp_id
                        OR e.data->'GuaranteesDistributed'->>'submission_id' = r.wp_id
                    )
                )
                GROUP BY r.wp_id, r.start_time
            )
            SELECT
                COALESCE(AVG(EXTRACT(EPOCH FROM (end_time - start_time)) * 1000), 0)::FLOAT8 as avg_processing_ms,
                COUNT(*) as completed_count
            FROM wp_times
            "#,
        )
        .bind(core_index)
        .fetch_one(&self.pool)
        .await?;

        let active_count = work_packages
            .iter()
            .filter(|wp| {
                wp.get("is_active")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            })
            .count();

        Ok(serde_json::json!({
            "core_index": core_index,
            "work_packages": work_packages,
            "queue_depth": active_count,
            "total_recent": work_packages.len(),
            "avg_processing_time_ms": processing_stats.get::<Option<f64>, _>("avg_processing_ms"),
            "completed_last_hour": processing_stats.get::<i64, _>("completed_count"),
        }))
    }

    // ========================================================================
    // MEDIUM PRIORITY: Analytics Endpoints
    // ========================================================================

    /// Get failure rate analytics across the system.
    pub async fn get_failure_rates(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Define failure event types (JIP-3 spec):
        // 41=BlockAuthoringFailed, 44=BlockSealingFailed, 46=BlockAuthoringTimedOut
        // 81=TicketGenerationFailed, 83=TicketTransferFailed
        // 92=WorkPackageFailed, 99=WorkPackageSharingFailed
        // 107=GuaranteeSendFailed, 111=GuaranteeReceiveFailed, 113=GuaranteeDiscarded
        // 122=ShardRequestFailed, 127=AssuranceSendFailed
        let failure_types = vec![41, 44, 46, 81, 83, 92, 99, 107, 111, 113, 122, 127];

        // Overall failure stats
        let overall = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_events,
                COUNT(*) FILTER (WHERE event_type = ANY($1)) as failed_events
            FROM events
            WHERE received_at > NOW() - INTERVAL '1 hour'
            "#,
        )
        .bind(&failure_types)
        .fetch_one(&self.pool)
        .await?;

        let total: i64 = overall.get("total_events");
        let failed: i64 = overall.get("failed_events");

        // By category
        let by_category: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'category', category,
                'attempts', attempts,
                'failures', failures,
                'rate', CASE WHEN attempts > 0 THEN ROUND((failures::numeric / attempts)::numeric, 4) ELSE 0 END
            )
            FROM (
                SELECT
                    'block_authoring' as category,
                    COUNT(*) FILTER (WHERE event_type IN (40, 41, 42, 44, 46)) as attempts,
                    COUNT(*) FILTER (WHERE event_type IN (41, 44, 46)) as failures
                FROM events WHERE received_at > NOW() - INTERVAL '1 hour'
                UNION ALL
                SELECT
                    'work_package' as category,
                    COUNT(*) FILTER (WHERE event_type IN (94, 95, 101, 102, 92, 99)) as attempts,
                    COUNT(*) FILTER (WHERE event_type IN (92, 99)) as failures
                FROM events WHERE received_at > NOW() - INTERVAL '1 hour'
                UNION ALL
                SELECT
                    'ticket_generation' as category,
                    COUNT(*) FILTER (WHERE event_type IN (80, 81, 82, 83, 84)) as attempts,
                    COUNT(*) FILTER (WHERE event_type IN (81, 83)) as failures
                FROM events WHERE received_at > NOW() - INTERVAL '1 hour'
                UNION ALL
                SELECT
                    'guarantee' as category,
                    COUNT(*) FILTER (WHERE event_type IN (105, 106, 107, 108, 109)) as attempts,
                    COUNT(*) FILTER (WHERE event_type = 107) as failures
                FROM events WHERE received_at > NOW() - INTERVAL '1 hour'
            ) categories
            WHERE attempts > 0
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // By node (top offenders)
        let by_node: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH node_failures AS (
                SELECT
                    node_id,
                    COUNT(*) as total_events,
                    COUNT(*) FILTER (WHERE event_type = ANY($1)) as failures,
                    MODE() WITHIN GROUP (ORDER BY event_type) FILTER (WHERE event_type = ANY($1)) as most_common_failure
                FROM events
                WHERE received_at > NOW() - INTERVAL '1 hour'
                GROUP BY node_id
                HAVING COUNT(*) FILTER (WHERE event_type = ANY($1)) > 0
            )
            SELECT jsonb_build_object(
                'node_id', node_id,
                'total_events', total_events,
                'failures', failures,
                'failure_rate', ROUND((failures::numeric / total_events)::numeric, 4),
                'top_failure_type', most_common_failure
            )
            FROM node_failures
            ORDER BY failures DESC
            LIMIT 20
            "#,
        )
        .bind(&failure_types)
        .fetch_all(&self.pool)
        .await?;

        // Recent failures (last 10)
        let recent_failures: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'event_type', event_type,
                'event_name', CASE event_type
                    WHEN 41 THEN 'BlockAuthoringFailed'
                    WHEN 44 THEN 'BlockSealingFailed'
                    WHEN 46 THEN 'BlockAuthoringTimedOut'
                    WHEN 81 THEN 'TicketGenerationFailed'
                    WHEN 83 THEN 'TicketTransferFailed'
                    WHEN 92 THEN 'WorkPackageFailed'
                    WHEN 99 THEN 'WorkPackageSharingFailed'
                    WHEN 107 THEN 'GuaranteeSendFailed'
                    WHEN 111 THEN 'GuaranteeReceiveFailed'
                    WHEN 113 THEN 'GuaranteeDiscarded'
                    WHEN 122 THEN 'ShardRequestFailed'
                    WHEN 127 THEN 'AssuranceSendFailed'
                    ELSE 'Unknown'
                END,
                'node_id', node_id,
                'time', time,
                'reason', COALESCE(
                    data->>'reason',
                    data->>'error',
                    data->'BlockAuthoringFailed'->>'reason',
                    data->'RefinementFailed'->>'error',
                    'unknown'
                )
            )
            FROM events
            WHERE event_type = ANY($1)
            AND received_at > NOW() - INTERVAL '1 hour'
            ORDER BY time DESC
            LIMIT 20
            "#,
        )
        .bind(&failure_types)
        .fetch_all(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "overall": {
                "total_events": total,
                "failed_events": failed,
                "failure_rate": if total > 0 { failed as f64 / total as f64 } else { 0.0 },
            },
            "by_category": by_category,
            "by_node": by_node,
            "recent_failures": recent_failures,
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Get block propagation analytics.
    pub async fn get_block_propagation(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Calculate propagation times from BlockAnnounced (62) to BlockTransferred (68)
        let propagation_stats = sqlx::query(
            r#"
            WITH block_times AS (
                SELECT
                    data->'BlockAnnounced'->>'slot' as slot,
                    node_id,
                    MIN(time) as announced_at
                FROM events
                WHERE event_type = 62
                AND received_at > NOW() - INTERVAL '1 hour'
                GROUP BY data->'BlockAnnounced'->>'slot', node_id
            ),
            transfer_times AS (
                SELECT
                    data->'BlockTransferred'->>'slot' as slot,
                    node_id,
                    MIN(time) as transferred_at
                FROM events
                WHERE event_type = 68
                AND received_at > NOW() - INTERVAL '1 hour'
                GROUP BY data->'BlockTransferred'->>'slot', node_id
            ),
            propagation AS (
                SELECT
                    bt.slot,
                    bt.node_id,
                    EXTRACT(EPOCH FROM (tt.transferred_at - bt.announced_at)) * 1000 as propagation_ms
                FROM block_times bt
                JOIN transfer_times tt ON bt.slot = tt.slot AND bt.node_id != tt.node_id
                WHERE tt.transferred_at > bt.announced_at
            )
            SELECT
                AVG(propagation_ms)::FLOAT8 as avg_propagation_ms,
                (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY propagation_ms))::FLOAT8 as p50_propagation_ms,
                (PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY propagation_ms))::FLOAT8 as p95_propagation_ms,
                (PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY propagation_ms))::FLOAT8 as p99_propagation_ms,
                COUNT(*) as sample_count
            FROM propagation
            WHERE propagation_ms > 0 AND propagation_ms < 60000
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        // Per-node receive delays
        let by_node: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH node_blocks AS (
                SELECT
                    node_id,
                    COUNT(DISTINCT data->'BlockAnnounced'->>'slot') FILTER (WHERE event_type = 62) as blocks_announced,
                    COUNT(DISTINCT data->'BlockTransferred'->>'slot') FILTER (WHERE event_type = 68) as blocks_received
                FROM events
                WHERE event_type IN (62, 68)
                AND received_at > NOW() - INTERVAL '1 hour'
                GROUP BY node_id
            )
            SELECT jsonb_build_object(
                'node_id', node_id,
                'blocks_announced', blocks_announced,
                'blocks_received', blocks_received,
                'blocks_originated', blocks_announced - blocks_received
            )
            FROM node_blocks
            WHERE blocks_announced > 0 OR blocks_received > 0
            ORDER BY blocks_announced DESC
            LIMIT 50
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "last_hour": {
                "avg_propagation_ms": propagation_stats.get::<Option<f64>, _>("avg_propagation_ms"),
                "p50_propagation_ms": propagation_stats.get::<Option<f64>, _>("p50_propagation_ms"),
                "p95_propagation_ms": propagation_stats.get::<Option<f64>, _>("p95_propagation_ms"),
                "p99_propagation_ms": propagation_stats.get::<Option<f64>, _>("p99_propagation_ms"),
                "sample_count": propagation_stats.get::<i64, _>("sample_count"),
            },
            "by_node": by_node,
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Get guarantor statistics aggregated by guarantor.
    pub async fn get_guarantees_by_guarantor(&self) -> Result<serde_json::Value, sqlx::Error> {
        let guarantors: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH guarantee_events AS (
                SELECT
                    node_id,
                    time,
                    received_at,
                    CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER) as core_index
                FROM events
                WHERE event_type = 105
                AND received_at > NOW() - INTERVAL '24 hours'
            ),
            guarantor_stats AS (
                SELECT
                    node_id,
                    COUNT(*) as total_guarantees,
                    COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '1 hour') as guarantees_last_hour,
                    array_agg(DISTINCT core_index) as cores_active,
                    MODE() WITHIN GROUP (ORDER BY core_index) as primary_core,
                    MAX(time) as last_guarantee
                FROM guarantee_events
                GROUP BY node_id
            ),
            success_rates AS (
                SELECT
                    node_id,
                    COUNT(*) FILTER (WHERE event_type = 105) as built,
                    COUNT(*) FILTER (WHERE event_type = 108) as accumulated,
                    COUNT(*) FILTER (WHERE event_type IN (109, 110)) as failed
                FROM events
                WHERE event_type IN (105, 108, 109, 110)
                AND received_at > NOW() - INTERVAL '24 hours'
                GROUP BY node_id
            )
            SELECT jsonb_build_object(
                'node_id', gs.node_id,
                'total_guarantees', gs.total_guarantees,
                'guarantees_last_hour', gs.guarantees_last_hour,
                'success_rate', CASE
                    WHEN sr.built > 0
                    THEN ROUND(((sr.built - sr.failed)::numeric / sr.built)::numeric, 3)
                    ELSE 1.0
                END,
                'cores_active', gs.cores_active,
                'primary_core', gs.primary_core,
                'last_guarantee', gs.last_guarantee
            )
            FROM guarantor_stats gs
            LEFT JOIN success_rates sr ON gs.node_id = sr.node_id
            ORDER BY gs.total_guarantees DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Get top and bottom performers
        let top_performers: Vec<String> = guarantors
            .iter()
            .take(5)
            .filter_map(|g| g.get("node_id").and_then(|v| v.as_str()).map(String::from))
            .collect();

        let bottom_performers: Vec<String> = guarantors
            .iter()
            .rev()
            .take(5)
            .filter_map(|g| g.get("node_id").and_then(|v| v.as_str()).map(String::from))
            .collect();

        Ok(serde_json::json!({
            "guarantors": guarantors,
            "total_guarantors": guarantors.len(),
            "top_performers": top_performers,
            "bottom_performers": bottom_performers,
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Get network health and congestion metrics.
    /// Returns shape matching ApiNetworkHealth: overall_health, health_score, components[], alerts[].
    pub async fn get_network_health(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Gather raw metrics for health scoring
        let stats = sqlx::query(
            r#"
            SELECT
                COUNT(DISTINCT node_id) FILTER (WHERE received_at > NOW() - INTERVAL '1 minute') as active_nodes_1m,
                COUNT(DISTINCT node_id) FILTER (WHERE received_at > NOW() - INTERVAL '1 hour') as active_nodes_1h,
                COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '1 minute') as events_last_minute,
                COUNT(*) FILTER (WHERE event_type = 42 AND received_at > NOW() - INTERVAL '1 hour') as blocks_authored_1h,
                COUNT(*) FILTER (WHERE event_type = 41 AND received_at > NOW() - INTERVAL '1 hour') as authoring_failures_1h,
                COUNT(*) FILTER (WHERE event_type = 40 AND received_at > NOW() - INTERVAL '1 hour') as authoring_attempts_1h,
                COUNT(*) FILTER (WHERE event_type IN (120,124) AND received_at > NOW() - INTERVAL '1 hour') as da_ops_1h,
                COUNT(*) FILTER (WHERE event_type = 122 AND received_at > NOW() - INTERVAL '1 hour') as da_failures_1h,
                COUNT(*) FILTER (WHERE event_type IN (105) AND received_at > NOW() - INTERVAL '1 hour') as guarantees_1h,
                COUNT(*) FILTER (WHERE event_type IN (92) AND received_at > NOW() - INTERVAL '1 hour') as wp_failures_1h,
                COUNT(*) FILTER (WHERE event_type IN (94) AND received_at > NOW() - INTERVAL '1 hour') as wp_received_1h
            FROM events
            WHERE received_at > NOW() - INTERVAL '1 hour'
            "#,
        )
        .fetch_one(&self.pool)
        .await?;

        let active_nodes_1m = stats.get::<i64, _>("active_nodes_1m");
        let active_nodes_1h = stats.get::<i64, _>("active_nodes_1h");
        let events_per_min = stats.get::<i64, _>("events_last_minute");
        let blocks_authored = stats.get::<i64, _>("blocks_authored_1h");
        let auth_failures = stats.get::<i64, _>("authoring_failures_1h");
        let auth_attempts = stats.get::<i64, _>("authoring_attempts_1h");
        let da_ops = stats.get::<i64, _>("da_ops_1h");
        let da_failures = stats.get::<i64, _>("da_failures_1h");
        let guarantees = stats.get::<i64, _>("guarantees_1h");
        let wp_failures = stats.get::<i64, _>("wp_failures_1h");
        let wp_received = stats.get::<i64, _>("wp_received_1h");

        // ── Component: Node Connectivity ──
        // Score: % of 1h nodes still active in last 1m
        let connectivity_score = if active_nodes_1h > 0 {
            ((active_nodes_1m as f64 / active_nodes_1h as f64) * 100.0).min(100.0)
        } else {
            0.0
        };
        let connectivity_status = if connectivity_score >= 80.0 {
            "healthy"
        } else if connectivity_score >= 50.0 {
            "degraded"
        } else {
            "unhealthy"
        };
        let mut connectivity_issues: Vec<&str> = vec![];
        if active_nodes_1m == 0 {
            connectivity_issues.push("No nodes reporting in last minute");
        } else if connectivity_score < 80.0 {
            connectivity_issues.push("Some nodes have gone offline");
        }

        // ── Component: Block Production ──
        let block_score = if auth_attempts > 0 {
            let success_rate = blocks_authored as f64 / auth_attempts as f64;
            (success_rate * 100.0).clamp(0.0, 100.0)
        } else if events_per_min > 0 {
            // No authoring attempts but events flowing — may just be no slots assigned
            70.0
        } else {
            0.0
        };
        let block_status = if block_score >= 80.0 {
            "healthy"
        } else if block_score >= 50.0 {
            "degraded"
        } else {
            "unhealthy"
        };
        let mut block_issues: Vec<&str> = vec![];
        if auth_failures > 0 {
            block_issues.push("Authoring failures detected");
        }
        if auth_attempts > 0 && blocks_authored == 0 {
            block_issues.push("No blocks produced despite attempts");
        }

        // ── Component: Data Availability ──
        let da_score = if da_ops > 0 {
            let success_rate = 1.0 - (da_failures as f64 / da_ops as f64);
            (success_rate * 100.0).clamp(0.0, 100.0)
        } else {
            50.0
        };
        let da_status = if da_score >= 90.0 {
            "healthy"
        } else if da_score >= 60.0 {
            "degraded"
        } else {
            "unhealthy"
        };
        let mut da_issues: Vec<&str> = vec![];
        if da_failures > 0 {
            da_issues.push("Shard request failures detected");
        }

        // ── Component: Work Package Pipeline ──
        let wp_total = wp_received + wp_failures;
        let wp_score = if wp_total > 0 {
            let success_rate = wp_received as f64 / wp_total as f64;
            (success_rate * 100.0).clamp(0.0, 100.0)
        } else {
            50.0 // no WPs — neutral
        };
        let wp_status = if wp_score >= 90.0 {
            "healthy"
        } else if wp_score >= 60.0 {
            "degraded"
        } else {
            "unhealthy"
        };
        let mut wp_issues: Vec<&str> = vec![];
        if wp_failures > 0 {
            wp_issues.push("Work package failures detected");
        }

        // ── Component: Event Throughput ──
        let throughput_score = if events_per_min > 100 {
            100.0
        } else if events_per_min > 10 {
            80.0
        } else if events_per_min > 0 {
            50.0
        } else {
            0.0
        };
        let throughput_status = if throughput_score >= 80.0 {
            "healthy"
        } else if throughput_score >= 50.0 {
            "degraded"
        } else {
            "unhealthy"
        };
        let mut throughput_issues: Vec<&str> = vec![];
        if events_per_min == 0 {
            throughput_issues.push("No events received");
        } else if events_per_min < 10 {
            throughput_issues.push("Low event throughput");
        }

        // ── Overall health ──
        let scores = [
            connectivity_score,
            block_score,
            da_score,
            wp_score,
            throughput_score,
        ];
        let health_score = scores.iter().sum::<f64>() / scores.len() as f64;
        let overall_health = if health_score >= 75.0 {
            "healthy"
        } else if health_score >= 45.0 {
            "degraded"
        } else {
            "unhealthy"
        };

        // ── Alerts ──
        let mut alerts: Vec<serde_json::Value> = vec![];
        let now = chrono::Utc::now();
        if active_nodes_1m == 0 {
            alerts.push(serde_json::json!({
                "severity": "critical", "message": "No nodes reporting events",
                "component": "connectivity", "timestamp": now, "acknowledged": false
            }));
        }
        if auth_failures > 3 {
            alerts.push(serde_json::json!({
                "severity": "warning",
                "message": format!("{} block authoring failures in last hour", auth_failures),
                "component": "block_production", "timestamp": now, "acknowledged": false
            }));
        }
        if da_failures > 5 {
            alerts.push(serde_json::json!({
                "severity": "warning",
                "message": format!("{} DA shard request failures in last hour", da_failures),
                "component": "data_availability", "timestamp": now, "acknowledged": false
            }));
        }
        if wp_failures > 0 {
            alerts.push(serde_json::json!({
                "severity": "warning",
                "message": format!("{} work package failures in last hour", wp_failures),
                "component": "work_packages", "timestamp": now, "acknowledged": false
            }));
        }

        Ok(serde_json::json!({
            "overall_health": overall_health,
            "health_score": health_score,
            "components": [
                {
                    "name": "Node Connectivity",
                    "status": connectivity_status,
                    "score": connectivity_score.round() as i64,
                    "metrics": { "active_1m": active_nodes_1m, "active_1h": active_nodes_1h },
                    "issues": connectivity_issues,
                },
                {
                    "name": "Block Production",
                    "status": block_status,
                    "score": block_score.round() as i64,
                    "metrics": { "authored": blocks_authored, "attempts": auth_attempts, "failures": auth_failures },
                    "issues": block_issues,
                },
                {
                    "name": "Data Availability",
                    "status": da_status,
                    "score": da_score.round() as i64,
                    "metrics": { "operations": da_ops, "failures": da_failures },
                    "issues": da_issues,
                },
                {
                    "name": "Work Packages",
                    "status": wp_status,
                    "score": wp_score.round() as i64,
                    "metrics": { "received": wp_received, "failures": wp_failures, "guarantees": guarantees },
                    "issues": wp_issues,
                },
                {
                    "name": "Event Throughput",
                    "status": throughput_status,
                    "score": throughput_score.round() as i64,
                    "metrics": { "events_per_minute": events_per_min },
                    "issues": throughput_issues,
                },
            ],
            "alerts": alerts,
            "recommendations": [],
        }))
    }

    // ========================================================================
    // LOW PRIORITY: Additional Analytics & Timeline Endpoints
    // ========================================================================

    /// Get time-series metrics with optional group_by parameter.
    /// Supports grouping by: node, core, event_type
    pub async fn get_timeseries_grouped(
        &self,
        metric: &str,
        group_by: &str,
        interval_minutes: i32,
        duration_hours: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let interval = format!("{} minutes", interval_minutes);
        let duration = format!("{} hours", duration_hours);

        match (metric, group_by) {
            ("events", "node") => {
                let data: Vec<serde_json::Value> = sqlx::query_scalar(
                    r#"
                    WITH bucketed AS (
                        SELECT
                            date_trunc('minute', received_at) -
                                (EXTRACT(MINUTE FROM received_at)::int % $3) * interval '1 minute' AS bucket,
                            node_id,
                            event_type
                        FROM events
                        WHERE received_at > NOW() - $1::interval
                    )
                    SELECT jsonb_build_object(
                        'time', bucket,
                        'node_id', node_id,
                        'events', COUNT(*),
                        'event_types', COUNT(DISTINCT event_type)
                    )
                    FROM bucketed
                    GROUP BY bucket, node_id
                    ORDER BY bucket DESC, COUNT(*) DESC
                    LIMIT 1000
                    "#,
                )
                .bind(&duration)
                .bind(&interval)
                .bind(interval_minutes)
                .fetch_all(&self.pool)
                .await?;

                Ok(serde_json::json!({
                    "metric": metric,
                    "group_by": group_by,
                    "interval_minutes": interval_minutes,
                    "duration_hours": duration_hours,
                    "data": data,
                }))
            }
            ("events", "event_type") => {
                let data: Vec<serde_json::Value> = sqlx::query_scalar(
                    r#"
                    WITH bucketed AS (
                        SELECT
                            date_trunc('minute', received_at) -
                                (EXTRACT(MINUTE FROM received_at)::int % $3) * interval '1 minute' AS bucket,
                            event_type,
                            node_id
                        FROM events
                        WHERE received_at > NOW() - $1::interval
                    )
                    SELECT jsonb_build_object(
                        'time', bucket,
                        'event_type', event_type,
                        'events', COUNT(*),
                        'nodes', COUNT(DISTINCT node_id)
                    )
                    FROM bucketed
                    GROUP BY bucket, event_type
                    ORDER BY bucket DESC, COUNT(*) DESC
                    LIMIT 1000
                    "#,
                )
                .bind(&duration)
                .bind(&interval)
                .bind(interval_minutes)
                .fetch_all(&self.pool)
                .await?;

                Ok(serde_json::json!({
                    "metric": metric,
                    "group_by": group_by,
                    "interval_minutes": interval_minutes,
                    "duration_hours": duration_hours,
                    "data": data,
                }))
            }
            ("guarantees", "core") => {
                let data: Vec<serde_json::Value> = sqlx::query_scalar(
                    r#"
                    WITH bucketed AS (
                        SELECT
                            date_trunc('minute', received_at) -
                                (EXTRACT(MINUTE FROM received_at)::int % $3) * interval '1 minute' AS bucket,
                            CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER) as core_index,
                            node_id
                        FROM events
                        WHERE event_type = 105
                        AND received_at > NOW() - $1::interval
                        AND data->'GuaranteeBuilt'->'outline'->>'core' IS NOT NULL
                    )
                    SELECT jsonb_build_object(
                        'time', bucket,
                        'core_index', core_index,
                        'guarantees', COUNT(*),
                        'nodes', COUNT(DISTINCT node_id)
                    )
                    FROM bucketed
                    GROUP BY bucket, core_index
                    ORDER BY bucket DESC, COUNT(*) DESC
                    LIMIT 1000
                    "#,
                )
                .bind(&duration)
                .bind(&interval)
                .bind(interval_minutes)
                .fetch_all(&self.pool)
                .await?;

                Ok(serde_json::json!({
                    "metric": metric,
                    "group_by": group_by,
                    "interval_minutes": interval_minutes,
                    "duration_hours": duration_hours,
                    "data": data,
                }))
            }
            ("failures", "category") => {
                let data: Vec<serde_json::Value> = sqlx::query_scalar(
                    r#"
                    WITH bucketed AS (
                        SELECT
                            date_trunc('minute', received_at) -
                                (EXTRACT(MINUTE FROM received_at)::int % $3) * interval '1 minute' AS bucket,
                            CASE
                                WHEN event_type IN (41, 44, 46) THEN 'block_authoring'
                                WHEN event_type IN (94, 97, 99, 102, 109, 110, 112, 113) THEN 'work_package'
                                WHEN event_type = 81 THEN 'ticket_generation'
                                ELSE 'other'
                            END as category,
                            node_id
                        FROM events
                        WHERE event_type IN (41, 44, 46, 81, 94, 97, 99, 102, 109, 110, 112, 113)
                        AND received_at > NOW() - $1::interval
                    )
                    SELECT jsonb_build_object(
                        'time', bucket,
                        'category', category,
                        'failures', COUNT(*),
                        'nodes', COUNT(DISTINCT node_id)
                    )
                    FROM bucketed
                    GROUP BY bucket, category
                    ORDER BY bucket DESC, COUNT(*) DESC
                    LIMIT 1000
                    "#,
                )
                .bind(&duration)
                .bind(&interval)
                .bind(interval_minutes)
                .fetch_all(&self.pool)
                .await?;

                Ok(serde_json::json!({
                    "metric": metric,
                    "group_by": group_by,
                    "interval_minutes": interval_minutes,
                    "duration_hours": duration_hours,
                    "data": data,
                }))
            }
            _ => Ok(serde_json::json!({
                "error": format!("Unsupported metric/group_by combination: {}/{}", metric, group_by),
                "supported": [
                    {"metric": "events", "group_by": ["node", "event_type"]},
                    {"metric": "guarantees", "group_by": ["core"]},
                    {"metric": "failures", "group_by": ["category"]}
                ]
            })),
        }
    }

    /// Get sync status timeline - time-series of synced vs out-of-sync nodes.
    pub async fn get_sync_status_timeline(
        &self,
        duration_hours: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get sync status over time based on Status events (type 10)
        // A node is "synced" if its best block slot is close to the network max
        let timeline: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH time_buckets AS (
                SELECT generate_series(
                    date_trunc('minute', NOW() - $1::interval),
                    date_trunc('minute', NOW()),
                    '5 minutes'::interval
                ) AS bucket
            ),
            node_slots AS (
                SELECT
                    date_trunc('minute', received_at) -
                        (EXTRACT(MINUTE FROM received_at)::int % 5) * interval '1 minute' AS bucket,
                    node_id,
                    MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER)) as node_slot
                FROM events
                WHERE event_type = 11
                AND received_at > NOW() - $1::interval
                GROUP BY 1, node_id
            ),
            network_max AS (
                SELECT
                    bucket,
                    MAX(node_slot) as max_slot
                FROM node_slots
                GROUP BY bucket
            ),
            sync_status AS (
                SELECT
                    ns.bucket,
                    COUNT(DISTINCT ns.node_id) as total_nodes,
                    COUNT(DISTINCT ns.node_id) FILTER (WHERE ns.node_slot >= nm.max_slot - 2) as synced_nodes,
                    COUNT(DISTINCT ns.node_id) FILTER (WHERE ns.node_slot < nm.max_slot - 2) as behind_nodes,
                    nm.max_slot as network_slot
                FROM node_slots ns
                JOIN network_max nm ON ns.bucket = nm.bucket
                GROUP BY ns.bucket, nm.max_slot
            )
            SELECT jsonb_build_object(
                'time', tb.bucket,
                'total_nodes', COALESCE(ss.total_nodes, 0),
                'synced_nodes', COALESCE(ss.synced_nodes, 0),
                'behind_nodes', COALESCE(ss.behind_nodes, 0),
                'sync_percentage', CASE
                    WHEN COALESCE(ss.total_nodes, 0) > 0
                    THEN ROUND((ss.synced_nodes::numeric / ss.total_nodes * 100)::numeric, 1)
                    ELSE 100
                END,
                'network_slot', ss.network_slot
            )
            FROM time_buckets tb
            LEFT JOIN sync_status ss ON tb.bucket = ss.bucket
            ORDER BY tb.bucket DESC
            LIMIT 500
            "#,
        )
        .bind(format!("{} hours", duration_hours))
        .fetch_all(&self.pool)
        .await?;

        // Get current sync status per node
        let current_status: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH recent_slots AS (
                SELECT
                    node_id,
                    MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER)) as slot,
                    MAX(time) as last_update
                FROM events
                WHERE event_type = 11
                AND received_at > NOW() - INTERVAL '5 minutes'
                GROUP BY node_id
            ),
            network_max AS (
                SELECT MAX(slot) as max_slot FROM recent_slots
            )
            SELECT jsonb_build_object(
                'node_id', rs.node_id,
                'slot', rs.slot,
                'slots_behind', nm.max_slot - rs.slot,
                'is_synced', rs.slot >= nm.max_slot - 2,
                'last_update', rs.last_update
            )
            FROM recent_slots rs
            CROSS JOIN network_max nm
            ORDER BY rs.slot DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "timeline": timeline,
            "current_status": current_status,
            "duration_hours": duration_hours,
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Get connection health timeline - connection/disconnection events with MTBF.
    pub async fn get_connections_timeline(
        &self,
        duration_hours: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get connection events over time
        // Event type 1 = Connected, type 2 = Disconnected (or similar based on your schema)
        let timeline: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH time_buckets AS (
                SELECT generate_series(
                    date_trunc('minute', NOW() - $1::interval),
                    date_trunc('minute', NOW()),
                    '5 minutes'::interval
                ) AS bucket
            ),
            connection_events AS (
                SELECT
                    date_trunc('minute', received_at) -
                        (EXTRACT(MINUTE FROM received_at)::int % 5) * interval '1 minute' AS bucket,
                    COUNT(DISTINCT node_id) FILTER (WHERE event_type = 1) as connections,
                    COUNT(DISTINCT node_id) FILTER (WHERE event_type = 2) as disconnections,
                    COUNT(DISTINCT node_id) as active_nodes
                FROM events
                WHERE received_at > NOW() - $1::interval
                AND event_type IN (1, 2, 10, 11)
                GROUP BY 1
            )
            SELECT jsonb_build_object(
                'time', tb.bucket,
                'connections', COALESCE(ce.connections, 0),
                'disconnections', COALESCE(ce.disconnections, 0),
                'active_nodes', COALESCE(ce.active_nodes, 0),
                'net_change', COALESCE(ce.connections, 0) - COALESCE(ce.disconnections, 0)
            )
            FROM time_buckets tb
            LEFT JOIN connection_events ce ON tb.bucket = ce.bucket
            ORDER BY tb.bucket DESC
            LIMIT 500
            "#,
        )
        .bind(format!("{} hours", duration_hours))
        .fetch_all(&self.pool)
        .await?;

        // Get per-node connection stats
        let by_node: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH node_activity AS (
                SELECT
                    node_id,
                    MIN(time) as first_seen,
                    MAX(time) as last_seen,
                    COUNT(DISTINCT DATE(time)) as days_active,
                    COUNT(*) as total_events
                FROM events
                WHERE received_at > NOW() - $1::interval
                GROUP BY node_id
            )
            SELECT jsonb_build_object(
                'node_id', node_id,
                'first_seen', first_seen,
                'last_seen', last_seen,
                'uptime_hours', EXTRACT(EPOCH FROM (last_seen - first_seen)) / 3600,
                'days_active', days_active,
                'total_events', total_events,
                'events_per_hour', total_events::float / GREATEST(EXTRACT(EPOCH FROM (last_seen - first_seen)) / 3600, 1)
            )
            FROM node_activity
            ORDER BY last_seen DESC
            "#,
        )
        .bind(format!("{} hours", duration_hours))
        .fetch_all(&self.pool)
        .await?;

        // Calculate overall connection health metrics
        let health_stats = sqlx::query(
            r#"
            SELECT
                COUNT(DISTINCT node_id) as total_nodes_seen,
                COUNT(DISTINCT node_id) FILTER (WHERE received_at > NOW() - INTERVAL '5 minutes') as currently_active,
                COUNT(DISTINCT node_id) FILTER (WHERE received_at > NOW() - INTERVAL '1 hour') as active_last_hour
            FROM events
            WHERE received_at > NOW() - $1::interval
            "#,
        )
        .bind(format!("{} hours", duration_hours))
        .fetch_one(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "timeline": timeline,
            "by_node": by_node,
            "health_stats": {
                "total_nodes_seen": health_stats.get::<i64, _>("total_nodes_seen"),
                "currently_active": health_stats.get::<i64, _>("currently_active"),
                "active_last_hour": health_stats.get::<i64, _>("active_last_hour"),
            },
            "duration_hours": duration_hours,
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Get aggregated metrics for WebSocket streaming.
    /// Lightweight query designed for frequent polling (1-second intervals).
    pub async fn get_aggregated_metrics(&self) -> Result<serde_json::Value, sqlx::Error> {
        let metrics = sqlx::query(
            r#"
            SELECT
                -- Last second
                COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '1 second') as events_1s,
                COUNT(*) FILTER (WHERE event_type = 11 AND received_at > NOW() - INTERVAL '1 second') as blocks_1s,
                -- Last 10 seconds
                COUNT(*) FILTER (WHERE received_at > NOW() - INTERVAL '10 seconds') as events_10s,
                COUNT(*) FILTER (WHERE event_type = 11 AND received_at > NOW() - INTERVAL '10 seconds') as blocks_10s,
                COUNT(*) FILTER (WHERE event_type = 12 AND received_at > NOW() - INTERVAL '10 seconds') as finalized_10s,
                COUNT(DISTINCT node_id) FILTER (WHERE received_at > NOW() - INTERVAL '10 seconds') as nodes_10s,
                -- Failures last minute
                COUNT(*) FILTER (WHERE event_type IN (41, 44, 46, 81, 83, 92, 99, 107, 111, 113, 122, 127)
                    AND received_at > NOW() - INTERVAL '1 minute') as failures_1m,
                -- Work packages last minute
                COUNT(*) FILTER (WHERE event_type BETWEEN 90 AND 113
                    AND received_at > NOW() - INTERVAL '1 minute') as wp_events_1m,
                -- Latest slots
                MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER)) FILTER (WHERE event_type = 11) as latest_slot,
                MAX(CAST(data->'FinalizedBlockChanged'->>'slot' AS INTEGER)) FILTER (WHERE event_type = 12) as finalized_slot
            FROM events
            WHERE received_at > NOW() - INTERVAL '1 minute'
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

        // Check for high failure rates
        let failure_check = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_events,
                COUNT(*) FILTER (WHERE event_type IN (41, 44, 46, 81, 94, 97, 99, 102, 109, 110, 112, 113)) as failures
            FROM events
            WHERE received_at > NOW() - INTERVAL '5 minutes'
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

        // Check for dropped events
        let dropped_check = sqlx::query(
            r#"
            SELECT
                node_id,
                COUNT(*) as dropped
            FROM events
            WHERE event_type = 0
            AND received_at > NOW() - INTERVAL '5 minutes'
            GROUP BY node_id
            HAVING COUNT(*) > 10
            ORDER BY COUNT(*) DESC
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

        // Check for nodes falling behind
        let sync_check: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH recent_slots AS (
                SELECT
                    node_id,
                    MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER)) as slot
                FROM events
                WHERE event_type = 11
                AND received_at > NOW() - INTERVAL '2 minutes'
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

        // Check for inactive nodes (were active but stopped)
        let inactive_check = sqlx::query(
            r#"
            SELECT
                node_id,
                MAX(time) as last_seen
            FROM events
            WHERE received_at > NOW() - INTERVAL '1 hour'
            GROUP BY node_id
            HAVING MAX(time) < NOW() - INTERVAL '5 minutes'
            AND MAX(time) > NOW() - INTERVAL '10 minutes'
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

    // ========================================================================
    // Dashboard Priority Endpoints - Replacing Mock Data
    // ========================================================================

    /// Get validators assigned to a specific core with their node IDs.
    /// Derives validator-to-core mapping from guarantee and ticket events.
    pub async fn get_core_validators(
        &self,
        core_index: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get validators who have processed work packages for this core.
        // Start from WorkPackageReceived (94) which has the core field,
        // then find all nodes that participated via submission_or_share_id linkage.
        let validators: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH core_wp_ids AS (
                SELECT DISTINCT (data->'WorkPackageReceived'->>'submission_or_share_id') as wp_id
                FROM events
                WHERE event_type = 94
                AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '1 hour'
            ),
            core_events AS (
                SELECT e.node_id, e.event_type, e.time
                FROM events e
                INNER JOIN core_wp_ids c ON (
                    e.data->'WorkPackageReceived'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'Authorized'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'Refined'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'WorkReportBuilt'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'WorkPackageFailed'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'GuaranteeBuilt'->>'submission_id' = c.wp_id
                    OR e.data->'GuaranteesDistributed'->>'submission_id' = c.wp_id
                )
                WHERE e.event_type IN (94, 95, 101, 102, 105, 109, 92)
                AND e.received_at > NOW() - INTERVAL '1 hour'
            ),
            validator_activity AS (
                SELECT
                    node_id,
                    COUNT(*) as event_count,
                    MAX(time) as last_active,
                    MIN(time) as first_seen
                FROM core_events
                GROUP BY node_id
            ),
            node_info AS (
                SELECT node_id, implementation_name, implementation_version
                FROM nodes
            )
            SELECT jsonb_build_object(
                'node_id', va.node_id,
                'validator_index', ROW_NUMBER() OVER (ORDER BY va.event_count DESC) - 1,
                'guarantee_count', va.event_count,
                'last_active', va.last_active,
                'latest_slot', 0,
                'implementation_name', COALESCE(ni.implementation_name, 'unknown'),
                'implementation_version', COALESCE(ni.implementation_version, 'unknown'),
                'is_active', va.last_active > NOW() - INTERVAL '2 minutes'
            )
            FROM validator_activity va
            LEFT JOIN node_info ni ON va.node_id = ni.node_id
            WHERE va.event_count > 0
            ORDER BY va.event_count DESC
            "#,
        )
        .bind(core_index)
        .fetch_all(&self.pool)
        .await?;

        // If no guarantees, get all active validators
        let all_validators: Vec<serde_json::Value> = if validators.is_empty() {
            sqlx::query_scalar(
                r#"
                WITH active_nodes AS (
                    SELECT
                        n.node_id,
                        n.implementation_name,
                        n.implementation_version,
                        n.last_seen_at as last_active,
                        (SELECT MAX(CAST(data->'BestBlockChanged'->>'slot' AS INTEGER))
                         FROM events WHERE node_id = n.node_id AND event_type = 11
                         AND received_at > NOW() - INTERVAL '5 minutes') as latest_slot
                    FROM nodes n
                    WHERE n.is_connected = true
                )
                SELECT jsonb_build_object(
                    'node_id', node_id,
                    'validator_index', ROW_NUMBER() OVER (ORDER BY node_id) - 1,
                    'guarantee_count', 0,
                    'last_active', last_active,
                    'latest_slot', COALESCE(latest_slot, 0),
                    'implementation_name', COALESCE(implementation_name, 'unknown'),
                    'implementation_version', COALESCE(implementation_version, 'unknown'),
                    'is_active', true
                )
                FROM active_nodes
                "#,
            )
            .fetch_all(&self.pool)
            .await?
        } else {
            validators
        };

        Ok(serde_json::json!({
            "core_index": core_index,
            "validators": all_validators,
            "validator_count": all_validators.len(),
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Get real-time performance metrics for a specific core.
    pub async fn get_core_metrics(
        &self,
        core_index: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get processing metrics for this core (1 hour window).
        // Start from WorkPackageReceived (94) which has the core field,
        // then count downstream events linked via submission_or_share_id.
        let metrics = sqlx::query(
            r#"
            WITH core_wp_ids AS (
                SELECT DISTINCT (data->'WorkPackageReceived'->>'submission_or_share_id') as wp_id
                FROM events
                WHERE event_type = 94
                AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '1 hour'
            ),
            core_events AS (
                SELECT e.event_type, e.node_id, e.data
                FROM events e
                INNER JOIN core_wp_ids c ON (
                    e.data->'WorkPackageReceived'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'Authorized'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'Refined'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'WorkReportBuilt'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'WorkPackageFailed'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'GuaranteeBuilt'->>'submission_id' = c.wp_id
                    OR e.data->'GuaranteesDistributed'->>'submission_id' = c.wp_id
                )
                WHERE e.event_type IN (92, 94, 95, 101, 102, 105, 109)
                AND e.received_at > NOW() - INTERVAL '1 hour'
            )
            SELECT
                COUNT(*) FILTER (WHERE event_type = 94) as wps_received,
                COUNT(*) FILTER (WHERE event_type = 101) as refinements_completed,
                COUNT(*) FILTER (WHERE event_type = 92) as refinements_failed,
                COUNT(*) FILTER (WHERE event_type = 102) as reports_built,
                COUNT(*) FILTER (WHERE event_type = 105) as guarantees_built,
                COUNT(*) FILTER (WHERE event_type = 109) as guarantees_distributed,
                COUNT(DISTINCT node_id) as active_validators
            FROM core_events
            "#,
        )
        .bind(core_index)
        .fetch_one(&self.pool)
        .await?;

        let wps_received: i64 = metrics.get("wps_received");
        let refinements_completed: i64 = metrics.get("refinements_completed");
        let refinements_failed: i64 = metrics.get("refinements_failed");
        let reports_built: i64 = metrics.get("reports_built");
        let guarantees_built: i64 = metrics.get("guarantees_built");
        let guarantees_distributed: i64 = metrics.get("guarantees_distributed");

        // Calculate efficiency: refinements_completed / (completed + failed)
        let total_refinements = refinements_completed + refinements_failed;
        let processing_efficiency = if total_refinements > 0 {
            (refinements_completed as f64 / total_refinements as f64) * 100.0
        } else if wps_received > 0 {
            0.0 // WPs received but none refined
        } else {
            100.0 // No data
        };

        // Accumulate efficiency: guarantees distributed / guarantees built
        let accumulate_efficiency = if guarantees_built > 0 {
            (guarantees_distributed as f64 / guarantees_built as f64) * 100.0
        } else {
            100.0
        };

        // Get gas usage from Refined (101) events — costs[].total.gas_used
        // Also get gas limits from WorkPackageReceived (94) — outline.work_items[].refine_gas_limit
        let gas = sqlx::query(
            r#"
            WITH core_wp_ids AS (
                SELECT DISTINCT (data->'WorkPackageReceived'->>'submission_or_share_id') as wp_id
                FROM events
                WHERE event_type = 94
                AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '1 hour'
            ),
            refined_gas AS (
                SELECT
                    (SELECT COALESCE(SUM((c->'total'->>'gas_used')::BIGINT), 0)
                     FROM jsonb_array_elements(e.data->'Refined'->'costs') c
                    ) as gas_used
                FROM events e
                INNER JOIN core_wp_ids c ON e.data->'Refined'->>'submission_or_share_id' = c.wp_id
                WHERE e.event_type = 101
                AND e.received_at > NOW() - INTERVAL '1 hour'
            ),
            wp_gas_limits AS (
                SELECT
                    (SELECT COALESCE(SUM((wi->>'refine_gas_limit')::BIGINT), 0)
                     FROM jsonb_array_elements(e.data->'WorkPackageReceived'->'outline'->'work_items') wi
                    ) as gas_limit
                FROM events e
                WHERE e.event_type = 94
                AND CAST(e.data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
                AND e.received_at > NOW() - INTERVAL '1 hour'
            )
            SELECT
                COALESCE(AVG(rg.gas_used), 0)::FLOAT8 as avg_gas_used,
                COALESCE(SUM(rg.gas_used), 0)::BIGINT as total_gas_used,
                COALESCE(AVG(wl.gas_limit), 0)::FLOAT8 as avg_gas_limit
            FROM refined_gas rg
            FULL OUTER JOIN (SELECT AVG(gas_limit) as gas_limit FROM wp_gas_limits) wl ON true
            "#,
        )
        .bind(core_index)
        .fetch_one(&self.pool)
        .await?;

        let avg_gas_used: f64 = gas.get("avg_gas_used");
        let avg_gas_limit: f64 = gas.get("avg_gas_limit");
        let gas_utilization_pct = if avg_gas_limit > 0.0 {
            (avg_gas_used / avg_gas_limit) * 100.0
        } else {
            0.0
        };

        // Get latency metrics using received_at (wall clock), NOT time (JCE epoch).
        // Measures time from WorkPackageReceived to each downstream stage.
        let latency = sqlx::query(
            r#"
            WITH core_received AS (
                SELECT
                    (data->'WorkPackageReceived'->>'submission_or_share_id') as wp_id,
                    received_at as received_at
                FROM events
                WHERE event_type = 94
                AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '1 hour'
            ),
            wp_durations AS (
                SELECT
                    EXTRACT(EPOCH FROM (MAX(e.received_at) - r.received_at)) * 1000 as completion_time_ms
                FROM core_received r
                INNER JOIN events e ON (
                    e.received_at > NOW() - INTERVAL '1 hour'
                    AND e.event_type IN (95, 101, 102, 105, 109)
                    AND (
                        e.data->'Authorized'->>'submission_or_share_id' = r.wp_id
                        OR e.data->'Refined'->>'submission_or_share_id' = r.wp_id
                        OR e.data->'WorkReportBuilt'->>'submission_or_share_id' = r.wp_id
                        OR e.data->'GuaranteeBuilt'->>'submission_id' = r.wp_id
                        OR e.data->'GuaranteesDistributed'->>'submission_id' = r.wp_id
                    )
                )
                GROUP BY r.wp_id, r.received_at
            )
            SELECT
                COALESCE(AVG(completion_time_ms), 0)::FLOAT8 as avg_completion_ms,
                COALESCE(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY completion_time_ms), 0)::FLOAT8 as p95_completion_ms,
                COUNT(*) as sample_count
            FROM wp_durations
            WHERE completion_time_ms > 0 AND completion_time_ms < 300000
            "#,
        )
        .bind(core_index)
        .fetch_one(&self.pool)
        .await?;

        let avg_completion_ms: f64 = latency.get("avg_completion_ms");
        let p95_completion_ms: f64 = latency.get("p95_completion_ms");
        let sample_count: i64 = latency.get("sample_count");

        // 24h WP count
        let wps_24h: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM events
            WHERE event_type = 94
            AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
            AND received_at > NOW() - INTERVAL '24 hours'
            "#,
        )
        .bind(core_index)
        .fetch_one(&self.pool)
        .await?;

        // Throughput: WPs received per second in the 1h window
        let throughput = wps_received as f64 / 3600.0;

        Ok(serde_json::json!({
            "core_index": core_index,
            "processing_efficiency_pct": processing_efficiency,
            "accumulate_efficiency_pct": accumulate_efficiency,
            "network_latency_ms": avg_completion_ms,
            "p95_latency_ms": p95_completion_ms,
            "throughput_per_second": throughput,
            "average_completion_time_ms": avg_completion_ms,
            "gas_utilization_pct": gas_utilization_pct,
            "work_packages_processed_24h": wps_24h,
            "stats": {
                "wps_received": wps_received,
                "refinements_completed": refinements_completed,
                "refinements_failed": refinements_failed,
                "reports_built": reports_built,
                "guarantees_built": guarantees_built,
                "guarantees_distributed": guarantees_distributed,
                "active_validators": metrics.get::<i64, _>("active_validators"),
                "avg_gas_used": avg_gas_used as i64,
                "avg_gas_limit": avg_gas_limit as i64,
            },
            "sample_count": sample_count,
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Get audit progress for a specific work package.
    pub async fn get_workpackage_audit_progress(
        &self,
        wp_hash: &str,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get audit-related events for this work package
        let audit_events: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'event_type', event_type,
                'event_name', CASE event_type
                    WHEN 105 THEN 'GuaranteeBuilt'
                    WHEN 106 THEN 'GuaranteeSigned'
                    WHEN 107 THEN 'GuaranteeShared'
                    WHEN 108 THEN 'Accumulated'
                    WHEN 109 THEN 'AccumulateFailed'
                    WHEN 110 THEN 'AccumulateIgnored'
                    ELSE 'Unknown'
                END,
                'node_id', node_id,
                'time', time,
                'tranche', COALESCE(
                    data->'GuaranteeBuilt'->'outline'->>'tranche',
                    data->>'tranche'
                ),
                'core_index', COALESCE(
                    CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER),
                    CAST(data->>'core' AS INTEGER)
                )
            )
            FROM events
            WHERE event_type BETWEEN 105 AND 113
            AND (
                data->'GuaranteeBuilt'->'outline'->>'hash' = $1
                OR data->'GuaranteeSigned'->>'hash' = $1
                OR data->'GuaranteeShared'->>'hash' = $1
                OR data->'Accumulated'->>'hash' = $1
                OR data->>'hash' = $1
            )
            AND received_at > NOW() - INTERVAL '24 hours'
            ORDER BY time
            "#,
        )
        .bind(wp_hash)
        .fetch_all(&self.pool)
        .await?;

        // Calculate audit statistics
        let guarantees_built = audit_events
            .iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_i64()) == Some(105))
            .count();

        let guarantees_signed = audit_events
            .iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_i64()) == Some(106))
            .count();

        let guarantees_shared = audit_events
            .iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_i64()) == Some(107))
            .count();

        let accumulated = audit_events
            .iter()
            .any(|e| e.get("event_type").and_then(|v| v.as_i64()) == Some(108));

        let failed = audit_events.iter().any(|e| {
            let et = e.get("event_type").and_then(|v| v.as_i64());
            et == Some(109) || et == Some(110)
        });

        // Get unique auditors (validators who signed)
        let auditors: Vec<&str> = audit_events
            .iter()
            .filter(|e| e.get("event_type").and_then(|v| v.as_i64()) == Some(106))
            .filter_map(|e| e.get("node_id").and_then(|v| v.as_str()))
            .collect();

        // Determine tranche from events
        let tranche = audit_events
            .iter()
            .find_map(|e| e.get("tranche").and_then(|v| v.as_i64()))
            .unwrap_or(0);

        // Calculate completion status
        let status = if accumulated {
            "completed"
        } else if failed {
            "failed"
        } else if guarantees_shared > 0 {
            "sharing"
        } else if guarantees_signed > 0 {
            "signing"
        } else if guarantees_built > 0 {
            "building"
        } else {
            "pending"
        };

        // Panic mode detection (simplified - based on multiple failed attempts)
        let panic_mode = failed && audit_events.len() > 5;

        Ok(serde_json::json!({
            "hash": wp_hash,
            "status": status,
            "tranche": tranche,
            "audit_progress": {
                "guarantees_built": guarantees_built,
                "guarantees_signed": guarantees_signed,
                "guarantees_shared": guarantees_shared,
                "auditors_completed": auditors.len(),
                "auditor_node_ids": auditors,
            },
            "panic_mode": panic_mode,
            "is_accumulated": accumulated,
            "is_failed": failed,
            "events": audit_events,
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Detect bottlenecks and slow validators for a specific core.
    pub async fn get_core_bottlenecks(
        &self,
        core_index: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Find slow validators based on processing times.
        // Start from WorkPackageReceived (94) which has the core field,
        // then find downstream events linked via submission_or_share_id.
        let slow_validators: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH core_wp_ids AS (
                SELECT DISTINCT (data->'WorkPackageReceived'->>'submission_or_share_id') as wp_id
                FROM events
                WHERE event_type = 94
                AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '1 hour'
            ),
            core_events AS (
                SELECT e.node_id, e.event_type, e.time
                FROM events e
                INNER JOIN core_wp_ids c ON (
                    e.data->'WorkPackageReceived'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'Authorized'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'Refined'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'WorkReportBuilt'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'WorkPackageFailed'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'GuaranteeBuilt'->>'submission_id' = c.wp_id
                    OR e.data->'GuaranteesDistributed'->>'submission_id' = c.wp_id
                )
                WHERE e.event_type IN (94, 95, 101, 102, 105, 109, 92)
                AND e.received_at > NOW() - INTERVAL '1 hour'
            ),
            event_lags AS (
                SELECT
                    node_id,
                    event_type,
                    time,
                    LAG(time) OVER (PARTITION BY node_id ORDER BY time) as prev_timestamp
                FROM core_events
            ),
            validator_times AS (
                SELECT
                    node_id,
                    COUNT(*) as event_count,
                    COUNT(*) FILTER (WHERE event_type IN (92)) as failure_count,
                    AVG(EXTRACT(EPOCH FROM (time - prev_timestamp)) * 1000) as avg_processing_ms
                FROM event_lags
                WHERE prev_timestamp IS NOT NULL
                GROUP BY node_id
                HAVING COUNT(*) > 5
            ),
            network_avg AS (
                SELECT AVG(avg_processing_ms) as network_avg_ms FROM validator_times
            )
            SELECT jsonb_build_object(
                'node_id', vt.node_id,
                'avg_processing_ms', vt.avg_processing_ms,
                'event_count', vt.event_count,
                'failure_count', vt.failure_count,
                'failure_rate', ROUND((vt.failure_count::numeric / vt.event_count)::numeric, 3),
                'slowdown_factor', ROUND((vt.avg_processing_ms / NULLIF(na.network_avg_ms, 0))::numeric, 2),
                'is_bottleneck', vt.avg_processing_ms > na.network_avg_ms * 1.5 OR vt.failure_count::float / vt.event_count > 0.1
            )
            FROM validator_times vt
            CROSS JOIN network_avg na
            WHERE vt.avg_processing_ms > na.network_avg_ms * 1.2 OR vt.failure_count::float / vt.event_count > 0.05
            ORDER BY vt.avg_processing_ms DESC
            LIMIT 10
            "#,
        )
        .bind(core_index)
        .fetch_all(&self.pool)
        .await?;

        // Get overall bottleneck statistics for this core
        let bottleneck_stats = sqlx::query(
            r#"
            WITH core_wp_ids AS (
                SELECT DISTINCT (data->'WorkPackageReceived'->>'submission_or_share_id') as wp_id
                FROM events
                WHERE event_type = 94
                AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '1 hour'
            ),
            core_events AS (
                SELECT e.event_type, e.node_id
                FROM events e
                INNER JOIN core_wp_ids c ON (
                    e.data->'WorkPackageReceived'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'Authorized'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'Refined'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'WorkReportBuilt'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'WorkPackageFailed'->>'submission_or_share_id' = c.wp_id
                    OR e.data->'GuaranteeBuilt'->>'submission_id' = c.wp_id
                    OR e.data->'GuaranteesDistributed'->>'submission_id' = c.wp_id
                )
                WHERE e.event_type IN (94, 95, 101, 102, 105, 109, 92)
                AND e.received_at > NOW() - INTERVAL '1 hour'
            )
            SELECT
                COUNT(*) as total_events,
                COUNT(*) FILTER (WHERE event_type = 92) as total_failures,
                COUNT(DISTINCT node_id) as validator_count
            FROM core_events
            "#,
        )
        .bind(core_index)
        .fetch_one(&self.pool)
        .await?;

        let total_events: i64 = bottleneck_stats.get("total_events");
        let total_failures: i64 = bottleneck_stats.get("total_failures");

        // Generate bottleneck messages
        let mut messages: Vec<serde_json::Value> = Vec::new();

        for validator in &slow_validators {
            if let (Some(node_id), Some(slowdown)) = (
                validator.get("node_id").and_then(|v| v.as_str()),
                validator.get("slowdown_factor").and_then(|v| v.as_f64()),
            ) {
                if slowdown > 1.5 {
                    messages.push(serde_json::json!({
                        "severity": "warning",
                        "type": "slow_validator",
                        "message": format!("Validator {}... is {:.1}x slower than average", &node_id[..8], slowdown),
                        "node_id": node_id
                    }));
                }
            }

            if let (Some(node_id), Some(failure_rate)) = (
                validator.get("node_id").and_then(|v| v.as_str()),
                validator.get("failure_rate").and_then(|v| v.as_f64()),
            ) {
                if failure_rate > 0.1 {
                    messages.push(serde_json::json!({
                        "severity": "error",
                        "type": "high_failure_rate",
                        "message": format!("Validator {}... has {:.0}% failure rate", &node_id[..8], failure_rate * 100.0),
                        "node_id": node_id
                    }));
                }
            }
        }

        Ok(serde_json::json!({
            "core_index": core_index,
            "slow_validators": slow_validators,
            "bottleneck_messages": messages,
            "stats": {
                "total_events": total_events,
                "total_failures": total_failures,
                "failure_rate": if total_events > 0 { total_failures as f64 / total_events as f64 } else { 0.0 },
                "validator_count": bottleneck_stats.get::<i64, _>("validator_count"),
                "bottleneck_count": slow_validators.len(),
            },
            "timestamp": chrono::Utc::now(),
        }))
    }

    /// Get enhanced guarantor information with import sharing data.
    pub async fn get_core_guarantors_with_sharing(
        &self,
        core_index: i32,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Get guarantors with their activity.
        // Start from WorkPackageReceived (94) which has the core field,
        // then find GuaranteeBuilt events linked via submission_id.
        let guarantors: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH core_wp_ids AS (
                SELECT DISTINCT (data->'WorkPackageReceived'->>'submission_or_share_id') as wp_id
                FROM events
                WHERE event_type = 94
                AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
                AND received_at > NOW() - INTERVAL '24 hours'
            ),
            guarantee_activity AS (
                SELECT
                    e.node_id,
                    COUNT(*) as guarantee_count,
                    MAX(e.time) as last_guarantee
                FROM events e
                INNER JOIN core_wp_ids c ON e.data->'GuaranteeBuilt'->>'submission_id' = c.wp_id
                WHERE e.event_type = 105
                AND e.received_at > NOW() - INTERVAL '24 hours'
                GROUP BY e.node_id
            ),
            shard_activity AS (
                SELECT
                    node_id,
                    COUNT(*) FILTER (WHERE event_type = 121) as shards_requested,
                    COUNT(*) FILTER (WHERE event_type = 123) as shards_stored,
                    COUNT(*) FILTER (WHERE event_type = 124) as shards_transferred,
                    COALESCE(SUM(CAST(data->'ShardStored'->>'size' AS BIGINT)) FILTER (WHERE event_type = 123), 0) as bytes_stored,
                    COALESCE(SUM(CAST(data->'ShardTransferred'->>'size' AS BIGINT)) FILTER (WHERE event_type = 124), 0) as bytes_transferred
                FROM events
                WHERE event_type IN (121, 123, 124)
                AND received_at > NOW() - INTERVAL '24 hours'
                GROUP BY node_id
            ),
            import_exports AS (
                SELECT
                    e1.node_id as sender_id,
                    e2.node_id as receiver_id,
                    COUNT(*) as transfer_count,
                    COALESCE(SUM(CAST(e1.data->'ShardTransferred'->>'size' AS BIGINT)), 0) as bytes_sent
                FROM events e1
                JOIN events e2 ON e1.data->'ShardTransferred'->>'shard_index' = e2.data->'ShardRequested'->>'shard_index'
                    AND e2.event_type = 121
                    AND e1.event_type = 124
                    AND e2.time < e1.time
                    AND e1.time < e2.time + INTERVAL '10 seconds'
                WHERE e1.received_at > NOW() - INTERVAL '1 hour'
                GROUP BY e1.node_id, e2.node_id
            )
            SELECT jsonb_build_object(
                'node_id', ga.node_id,
                'guarantee_count', ga.guarantee_count,
                'last_activity', ga.last_guarantee,
                'da_usage_bytes', COALESCE(sa.bytes_stored, 0),
                'shards_stored', COALESCE(sa.shards_stored, 0),
                'shards_served', COALESCE(sa.shards_transferred, 0),
                'bytes_served', COALESCE(sa.bytes_transferred, 0),
                'import_efficiency', CASE
                    WHEN COALESCE(sa.shards_requested, 0) > 0
                    THEN ROUND((COALESCE(sa.shards_stored, 0)::numeric / sa.shards_requested)::numeric, 3)
                    ELSE 1.0
                END,
                'export_efficiency', CASE
                    WHEN COALESCE(sa.shards_requested, 0) > 0
                    THEN ROUND((COALESCE(sa.shards_transferred, 0)::numeric / sa.shards_requested)::numeric, 3)
                    ELSE 1.0
                END
            )
            FROM guarantee_activity ga
            LEFT JOIN shard_activity sa ON ga.node_id = sa.node_id
            ORDER BY ga.guarantee_count DESC
            "#,
        )
        .bind(core_index)
        .fetch_all(&self.pool)
        .await?;

        // Get import sharing matrix
        let import_sharing: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH transfers AS (
                SELECT
                    node_id as from_node,
                    data->'ShardTransferred'->>'to_peer' as to_node,
                    COUNT(*) as count,
                    COALESCE(SUM(CAST(data->'ShardTransferred'->>'size' AS BIGINT)), 0) as bytes
                FROM events
                WHERE event_type = 124
                AND received_at > NOW() - INTERVAL '1 hour'
                AND data->'ShardTransferred'->>'to_peer' IS NOT NULL
                GROUP BY node_id, data->'ShardTransferred'->>'to_peer'
            )
            SELECT jsonb_build_object(
                'from_node', from_node,
                'to_node', to_node,
                'transfer_count', count,
                'bytes_transferred', bytes
            )
            FROM transfers
            WHERE count > 0
            ORDER BY count DESC
            LIMIT 50
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        // Calculate totals
        let total_da_bytes: i64 = guarantors
            .iter()
            .filter_map(|g| g.get("da_usage_bytes").and_then(|v| v.as_i64()))
            .sum();

        Ok(serde_json::json!({
            "core_index": core_index,
            "guarantors": guarantors,
            "import_sharing": {
                "transfers": import_sharing,
                "sent": import_sharing.iter()
                    .filter_map(|t| t.get("from_node").and_then(|v| v.as_str()))
                    .collect::<Vec<_>>(),
                "received": import_sharing.iter()
                    .filter_map(|t| t.get("to_node").and_then(|v| v.as_str()))
                    .collect::<Vec<_>>(),
            },
            "core_total_da_bytes": total_da_bytes,
            "active_guarantor_count": guarantors.len(),
            "timestamp": chrono::Utc::now(),
        }))
    }

    // ========================================================================
    // Frontend Search & Explorer Endpoints
    // ========================================================================

    /// Multi-criteria event search with pagination.
    /// Supports filtering by event_types, node_id, core_index, wp_hash, and time range.
    #[allow(clippy::too_many_arguments)]
    pub async fn search_events(
        &self,
        event_types: Option<&[i32]>,
        node_id: Option<&str>,
        core_index: Option<i32>,
        wp_hash: Option<&str>,
        start_time: Option<DateTime<Utc>>,
        end_time: Option<DateTime<Utc>>,
        limit: i64,
        offset: i64,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let events: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            SELECT jsonb_build_object(
                'time', e.time,
                'node_id', e.node_id,
                'event_type', e.event_type,
                'timestamp', e.time,
                'received_at', e.received_at,
                'data', e.data
            )
            FROM events e
            WHERE ($1::integer[] IS NULL OR e.event_type = ANY($1))
            AND ($2::text IS NULL OR e.node_id = $2)
            AND ($3::integer IS NULL OR (
                COALESCE(
                    CAST(e.data->'WorkPackageReceived'->>'core' AS INTEGER),
                    CAST(e.data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER),
                    CAST(e.data->'Refined'->>'core' AS INTEGER)
                ) = $3
            ))
            AND ($4::text IS NULL OR (
                e.data->>'hash' = $4
                OR e.data->'WorkPackageSubmitted'->>'hash' = $4
                OR e.data->'WorkPackageReceived'->>'hash' = $4
                OR e.data->'Refined'->>'hash' = $4
                OR e.data->'GuaranteeBuilt'->'outline'->>'hash' = $4
            ))
            AND ($5::timestamptz IS NULL OR e.received_at >= $5)
            AND ($6::timestamptz IS NULL OR e.received_at <= $6)
            ORDER BY e.received_at DESC
            LIMIT $7 OFFSET $8
            "#,
        )
        .bind(event_types)
        .bind(node_id)
        .bind(core_index)
        .bind(wp_hash)
        .bind(start_time)
        .bind(end_time)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;

        // Get total count for pagination
        let total: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM events e
            WHERE ($1::integer[] IS NULL OR e.event_type = ANY($1))
            AND ($2::text IS NULL OR e.node_id = $2)
            AND ($3::integer IS NULL OR (
                COALESCE(
                    CAST(e.data->'WorkPackageReceived'->>'core' AS INTEGER),
                    CAST(e.data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER),
                    CAST(e.data->'Refined'->>'core' AS INTEGER)
                ) = $3
            ))
            AND ($4::text IS NULL OR (
                e.data->>'hash' = $4
                OR e.data->'WorkPackageSubmitted'->>'hash' = $4
                OR e.data->'WorkPackageReceived'->>'hash' = $4
                OR e.data->'Refined'->>'hash' = $4
                OR e.data->'GuaranteeBuilt'->'outline'->>'hash' = $4
            ))
            AND ($5::timestamptz IS NULL OR e.received_at >= $5)
            AND ($6::timestamptz IS NULL OR e.received_at <= $6)
            "#,
        )
        .bind(event_types)
        .bind(node_id)
        .bind(core_index)
        .bind(wp_hash)
        .bind(start_time)
        .bind(end_time)
        .fetch_one(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "events": events,
            "total": total,
            "limit": limit,
            "offset": offset,
            "has_more": (offset + limit) < total,
            "timestamp": chrono::Utc::now(),
        }))
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
                SELECT event_id, event_type, node_id, received_at, data
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
                AND received_at > NOW() - INTERVAL '7 days'
            ),
            slot_authoring AS (
                SELECT event_id, node_id, received_at
                FROM direct_slot_events
                WHERE event_type = 40
            ),
            linked_events AS (
                SELECT next_evt.event_id, next_evt.event_type, next_evt.node_id, next_evt.received_at, next_evt.data
                FROM slot_authoring sa
                CROSS JOIN LATERAL (
                    SELECT e.event_id, e.event_type, e.node_id, e.received_at, e.data
                    FROM events e
                    WHERE e.node_id = sa.node_id
                    AND e.event_type IN (41, 42)
                    AND e.received_at > sa.received_at
                    ORDER BY e.received_at ASC
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
                'first_event', MIN(received_at),
                'last_event', MAX(received_at)
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
                    SELECT event_id, event_type, node_id, time, data
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
                    AND received_at > NOW() - INTERVAL '7 days'
                ),
                slot_authoring AS (
                    SELECT event_id, node_id, time
                    FROM direct_slot_events
                    WHERE event_type = 40
                ),
                linked_events AS (
                    SELECT next_evt.event_id, next_evt.event_type, next_evt.node_id, next_evt.time, next_evt.data
                    FROM slot_authoring sa
                    CROSS JOIN LATERAL (
                        SELECT e.event_id, e.event_type, e.node_id, e.time, e.data
                        FROM events e
                        WHERE e.node_id = sa.node_id
                        AND e.event_type IN (41, 42)
                        AND e.time > sa.time
                        ORDER BY e.time ASC
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
                            'time', time,
                            'data', data
                        ) ORDER BY time
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

    /// Get validator activity timeline with time range and category filtering.
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
                'time', time,
                'received_at', received_at,
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
            AND ($2::timestamptz IS NULL OR received_at >= $2)
            AND ($3::timestamptz IS NULL OR received_at <= $3)
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
            ORDER BY received_at DESC
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
            AND ($2::timestamptz IS NULL OR received_at >= $2)
            AND ($3::timestamptz IS NULL OR received_at <= $3)
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
            let earliest = events.last().and_then(|e| e.get("received_at").cloned());
            let latest = events.first().and_then(|e| e.get("received_at").cloned());
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

    /// Batch work package journey lookup for multiple hashes.
    pub async fn batch_workpackage_journeys(
        &self,
        hashes: &[String],
    ) -> Result<serde_json::Value, sqlx::Error> {
        if hashes.is_empty() {
            return Ok(serde_json::json!({
                "journeys": [],
                "timestamp": chrono::Utc::now(),
            }));
        }

        // Limit batch size to prevent abuse
        let hashes: Vec<&str> = hashes.iter().take(50).map(|h| h.as_str()).collect();

        let journeys: Vec<serde_json::Value> = sqlx::query_scalar(
            r#"
            WITH wp_events AS (
                SELECT
                    COALESCE(
                        data->>'hash',
                        data->'WorkPackageSubmitted'->>'hash',
                        data->'WorkPackageReceived'->>'hash',
                        data->'Refined'->>'hash',
                        data->'RefinementFailed'->>'hash',
                        data->'GuaranteeBuilt'->'outline'->>'hash',
                        data->'GuaranteeSigned'->>'hash',
                        data->'GuaranteeShared'->>'hash',
                        data->'Accumulated'->>'hash'
                    ) as wp_hash,
                    event_type,
                    node_id,
                    time,
                    received_at,
                    data
                FROM events
                WHERE event_type BETWEEN 90 AND 113
                AND (
                    data->>'hash' = ANY($1)
                    OR data->'WorkPackageSubmitted'->>'hash' = ANY($1)
                    OR data->'WorkPackageReceived'->>'hash' = ANY($1)
                    OR data->'Refined'->>'hash' = ANY($1)
                    OR data->'RefinementFailed'->>'hash' = ANY($1)
                    OR data->'GuaranteeBuilt'->'outline'->>'hash' = ANY($1)
                    OR data->'GuaranteeSigned'->>'hash' = ANY($1)
                    OR data->'GuaranteeShared'->>'hash' = ANY($1)
                    OR data->'Accumulated'->>'hash' = ANY($1)
                )
                AND received_at > NOW() - INTERVAL '24 hours'
            )
            SELECT jsonb_build_object(
                'hash', wp_hash,
                'stages', jsonb_agg(
                    jsonb_build_object(
                        'stage', CASE event_type
                            WHEN 90 THEN 'submitted'
                            WHEN 91 THEN 'being_shared'
                            WHEN 92 THEN 'failed'
                            WHEN 94 THEN 'received'
                            WHEN 95 THEN 'authorized'
                            WHEN 101 THEN 'refined'
                            WHEN 102 THEN 'report_built'
                            WHEN 105 THEN 'guarantee_built'
                            WHEN 107 THEN 'guarantee_sent'
                            WHEN 108 THEN 'accumulated'
                            ELSE 'other'
                        END,
                        'status', CASE
                            WHEN event_type IN (92, 93, 99, 112, 113) THEN 'failed'
                            WHEN event_type IN (91, 98, 100, 103, 106) THEN 'in_progress'
                            ELSE 'completed'
                        END,
                        'event_type', event_type,
                        'node_id', node_id,
                        'time', time,
                        'core_index', COALESCE(
                            CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER),
                            CAST(data->'Refined'->>'core' AS INTEGER),
                            CAST(data->'WorkPackageReceived'->>'core' AS INTEGER)
                        )
                    ) ORDER BY time
                ),
                'stage_count', COUNT(*),
                'has_errors', bool_or(event_type IN (92, 93, 99, 112, 113)),
                'first_seen', MIN(time),
                'last_seen', MAX(time),
                'nodes_involved', COUNT(DISTINCT node_id),
                'current_stage', (
                    SELECT CASE event_type
                        WHEN 90 THEN 'submitted'
                        WHEN 91 THEN 'being_shared'
                        WHEN 92 THEN 'failed'
                        WHEN 94 THEN 'received'
                        WHEN 95 THEN 'authorized'
                        WHEN 101 THEN 'refined'
                        WHEN 102 THEN 'report_built'
                        WHEN 105 THEN 'guarantee_built'
                        WHEN 107 THEN 'guarantee_sent'
                        WHEN 108 THEN 'accumulated'
                        ELSE 'other'
                    END
                    FROM wp_events w2
                    WHERE w2.wp_hash = wp_events.wp_hash
                    ORDER BY w2.time DESC
                    LIMIT 1
                )
            )
            FROM wp_events
            WHERE wp_hash IS NOT NULL
            GROUP BY wp_hash
            "#,
        )
        .bind(&hashes as &[&str])
        .fetch_all(&self.pool)
        .await?;

        Ok(serde_json::json!({
            "journeys": journeys,
            "requested": hashes.len(),
            "found": journeys.len(),
            "timestamp": chrono::Utc::now(),
        }))
    }
}

