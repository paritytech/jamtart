use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::store::EventStore;

/// Whitelisted time_bucket intervals for dynamic SQL.
const VALID_INTERVALS: &[&str] = &[
    "10s", "15s", "30s", "1m", "2m", "5m", "10m", "15m", "30m", "1h", "2h", "4h", "6h", "12h",
    "1d",
];

/// Whitelisted group_by columns for dynamic SQL.
const VALID_GROUP_BY: &[&str] = &["node_id", "event_type", "core"];

/// Whitelisted aggregate table names for dynamic SQL.
const VALID_TABLES: &[&str] = &[
    "event_stats_30s",
    "event_stats_1m",
    "event_stats_1h",
    "core_stats_1m",
];

/// Convert a human-friendly interval string to seconds for table selection.
fn interval_to_seconds(interval: &str) -> Option<i64> {
    let s = interval.trim();
    if let Some(n) = s.strip_suffix('s') {
        return n.parse::<i64>().ok();
    }
    if let Some(n) = s.strip_suffix('m') {
        return n.parse::<i64>().ok().map(|v| v * 60);
    }
    if let Some(n) = s.strip_suffix('h') {
        return n.parse::<i64>().ok().map(|v| v * 3600);
    }
    if let Some(n) = s.strip_suffix('d') {
        return n.parse::<i64>().ok().map(|v| v * 86400);
    }
    None
}

/// Convert interval shorthand (e.g. "5m") to PostgreSQL interval literal (e.g. "5 minutes").
fn interval_to_pg(interval: &str) -> String {
    let s = interval.trim();
    if let Some(n) = s.strip_suffix('s') {
        return format!("{n} seconds");
    }
    if let Some(n) = s.strip_suffix('m') {
        return format!("{n} minutes");
    }
    if let Some(n) = s.strip_suffix('h') {
        return format!("{n} hours");
    }
    if let Some(n) = s.strip_suffix('d') {
        return format!("{n} days");
    }
    // Fallback: pass through (should not happen with validated intervals)
    s.to_string()
}

impl EventStore {
    // ── 1. grafana_timeseries ──────────────────────────────────────────

    /// Time-series query with automatic aggregate table selection.
    ///
    /// Auto-selects:
    ///   - `core_stats_1m`    if group_by = "core"
    ///   - `event_stats_30s`  if interval < 60s
    ///   - `event_stats_1m`   if interval < 3600s
    ///   - `event_stats_1h`   otherwise
    pub async fn grafana_timeseries(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
        group_by: Option<&str>,
        node: Option<&str>,
        event_types: Option<&[i16]>,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Validate interval
        if !VALID_INTERVALS.contains(&interval) {
            return Err(sqlx::Error::Protocol(format!(
                "invalid interval: {interval}"
            )));
        }
        // Validate group_by
        if let Some(gb) = group_by {
            if !VALID_GROUP_BY.contains(&gb) {
                return Err(sqlx::Error::Protocol(format!(
                    "invalid group_by: {gb}"
                )));
            }
        }

        let interval_secs = interval_to_seconds(interval).unwrap_or(60);
        let pg_interval = interval_to_pg(interval);

        // Select aggregate table
        let table = if group_by == Some("core") {
            "core_stats_1m"
        } else if interval_secs < 60 {
            "event_stats_30s"
        } else if interval_secs < 3600 {
            "event_stats_1m"
        } else {
            "event_stats_1h"
        };

        // Safety: table is from a hardcoded set
        if !VALID_TABLES.contains(&table) {
            return Err(sqlx::Error::Protocol(format!(
                "invalid table: {table}"
            )));
        }

        // Build SELECT columns
        let group_col = group_by.unwrap_or("event_type");
        let select_group = if group_col == "core" {
            "core".to_string()
        } else {
            group_col.to_string()
        };

        // Build WHERE clauses
        let mut wheres = vec![
            "bucket >= $1".to_string(),
            "bucket < $2".to_string(),
        ];
        let mut bind_idx = 3u32;

        if node.is_some() && table != "core_stats_1m" {
            wheres.push(format!("node_id = ${bind_idx}"));
            bind_idx += 1;
        }

        if event_types.is_some() {
            wheres.push(format!("event_type = ANY(${bind_idx})"));
            // bind_idx += 1; // last bind
        }

        let where_clause = wheres.join(" AND ");

        // Build query — all interpolated identifiers are validated above
        let sql = format!(
            r#"
            SELECT
                time_bucket('{pg_interval}', bucket) AS ts,
                {select_group},
                SUM(event_count)::BIGINT AS count
            FROM {table}
            WHERE {where_clause}
            GROUP BY ts, {select_group}
            ORDER BY ts ASC
            "#,
        );

        let mut query = sqlx::query(&sql)
            .bind(start)
            .bind(end);

        if let Some(n) = node {
            if table != "core_stats_1m" {
                query = query.bind(n.to_string());
            }
        }
        if let Some(types) = event_types {
            query = query.bind(types.to_vec());
        }

        let rows = query.fetch_all(self.pool()).await?;

        let results: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let ts: DateTime<Utc> = row.get("ts");
                let count: i64 = row.get("count");
                let mut obj = serde_json::json!({
                    "ts": ts,
                    "count": count,
                });
                // Add the group column
                if group_col == "core" {
                    let core: Option<i16> = row.try_get("core").ok();
                    obj["core"] = serde_json::json!(core);
                } else if group_col == "event_type" {
                    let et: i16 = row.get("event_type");
                    obj["event_type"] = serde_json::json!(et);
                } else if group_col == "node_id" {
                    let nid: String = row.get("node_id");
                    obj["node_id"] = serde_json::json!(nid);
                }
                obj
            })
            .collect();

        Ok(serde_json::Value::Array(results))
    }

    // ── 2. grafana_stats ───────────────────────────────────────────────

    /// Dashboard summary stats: connected nodes, current slot, guarantees, failures, WP events.
    pub async fn grafana_stats(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let row = sqlx::query(
            r#"
            SELECT
                (SELECT COUNT(*)::INT FROM nodes WHERE is_connected = true) AS connected_nodes,
                COALESCE((
                    SELECT MAX(event_count)::BIGINT
                    FROM event_stats_1m
                    WHERE bucket >= $1 AND bucket < $2
                      AND event_type = 42
                ), 0) AS slot_events,
                COALESCE((
                    SELECT SUM(event_count)::BIGINT
                    FROM event_stats_1m
                    WHERE bucket >= $1 AND bucket < $2
                      AND event_type = 105
                ), 0) AS guarantees,
                COALESCE((
                    SELECT SUM(event_count)::BIGINT
                    FROM event_stats_1m
                    WHERE bucket >= $1 AND bucket < $2
                      AND event_type = 92
                ), 0) AS failures,
                COALESCE((
                    SELECT SUM(event_count)::BIGINT
                    FROM event_stats_1m
                    WHERE bucket >= $1 AND bucket < $2
                      AND event_type = 94
                ), 0) AS wp_events
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        Ok(serde_json::json!({
            "connected_nodes": row.get::<i32, _>("connected_nodes"),
            "slot_events": row.get::<i64, _>("slot_events"),
            "guarantees": row.get::<i64, _>("guarantees"),
            "failures": row.get::<i64, _>("failures"),
            "wp_events": row.get::<i64, _>("wp_events"),
        }))
    }

    // ── 3. grafana_cores ───────────────────────────────────────────────

    /// Per-core activity summary or detail (with WP tracking for a single core).
    pub async fn grafana_cores(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        core_filter: Option<i16>,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Summary: aggregate per core
        let rows = sqlx::query(
            r#"
            SELECT
                core,
                SUM(event_count) FILTER (WHERE event_type = 94)::BIGINT  AS work_packages,
                SUM(event_count) FILTER (WHERE event_type = 105)::BIGINT AS guarantees,
                SUM(event_count) FILTER (WHERE event_type = 92)::BIGINT  AS failures
            FROM core_stats_1m
            WHERE bucket >= $1 AND bucket < $2
              AND ($3::SMALLINT IS NULL OR core = $3)
            GROUP BY core
            ORDER BY core ASC
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(core_filter)
        .fetch_all(self.pool())
        .await?;

        let mut cores: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "core": row.get::<i16, _>("core"),
                    "work_packages": row.get::<Option<i64>, _>("work_packages").unwrap_or(0),
                    "guarantees": row.get::<Option<i64>, _>("guarantees").unwrap_or(0),
                    "failures": row.get::<Option<i64>, _>("failures").unwrap_or(0),
                })
            })
            .collect();

        // Detail mode: attach recent WP tracking data for the filtered core
        if let Some(core) = core_filter {
            let wps = sqlx::query(
                r#"
                SELECT
                    encode(wp_hash, 'hex') AS wp_hash,
                    first_seen,
                    last_updated,
                    stage,
                    received_by,
                    guaranteed_by,
                    service_ids,
                    received_at,
                    authorized_at,
                    refined_at,
                    report_built_at,
                    guarantee_built_at,
                    distributed_at,
                    failed_at
                FROM wp_tracking
                WHERE core = $1
                  AND first_seen >= $2 AND first_seen < $3
                ORDER BY first_seen DESC
                LIMIT 100
                "#,
            )
            .bind(core)
            .bind(start)
            .bind(end)
            .fetch_all(self.pool())
            .await?;

            let wp_list: Vec<serde_json::Value> = wps
                .iter()
                .map(|row| {
                    serde_json::json!({
                        "wp_hash": row.get::<String, _>("wp_hash"),
                        "first_seen": row.get::<DateTime<Utc>, _>("first_seen"),
                        "last_updated": row.get::<DateTime<Utc>, _>("last_updated"),
                        "stage": row.get::<i16, _>("stage"),
                        "received_by": row.get::<i16, _>("received_by"),
                        "guaranteed_by": row.get::<i16, _>("guaranteed_by"),
                        "service_ids": row.get::<Vec<i32>, _>("service_ids"),
                        "received_at": row.get::<Option<DateTime<Utc>>, _>("received_at"),
                        "authorized_at": row.get::<Option<DateTime<Utc>>, _>("authorized_at"),
                        "refined_at": row.get::<Option<DateTime<Utc>>, _>("refined_at"),
                        "report_built_at": row.get::<Option<DateTime<Utc>>, _>("report_built_at"),
                        "guarantee_built_at": row.get::<Option<DateTime<Utc>>, _>("guarantee_built_at"),
                        "distributed_at": row.get::<Option<DateTime<Utc>>, _>("distributed_at"),
                        "failed_at": row.get::<Option<DateTime<Utc>>, _>("failed_at"),
                    })
                })
                .collect();

            // Attach WP list to the single core entry
            if let Some(entry) = cores.first_mut() {
                entry["recent_work_packages"] = serde_json::Value::Array(wp_list);
            }
        }

        Ok(serde_json::Value::Array(cores))
    }

    // ── 4. grafana_blocks_convergence ──────────────────────────────────

    /// Block propagation convergence data per slot.
    pub async fn grafana_blocks_convergence(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                slot,
                event_type,
                node_count,
                p50_ms,
                p99_ms,
                p100_ms,
                authored_at
            FROM slot_convergence
            WHERE authored_at >= $1 AND authored_at < $2
            ORDER BY slot ASC, event_type ASC
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        let results: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "slot": row.get::<i32, _>("slot"),
                    "event_type": row.get::<i16, _>("event_type"),
                    "node_count": row.get::<i16, _>("node_count"),
                    "p50_ms": row.get::<i32, _>("p50_ms"),
                    "p99_ms": row.get::<i32, _>("p99_ms"),
                    "p100_ms": row.get::<i32, _>("p100_ms"),
                    "authored_at": row.get::<DateTime<Utc>, _>("authored_at"),
                })
            })
            .collect();

        Ok(serde_json::Value::Array(results))
    }

    // ── 5. grafana_blocks_contents ─────────────────────────────────────

    /// Block contents extracted from BlockAuthored (event_type=42) JSONB data.
    pub async fn grafana_blocks_contents(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                slot,
                timestamp,
                node_id,
                (data->'outline'->>'num_guarantees')::INT   AS num_guarantees,
                (data->'outline'->>'num_assurances')::INT   AS num_assurances,
                (data->'outline'->>'num_preimages')::INT    AS num_preimages,
                (data->'outline'->>'num_tickets')::INT      AS num_tickets,
                (data->'outline'->>'num_disputes')::INT     AS num_disputes,
                (data->'outline'->>'extrinsic_size')::INT   AS extrinsic_size
            FROM events
            WHERE event_type = 42
              AND slot IS NOT NULL
              AND timestamp >= $1 AND timestamp < $2
            ORDER BY slot ASC
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        let results: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "slot": row.get::<i32, _>("slot"),
                    "timestamp": row.get::<DateTime<Utc>, _>("timestamp"),
                    "node_id": row.get::<String, _>("node_id"),
                    "num_guarantees": row.get::<Option<i32>, _>("num_guarantees"),
                    "num_assurances": row.get::<Option<i32>, _>("num_assurances"),
                    "num_preimages": row.get::<Option<i32>, _>("num_preimages"),
                    "num_tickets": row.get::<Option<i32>, _>("num_tickets"),
                    "num_disputes": row.get::<Option<i32>, _>("num_disputes"),
                    "extrinsic_size": row.get::<Option<i32>, _>("extrinsic_size"),
                })
            })
            .collect();

        Ok(serde_json::Value::Array(results))
    }

    // ── 6. grafana_services ────────────────────────────────────────────

    /// Per-service stats from the service_stats_1m continuous aggregate.
    pub async fn grafana_services(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                service_id,
                SUM(event_count) FILTER (WHERE event_type = 94)::BIGINT  AS work_packages,
                SUM(event_count) FILTER (WHERE event_type = 101)::BIGINT AS refinements,
                SUM(total_gas) FILTER (WHERE event_type = 101)::BIGINT   AS refinement_gas,
                SUM(event_count) FILTER (WHERE event_type = 95)::BIGINT  AS authorizations,
                SUM(total_gas) FILTER (WHERE event_type = 95)::BIGINT    AS authorization_gas,
                SUM(event_count) FILTER (WHERE event_type = 47)::BIGINT  AS executions,
                SUM(total_gas) FILTER (WHERE event_type = 47)::BIGINT    AS execution_gas
            FROM service_stats_1m
            WHERE bucket >= $1 AND bucket < $2
            GROUP BY service_id
            ORDER BY service_id ASC
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        let results: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "service_id": row.get::<i32, _>("service_id"),
                    "work_packages": row.get::<Option<i64>, _>("work_packages").unwrap_or(0),
                    "refinements": row.get::<Option<i64>, _>("refinements").unwrap_or(0),
                    "refinement_gas": row.get::<Option<i64>, _>("refinement_gas").unwrap_or(0),
                    "authorizations": row.get::<Option<i64>, _>("authorizations").unwrap_or(0),
                    "authorization_gas": row.get::<Option<i64>, _>("authorization_gas").unwrap_or(0),
                    "executions": row.get::<Option<i64>, _>("executions").unwrap_or(0),
                    "execution_gas": row.get::<Option<i64>, _>("execution_gas").unwrap_or(0),
                })
            })
            .collect();

        Ok(serde_json::Value::Array(results))
    }

    // ── 7. grafana_nodes ───────────────────────────────────────────────

    /// All nodes from the nodes table.
    pub async fn grafana_nodes(&self) -> Result<serde_json::Value, sqlx::Error> {
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
                event_count + total_events AS total_event_count,
                address
            FROM nodes
            ORDER BY is_connected DESC, last_seen_at DESC
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        let results: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "node_id": row.get::<String, _>("node_id"),
                    "peer_id": row.get::<String, _>("peer_id"),
                    "implementation_name": row.get::<String, _>("implementation_name"),
                    "implementation_version": row.get::<String, _>("implementation_version"),
                    "node_info": row.get::<serde_json::Value, _>("node_info"),
                    "connected_at": row.get::<DateTime<Utc>, _>("connected_at"),
                    "disconnected_at": row.get::<Option<DateTime<Utc>>, _>("disconnected_at"),
                    "last_seen_at": row.get::<DateTime<Utc>, _>("last_seen_at"),
                    "is_connected": row.get::<bool, _>("is_connected"),
                    "total_event_count": row.get::<i64, _>("total_event_count"),
                    "address": row.get::<Option<String>, _>("address"),
                })
            })
            .collect();

        Ok(serde_json::Value::Array(results))
    }

    // ── 8. grafana_node_stats ──────────────────────────────────────────

    /// Raw rows from the node_stats hypertable, optionally filtered by node(s).
    pub async fn grafana_node_stats(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        nodes: Option<&[String]>,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let sql = if nodes.is_some() {
            r#"
            SELECT
                timestamp, node_id,
                num_peers, num_val_peers, num_sync_peers,
                num_shards, shards_size,
                num_preimages, preimages_size,
                min_guarantees, max_guarantees, avg_guarantees, zero_guarantee_cores
            FROM node_stats
            WHERE timestamp >= $1 AND timestamp < $2
              AND node_id = ANY($3)
            ORDER BY timestamp ASC
            "#
        } else {
            r#"
            SELECT
                timestamp, node_id,
                num_peers, num_val_peers, num_sync_peers,
                num_shards, shards_size,
                num_preimages, preimages_size,
                min_guarantees, max_guarantees, avg_guarantees, zero_guarantee_cores
            FROM node_stats
            WHERE timestamp >= $1 AND timestamp < $2
            ORDER BY timestamp ASC
            "#
        };

        let rows = if let Some(node_list) = nodes {
            sqlx::query(sql)
                .bind(start)
                .bind(end)
                .bind(node_list)
                .fetch_all(self.pool())
                .await?
        } else {
            sqlx::query(sql)
                .bind(start)
                .bind(end)
                .fetch_all(self.pool())
                .await?
        };

        let results: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "timestamp": row.get::<DateTime<Utc>, _>("timestamp"),
                    "node_id": row.get::<String, _>("node_id"),
                    "num_peers": row.get::<i32, _>("num_peers"),
                    "num_val_peers": row.get::<i32, _>("num_val_peers"),
                    "num_sync_peers": row.get::<i32, _>("num_sync_peers"),
                    "num_shards": row.get::<i32, _>("num_shards"),
                    "shards_size": row.get::<i64, _>("shards_size"),
                    "num_preimages": row.get::<i32, _>("num_preimages"),
                    "preimages_size": row.get::<i32, _>("preimages_size"),
                    "min_guarantees": row.get::<i16, _>("min_guarantees"),
                    "max_guarantees": row.get::<i16, _>("max_guarantees"),
                    "avg_guarantees": row.get::<f32, _>("avg_guarantees"),
                    "zero_guarantee_cores": row.get::<i16, _>("zero_guarantee_cores"),
                })
            })
            .collect();

        Ok(serde_json::Value::Array(results))
    }

    // ── 9. grafana_node_stats_aggregate ────────────────────────────────

    /// Aggregated node stats from node_stats_1m. Network-wide when no node filter.
    pub async fn grafana_node_stats_aggregate(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        nodes: Option<&[String]>,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Network-wide: aggregate across all nodes per bucket
        let (sql, has_node_filter) = if let Some(ref _n) = nodes {
            (
                r#"
                SELECT
                    bucket,
                    node_id,
                    avg_peers, min_peers, max_peers,
                    avg_val_peers, min_val_peers, max_val_peers,
                    avg_sync_peers, min_sync_peers, max_sync_peers,
                    avg_shards, min_shards, max_shards,
                    avg_shards_size, max_shards_size,
                    avg_preimages, max_preimages,
                    avg_preimages_size, max_preimages_size,
                    avg_guarantees, min_guarantees, max_guarantees,
                    max_zero_guarantee_cores,
                    status_count
                FROM node_stats_1m
                WHERE bucket >= $1 AND bucket < $2
                  AND node_id = ANY($3)
                ORDER BY bucket ASC, node_id ASC
                "#,
                true,
            )
        } else {
            (
                r#"
                SELECT
                    bucket,
                    AVG(avg_peers)::INT         AS avg_peers,
                    MIN(min_peers)              AS min_peers,
                    MAX(max_peers)              AS max_peers,
                    AVG(avg_val_peers)::INT     AS avg_val_peers,
                    MIN(min_val_peers)          AS min_val_peers,
                    MAX(max_val_peers)          AS max_val_peers,
                    AVG(avg_sync_peers)::INT    AS avg_sync_peers,
                    MIN(min_sync_peers)         AS min_sync_peers,
                    MAX(max_sync_peers)         AS max_sync_peers,
                    AVG(avg_shards)::INT        AS avg_shards,
                    MIN(min_shards)             AS min_shards,
                    MAX(max_shards)             AS max_shards,
                    AVG(avg_shards_size)::BIGINT AS avg_shards_size,
                    MAX(max_shards_size)        AS max_shards_size,
                    AVG(avg_preimages)::INT     AS avg_preimages,
                    MAX(max_preimages)          AS max_preimages,
                    AVG(avg_preimages_size)::INT AS avg_preimages_size,
                    MAX(max_preimages_size)     AS max_preimages_size,
                    AVG(avg_guarantees)         AS avg_guarantees,
                    MIN(min_guarantees)         AS min_guarantees,
                    MAX(max_guarantees)         AS max_guarantees,
                    MAX(max_zero_guarantee_cores) AS max_zero_guarantee_cores,
                    SUM(status_count)::BIGINT   AS status_count
                FROM node_stats_1m
                WHERE bucket >= $1 AND bucket < $2
                GROUP BY bucket
                ORDER BY bucket ASC
                "#,
                false,
            )
        };

        let rows = if has_node_filter {
            sqlx::query(sql)
                .bind(start)
                .bind(end)
                .bind(nodes.unwrap())
                .fetch_all(self.pool())
                .await?
        } else {
            sqlx::query(sql)
                .bind(start)
                .bind(end)
                .fetch_all(self.pool())
                .await?
        };

        let results: Vec<serde_json::Value> = rows
            .iter()
            .map(|row| {
                let mut obj = serde_json::json!({
                    "bucket": row.get::<DateTime<Utc>, _>("bucket"),
                    "avg_peers": row.get::<i32, _>("avg_peers"),
                    "min_peers": row.get::<i32, _>("min_peers"),
                    "max_peers": row.get::<i32, _>("max_peers"),
                    "avg_val_peers": row.get::<i32, _>("avg_val_peers"),
                    "min_val_peers": row.get::<i32, _>("min_val_peers"),
                    "max_val_peers": row.get::<i32, _>("max_val_peers"),
                    "avg_sync_peers": row.get::<i32, _>("avg_sync_peers"),
                    "min_sync_peers": row.get::<i32, _>("min_sync_peers"),
                    "max_sync_peers": row.get::<i32, _>("max_sync_peers"),
                    "avg_shards": row.get::<i32, _>("avg_shards"),
                    "min_shards": row.get::<i32, _>("min_shards"),
                    "max_shards": row.get::<i32, _>("max_shards"),
                    "avg_shards_size": row.get::<i64, _>("avg_shards_size"),
                    "max_shards_size": row.get::<i64, _>("max_shards_size"),
                    "avg_preimages": row.get::<i32, _>("avg_preimages"),
                    "max_preimages": row.get::<i32, _>("max_preimages"),
                    "avg_preimages_size": row.get::<i32, _>("avg_preimages_size"),
                    "max_preimages_size": row.get::<i32, _>("max_preimages_size"),
                    "avg_guarantees": row.get::<f64, _>("avg_guarantees"),
                    "min_guarantees": row.get::<i16, _>("min_guarantees"),
                    "max_guarantees": row.get::<i16, _>("max_guarantees"),
                    "max_zero_guarantee_cores": row.get::<i16, _>("max_zero_guarantee_cores"),
                    "status_count": row.get::<i64, _>("status_count"),
                });
                if has_node_filter {
                    obj["node_id"] = serde_json::json!(row.get::<String, _>("node_id"));
                }
                obj
            })
            .collect();

        Ok(serde_json::Value::Array(results))
    }

    // ── 10. grafana_db_stats ───────────────────────────────────────────

    /// TimescaleDB metadata: table sizes, row counts, compression stats.
    pub async fn grafana_db_stats(&self) -> Result<serde_json::Value, sqlx::Error> {
        // Hypertable sizes
        let table_rows = sqlx::query(
            r#"
            SELECT
                hypertable_name::TEXT AS table_name,
                total_bytes::BIGINT,
                table_bytes::BIGINT,
                index_bytes::BIGINT,
                toast_bytes::BIGINT
            FROM hypertable_detailed_size('events')
            UNION ALL
            SELECT
                hypertable_name::TEXT,
                total_bytes::BIGINT,
                table_bytes::BIGINT,
                index_bytes::BIGINT,
                toast_bytes::BIGINT
            FROM hypertable_detailed_size('node_stats')
            UNION ALL
            SELECT
                hypertable_name::TEXT,
                total_bytes::BIGINT,
                table_bytes::BIGINT,
                index_bytes::BIGINT,
                toast_bytes::BIGINT
            FROM hypertable_detailed_size('event_services')
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        let tables: Vec<serde_json::Value> = table_rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "table_name": row.get::<String, _>("table_name"),
                    "total_bytes": row.get::<i64, _>("total_bytes"),
                    "table_bytes": row.get::<i64, _>("table_bytes"),
                    "index_bytes": row.get::<i64, _>("index_bytes"),
                    "toast_bytes": row.get::<i64, _>("toast_bytes"),
                })
            })
            .collect();

        // Approximate row counts
        let count_rows = sqlx::query(
            r#"
            SELECT
                'events'::TEXT AS table_name,
                approximate_row_count('events') AS row_count
            UNION ALL
            SELECT 'node_stats', approximate_row_count('node_stats')
            UNION ALL
            SELECT 'event_services', approximate_row_count('event_services')
            UNION ALL
            SELECT 'wp_tracking', (SELECT COUNT(*) FROM wp_tracking)
            UNION ALL
            SELECT 'slot_convergence', (SELECT COUNT(*) FROM slot_convergence)
            UNION ALL
            SELECT 'nodes', (SELECT COUNT(*) FROM nodes)
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        let row_counts: Vec<serde_json::Value> = count_rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "table_name": row.get::<String, _>("table_name"),
                    "row_count": row.get::<i64, _>("row_count"),
                })
            })
            .collect();

        // Compression stats
        let compression_rows = sqlx::query(
            r#"
            SELECT
                hypertable_name::TEXT AS table_name,
                number_compressed_chunks::BIGINT,
                before_compression_total_bytes::BIGINT,
                after_compression_total_bytes::BIGINT
            FROM chunk_compression_stats('events')
            UNION ALL
            SELECT
                hypertable_name::TEXT,
                number_compressed_chunks::BIGINT,
                before_compression_total_bytes::BIGINT,
                after_compression_total_bytes::BIGINT
            FROM chunk_compression_stats('node_stats')
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        let compression: Vec<serde_json::Value> = compression_rows
            .iter()
            .map(|row| {
                serde_json::json!({
                    "table_name": row.get::<String, _>("table_name"),
                    "compressed_chunks": row.get::<i64, _>("number_compressed_chunks"),
                    "before_compression_bytes": row.get::<i64, _>("before_compression_total_bytes"),
                    "after_compression_bytes": row.get::<i64, _>("after_compression_total_bytes"),
                })
            })
            .collect();

        Ok(serde_json::json!({
            "tables": tables,
            "row_counts": row_counts,
            "compression": compression,
        }))
    }

    // ── 11. grafana_bottlenecks ────────────────────────────────────────

    /// Pipeline bottleneck analysis from wp_tracking: percentile timing per stage.
    pub async fn grafana_bottlenecks(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        core_filter: Option<i16>,
    ) -> Result<serde_json::Value, sqlx::Error> {
        // Stage timing percentiles
        let timing_rows = sqlx::query(
            r#"
            SELECT
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (authorized_at - received_at)) * 1000
                )::DOUBLE PRECISION AS p50_authorize_ms,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (authorized_at - received_at)) * 1000
                )::DOUBLE PRECISION AS p95_authorize_ms,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (refined_at - authorized_at)) * 1000
                )::DOUBLE PRECISION AS p50_refine_ms,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (refined_at - authorized_at)) * 1000
                )::DOUBLE PRECISION AS p95_refine_ms,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (report_built_at - refined_at)) * 1000
                )::DOUBLE PRECISION AS p50_report_ms,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (report_built_at - refined_at)) * 1000
                )::DOUBLE PRECISION AS p95_report_ms,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (guarantee_built_at - report_built_at)) * 1000
                )::DOUBLE PRECISION AS p50_guarantee_ms,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (guarantee_built_at - report_built_at)) * 1000
                )::DOUBLE PRECISION AS p95_guarantee_ms,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (distributed_at - guarantee_built_at)) * 1000
                )::DOUBLE PRECISION AS p50_distribute_ms,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (distributed_at - guarantee_built_at)) * 1000
                )::DOUBLE PRECISION AS p95_distribute_ms,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (COALESCE(distributed_at, last_updated) - received_at)) * 1000
                )::DOUBLE PRECISION AS p50_pipeline_ms,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (COALESCE(distributed_at, last_updated) - received_at)) * 1000
                )::DOUBLE PRECISION AS p95_pipeline_ms
            FROM wp_tracking
            WHERE first_seen >= $1 AND first_seen < $2
              AND ($3::SMALLINT IS NULL OR core = $3)
              AND received_at IS NOT NULL
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(core_filter)
        .fetch_one(self.pool())
        .await?;

        let stage_timing = serde_json::json!({
            "authorize": {
                "p50_ms": timing_rows.get::<Option<f64>, _>("p50_authorize_ms"),
                "p95_ms": timing_rows.get::<Option<f64>, _>("p95_authorize_ms"),
            },
            "refine": {
                "p50_ms": timing_rows.get::<Option<f64>, _>("p50_refine_ms"),
                "p95_ms": timing_rows.get::<Option<f64>, _>("p95_refine_ms"),
            },
            "report": {
                "p50_ms": timing_rows.get::<Option<f64>, _>("p50_report_ms"),
                "p95_ms": timing_rows.get::<Option<f64>, _>("p95_report_ms"),
            },
            "guarantee": {
                "p50_ms": timing_rows.get::<Option<f64>, _>("p50_guarantee_ms"),
                "p95_ms": timing_rows.get::<Option<f64>, _>("p95_guarantee_ms"),
            },
            "distribute": {
                "p50_ms": timing_rows.get::<Option<f64>, _>("p50_distribute_ms"),
                "p95_ms": timing_rows.get::<Option<f64>, _>("p95_distribute_ms"),
            },
            "pipeline_total": {
                "p50_ms": timing_rows.get::<Option<f64>, _>("p50_pipeline_ms"),
                "p95_ms": timing_rows.get::<Option<f64>, _>("p95_pipeline_ms"),
            },
        });

        // Failure rate + average pipeline
        let summary = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS total,
                COUNT(*) FILTER (WHERE failed_at IS NOT NULL)::BIGINT AS failed,
                AVG(
                    EXTRACT(EPOCH FROM (COALESCE(distributed_at, last_updated) - received_at)) * 1000
                )::DOUBLE PRECISION AS avg_pipeline_ms
            FROM wp_tracking
            WHERE first_seen >= $1 AND first_seen < $2
              AND ($3::SMALLINT IS NULL OR core = $3)
              AND received_at IS NOT NULL
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(core_filter)
        .fetch_one(self.pool())
        .await?;

        let total: i64 = summary.get("total");
        let failed: i64 = summary.get("failed");
        let failure_rate = if total > 0 {
            failed as f64 / total as f64
        } else {
            0.0
        };

        Ok(serde_json::json!({
            "stage_timing": stage_timing,
            "failure_rate": failure_rate,
            "total_wps": total,
            "failed_wps": failed,
            "avg_pipeline_ms": summary.get::<Option<f64>, _>("avg_pipeline_ms"),
        }))
    }

    // ── 12. grafana_wp_funnel ──────────────────────────────────────────

    /// Work package pipeline funnel: counts at each stage.
    pub async fn grafana_wp_funnel(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<serde_json::Value, sqlx::Error> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS total,
                COUNT(*) FILTER (WHERE received_at IS NOT NULL)::BIGINT       AS received,
                COUNT(*) FILTER (WHERE authorized_at IS NOT NULL)::BIGINT     AS authorized,
                COUNT(*) FILTER (WHERE refined_at IS NOT NULL)::BIGINT        AS refined,
                COUNT(*) FILTER (WHERE report_built_at IS NOT NULL)::BIGINT   AS report_built,
                COUNT(*) FILTER (WHERE guarantee_built_at IS NOT NULL)::BIGINT AS guarantee_built,
                COUNT(*) FILTER (WHERE distributed_at IS NOT NULL)::BIGINT    AS distributed,
                COUNT(*) FILTER (WHERE failed_at IS NOT NULL)::BIGINT         AS failed
            FROM wp_tracking
            WHERE first_seen >= $1 AND first_seen < $2
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        Ok(serde_json::json!({
            "total": row.get::<i64, _>("total"),
            "received": row.get::<i64, _>("received"),
            "authorized": row.get::<i64, _>("authorized"),
            "refined": row.get::<i64, _>("refined"),
            "report_built": row.get::<i64, _>("report_built"),
            "guarantee_built": row.get::<i64, _>("guarantee_built"),
            "distributed": row.get::<i64, _>("distributed"),
            "failed": row.get::<i64, _>("failed"),
        }))
    }
}
