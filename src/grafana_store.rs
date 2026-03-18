//! Grafana-specific query builder. Constructs dynamic SQL for time-series
//! endpoints with whitelisted intervals, group-by columns, and safe parameter
//! binding.

use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::grafana_types::*;
use crate::store::EventStore;

/// Whitelisted time_bucket intervals for dynamic SQL (6s-aligned sub-minute).
const VALID_INTERVALS: &[&str] = &[
    "6s", "12s", "18s", "24s", "30s", "1m", "2m", "5m", "10m", "15m", "30m", "1h", "2h", "4h",
    "6h", "12h", "1d",
];

/// Whitelisted group_by columns for dynamic SQL.
const VALID_GROUP_BY: &[&str] = &["node_id", "node", "event_type", "core"];

/// Whitelisted aggregate table names for dynamic SQL.
const VALID_TABLES: &[&str] = &[
    "event_stats_30s",
    "event_stats_1m",
    "event_stats_1h",
    "core_stats_1m",
    "all_event_stats_30s",
    "all_event_stats_1m",
    "all_event_stats_1h",
    "all_core_stats_1m",
];

/// Convert a human-friendly interval string to seconds for table selection.
fn interval_to_seconds(interval: &str) -> Option<i64> {
    let s = interval.trim();
    // Must check "ms" before "s" since "s" is a suffix of "ms"
    if let Some(n) = s.strip_suffix("ms") {
        let ms = n.parse::<i64>().ok()?;
        return Some(if ms < 1000 { 1 } else { ms / 1000 });
    }
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

/// Snap an arbitrary interval to the nearest valid (>= input) whitelisted value.
/// Accepts any parseable interval (e.g. Grafana's `$__interval` producing `20s`, `3m`).
fn snap_interval(input: &str) -> &'static str {
    // Fast path: already valid
    if let Some(&valid) = VALID_INTERVALS.iter().find(|&&v| v == input) {
        return valid;
    }
    let input_secs = match interval_to_seconds(input) {
        Some(s) if s > 0 => s,
        _ => return "1m", // unparseable → safe default
    };
    // Find smallest valid interval >= input
    for &candidate in VALID_INTERVALS {
        if let Some(candidate_secs) = interval_to_seconds(candidate) {
            if candidate_secs >= input_secs {
                return candidate;
            }
        }
    }
    // Exceeds largest → cap
    "1d"
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
        core: Option<i16>,
    ) -> Result<Vec<TimeseriesRow>, sqlx::Error> {
        let interval = snap_interval(interval);
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

        // Select aggregate table (interval-based, then retention-aware upgrade)
        let age = Utc::now() - start;
        let table = if group_by == Some("core") || core.is_some() {
            "all_core_stats_1m"
        } else if interval_secs < 60 {
            if age > chrono::Duration::days(3) {
                "all_event_stats_1m" // 30s retention is 3 days, upgrade silently
            } else {
                "all_event_stats_30s"
            }
        } else if interval_secs < 3600 {
            if age > chrono::Duration::days(30) {
                "all_event_stats_1h" // 1m retention is 30 days, upgrade silently
            } else {
                "all_event_stats_1m"
            }
        } else {
            "all_event_stats_1h"
        };

        // Safety: table is from a hardcoded set
        if !VALID_TABLES.contains(&table) {
            return Err(sqlx::Error::Protocol(format!(
                "invalid table: {table}"
            )));
        }

        // Build SELECT columns
        let group_col = match group_by.unwrap_or("event_type") {
            "node" => "node_id",
            other => other,
        };
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

        if node.is_some() && table != "all_core_stats_1m" {
            wheres.push(format!("node_id = ${bind_idx}"));
            bind_idx += 1;
        }

        if core.is_some() && table == "all_core_stats_1m" && group_by != Some("core") {
            wheres.push(format!("core = ${bind_idx}"));
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
            if table != "all_core_stats_1m" {
                query = query.bind(n.to_string());
            }
        }
        if let Some(c) = core {
            if table == "all_core_stats_1m" && group_by != Some("core") {
                query = query.bind(c);
            }
        }
        if let Some(types) = event_types {
            query = query.bind(types.to_vec());
        }

        let rows = query.fetch_all(self.pool()).await?;

        let results: Vec<TimeseriesRow> = rows
            .iter()
            .map(|row| {
                let ts: DateTime<Utc> = row.get("ts");
                let count: i64 = row.get("count");

                let mut r = TimeseriesRow {
                    ts,
                    count,
                    event_type: None,
                    event_type_name: None,
                    core: None,
                    node_id: None,
                };

                if group_col == "core" {
                    r.core = row.try_get("core").ok();
                } else if group_col == "event_type" {
                    let et: i16 = row.get("event_type");
                    r.event_type = Some(et);
                    r.event_type_name = Some(crate::event_type_meta::event_type_name(et));
                } else if group_col == "node_id" {
                    r.node_id = Some(row.get("node_id"));
                }

                r
            })
            .collect();

        Ok(results)
    }

    // ── 2. grafana_stats ───────────────────────────────────────────────

    /// Dashboard summary stats: connected nodes, current slot, guarantees, failures, WP events.
    pub async fn grafana_stats(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<StatsResponse, sqlx::Error> {
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

        Ok(StatsResponse {
            connected_nodes: row.get("connected_nodes"),
            slot_events: row.get("slot_events"),
            guarantees: row.get("guarantees"),
            failures: row.get("failures"),
            wp_events: row.get("wp_events"),
            // Real-time fields are overlaid by the handler
            events_per_sec_10s: None,
            blocks_per_sec_10s: None,
            best_slot: None,
            finalized_slot: None,
            active_nodes: None,
        })
    }

    // ── 3. grafana_cores_summary ────────────────────────────────────────

    /// Per-core activity summary from core_stats_1m.
    pub async fn grafana_cores_summary(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        core_filter: Option<i16>,
    ) -> Result<Vec<CoreSummary>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                core,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 94), 0)::BIGINT  AS work_packages,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 105), 0)::BIGINT AS guarantees,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 92), 0)::BIGINT  AS failures
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

        let results = rows
            .iter()
            .map(|row| CoreSummary {
                core: row.get("core"),
                work_packages: row.get("work_packages"),
                guarantees: row.get("guarantees"),
                failures: row.get("failures"),
            })
            .collect();

        Ok(results)
    }

    // ── 3b. grafana_core_detail ─────────────────────────────────────────

    /// Single core detail with recent work packages from wp_tracking.
    pub async fn grafana_core_detail(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        core: i16,
    ) -> Result<CoreDetail, sqlx::Error> {
        // Get summary for this core
        let summaries = self.grafana_cores_summary(start, end, Some(core)).await?;
        let summary = summaries.into_iter().next().unwrap_or(CoreSummary {
            core,
            work_packages: 0,
            guarantees: 0,
            failures: 0,
        });

        // Recent work packages
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

        let wp_list: Vec<WpTrackingRow> = wps
            .iter()
            .map(|row| WpTrackingRow {
                wp_hash: row.get("wp_hash"),
                first_seen: row.get("first_seen"),
                last_updated: row.get("last_updated"),
                stage: row.get("stage"),
                received_by: row.get("received_by"),
                guaranteed_by: row.get("guaranteed_by"),
                service_ids: row.get::<Vec<i32>, _>("service_ids").into_iter().map(DbServiceId).collect(),
                received_at: row.get("received_at"),
                authorized_at: row.get("authorized_at"),
                refined_at: row.get("refined_at"),
                report_built_at: row.get("report_built_at"),
                guarantee_built_at: row.get("guarantee_built_at"),
                distributed_at: row.get("distributed_at"),
                failed_at: row.get("failed_at"),
            })
            .collect();

        Ok(CoreDetail {
            core: summary.core,
            work_packages: summary.work_packages,
            guarantees: summary.guarantees,
            failures: summary.failures,
            recent_work_packages: wp_list,
        })
    }

    // ── 4. grafana_blocks_convergence ──────────────────────────────────

    /// Block propagation convergence data per slot.
    pub async fn grafana_blocks_convergence(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        event_type: Option<i16>,
    ) -> Result<Vec<BlockConvergenceRow>, sqlx::Error> {
        let rows = if let Some(et) = event_type {
            sqlx::query(
                r#"
                SELECT slot, event_type, node_count, p50_ms, p99_ms, p100_ms, authored_at
                FROM slot_convergence
                WHERE authored_at >= $1 AND authored_at < $2 AND event_type = $3
                ORDER BY slot ASC
                "#,
            )
            .bind(start)
            .bind(end)
            .bind(et)
            .fetch_all(self.pool())
            .await?
        } else {
            sqlx::query(
                r#"
                SELECT slot, event_type, node_count, p50_ms, p99_ms, p100_ms, authored_at
                FROM slot_convergence
                WHERE authored_at >= $1 AND authored_at < $2
                ORDER BY slot ASC, event_type ASC
                "#,
            )
            .bind(start)
            .bind(end)
            .fetch_all(self.pool())
            .await?
        };

        let results = rows
            .iter()
            .map(|row| {
                let et: i16 = row.get("event_type");
                BlockConvergenceRow {
                    slot: row.get("slot"),
                    event_type: et,
                    event_type_name: crate::event_type_meta::event_type_name(et),
                    node_count: row.get("node_count"),
                    p50_ms: row.get("p50_ms"),
                    p99_ms: row.get("p99_ms"),
                    p100_ms: row.get("p100_ms"),
                    authored_at: row.get("authored_at"),
                }
            })
            .collect();

        Ok(results)
    }

    // ── 5. grafana_blocks_contents ─────────────────────────────────────

    /// Block contents extracted from BlockAuthored (event_type=42) JSONB data.
    pub async fn grafana_blocks_contents(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<BlockContentsRow>, sqlx::Error> {
        sqlx::query_as::<_, BlockContentsRow>(
            r#"
            SELECT
                slot,
                timestamp,
                node_id,
                (data->'Authored'->'outline'->>'num_guarantees')::INT       AS num_guarantees,
                (data->'Authored'->'outline'->>'num_assurances')::INT       AS num_assurances,
                (data->'Authored'->'outline'->>'num_preimages')::INT        AS num_preimages,
                (data->'Authored'->'outline'->>'num_tickets')::INT          AS num_tickets,
                (data->'Authored'->'outline'->>'num_dispute_verdicts')::INT AS num_disputes,
                (data->'Authored'->'outline'->>'size_bytes')::INT           AS extrinsic_size
            FROM ingested_raw_events
            WHERE event_type = 42
              AND slot IS NOT NULL
              AND timestamp >= $1 AND timestamp < $2
            ORDER BY slot ASC
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await
    }

    // ── 6. grafana_services ────────────────────────────────────────────

    /// Per-service stats from the service_stats_1m continuous aggregate.
    pub async fn grafana_services(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        services: Option<&[DbServiceId]>,
    ) -> Result<Vec<ServiceRow>, sqlx::Error> {
        let service_filter = if services.is_some() {
            "AND service_id = ANY($3)"
        } else {
            ""
        };
        let sql = format!(
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
            WHERE bucket >= $1 AND bucket < $2 {service_filter}
            GROUP BY service_id
            ORDER BY service_id ASC
            "#
        );
        let svc_i32 = services.map(DbServiceId::as_i32_vec);
        let mut query = sqlx::query(&sql).bind(start).bind(end);
        if let Some(ref svc) = svc_i32 {
            query = query.bind(svc);
        }
        let rows = query.fetch_all(self.pool()).await?;

        let results = rows
            .iter()
            .map(|row| ServiceRow {
                service_id: DbServiceId(row.get("service_id")),
                work_packages: row.get::<Option<i64>, _>("work_packages").unwrap_or(0),
                refinements: row.get::<Option<i64>, _>("refinements").unwrap_or(0),
                refinement_gas: row.get::<Option<i64>, _>("refinement_gas").unwrap_or(0),
                authorizations: row.get::<Option<i64>, _>("authorizations").unwrap_or(0),
                authorization_gas: row.get::<Option<i64>, _>("authorization_gas").unwrap_or(0),
                executions: row.get::<Option<i64>, _>("executions").unwrap_or(0),
                execution_gas: row.get::<Option<i64>, _>("execution_gas").unwrap_or(0),
            })
            .collect();

        Ok(results)
    }

    // ── 6b. grafana_services_timeseries ────────────────────────────────

    /// Per-service time-series from the service_stats_1m continuous aggregate.
    pub async fn grafana_services_timeseries(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
        services: Option<&[DbServiceId]>,
    ) -> Result<Vec<ServiceTimeseriesRow>, sqlx::Error> {
        let interval = snap_interval(interval);
        let pg_interval = interval_to_pg(interval);

        let mut wheres = vec!["bucket >= $1".to_string(), "bucket < $2".to_string()];

        if services.is_some() {
            wheres.push("service_id = ANY($3)".to_string());
        }

        let where_clause = wheres.join(" AND ");
        let sql = format!(
            r#"SELECT
                time_bucket('{pg_interval}'::interval, bucket) AS ts,
                service_id,
                SUM(event_count) FILTER (WHERE event_type = 94)::BIGINT  AS work_packages,
                SUM(total_gas) FILTER (WHERE event_type = 95)::BIGINT    AS authorization_gas,
                SUM(total_gas) FILTER (WHERE event_type = 101)::BIGINT   AS refinement_gas,
                SUM(total_gas) FILTER (WHERE event_type = 47)::BIGINT    AS execution_gas
            FROM service_stats_1m
            WHERE {where_clause}
            GROUP BY ts, service_id
            ORDER BY ts, service_id"#
        );

        let svc_i32 = services.map(DbServiceId::as_i32_vec);
        let mut query = sqlx::query(&sql).bind(start).bind(end);
        if let Some(ref svc) = svc_i32 {
            query = query.bind(svc);
        }

        let rows = query.fetch_all(self.pool()).await?;

        let results = rows
            .iter()
            .map(|row| ServiceTimeseriesRow {
                ts: row.get("ts"),
                service_id: DbServiceId(row.get("service_id")),
                work_packages: row.get::<Option<i64>, _>("work_packages").unwrap_or(0),
                authorization_gas: row.get::<Option<i64>, _>("authorization_gas").unwrap_or(0),
                refinement_gas: row.get::<Option<i64>, _>("refinement_gas").unwrap_or(0),
                execution_gas: row.get::<Option<i64>, _>("execution_gas").unwrap_or(0),
            })
            .collect();

        Ok(results)
    }

    // ── 7. grafana_nodes ───────────────────────────────────────────────

    /// All nodes from the nodes table.
    pub async fn grafana_nodes(&self) -> Result<Vec<NodeRow>, sqlx::Error> {
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

        let results = rows
            .iter()
            .map(|row| NodeRow {
                node_id: row.get("node_id"),
                peer_id: row.get("peer_id"),
                implementation_name: row.get("implementation_name"),
                implementation_version: row.get("implementation_version"),
                node_info: row.get("node_info"),
                connected_at: row.get("connected_at"),
                disconnected_at: row.get("disconnected_at"),
                last_seen_at: row.get("last_seen_at"),
                is_connected: row.get("is_connected"),
                total_event_count: row.get("total_event_count"),
                address: row.get("address"),
            })
            .collect();

        Ok(results)
    }

    // ── 8. grafana_node_stats ──────────────────────────────────────────

    /// Raw rows from the node_stats hypertable, optionally filtered by node(s).
    pub async fn grafana_node_stats(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        nodes: Option<&[String]>,
    ) -> Result<Vec<NodeStatsRow>, sqlx::Error> {
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
            sqlx::query_as::<_, NodeStatsRow>(sql)
                .bind(start)
                .bind(end)
                .bind(node_list)
                .fetch_all(self.pool())
                .await?
        } else {
            sqlx::query_as::<_, NodeStatsRow>(sql)
                .bind(start)
                .bind(end)
                .fetch_all(self.pool())
                .await?
        };

        Ok(rows)
    }

    // ── 9. grafana_node_stats_aggregate ────────────────────────────────

    /// Aggregated node stats from node_stats_1m. Network-wide when no node filter.
    pub async fn grafana_node_stats_aggregate(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        nodes: Option<&[String]>,
    ) -> Result<Vec<NodeStatsAggregateRow>, sqlx::Error> {
        if let Some(node_list) = nodes {
            // Per-node mode: return raw aggregate rows with node_id
            sqlx::query_as::<_, NodeStatsAggregateRow>(
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
                    avg_guarantees::DOUBLE PRECISION, min_guarantees, max_guarantees,
                    max_zero_guarantee_cores,
                    status_count
                FROM node_stats_1m
                WHERE bucket >= $1 AND bucket < $2
                  AND node_id = ANY($3)
                ORDER BY bucket ASC, node_id ASC
                "#,
            )
            .bind(start)
            .bind(end)
            .bind(node_list)
            .fetch_all(self.pool())
            .await
        } else {
            // Network-wide mode: aggregate across all nodes per bucket
            sqlx::query_as::<_, NodeStatsAggregateRow>(
                r#"
                SELECT
                    bucket,
                    NULL::TEXT AS node_id,
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
                    AVG(avg_guarantees)::DOUBLE PRECISION AS avg_guarantees,
                    MIN(min_guarantees)         AS min_guarantees,
                    MAX(max_guarantees)         AS max_guarantees,
                    MAX(max_zero_guarantee_cores) AS max_zero_guarantee_cores,
                    SUM(status_count)::BIGINT   AS status_count
                FROM node_stats_1m
                WHERE bucket >= $1 AND bucket < $2
                GROUP BY bucket
                ORDER BY bucket ASC
                "#,
            )
            .bind(start)
            .bind(end)
            .fetch_all(self.pool())
            .await
        }
    }

    // ── 10. grafana_db_stats ───────────────────────────────────────────

    /// TimescaleDB metadata: table sizes, row counts, compression stats.
    pub async fn grafana_db_stats(&self) -> Result<DbStatsResponse, sqlx::Error> {
        let table_rows = sqlx::query_as::<_, TableSize>(
            r#"
            SELECT
                'ingested_raw_events'::TEXT AS table_name,
                total_bytes::BIGINT,
                table_bytes::BIGINT,
                index_bytes::BIGINT,
                toast_bytes::BIGINT
            FROM hypertable_detailed_size('ingested_raw_events')
            UNION ALL
            SELECT
                'node_stats'::TEXT,
                total_bytes::BIGINT,
                table_bytes::BIGINT,
                index_bytes::BIGINT,
                toast_bytes::BIGINT
            FROM hypertable_detailed_size('node_stats')
            UNION ALL
            SELECT
                'event_services'::TEXT,
                total_bytes::BIGINT,
                table_bytes::BIGINT,
                index_bytes::BIGINT,
                toast_bytes::BIGINT
            FROM hypertable_detailed_size('event_services')
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        let row_counts = sqlx::query_as::<_, RowCount>(
            r#"
            SELECT
                'ingested_raw_events'::TEXT AS table_name,
                approximate_row_count('ingested_raw_events') AS row_count
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

        let compression = sqlx::query_as::<_, CompressionInfo>(
            r#"
            SELECT
                'ingested_raw_events'::TEXT AS table_name,
                COUNT(*) FILTER (WHERE compression_status = 'Compressed')::BIGINT AS compressed_chunks,
                COALESCE(SUM(before_compression_total_bytes), 0)::BIGINT AS before_compression_bytes,
                COALESCE(SUM(after_compression_total_bytes), 0)::BIGINT AS after_compression_bytes
            FROM chunk_compression_stats('ingested_raw_events')
            UNION ALL
            SELECT
                'node_stats'::TEXT,
                COUNT(*) FILTER (WHERE compression_status = 'Compressed')::BIGINT,
                COALESCE(SUM(before_compression_total_bytes), 0)::BIGINT,
                COALESCE(SUM(after_compression_total_bytes), 0)::BIGINT
            FROM chunk_compression_stats('node_stats')
            "#,
        )
        .fetch_all(self.pool())
        .await?;

        Ok(DbStatsResponse {
            tables: table_rows,
            row_counts,
            compression,
        })
    }

    // ── 11. grafana_bottlenecks ────────────────────────────────────────

    /// Pipeline bottleneck analysis from wp_tracking: percentile timing per stage.
    pub async fn grafana_bottlenecks(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        core_filter: Option<i16>,
    ) -> Result<Vec<BottlenecksResponse>, sqlx::Error> {
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

        let stage_timing = StageTiming {
            authorize: Percentiles {
                p50_ms: timing_rows.get("p50_authorize_ms"),
                p95_ms: timing_rows.get("p95_authorize_ms"),
            },
            refine: Percentiles {
                p50_ms: timing_rows.get("p50_refine_ms"),
                p95_ms: timing_rows.get("p95_refine_ms"),
            },
            report: Percentiles {
                p50_ms: timing_rows.get("p50_report_ms"),
                p95_ms: timing_rows.get("p95_report_ms"),
            },
            guarantee: Percentiles {
                p50_ms: timing_rows.get("p50_guarantee_ms"),
                p95_ms: timing_rows.get("p95_guarantee_ms"),
            },
            distribute: Percentiles {
                p50_ms: timing_rows.get("p50_distribute_ms"),
                p95_ms: timing_rows.get("p95_distribute_ms"),
            },
            pipeline_total: Percentiles {
                p50_ms: timing_rows.get("p50_pipeline_ms"),
                p95_ms: timing_rows.get("p95_pipeline_ms"),
            },
        };

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

        Ok(vec![BottlenecksResponse {
            stage_timing,
            failure_rate,
            total_wps: total,
            failed_wps: failed,
            avg_pipeline_ms: summary.get("avg_pipeline_ms"),
        }])
    }

    // ── 11b. grafana_guarantee_discards ─────────────────────────────

    /// Time-bucketed guarantee discard counts grouped by reason.
    /// Queries the pre-aggregated guarantee_receiving_counts table.
    pub async fn grafana_guarantee_discards(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<GuaranteeDiscardRow>, sqlx::Error> {
        let interval = snap_interval(interval);
        let pg_interval = interval_to_pg(interval);

        let sql = format!(
            r#"
            SELECT
                time_bucket('{pg_interval}'::interval, bucket) AS ts,
                reason,
                SUM(event_count)::BIGINT AS count
            FROM guarantee_receiving_counts
            WHERE bucket >= $1 AND bucket < $2
              AND event_type = 113
              AND reason IS NOT NULL
            GROUP BY ts, reason
            ORDER BY ts, reason
            "#
        );

        let rows = sqlx::query(&sql)
            .bind(start)
            .bind(end)
            .fetch_all(self.pool())
            .await?;

        let results = rows
            .iter()
            .map(|row| GuaranteeDiscardRow {
                ts: row.get("ts"),
                reason: row.get("reason"),
                count: row.get("count"),
            })
            .collect();

        Ok(results)
    }

    // ── 12. grafana_events ────────────────────────────────────────────

    /// Generic raw event query from the events hypertable.
    /// Returns individual events as-is with their JSONB data.
    /// NOTE: This queries the raw events table (not a continuous aggregate),
    /// so it can be slow for large time ranges or high-volume event types.
    pub async fn grafana_events(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        event_types: &[i16],
        limit: i64,
    ) -> Result<Vec<EventRow>, sqlx::Error> {
        if event_types.is_empty() {
            return Err(sqlx::Error::Protocol(
                "event_types parameter is required".into(),
            ));
        }
        let pre_agg: Vec<i16> = event_types
            .iter()
            .copied()
            .filter(|&et| crate::event_counter::is_pre_aggregated(et as u16))
            .collect();
        if !pre_agg.is_empty() {
            return Err(sqlx::Error::Protocol(
                format!(
                    "Event types {:?} are pre-aggregated. Use /timeseries instead.",
                    pre_agg
                ),
            ));
        }
        let limit = limit.min(2000);

        let rows = sqlx::query(
            r#"
            SELECT timestamp, node_id, event_type, data
            FROM ingested_raw_events
            WHERE timestamp >= $1 AND timestamp < $2
              AND event_type = ANY($3)
            ORDER BY timestamp DESC
            LIMIT $4
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(event_types)
        .bind(limit)
        .fetch_all(self.pool())
        .await?;

        let results = rows
            .iter()
            .map(|row| EventRow {
                ts: row.get("timestamp"),
                node_id: row.get("node_id"),
                event_type: row.get("event_type"),
                data: row.get("data"),
            })
            .collect();

        Ok(results)
    }

    // ── 13. grafana_wp_funnel ──────────────────────────────────────────

    /// Work package pipeline funnel: counts at each stage.
    pub async fn grafana_wp_funnel(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<WpFunnelResponse, sqlx::Error> {
        sqlx::query_as::<_, WpFunnelResponse>(
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
        .await
    }
    // ── 14. grafana_wp_funnel_timeseries ─────────────────────────────────

    /// Work package pipeline funnel bucketed over time.
    pub async fn grafana_wp_funnel_timeseries(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
        core_filter: Option<i16>,
    ) -> Result<Vec<WpFunnelTimeseriesRow>, sqlx::Error> {
        let interval = snap_interval(interval);
        let pg_interval = interval_to_pg(interval);

        let sql = format!(
            r#"
            SELECT
                time_bucket('{pg_interval}'::interval, first_seen) AS ts,
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
              AND ($3::SMALLINT IS NULL OR core = $3)
            GROUP BY 1
            ORDER BY 1
            "#,
        );

        sqlx::query_as::<_, WpFunnelTimeseriesRow>(&sql)
            .bind(start)
            .bind(end)
            .bind(core_filter)
            .fetch_all(self.pool())
            .await
    }

    // ── 15. grafana_bottlenecks_timeseries ────────────────────────────────

    /// Work package pipeline bottleneck analysis bucketed over time.
    pub async fn grafana_bottlenecks_timeseries(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
        core_filter: Option<i16>,
    ) -> Result<Vec<BottlenecksTimeseriesRow>, sqlx::Error> {
        let interval = snap_interval(interval);
        let pg_interval = interval_to_pg(interval);

        let sql = format!(
            r#"
            SELECT
                time_bucket('{pg_interval}'::interval, first_seen) AS ts,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (authorized_at - received_at)) * 1000
                )::DOUBLE PRECISION AS authorize_p50,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (authorized_at - received_at)) * 1000
                )::DOUBLE PRECISION AS authorize_p95,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (refined_at - authorized_at)) * 1000
                )::DOUBLE PRECISION AS refine_p50,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (refined_at - authorized_at)) * 1000
                )::DOUBLE PRECISION AS refine_p95,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (report_built_at - refined_at)) * 1000
                )::DOUBLE PRECISION AS report_p50,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (report_built_at - refined_at)) * 1000
                )::DOUBLE PRECISION AS report_p95,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (guarantee_built_at - report_built_at)) * 1000
                )::DOUBLE PRECISION AS guarantee_p50,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (guarantee_built_at - report_built_at)) * 1000
                )::DOUBLE PRECISION AS guarantee_p95,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (distributed_at - guarantee_built_at)) * 1000
                )::DOUBLE PRECISION AS distribute_p50,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (distributed_at - guarantee_built_at)) * 1000
                )::DOUBLE PRECISION AS distribute_p95,
                percentile_cont(0.5) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (COALESCE(distributed_at, last_updated) - received_at)) * 1000
                )::DOUBLE PRECISION AS pipeline_p50,
                percentile_cont(0.95) WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (COALESCE(distributed_at, last_updated) - received_at)) * 1000
                )::DOUBLE PRECISION AS pipeline_p95,
                COUNT(*)::BIGINT AS total_wps,
                COUNT(*) FILTER (WHERE failed_at IS NOT NULL)::BIGINT AS failed_wps
            FROM wp_tracking
            WHERE first_seen >= $1 AND first_seen < $2
              AND ($3::SMALLINT IS NULL OR core = $3)
              AND received_at IS NOT NULL
            GROUP BY 1
            ORDER BY 1
            "#,
        );

        sqlx::query_as::<_, BottlenecksTimeseriesRow>(&sql)
            .bind(start)
            .bind(end)
            .bind(core_filter)
            .fetch_all(self.pool())
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snap_exact_match() {
        assert_eq!(snap_interval("6s"), "6s");
        assert_eq!(snap_interval("1m"), "1m");
        assert_eq!(snap_interval("1d"), "1d");
    }

    #[test]
    fn snap_rounds_up() {
        assert_eq!(snap_interval("8s"), "12s");
        assert_eq!(snap_interval("20s"), "24s");
        assert_eq!(snap_interval("25s"), "30s");
        assert_eq!(snap_interval("45s"), "1m");
        assert_eq!(snap_interval("3m"), "5m");
        assert_eq!(snap_interval("7m"), "10m");
    }

    #[test]
    fn snap_below_minimum() {
        assert_eq!(snap_interval("1s"), "6s");
        assert_eq!(snap_interval("5s"), "6s");
    }

    #[test]
    fn snap_above_maximum() {
        assert_eq!(snap_interval("2d"), "1d");
        assert_eq!(snap_interval("7d"), "1d");
    }

    #[test]
    fn snap_unparseable() {
        assert_eq!(snap_interval("garbage"), "1m");
        assert_eq!(snap_interval(""), "1m");
    }

    #[test]
    fn snap_grafana_intervals() {
        // Typical $__interval values from Grafana
        assert_eq!(snap_interval("10s"), "12s");
        assert_eq!(snap_interval("15s"), "18s");
        assert_eq!(snap_interval("20s"), "24s");
        assert_eq!(snap_interval("1m"), "1m");
        assert_eq!(snap_interval("2m"), "2m");
        assert_eq!(snap_interval("5m"), "5m");
    }

    #[test]
    fn snap_milliseconds() {
        assert_eq!(snap_interval("100ms"), "6s");
        assert_eq!(snap_interval("500ms"), "6s");
        assert_eq!(snap_interval("1000ms"), "6s");
        assert_eq!(snap_interval("5000ms"), "6s");
        assert_eq!(snap_interval("10000ms"), "12s");
    }
}
