//! Grafana-specific query builder. Constructs dynamic SQL for time-series
//! endpoints with whitelisted intervals, group-by columns, and safe parameter
//! binding.

use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::grafana_types::*;
use crate::store::EventStore;

/// Compute approximate percentiles (p50, p75, p95, p99, p100) from a merged DA histogram.
/// Delegates to the shared `histogram::percentiles_from_histogram` with DA bounds.
fn percentiles_from_histogram(buckets: &[u32; 14], total: u32) -> Option<(i32, i32, i32, i32, i32)> {
    crate::histogram::percentiles_from_histogram(buckets, total, &crate::histogram::DA_BOUNDS)
}

/// Whitelisted time_bucket intervals for dynamic SQL (6s-aligned sub-minute).
const VALID_INTERVALS: &[&str] = &[
    "6s", "12s", "18s", "24s", "30s", "1m", "2m", "5m", "10m", "15m", "30m", "1h", "2h", "4h",
    "6h", "12h", "1d",
];

/// Whitelisted group_by columns for dynamic SQL.
const VALID_GROUP_BY: &[&str] = &["node_id", "node", "event_type", "core"];

/// Whitelisted aggregate table/view names for dynamic SQL.
/// After migration 020: only UNION views (backed by count tables).
/// Old continuous aggregates (event_stats_30s/1m/1h, core_stats_1m) are dropped.
const VALID_TABLES: &[&str] = &[
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
                    FROM all_event_stats_1m
                    WHERE bucket >= $1 AND bucket < $2
                      AND event_type = 42
                ), 0) AS slot_events,
                COALESCE((
                    SELECT SUM(event_count)::BIGINT
                    FROM all_event_stats_1m
                    WHERE bucket >= $1 AND bucket < $2
                      AND event_type = 105
                ), 0) AS guarantees,
                COALESCE((
                    SELECT SUM(event_count)::BIGINT
                    FROM all_event_stats_1m
                    WHERE bucket >= $1 AND bucket < $2
                      AND event_type = 92
                ), 0) AS failures,
                COALESCE((
                    SELECT SUM(event_count)::BIGINT
                    FROM all_event_stats_1m
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

    /// Per-core activity summary from `all_core_stats_1m` UNION view.
    ///
    /// Counts: WorkPackageReceived (94), GuaranteeBuilt (105), WorkPackageFailed (92).
    /// `last_activity`: correlated subquery on `wp_tracking` — MAX(first_seen) per core.
    pub async fn grafana_cores_summary(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        core_filter: Option<i16>,
    ) -> Result<Vec<CoreSummary>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                cs.core,
                COALESCE(SUM(cs.event_count) FILTER (WHERE cs.event_type = 94), 0)::BIGINT  AS work_packages,
                COALESCE(SUM(cs.event_count) FILTER (WHERE cs.event_type = 105), 0)::BIGINT AS guarantees,
                COALESCE(SUM(cs.event_count) FILTER (WHERE cs.event_type = 92), 0)::BIGINT  AS failures,
                (SELECT MAX(first_seen) FROM wp_tracking wt WHERE wt.core = cs.core AND wt.first_seen >= $1) AS last_activity
            FROM all_core_stats_1m cs
            WHERE cs.bucket >= $1 AND cs.bucket < $2
              AND ($3::SMALLINT IS NULL OR cs.core = $3)
            GROUP BY cs.core
            ORDER BY cs.core ASC
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
                last_activity: row.get("last_activity"),
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
            last_activity: None,
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
                SELECT slot, event_type, node_count, p50_ms, p75_ms, p95_ms, p99_ms, p100_ms, authored_at
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
                SELECT slot, event_type, node_count, p50_ms, p75_ms, p95_ms, p99_ms, p100_ms, authored_at
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
                    p75_ms: row.get("p75_ms"),
                    p95_ms: row.get("p95_ms"),
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
            UNION ALL
            SELECT 'da_node_stats', total_bytes, table_bytes, index_bytes, toast_bytes
            FROM hypertable_detailed_size('da_node_stats')
            UNION ALL
            SELECT 'shard_latency_hist', total_bytes, table_bytes, index_bytes, toast_bytes
            FROM hypertable_detailed_size('shard_latency_hist')
            UNION ALL
            SELECT 'assurance_convergence_senders', total_bytes, table_bytes, index_bytes, toast_bytes
            FROM hypertable_detailed_size('assurance_convergence_senders')
            UNION ALL
            SELECT 'guarantee_convergence', pg_total_relation_size('guarantee_convergence'), pg_relation_size('guarantee_convergence'), pg_indexes_size('guarantee_convergence'), 0
            UNION ALL
            SELECT 'guarantee_convergence_slots', pg_total_relation_size('guarantee_convergence_slots'), pg_relation_size('guarantee_convergence_slots'), pg_indexes_size('guarantee_convergence_slots'), 0
            UNION ALL
            SELECT 'assurance_convergence', pg_total_relation_size('assurance_convergence'), pg_relation_size('assurance_convergence'), pg_indexes_size('assurance_convergence'), 0
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
            UNION ALL
            SELECT 'guarantee_convergence', (SELECT COUNT(*) FROM guarantee_convergence)
            UNION ALL
            SELECT 'guarantee_convergence_slots', (SELECT COUNT(*) FROM guarantee_convergence_slots)
            UNION ALL
            SELECT 'assurance_convergence', (SELECT COUNT(*) FROM assurance_convergence)
            UNION ALL
            SELECT 'assurance_convergence_senders', approximate_row_count('assurance_convergence_senders')
            UNION ALL
            SELECT 'da_node_stats', approximate_row_count('da_node_stats')
            UNION ALL
            SELECT 'shard_latency_hist', approximate_row_count('shard_latency_hist')
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

    /// Search raw events with optional filtering and pagination.
    ///
    /// Queries `ingested_raw_events` (1h retention, all 115 event types).
    /// Filters: event_types (optional), node_id, core (hot column), wp_hash (hot column).
    /// Returns paginated response with total count for UI pagination controls.
    pub async fn grafana_events(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        event_types: &[i16],
        limit: i64,
        offset: i64,
        node: Option<&str>,
        core: Option<i16>,
        wp_hash: Option<&[u8]>,
    ) -> Result<EventsSearchResponse, sqlx::Error> {
        let limit = limit.min(2000);

        // Build WHERE clause dynamically based on provided filters
        let mut conditions = vec![
            "timestamp >= $1".to_string(),
            "timestamp < $2".to_string(),
        ];
        let mut param_idx = 3;

        if !event_types.is_empty() {
            conditions.push(format!("event_type = ANY(${})", param_idx));
            param_idx += 1;
        }
        if node.is_some() {
            conditions.push(format!("node_id = ${}", param_idx));
            param_idx += 1;
        }
        if core.is_some() {
            conditions.push(format!("core = ${}", param_idx));
            param_idx += 1;
        }
        if wp_hash.is_some() {
            conditions.push(format!("wp_hash = ${}", param_idx));
            param_idx += 1;
        }

        let where_clause = conditions.join(" AND ");
        let data_sql = format!(
            "SELECT timestamp, node_id, event_type, data, created_at \
             FROM ingested_raw_events WHERE {} \
             ORDER BY timestamp DESC LIMIT {} OFFSET {}",
            where_clause, limit, offset
        );
        let count_sql = format!(
            "SELECT COUNT(*)::BIGINT FROM ingested_raw_events WHERE {}",
            where_clause
        );

        // Build query with dynamic bindings
        let mut data_query = sqlx::query(&data_sql).bind(start).bind(end);
        let mut count_query = sqlx::query_scalar::<_, i64>(&count_sql).bind(start).bind(end);

        if !event_types.is_empty() {
            data_query = data_query.bind(event_types);
            count_query = count_query.bind(event_types);
        }
        if let Some(n) = node {
            data_query = data_query.bind(n);
            count_query = count_query.bind(n);
        }
        if let Some(c) = core {
            data_query = data_query.bind(c);
            count_query = count_query.bind(c);
        }
        if let Some(wh) = wp_hash {
            data_query = data_query.bind(wh);
            count_query = count_query.bind(wh);
        }

        let (rows, total) = tokio::try_join!(
            data_query.fetch_all(self.pool()),
            count_query.fetch_one(self.pool()),
        )?;

        let events: Vec<EventRow> = rows
            .iter()
            .map(|row| EventRow {
                ts: row.get("timestamp"),
                node_id: row.get("node_id"),
                event_type: row.get("event_type"),
                data: row.get("data"),
                created_at: row.get("created_at"),
            })
            .collect();

        Ok(EventsSearchResponse {
            pagination: PaginationMeta {
                offset,
                limit,
                total,
                has_more: offset + limit < total,
            },
            events,
        })
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
    // ── 14. grafana_guarantee_convergence (per-slot summary) ────────────

    /// Guarantee convergence overview: per-slot summary.
    pub async fn grafana_guarantee_convergence(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<GuaranteeConvergenceSlotRow>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT slot, guarantee_count, node_count,
                   p50_ms, p75_ms, p95_ms, p99_ms, p100_ms, built_at
            FROM guarantee_convergence_slots
            WHERE built_at >= $1 AND built_at < $2
            ORDER BY slot ASC
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let slot: i32 = row.get("slot");
                let slot_timestamp =
                    crate::onchain_stats::slot_to_timestamp(slot as u32, 6);
                GuaranteeConvergenceSlotRow {
                    slot,
                    slot_timestamp,
                    guarantee_count: row.get("guarantee_count"),
                    node_count: row.get("node_count"),
                    p50_ms: row.get("p50_ms"),
                    p75_ms: row.get("p75_ms"),
                    p95_ms: row.get("p95_ms"),
                    p99_ms: row.get("p99_ms"),
                    p100_ms: row.get("p100_ms"),
                    built_at: row.get("built_at"),
                }
            })
            .collect())
    }

    // ── 15. grafana_guarantee_convergence_detail ──────────────────────────

    /// Guarantee convergence detail: per-guarantee rows filtered by core/wp_hash.
    pub async fn grafana_guarantee_convergence_detail(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        core_filter: Option<i16>,
        wp_hash_filter: Option<&[u8]>,
    ) -> Result<Vec<GuaranteeConvergenceDetailRow>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT work_report_hash, slot, core, wp_hash, builder_node_id, node_count,
                   p50_ms, p75_ms, p95_ms, p99_ms, p100_ms, built_at
            FROM guarantee_convergence
            WHERE built_at >= $1 AND built_at < $2
              AND ($3::SMALLINT IS NULL OR core = $3)
              AND ($4::BYTEA IS NULL OR wp_hash = $4)
            ORDER BY slot ASC, built_at ASC
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(core_filter)
        .bind(wp_hash_filter)
        .fetch_all(self.pool())
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let wrh: Vec<u8> = row.get("work_report_hash");
                let wp: Option<Vec<u8>> = row.get("wp_hash");
                GuaranteeConvergenceDetailRow {
                    work_report_hash: hex::encode(&wrh),
                    slot: row.get("slot"),
                    core: row.get("core"),
                    wp_hash: wp.map(|h| hex::encode(&h)),
                    builder_node_id: row.get("builder_node_id"),
                    node_count: row.get("node_count"),
                    p50_ms: row.get("p50_ms"),
                    p75_ms: row.get("p75_ms"),
                    p95_ms: row.get("p95_ms"),
                    p99_ms: row.get("p99_ms"),
                    p100_ms: row.get("p100_ms"),
                    built_at: row.get("built_at"),
                }
            })
            .collect())
    }

    // ── 16. grafana_assurance_convergence ────────────────────────────────

    /// Assurance convergence overview: per-anchor summary.
    pub async fn grafana_assurance_convergence(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<AssuranceConvergenceRow>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT anchor, slot, slot_timestamp, sender_count, receiver_count,
                   p50_ms, p75_ms, p95_ms, p99_ms, p100_ms,
                   dist_start_p50_ms, dist_start_p95_ms, dist_start_p99_ms, dist_start_p100_ms,
                   first_distributed_at, last_distributed_at
            FROM assurance_convergence
            WHERE ($1::TIMESTAMPTZ IS NULL OR first_distributed_at >= $1)
              AND ($2::TIMESTAMPTZ IS NULL OR first_distributed_at < $2)
            ORDER BY slot ASC NULLS LAST
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let anchor_bytes: Vec<u8> = row.get("anchor");
                AssuranceConvergenceRow {
                    anchor: hex::encode(&anchor_bytes),
                    slot: row.get("slot"),
                    slot_timestamp: row.get("slot_timestamp"),
                    sender_count: row.get("sender_count"),
                    receiver_count: row.get("receiver_count"),
                    p50_ms: row.get("p50_ms"),
                    p75_ms: row.get("p75_ms"),
                    p95_ms: row.get("p95_ms"),
                    p99_ms: row.get("p99_ms"),
                    p100_ms: row.get("p100_ms"),
                    dist_start_p50_ms: row.get("dist_start_p50_ms"),
                    dist_start_p95_ms: row.get("dist_start_p95_ms"),
                    dist_start_p99_ms: row.get("dist_start_p99_ms"),
                    dist_start_p100_ms: row.get("dist_start_p100_ms"),
                    first_distributed_at: row.get("first_distributed_at"),
                    last_distributed_at: row.get("last_distributed_at"),
                }
            })
            .collect())
    }

    // ── 17. grafana_assurance_convergence_senders ─────────────────────────

    /// Assurance convergence per-sender detail.
    pub async fn grafana_assurance_convergence_senders(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        anchor_filter: Option<&[u8]>,
        node_filter: Option<&str>,
    ) -> Result<Vec<AssuranceConvergenceSenderRow>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT s.anchor, s.sender_node_id, s.node_count,
                   s.p50_ms, s.p75_ms, s.p95_ms, s.p99_ms, s.p100_ms,
                   s.distributed_at,
                   a.slot
            FROM assurance_convergence_senders s
            LEFT JOIN assurance_convergence a ON a.anchor = s.anchor
            WHERE s.distributed_at >= $1 AND s.distributed_at < $2
              AND ($3::BYTEA IS NULL OR s.anchor = $3)
              AND ($4::TEXT IS NULL OR s.sender_node_id = $4)
            ORDER BY s.distributed_at ASC
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(anchor_filter)
        .bind(node_filter)
        .fetch_all(self.pool())
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let anchor_bytes: Vec<u8> = row.get("anchor");
                AssuranceConvergenceSenderRow {
                    anchor: hex::encode(&anchor_bytes),
                    slot: row.get("slot"),
                    sender_node_id: row.get("sender_node_id"),
                    node_count: row.get("node_count"),
                    p50_ms: row.get("p50_ms"),
                    p75_ms: row.get("p75_ms"),
                    p95_ms: row.get("p95_ms"),
                    p99_ms: row.get("p99_ms"),
                    p100_ms: row.get("p100_ms"),
                    distributed_at: row.get("distributed_at"),
                }
            })
            .collect())
    }

    // ── 16b. grafana_guarantee_convergence_hist (interval mode) ─────────

    /// Guarantee convergence histogram timeseries — percentiles from merged histograms.
    pub async fn grafana_guarantee_convergence_hist(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<ConvergenceTimeseriesRow>, sqlx::Error> {
        let interval = snap_interval(interval);
        let pg_interval = interval_to_pg(interval);

        let sql = format!(
            r#"
            SELECT time_bucket('{pg_interval}'::interval, built_at) AS ts,
                SUM(h_0_2)::INT AS h_0_2, SUM(h_2_5)::INT AS h_2_5,
                SUM(h_5_10)::INT AS h_5_10, SUM(h_10_15)::INT AS h_10_15,
                SUM(h_15_20)::INT AS h_15_20, SUM(h_20_30)::INT AS h_20_30,
                SUM(h_30_50)::INT AS h_30_50, SUM(h_50_75)::INT AS h_50_75,
                SUM(h_75_100)::INT AS h_75_100, SUM(h_100_150)::INT AS h_100_150,
                SUM(h_150_250)::INT AS h_150_250, SUM(h_250_500)::INT AS h_250_500,
                SUM(h_500_1000)::INT AS h_500_1000, SUM(h_1000_2000)::INT AS h_1000_2000,
                SUM(h_2000_5000)::INT AS h_2000_5000, SUM(h_5000_10000)::INT AS h_5000_10000,
                SUM(h_10000_15000)::INT AS h_10000_15000, SUM(h_15000_20000)::INT AS h_15000_20000,
                SUM(h_20000_25000)::INT AS h_20000_25000, SUM(h_25000_30000)::INT AS h_25000_30000,
                SUM(h_30000_60000)::INT AS h_30000_60000, SUM(h_60000_120000)::INT AS h_60000_120000,
                SUM(h_120000_plus)::INT AS h_120000_plus,
                SUM(hist_total)::INT AS hist_total
            FROM guarantee_convergence
            WHERE built_at >= $1 AND built_at < $2
            GROUP BY 1
            ORDER BY 1
            "#,
        );

        let rows = sqlx::query(&sql)
            .bind(start)
            .bind(end)
            .fetch_all(self.pool())
            .await?;

        Ok(rows_to_convergence_timeseries(&rows))
    }

    // ── 17b. grafana_assurance_convergence_hist (interval mode) ──────────

    /// Assurance convergence histogram timeseries — percentiles from merged histograms.
    pub async fn grafana_assurance_convergence_hist(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<ConvergenceTimeseriesRow>, sqlx::Error> {
        let interval = snap_interval(interval);
        let pg_interval = interval_to_pg(interval);

        let sql = format!(
            r#"
            SELECT time_bucket('{pg_interval}'::interval, first_distributed_at) AS ts,
                SUM(h_0_2)::INT AS h_0_2, SUM(h_2_5)::INT AS h_2_5,
                SUM(h_5_10)::INT AS h_5_10, SUM(h_10_15)::INT AS h_10_15,
                SUM(h_15_20)::INT AS h_15_20, SUM(h_20_30)::INT AS h_20_30,
                SUM(h_30_50)::INT AS h_30_50, SUM(h_50_75)::INT AS h_50_75,
                SUM(h_75_100)::INT AS h_75_100, SUM(h_100_150)::INT AS h_100_150,
                SUM(h_150_250)::INT AS h_150_250, SUM(h_250_500)::INT AS h_250_500,
                SUM(h_500_1000)::INT AS h_500_1000, SUM(h_1000_2000)::INT AS h_1000_2000,
                SUM(h_2000_5000)::INT AS h_2000_5000, SUM(h_5000_10000)::INT AS h_5000_10000,
                SUM(h_10000_15000)::INT AS h_10000_15000, SUM(h_15000_20000)::INT AS h_15000_20000,
                SUM(h_20000_25000)::INT AS h_20000_25000, SUM(h_25000_30000)::INT AS h_25000_30000,
                SUM(h_30000_60000)::INT AS h_30000_60000, SUM(h_60000_120000)::INT AS h_60000_120000,
                SUM(h_120000_plus)::INT AS h_120000_plus,
                SUM(hist_total)::INT AS hist_total
            FROM assurance_convergence
            WHERE first_distributed_at >= $1 AND first_distributed_at < $2
            GROUP BY 1
            ORDER BY 1
            "#,
        );

        let rows = sqlx::query(&sql)
            .bind(start)
            .bind(end)
            .fetch_all(self.pool())
            .await?;

        Ok(rows_to_convergence_timeseries(&rows))
    }

    // ── 17c. grafana_assurance_convergence_senders_hist (interval mode) ──

    /// Assurance convergence senders histogram timeseries — percentiles from merged histograms.
    pub async fn grafana_assurance_convergence_senders_hist(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
        node_filter: Option<&str>,
    ) -> Result<Vec<ConvergenceTimeseriesRow>, sqlx::Error> {
        let interval = snap_interval(interval);
        let pg_interval = interval_to_pg(interval);

        let sql = format!(
            r#"
            SELECT time_bucket('{pg_interval}'::interval, distributed_at) AS ts,
                SUM(h_0_2)::INT AS h_0_2, SUM(h_2_5)::INT AS h_2_5,
                SUM(h_5_10)::INT AS h_5_10, SUM(h_10_15)::INT AS h_10_15,
                SUM(h_15_20)::INT AS h_15_20, SUM(h_20_30)::INT AS h_20_30,
                SUM(h_30_50)::INT AS h_30_50, SUM(h_50_75)::INT AS h_50_75,
                SUM(h_75_100)::INT AS h_75_100, SUM(h_100_150)::INT AS h_100_150,
                SUM(h_150_250)::INT AS h_150_250, SUM(h_250_500)::INT AS h_250_500,
                SUM(h_500_1000)::INT AS h_500_1000, SUM(h_1000_2000)::INT AS h_1000_2000,
                SUM(h_2000_5000)::INT AS h_2000_5000, SUM(h_5000_10000)::INT AS h_5000_10000,
                SUM(h_10000_15000)::INT AS h_10000_15000, SUM(h_15000_20000)::INT AS h_15000_20000,
                SUM(h_20000_25000)::INT AS h_20000_25000, SUM(h_25000_30000)::INT AS h_25000_30000,
                SUM(h_30000_60000)::INT AS h_30000_60000, SUM(h_60000_120000)::INT AS h_60000_120000,
                SUM(h_120000_plus)::INT AS h_120000_plus,
                SUM(hist_total)::INT AS hist_total
            FROM assurance_convergence_senders
            WHERE distributed_at >= $1 AND distributed_at < $2
              AND ($3::TEXT IS NULL OR sender_node_id = $3)
            GROUP BY 1
            ORDER BY 1
            "#,
        );

        let rows = sqlx::query(&sql)
            .bind(start)
            .bind(end)
            .bind(node_filter)
            .fetch_all(self.pool())
            .await?;

        Ok(rows_to_convergence_timeseries(&rows))
    }

    // ── 18. grafana_da_stats ────────────────────────────────────────────

    /// Per-node DA operational stats aggregated over time range.
    pub async fn grafana_da_stats(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        node_filter: Option<&str>,
    ) -> Result<Vec<DaStatsRow>, sqlx::Error> {
        sqlx::query_as::<_, DaStatsRow>(
            r#"
            SELECT node_id,
                SUM(shard_requests_sent)::BIGINT AS shard_requests_sent,
                SUM(shard_requests_received)::BIGINT AS shard_requests_received,
                SUM(shard_sent_confirmed)::BIGINT AS shard_sent_confirmed,
                SUM(shard_received_confirmed)::BIGINT AS shard_received_confirmed,
                SUM(shards_transferred)::BIGINT AS shards_transferred,
                SUM(shard_failures)::BIGINT AS shard_failures,
                SUM(preimage_ann_failures)::BIGINT AS preimage_ann_failures,
                SUM(preimages_announced)::BIGINT AS preimages_announced,
                SUM(preimages_forgotten)::BIGINT AS preimages_forgotten,
                CASE WHEN SUM(assurer_latency_samples) > 0
                    THEN (SUM(assurer_avg_latency_ms::DOUBLE PRECISION * assurer_latency_samples) / SUM(assurer_latency_samples))::REAL
                END AS assurer_avg_latency_ms,
                SUM(assurer_latency_samples)::BIGINT AS assurer_latency_samples,
                CASE WHEN SUM(guarantor_latency_samples) > 0
                    THEN (SUM(guarantor_avg_latency_ms::DOUBLE PRECISION * guarantor_latency_samples) / SUM(guarantor_latency_samples))::REAL
                END AS guarantor_avg_latency_ms,
                SUM(guarantor_latency_samples)::BIGINT AS guarantor_latency_samples,
                MAX(active_shards)::INT AS active_shards
            FROM da_node_stats
            WHERE ts >= $1 AND ts < $2
              AND ($3::TEXT IS NULL OR node_id = $3)
            GROUP BY node_id
            ORDER BY shards_transferred DESC
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(node_filter)
        .fetch_all(self.pool())
        .await
    }

    // ── 19. grafana_shard_latency ─────────────────────────────────────────

    /// Shard latency histograms merged per time bucket. Returns raw merged
    /// histogram data; percentile interpolation happens in Rust.
    pub async fn grafana_shard_latency_raw(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<ShardLatencyRow>, sqlx::Error> {
        let interval = snap_interval(interval);
        let pg_interval = interval_to_pg(interval);

        let sql = format!(
            r#"
            SELECT time_bucket('{pg_interval}'::interval, ts) AS bucket, side,
                SUM(b_0_1)::INT AS b_0_1, SUM(b_1_2)::INT AS b_1_2, SUM(b_2_5)::INT AS b_2_5,
                SUM(b_5_10)::INT AS b_5_10, SUM(b_10_25)::INT AS b_10_25, SUM(b_25_50)::INT AS b_25_50,
                SUM(b_50_100)::INT AS b_50_100, SUM(b_100_250)::INT AS b_100_250, SUM(b_250_500)::INT AS b_250_500,
                SUM(b_500_1000)::INT AS b_500_1000, SUM(b_1000_2000)::INT AS b_1000_2000,
                SUM(b_2000_3000)::INT AS b_2000_3000, SUM(b_3000_5000)::INT AS b_3000_5000,
                SUM(b_5000_plus)::INT AS b_5000_plus,
                SUM(total_count)::INT AS total_count,
                SUM(failed_count)::INT AS failed_count
            FROM shard_latency_hist
            WHERE ts >= $1 AND ts < $2
            GROUP BY 1, 2
            ORDER BY 1, 2
            "#,
        );

        let rows = sqlx::query(&sql)
            .bind(start)
            .bind(end)
            .fetch_all(self.pool())
            .await?;

        // Group by bucket, compute percentiles from merged histograms
        use std::collections::BTreeMap;
        let mut buckets: BTreeMap<DateTime<Utc>, ShardLatencyRow> = BTreeMap::new();

        for row in &rows {
            let ts: DateTime<Utc> = row.get("bucket");
            let side: i16 = row.get("side");
            let hist = [
                row.get::<i32, _>("b_0_1") as u32,
                row.get::<i32, _>("b_1_2") as u32,
                row.get::<i32, _>("b_2_5") as u32,
                row.get::<i32, _>("b_5_10") as u32,
                row.get::<i32, _>("b_10_25") as u32,
                row.get::<i32, _>("b_25_50") as u32,
                row.get::<i32, _>("b_50_100") as u32,
                row.get::<i32, _>("b_100_250") as u32,
                row.get::<i32, _>("b_250_500") as u32,
                row.get::<i32, _>("b_500_1000") as u32,
                row.get::<i32, _>("b_1000_2000") as u32,
                row.get::<i32, _>("b_2000_3000") as u32,
                row.get::<i32, _>("b_3000_5000") as u32,
                row.get::<i32, _>("b_5000_plus") as u32,
            ];
            let total: i32 = row.get("total_count");
            let failed: i32 = row.get("failed_count");

            let p = percentiles_from_histogram(&hist, total as u32);

            let entry = buckets.entry(ts).or_insert_with(|| ShardLatencyRow {
                ts,
                assurer_p50: None, assurer_p75: None, assurer_p95: None, assurer_p99: None, assurer_p100: None, assurer_samples: 0,
                guarantor_p50: None, guarantor_p75: None, guarantor_p95: None, guarantor_p99: None, guarantor_p100: None, guarantor_samples: 0,
                failed_count: 0,
            });

            if side == 0 {
                if let Some(p) = p {
                    entry.assurer_p50 = Some(p.0);
                    entry.assurer_p75 = Some(p.1);
                    entry.assurer_p95 = Some(p.2);
                    entry.assurer_p99 = Some(p.3);
                    entry.assurer_p100 = Some(p.4);
                }
                entry.assurer_samples = total;
                entry.failed_count += failed;
            } else {
                if let Some(p) = p {
                    entry.guarantor_p50 = Some(p.0);
                    entry.guarantor_p75 = Some(p.1);
                    entry.guarantor_p95 = Some(p.2);
                    entry.guarantor_p99 = Some(p.3);
                    entry.guarantor_p100 = Some(p.4);
                }
                entry.guarantor_samples = total;
                entry.failed_count += failed;
            }
        }

        Ok(buckets.into_values().collect())
    }

    // ── 20. grafana_wp_funnel_timeseries ─────────────────────────────────

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

    // ── Phase 3: Shared node_core_mapping helper ────────────────────────

    /// Observed node→core mapping from guarantee_convergence.
    /// Returns per-node: core, guarantee_count, last_guarantee.
    /// Used by /guarantees/by-guarantor and /validators/cores.
    pub async fn node_core_mapping(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<NodeCoreRow>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT builder_node_id AS node_id, core,
                   COUNT(*)::BIGINT AS guarantee_count,
                   MAX(built_at) AS last_guarantee
            FROM guarantee_convergence
            WHERE built_at >= $1 AND built_at < $2
              AND builder_node_id IS NOT NULL
              AND core IS NOT NULL
            GROUP BY builder_node_id, core
            ORDER BY guarantee_count DESC
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        Ok(rows
            .iter()
            .map(|row| NodeCoreRow {
                node_id: row.get("node_id"),
                core: row.get("core"),
                guarantee_count: row.get("guarantee_count"),
                last_guarantee: row.get("last_guarantee"),
            })
            .collect())
    }

    // ── Phase 3: /grafana/failure-rates ─────────────────────────────────

    pub async fn grafana_failure_rates(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<FailureRatesResponse, sqlx::Error> {
        // Failure type IDs
        let failure_types: &[i16] = &[41, 44, 46, 81, 83, 92, 99, 107, 111, 113, 122, 127];
        // Success counterparts for rate calculation
        let all_types: &[i16] = &[
            40, 41, 42, 44, 46, // block authoring
            80, 81, 83, // tickets
            92, 94, 99, // work packages
            105, 107, 108, 109, 111, 113, // guarantees
            120, 122, 125, // shards
            126, 127, // assurances
        ];

        // Overall counts
        let overall_row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(event_count), 0)::BIGINT AS total,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = ANY($3)), 0)::BIGINT AS failures
            FROM all_event_stats_1m
            WHERE bucket >= $1 AND bucket < $2 AND event_type = ANY($4)
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(failure_types)
        .bind(all_types)
        .fetch_one(self.pool())
        .await?;

        let total: i64 = overall_row.get("total");
        let failures: i64 = overall_row.get("failures");

        // By category
        let categories = vec![
            ("block_authoring", vec![40i16, 42], vec![41i16, 44, 46]),
            ("tickets", vec![80, 82, 84], vec![81, 83]),
            ("work_packages", vec![94], vec![92, 99]),
            ("guarantees", vec![105, 108, 109], vec![107, 111, 113]),
            ("shards", vec![120, 125], vec![122]),
            ("assurances", vec![126], vec![127]),
        ];

        let mut by_category = Vec::new();
        for (name, success_types, fail_types) in &categories {
            let mut all = success_types.clone();
            all.extend(fail_types.iter());
            let cat_row = sqlx::query(
                r#"
                SELECT
                    COALESCE(SUM(event_count), 0)::BIGINT AS total,
                    COALESCE(SUM(event_count) FILTER (WHERE event_type = ANY($3)), 0)::BIGINT AS failures
                FROM all_event_stats_1m
                WHERE bucket >= $1 AND bucket < $2 AND event_type = ANY($4)
                "#,
            )
            .bind(start)
            .bind(end)
            .bind(fail_types.as_slice())
            .bind(all.as_slice())
            .fetch_one(self.pool())
            .await?;

            let cat_total: i64 = cat_row.get("total");
            let cat_failures: i64 = cat_row.get("failures");
            by_category.push(FailureCategory {
                category: name.to_string(),
                attempts: cat_total,
                failures: cat_failures,
                rate: if cat_total > 0 { cat_failures as f64 / cat_total as f64 } else { 0.0 },
            });
        }

        // By node (top 20 failing nodes)
        let node_rows = sqlx::query(
            r#"
            SELECT node_id,
                COALESCE(SUM(event_count), 0)::BIGINT AS total,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = ANY($3)), 0)::BIGINT AS failures
            FROM all_event_stats_1m
            WHERE bucket >= $1 AND bucket < $2 AND event_type = ANY($4)
            GROUP BY node_id
            HAVING SUM(event_count) FILTER (WHERE event_type = ANY($3)) > 0
            ORDER BY failures DESC
            LIMIT 20
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(failure_types)
        .bind(all_types)
        .fetch_all(self.pool())
        .await?;

        let by_node: Vec<FailureByNode> = node_rows
            .iter()
            .map(|row| {
                let t: i64 = row.get("total");
                let f: i64 = row.get("failures");
                FailureByNode {
                    node_id: row.get("node_id"),
                    total_events: t,
                    failures: f,
                    failure_rate: if t > 0 { f as f64 / t as f64 } else { 0.0 },
                }
            })
            .collect();

        // Recent failures from raw events (last 5 minutes, limit 20)
        let recent_rows = sqlx::query(
            r#"
            SELECT timestamp, node_id, event_type, data
            FROM ingested_raw_events
            WHERE timestamp >= NOW() - INTERVAL '5 minutes'
              AND event_type = ANY($1)
            ORDER BY timestamp DESC
            LIMIT 20
            "#,
        )
        .bind(failure_types)
        .fetch_all(self.pool())
        .await?;

        let recent_failures: Vec<RecentFailure> = recent_rows
            .iter()
            .map(|row| {
                let et: i16 = row.get("event_type");
                let data: serde_json::Value = row.get("data");
                // Extract reason from JSONB — try common patterns
                let reason = data.as_object().and_then(|obj| {
                    obj.values().next().and_then(|v| {
                        v.get("reason").and_then(|r| r.as_str().map(String::from))
                    })
                });
                RecentFailure {
                    event_type: et,
                    event_name: crate::event_type_meta::event_type_name(et).to_string(),
                    node_id: row.get("node_id"),
                    timestamp: row.get("timestamp"),
                    reason,
                }
            })
            .collect();

        Ok(FailureRatesResponse {
            overall: FailureOverall {
                total_events: total,
                failed_events: failures,
                failure_rate: if total > 0 { failures as f64 / total as f64 } else { 0.0 },
            },
            by_category,
            by_node,
            recent_failures,
        })
    }

    // ── Phase 3: /grafana/sync-timeline ─────────────────────────────────

    pub async fn grafana_sync_timeline(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<SyncTimelineRow>, sqlx::Error> {
        let interval = snap_interval(interval);
        let pg_interval = interval_to_pg(interval);

        let rows = sqlx::query(&format!(
            r#"
            WITH bucketed AS (
                SELECT
                    time_bucket('{pg_interval}'::interval, bucket) AS ts,
                    node_id,
                    MAX(slot) AS max_slot
                FROM status_counts
                WHERE bucket >= $1 AND bucket < $2
                  AND event_type = 11
                  AND slot IS NOT NULL
                GROUP BY 1, node_id
            ),
            network AS (
                SELECT ts, MAX(max_slot) AS network_slot
                FROM bucketed GROUP BY ts
            )
            SELECT
                b.ts,
                COUNT(DISTINCT b.node_id)::BIGINT AS total_nodes,
                COUNT(DISTINCT b.node_id) FILTER (WHERE b.max_slot >= n.network_slot - 2)::BIGINT AS synced_nodes,
                n.network_slot
            FROM bucketed b
            JOIN network n ON b.ts = n.ts
            GROUP BY b.ts, n.network_slot
            ORDER BY b.ts ASC
            "#,
        ))
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        Ok(rows
            .iter()
            .map(|row| {
                let total: i64 = row.get("total_nodes");
                let synced: i64 = row.get("synced_nodes");
                SyncTimelineRow {
                    ts: row.get("ts"),
                    total_nodes: total,
                    synced_nodes: synced,
                    behind_nodes: total - synced,
                    sync_percentage: if total > 0 { synced as f64 / total as f64 * 100.0 } else { 0.0 },
                    network_slot: row.get("network_slot"),
                }
            })
            .collect())
    }

    // ── Phase 3: /grafana/connections-timeline ──────────────────────────

    pub async fn grafana_connections_timeline(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<ConnectionsTimelineResponse, sqlx::Error> {
        let interval = snap_interval(interval);
        let pg_interval = interval_to_pg(interval);

        let timeline_rows = sqlx::query(&format!(
            r#"
            SELECT
                time_bucket('{pg_interval}'::interval, bucket) AS ts,
                COALESCE(SUM(event_count) FILTER (WHERE event_type IN (23, 26)), 0)::BIGINT AS connections,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 27), 0)::BIGINT AS disconnections,
                COUNT(DISTINCT node_id)::BIGINT AS active_nodes
            FROM all_event_stats_30s
            WHERE bucket >= $1 AND bucket < $2
              AND event_type IN (23, 26, 27)
            GROUP BY 1
            ORDER BY 1 ASC
            "#,
        ))
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        let timeline: Vec<ConnectionsBucket> = timeline_rows
            .iter()
            .map(|row| ConnectionsBucket {
                ts: row.get("ts"),
                connections: row.get("connections"),
                disconnections: row.get("disconnections"),
                active_nodes: row.get("active_nodes"),
            })
            .collect();

        // Health stats from nodes table
        let health = sqlx::query(
            "SELECT COUNT(*)::BIGINT AS total, COUNT(*) FILTER (WHERE is_connected)::BIGINT AS connected FROM nodes",
        )
        .fetch_one(self.pool())
        .await?;

        Ok(ConnectionsTimelineResponse {
            timeline,
            health_stats: ConnectionHealthStats {
                total_nodes_seen: health.get("total"),
                currently_connected: health.get("connected"),
            },
        })
    }

    // ── Phase 3: /grafana/guarantees ────────────────────────────────────

    pub async fn grafana_guarantees(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<GuaranteesResponse, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT event_type, COALESCE(SUM(event_count), 0)::BIGINT AS count
            FROM all_event_stats_1m
            WHERE bucket >= $1 AND bucket < $2
              AND event_type BETWEEN 105 AND 113
            GROUP BY event_type
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        let mut totals = GuaranteeTotals {
            built: 0, sending: 0, send_failed: 0, sent: 0, distributed: 0,
            receiving: 0, receive_failed: 0, received: 0, discarded: 0,
        };
        for row in &rows {
            let et: i16 = row.get("event_type");
            let count: i64 = row.get("count");
            match et {
                105 => totals.built = count,
                106 => totals.sending = count,
                107 => totals.send_failed = count,
                108 => totals.sent = count,
                109 => totals.distributed = count,
                110 => totals.receiving = count,
                111 => totals.receive_failed = count,
                112 => totals.received = count,
                113 => totals.discarded = count,
                _ => {}
            }
        }

        let send_total = totals.sending + totals.send_failed + totals.sent;
        let recv_total = totals.receiving + totals.receive_failed + totals.received;
        let success_rates = GuaranteeSuccessRates {
            send_success_rate: if send_total > 0 { totals.sent as f64 / send_total as f64 } else { 1.0 },
            receive_success_rate: if recv_total > 0 { totals.received as f64 / recv_total as f64 } else { 1.0 },
        };

        Ok(GuaranteesResponse {
            totals,
            success_rates,
        })
    }

    // ── Phase 3: /grafana/guarantees/by-guarantor ───────────────────────

    pub async fn grafana_guarantees_by_guarantor(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<GuarantorBreakdownResponse, sqlx::Error> {
        let mapping = self.node_core_mapping(start, end).await?;

        // Group by node_id to get primary core + all cores
        let mut node_map: std::collections::HashMap<String, (Vec<i16>, i64, Option<DateTime<Utc>>)> =
            std::collections::HashMap::new();
        for row in &mapping {
            let entry = node_map
                .entry(row.node_id.clone())
                .or_insert_with(|| (Vec::new(), 0, None));
            entry.0.push(row.core);
            entry.1 += row.guarantee_count;
            entry.2 = Some(entry.2.map_or(row.last_guarantee, |prev: DateTime<Utc>| prev.max(row.last_guarantee)));
        }

        let mut guarantors: Vec<GuarantorRow> = node_map
            .into_iter()
            .map(|(node_id, (mut cores, count, last))| {
                cores.sort();
                cores.dedup();
                let primary = cores.first().copied();
                GuarantorRow {
                    node_id,
                    primary_core: primary,
                    guarantee_count: count,
                    last_guarantee: last,
                    cores_active: cores,
                }
            })
            .collect();
        guarantors.sort_by(|a, b| b.guarantee_count.cmp(&a.guarantee_count));
        let total = guarantors.len() as i64;

        Ok(GuarantorBreakdownResponse {
            guarantors,
            total_guarantors: total,
        })
    }

    // ── Phase 3: /grafana/wp-stats ──────────────────────────────────────

    pub async fn grafana_wp_stats(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<WpStatsResponse, sqlx::Error> {
        // Pipeline stages from wp_tracking
        let stage_row = sqlx::query(
            r#"
            SELECT
                COUNT(*) FILTER (WHERE received_at IS NOT NULL)::BIGINT AS received,
                COUNT(*) FILTER (WHERE authorized_at IS NOT NULL)::BIGINT AS authorized,
                COUNT(*) FILTER (WHERE refined_at IS NOT NULL)::BIGINT AS refined,
                COUNT(*) FILTER (WHERE report_built_at IS NOT NULL)::BIGINT AS report_built,
                COUNT(*) FILTER (WHERE guarantee_built_at IS NOT NULL)::BIGINT AS guarantee_built,
                COUNT(*) FILTER (WHERE distributed_at IS NOT NULL)::BIGINT AS distributed,
                COUNT(*) FILTER (WHERE failed_at IS NOT NULL)::BIGINT AS failed
            FROM wp_tracking
            WHERE first_seen >= $1 AND first_seen < $2
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        // Pre-pipeline counts from aggregates (types 90, 91, 93)
        let pre_row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 90), 0)::BIGINT AS submissions,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 91), 0)::BIGINT AS being_shared,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 93), 0)::BIGINT AS duplicates
            FROM all_event_stats_1m
            WHERE bucket >= $1 AND bucket < $2
              AND event_type IN (90, 91, 93)
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        // By core
        let core_rows = sqlx::query(
            r#"
            SELECT core, COUNT(*)::BIGINT AS count
            FROM wp_tracking
            WHERE first_seen >= $1 AND first_seen < $2
            GROUP BY core
            ORDER BY core ASC
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        Ok(WpStatsResponse {
            totals: WpStageTotals {
                submissions: pre_row.get("submissions"),
                being_shared: pre_row.get("being_shared"),
                duplicates: pre_row.get("duplicates"),
                received: stage_row.get("received"),
                authorized: stage_row.get("authorized"),
                refined: stage_row.get("refined"),
                report_built: stage_row.get("report_built"),
                guarantee_built: stage_row.get("guarantee_built"),
                distributed: stage_row.get("distributed"),
                failed: stage_row.get("failed"),
            },
            by_core: core_rows
                .iter()
                .map(|row| WpCoreCount {
                    core: row.get("core"),
                    count: row.get("count"),
                })
                .collect(),
        })
    }

    // ── Phase 3: /grafana/validators/cores ──────────────────────────────

    pub async fn grafana_validators_cores(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<ValidatorCoreRow>, sqlx::Error> {
        let mapping = self.node_core_mapping(start, end).await?;

        // Group by node → pick primary core (highest guarantee_count)
        let mut node_map: std::collections::HashMap<String, (Option<i16>, i64)> =
            std::collections::HashMap::new();
        for row in &mapping {
            let entry = node_map
                .entry(row.node_id.clone())
                .or_insert((None, 0));
            entry.1 += row.guarantee_count;
            // Primary = core with most guarantees
            if entry.0.is_none() || row.guarantee_count > 0 {
                entry.0 = Some(row.core);
            }
        }

        let mut result: Vec<ValidatorCoreRow> = node_map
            .into_iter()
            .map(|(node_id, (core, count))| ValidatorCoreRow {
                node_id,
                primary_core: core,
                guarantee_count: count,
            })
            .collect();
        result.sort_by(|a, b| b.guarantee_count.cmp(&a.guarantee_count));

        Ok(result)
    }

    // ── Phase 4: /grafana/cores/{id}/validators ─────────────────────────

    /// Per-core validator list from guarantee_convergence + nodes.
    pub async fn grafana_core_validators(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        core: i16,
    ) -> Result<CoreValidatorsResponse, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                gc.builder_node_id AS node_id,
                COUNT(*)::BIGINT AS guarantee_count,
                MAX(gc.built_at) AS last_guarantee,
                n.implementation_name,
                n.implementation_version,
                n.is_connected
            FROM guarantee_convergence gc
            LEFT JOIN nodes n ON gc.builder_node_id = n.node_id
            WHERE gc.built_at >= $1 AND gc.built_at < $2
              AND gc.core = $3
              AND gc.builder_node_id IS NOT NULL
            GROUP BY gc.builder_node_id, n.implementation_name, n.implementation_version, n.is_connected
            ORDER BY guarantee_count DESC
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(core)
        .fetch_all(self.pool())
        .await?;

        let validators: Vec<CoreValidatorRow> = rows
            .iter()
            .map(|row| CoreValidatorRow {
                node_id: row.get("node_id"),
                guarantee_count: row.get("guarantee_count"),
                last_guarantee: row.get("last_guarantee"),
                implementation_name: row.get("implementation_name"),
                implementation_version: row.get("implementation_version"),
                is_connected: row.get("is_connected"),
            })
            .collect();

        let total_active = validators.len() as i64;

        Ok(CoreValidatorsResponse {
            core,
            validators,
            total_active,
        })
    }

    // ── Phase 3: /grafana/network-health ────────────────────────────────

    pub async fn grafana_network_health(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<NetworkHealthResponse, sqlx::Error> {
        // Get event counts for health scoring — only the types we actually use.
        // Restricting event_type lets the planner prune UNION view branches.
        let health_types: &[i16] = &[
            41, 42, 44, 46, // block production (authored, importing-failed, imported-failed, executed-failed)
            92, 94, 99,     // work packages (failed, received, duplicate)
            122, 125,       // DA shards (failed, transferred)
        ];
        let counts = sqlx::query(
            r#"
            SELECT event_type, COALESCE(SUM(event_count), 0)::BIGINT AS count
            FROM all_event_stats_1m
            WHERE bucket >= $1 AND bucket < $2
              AND event_type = ANY($3)
            GROUP BY event_type
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(health_types)
        .fetch_all(self.pool())
        .await?;

        let mut type_counts: std::collections::HashMap<i16, i64> = std::collections::HashMap::new();
        for row in &counts {
            type_counts.insert(row.get("event_type"), row.get("count"));
        }

        let get = |et: i16| -> i64 { *type_counts.get(&et).unwrap_or(&0) };

        // Component scores
        let mut components = Vec::new();
        let mut alerts = Vec::new();

        // 1. Block production
        let authored = get(42);
        let auth_failed = get(41) + get(44) + get(46);
        let block_total = authored + auth_failed;
        let block_score = if block_total > 0 { authored as f64 / block_total as f64 * 100.0 } else { 100.0 };
        let block_status = if block_score >= 95.0 { "healthy" } else if block_score >= 80.0 { "degraded" } else { "unhealthy" };
        if block_score < 95.0 {
            alerts.push(HealthAlert {
                severity: if block_score < 80.0 { "error".into() } else { "warning".into() },
                message: format!("Block production success rate: {:.1}%", block_score),
                component: "block_production".into(),
            });
        }
        components.push(HealthComponent {
            name: "block_production".into(),
            score: block_score,
            status: block_status.into(),
            issues: Vec::new(),
        });

        // 2. Work packages
        let wp_received = get(94);
        let wp_failed = get(92) + get(99);
        let wp_total = wp_received + wp_failed;
        let wp_score = if wp_total > 0 { wp_received as f64 / wp_total as f64 * 100.0 } else { 100.0 };
        let wp_status = if wp_score >= 95.0 { "healthy" } else if wp_score >= 80.0 { "degraded" } else { "unhealthy" };
        components.push(HealthComponent {
            name: "work_packages".into(),
            score: wp_score,
            status: wp_status.into(),
            issues: Vec::new(),
        });

        // 3. DA / Shards
        let shard_ok = get(125);
        let shard_fail = get(122);
        let shard_total = shard_ok + shard_fail;
        let da_score = if shard_total > 0 { shard_ok as f64 / shard_total as f64 * 100.0 } else { 100.0 };
        let da_status = if da_score >= 95.0 { "healthy" } else if da_score >= 80.0 { "degraded" } else { "unhealthy" };
        components.push(HealthComponent {
            name: "data_availability".into(),
            score: da_score,
            status: da_status.into(),
            issues: Vec::new(),
        });

        // 4. Connectivity (from nodes table)
        let conn_row = sqlx::query(
            "SELECT COUNT(*)::BIGINT AS total, COUNT(*) FILTER (WHERE is_connected)::BIGINT AS connected FROM nodes",
        )
        .fetch_one(self.pool())
        .await?;
        let total_nodes: i64 = conn_row.get("total");
        let connected: i64 = conn_row.get("connected");
        let conn_score = if total_nodes > 0 { connected as f64 / total_nodes as f64 * 100.0 } else { 100.0 };
        let conn_status = if conn_score >= 90.0 { "healthy" } else if conn_score >= 70.0 { "degraded" } else { "unhealthy" };
        components.push(HealthComponent {
            name: "connectivity".into(),
            score: conn_score,
            status: conn_status.into(),
            issues: Vec::new(),
        });

        // 5. Event throughput (sum of health-relevant types as alive/dead proxy)
        let total_events: i64 = type_counts.values().sum();
        let throughput_score = if total_events > 0 { 100.0 } else { 0.0 };
        components.push(HealthComponent {
            name: "event_throughput".into(),
            score: throughput_score,
            status: if throughput_score > 50.0 { "healthy" } else { "unhealthy" }.into(),
            issues: Vec::new(),
        });

        // Overall: weighted average
        let health_score = components.iter().map(|c| c.score).sum::<f64>() / components.len() as f64;
        let overall_health = if health_score >= 90.0 { "healthy" } else if health_score >= 70.0 { "degraded" } else { "unhealthy" };

        Ok(NetworkHealthResponse {
            health_score,
            overall_health: overall_health.into(),
            components,
            alerts,
        })
    }

    // ── Phase 4: /grafana/wp-active ─────────────────────────────────────

    /// Active (in-flight) work packages with pipeline health summary.
    pub async fn grafana_wp_active(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<WpActiveResponse, sqlx::Error> {
        // WP list
        let rows = sqlx::query(
            r#"
            SELECT
                encode(wp_hash, 'hex') AS wp_hash,
                core, node_id, service_ids, stage,
                refine_gas_used, failure_reason,
                first_seen, last_updated,
                received_at, authorized_at, refined_at,
                report_built_at, guarantee_built_at, distributed_at, failed_at,
                received_by, guaranteed_by,
                (EXTRACT(EPOCH FROM (last_updated - first_seen)) * 1000)::FLOAT8 AS elapsed_ms
            FROM wp_tracking
            WHERE first_seen >= $1 AND first_seen < $2
            ORDER BY first_seen DESC
            LIMIT 200
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        let work_packages: Vec<WpActiveRow> = rows
            .iter()
            .map(|row| WpActiveRow {
                wp_hash: row.get("wp_hash"),
                core: row.get("core"),
                node_id: row.get("node_id"),
                service_ids: row.get::<Vec<i32>, _>("service_ids").into_iter().map(DbServiceId).collect(),
                stage: row.get("stage"),
                refine_gas_used: row.get("refine_gas_used"),
                failure_reason: row.get("failure_reason"),
                first_seen: row.get("first_seen"),
                last_updated: row.get("last_updated"),
                received_at: row.get("received_at"),
                authorized_at: row.get("authorized_at"),
                refined_at: row.get("refined_at"),
                report_built_at: row.get("report_built_at"),
                guarantee_built_at: row.get("guarantee_built_at"),
                distributed_at: row.get("distributed_at"),
                failed_at: row.get("failed_at"),
                received_by: row.get("received_by"),
                guaranteed_by: row.get("guaranteed_by"),
                elapsed_ms: row.get::<Option<f64>, _>("elapsed_ms").unwrap_or(0.0),
            })
            .collect();

        // Aggregates from same filtered set
        let agg = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS total,
                COUNT(*) FILTER (WHERE stage = 0)::BIGINT AS at_received,
                COUNT(*) FILTER (WHERE stage = 1)::BIGINT AS at_authorized,
                COUNT(*) FILTER (WHERE stage = 2)::BIGINT AS at_refined,
                COUNT(*) FILTER (WHERE stage = 3)::BIGINT AS at_report_built,
                COUNT(*) FILTER (WHERE stage = 4)::BIGINT AS at_guarantee_built,
                COUNT(*) FILTER (WHERE received_at IS NOT NULL)::BIGINT AS reached_received,
                COUNT(*) FILTER (WHERE authorized_at IS NOT NULL)::BIGINT AS reached_authorized,
                COUNT(*) FILTER (WHERE refined_at IS NOT NULL)::BIGINT AS reached_refined,
                COUNT(*) FILTER (WHERE report_built_at IS NOT NULL)::BIGINT AS reached_report_built,
                COUNT(*) FILTER (WHERE guarantee_built_at IS NOT NULL)::BIGINT AS reached_guarantee_built,
                COUNT(*) FILTER (WHERE distributed_at IS NOT NULL)::BIGINT AS reached_distributed,
                COUNT(*) FILTER (WHERE failed_at IS NOT NULL)::BIGINT AS reached_failed,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (authorized_at - received_at)) * 1000)
                    FILTER (WHERE authorized_at IS NOT NULL AND received_at IS NOT NULL) AS auth_p50,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (authorized_at - received_at)) * 1000)
                    FILTER (WHERE authorized_at IS NOT NULL AND received_at IS NOT NULL) AS auth_p95,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (refined_at - authorized_at)) * 1000)
                    FILTER (WHERE refined_at IS NOT NULL AND authorized_at IS NOT NULL) AS refine_p50,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (refined_at - authorized_at)) * 1000)
                    FILTER (WHERE refined_at IS NOT NULL AND authorized_at IS NOT NULL) AS refine_p95,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (report_built_at - refined_at)) * 1000)
                    FILTER (WHERE report_built_at IS NOT NULL AND refined_at IS NOT NULL) AS report_p50,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (report_built_at - refined_at)) * 1000)
                    FILTER (WHERE report_built_at IS NOT NULL AND refined_at IS NOT NULL) AS report_p95,
                percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (guarantee_built_at - report_built_at)) * 1000)
                    FILTER (WHERE guarantee_built_at IS NOT NULL AND report_built_at IS NOT NULL) AS guarantee_p50,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (guarantee_built_at - report_built_at)) * 1000)
                    FILTER (WHERE guarantee_built_at IS NOT NULL AND report_built_at IS NOT NULL) AS guarantee_p95
            FROM wp_tracking
            WHERE first_seen >= $1 AND first_seen < $2
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        // Failure breakdown
        let failure_rows = sqlx::query(
            r#"
            SELECT failure_reason AS reason, COUNT(*)::BIGINT AS count
            FROM wp_tracking
            WHERE failed_at IS NOT NULL
              AND failure_reason IS NOT NULL
              AND first_seen >= $1 AND first_seen < $2
            GROUP BY failure_reason
            ORDER BY count DESC
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        Ok(WpActiveResponse {
            work_packages,
            summary: WpActiveSummary {
                total: agg.get("total"),
                at_received: agg.get("at_received"),
                at_authorized: agg.get("at_authorized"),
                at_refined: agg.get("at_refined"),
                at_report_built: agg.get("at_report_built"),
                at_guarantee_built: agg.get("at_guarantee_built"),
            },
            reached: WpReachedCounts {
                received: agg.get("reached_received"),
                authorized: agg.get("reached_authorized"),
                refined: agg.get("reached_refined"),
                report_built: agg.get("reached_report_built"),
                guarantee_built: agg.get("reached_guarantee_built"),
                distributed: agg.get("reached_distributed"),
                failed: agg.get("reached_failed"),
            },
            stage_duration_percentiles: WpStageDurations {
                authorize_p50_ms: agg.get("auth_p50"),
                authorize_p95_ms: agg.get("auth_p95"),
                refine_p50_ms: agg.get("refine_p50"),
                refine_p95_ms: agg.get("refine_p95"),
                report_p50_ms: agg.get("report_p50"),
                report_p95_ms: agg.get("report_p95"),
                guarantee_p50_ms: agg.get("guarantee_p50"),
                guarantee_p95_ms: agg.get("guarantee_p95"),
            },
            failure_breakdown: failure_rows
                .iter()
                .map(|row| FailureBreakdownEntry {
                    reason: row.get("reason"),
                    count: row.get("count"),
                })
                .collect(),
        })
    }

    // ── Phase 4: /grafana/wp/{hash} ─────────────────────────────────────

    /// Work package detail: summary + raw events.
    pub async fn grafana_wp_detail(
        &self,
        wp_hash_bytes: &[u8],
    ) -> Result<WpDetailResponse, sqlx::Error> {
        // Summary from wp_tracking
        let summary_row = sqlx::query(
            r#"
            SELECT
                encode(wp_hash, 'hex') AS wp_hash,
                first_seen, last_updated, stage,
                received_by, guaranteed_by,
                service_ids,
                received_at, authorized_at, refined_at,
                report_built_at, guarantee_built_at, distributed_at, failed_at
            FROM wp_tracking
            WHERE wp_hash = $1
            "#,
        )
        .bind(wp_hash_bytes)
        .fetch_optional(self.pool())
        .await?;

        let summary = summary_row.map(|row| WpTrackingRow {
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
        });

        // Raw events from ingested_raw_events (1h retention, wp_hash hot column)
        let event_rows = sqlx::query(
            r#"
            SELECT timestamp, node_id, event_type, data, created_at
            FROM ingested_raw_events
            WHERE wp_hash = $1
            ORDER BY timestamp ASC
            "#,
        )
        .bind(wp_hash_bytes)
        .fetch_all(self.pool())
        .await?;

        let events: Vec<EventRow> = event_rows
            .iter()
            .map(|row| EventRow {
                ts: row.get("timestamp"),
                node_id: row.get("node_id"),
                event_type: row.get("event_type"),
                data: row.get("data"),
                created_at: row.get("created_at"),
            })
            .collect();

        Ok(WpDetailResponse { summary, events })
    }

    // ── Phase 4: /grafana/wp/batch ──────────────────────────────────────

    /// Batch WP summary lookup by multiple hashes.
    pub async fn grafana_wp_batch(
        &self,
        wp_hashes: &[Vec<u8>],
    ) -> Result<Vec<WpTrackingRow>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                encode(wp_hash, 'hex') AS wp_hash,
                first_seen, last_updated, stage,
                received_by, guaranteed_by,
                service_ids,
                received_at, authorized_at, refined_at,
                report_built_at, guarantee_built_at, distributed_at, failed_at
            FROM wp_tracking
            WHERE wp_hash = ANY($1)
            ORDER BY first_seen DESC
            "#,
        )
        .bind(wp_hashes)
        .fetch_all(self.pool())
        .await?;

        Ok(rows
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
            .collect())
    }

    // ── Phase 4: /grafana/blocks/summary ────────────────────────────────

    /// Block production overview.
    pub async fn grafana_blocks_summary(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<BlocksSummaryResponse, sqlx::Error> {
        // Totals from aggregates
        let totals_row = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 40), 0)::BIGINT AS authoring_started,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 41), 0)::BIGINT AS authoring_failed,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 42), 0)::BIGINT AS authored,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 43), 0)::BIGINT AS importing,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 44), 0)::BIGINT AS verification_failed,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 45), 0)::BIGINT AS verified,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 46), 0)::BIGINT AS execution_failed,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 47), 0)::BIGINT AS executed,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 11), 0)::BIGINT AS best_block_changes,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 12), 0)::BIGINT AS finalized_block_changes
            FROM all_event_stats_1m
            WHERE bucket >= $1 AND bucket < $2
              AND event_type IN (40, 41, 42, 43, 44, 45, 46, 47, 11, 12)
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        // Authoring by node
        let node_rows = sqlx::query(
            r#"
            SELECT node_id, SUM(event_count)::BIGINT AS blocks_authored
            FROM all_event_stats_1m
            WHERE bucket >= $1 AND bucket < $2 AND event_type = 42
            GROUP BY node_id
            ORDER BY blocks_authored DESC
            LIMIT 50
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        Ok(BlocksSummaryResponse {
            totals: BlockTotals {
                authoring_started: totals_row.get("authoring_started"),
                authoring_failed: totals_row.get("authoring_failed"),
                authored: totals_row.get("authored"),
                importing: totals_row.get("importing"),
                verification_failed: totals_row.get("verification_failed"),
                verified: totals_row.get("verified"),
                execution_failed: totals_row.get("execution_failed"),
                executed: totals_row.get("executed"),
                best_block_changes: totals_row.get("best_block_changes"),
                finalized_block_changes: totals_row.get("finalized_block_changes"),
            },
            chain: ChainState {
                best_slot: None,  // Overlaid by handler from LiveCounters
                finalized_slot: None,
            },
            authoring_by_node: node_rows
                .iter()
                .map(|row| AuthoringByNode {
                    node_id: row.get("node_id"),
                    blocks_authored: row.get("blocks_authored"),
                })
                .collect(),
        })
    }

    // ── Phase 4: /grafana/cores/{id}/metrics ────────────────────────────

    /// Core performance metrics.
    pub async fn grafana_core_metrics(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        core: i16,
    ) -> Result<CoreMetricsResponse, sqlx::Error> {
        // Event counts from core_stats
        let counts = sqlx::query(
            r#"
            SELECT
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 94), 0)::BIGINT AS wp_received,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 101), 0)::BIGINT AS refined,
                COALESCE(SUM(event_count) FILTER (WHERE event_type = 92), 0)::BIGINT AS failed
            FROM all_core_stats_1m
            WHERE bucket >= $1 AND bucket < $2 AND core = $3
            "#,
        )
        .bind(start)
        .bind(end)
        .bind(core)
        .fetch_one(self.pool())
        .await?;

        let refined: i64 = counts.get("refined");
        let failed: i64 = counts.get("failed");
        let wp_received: i64 = counts.get("wp_received");
        let total_processed = refined + failed;

        // Latency percentiles from wp_tracking
        let latency = sqlx::query(
            r#"
            SELECT
                percentile_cont(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (COALESCE(distributed_at, last_updated) - received_at)) * 1000)
                    FILTER (WHERE received_at IS NOT NULL) AS p50_ms,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (COALESCE(distributed_at, last_updated) - received_at)) * 1000)
                    FILTER (WHERE received_at IS NOT NULL) AS p95_ms,
                (AVG(EXTRACT(EPOCH FROM (COALESCE(distributed_at, last_updated) - received_at)) * 1000)
                    FILTER (WHERE distributed_at IS NOT NULL AND received_at IS NOT NULL))::FLOAT8 AS avg_completion_ms,
                COALESCE(SUM(refine_gas_used), 0)::BIGINT AS total_gas
            FROM wp_tracking
            WHERE core = $1 AND first_seen >= $2 AND first_seen < $3
            "#,
        )
        .bind(core)
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        Ok(CoreMetricsResponse {
            core,
            processing_efficiency_pct: if total_processed > 0 {
                refined as f64 / total_processed as f64 * 100.0
            } else {
                100.0
            },
            p50_latency_ms: latency.get("p50_ms"),
            p95_latency_ms: latency.get("p95_ms"),
            average_completion_time_ms: latency.get("avg_completion_ms"),
            total_gas_used: latency.get("total_gas"),
            work_packages_processed: wp_received,
        })
    }

    // ── Phase 5: /grafana/execution ─────────────────────────────────────

    /// Execution performance: gas + timing per phase from raw events.
    pub async fn grafana_execution(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<ExecutionMetricsResponse, sqlx::Error> {
        // Refinement (type 101) — jsonb_array_elements on costs array
        let refinement = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS count,
                COALESCE(SUM(CAST(c->'total'->>'gas_used' AS BIGINT)), 0)::BIGINT AS total_gas,
                COALESCE(AVG(CAST(c->'total'->>'gas_used' AS BIGINT)), 0)::FLOAT8 AS avg_gas,
                COALESCE(AVG(CAST(c->'total'->>'elapsed_ns' AS BIGINT)), 0)::FLOAT8 AS avg_time_ns
            FROM ingested_raw_events e, jsonb_array_elements(e.data->'Refined'->'costs') c
            WHERE e.event_type = 101
              AND e.timestamp >= $1 AND e.timestamp < $2
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        // Authorization (type 95) — single cost object
        let authorization = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS count,
                COALESCE(SUM(CAST(data->'Authorized'->'cost'->'total'->>'gas_used' AS BIGINT)), 0)::BIGINT AS total_gas,
                COALESCE(AVG(CAST(data->'Authorized'->'cost'->'total'->>'gas_used' AS BIGINT)), 0)::FLOAT8 AS avg_gas,
                COALESCE(AVG(CAST(data->'Authorized'->'cost'->'total'->>'elapsed_ns' AS BIGINT)), 0)::FLOAT8 AS avg_time_ns
            FROM ingested_raw_events
            WHERE event_type = 95
              AND timestamp >= $1 AND timestamp < $2
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        // Accumulation (type 47) — array of [service_id, cost] pairs
        let accumulation = sqlx::query(
            r#"
            SELECT
                COUNT(*)::BIGINT AS count,
                COALESCE(SUM(CAST(pair->1->'total'->>'gas_used' AS BIGINT)), 0)::BIGINT AS total_gas,
                COALESCE(AVG(CAST(pair->1->'total'->>'gas_used' AS BIGINT)), 0)::FLOAT8 AS avg_gas,
                COALESCE(AVG(CAST(pair->1->'total'->>'elapsed_ns' AS BIGINT)), 0)::FLOAT8 AS avg_time_ns
            FROM ingested_raw_events e, jsonb_array_elements(e.data->'BlockExecuted'->'accumulate_costs') pair
            WHERE e.event_type = 47
              AND e.timestamp >= $1 AND e.timestamp < $2
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_one(self.pool())
        .await?;

        // Per-service gas from accumulation
        let by_service = sqlx::query(
            r#"
            SELECT
                CAST(pair->0 AS INT) AS service_id,
                COALESCE(SUM(CAST(pair->1->'total'->>'gas_used' AS BIGINT)), 0)::BIGINT AS total_gas,
                COUNT(*)::BIGINT AS count
            FROM ingested_raw_events e, jsonb_array_elements(e.data->'BlockExecuted'->'accumulate_costs') pair
            WHERE e.event_type = 47
              AND e.timestamp >= $1 AND e.timestamp < $2
            GROUP BY pair->0
            ORDER BY total_gas DESC
            LIMIT 50
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        fn to_phase(row: &sqlx::postgres::PgRow) -> ExecutionPhaseStats {
            ExecutionPhaseStats {
                count: row.get("count"),
                total_gas: row.get("total_gas"),
                avg_gas: row.get("avg_gas"),
                avg_time_ns: row.get("avg_time_ns"),
            }
        }

        Ok(ExecutionMetricsResponse {
            authorization: to_phase(&authorization),
            refinement: to_phase(&refinement),
            accumulation: to_phase(&accumulation),
            by_service: by_service
                .iter()
                .map(|row| ServiceExecutionRow {
                    service_id: row.get("service_id"),
                    total_gas: row.get("total_gas"),
                    count: row.get("count"),
                })
                .collect(),
        })
    }
}

fn rows_to_convergence_timeseries(rows: &[sqlx::postgres::PgRow]) -> Vec<ConvergenceTimeseriesRow> {
    use crate::histogram::{percentiles_from_histogram, CONVERGENCE_BOUNDS};
    rows.iter().map(|row| {
        let ts: DateTime<Utc> = row.get("ts");
        let hist = [
            row.get::<i32, _>("h_0_2") as u32, row.get::<i32, _>("h_2_5") as u32,
            row.get::<i32, _>("h_5_10") as u32, row.get::<i32, _>("h_10_15") as u32,
            row.get::<i32, _>("h_15_20") as u32, row.get::<i32, _>("h_20_30") as u32,
            row.get::<i32, _>("h_30_50") as u32, row.get::<i32, _>("h_50_75") as u32,
            row.get::<i32, _>("h_75_100") as u32, row.get::<i32, _>("h_100_150") as u32,
            row.get::<i32, _>("h_150_250") as u32, row.get::<i32, _>("h_250_500") as u32,
            row.get::<i32, _>("h_500_1000") as u32, row.get::<i32, _>("h_1000_2000") as u32,
            row.get::<i32, _>("h_2000_5000") as u32, row.get::<i32, _>("h_5000_10000") as u32,
            row.get::<i32, _>("h_10000_15000") as u32, row.get::<i32, _>("h_15000_20000") as u32,
            row.get::<i32, _>("h_20000_25000") as u32, row.get::<i32, _>("h_25000_30000") as u32,
            row.get::<i32, _>("h_30000_60000") as u32, row.get::<i32, _>("h_60000_120000") as u32,
            row.get::<i32, _>("h_120000_plus") as u32,
        ];
        let total: i32 = row.get("hist_total");
        let p = percentiles_from_histogram(&hist, total as u32, &CONVERGENCE_BOUNDS);
        ConvergenceTimeseriesRow {
            ts,
            p50_ms: p.map(|p| p.0), p75_ms: p.map(|p| p.1),
            p95_ms: p.map(|p| p.2), p99_ms: p.map(|p| p.3),
            p100_ms: p.map(|p| p.4),
            sample_count: total,
        }
    }).collect()
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

    #[test]
    fn hist_percentiles_empty() {
        let buckets = [0u32; 14];
        assert_eq!(percentiles_from_histogram(&buckets, 0), None);
    }

    #[test]
    fn hist_percentiles_all_in_one_bucket() {
        // 100 samples in bucket index 4 [10,25) → upper bound = 25
        let mut buckets = [0u32; 14];
        buckets[4] = 100;
        let (p50, p75, p95, p99, p100) = percentiles_from_histogram(&buckets, 100).unwrap();
        assert_eq!(p50, 25);
        assert_eq!(p75, 25);
        assert_eq!(p95, 25);
        assert_eq!(p99, 25);
        assert_eq!(p100, 25);
    }

    #[test]
    fn hist_percentiles_split_two_buckets() {
        // 50 in bucket 4 [10,25) + 50 in bucket 6 [50,100)
        let mut buckets = [0u32; 14];
        buckets[4] = 50;
        buckets[6] = 50;
        let (p50, _p75, _p95, _p99, p100) = percentiles_from_histogram(&buckets, 100).unwrap();
        // p50 threshold = ceil(100*0.50) = 50, cumsum reaches 50 at bucket 4 → upper bound = 25
        assert_eq!(p50, 25);
        // p100 threshold = ceil(100*1.0) = 100, cumsum reaches 100 at bucket 6 → upper bound = 100
        assert_eq!(p100, 100);
    }

    #[test]
    fn hist_percentiles_single_sample() {
        // 1 sample in bucket 5 [25,50) → upper bound = 50
        let mut buckets = [0u32; 14];
        buckets[5] = 1;
        let (p50, p75, p95, p99, p100) = percentiles_from_histogram(&buckets, 1).unwrap();
        assert_eq!(p50, 50);
        assert_eq!(p75, 50);
        assert_eq!(p95, 50);
        assert_eq!(p99, 50);
        assert_eq!(p100, 50);
    }

    #[test]
    fn hist_percentiles_overflow_bucket() {
        // Samples in last bucket index 13 [5000,∞) → lower bound = 5000 (overflow)
        let mut buckets = [0u32; 14];
        buckets[13] = 10;
        let (p50, p75, p95, p99, p100) = percentiles_from_histogram(&buckets, 10).unwrap();
        assert_eq!(p50, 5000);
        assert_eq!(p75, 5000);
        assert_eq!(p95, 5000);
        assert_eq!(p99, 5000);
        assert_eq!(p100, 5000);
    }

    #[test]
    fn hist_percentiles_spread() {
        // Spread across multiple buckets — verify ordering
        let mut buckets = [0u32; 14];
        buckets[0] = 5;   // [0,1)
        buckets[2] = 10;  // [2,5)
        buckets[5] = 20;  // [25,50)
        buckets[8] = 15;  // [250,500)
        buckets[11] = 10; // [2000,3000)
        let total = 5 + 10 + 20 + 15 + 10;
        let (p50, p75, p95, p99, p100) = percentiles_from_histogram(&buckets, total).unwrap();
        assert!(p50 <= p75, "p50 ({p50}) <= p75 ({p75})");
        assert!(p75 <= p95, "p75 ({p75}) <= p95 ({p95})");
        assert!(p95 <= p99, "p95 ({p95}) <= p99 ({p99})");
        assert!(p99 <= p100, "p99 ({p99}) <= p100 ({p100})");
    }
}
