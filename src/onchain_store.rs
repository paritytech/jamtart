//! Query methods for on-chain statistics tables.
//!
//! Follows the same `impl EventStore` pattern as `grafana_store.rs`.
//! All queries filter `on_best_chain = true` to exclude orphaned fork data.

use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::grafana_types::DbServiceId;
use crate::onchain_types::*;
use crate::store::EventStore;

/// Whitelisted time_bucket intervals for dynamic SQL (6s-aligned sub-minute).
const VALID_INTERVALS: &[&str] = &[
    "6s", "12s", "18s", "24s", "30s", "1m", "2m", "5m", "10m", "15m", "30m", "1h", "2h", "4h",
    "6h", "12h", "1d",
];

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
    format!("{s} seconds")
}

/// Convert a human-friendly interval string to seconds.
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

/// Snap an arbitrary interval to the nearest valid (>= input) whitelisted value.
fn snap_interval(input: &str) -> &'static str {
    if let Some(&valid) = VALID_INTERVALS.iter().find(|&&v| v == input) {
        return valid;
    }
    let input_secs = match interval_to_seconds(input) {
        Some(s) if s > 0 => s,
        _ => return "1m",
    };
    for &candidate in VALID_INTERVALS {
        if let Some(candidate_secs) = interval_to_seconds(candidate) {
            if candidate_secs >= input_secs {
                return candidate;
            }
        }
    }
    "1d"
}

fn validate_interval(interval: &str) -> Result<String, sqlx::Error> {
    Ok(interval_to_pg(snap_interval(interval)))
}

impl EventStore {
    // ── Cores ────────────────────────────────────────────────────────────

    /// Per-core on-chain activity summary (all 341 cores).
    pub async fn onchain_cores_summary(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<OnchainCoreSummary>, sqlx::Error> {
        sqlx::query_as::<_, OnchainCoreSummary>(
            r#"
            SELECT
                core,
                COALESCE(SUM(gas_used), 0)::BIGINT AS gas_used,
                COALESCE(SUM(da_load::BIGINT), 0)::BIGINT AS da_load,
                COALESCE(AVG(popularity), 0)::SMALLINT AS popularity_avg,
                COALESCE(SUM(imports::BIGINT), 0)::BIGINT AS imports,
                COALESCE(SUM(extrinsic_count::BIGINT), 0)::BIGINT AS extrinsic_count,
                COALESCE(SUM(extrinsic_size::BIGINT), 0)::BIGINT AS extrinsic_size,
                COALESCE(SUM(exports::BIGINT), 0)::BIGINT AS exports,
                COALESCE(SUM(bundle_size::BIGINT), 0)::BIGINT AS bundle_size
            FROM onchain_core_stats
            WHERE timestamp >= $1 AND timestamp < $2
              AND on_best_chain = true
            GROUP BY core
            ORDER BY core
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await
    }

    /// Time-bucketed per-core on-chain stats.
    pub async fn onchain_cores_timeseries(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
    ) -> Result<Vec<OnchainCoreTimeseries>, sqlx::Error> {
        let pg_interval = validate_interval(interval)?;

        let sql = format!(
            r#"
            SELECT
                time_bucket('{pg_interval}'::interval, timestamp) AS ts,
                core,
                COALESCE(SUM(gas_used), 0)::BIGINT AS gas_used,
                COALESCE(SUM(da_load::BIGINT), 0)::BIGINT AS da_load,
                COALESCE(AVG(popularity), 0)::SMALLINT AS popularity,
                COALESCE(SUM(imports::BIGINT), 0)::BIGINT AS imports,
                COALESCE(SUM(extrinsic_count::BIGINT), 0)::BIGINT AS extrinsic_count,
                COALESCE(SUM(extrinsic_size::BIGINT), 0)::BIGINT AS extrinsic_size,
                COALESCE(SUM(exports::BIGINT), 0)::BIGINT AS exports,
                COALESCE(SUM(bundle_size::BIGINT), 0)::BIGINT AS bundle_size
            FROM onchain_core_stats
            WHERE timestamp >= $1 AND timestamp < $2
              AND on_best_chain = true
            GROUP BY ts, core
            ORDER BY ts, core
            "#
        );

        sqlx::query_as::<_, OnchainCoreTimeseries>(&sql)
            .bind(start)
            .bind(end)
            .fetch_all(self.pool())
            .await
    }

    /// Raw per-block on-chain stats for a single core.
    pub async fn onchain_core_detail(
        &self,
        core: i16,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<OnchainCoreDetail>, sqlx::Error> {
        sqlx::query_as::<_, OnchainCoreDetail>(
            r#"
            SELECT timestamp, slot, core, gas_used, da_load, popularity,
                   imports, extrinsic_count, extrinsic_size, exports, bundle_size
            FROM onchain_core_stats
            WHERE core = $1
              AND timestamp >= $2 AND timestamp < $3
              AND on_best_chain = true
            ORDER BY timestamp DESC
            LIMIT 1000
            "#,
        )
        .bind(core)
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await
    }

    // ── Services ─────────────────────────────────────────────────────────

    /// Per-service on-chain activity summary.
    pub async fn onchain_services_summary(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        services: Option<&[DbServiceId]>,
    ) -> Result<Vec<OnchainServiceSummary>, sqlx::Error> {
        let service_filter = if services.is_some() {
            "AND service_id = ANY($3)"
        } else {
            ""
        };
        let sql = format!(
            r#"
            SELECT
                service_id,
                COALESCE(SUM(provided_count::BIGINT), 0)::BIGINT AS provided_count,
                COALESCE(SUM(provided_size::BIGINT), 0)::BIGINT AS provided_size,
                COALESCE(SUM(refinement_count::BIGINT), 0)::BIGINT AS refinement_count,
                COALESCE(SUM(refinement_gas), 0)::BIGINT AS refinement_gas,
                COALESCE(SUM(imports::BIGINT), 0)::BIGINT AS imports,
                COALESCE(SUM(extrinsic_count::BIGINT), 0)::BIGINT AS extrinsic_count,
                COALESCE(SUM(extrinsic_size::BIGINT), 0)::BIGINT AS extrinsic_size,
                COALESCE(SUM(exports::BIGINT), 0)::BIGINT AS exports,
                COALESCE(SUM(accumulate_count::BIGINT), 0)::BIGINT AS accumulate_count,
                COALESCE(SUM(accumulate_gas), 0)::BIGINT AS accumulate_gas
            FROM onchain_service_stats
            WHERE timestamp >= $1 AND timestamp < $2
              AND on_best_chain = true
              {service_filter}
            GROUP BY service_id
            ORDER BY service_id
            "#
        );
        let svc_i32 = services.map(DbServiceId::as_i32_vec);
        let mut query = sqlx::query(&sql).bind(start).bind(end);
        if let Some(ref svc) = svc_i32 {
            query = query.bind(svc);
        }
        let rows = query.fetch_all(self.pool()).await?;

        Ok(rows
            .iter()
            .map(|row| OnchainServiceSummary {
                service_id: DbServiceId(row.get("service_id")),
                provided_count: row.get("provided_count"),
                provided_size: row.get("provided_size"),
                refinement_count: row.get("refinement_count"),
                refinement_gas: row.get("refinement_gas"),
                imports: row.get("imports"),
                extrinsic_count: row.get("extrinsic_count"),
                extrinsic_size: row.get("extrinsic_size"),
                exports: row.get("exports"),
                accumulate_count: row.get("accumulate_count"),
                accumulate_gas: row.get("accumulate_gas"),
            })
            .collect())
    }

    /// Time-bucketed per-service on-chain stats.
    pub async fn onchain_services_timeseries(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
        services: Option<&[DbServiceId]>,
    ) -> Result<Vec<OnchainServiceTimeseries>, sqlx::Error> {
        let pg_interval = validate_interval(interval)?;

        let service_filter = if services.is_some() {
            "AND service_id = ANY($3)"
        } else {
            ""
        };
        let sql = format!(
            r#"
            SELECT
                time_bucket('{pg_interval}'::interval, timestamp) AS ts,
                service_id,
                COALESCE(SUM(provided_count::BIGINT), 0)::BIGINT AS provided_count,
                COALESCE(SUM(provided_size::BIGINT), 0)::BIGINT AS provided_size,
                COALESCE(SUM(refinement_count::BIGINT), 0)::BIGINT AS refinement_count,
                COALESCE(SUM(refinement_gas), 0)::BIGINT AS refinement_gas,
                COALESCE(SUM(imports::BIGINT), 0)::BIGINT AS imports,
                COALESCE(SUM(extrinsic_count::BIGINT), 0)::BIGINT AS extrinsic_count,
                COALESCE(SUM(extrinsic_size::BIGINT), 0)::BIGINT AS extrinsic_size,
                COALESCE(SUM(exports::BIGINT), 0)::BIGINT AS exports,
                COALESCE(SUM(accumulate_count::BIGINT), 0)::BIGINT AS accumulate_count,
                COALESCE(SUM(accumulate_gas), 0)::BIGINT AS accumulate_gas
            FROM onchain_service_stats
            WHERE timestamp >= $1 AND timestamp < $2
              AND on_best_chain = true
              {service_filter}
            GROUP BY ts, service_id
            ORDER BY ts, service_id
            "#
        );

        let svc_i32 = services.map(DbServiceId::as_i32_vec);
        let mut query = sqlx::query(&sql).bind(start).bind(end);
        if let Some(ref svc) = svc_i32 {
            query = query.bind(svc);
        }
        let rows = query.fetch_all(self.pool()).await?;

        Ok(rows
            .iter()
            .map(|row| OnchainServiceTimeseries {
                ts: row.get("ts"),
                service_id: DbServiceId(row.get("service_id")),
                provided_count: row.get("provided_count"),
                provided_size: row.get("provided_size"),
                refinement_count: row.get("refinement_count"),
                refinement_gas: row.get("refinement_gas"),
                imports: row.get("imports"),
                extrinsic_count: row.get("extrinsic_count"),
                extrinsic_size: row.get("extrinsic_size"),
                exports: row.get("exports"),
                accumulate_count: row.get("accumulate_count"),
                accumulate_gas: row.get("accumulate_gas"),
            })
            .collect())
    }

    /// Raw per-block on-chain stats for a single service.
    pub async fn onchain_service_detail(
        &self,
        service_id: DbServiceId,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<OnchainServiceDetail>, sqlx::Error> {
        let rows = sqlx::query(
            r#"
            SELECT timestamp, slot, service_id, provided_count, provided_size,
                   refinement_count, refinement_gas, imports, extrinsic_count,
                   extrinsic_size, exports, accumulate_count, accumulate_gas
            FROM onchain_service_stats
            WHERE service_id = $1
              AND timestamp >= $2 AND timestamp < $3
              AND on_best_chain = true
            ORDER BY timestamp DESC
            LIMIT 1000
            "#,
        )
        .bind(service_id.0)
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await?;

        Ok(rows
            .iter()
            .map(|row| OnchainServiceDetail {
                timestamp: row.get("timestamp"),
                slot: row.get("slot"),
                service_id: DbServiceId(row.get("service_id")),
                provided_count: row.get("provided_count"),
                provided_size: row.get("provided_size"),
                refinement_count: row.get("refinement_count"),
                refinement_gas: row.get("refinement_gas"),
                imports: row.get("imports"),
                extrinsic_count: row.get("extrinsic_count"),
                extrinsic_size: row.get("extrinsic_size"),
                exports: row.get("exports"),
                accumulate_count: row.get("accumulate_count"),
                accumulate_gas: row.get("accumulate_gas"),
            })
            .collect())
    }

    // ── Validators ───────────────────────────────────────────────────────

    /// Per-validator on-chain stats summary (all 1024 validators, MAX aggregation).
    pub async fn onchain_validators_summary(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<OnchainValidatorSummary>, sqlx::Error> {
        sqlx::query_as::<_, OnchainValidatorSummary>(
            r#"
            SELECT
                validator_index,
                COALESCE(MAX(blocks_produced), 0) AS blocks_produced,
                COALESCE(MAX(tickets), 0) AS tickets,
                COALESCE(MAX(preimages), 0) AS preimages,
                COALESCE(MAX(preimages_size), 0) AS preimages_size,
                COALESCE(MAX(guarantees), 0) AS guarantees,
                COALESCE(MAX(assurances), 0) AS assurances
            FROM onchain_validator_stats
            WHERE timestamp >= $1 AND timestamp < $2
              AND on_best_chain = true
            GROUP BY validator_index
            ORDER BY validator_index
            "#,
        )
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await
    }

    /// Time-bucketed per-validator on-chain stats (MAX aggregation).
    pub async fn onchain_validators_timeseries(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        interval: &str,
        validators: Option<&[i16]>,
    ) -> Result<Vec<OnchainValidatorTimeseries>, sqlx::Error> {
        let pg_interval = validate_interval(interval)?;

        let validator_filter = if validators.is_some() {
            "AND validator_index = ANY($3)"
        } else {
            ""
        };
        let sql = format!(
            r#"
            SELECT
                time_bucket('{pg_interval}'::interval, timestamp) AS ts,
                validator_index,
                COALESCE(MAX(blocks_produced), 0) AS blocks_produced,
                COALESCE(MAX(tickets), 0) AS tickets,
                COALESCE(MAX(preimages), 0) AS preimages,
                COALESCE(MAX(preimages_size), 0) AS preimages_size,
                COALESCE(MAX(guarantees), 0) AS guarantees,
                COALESCE(MAX(assurances), 0) AS assurances
            FROM onchain_validator_stats
            WHERE timestamp >= $1 AND timestamp < $2
              AND on_best_chain = true
              {validator_filter}
            GROUP BY ts, validator_index
            ORDER BY ts, validator_index
            "#
        );

        let mut query = sqlx::query_as::<_, OnchainValidatorTimeseries>(&sql)
            .bind(start)
            .bind(end);
        if let Some(vals) = validators {
            query = query.bind(vals);
        }
        query.fetch_all(self.pool()).await
    }

    /// Raw per-block on-chain stats for a single validator.
    pub async fn onchain_validator_detail(
        &self,
        validator_idx: i16,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<OnchainValidatorDetail>, sqlx::Error> {
        sqlx::query_as::<_, OnchainValidatorDetail>(
            r#"
            SELECT timestamp, slot, validator_index, blocks_produced, tickets,
                   preimages, preimages_size, guarantees, assurances
            FROM onchain_validator_stats
            WHERE validator_index = $1
              AND timestamp >= $2 AND timestamp < $3
              AND on_best_chain = true
            ORDER BY timestamp DESC
            LIMIT 1000
            "#,
        )
        .bind(validator_idx)
        .bind(start)
        .bind(end)
        .fetch_all(self.pool())
        .await
    }
}
