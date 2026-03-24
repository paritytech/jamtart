//! Typed response structs for the on-chain statistics API.
//!
//! Each struct documents its **data source pipeline** — how the data is collected,
//! aggregated, or enriched before being served. All structs derive `Serialize` for
//! JSON responses, `ToSchema` for OpenAPI documentation, and `FromRow` where the
//! SQL result maps directly to the struct fields.
//!
//! Fields map 1:1 to the Gray Paper's activity records:
//! - `CoreActivityRecord` → `OnchainCoreSummary`, `OnchainCoreTimeseries`, `OnchainCoreDetail`
//! - `ServiceActivityRecord` → `OnchainServiceSummary`, `OnchainServiceTimeseries`, `OnchainServiceDetail`
//! - `ValActivityRecord` → `OnchainValidatorSummary`, `OnchainValidatorTimeseries`, `OnchainValidatorDetail`

use chrono::{DateTime, Utc};
use serde::Serialize;
use utoipa::ToSchema;

use crate::grafana_types::DbServiceId;

// ── /api/grafana/onchain/cores ──────────────────────────────────────────

/// Per-core on-chain activity summary over a time range.
///
/// **Data source:** `onchain_core_stats` hypertable. Fields map 1:1 to the
/// Gray Paper's `CoreActivityRecord`. All values are SUMs across the requested
/// time range, except `popularity_avg` which is the mean.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainCoreSummary {
    /// Core index (0–340)
    pub core: i16,
    /// Total gas consumed (refinement + authorizations)
    pub gas_used: i64,
    /// Total bytes placed into DA (work-bundle + segments)
    pub da_load: i64,
    /// Average number of validators forming supermajority for assurance
    pub popularity_avg: i16,
    /// Total segments imported from DA
    pub imports: i64,
    /// Total number of extrinsics used
    pub extrinsic_count: i64,
    /// Total extrinsic bytes
    pub extrinsic_size: i64,
    /// Total segments exported to DA
    pub exports: i64,
    /// Total work-bundle size (Audits DA)
    pub bundle_size: i64,
}

/// Time-bucketed per-core on-chain stats.
///
/// **Data source:** `onchain_core_stats` with `time_bucket()` aggregation.
/// One row per (bucket, core) pair.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainCoreTimeseries {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Core index
    pub core: i16,
    /// Gas consumed in this bucket
    pub gas_used: i64,
    /// Bytes placed into DA in this bucket
    pub da_load: i64,
    /// Average popularity in this bucket
    pub popularity: i16,
    /// Segments imported in this bucket
    pub imports: i64,
    /// Extrinsics used in this bucket
    pub extrinsic_count: i64,
    /// Extrinsic bytes in this bucket
    pub extrinsic_size: i64,
    /// Segments exported in this bucket
    pub exports: i64,
    /// Work-bundle bytes in this bucket
    pub bundle_size: i64,
}

/// Network-wide aggregate time-bucketed core stats (no per-core breakdown).
///
/// **Data source:** `onchain_core_stats` with `time_bucket()` aggregation.
/// One row per time bucket — all cores SUMmed together (AVG for popularity).
/// Returned when no `core` filter is specified.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainCoreTimeseriesAgg {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Total gas consumed across all cores
    pub gas_used: i64,
    /// Total bytes placed into DA across all cores
    pub da_load: i64,
    /// Average popularity across all cores
    pub popularity: i16,
    /// Total segments imported across all cores
    pub imports: i64,
    /// Total extrinsics across all cores
    pub extrinsic_count: i64,
    /// Total extrinsic bytes across all cores
    pub extrinsic_size: i64,
    /// Total segments exported across all cores
    pub exports: i64,
    /// Total work-bundle bytes across all cores
    pub bundle_size: i64,
}

/// Raw per-block on-chain stats for a single core.
///
/// **Data source:** `onchain_core_stats` filtered by core, no aggregation.
/// Returns up to 1000 most recent rows.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainCoreDetail {
    /// Block timestamp
    pub timestamp: DateTime<Utc>,
    /// Slot number
    pub slot: i32,
    /// Core index
    pub core: i16,
    /// Gas consumed
    pub gas_used: i64,
    /// Bytes placed into DA
    pub da_load: i32,
    /// Validators forming supermajority for assurance
    pub popularity: i16,
    /// Segments imported from DA
    pub imports: i16,
    /// Number of extrinsics used
    pub extrinsic_count: i16,
    /// Total extrinsic bytes
    pub extrinsic_size: i32,
    /// Segments exported to DA
    pub exports: i16,
    /// Work-bundle size
    pub bundle_size: i32,
}

// ── /api/grafana/onchain/services ───────────────────────────────────────

/// Per-service on-chain activity summary over a time range.
///
/// **Data source:** `onchain_service_stats` hypertable. Fields map 1:1 to the
/// Gray Paper's `ServiceActivityRecord`. All values are SUMs.
#[derive(Debug, Serialize, ToSchema)]
pub struct OnchainServiceSummary {
    /// Service ID (hex-formatted, e.g. "0x0000000a")
    pub service_id: DbServiceId,
    /// Number of preimages provided to this service
    pub provided_count: i64,
    /// Total preimage bytes provided
    pub provided_size: i64,
    /// Work-items refined
    pub refinement_count: i64,
    /// Gas used for refinement
    pub refinement_gas: i64,
    /// Segments imported from DL
    pub imports: i64,
    /// Number of extrinsics used
    pub extrinsic_count: i64,
    /// Total extrinsic bytes
    pub extrinsic_size: i64,
    /// Segments exported to DL
    pub exports: i64,
    /// Work-items accumulated
    pub accumulate_count: i64,
    /// Gas used for accumulation
    pub accumulate_gas: i64,
}

/// Time-bucketed per-service on-chain stats.
///
/// **Data source:** `onchain_service_stats` with `time_bucket()` aggregation.
/// One row per (bucket, service_id) pair.
#[derive(Debug, Serialize, ToSchema)]
pub struct OnchainServiceTimeseries {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Service ID (hex-formatted, e.g. "0x0000000a")
    pub service_id: DbServiceId,
    /// Preimages provided in this bucket
    pub provided_count: i64,
    /// Preimage bytes provided in this bucket
    pub provided_size: i64,
    /// Work-items refined in this bucket
    pub refinement_count: i64,
    /// Refinement gas in this bucket
    pub refinement_gas: i64,
    /// Segments imported in this bucket
    pub imports: i64,
    /// Extrinsics used in this bucket
    pub extrinsic_count: i64,
    /// Extrinsic bytes in this bucket
    pub extrinsic_size: i64,
    /// Segments exported in this bucket
    pub exports: i64,
    /// Work-items accumulated in this bucket
    pub accumulate_count: i64,
    /// Accumulation gas in this bucket
    pub accumulate_gas: i64,
}

/// Raw per-block on-chain stats for a single service.
///
/// **Data source:** `onchain_service_stats` filtered by service_id, no aggregation.
/// Returns up to 1000 most recent rows.
#[derive(Debug, Serialize, ToSchema)]
pub struct OnchainServiceDetail {
    /// Block timestamp
    pub timestamp: DateTime<Utc>,
    /// Slot number
    pub slot: i32,
    /// Service ID (hex-formatted, e.g. "0x0000000a")
    pub service_id: DbServiceId,
    /// Preimages provided
    pub provided_count: i16,
    /// Preimage bytes provided
    pub provided_size: i32,
    /// Work-items refined
    pub refinement_count: i32,
    /// Gas used for refinement
    pub refinement_gas: i64,
    /// Segments imported from DL
    pub imports: i32,
    /// Number of extrinsics used
    pub extrinsic_count: i32,
    /// Total extrinsic bytes
    pub extrinsic_size: i32,
    /// Segments exported to DL
    pub exports: i32,
    /// Work-items accumulated
    pub accumulate_count: i32,
    /// Gas used for accumulation
    pub accumulate_gas: i64,
}

// ── /api/grafana/onchain/validators ─────────────────────────────────────

/// Per-validator on-chain stats summary over a time range.
///
/// **Data source:** `onchain_validator_stats` hypertable. Fields map 1:1 to the
/// Gray Paper's `ValActivityRecord`. Since validator stats are epoch-cumulative
/// (not reset per block), all aggregations use MAX to get the peak value in the
/// requested range.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainValidatorSummary {
    /// Validator index (0–1023)
    pub validator_index: i16,
    /// Blocks produced by this validator (epoch-cumulative MAX)
    pub blocks_produced: i32,
    /// Tickets introduced (epoch-cumulative MAX)
    pub tickets: i32,
    /// Preimages introduced (epoch-cumulative MAX)
    pub preimages: i32,
    /// Total preimage bytes introduced (epoch-cumulative MAX)
    pub preimages_size: i32,
    /// Work reports guaranteed (epoch-cumulative MAX)
    pub guarantees: i32,
    /// Availability assurances made (epoch-cumulative MAX)
    pub assurances: i32,
}

/// Time-bucketed per-validator on-chain stats.
///
/// **Data source:** `onchain_validator_stats` with `time_bucket()` aggregation.
/// One row per (bucket, validator_index) pair. MAX aggregation (epoch-cumulative).
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainValidatorTimeseries {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Validator index
    pub validator_index: i16,
    /// Blocks produced (MAX in bucket)
    pub blocks_produced: i32,
    /// Tickets introduced (MAX in bucket)
    pub tickets: i32,
    /// Preimages introduced (MAX in bucket)
    pub preimages: i32,
    /// Preimage bytes (MAX in bucket)
    pub preimages_size: i32,
    /// Work reports guaranteed (MAX in bucket)
    pub guarantees: i32,
    /// Assurances made (MAX in bucket)
    pub assurances: i32,
}

/// Network-wide aggregate time-bucketed validator stats (no per-validator breakdown).
///
/// **Data source:** `onchain_validator_stats` with `time_bucket()` aggregation.
/// One row per time bucket — all validators SUMmed together.
/// Returned when no `validator` filter is specified.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainValidatorTimeseriesAgg {
    /// Bucket start timestamp
    pub ts: DateTime<Utc>,
    /// Total blocks produced across all validators (SUM of per-validator MAX)
    pub blocks_produced: i64,
    /// Total tickets across all validators
    pub tickets: i64,
    /// Total preimages across all validators
    pub preimages: i64,
    /// Total preimage bytes across all validators
    pub preimages_size: i64,
    /// Total guarantees across all validators
    pub guarantees: i64,
    /// Total assurances across all validators
    pub assurances: i64,
}

/// Raw per-block on-chain stats for a single validator.
///
/// **Data source:** `onchain_validator_stats` filtered by validator_index, no aggregation.
/// Shows epoch-cumulative values growing block by block. Returns up to 1000 most recent rows.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainValidatorDetail {
    /// Block timestamp
    pub timestamp: DateTime<Utc>,
    /// Slot number
    pub slot: i32,
    /// Validator index
    pub validator_index: i16,
    /// Blocks produced (epoch-cumulative)
    pub blocks_produced: i32,
    /// Tickets introduced (epoch-cumulative)
    pub tickets: i32,
    /// Preimages introduced (epoch-cumulative)
    pub preimages: i32,
    /// Preimage bytes introduced (epoch-cumulative)
    pub preimages_size: i32,
    /// Work reports guaranteed (epoch-cumulative)
    pub guarantees: i32,
    /// Availability assurances made (epoch-cumulative)
    pub assurances: i32,
}
