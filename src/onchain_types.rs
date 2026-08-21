//! Typed response structs for the on-chain statistics API.
//!
//! These carry the JAM chain's own activity statistics, as the chain maintains
//! them in its state: per-core and per-service records covering a single block,
//! and per-validator records accumulating over an epoch.
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

/// One core's on-chain activity, totalled over a time range.
///
/// The chain records these figures for every core in every block; each field
/// here adds up the blocks of the requested range, except `popularity_avg`,
/// which is a mean over those blocks.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainCoreSummary {
    /// Core index
    pub core: i16,
    /// Gas consumed by the work reported on this core, refinement and
    /// authorization together
    pub gas_used: i64,
    /// Bytes the core made available: work bundles plus exported segments
    pub da_load: i64,
    /// Mean number of validators assuring this core's reports per block
    pub popularity_avg: i16,
    /// Segments the core's work items imported from data availability
    pub imports: i64,
    /// Extrinsics referenced by the core's work items
    pub extrinsic_count: i64,
    /// Total bytes of those extrinsics
    pub extrinsic_size: i64,
    /// Segments the core's work items exported into data availability
    pub exports: i64,
    /// Work-bundle bytes reported on this core
    pub bundle_size: i64,
}

/// One core's on-chain activity within one time bucket.
///
/// Returned by `/onchain/cores/timeseries` when a `core` filter is given: one
/// row per bucket for that core.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainCoreTimeseries {
    /// Start of the time bucket
    pub ts: DateTime<Utc>,
    /// Core index
    pub core: i16,
    /// Gas consumed by the work reported on this core during the bucket
    pub gas_used: i64,
    /// Bytes the core made available during the bucket
    pub da_load: i64,
    /// Mean number of validators assuring this core's reports during the bucket
    pub popularity: i16,
    /// Segments imported from data availability during the bucket
    pub imports: i64,
    /// Extrinsics referenced by the core's work items during the bucket
    pub extrinsic_count: i64,
    /// Total bytes of those extrinsics
    pub extrinsic_size: i64,
    /// Segments exported into data availability during the bucket
    pub exports: i64,
    /// Work-bundle bytes reported on this core during the bucket
    pub bundle_size: i64,
}

/// All cores' on-chain activity combined, within one time bucket.
///
/// Returned by `/onchain/cores/timeseries` when no `core` filter is given: one
/// row per bucket with every core's figures added together, popularity averaged.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainCoreTimeseriesAgg {
    /// Start of the time bucket
    pub ts: DateTime<Utc>,
    /// Gas consumed by reported work on all cores during the bucket
    pub gas_used: i64,
    /// Bytes made available by all cores during the bucket
    pub da_load: i64,
    /// Mean number of validators assuring a core's reports, across all cores
    pub popularity: i16,
    /// Segments imported from data availability by all cores
    pub imports: i64,
    /// Extrinsics referenced by work items on all cores
    pub extrinsic_count: i64,
    /// Total bytes of those extrinsics
    pub extrinsic_size: i64,
    /// Segments exported into data availability by all cores
    pub exports: i64,
    /// Work-bundle bytes reported on all cores
    pub bundle_size: i64,
}

/// One core's on-chain activity in a single block.
///
/// The chain resets these figures every block, so each instance describes what
/// the core did in that one block alone.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainCoreDetail {
    /// Wall-clock time of the block's slot
    pub timestamp: DateTime<Utc>,
    /// Slot the block was authored in
    pub slot: i32,
    /// Core index
    pub core: i16,
    /// Gas consumed by the work reported on this core in the block
    pub gas_used: i64,
    /// Bytes the core made available in the block: work bundles plus exported
    /// segments
    pub da_load: i32,
    /// Validators that assured this core's reports in the block
    pub popularity: i16,
    /// Segments imported from data availability
    pub imports: i16,
    /// Extrinsics referenced by the core's work items
    pub extrinsic_count: i16,
    /// Total bytes of those extrinsics
    pub extrinsic_size: i32,
    /// Segments exported into data availability
    pub exports: i16,
    /// Work-bundle bytes reported on this core
    pub bundle_size: i32,
}

// ── /api/grafana/onchain/services ───────────────────────────────────────

/// One service's on-chain activity, totalled over a time range.
///
/// The chain records these figures in every block in which the service was
/// active; each field adds up the blocks of the requested range.
#[derive(Debug, Serialize, ToSchema)]
pub struct OnchainServiceSummary {
    /// Service ID, zero-padded hex (e.g. "0x0000000a")
    pub service_id: DbServiceId,
    /// Preimages provided to this service
    pub provided_count: i64,
    /// Total bytes of those preimages
    pub provided_size: i64,
    /// Work items refined for this service
    pub refinement_count: i64,
    /// Gas the service's code used refining them
    pub refinement_gas: i64,
    /// Segments the service's work items imported from data availability
    pub imports: i64,
    /// Extrinsics referenced by the service's work items
    pub extrinsic_count: i64,
    /// Total bytes of those extrinsics
    pub extrinsic_size: i64,
    /// Segments the service's work items exported into data availability
    pub exports: i64,
    /// Work items accumulated for this service
    pub accumulate_count: i64,
    /// Gas the service's code used accumulating them
    pub accumulate_gas: i64,
}

/// One service's on-chain activity within one time bucket.
///
/// One instance per bucket and service, covering the blocks that fall in the
/// bucket.
#[derive(Debug, Serialize, ToSchema)]
pub struct OnchainServiceTimeseries {
    /// Start of the time bucket
    pub ts: DateTime<Utc>,
    /// Service ID, zero-padded hex (e.g. "0x0000000a")
    pub service_id: DbServiceId,
    /// Preimages provided to this service during the bucket
    pub provided_count: i64,
    /// Total bytes of those preimages
    pub provided_size: i64,
    /// Work items refined for this service during the bucket
    pub refinement_count: i64,
    /// Gas the service's code used refining them
    pub refinement_gas: i64,
    /// Segments imported from data availability during the bucket
    pub imports: i64,
    /// Extrinsics referenced by the service's work items during the bucket
    pub extrinsic_count: i64,
    /// Total bytes of those extrinsics
    pub extrinsic_size: i64,
    /// Segments exported into data availability during the bucket
    pub exports: i64,
    /// Work items accumulated for this service during the bucket
    pub accumulate_count: i64,
    /// Gas the service's code used accumulating them
    pub accumulate_gas: i64,
}

/// One service's on-chain activity in a single block.
///
/// The chain resets these figures every block, so each instance describes the
/// service's activity in that one block alone.
#[derive(Debug, Serialize, ToSchema)]
pub struct OnchainServiceDetail {
    /// Wall-clock time of the block's slot
    pub timestamp: DateTime<Utc>,
    /// Slot the block was authored in
    pub slot: i32,
    /// Service ID, zero-padded hex (e.g. "0x0000000a")
    pub service_id: DbServiceId,
    /// Preimages provided to this service in the block
    pub provided_count: i16,
    /// Total bytes of those preimages
    pub provided_size: i32,
    /// Work items refined for this service in the block
    pub refinement_count: i32,
    /// Gas the service's code used refining them
    pub refinement_gas: i64,
    /// Segments imported from data availability
    pub imports: i32,
    /// Extrinsics referenced by the service's work items
    pub extrinsic_count: i32,
    /// Total bytes of those extrinsics
    pub extrinsic_size: i32,
    /// Segments exported into data availability
    pub exports: i32,
    /// Work items accumulated for this service in the block
    pub accumulate_count: i32,
    /// Gas the service's code used accumulating them
    pub accumulate_gas: i64,
}

// ── /api/grafana/onchain/validators ─────────────────────────────────────

/// One validator's epoch activity tallies, at their peak within a time range.
///
/// The chain accumulates these tallies over an epoch and resets them when the
/// next epoch starts, so each field is the highest value the tally reached
/// inside the requested range.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainValidatorSummary {
    /// Validator index
    pub validator_index: i16,
    /// Blocks the validator authored in the epoch
    pub blocks_produced: i32,
    /// Tickets the validator introduced in the epoch
    pub tickets: i32,
    /// Preimages the validator introduced in the epoch
    pub preimages: i32,
    /// Total bytes across those preimages
    pub preimages_size: i32,
    /// Work reports the validator guaranteed in the epoch
    pub guarantees: i32,
    /// Availability assurances the validator made in the epoch
    pub assurances: i32,
}

/// One validator's epoch tallies, at their peak within one time bucket.
///
/// Returned by `/onchain/validators/timeseries` when a `validator` filter is
/// given. Because the tallies accumulate over an epoch, each series steps upward
/// through the epoch and falls back to zero at the epoch boundary.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainValidatorTimeseries {
    /// Start of the time bucket
    pub ts: DateTime<Utc>,
    /// Validator index
    pub validator_index: i16,
    /// Blocks authored in the epoch, highest tally within the bucket
    pub blocks_produced: i32,
    /// Tickets introduced in the epoch, highest tally within the bucket
    pub tickets: i32,
    /// Preimages introduced in the epoch, highest tally within the bucket
    pub preimages: i32,
    /// Total preimage bytes in the epoch, highest tally within the bucket
    pub preimages_size: i32,
    /// Work reports guaranteed in the epoch, highest tally within the bucket
    pub guarantees: i32,
    /// Availability assurances made in the epoch, highest tally within the bucket
    pub assurances: i32,
}

/// All validators' epoch tallies added together, within one time bucket.
///
/// Returned by `/onchain/validators/timeseries` when no `validator` filter is
/// given. Every validator's tallies are added up across every block of the
/// bucket; since the tallies are epoch-cumulative, these sums indicate the
/// relative level of validator participation rather than counting events that
/// happened inside the bucket.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainValidatorTimeseriesAgg {
    /// Start of the time bucket
    pub ts: DateTime<Utc>,
    /// Block-authoring tallies of all validators, summed over the bucket
    pub blocks_produced: i64,
    /// Ticket tallies of all validators, summed over the bucket
    pub tickets: i64,
    /// Preimage tallies of all validators, summed over the bucket
    pub preimages: i64,
    /// Preimage-byte tallies of all validators, summed over the bucket
    pub preimages_size: i64,
    /// Guarantee tallies of all validators, summed over the bucket
    pub guarantees: i64,
    /// Assurance tallies of all validators, summed over the bucket
    pub assurances: i64,
}

/// One validator's epoch tallies as of a single block.
///
/// The values climb block by block through the epoch and restart at zero once
/// the next epoch begins.
#[derive(Debug, Serialize, sqlx::FromRow, ToSchema)]
pub struct OnchainValidatorDetail {
    /// Wall-clock time of the block's slot
    pub timestamp: DateTime<Utc>,
    /// Slot the block was authored in
    pub slot: i32,
    /// Validator index
    pub validator_index: i16,
    /// Blocks the validator had authored in the epoch as of this block
    pub blocks_produced: i32,
    /// Tickets the validator had introduced in the epoch as of this block
    pub tickets: i32,
    /// Preimages the validator had introduced in the epoch as of this block
    pub preimages: i32,
    /// Total bytes across those preimages
    pub preimages_size: i32,
    /// Work reports the validator had guaranteed in the epoch as of this block
    pub guarantees: i32,
    /// Availability assurances the validator had made in the epoch as of this
    /// block
    pub assurances: i32,
}
