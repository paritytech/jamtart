//! Convergence trackers for guarantee and assurance propagation across the validator network.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use dashmap::DashMap;
use sqlx::PgPool;
use tracing::{debug, warn};

use crate::types::JCE_EPOCH_UNIX_MICROS;

pub type GuaranteeConvergenceTracker = Arc<DashMap<[u8; 32], GuaranteeConvergenceState>>;

pub fn new_guarantee_convergence_tracker() -> GuaranteeConvergenceTracker {
    Arc::new(DashMap::new())
}

pub struct GuaranteeConvergenceState {
    pub built_at: Option<u64>,
    pub slot: Option<u32>,
    pub core: Option<u16>,
    pub wp_hash: Option<[u8; 32]>,
    pub builder_node_id: Option<Arc<str>>,
    pub received_timestamps: Vec<u64>,
    pub last_event: Instant,
    pub flushed: bool,
    pub dirty: bool,
}

pub struct GuaranteeConvergenceRow {
    pub work_report_hash: [u8; 32],
    pub slot: i32,
    pub core: Option<i16>,
    pub wp_hash: Option<Vec<u8>>,
    pub builder_node_id: Option<Arc<str>>,
    pub node_count: i16,
    pub p50_ms: i32,
    pub p75_ms: Option<i32>,
    pub p95_ms: Option<i32>,
    pub p99_ms: i32,
    pub p100_ms: i32,
    pub built_at: u64,
    pub histogram: [u32; crate::histogram::CONVERGENCE_BUCKET_COUNT],
    pub hist_total: u32,
}

pub struct GuaranteeConvergenceSlotRow {
    pub slot: i32,
    pub slot_timestamp: Option<DateTime<Utc>>,
    pub guarantee_count: i16,
    pub node_count: i16,
    pub p50_ms: Option<i32>,
    pub p75_ms: Option<i32>,
    pub p95_ms: Option<i32>,
    pub p99_ms: Option<i32>,
    pub p100_ms: Option<i32>,
    pub built_at: u64,
}

pub struct ConvergencePercentiles {
    pub p50_ms: i32,
    pub p75_ms: i32,
    pub p95_ms: i32,
    pub p99_ms: i32,
    pub p100_ms: i32,
}

/// Compute percentiles from a set of timestamps relative to an anchor.
/// Returns None if timestamps is empty.
pub fn compute_percentiles(anchor_ts: u64, timestamps: &[u64]) -> Option<ConvergencePercentiles> {
    if timestamps.is_empty() {
        return None;
    }
    let mut offsets_ms: Vec<i64> = timestamps
        .iter()
        .map(|&t| (t as i64 - anchor_ts as i64) / 1000)
        .collect();
    offsets_ms.sort();
    let len = offsets_ms.len();
    let p50 = offsets_ms[len / 2];
    let p75_idx = ((len as f64 * 0.75) as usize).min(len - 1);
    let p95_idx = ((len as f64 * 0.95) as usize).min(len - 1);
    let p99_idx = ((len as f64 * 0.99) as usize).min(len - 1);
    Some(ConvergencePercentiles {
        p50_ms: p50 as i32,
        p75_ms: offsets_ms[p75_idx] as i32,
        p95_ms: offsets_ms[p95_idx] as i32,
        p99_ms: offsets_ms[p99_idx] as i32,
        p100_ms: offsets_ms[len - 1] as i32,
    })
}

/// Compute offsets in ms from an anchor timestamp, and bucket them into a convergence histogram.
fn offsets_to_histogram(anchor_ts: u64, timestamps: &[u64]) -> ([u32; crate::histogram::CONVERGENCE_BUCKET_COUNT], u32) {
    let deltas: Vec<i32> = timestamps
        .iter()
        .map(|&t| ((t as i64 - anchor_ts as i64) / 1000) as i32)
        .collect();
    crate::histogram::bucket_deltas_convergence(&deltas)
}

fn ts_to_datetime(jce_micros: u64) -> DateTime<Utc> {
    let unix_us = JCE_EPOCH_UNIX_MICROS + jce_micros as i64;
    let secs = unix_us / 1_000_000;
    let nsecs = ((unix_us % 1_000_000) * 1_000) as u32;
    Utc.timestamp_opt(secs, nsecs).unwrap()
}

pub async fn flush_guarantee_convergence(
    tracker: &GuaranteeConvergenceTracker,
    pool: &PgPool,
    age_insert: Duration,
    age_evict: Duration,
) {
    let now = Instant::now();

    // Phase 1: Collect dirty entries and eviction candidates
    let mut to_insert: Vec<GuaranteeConvergenceRow> = Vec::new();
    let mut to_upsert: Vec<GuaranteeConvergenceRow> = Vec::new();
    let mut to_evict: Vec<[u8; 32]> = Vec::new();
    let mut dirty_slots: Vec<i32> = Vec::new();

    for entry in tracker.iter() {
        let work_report_hash = *entry.key();
        let state = entry.value();
        let age = now.duration_since(state.last_event);

        let built_at = match state.built_at {
            Some(ts) => ts,
            None => {
                if age >= age_evict {
                    to_evict.push(work_report_hash);
                }
                continue;
            }
        };

        let slot = state.slot.unwrap_or(0) as i32;

        if age >= age_evict {
            if state.dirty {
                if let Some(p) = compute_percentiles(built_at, &state.received_timestamps) {
                    let (histogram, hist_total) = offsets_to_histogram(built_at, &state.received_timestamps);
                    to_upsert.push(GuaranteeConvergenceRow {
                        work_report_hash,
                        slot,
                        core: state.core.map(|c| c as i16),
                        wp_hash: state.wp_hash.map(|h| h.to_vec()),
                        builder_node_id: state.builder_node_id.clone(),
                        node_count: state.received_timestamps.len() as i16,
                        p50_ms: p.p50_ms,
                        p75_ms: Some(p.p75_ms),
                        p95_ms: Some(p.p95_ms),
                        p99_ms: p.p99_ms,
                        p100_ms: p.p100_ms,
                        built_at,
                        histogram,
                        hist_total,
                    });
                    dirty_slots.push(slot);
                }
            }
            to_evict.push(work_report_hash);
        } else if !state.flushed && age >= age_insert {
            if let Some(p) = compute_percentiles(built_at, &state.received_timestamps) {
                let (histogram, hist_total) = offsets_to_histogram(built_at, &state.received_timestamps);
                to_insert.push(GuaranteeConvergenceRow {
                    work_report_hash,
                    slot,
                    core: state.core.map(|c| c as i16),
                    wp_hash: state.wp_hash.map(|h| h.to_vec()),
                    builder_node_id: state.builder_node_id.clone(),
                    node_count: state.received_timestamps.len() as i16,
                    p50_ms: p.p50_ms,
                    p75_ms: Some(p.p75_ms),
                    p95_ms: Some(p.p95_ms),
                    p99_ms: p.p99_ms,
                    p100_ms: p.p100_ms,
                    built_at,
                    histogram,
                    hist_total,
                });
                dirty_slots.push(slot);
            }
        } else if state.flushed && state.dirty {
            if let Some(p) = compute_percentiles(built_at, &state.received_timestamps) {
                let (histogram, hist_total) = offsets_to_histogram(built_at, &state.received_timestamps);
                to_upsert.push(GuaranteeConvergenceRow {
                    work_report_hash,
                    slot,
                    core: state.core.map(|c| c as i16),
                    wp_hash: state.wp_hash.map(|h| h.to_vec()),
                    builder_node_id: state.builder_node_id.clone(),
                    node_count: state.received_timestamps.len() as i16,
                    p50_ms: p.p50_ms,
                    p75_ms: Some(p.p75_ms),
                    p95_ms: Some(p.p95_ms),
                    p99_ms: p.p99_ms,
                    p100_ms: p.p100_ms,
                    built_at,
                    histogram,
                    hist_total,
                });
                dirty_slots.push(slot);
            }
        }
    }

    // Collect unique dirty slots for per-slot summary
    dirty_slots.sort();
    dirty_slots.dedup();

    // Build per-slot summaries scanning ALL in-memory entries for those slots.
    // Each guarantee's deltas are computed against its OWN built_at, then all
    // deltas are flattened into one vector for the slot-wide percentile computation.
    // This avoids inflating deltas when guarantees in the same slot are built seconds apart.
    let mut slot_summaries: Vec<GuaranteeConvergenceSlotRow> = Vec::new();
    for &slot in &dirty_slots {
        let mut all_deltas_ms: Vec<i32> = Vec::new();
        let mut min_built_at: Option<u64> = None;
        let mut guarantee_count: i16 = 0;
        let mut min_node_count: Option<i16> = None;

        for entry in tracker.iter() {
            let state = entry.value();
            let entry_slot = state.slot.unwrap_or(0) as i32;
            if entry_slot != slot {
                continue;
            }
            let built_at = match state.built_at {
                Some(ts) => ts,
                None => continue,
            };
            if state.received_timestamps.is_empty() {
                continue;
            }

            guarantee_count += 1;
            let nc = state.received_timestamps.len() as i16;
            min_node_count = Some(min_node_count.map_or(nc, |prev: i16| prev.min(nc)));
            min_built_at = Some(min_built_at.map_or(built_at, |prev| prev.min(built_at)));

            // Compute deltas against THIS guarantee's built_at (not min across slot)
            for &t in &state.received_timestamps {
                let delta = (t as i64 - built_at as i64) / 1000;
                all_deltas_ms.push(delta.max(0) as i32);
            }
        }

        if guarantee_count == 0 || all_deltas_ms.is_empty() {
            continue;
        }

        let anchor = min_built_at.unwrap();
        let percentiles = compute_percentiles_from_deltas(&all_deltas_ms);

        let slot_ts = crate::onchain_stats::slot_to_timestamp(slot as u32, 6);
        slot_summaries.push(GuaranteeConvergenceSlotRow {
            slot,
            slot_timestamp: Some(slot_ts),
            guarantee_count,
            node_count: min_node_count.unwrap_or(0),
            p50_ms: percentiles.as_ref().map(|p| p.p50_ms),
            p75_ms: percentiles.as_ref().map(|p| p.p75_ms),
            p95_ms: percentiles.as_ref().map(|p| p.p95_ms),
            p99_ms: percentiles.as_ref().map(|p| p.p99_ms),
            p100_ms: percentiles.as_ref().map(|p| p.p100_ms),
            built_at: anchor,
        });
    }

    // Phase 2: DB writes — per-guarantee rows (INSERT and UPSERT share the same SQL)
    let all_guarantee_rows: Vec<(&GuaranteeConvergenceRow, &str)> = to_insert
        .iter().map(|r| (r, "INSERT"))
        .chain(to_upsert.iter().map(|r| (r, "UPSERT")))
        .collect();

    for (row, label) in &all_guarantee_rows {
        let built_dt = ts_to_datetime(row.built_at);
        let h = &row.histogram;
        let result = sqlx::query(
            r#"INSERT INTO guarantee_convergence (work_report_hash, slot, core, wp_hash, builder_node_id,
                node_count, p50_ms, p75_ms, p95_ms, p99_ms, p100_ms, built_at,
                h_0_2, h_2_5, h_5_10, h_10_15, h_15_20, h_20_30, h_30_50, h_50_75, h_75_100,
                h_100_150, h_150_250, h_250_500, h_500_1000, h_1000_2000, h_2000_5000,
                h_5000_10000, h_10000_15000, h_15000_20000, h_20000_25000, h_25000_30000,
                h_30000_60000, h_60000_120000, h_120000_plus, hist_total)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24,
                $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36)
               ON CONFLICT (work_report_hash) DO UPDATE SET
                   builder_node_id = COALESCE(guarantee_convergence.builder_node_id, EXCLUDED.builder_node_id),
                   node_count = GREATEST(guarantee_convergence.node_count, EXCLUDED.node_count),
                   p50_ms = EXCLUDED.p50_ms, p75_ms = EXCLUDED.p75_ms,
                   p95_ms = EXCLUDED.p95_ms, p99_ms = EXCLUDED.p99_ms,
                   p100_ms = GREATEST(guarantee_convergence.p100_ms, EXCLUDED.p100_ms),
                   built_at = COALESCE(guarantee_convergence.built_at, EXCLUDED.built_at),
                   h_0_2 = EXCLUDED.h_0_2, h_2_5 = EXCLUDED.h_2_5, h_5_10 = EXCLUDED.h_5_10,
                   h_10_15 = EXCLUDED.h_10_15, h_15_20 = EXCLUDED.h_15_20, h_20_30 = EXCLUDED.h_20_30,
                   h_30_50 = EXCLUDED.h_30_50, h_50_75 = EXCLUDED.h_50_75, h_75_100 = EXCLUDED.h_75_100,
                   h_100_150 = EXCLUDED.h_100_150, h_150_250 = EXCLUDED.h_150_250, h_250_500 = EXCLUDED.h_250_500,
                   h_500_1000 = EXCLUDED.h_500_1000, h_1000_2000 = EXCLUDED.h_1000_2000, h_2000_5000 = EXCLUDED.h_2000_5000,
                   h_5000_10000 = EXCLUDED.h_5000_10000, h_10000_15000 = EXCLUDED.h_10000_15000, h_15000_20000 = EXCLUDED.h_15000_20000,
                   h_20000_25000 = EXCLUDED.h_20000_25000, h_25000_30000 = EXCLUDED.h_25000_30000,
                   h_30000_60000 = EXCLUDED.h_30000_60000, h_60000_120000 = EXCLUDED.h_60000_120000,
                   h_120000_plus = EXCLUDED.h_120000_plus, hist_total = EXCLUDED.hist_total"#,
        )
        .bind(&row.work_report_hash[..])
        .bind(row.slot)
        .bind(row.core)
        .bind(&row.wp_hash)
        .bind(row.builder_node_id.as_deref())
        .bind(row.node_count)
        .bind(row.p50_ms)
        .bind(row.p75_ms)
        .bind(row.p95_ms)
        .bind(row.p99_ms)
        .bind(row.p100_ms)
        .bind(built_dt)
        .bind(h[0] as i32).bind(h[1] as i32).bind(h[2] as i32).bind(h[3] as i32)
        .bind(h[4] as i32).bind(h[5] as i32).bind(h[6] as i32).bind(h[7] as i32)
        .bind(h[8] as i32).bind(h[9] as i32).bind(h[10] as i32).bind(h[11] as i32)
        .bind(h[12] as i32).bind(h[13] as i32).bind(h[14] as i32).bind(h[15] as i32)
        .bind(h[16] as i32).bind(h[17] as i32).bind(h[18] as i32).bind(h[19] as i32)
        .bind(h[20] as i32).bind(h[21] as i32).bind(h[22] as i32)
        .bind(row.hist_total as i32)
        .execute(pool)
        .await;

        if let Err(e) = result {
            warn!(slot = row.slot, "guarantee_convergence {label} failed: {e}");
        }
    }

    // Per-slot summary rows
    for row in &slot_summaries {
        let built_dt = ts_to_datetime(row.built_at);
        let result = sqlx::query(
            r#"INSERT INTO guarantee_convergence_slots (slot, slot_timestamp, guarantee_count, node_count, p50_ms, p75_ms, p95_ms, p99_ms, p100_ms, built_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
               ON CONFLICT (slot) DO UPDATE SET
                   slot_timestamp = COALESCE(guarantee_convergence_slots.slot_timestamp, EXCLUDED.slot_timestamp),
                   guarantee_count = EXCLUDED.guarantee_count,
                   node_count = EXCLUDED.node_count,
                   p50_ms = EXCLUDED.p50_ms,
                   p75_ms = EXCLUDED.p75_ms,
                   p95_ms = EXCLUDED.p95_ms,
                   p99_ms = EXCLUDED.p99_ms,
                   p100_ms = EXCLUDED.p100_ms,
                   built_at = EXCLUDED.built_at"#,
        )
        .bind(row.slot)
        .bind(row.slot_timestamp)
        .bind(row.guarantee_count)
        .bind(row.node_count)
        .bind(row.p50_ms)
        .bind(row.p75_ms)
        .bind(row.p95_ms)
        .bind(row.p99_ms)
        .bind(row.p100_ms)
        .bind(built_dt)
        .execute(pool)
        .await;

        if let Err(e) = result {
            warn!(slot = row.slot, "guarantee_convergence_slots UPSERT failed: {e}");
        }
    }

    // Phase 3: Update flags and evict
    for row in &to_insert {
        if let Some(mut state) = tracker.get_mut(&row.work_report_hash) {
            state.flushed = true;
            state.dirty = false;
        }
    }

    for row in &to_upsert {
        if let Some(mut state) = tracker.get_mut(&row.work_report_hash) {
            state.dirty = false;
        }
    }

    for key in &to_evict {
        tracker.remove(key);
    }

    let total = to_insert.len() + to_upsert.len();
    if total > 0 || !to_evict.is_empty() || !slot_summaries.is_empty() {
        debug!(
            inserted = to_insert.len(),
            upserted = to_upsert.len(),
            evicted = to_evict.len(),
            slot_summaries = slot_summaries.len(),
            "guarantee_convergence flush complete"
        );
    }
}

// ---------------------------------------------------------------------------
// Header hash lookup (Importing/Authored → slot mapping)
// ---------------------------------------------------------------------------

/// Maps block header hash → slot number. Populated from Importing(43) and Authored(42) events.
pub type HeaderHashLookup = Arc<DashMap<[u8; 32], u32>>;

pub fn new_header_hash_lookup() -> HeaderHashLookup {
    Arc::new(DashMap::new())
}

/// Remove entries older than the TTL from the header hash lookup.
pub fn evict_header_hash_lookup(lookup: &HeaderHashLookup, max_entries: usize) {
    // Simple size-based eviction: if over max_entries, remove ~25%
    if lookup.len() > max_entries {
        let to_remove = lookup.len() / 4;
        let keys: Vec<[u8; 32]> = lookup.iter().take(to_remove).map(|e| *e.key()).collect();
        for key in keys {
            lookup.remove(&key);
        }
    }
}

// ---------------------------------------------------------------------------
// Assurance convergence tracker
// ---------------------------------------------------------------------------

pub type AssuranceConvergenceTracker = Arc<DashMap<[u8; 32], AnchorState>>;

pub fn new_assurance_convergence_tracker() -> AssuranceConvergenceTracker {
    Arc::new(DashMap::new())
}

pub struct AnchorState {
    pub slot: Option<u32>,
    pub senders: HashMap<Arc<str>, SenderAssuranceState>,
    /// Buffered (sender_node_id, received_ts) for AssuranceReceived events that arrived
    /// before the sender's DistributingAssurance. Drained when the sender is seen.
    pub pending_received: Vec<(Arc<str>, u64)>,
    pub last_event: Instant,
    pub flushed: bool,
    pub dirty: bool,
}

pub struct SenderAssuranceState {
    pub distributed_at: u64,
    /// (received_ts - distributed_at) deltas in ms, one per receiving node. Clamped to max(0, delta).
    pub deltas_ms: Vec<i32>,
}

/// Compute percentiles from pre-computed deltas (ms). Returns None if empty.
pub fn compute_percentiles_from_deltas(deltas: &[i32]) -> Option<ConvergencePercentiles> {
    if deltas.is_empty() {
        return None;
    }
    let mut sorted: Vec<i32> = deltas.to_vec();
    sorted.sort();
    let len = sorted.len();
    let p50 = sorted[len / 2];
    let p75_idx = ((len as f64 * 0.75) as usize).min(len - 1);
    let p95_idx = ((len as f64 * 0.95) as usize).min(len - 1);
    let p99_idx = ((len as f64 * 0.99) as usize).min(len - 1);
    Some(ConvergencePercentiles {
        p50_ms: p50,
        p75_ms: sorted[p75_idx],
        p95_ms: sorted[p95_idx],
        p99_ms: sorted[p99_idx],
        p100_ms: sorted[len - 1],
    })
}

struct AssuranceAnchorFlushRow {
    anchor: [u8; 32],
    slot: Option<i32>,
    slot_timestamp: Option<DateTime<Utc>>,
    sender_count: i16,
    receiver_count: i32,
    p50_ms: i32,
    p75_ms: i32,
    p95_ms: i32,
    p99_ms: i32,
    p100_ms: i32,
    dist_start_p50_ms: i32,
    dist_start_p95_ms: i32,
    dist_start_p99_ms: i32,
    dist_start_p100_ms: i32,
    first_distributed_at: DateTime<Utc>,
    last_distributed_at: DateTime<Utc>,
    histogram: [u32; crate::histogram::CONVERGENCE_BUCKET_COUNT],
    hist_total: u32,
}

struct AssuranceSenderFlushRow {
    distributed_at: DateTime<Utc>,
    anchor: [u8; 32],
    sender_node_id: Arc<str>,
    node_count: i16,
    p50_ms: i32,
    p75_ms: i32,
    p95_ms: i32,
    p99_ms: i32,
    p100_ms: i32,
    histogram: [u32; crate::histogram::CONVERGENCE_BUCKET_COUNT],
    hist_total: u32,
}

pub async fn flush_assurance_convergence(
    tracker: &AssuranceConvergenceTracker,
    pool: &PgPool,
    age_insert: Duration,
    age_evict: Duration,
) {
    let now = Instant::now();

    // Phase 1: Collect entries to flush/evict
    let mut anchor_rows: Vec<AssuranceAnchorFlushRow> = Vec::new();
    let mut sender_rows: Vec<AssuranceSenderFlushRow> = Vec::new();
    let mut to_mark_flushed: Vec<[u8; 32]> = Vec::new();
    let mut to_mark_clean: Vec<[u8; 32]> = Vec::new();
    let mut to_evict: Vec<[u8; 32]> = Vec::new();

    for entry in tracker.iter() {
        let anchor = *entry.key();
        let state = entry.value();
        let age = now.duration_since(state.last_event);

        // Skip if no senders (nothing to compute)
        if state.senders.is_empty() {
            if age >= age_evict {
                to_evict.push(anchor);
            }
            continue;
        }

        let should_flush = if age >= age_evict {
            // About to evict — flush if dirty
            state.dirty
        } else if !state.flushed && age >= age_insert {
            true
        } else {
            state.flushed && state.dirty
        };

        let should_evict = age >= age_evict;

        if should_flush {
            // Per-sender percentiles + collect all deltas for anchor summary
            let mut all_deltas: Vec<i32> = Vec::new();
            let mut distributed_ats: Vec<u64> = Vec::new();
            // Write per-sender rows if dirty (new data since last flush)
            let write_senders = state.dirty;

            for (sender_id, sender_state) in &state.senders {
                distributed_ats.push(sender_state.distributed_at);
                all_deltas.extend_from_slice(&sender_state.deltas_ms);

                if write_senders {
                    if let Some(sp) = compute_percentiles_from_deltas(&sender_state.deltas_ms) {
                        let (sh, st) = crate::histogram::bucket_deltas_convergence(&sender_state.deltas_ms);
                        sender_rows.push(AssuranceSenderFlushRow {
                            distributed_at: ts_to_datetime(sender_state.distributed_at),
                            anchor,
                            sender_node_id: sender_id.clone(),
                            node_count: sender_state.deltas_ms.len() as i16,
                            p50_ms: sp.p50_ms,
                            p75_ms: sp.p75_ms,
                            p95_ms: sp.p95_ms,
                            p99_ms: sp.p99_ms,
                            p100_ms: sp.p100_ms,
                            histogram: sh,
                            hist_total: st,
                        });
                    }
                }
            }

            // Anchor summary percentiles
            if let Some(ap) = compute_percentiles_from_deltas(&all_deltas) {
                // Distribution start spread: deltas relative to min distributed_at
                let min_dist = *distributed_ats.iter().min().unwrap();
                let max_dist = *distributed_ats.iter().max().unwrap();
                let dist_spread_deltas: Vec<i32> = distributed_ats
                    .iter()
                    .map(|&t| ((t as i64 - min_dist as i64) / 1000).max(0) as i32)
                    .collect();
                let dist_p = compute_percentiles_from_deltas(&dist_spread_deltas)
                    .unwrap_or(ConvergencePercentiles {
                        p50_ms: 0,
                        p75_ms: 0,
                        p95_ms: 0,
                        p99_ms: 0,
                        p100_ms: 0,
                    });

                let slot_i32 = state.slot.map(|s| s as i32);
                let slot_ts = state.slot.map(|s| {
                    crate::onchain_stats::slot_to_timestamp(s, 6)
                });

                let (ah, at) = crate::histogram::bucket_deltas_convergence(&all_deltas);
                anchor_rows.push(AssuranceAnchorFlushRow {
                    anchor,
                    slot: slot_i32,
                    slot_timestamp: slot_ts,
                    sender_count: state.senders.len() as i16,
                    receiver_count: all_deltas.len() as i32,
                    p50_ms: ap.p50_ms,
                    p75_ms: ap.p75_ms,
                    p95_ms: ap.p95_ms,
                    p99_ms: ap.p99_ms,
                    p100_ms: ap.p100_ms,
                    dist_start_p50_ms: dist_p.p50_ms,
                    dist_start_p95_ms: dist_p.p95_ms,
                    dist_start_p99_ms: dist_p.p99_ms,
                    dist_start_p100_ms: dist_p.p100_ms,
                    first_distributed_at: ts_to_datetime(min_dist),
                    last_distributed_at: ts_to_datetime(max_dist),
                    histogram: ah,
                    hist_total: at,
                });

                if !state.flushed {
                    to_mark_flushed.push(anchor);
                }
                to_mark_clean.push(anchor);
            }
        }

        if should_evict {
            to_evict.push(anchor);
        }
    }

    // Phase 2: DB writes — per-anchor summary
    for row in &anchor_rows {
        let h = &row.histogram;
        let result = sqlx::query(
            r#"INSERT INTO assurance_convergence (anchor, slot, slot_timestamp, sender_count, receiver_count,
                p50_ms, p75_ms, p95_ms, p99_ms, p100_ms,
                dist_start_p50_ms, dist_start_p95_ms, dist_start_p99_ms, dist_start_p100_ms,
                first_distributed_at, last_distributed_at,
                h_0_2, h_2_5, h_5_10, h_10_15, h_15_20, h_20_30, h_30_50, h_50_75, h_75_100,
                h_100_150, h_150_250, h_250_500, h_500_1000, h_1000_2000, h_2000_5000,
                h_5000_10000, h_10000_15000, h_15000_20000, h_20000_25000, h_25000_30000,
                h_30000_60000, h_60000_120000, h_120000_plus, hist_total)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
                $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40)
            ON CONFLICT (anchor) DO UPDATE SET
                slot = COALESCE(assurance_convergence.slot, EXCLUDED.slot),
                slot_timestamp = COALESCE(assurance_convergence.slot_timestamp, EXCLUDED.slot_timestamp),
                sender_count = EXCLUDED.sender_count,
                receiver_count = EXCLUDED.receiver_count,
                p50_ms = EXCLUDED.p50_ms, p75_ms = EXCLUDED.p75_ms, p95_ms = EXCLUDED.p95_ms,
                p99_ms = EXCLUDED.p99_ms, p100_ms = EXCLUDED.p100_ms,
                dist_start_p50_ms = EXCLUDED.dist_start_p50_ms,
                dist_start_p95_ms = EXCLUDED.dist_start_p95_ms,
                dist_start_p99_ms = EXCLUDED.dist_start_p99_ms,
                dist_start_p100_ms = EXCLUDED.dist_start_p100_ms,
                first_distributed_at = EXCLUDED.first_distributed_at,
                last_distributed_at = EXCLUDED.last_distributed_at,
                h_0_2 = EXCLUDED.h_0_2, h_2_5 = EXCLUDED.h_2_5, h_5_10 = EXCLUDED.h_5_10,
                h_10_15 = EXCLUDED.h_10_15, h_15_20 = EXCLUDED.h_15_20, h_20_30 = EXCLUDED.h_20_30,
                h_30_50 = EXCLUDED.h_30_50, h_50_75 = EXCLUDED.h_50_75, h_75_100 = EXCLUDED.h_75_100,
                h_100_150 = EXCLUDED.h_100_150, h_150_250 = EXCLUDED.h_150_250, h_250_500 = EXCLUDED.h_250_500,
                h_500_1000 = EXCLUDED.h_500_1000, h_1000_2000 = EXCLUDED.h_1000_2000, h_2000_5000 = EXCLUDED.h_2000_5000,
                h_5000_10000 = EXCLUDED.h_5000_10000, h_10000_15000 = EXCLUDED.h_10000_15000, h_15000_20000 = EXCLUDED.h_15000_20000,
                h_20000_25000 = EXCLUDED.h_20000_25000, h_25000_30000 = EXCLUDED.h_25000_30000,
                h_30000_60000 = EXCLUDED.h_30000_60000, h_60000_120000 = EXCLUDED.h_60000_120000,
                h_120000_plus = EXCLUDED.h_120000_plus, hist_total = EXCLUDED.hist_total"#,
        )
        .bind(&row.anchor[..])
        .bind(row.slot)
        .bind(row.slot_timestamp)
        .bind(row.sender_count)
        .bind(row.receiver_count)
        .bind(row.p50_ms)
        .bind(row.p75_ms)
        .bind(row.p95_ms)
        .bind(row.p99_ms)
        .bind(row.p100_ms)
        .bind(row.dist_start_p50_ms)
        .bind(row.dist_start_p95_ms)
        .bind(row.dist_start_p99_ms)
        .bind(row.dist_start_p100_ms)
        .bind(row.first_distributed_at)
        .bind(row.last_distributed_at)
        .bind(h[0] as i32).bind(h[1] as i32).bind(h[2] as i32).bind(h[3] as i32)
        .bind(h[4] as i32).bind(h[5] as i32).bind(h[6] as i32).bind(h[7] as i32)
        .bind(h[8] as i32).bind(h[9] as i32).bind(h[10] as i32).bind(h[11] as i32)
        .bind(h[12] as i32).bind(h[13] as i32).bind(h[14] as i32).bind(h[15] as i32)
        .bind(h[16] as i32).bind(h[17] as i32).bind(h[18] as i32).bind(h[19] as i32)
        .bind(h[20] as i32).bind(h[21] as i32).bind(h[22] as i32)
        .bind(row.hist_total as i32)
        .execute(pool)
        .await;

        if let Err(e) = result {
            warn!(slot = ?row.slot, "assurance_convergence UPSERT failed: {e}");
        }
    }

    // Per-sender detail rows (INSERT-only, hypertable)
    for row in &sender_rows {
        let h = &row.histogram;
        let result = sqlx::query(
            r#"INSERT INTO assurance_convergence_senders (distributed_at, anchor, sender_node_id, node_count,
                p50_ms, p75_ms, p95_ms, p99_ms, p100_ms,
                h_0_2, h_2_5, h_5_10, h_10_15, h_15_20, h_20_30, h_30_50, h_50_75, h_75_100,
                h_100_150, h_150_250, h_250_500, h_500_1000, h_1000_2000, h_2000_5000,
                h_5000_10000, h_10000_15000, h_15000_20000, h_20000_25000, h_25000_30000,
                h_30000_60000, h_60000_120000, h_120000_plus, hist_total)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9,
                $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21,
                $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33)"#,
        )
        .bind(row.distributed_at)
        .bind(&row.anchor[..])
        .bind(row.sender_node_id.as_ref())
        .bind(row.node_count)
        .bind(row.p50_ms)
        .bind(row.p75_ms)
        .bind(row.p95_ms)
        .bind(row.p99_ms)
        .bind(row.p100_ms)
        .bind(h[0] as i32).bind(h[1] as i32).bind(h[2] as i32).bind(h[3] as i32)
        .bind(h[4] as i32).bind(h[5] as i32).bind(h[6] as i32).bind(h[7] as i32)
        .bind(h[8] as i32).bind(h[9] as i32).bind(h[10] as i32).bind(h[11] as i32)
        .bind(h[12] as i32).bind(h[13] as i32).bind(h[14] as i32).bind(h[15] as i32)
        .bind(h[16] as i32).bind(h[17] as i32).bind(h[18] as i32).bind(h[19] as i32)
        .bind(h[20] as i32).bind(h[21] as i32).bind(h[22] as i32)
        .bind(row.hist_total as i32)
        .execute(pool)
        .await;

        if let Err(e) = result {
            warn!("assurance_convergence_senders INSERT failed: {e}");
        }
    }

    // Phase 3: Update flags and evict
    for key in &to_mark_flushed {
        if let Some(mut state) = tracker.get_mut(key) {
            state.flushed = true;
        }
    }

    for key in &to_mark_clean {
        if let Some(mut state) = tracker.get_mut(key) {
            state.dirty = false;
        }
    }

    for key in &to_evict {
        tracker.remove(key);
    }

    let total = anchor_rows.len();
    if total > 0 || !to_evict.is_empty() {
        debug!(
            anchor_rows = anchor_rows.len(),
            sender_rows = sender_rows.len(),
            evicted = to_evict.len(),
            "assurance_convergence flush complete"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_percentiles_empty_returns_none() {
        assert!(compute_percentiles(1000, &[]).is_none());
    }

    #[test]
    fn compute_percentiles_single_element() {
        // offset = (2000 - 1000) / 1000 = 1 ms
        let p = compute_percentiles(1000, &[2000]).unwrap();
        assert_eq!(p.p50_ms, 1);
        assert_eq!(p.p75_ms, 1);
        assert_eq!(p.p95_ms, 1);
        assert_eq!(p.p99_ms, 1);
        assert_eq!(p.p100_ms, 1);
    }

    #[test]
    fn compute_percentiles_three_elements() {
        // offsets_ms sorted: [1, 2, 3] (each i*1000 / 1000)
        let p = compute_percentiles(0, &[1000, 3000, 2000]).unwrap();
        assert_eq!(p.p50_ms, 2); // index 1
        assert_eq!(p.p75_ms, 3); // index min((3*0.75) as usize, 2) = min(2, 2) = 2
        assert_eq!(p.p95_ms, 3); // index min((3*0.95) as usize, 2) = min(2, 2) = 2
        assert_eq!(p.p99_ms, 3); // index min((3*0.99) as usize, 2) = min(2, 2) = 2
        assert_eq!(p.p100_ms, 3);
    }

    #[test]
    fn compute_percentiles_100_elements() {
        // timestamps: 1000, 2000, ..., 100_000
        // offsets_ms sorted: [1, 2, ..., 100]
        let timestamps: Vec<u64> = (1..=100).map(|i| i * 1000).collect();
        let p = compute_percentiles(0, &timestamps).unwrap();
        // offsets[50]=51, offsets[75]=76, offsets[95]=96, offsets[99]=100
        assert_eq!(p.p50_ms, 51);  // index 100/2 = 50
        assert_eq!(p.p75_ms, 76);  // index (100*0.75) as usize = 75
        assert_eq!(p.p95_ms, 96);  // index (100*0.95) as usize = 95
        assert_eq!(p.p99_ms, 100); // index (100*0.99) as usize = 99
        assert_eq!(p.p100_ms, 100);
    }

    #[test]
    fn state_creation_and_convergence() {
        let state = GuaranteeConvergenceState {
            built_at: Some(1000),
            slot: Some(42),
            core: Some(3),
            wp_hash: Some([0xAB; 32]),
            builder_node_id: None,
            received_timestamps: vec![2000, 5000, 3000],
            last_event: Instant::now(),
            flushed: false,
            dirty: true,
        };

        assert_eq!(state.slot, Some(42));
        assert_eq!(state.core, Some(3));
        assert_eq!(state.received_timestamps.len(), 3);

        let p = compute_percentiles(state.built_at.unwrap(), &state.received_timestamps).unwrap();
        // offsets_ms sorted: [1, 2, 4]
        assert_eq!(p.p50_ms, 2);
        assert_eq!(p.p100_ms, 4);
    }

    #[test]
    fn state_no_built_at_no_convergence() {
        let state = GuaranteeConvergenceState {
            built_at: None,
            slot: Some(1),
            core: None,
            wp_hash: None,
            builder_node_id: None,
            received_timestamps: vec![1000, 2000],
            last_event: Instant::now(),
            flushed: false,
            dirty: true,
        };

        assert!(state.built_at.is_none());
        // No anchor means we cannot compute convergence
    }

    #[test]
    fn state_empty_received_timestamps() {
        let state = GuaranteeConvergenceState {
            built_at: Some(1000),
            slot: Some(1),
            core: None,
            wp_hash: None,
            builder_node_id: None,
            received_timestamps: vec![],
            last_event: Instant::now(),
            flushed: false,
            dirty: true,
        };

        assert!(compute_percentiles(state.built_at.unwrap(), &state.received_timestamps).is_none());
    }

    // --- Assurance convergence tests ---

    #[test]
    fn compute_percentiles_from_deltas_empty_returns_none() {
        assert!(compute_percentiles_from_deltas(&[]).is_none());
    }

    #[test]
    fn compute_percentiles_from_deltas_single() {
        let p = compute_percentiles_from_deltas(&[42]).unwrap();
        assert_eq!(p.p50_ms, 42);
        assert_eq!(p.p75_ms, 42);
        assert_eq!(p.p95_ms, 42);
        assert_eq!(p.p99_ms, 42);
        assert_eq!(p.p100_ms, 42);
    }

    #[test]
    fn compute_percentiles_from_deltas_multiple() {
        // sorted: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        let deltas: Vec<i32> = (1..=10).collect();
        let p = compute_percentiles_from_deltas(&deltas).unwrap();
        assert_eq!(p.p50_ms, 6);   // index 10/2 = 5 → sorted[5] = 6
        assert_eq!(p.p75_ms, 8);   // index (10*0.75)=7 → sorted[7] = 8
        assert_eq!(p.p95_ms, 10);  // index (10*0.95)=9 → sorted[9] = 10
        assert_eq!(p.p99_ms, 10);  // index (10*0.99)=9 → sorted[9] = 10
        assert_eq!(p.p100_ms, 10);
    }

    #[test]
    fn pending_received_buffer_resolves() {
        let mut state = AnchorState {
            slot: Some(100),
            senders: HashMap::new(),
            pending_received: vec![
                (Arc::from("node-A"), 200_000),
                (Arc::from("node-B"), 300_000),
            ],
            last_event: Instant::now(),
            flushed: false,
            dirty: false,
        };

        assert_eq!(state.pending_received.len(), 2);

        // Simulate sender appearing: drain pending and create sender state
        let sender_id: Arc<str> = Arc::from("sender-1");
        let distributed_at: u64 = 100_000;
        state.senders.insert(
            sender_id.clone(),
            SenderAssuranceState {
                distributed_at,
                deltas_ms: Vec::new(),
            },
        );

        // Drain pending_received for this sender
        let pending = std::mem::take(&mut state.pending_received);
        for (_receiver_node_id, received_ts) in &pending {
            let delta_ms = ((*received_ts as i64 - distributed_at as i64) / 1000).max(0) as i32;
            state
                .senders
                .get_mut(&sender_id)
                .unwrap()
                .deltas_ms
                .push(delta_ms);
        }

        assert!(state.pending_received.is_empty());
        let sender = state.senders.get(&sender_id).unwrap();
        assert_eq!(sender.deltas_ms.len(), 2);
        // (200_000 - 100_000) / 1000 = 100 ms
        assert_eq!(sender.deltas_ms[0], 100);
        // (300_000 - 100_000) / 1000 = 200 ms
        assert_eq!(sender.deltas_ms[1], 200);
    }

    #[test]
    fn distribution_start_spread() {
        // 3 senders at timestamps 100_000, 105_000, 110_000 (in microseconds)
        // Spread deltas relative to min (100_000):
        //   (100_000 - 100_000) / 1000 = 0 ms
        //   (105_000 - 100_000) / 1000 = 5 ms
        //   (110_000 - 100_000) / 1000 = 10 ms
        let distributed_ats: Vec<u64> = vec![100_000, 105_000, 110_000];
        let min_dist = *distributed_ats.iter().min().unwrap();
        let spread_deltas: Vec<i32> = distributed_ats
            .iter()
            .map(|&t| ((t as i64 - min_dist as i64) / 1000).max(0) as i32)
            .collect();

        assert_eq!(spread_deltas, vec![0, 5, 10]);

        let p = compute_percentiles_from_deltas(&spread_deltas).unwrap();
        // sorted: [0, 5, 10], len=3
        assert_eq!(p.p50_ms, 5);   // index 3/2 = 1 → sorted[1] = 5
        assert_eq!(p.p75_ms, 10);  // index (3*0.75)=2 → sorted[2] = 10
        assert_eq!(p.p95_ms, 10);  // index (3*0.95)=2 → sorted[2] = 10
        assert_eq!(p.p99_ms, 10);  // index (3*0.99)=2 → sorted[2] = 10
        assert_eq!(p.p100_ms, 10);
    }

    #[test]
    fn header_hash_lookup_eviction() {
        let lookup = new_header_hash_lookup();
        // Insert 100 entries with distinct keys
        for i in 0u32..100 {
            let mut key = [0u8; 32];
            key[0..4].copy_from_slice(&i.to_le_bytes());
            lookup.insert(key, i);
        }
        assert_eq!(lookup.len(), 100);
        // Evict with max_entries=50 — should remove ~25% of entries
        evict_header_hash_lookup(&lookup, 50);
        assert!(lookup.len() < 100, "size should be reduced after eviction");
    }
}
