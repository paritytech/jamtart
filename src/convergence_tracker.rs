//! Guarantee convergence tracker. Measures how quickly guarantees propagate across the validator network.

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
    pub node_count: i16,
    pub p50_ms: i32,
    pub p75_ms: Option<i32>,
    pub p95_ms: Option<i32>,
    pub p99_ms: i32,
    pub p100_ms: i32,
    pub built_at: u64,
}

pub struct GuaranteeConvergenceSlotRow {
    pub slot: i32,
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
                    to_upsert.push(GuaranteeConvergenceRow {
                        work_report_hash,
                        slot,
                        core: state.core.map(|c| c as i16),
                        wp_hash: state.wp_hash.map(|h| h.to_vec()),
                        node_count: state.received_timestamps.len() as i16,
                        p50_ms: p.p50_ms,
                        p75_ms: Some(p.p75_ms),
                        p95_ms: Some(p.p95_ms),
                        p99_ms: p.p99_ms,
                        p100_ms: p.p100_ms,
                        built_at,
                    });
                    dirty_slots.push(slot);
                }
            }
            to_evict.push(work_report_hash);
        } else if !state.flushed && age >= age_insert {
            if let Some(p) = compute_percentiles(built_at, &state.received_timestamps) {
                to_insert.push(GuaranteeConvergenceRow {
                    work_report_hash,
                    slot,
                    core: state.core.map(|c| c as i16),
                    wp_hash: state.wp_hash.map(|h| h.to_vec()),
                    node_count: state.received_timestamps.len() as i16,
                    p50_ms: p.p50_ms,
                    p75_ms: Some(p.p75_ms),
                    p95_ms: Some(p.p95_ms),
                    p99_ms: p.p99_ms,
                    p100_ms: p.p100_ms,
                    built_at,
                });
                dirty_slots.push(slot);
            }
        } else if state.flushed && state.dirty {
            if let Some(p) = compute_percentiles(built_at, &state.received_timestamps) {
                to_upsert.push(GuaranteeConvergenceRow {
                    work_report_hash,
                    slot,
                    core: state.core.map(|c| c as i16),
                    wp_hash: state.wp_hash.map(|h| h.to_vec()),
                    node_count: state.received_timestamps.len() as i16,
                    p50_ms: p.p50_ms,
                    p75_ms: Some(p.p75_ms),
                    p95_ms: Some(p.p95_ms),
                    p99_ms: p.p99_ms,
                    p100_ms: p.p100_ms,
                    built_at,
                });
                dirty_slots.push(slot);
            }
        }
    }

    // Collect unique dirty slots for per-slot summary
    dirty_slots.sort();
    dirty_slots.dedup();

    // Build per-slot summaries scanning ALL in-memory entries for those slots
    let mut slot_summaries: Vec<GuaranteeConvergenceSlotRow> = Vec::new();
    for &slot in &dirty_slots {
        let mut all_timestamps: Vec<u64> = Vec::new();
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

            // Flatten all received timestamps relative to this guarantee's built_at
            for &t in &state.received_timestamps {
                // Store raw timestamps; we'll compute offsets from min_built_at later
                all_timestamps.push(t);
            }
        }

        if guarantee_count == 0 || all_timestamps.is_empty() {
            continue;
        }

        let anchor = min_built_at.unwrap();
        let percentiles = compute_percentiles(anchor, &all_timestamps);

        slot_summaries.push(GuaranteeConvergenceSlotRow {
            slot,
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

    // Phase 2: DB writes — per-guarantee rows
    for row in &to_insert {
        let built_dt = ts_to_datetime(row.built_at);
        let result = sqlx::query(
            r#"INSERT INTO guarantee_convergence (work_report_hash, slot, core, wp_hash, node_count, p50_ms, p75_ms, p95_ms, p99_ms, p100_ms, built_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
               ON CONFLICT (work_report_hash) DO UPDATE SET
                   node_count = GREATEST(guarantee_convergence.node_count, EXCLUDED.node_count),
                   p50_ms = EXCLUDED.p50_ms,
                   p75_ms = EXCLUDED.p75_ms,
                   p95_ms = EXCLUDED.p95_ms,
                   p99_ms = EXCLUDED.p99_ms,
                   p100_ms = GREATEST(guarantee_convergence.p100_ms, EXCLUDED.p100_ms),
                   built_at = COALESCE(guarantee_convergence.built_at, EXCLUDED.built_at)"#,
        )
        .bind(&row.work_report_hash[..])
        .bind(row.slot)
        .bind(row.core)
        .bind(&row.wp_hash)
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
            warn!(slot = row.slot, "guarantee_convergence INSERT failed: {e}");
        }
    }

    for row in &to_upsert {
        let built_dt = ts_to_datetime(row.built_at);
        let result = sqlx::query(
            r#"INSERT INTO guarantee_convergence (work_report_hash, slot, core, wp_hash, node_count, p50_ms, p75_ms, p95_ms, p99_ms, p100_ms, built_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
               ON CONFLICT (work_report_hash) DO UPDATE SET
                   node_count = GREATEST(guarantee_convergence.node_count, EXCLUDED.node_count),
                   p50_ms = EXCLUDED.p50_ms,
                   p75_ms = EXCLUDED.p75_ms,
                   p95_ms = EXCLUDED.p95_ms,
                   p99_ms = EXCLUDED.p99_ms,
                   p100_ms = GREATEST(guarantee_convergence.p100_ms, EXCLUDED.p100_ms),
                   built_at = COALESCE(guarantee_convergence.built_at, EXCLUDED.built_at)"#,
        )
        .bind(&row.work_report_hash[..])
        .bind(row.slot)
        .bind(row.core)
        .bind(&row.wp_hash)
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
            warn!(slot = row.slot, "guarantee_convergence UPSERT failed: {e}");
        }
    }

    // Per-slot summary rows
    for row in &slot_summaries {
        let built_dt = ts_to_datetime(row.built_at);
        let result = sqlx::query(
            r#"INSERT INTO guarantee_convergence_slots (slot, guarantee_count, node_count, p50_ms, p75_ms, p95_ms, p99_ms, p100_ms, built_at)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
               ON CONFLICT (slot) DO UPDATE SET
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
            received_timestamps: vec![],
            last_event: Instant::now(),
            flushed: false,
            dirty: true,
        };

        assert!(compute_percentiles(state.built_at.unwrap(), &state.received_timestamps).is_none());
    }
}
