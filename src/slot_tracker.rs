//! Per-slot convergence metrics. Collects event timestamps from multiple nodes for
//! each block slot and computes latency percentiles (p50/p99/p100) relative to
//! block authoring time. Periodically flushes results to Postgres.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use dashmap::DashMap;
use sqlx::PgPool;
use tracing::{debug, warn};

use crate::types::JCE_EPOCH_UNIX_MICROS;

pub type SlotTracker = Arc<DashMap<u32, SlotState>>;

pub fn new_slot_tracker() -> SlotTracker {
    Arc::new(DashMap::new())
}

pub struct SlotState {
    pub authored_at: Option<u64>,
    pub stages: HashMap<u16, Vec<u64>>,
    pub last_event: Instant,
    pub flushed: bool,
    pub dirty: bool,
}

pub struct SlotConvergenceRow {
    pub slot: u32,
    pub event_type: i16,
    pub node_count: i16,
    pub p50_ms: i32,
    pub p99_ms: i32,
    pub p100_ms: i32,
    pub authored_at: u64,
}

impl SlotState {
    pub fn new(event_type: u16, timestamp: u64, authored_at: Option<u64>) -> Self {
        let mut stages = HashMap::new();
        stages.insert(event_type, vec![timestamp]);
        Self {
            authored_at,
            stages,
            last_event: Instant::now(),
            flushed: false,
            dirty: true,
        }
    }

    pub fn record(&mut self, event_type: u16, timestamp: u64) {
        self.stages.entry(event_type).or_default().push(timestamp);
        self.dirty = true;
        self.last_event = Instant::now();
    }

    pub fn compute_convergence(&self, slot: u32) -> Vec<SlotConvergenceRow> {
        let authored_at = match self.authored_at {
            Some(ts) => ts,
            None => return Vec::new(),
        };

        let mut rows = Vec::new();

        for (&event_type, timestamps) in &self.stages {
            if timestamps.is_empty() {
                continue;
            }

            let mut offsets_ms: Vec<i64> = timestamps
                .iter()
                .map(|&t| (t as i64 - authored_at as i64) / 1000)
                .collect();
            offsets_ms.sort();

            let len = offsets_ms.len();
            let p50 = offsets_ms[len / 2];
            let p99_idx = ((len as f64 * 0.99) as usize).min(len - 1);
            let p99 = offsets_ms[p99_idx];
            let p100 = offsets_ms[len - 1];

            rows.push(SlotConvergenceRow {
                slot,
                event_type: event_type as i16,
                node_count: len as i16,
                p50_ms: p50 as i32,
                p99_ms: p99 as i32,
                p100_ms: p100 as i32,
                authored_at,
            });
        }

        rows
    }
}

fn timestamp_to_datetime(authored_at: u64) -> DateTime<Utc> {
    let unix_us = JCE_EPOCH_UNIX_MICROS + authored_at as i64;
    let secs = unix_us / 1_000_000;
    let nsecs = ((unix_us % 1_000_000) * 1_000) as u32;
    Utc.timestamp_opt(secs, nsecs).unwrap()
}

pub async fn flush_slot_tracker(
    tracker: &SlotTracker,
    pool: &PgPool,
    age_insert: Duration,
    age_evict: Duration,
) {
    let now = Instant::now();

    // Phase 1: Collect — never hold DashMap guards across await
    let mut to_insert: Vec<(u32, Vec<SlotConvergenceRow>)> = Vec::new();
    let mut to_upsert: Vec<(u32, Vec<SlotConvergenceRow>)> = Vec::new();
    let mut to_evict: Vec<u32> = Vec::new();

    for entry in tracker.iter() {
        let slot = *entry.key();
        let state = entry.value();
        let age = now.duration_since(state.last_event);

        if age >= age_evict {
            // Collect final convergence before eviction if dirty
            if state.dirty {
                let rows = state.compute_convergence(slot);
                if !rows.is_empty() {
                    to_upsert.push((slot, rows));
                }
            }
            to_evict.push(slot);
        } else if !state.flushed && age >= age_insert {
            let rows = state.compute_convergence(slot);
            if !rows.is_empty() {
                to_insert.push((slot, rows));
            }
        } else if state.flushed && state.dirty {
            let rows = state.compute_convergence(slot);
            if !rows.is_empty() {
                to_upsert.push((slot, rows));
            }
        }
    }

    // Phase 2: DB writes
    for (slot, rows) in &to_insert {
        for row in rows {
            let authored_dt = timestamp_to_datetime(row.authored_at);
            let result = sqlx::query(
                r#"INSERT INTO slot_convergence (slot, event_type, node_count, p50_ms, p99_ms, p100_ms, authored_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (slot, event_type)
                   DO UPDATE SET
                       node_count = GREATEST(slot_convergence.node_count, EXCLUDED.node_count),
                       p50_ms = EXCLUDED.p50_ms,
                       p99_ms = EXCLUDED.p99_ms,
                       p100_ms = GREATEST(slot_convergence.p100_ms, EXCLUDED.p100_ms),
                       authored_at = COALESCE(slot_convergence.authored_at, EXCLUDED.authored_at)"#,
            )
            .bind(row.slot as i32)
            .bind(row.event_type)
            .bind(row.node_count)
            .bind(row.p50_ms)
            .bind(row.p99_ms)
            .bind(row.p100_ms)
            .bind(authored_dt)
            .execute(pool)
            .await;

            if let Err(e) = result {
                warn!(slot = slot, event_type = row.event_type, "slot_convergence INSERT failed: {e}");
            }
        }
    }

    for (slot, rows) in &to_upsert {
        for row in rows {
            let authored_dt = timestamp_to_datetime(row.authored_at);
            let result = sqlx::query(
                r#"INSERT INTO slot_convergence (slot, event_type, node_count, p50_ms, p99_ms, p100_ms, authored_at)
                   VALUES ($1, $2, $3, $4, $5, $6, $7)
                   ON CONFLICT (slot, event_type)
                   DO UPDATE SET
                       node_count = GREATEST(slot_convergence.node_count, EXCLUDED.node_count),
                       p50_ms = EXCLUDED.p50_ms,
                       p99_ms = EXCLUDED.p99_ms,
                       p100_ms = GREATEST(slot_convergence.p100_ms, EXCLUDED.p100_ms),
                       authored_at = COALESCE(slot_convergence.authored_at, EXCLUDED.authored_at)"#,
            )
            .bind(row.slot as i32)
            .bind(row.event_type)
            .bind(row.node_count)
            .bind(row.p50_ms)
            .bind(row.p99_ms)
            .bind(row.p100_ms)
            .bind(authored_dt)
            .execute(pool)
            .await;

            if let Err(e) = result {
                warn!(slot = slot, event_type = row.event_type, "slot_convergence UPSERT failed: {e}");
            }
        }
    }

    // Phase 3: Update flags and evict
    for (slot, _) in &to_insert {
        if let Some(mut state) = tracker.get_mut(slot) {
            state.flushed = true;
            state.dirty = false;
        }
    }

    for (slot, _) in &to_upsert {
        if let Some(mut state) = tracker.get_mut(slot) {
            state.dirty = false;
        }
    }

    for slot in &to_evict {
        tracker.remove(slot);
    }

    let total = to_insert.len() + to_upsert.len();
    if total > 0 || !to_evict.is_empty() {
        debug!(
            inserted = to_insert.len(),
            upserted = to_upsert.len(),
            evicted = to_evict.len(),
            "slot_tracker flush complete"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_initial_state() {
        let state = SlotState::new(11, 1000, Some(500));
        assert_eq!(state.authored_at, Some(500));
        assert_eq!(state.stages.get(&11), Some(&vec![1000u64]));
        assert!(state.dirty);
        assert!(!state.flushed);
    }

    #[test]
    fn record_adds_to_existing_stage() {
        let mut state = SlotState::new(11, 1000, None);
        state.record(11, 2000);
        assert_eq!(state.stages[&11], vec![1000, 2000]);
    }

    #[test]
    fn record_creates_new_stage() {
        let mut state = SlotState::new(11, 1000, None);
        state.record(12, 2000);
        assert_eq!(state.stages[&12], vec![2000]);
        assert_eq!(state.stages[&11], vec![1000]);
    }

    #[test]
    fn convergence_single_timestamp() {
        // offset = (1500 - 1000) / 1000 = 0 ms
        let state = SlotState::new(11, 1500, Some(1000));
        let rows = state.compute_convergence(42);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].slot, 42);
        assert_eq!(rows[0].event_type, 11);
        assert_eq!(rows[0].node_count, 1);
        assert_eq!(rows[0].p50_ms, rows[0].p99_ms);
        assert_eq!(rows[0].p99_ms, rows[0].p100_ms);
    }

    #[test]
    fn convergence_multiple_timestamps() {
        let mut state = SlotState::new(11, 1_000, Some(0));
        // Add 99 more timestamps: 2_000, 3_000, ..., 100_000
        for i in 2..=100u64 {
            state.record(11, i * 1_000);
        }
        let rows = state.compute_convergence(1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].node_count, 100);
        // offsets_ms sorted: [1, 2, ..., 100] (each i*1000 / 1000 = i)
        // p50 = offsets[50] = 51
        assert_eq!(rows[0].p50_ms, 51);
        // p99_idx = min((100 * 0.99) as usize, 99) = min(99, 99) = 99
        assert_eq!(rows[0].p99_ms, 100);
        assert_eq!(rows[0].p100_ms, 100);
    }

    #[test]
    fn convergence_no_authored_at_returns_empty() {
        let state = SlotState::new(11, 1000, None);
        assert!(state.compute_convergence(1).is_empty());
    }

    #[test]
    fn convergence_mixed_stages() {
        let mut state = SlotState::new(11, 2000, Some(1000));
        state.record(12, 5000); // different event type
        let rows = state.compute_convergence(1);
        assert_eq!(rows.len(), 2);
        let types: Vec<i16> = rows.iter().map(|r| r.event_type).collect();
        assert!(types.contains(&11));
        assert!(types.contains(&12));
    }
}
