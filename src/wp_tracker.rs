use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use dashmap::DashMap;
use sqlx::PgPool;
use tracing::warn;

use crate::types::JCE_EPOCH_UNIX_MICROS;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

pub type WpTracker = Arc<DashMap<[u8; 32], WpState>>;

pub fn new_wp_tracker() -> WpTracker {
    Arc::new(DashMap::new())
}

// ---------------------------------------------------------------------------
// WpState
// ---------------------------------------------------------------------------

pub struct WpState {
    pub first_seen: u64,
    pub last_updated: u64,
    pub core: u16,
    pub service_ids: Vec<u32>,
    pub received_by: u16,
    pub guaranteed_by: u16,
    /// Nodes that contributed to received_by (in-memory dedup only).
    pub received_nodes: HashSet<Arc<str>>,
    /// Nodes that contributed to guaranteed_by (in-memory dedup only).
    pub guaranteed_nodes: HashSet<Arc<str>>,
    /// 0=received, 1=authorized, 2=refined, 3=report_built,
    /// 4=guarantee_built, 5=distributed
    pub stage: u8,
    pub received_at: Option<u64>,
    pub authorized_at: Option<u64>,
    pub refined_at: Option<u64>,
    pub report_built_at: Option<u64>,
    pub guarantee_built_at: Option<u64>,
    pub distributed_at: Option<u64>,
    pub failed_at: Option<u64>,
    pub dirty: bool,
    pub last_activity: Instant,
}

impl Default for WpState {
    fn default() -> Self {
        Self {
            first_seen: 0,
            last_updated: 0,
            core: 0,
            service_ids: Vec::new(),
            received_by: 0,
            guaranteed_by: 0,
            received_nodes: HashSet::new(),
            guaranteed_nodes: HashSet::new(),
            stage: 0,
            received_at: None,
            authorized_at: None,
            refined_at: None,
            report_built_at: None,
            guarantee_built_at: None,
            distributed_at: None,
            failed_at: None,
            dirty: false,
            last_activity: Instant::now(),
        }
    }
}

impl WpState {
    /// Advance the work-package through its pipeline stage.
    ///
    /// `ordinal` is the numeric stage (0..=5).  For the special failure
    /// event (event_type 92) the caller should set `failed_at` directly or
    /// pass `ordinal` obtained from `event_type_to_ordinal` — but since 92
    /// maps to 0 we handle failure separately via `mark_failed`.
    pub fn update_stage(&mut self, ordinal: u8, timestamp: u64) {
        match ordinal {
            0 => {
                if self.received_at.is_none() {
                    self.received_at = Some(timestamp);
                }
            }
            1 => {
                if self.authorized_at.is_none() {
                    self.authorized_at = Some(timestamp);
                }
            }
            2 => {
                if self.refined_at.is_none() {
                    self.refined_at = Some(timestamp);
                }
            }
            3 => {
                if self.report_built_at.is_none() {
                    self.report_built_at = Some(timestamp);
                }
            }
            4 => {
                if self.guarantee_built_at.is_none() {
                    self.guarantee_built_at = Some(timestamp);
                }
            }
            5 => {
                if self.distributed_at.is_none() {
                    self.distributed_at = Some(timestamp);
                }
            }
            _ => {}
        }

        if ordinal > self.stage {
            self.stage = ordinal;
        }

        self.last_updated = timestamp;
        self.dirty = true;
        self.last_activity = Instant::now();
    }

    /// Mark the work-package as failed.
    pub fn mark_failed(&mut self, timestamp: u64) {
        if self.failed_at.is_none() {
            self.failed_at = Some(timestamp);
        }
        self.last_updated = timestamp;
        self.dirty = true;
        self.last_activity = Instant::now();
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Map an event-type id to a pipeline ordinal.
pub fn event_type_to_ordinal(et: u16) -> u8 {
    match et {
        94 => 0,  // received
        95 => 1,  // authorized
        101 => 2, // refined
        102 => 3, // report_built
        105 => 4, // guarantee_built
        109 => 5, // distributed
        _ => 0,
    }
}

/// Convert a JCE-epoch microsecond timestamp to a `chrono::DateTime<Utc>`.
pub fn ts_to_chrono(ts: u64) -> DateTime<Utc> {
    let unix_us = JCE_EPOCH_UNIX_MICROS + ts as i64;
    Utc.timestamp_micros(unix_us)
        .single()
        .unwrap_or_else(|| Utc.timestamp_micros(0).unwrap())
}

/// Convert an `Option<u64>` timestamp the same way, returning `None` when the
/// input is `None`.
fn opt_ts_to_chrono(ts: Option<u64>) -> Option<DateTime<Utc>> {
    ts.map(ts_to_chrono)
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

/// Two-phase flush: collect dirty entries without holding guards across await,
/// then upsert to Postgres, then clear dirty flags / evict stale entries.
pub async fn flush_wp_tracker(tracker: &WpTracker, pool: &PgPool) {
    const EVICT_AFTER: Duration = Duration::from_secs(60);

    // Phase 1 — snapshot dirty entries and find eviction candidates.
    let mut to_flush: Vec<([u8; 32], WpStateSnapshot)> = Vec::new();
    let mut to_evict: Vec<[u8; 32]> = Vec::new();

    for entry in tracker.iter() {
        let key = *entry.key();
        let val = entry.value();

        if val.dirty {
            to_flush.push((key, WpStateSnapshot::from(val)));
        }

        if val.last_activity.elapsed() > EVICT_AFTER {
            to_evict.push(key);
        }
    }

    // Phase 2 — upsert to DB (no DashMap guards held).
    for (hash, snap) in &to_flush {
        let first_seen = ts_to_chrono(snap.first_seen);
        let last_updated = ts_to_chrono(snap.last_updated);
        let received_at = opt_ts_to_chrono(snap.received_at);
        let authorized_at = opt_ts_to_chrono(snap.authorized_at);
        let refined_at = opt_ts_to_chrono(snap.refined_at);
        let report_built_at = opt_ts_to_chrono(snap.report_built_at);
        let guarantee_built_at = opt_ts_to_chrono(snap.guarantee_built_at);
        let distributed_at = opt_ts_to_chrono(snap.distributed_at);
        let failed_at = opt_ts_to_chrono(snap.failed_at);

        let service_ids: Vec<i32> = snap.service_ids.iter().map(|&s| s as i32).collect();

        let res = sqlx::query(
            r#"
INSERT INTO wp_tracking (wp_hash, first_seen, last_updated, core, service_ids,
    received_at, authorized_at, refined_at, report_built_at,
    guarantee_built_at, distributed_at, failed_at,
    received_by, guaranteed_by, stage)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
ON CONFLICT (wp_hash)
DO UPDATE SET
    last_updated = EXCLUDED.last_updated,
    received_at = COALESCE(wp_tracking.received_at, EXCLUDED.received_at),
    authorized_at = COALESCE(wp_tracking.authorized_at, EXCLUDED.authorized_at),
    refined_at = COALESCE(wp_tracking.refined_at, EXCLUDED.refined_at),
    report_built_at = COALESCE(wp_tracking.report_built_at, EXCLUDED.report_built_at),
    guarantee_built_at = COALESCE(wp_tracking.guarantee_built_at, EXCLUDED.guarantee_built_at),
    distributed_at = COALESCE(wp_tracking.distributed_at, EXCLUDED.distributed_at),
    failed_at = COALESCE(wp_tracking.failed_at, EXCLUDED.failed_at),
    received_by = GREATEST(wp_tracking.received_by, EXCLUDED.received_by),
    guaranteed_by = GREATEST(wp_tracking.guaranteed_by, EXCLUDED.guaranteed_by),
    stage = GREATEST(wp_tracking.stage, EXCLUDED.stage)
"#,
        )
        .bind(hash.as_slice())
        .bind(first_seen)
        .bind(last_updated)
        .bind(snap.core as i16)
        .bind(&service_ids)
        .bind(received_at)
        .bind(authorized_at)
        .bind(refined_at)
        .bind(report_built_at)
        .bind(guarantee_built_at)
        .bind(distributed_at)
        .bind(failed_at)
        .bind(snap.received_by as i16)
        .bind(snap.guaranteed_by as i16)
        .bind(snap.stage as i16)
        .execute(pool)
        .await;

        if let Err(e) = res {
            warn!(wp_hash = hex::encode(hash), error = %e, "wp_tracking upsert failed");
        }
    }

    // Phase 3 — clear dirty flags and evict stale entries.
    for (hash, _) in &to_flush {
        if let Some(mut entry) = tracker.get_mut(hash) {
            entry.dirty = false;
        }
    }

    for hash in &to_evict {
        tracker.remove(hash);
    }
}

// ---------------------------------------------------------------------------
// Internal snapshot (owned copy, safe to hold across await)
// ---------------------------------------------------------------------------

struct WpStateSnapshot {
    first_seen: u64,
    last_updated: u64,
    core: u16,
    service_ids: Vec<u32>,
    received_by: u16,
    guaranteed_by: u16,
    stage: u8,
    received_at: Option<u64>,
    authorized_at: Option<u64>,
    refined_at: Option<u64>,
    report_built_at: Option<u64>,
    guarantee_built_at: Option<u64>,
    distributed_at: Option<u64>,
    failed_at: Option<u64>,
}

impl From<&WpState> for WpStateSnapshot {
    fn from(s: &WpState) -> Self {
        Self {
            first_seen: s.first_seen,
            last_updated: s.last_updated,
            core: s.core,
            service_ids: s.service_ids.clone(),
            received_by: s.received_by,
            guaranteed_by: s.guaranteed_by,
            stage: s.stage,
            received_at: s.received_at,
            authorized_at: s.authorized_at,
            refined_at: s.refined_at,
            report_built_at: s.report_built_at,
            guarantee_built_at: s.guarantee_built_at,
            distributed_at: s.distributed_at,
            failed_at: s.failed_at,
        }
    }
}
