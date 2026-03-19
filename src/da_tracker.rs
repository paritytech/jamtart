//! DA tracker — shard distribution latency, event counts, and shard inventory per node.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use dashmap::DashMap;
use sqlx::PgPool;
use tracing::{debug, warn};

use crate::types::JCE_EPOCH_UNIX_MICROS;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

pub type DaTracker = Arc<DashMap<Arc<str>, DaNodeState>>;

pub fn new_da_tracker() -> DaTracker {
    Arc::new(DashMap::new())
}

// ---------------------------------------------------------------------------
// Per-node state
// ---------------------------------------------------------------------------

pub struct DaNodeState {
    // Shard event counts (since last flush)
    pub shard_requests_sent: u32,       // SendingShardRequest(120)
    pub shard_requests_received: u32,   // ReceivingShardRequest(121)
    pub shard_sent_confirmed: u32,      // ShardRequestSent(123)
    pub shard_received_confirmed: u32,  // ShardRequestReceived(124)
    pub shards_transferred: u32,        // ShardsTransferred(125)
    pub shard_failures: u32,            // ShardRequestFailed(122)

    // Preimage event counts
    pub preimage_ann_failures: u32,     // PreimageAnnouncementFailed(190)
    pub preimages_announced: u32,       // PreimageAnnounced(191)
    pub preimages_forgotten: u32,       // AnnouncedPreimageForgotten(192)

    // Shard latency — assurer side: SendingShardRequest(120) → ShardsTransferred(125)
    pub assurer_pending: HashMap<u64, u64>,  // event_id → timestamp (JCE micros)
    pub assurer_latency_sum_us: u64,
    pub assurer_latency_count: u32,

    // Shard latency — guarantor side: ReceivingShardRequest(121) → ShardRequestReceived(124)
    pub guarantor_pending: HashMap<u64, u64>,  // event_id → timestamp (JCE micros)
    pub guarantor_latency_sum_us: u64,
    pub guarantor_latency_count: u32,

    // Shard latency histograms (per side)
    pub assurer_hist: [u32; 14],
    pub assurer_hist_total: u32,
    pub assurer_hist_failed: u32,
    pub guarantor_hist: [u32; 14],
    pub guarantor_hist_total: u32,
    pub guarantor_hist_failed: u32,

    // Shard inventory (count only in DB, HashSet for dedup)
    pub active_shards: HashSet<u16>,

    // Flush control
    pub dirty: bool,
    pub last_activity: Instant,
}

impl Default for DaNodeState {
    fn default() -> Self {
        Self {
            shard_requests_sent: 0,
            shard_requests_received: 0,
            shard_sent_confirmed: 0,
            shard_received_confirmed: 0,
            shards_transferred: 0,
            shard_failures: 0,
            preimage_ann_failures: 0,
            preimages_announced: 0,
            preimages_forgotten: 0,
            assurer_pending: HashMap::new(),
            assurer_latency_sum_us: 0,
            assurer_latency_count: 0,
            guarantor_pending: HashMap::new(),
            guarantor_latency_sum_us: 0,
            guarantor_latency_count: 0,
            assurer_hist: [0; 14],
            assurer_hist_total: 0,
            assurer_hist_failed: 0,
            guarantor_hist: [0; 14],
            guarantor_hist_total: 0,
            guarantor_hist_failed: 0,
            active_shards: HashSet::new(),
            dirty: false,
            last_activity: Instant::now(),
        }
    }
}

// ---------------------------------------------------------------------------
// Histogram
// ---------------------------------------------------------------------------

const HIST_BOUNDARIES_MS: [u32; 14] = [0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 3000, 5000];

/// Find the histogram bucket index for a delta in milliseconds.
/// Boundaries: [0,1), [1,2), [2,5), [5,10), [10,25), [25,50), [50,100),
///   [100,250), [250,500), [500,1000), [1000,2000), [2000,3000), [3000,5000), [5000,∞)
pub fn hist_bucket_index(delta_ms: i32) -> usize {
    let delta = delta_ms.max(0) as u32;
    for i in (0..HIST_BOUNDARIES_MS.len()).rev() {
        if delta >= HIST_BOUNDARIES_MS[i] {
            return i;
        }
    }
    0
}

// ---------------------------------------------------------------------------
// Pending eviction
// ---------------------------------------------------------------------------

const PENDING_TTL_US: u64 = 30_000_000; // 30 seconds in microseconds
const PENDING_CAP: usize = 20_000;

/// Evict stale entries from a pending map. Returns count of evicted entries.
fn evict_pending(pending: &mut HashMap<u64, u64>, now_us: u64) -> usize {
    let before = pending.len();
    pending.retain(|_, &mut ts| now_us.saturating_sub(ts) < PENDING_TTL_US);
    let after = pending.len();
    // Cap enforcement
    if after > PENDING_CAP {
        let to_remove = after - PENDING_CAP;
        let keys: Vec<u64> = pending.keys().take(to_remove).copied().collect();
        for k in keys {
            pending.remove(&k);
        }
    }
    before - pending.len().min(before)
}

// ---------------------------------------------------------------------------
// Timestamp conversion
// ---------------------------------------------------------------------------

#[allow(dead_code)]
fn ts_to_datetime(jce_micros: u64) -> DateTime<Utc> {
    let unix_us = JCE_EPOCH_UNIX_MICROS + jce_micros as i64;
    let secs = unix_us / 1_000_000;
    let nsecs = ((unix_us % 1_000_000) * 1_000) as u32;
    Utc.timestamp_opt(secs, nsecs).unwrap()
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

const STALE_TTL: Duration = Duration::from_secs(60);

pub async fn flush_da_tracker(tracker: &DaTracker, pool: &PgPool) {
    let now = Instant::now();
    let mut rows_written = 0u32;
    let mut hist_rows_written = 0u32;
    let mut nodes_evicted = 0u32;

    // Phase 1: Snapshot dirty entries and collect stale keys
    struct Snapshot {
        node_id: Arc<str>,
        shard_requests_sent: u32,
        shard_requests_received: u32,
        shard_sent_confirmed: u32,
        shard_received_confirmed: u32,
        shards_transferred: u32,
        shard_failures: u32,
        preimage_ann_failures: u32,
        preimages_announced: u32,
        preimages_forgotten: u32,
        assurer_avg_latency_ms: Option<f64>,
        assurer_latency_samples: u32,
        guarantor_avg_latency_ms: Option<f64>,
        guarantor_latency_samples: u32,
        active_shards: i32,
        assurer_hist: [u32; 14],
        assurer_hist_total: u32,
        assurer_hist_failed: u32,
        guarantor_hist: [u32; 14],
        guarantor_hist_total: u32,
        guarantor_hist_failed: u32,
    }

    let mut snapshots: Vec<Snapshot> = Vec::new();
    let mut stale_keys: Vec<Arc<str>> = Vec::new();

    for mut entry in tracker.iter_mut() {
        let node_id = entry.key().clone();
        let state = entry.value_mut();

        if state.dirty {
            let assurer_avg = if state.assurer_latency_count > 0 {
                Some(state.assurer_latency_sum_us as f64 / state.assurer_latency_count as f64 / 1000.0)
            } else {
                None
            };
            let guarantor_avg = if state.guarantor_latency_count > 0 {
                Some(state.guarantor_latency_sum_us as f64 / state.guarantor_latency_count as f64 / 1000.0)
            } else {
                None
            };

            snapshots.push(Snapshot {
                node_id: node_id.clone(),
                shard_requests_sent: state.shard_requests_sent,
                shard_requests_received: state.shard_requests_received,
                shard_sent_confirmed: state.shard_sent_confirmed,
                shard_received_confirmed: state.shard_received_confirmed,
                shards_transferred: state.shards_transferred,
                shard_failures: state.shard_failures,
                preimage_ann_failures: state.preimage_ann_failures,
                preimages_announced: state.preimages_announced,
                preimages_forgotten: state.preimages_forgotten,
                assurer_avg_latency_ms: assurer_avg,
                assurer_latency_samples: state.assurer_latency_count,
                guarantor_avg_latency_ms: guarantor_avg,
                guarantor_latency_samples: state.guarantor_latency_count,
                active_shards: state.active_shards.len() as i32,
                assurer_hist: state.assurer_hist,
                assurer_hist_total: state.assurer_hist_total,
                assurer_hist_failed: state.assurer_hist_failed,
                guarantor_hist: state.guarantor_hist,
                guarantor_hist_total: state.guarantor_hist_total,
                guarantor_hist_failed: state.guarantor_hist_failed,
            });

            // Phase 3: Reset counters and histograms
            state.shard_requests_sent = 0;
            state.shard_requests_received = 0;
            state.shard_sent_confirmed = 0;
            state.shard_received_confirmed = 0;
            state.shards_transferred = 0;
            state.shard_failures = 0;
            state.preimage_ann_failures = 0;
            state.preimages_announced = 0;
            state.preimages_forgotten = 0;
            state.assurer_latency_sum_us = 0;
            state.assurer_latency_count = 0;
            state.guarantor_latency_sum_us = 0;
            state.guarantor_latency_count = 0;
            state.assurer_hist = [0; 14];
            state.assurer_hist_total = 0;
            state.assurer_hist_failed = 0;
            state.guarantor_hist = [0; 14];
            state.guarantor_hist_total = 0;
            state.guarantor_hist_failed = 0;
            state.active_shards.clear();
            state.dirty = false;

            // Evict stale pending entries (keep pending maps across flushes)
            let now_jce_approx = {
                let unix_us = Utc::now().timestamp_micros();
                (unix_us - JCE_EPOCH_UNIX_MICROS) as u64
            };
            evict_pending(&mut state.assurer_pending, now_jce_approx);
            evict_pending(&mut state.guarantor_pending, now_jce_approx);
        }

        // Collect stale entries for eviction
        if now.duration_since(state.last_activity) > STALE_TTL && !state.dirty {
            stale_keys.push(node_id.clone());
        }
    }

    // Phase 2: DB writes
    let ts = Utc::now();

    for snap in &snapshots {
        // da_node_stats
        let result = sqlx::query(
            "INSERT INTO da_node_stats (ts, node_id, shard_requests_sent, shard_requests_received, \
             shard_sent_confirmed, shard_received_confirmed, shards_transferred, shard_failures, \
             preimage_ann_failures, preimages_announced, preimages_forgotten, \
             assurer_avg_latency_ms, assurer_latency_samples, \
             guarantor_avg_latency_ms, guarantor_latency_samples, \
             active_shards) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)"
        )
        .bind(ts)
        .bind(snap.node_id.as_ref())
        .bind(snap.shard_requests_sent as i32)
        .bind(snap.shard_requests_received as i32)
        .bind(snap.shard_sent_confirmed as i32)
        .bind(snap.shard_received_confirmed as i32)
        .bind(snap.shards_transferred as i32)
        .bind(snap.shard_failures as i32)
        .bind(snap.preimage_ann_failures as i32)
        .bind(snap.preimages_announced as i32)
        .bind(snap.preimages_forgotten as i32)
        .bind(snap.assurer_avg_latency_ms)
        .bind(snap.assurer_latency_samples as i32)
        .bind(snap.guarantor_avg_latency_ms)
        .bind(snap.guarantor_latency_samples as i32)
        .bind(snap.active_shards)
        .execute(pool)
        .await;

        match result {
            Ok(_) => rows_written += 1,
            Err(e) => warn!("da_node_stats insert failed for {}: {e}", snap.node_id),
        }

        // shard_latency_hist — assurer side
        if snap.assurer_hist_total > 0 {
            let h = &snap.assurer_hist;
            let result = sqlx::query(
                "INSERT INTO shard_latency_hist (ts, node_id, side, \
                 b_0_1, b_1_2, b_2_5, b_5_10, b_10_25, b_25_50, b_50_100, \
                 b_100_250, b_250_500, b_500_1000, b_1000_2000, b_2000_3000, b_3000_5000, b_5000_plus, \
                 total_count, failed_count) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)"
            )
            .bind(ts)
            .bind(snap.node_id.as_ref())
            .bind(0i16)
            .bind(h[0] as i32)
            .bind(h[1] as i32)
            .bind(h[2] as i32)
            .bind(h[3] as i32)
            .bind(h[4] as i32)
            .bind(h[5] as i32)
            .bind(h[6] as i32)
            .bind(h[7] as i32)
            .bind(h[8] as i32)
            .bind(h[9] as i32)
            .bind(h[10] as i32)
            .bind(h[11] as i32)
            .bind(h[12] as i32)
            .bind(h[13] as i32)
            .bind(snap.assurer_hist_total as i32)
            .bind(snap.assurer_hist_failed as i32)
            .execute(pool)
            .await;

            match result {
                Ok(_) => hist_rows_written += 1,
                Err(e) => warn!("shard_latency_hist (assurer) insert failed for {}: {e}", snap.node_id),
            }
        }

        // shard_latency_hist — guarantor side
        if snap.guarantor_hist_total > 0 {
            let h = &snap.guarantor_hist;
            let result = sqlx::query(
                "INSERT INTO shard_latency_hist (ts, node_id, side, \
                 b_0_1, b_1_2, b_2_5, b_5_10, b_10_25, b_25_50, b_50_100, \
                 b_100_250, b_250_500, b_500_1000, b_1000_2000, b_2000_3000, b_3000_5000, b_5000_plus, \
                 total_count, failed_count) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)"
            )
            .bind(ts)
            .bind(snap.node_id.as_ref())
            .bind(1i16)
            .bind(h[0] as i32)
            .bind(h[1] as i32)
            .bind(h[2] as i32)
            .bind(h[3] as i32)
            .bind(h[4] as i32)
            .bind(h[5] as i32)
            .bind(h[6] as i32)
            .bind(h[7] as i32)
            .bind(h[8] as i32)
            .bind(h[9] as i32)
            .bind(h[10] as i32)
            .bind(h[11] as i32)
            .bind(h[12] as i32)
            .bind(h[13] as i32)
            .bind(snap.guarantor_hist_total as i32)
            .bind(snap.guarantor_hist_failed as i32)
            .execute(pool)
            .await;

            match result {
                Ok(_) => hist_rows_written += 1,
                Err(e) => warn!("shard_latency_hist (guarantor) insert failed for {}: {e}", snap.node_id),
            }
        }
    }

    // Evict stale DaNodeState entries
    for key in &stale_keys {
        tracker.remove(key.as_ref());
        nodes_evicted += 1;
    }

    if rows_written > 0 || !stale_keys.is_empty() {
        debug!(
            "da_tracker flush: {} node_stats rows, {} hist rows, {} nodes evicted, {} tracked",
            rows_written,
            hist_rows_written,
            nodes_evicted,
            tracker.len(),
        );
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hist_bucket_index() {
        assert_eq!(hist_bucket_index(0), 0);    // [0,1)
        assert_eq!(hist_bucket_index(1), 1);    // [1,2)
        assert_eq!(hist_bucket_index(4), 2);    // [2,5)
        assert_eq!(hist_bucket_index(5), 3);    // [5,10)
        assert_eq!(hist_bucket_index(47), 5);   // [25,50)
        assert_eq!(hist_bucket_index(3001), 12); // [3000,5000)
        assert_eq!(hist_bucket_index(6000), 13); // [5000,∞)
        assert_eq!(hist_bucket_index(-5), 0);   // negative → clamped to 0
    }

    #[test]
    fn test_evict_pending() {
        let mut pending = HashMap::new();
        let now_us = 100_000_000u64; // 100 seconds in micros

        // Fresh entry — should survive
        pending.insert(1, now_us - 1_000_000); // 1s ago
        // Stale entry — should be evicted (>30s)
        pending.insert(2, now_us - 31_000_000); // 31s ago
        // Another stale
        pending.insert(3, now_us - 50_000_000); // 50s ago
        // Fresh
        pending.insert(4, now_us - 5_000_000); // 5s ago

        let evicted = evict_pending(&mut pending, now_us);
        assert_eq!(evicted, 2);
        assert_eq!(pending.len(), 2);
        assert!(pending.contains_key(&1));
        assert!(pending.contains_key(&4));
        assert!(!pending.contains_key(&2));
        assert!(!pending.contains_key(&3));
    }

    #[test]
    fn test_assurer_latency_computation() {
        let mut state = DaNodeState::default();

        // Simulate SendingShardRequest(120) at t=1000000 (1s JCE)
        let event_id = 42u64;
        let start_ts = 1_000_000u64;
        state.assurer_pending.insert(event_id, start_ts);

        // Simulate ShardsTransferred(125) completing at t=1050000 (1.05s JCE)
        let end_ts = 1_050_000u64;
        if let Some(start) = state.assurer_pending.remove(&event_id) {
            let delta_us = end_ts.saturating_sub(start);
            state.assurer_latency_sum_us += delta_us;
            state.assurer_latency_count += 1;
            let delta_ms = (delta_us / 1000) as i32;
            let bucket = hist_bucket_index(delta_ms);
            state.assurer_hist[bucket] += 1;
            state.assurer_hist_total += 1;
        }

        assert_eq!(state.assurer_latency_count, 1);
        assert_eq!(state.assurer_latency_sum_us, 50_000); // 50ms in micros
        let avg_ms = state.assurer_latency_sum_us as f64 / state.assurer_latency_count as f64 / 1000.0;
        assert!((avg_ms - 50.0).abs() < 0.001);
        // 50ms → bucket 6 [50,100)
        assert_eq!(state.assurer_hist[6], 1);
        assert_eq!(state.assurer_hist_total, 1);
    }

    #[test]
    fn test_guarantor_latency_computation() {
        let mut state = DaNodeState::default();

        // Simulate ReceivingShardRequest(121) at t=2000000 (2s JCE)
        let event_id = 99u64;
        let start_ts = 2_000_000u64;
        state.guarantor_pending.insert(event_id, start_ts);

        // Simulate ShardRequestReceived(124) completing at t=2005000 (2.005s JCE → 5ms delta)
        let end_ts = 2_005_000u64;
        if let Some(start) = state.guarantor_pending.remove(&event_id) {
            let delta_us = end_ts.saturating_sub(start);
            state.guarantor_latency_sum_us += delta_us;
            state.guarantor_latency_count += 1;
            let delta_ms = (delta_us / 1000) as i32;
            let bucket = hist_bucket_index(delta_ms);
            state.guarantor_hist[bucket] += 1;
            state.guarantor_hist_total += 1;
        }

        assert_eq!(state.guarantor_latency_count, 1);
        assert_eq!(state.guarantor_latency_sum_us, 5_000); // 5ms in micros
        let avg_ms = state.guarantor_latency_sum_us as f64 / state.guarantor_latency_count as f64 / 1000.0;
        assert!((avg_ms - 5.0).abs() < 0.001);
        // 5ms → bucket 3 [5,10)
        assert_eq!(state.guarantor_hist[3], 1);
        assert_eq!(state.guarantor_hist_total, 1);
    }

    #[test]
    fn negative_delta_clamping() {
        // Negative delta should be clamped to bucket 0 [0,1)
        assert_eq!(hist_bucket_index(-5), 0);
    }

    #[test]
    fn cap_enforcement() {
        let mut state = DaNodeState::default();
        // Insert 20001 entries into assurer_pending
        let now_us = 100_000_000u64;
        for i in 0..20_001u64 {
            state.assurer_pending.insert(i, now_us - 1_000); // all fresh (1ms ago)
        }
        assert_eq!(state.assurer_pending.len(), 20_001);
        evict_pending(&mut state.assurer_pending, now_us);
        // After eviction, count should be capped at PENDING_CAP (20000)
        assert!(state.assurer_pending.len() <= 20_000);
    }

    #[test]
    fn hist_bucket_boundary_5ms() {
        // Exactly 5ms should go to bucket index 3 [5,10), not index 2 [2,5)
        assert_eq!(hist_bucket_index(5), 3);
    }
}
