//! DA latency tracker — histogram-based latency tracking for bundle reconstruction,
//! segment fetching, and preimage transfers.
//!
//! Follows the same pattern as `da_tracker.rs`: in-memory per-node state with pending
//! maps for start→end event correlation, 23-bucket histograms (CONVERGENCE_BOUNDS),
//! and periodic flush to `*_latency_hist` hypertables.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::Utc;
use dashmap::DashMap;
use sqlx::PgPool;
use tracing::{debug, warn};

use crate::histogram::{convergence_bucket_index, CONVERGENCE_BUCKET_COUNT, CONVERGENCE_HIST_COLUMNS};
use crate::types::JCE_EPOCH_UNIX_MICROS;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

pub type DaLatencyTracker = Arc<DashMap<Arc<str>, DaLatencyNodeState>>;

pub fn new_da_latency_tracker() -> DaLatencyTracker {
    Arc::new(DashMap::new())
}

// ---------------------------------------------------------------------------
// LatencyFlow — reusable building block for start→end correlation
// ---------------------------------------------------------------------------

const PENDING_TTL_US: u64 = 30_000_000; // 30 seconds
const PENDING_CAP: usize = 20_000;

/// A single latency tracking flow: pending map + histogram + counters.
///
/// Used for each start→end event pair (e.g. SendingBundleShardRequest → BundleShardTransferred).
pub struct LatencyFlow {
    /// Correlation key → start timestamp (JCE microseconds).
    pending: HashMap<u64, u64>,
    /// 23-bucket histogram matching CONVERGENCE_BOUNDS.
    hist: [u32; CONVERGENCE_BUCKET_COUNT],
    /// Total completed measurements (success + failure).
    pub total: u32,
    /// Failed measurements (subset of total).
    pub failed: u32,
    /// Running sum of latencies in microseconds.
    latency_sum_us: u64,
    /// Number of latency samples (= total, but tracked separately for avg computation).
    latency_count: u32,
}

/// Snapshot of a LatencyFlow for flushing to DB.
pub struct LatencyFlowSnapshot {
    pub hist: [u32; CONVERGENCE_BUCKET_COUNT],
    pub total: u32,
    pub failed: u32,
    pub avg_latency_ms: Option<f64>,
    pub latency_samples: u32,
}

impl Default for LatencyFlow {
    fn default() -> Self {
        Self {
            pending: HashMap::new(),
            hist: [0; CONVERGENCE_BUCKET_COUNT],
            total: 0,
            failed: 0,
            latency_sum_us: 0,
            latency_count: 0,
        }
    }
}

impl LatencyFlow {
    /// Record start of a flow. Inserts (key, timestamp) into pending.
    pub fn start(&mut self, key: u64, ts: u64) {
        self.pending.insert(key, ts);
    }

    /// Record start only if key is not already pending (first-event-wins).
    /// Used for e2e flows where multiple start events share the same correlation key.
    pub fn start_if_absent(&mut self, key: u64, ts: u64) {
        self.pending.entry(key).or_insert(ts);
    }

    /// Record successful completion. Returns delta_us if the key was found in pending.
    pub fn complete(&mut self, key: u64, ts: u64) -> Option<u64> {
        let start_ts = self.pending.remove(&key)?;
        let delta_us = ts.saturating_sub(start_ts);
        let delta_ms = (delta_us / 1000) as i32;
        self.latency_sum_us += delta_us;
        self.latency_count += 1;
        self.total += 1;
        let idx = convergence_bucket_index(delta_ms);
        self.hist[idx] += 1;
        Some(delta_us)
    }

    /// Record a failure. Computes latency (if key found) and increments failed counter.
    pub fn fail(&mut self, key: u64, ts: u64) -> Option<u64> {
        let start_ts = self.pending.remove(&key)?;
        let delta_us = ts.saturating_sub(start_ts);
        let delta_ms = (delta_us / 1000) as i32;
        self.latency_sum_us += delta_us;
        self.latency_count += 1;
        self.total += 1;
        self.failed += 1;
        let idx = convergence_bucket_index(delta_ms);
        self.hist[idx] += 1;
        Some(delta_us)
    }

    /// Snapshot current state and reset counters. Pending map is NOT cleared.
    pub fn snapshot_and_reset(&mut self) -> LatencyFlowSnapshot {
        let avg = if self.latency_count > 0 {
            Some(self.latency_sum_us as f64 / self.latency_count as f64 / 1000.0)
        } else {
            None
        };
        let snap = LatencyFlowSnapshot {
            hist: self.hist,
            total: self.total,
            failed: self.failed,
            avg_latency_ms: avg,
            latency_samples: self.latency_count,
        };
        self.hist = [0; CONVERGENCE_BUCKET_COUNT];
        self.total = 0;
        self.failed = 0;
        self.latency_sum_us = 0;
        self.latency_count = 0;
        snap
    }

    /// Evict stale pending entries older than PENDING_TTL_US. Returns count evicted.
    pub fn evict_stale(&mut self, now_us: u64) -> usize {
        let before = self.pending.len();
        self.pending.retain(|_, &mut ts| now_us.saturating_sub(ts) < PENDING_TTL_US);
        let after = self.pending.len();
        // Cap enforcement
        if after > PENDING_CAP {
            let to_remove = after - PENDING_CAP;
            let keys: Vec<u64> = self.pending.keys().take(to_remove).copied().collect();
            for k in keys {
                self.pending.remove(&k);
            }
        }
        before - self.pending.len().min(before)
    }

    /// True if no pending entries and no accumulated data.
    pub fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.total == 0
    }

    /// True if there is accumulated data to flush.
    pub fn has_data(&self) -> bool {
        self.total > 0
    }
}

// ---------------------------------------------------------------------------
// Per-node state
// ---------------------------------------------------------------------------

pub struct DaLatencyNodeState {
    // -- Bundle reconstruction (140-153) --
    /// Requestor shard path: SendingBundleShardRequest(140) → BundleShardTransferred(145)
    pub bundle_shard_req: LatencyFlow,
    /// Responder shard path: ReceivingBundleShardRequest(141) → BundleShardTransferred(145)
    pub bundle_shard_resp: LatencyFlow,
    /// Requestor full bundle: SendingBundleRequest(148) → BundleTransferred(153)
    pub bundle_full_req: LatencyFlow,
    /// Responder full bundle: ReceivingBundleRequest(149) → BundleTransferred(153)
    pub bundle_full_resp: LatencyFlow,
    /// Reconstruction CPU time: ReconstructingBundle(146) → BundleReconstructed(147)
    pub bundle_reconstruct: LatencyFlow,
    /// End-to-end recovery: first SendingBundleShardRequest(140) → BundleReconstructed(147) per audit_id
    pub bundle_e2e: LatencyFlow,
    pub bundle_trivial: u32,
    pub bundle_nontrivial: u32,

    // -- Segment fetching (160-178) --
    /// Requestor shard path: SendingSegmentShardRequest(162) → SegmentShardsTransferred(167)
    pub seg_shard_req: LatencyFlow,
    /// Responder shard path: ReceivingSegmentShardRequest(163) → SegmentShardsTransferred(167)
    pub seg_shard_resp: LatencyFlow,
    /// Requestor full segment: SendingSegmentRequest(173) → SegmentsTransferred(178)
    pub seg_full_req: LatencyFlow,
    /// Responder full segment: ReceivingSegmentRequest(174) → SegmentsTransferred(178)
    pub seg_full_resp: LatencyFlow,
    /// Reconstruction CPU time: ReconstructingSegments(168) → SegmentsReconstructed(170)
    pub seg_reconstruct: LatencyFlow,
    pub seg_trivial: u32,
    pub seg_nontrivial: u32,
    pub seg_verification_failures: u32,

    // -- Preimage transfers (190-199) --
    /// Requestor: SendingPreimageRequest(193) → PreimageTransferred(198)
    pub preimage_req: LatencyFlow,
    /// Responder: ReceivingPreimageRequest(194) → PreimageTransferred(198)
    pub preimage_resp: LatencyFlow,

    // Flush control
    pub dirty: bool,
    pub last_activity: Instant,
}

impl Default for DaLatencyNodeState {
    fn default() -> Self {
        Self {
            bundle_shard_req: LatencyFlow::default(),
            bundle_shard_resp: LatencyFlow::default(),
            bundle_full_req: LatencyFlow::default(),
            bundle_full_resp: LatencyFlow::default(),
            bundle_reconstruct: LatencyFlow::default(),
            bundle_e2e: LatencyFlow::default(),
            bundle_trivial: 0,
            bundle_nontrivial: 0,
            seg_shard_req: LatencyFlow::default(),
            seg_shard_resp: LatencyFlow::default(),
            seg_full_req: LatencyFlow::default(),
            seg_full_resp: LatencyFlow::default(),
            seg_reconstruct: LatencyFlow::default(),
            seg_trivial: 0,
            seg_nontrivial: 0,
            seg_verification_failures: 0,
            preimage_req: LatencyFlow::default(),
            preimage_resp: LatencyFlow::default(),
            dirty: false,
            last_activity: Instant::now(),
        }
    }
}

// ---------------------------------------------------------------------------
// Flush
// ---------------------------------------------------------------------------

const STALE_TTL: Duration = Duration::from_secs(60);

/// Write a single histogram row to the given table.
async fn write_hist_row(
    pool: &PgPool,
    table: &str,
    ts: chrono::DateTime<Utc>,
    node_id: &str,
    side: i16,
    snap: &LatencyFlowSnapshot,
) -> Result<(), sqlx::Error> {
    let h = &snap.hist;
    // Build column list dynamically from CONVERGENCE_HIST_COLUMNS
    let cols = CONVERGENCE_HIST_COLUMNS.join(", ");
    let sql = format!(
        "INSERT INTO {table} (ts, node_id, side, {cols}, total_count, failed_count) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, \
                 $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)"
    );
    sqlx::query(&sql)
        .bind(ts)
        .bind(node_id)
        .bind(side)
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
        .bind(h[14] as i32)
        .bind(h[15] as i32)
        .bind(h[16] as i32)
        .bind(h[17] as i32)
        .bind(h[18] as i32)
        .bind(h[19] as i32)
        .bind(h[20] as i32)
        .bind(h[21] as i32)
        .bind(h[22] as i32)
        .bind(snap.total as i32)
        .bind(snap.failed as i32)
        .execute(pool)
        .await?;
    Ok(())
}

struct FlowToFlush {
    table: &'static str,
    side: i16,
    snap: LatencyFlowSnapshot,
}

pub async fn flush_da_latency_tracker(tracker: &DaLatencyTracker, pool: &PgPool) {
    let now = Instant::now();
    let mut total_rows = 0u32;

    // Phase 1: Snapshot dirty entries
    struct NodeSnapshot {
        node_id: Arc<str>,
        flows: Vec<FlowToFlush>,
    }

    let mut snapshots: Vec<NodeSnapshot> = Vec::new();
    let mut stale_keys: Vec<Arc<str>> = Vec::new();

    for mut entry in tracker.iter_mut() {
        let node_id = entry.key().clone();
        let state = entry.value_mut();

        if state.dirty {
            let mut flows = Vec::new();

            // Bundle flows
            let snap = state.bundle_shard_req.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "bundle_latency_hist", side: 0, snap }); }
            let snap = state.bundle_shard_resp.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "bundle_latency_hist", side: 1, snap }); }
            let snap = state.bundle_full_req.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "bundle_latency_hist", side: 2, snap }); }
            let snap = state.bundle_full_resp.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "bundle_latency_hist", side: 3, snap }); }
            let snap = state.bundle_reconstruct.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "bundle_latency_hist", side: 4, snap }); }
            let snap = state.bundle_e2e.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "bundle_latency_hist", side: 5, snap }); }

            // Segment flows
            let snap = state.seg_shard_req.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "segment_latency_hist", side: 0, snap }); }
            let snap = state.seg_shard_resp.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "segment_latency_hist", side: 1, snap }); }
            let snap = state.seg_full_req.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "segment_latency_hist", side: 2, snap }); }
            let snap = state.seg_full_resp.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "segment_latency_hist", side: 3, snap }); }
            let snap = state.seg_reconstruct.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "segment_latency_hist", side: 4, snap }); }

            // Preimage flows
            let snap = state.preimage_req.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "preimage_latency_hist", side: 0, snap }); }
            let snap = state.preimage_resp.snapshot_and_reset();
            if snap.total > 0 { flows.push(FlowToFlush { table: "preimage_latency_hist", side: 1, snap }); }

            // Reset counters
            state.bundle_trivial = 0;
            state.bundle_nontrivial = 0;
            state.seg_trivial = 0;
            state.seg_nontrivial = 0;
            state.seg_verification_failures = 0;
            state.dirty = false;

            // Evict stale pending entries
            let now_jce_approx = {
                let unix_us = Utc::now().timestamp_micros();
                (unix_us - JCE_EPOCH_UNIX_MICROS) as u64
            };
            state.bundle_shard_req.evict_stale(now_jce_approx);
            state.bundle_shard_resp.evict_stale(now_jce_approx);
            state.bundle_full_req.evict_stale(now_jce_approx);
            state.bundle_full_resp.evict_stale(now_jce_approx);
            state.bundle_reconstruct.evict_stale(now_jce_approx);
            state.bundle_e2e.evict_stale(now_jce_approx);
            state.seg_shard_req.evict_stale(now_jce_approx);
            state.seg_shard_resp.evict_stale(now_jce_approx);
            state.seg_full_req.evict_stale(now_jce_approx);
            state.seg_full_resp.evict_stale(now_jce_approx);
            state.seg_reconstruct.evict_stale(now_jce_approx);
            state.preimage_req.evict_stale(now_jce_approx);
            state.preimage_resp.evict_stale(now_jce_approx);

            if !flows.is_empty() {
                snapshots.push(NodeSnapshot { node_id: node_id.clone(), flows });
            }
        }

        // Collect stale entries for eviction
        if now.duration_since(state.last_activity) > STALE_TTL && !state.dirty {
            stale_keys.push(node_id.clone());
        }
    }

    // Phase 2: DB writes
    let ts = Utc::now();

    for node_snap in &snapshots {
        for flow in &node_snap.flows {
            match write_hist_row(pool, flow.table, ts, node_snap.node_id.as_ref(), flow.side, &flow.snap).await {
                Ok(_) => total_rows += 1,
                Err(e) => warn!(
                    "{} (side={}) insert failed for {}: {e}",
                    flow.table, flow.side, node_snap.node_id
                ),
            }
        }
    }

    // Evict stale nodes
    let mut nodes_evicted = 0u32;
    for key in &stale_keys {
        tracker.remove(key.as_ref());
        nodes_evicted += 1;
    }

    if total_rows > 0 || nodes_evicted > 0 {
        debug!(
            "da_latency_tracker flush: {} hist rows, {} nodes evicted, {} tracked",
            total_rows, nodes_evicted, tracker.len(),
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
    fn latency_flow_start_complete() {
        let mut flow = LatencyFlow::default();
        flow.start(100, 1_000_000); // event_id=100, ts=1s in micros
        assert_eq!(flow.pending.len(), 1);

        let delta = flow.complete(100, 1_500_000); // 500ms later
        assert_eq!(delta, Some(500_000));
        assert_eq!(flow.total, 1);
        assert_eq!(flow.failed, 0);
        assert_eq!(flow.latency_count, 1);
        assert_eq!(flow.pending.len(), 0);
        // 500ms → bucket [500,1000) = index 12 (CONVERGENCE_BOUNDS)
        assert_eq!(flow.hist[12], 1);
    }

    #[test]
    fn latency_flow_start_fail() {
        let mut flow = LatencyFlow::default();
        flow.start(100, 1_000_000);

        let delta = flow.fail(100, 1_050_000); // 50ms later
        assert_eq!(delta, Some(50_000));
        assert_eq!(flow.total, 1);
        assert_eq!(flow.failed, 1);
        assert_eq!(flow.pending.len(), 0);
        // 50ms → bucket [50,75) = index 7
        assert_eq!(flow.hist[7], 1);
    }

    #[test]
    fn latency_flow_complete_missing_key() {
        let mut flow = LatencyFlow::default();
        let delta = flow.complete(999, 1_000_000);
        assert_eq!(delta, None);
        assert_eq!(flow.total, 0);
    }

    #[test]
    fn latency_flow_start_if_absent() {
        let mut flow = LatencyFlow::default();
        flow.start_if_absent(100, 1_000_000);
        flow.start_if_absent(100, 2_000_000); // should NOT overwrite

        let delta = flow.complete(100, 1_500_000);
        assert_eq!(delta, Some(500_000)); // uses first timestamp, not second
    }

    #[test]
    fn latency_flow_snapshot_and_reset() {
        let mut flow = LatencyFlow::default();
        flow.start(1, 0);
        flow.complete(1, 5_000); // 5ms → bucket [5,10) = index 2
        flow.start(2, 0);
        flow.fail(2, 100_000); // 100ms → bucket [100,150) = index 9

        let snap = flow.snapshot_and_reset();
        assert_eq!(snap.total, 2);
        assert_eq!(snap.failed, 1);
        assert_eq!(snap.latency_samples, 2);
        assert!(snap.avg_latency_ms.is_some());
        assert_eq!(snap.hist[2], 1);  // 5ms bucket
        assert_eq!(snap.hist[9], 1);  // 100ms bucket

        // After reset
        assert_eq!(flow.total, 0);
        assert_eq!(flow.failed, 0);
        assert_eq!(flow.latency_count, 0);
        assert_eq!(flow.hist, [0; CONVERGENCE_BUCKET_COUNT]);
    }

    #[test]
    fn latency_flow_evict_stale() {
        let mut flow = LatencyFlow::default();
        let now_us = 100_000_000u64; // 100s

        // Insert an old entry (> 30s ago) and a fresh one
        flow.start(1, now_us - 40_000_000); // 40s ago → stale
        flow.start(2, now_us - 10_000_000); // 10s ago → fresh

        let evicted = flow.evict_stale(now_us);
        assert_eq!(evicted, 1);
        assert_eq!(flow.pending.len(), 1);
        assert!(flow.pending.contains_key(&2));
    }

    #[test]
    fn latency_flow_is_empty() {
        let flow = LatencyFlow::default();
        assert!(flow.is_empty());

        let mut flow2 = LatencyFlow::default();
        flow2.start(1, 0);
        assert!(!flow2.is_empty());
    }

    #[test]
    fn latency_flow_has_data() {
        let mut flow = LatencyFlow::default();
        assert!(!flow.has_data());

        flow.start(1, 0);
        assert!(!flow.has_data()); // pending only, no completed

        flow.complete(1, 1000);
        assert!(flow.has_data());
    }

    #[test]
    fn latency_flow_dual_pending_resolve() {
        // Simulates how failure events (e.g. 142) try both req and resp pending maps
        let mut req_flow = LatencyFlow::default();
        let mut resp_flow = LatencyFlow::default();

        req_flow.start(100, 1_000_000);
        resp_flow.start(200, 2_000_000);

        // Failure with request_id=100 → found in req, not resp
        let delta = req_flow.fail(100, 1_100_000)
            .or_else(|| resp_flow.fail(100, 1_100_000));
        assert_eq!(delta, Some(100_000));
        assert_eq!(req_flow.failed, 1);
        assert_eq!(resp_flow.failed, 0);

        // Failure with request_id=200 → not in req, found in resp
        let delta = req_flow.fail(200, 2_200_000)
            .or_else(|| resp_flow.fail(200, 2_200_000));
        assert_eq!(delta, Some(200_000));
        assert_eq!(req_flow.failed, 1);
        assert_eq!(resp_flow.failed, 1);
    }
}
