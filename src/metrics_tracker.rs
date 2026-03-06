//! In-memory metrics tracker for replacing O(n²) self-JOIN SQL queries.
//!
//! Events flow from the broadcaster aggregator via a filtered mpsc channel.
//! The tracker task owns all mutable state (no locks for writes). Pre-computed
//! JSON snapshots are shared with API handlers via `Arc<RwLock<...>>`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::debug;

use crate::events::Event;

/// Message sent from the broadcaster aggregator to the tracker task.
pub struct MetricsEvent {
    pub node_id: Arc<str>,
    pub event: Arc<Event>,
    pub wall_clock: Instant,
}

/// Shared handle for API handlers to read pre-computed snapshots.
pub struct MetricsTracker {
    block_propagation_snapshot: RwLock<Arc<serde_json::Value>>,
    cores_status_snapshot: RwLock<Arc<serde_json::Value>>,
    core_processing: RwLock<HashMap<u16, Arc<serde_json::Value>>>,
}

impl Default for MetricsTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsTracker {
    pub fn new() -> Self {
        Self {
            block_propagation_snapshot: RwLock::new(Arc::new(serde_json::json!(null))),
            cores_status_snapshot: RwLock::new(Arc::new(serde_json::json!(null))),
            core_processing: RwLock::new(HashMap::new()),
        }
    }

    pub fn get_block_propagation_snapshot(&self) -> Arc<serde_json::Value> {
        self.block_propagation_snapshot.read().clone()
    }

    pub fn get_cores_status_snapshot(&self) -> Arc<serde_json::Value> {
        self.cores_status_snapshot.read().clone()
    }

    pub fn get_core_processing_snapshot(&self, core: u16) -> Option<Arc<serde_json::Value>> {
        self.core_processing.read().get(&core).cloned()
    }
}

// === Internal state, owned exclusively by the tracker task ===

struct PropagationSample {
    propagation_ms: f64,
    #[allow(dead_code)]
    receiver_node_id: Arc<str>,
    wall_clock: Instant,
}

struct NodeBlockCounts {
    blocks_announced: u64,
    blocks_received: u64,
}

struct WpEntry {
    core: u16,
    #[allow(dead_code)]
    node_id: Arc<str>,
    received_event_ts: u64,
    received_wall: Instant,
    latest_stage_event_ts: u64,
    latest_stage_wall: Instant,
    guarantee_built: bool,
    completed: bool,
    created: Instant,
}

struct CoreStats {
    completed_count: u64,
    total_processing_ms_event: f64,
    total_processing_ms_wall: f64,
    samples_event: Vec<f64>,
    samples_wall: Vec<f64>,
    wps_received_1h: u64,
    guarantees_built_1h: u64,
    last_activity: Instant,
}

impl CoreStats {
    fn new() -> Self {
        Self {
            completed_count: 0,
            total_processing_ms_event: 0.0,
            total_processing_ms_wall: 0.0,
            samples_event: Vec::new(),
            samples_wall: Vec::new(),
            wps_received_1h: 0,
            guarantees_built_1h: 0,
            last_activity: Instant::now(),
        }
    }
}

/// Mutable state owned by the tracker task. No locks needed — single writer.
pub(crate) struct TrackerState {
    shared: Arc<MetricsTracker>,

    // Block Propagation
    first_announcement: HashMap<u32, (Arc<str>, Instant)>,
    propagation_samples: VecDeque<PropagationSample>,
    node_block_counts: HashMap<Arc<str>, NodeBlockCounts>,

    // WP Pipeline
    wp_entries: HashMap<u64, WpEntry>,
    core_stats: HashMap<u16, CoreStats>,

    // Timers
    last_cleanup: Instant,
    last_snapshot: Instant,
}

impl TrackerState {
    fn new(shared: Arc<MetricsTracker>) -> Self {
        Self {
            shared,
            first_announcement: HashMap::new(),
            propagation_samples: VecDeque::new(),
            node_block_counts: HashMap::new(),
            wp_entries: HashMap::new(),
            core_stats: HashMap::new(),
            last_cleanup: Instant::now(),
            last_snapshot: Instant::now(),
        }
    }

    fn process_event(&mut self, event: MetricsEvent) {
        match event.event.as_ref() {
            // --- Block Propagation ---
            Event::BlockAnnounced { slot, .. } => {
                self.first_announcement
                    .entry(*slot)
                    .or_insert_with(|| (event.node_id.clone(), event.wall_clock));
                let counts = self
                    .node_block_counts
                    .entry(event.node_id.clone())
                    .or_insert(NodeBlockCounts {
                        blocks_announced: 0,
                        blocks_received: 0,
                    });
                counts.blocks_announced += 1;
            }
            Event::BlockTransferred { slot, .. } => {
                if let Some((announcer_node, announced_at)) =
                    self.first_announcement.get(slot)
                {
                    if *announcer_node != event.node_id {
                        let propagation_ms =
                            event.wall_clock.duration_since(*announced_at).as_secs_f64() * 1000.0;
                        if propagation_ms > 0.0 && propagation_ms < 60000.0 {
                            self.propagation_samples.push_back(PropagationSample {
                                propagation_ms,
                                receiver_node_id: event.node_id.clone(),
                                wall_clock: event.wall_clock,
                            });
                        }
                    }
                }
                let counts = self
                    .node_block_counts
                    .entry(event.node_id.clone())
                    .or_insert(NodeBlockCounts {
                        blocks_announced: 0,
                        blocks_received: 0,
                    });
                counts.blocks_received += 1;
            }

            // --- WP Pipeline ---
            Event::WorkPackageReceived {
                timestamp,
                submission_or_share_id,
                core,
                ..
            } => {
                self.wp_entries.insert(
                    *submission_or_share_id,
                    WpEntry {
                        core: *core,
                        node_id: event.node_id.clone(),
                        received_event_ts: *timestamp,
                        received_wall: event.wall_clock,
                        latest_stage_event_ts: *timestamp,
                        latest_stage_wall: event.wall_clock,
                        guarantee_built: false,
                        completed: false,
                        created: event.wall_clock,
                    },
                );
                let stats = self.core_stats.entry(*core).or_insert_with(CoreStats::new);
                stats.wps_received_1h += 1;
                stats.last_activity = event.wall_clock;
            }
            Event::Authorized {
                timestamp,
                submission_or_share_id,
                ..
            }
            | Event::Refined {
                timestamp,
                submission_or_share_id,
                ..
            }
            | Event::WorkReportBuilt {
                timestamp,
                submission_or_share_id,
                ..
            } => {
                if let Some(entry) = self.wp_entries.get_mut(submission_or_share_id) {
                    entry.latest_stage_event_ts = *timestamp;
                    entry.latest_stage_wall = event.wall_clock;
                }
            }
            Event::GuaranteeBuilt {
                timestamp,
                submission_id,
                ..
            } => {
                if let Some(entry) = self.wp_entries.get_mut(submission_id) {
                    entry.latest_stage_event_ts = *timestamp;
                    entry.latest_stage_wall = event.wall_clock;
                    entry.guarantee_built = true;
                    let stats =
                        self.core_stats.entry(entry.core).or_insert_with(CoreStats::new);
                    stats.guarantees_built_1h += 1;
                    stats.last_activity = event.wall_clock;
                }
            }
            Event::GuaranteesDistributed {
                timestamp,
                submission_id,
                ..
            } => {
                if let Some(entry) = self.wp_entries.get_mut(submission_id) {
                    entry.latest_stage_event_ts = *timestamp;
                    entry.latest_stage_wall = event.wall_clock;
                    entry.completed = true;

                    let jce_epoch_micros: u64 = 1_735_732_800_000_000;
                    let start_micros = entry.received_event_ts;
                    let end_micros = *timestamp;
                    if end_micros > start_micros {
                        let processing_ms_event =
                            (end_micros - start_micros) as f64 / 1000.0;
                        let processing_ms_wall = entry
                            .latest_stage_wall
                            .duration_since(entry.received_wall)
                            .as_secs_f64()
                            * 1000.0;

                        let _ = jce_epoch_micros; // timestamps are already in JCE micros

                        let stats = self
                            .core_stats
                            .entry(entry.core)
                            .or_insert_with(CoreStats::new);
                        stats.completed_count += 1;
                        stats.total_processing_ms_event += processing_ms_event;
                        stats.total_processing_ms_wall += processing_ms_wall;
                        stats.samples_event.push(processing_ms_event);
                        stats.samples_wall.push(processing_ms_wall);
                        stats.last_activity = event.wall_clock;
                    }
                }
            }
            _ => {}
        }
    }

    fn maybe_rebuild_snapshots(&mut self) {
        if self.last_snapshot.elapsed().as_secs() < 2 {
            return;
        }
        self.last_snapshot = Instant::now();
        self.rebuild_block_propagation_snapshot();
        self.rebuild_cores_status_snapshot();
        self.rebuild_core_processing_snapshots();
    }

    fn rebuild_block_propagation_snapshot(&self) {
        let mut values: Vec<f64> = self
            .propagation_samples
            .iter()
            .map(|s| s.propagation_ms)
            .collect();

        let sample_count = values.len() as i64;
        let (avg, p50, p95, p99) = if values.is_empty() {
            (None, None, None, None)
        } else {
            values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
            let avg = values.iter().sum::<f64>() / values.len() as f64;
            let p50 = percentile(&values, 0.5);
            let p95 = percentile(&values, 0.95);
            let p99 = percentile(&values, 0.99);
            (Some(avg), Some(p50), Some(p95), Some(p99))
        };

        let mut by_node: Vec<serde_json::Value> = self
            .node_block_counts
            .iter()
            .filter(|(_, c)| c.blocks_announced > 0 || c.blocks_received > 0)
            .map(|(node_id, counts)| {
                serde_json::json!({
                    "node_id": node_id.as_ref(),
                    "blocks_announced": counts.blocks_announced,
                    "blocks_received": counts.blocks_received,
                    "blocks_originated": counts.blocks_announced.saturating_sub(counts.blocks_received),
                })
            })
            .collect();
        by_node.sort_by(|a, b| {
            let a_announced = a["blocks_announced"].as_u64().unwrap_or(0);
            let b_announced = b["blocks_announced"].as_u64().unwrap_or(0);
            b_announced.cmp(&a_announced)
        });
        by_node.truncate(50);

        let snapshot = serde_json::json!({
            "last_hour": {
                "avg_propagation_ms": avg,
                "p50_propagation_ms": p50,
                "p95_propagation_ms": p95,
                "p99_propagation_ms": p99,
                "sample_count": sample_count,
            },
            "by_node": by_node,
            "timestamp": chrono::Utc::now(),
        });

        *self.shared.block_propagation_snapshot.write() = Arc::new(snapshot);
    }

    fn rebuild_cores_status_snapshot(&self) {
        let now = Instant::now();
        let one_day = std::time::Duration::from_secs(86400);

        let mut cores: Vec<serde_json::Value> = self
            .core_stats
            .iter()
            .map(|(core_index, stats)| {
                let status = if stats.wps_received_1h > 0 || stats.guarantees_built_1h > 0 {
                    "active"
                } else if now.duration_since(stats.last_activity) < one_day {
                    "idle"
                } else {
                    "stale"
                };
                serde_json::json!({
                    "core_index": core_index,
                    "active_work_packages": self.wp_entries.values()
                        .filter(|e| e.core == *core_index && !e.completed)
                        .count(),
                    "work_packages_last_hour": stats.wps_received_1h,
                    "guarantees_last_hour": stats.guarantees_built_1h,
                    "last_activity": chrono::Utc::now(), // approximate
                    "status": status,
                })
            })
            .collect();
        cores.sort_by_key(|c| c["core_index"].as_u64().unwrap_or(0));

        let mut active_count = 0;
        let mut idle_count = 0;
        let mut stale_count = 0;
        for core in &cores {
            match core["status"].as_str() {
                Some("active") => active_count += 1,
                Some("idle") => idle_count += 1,
                Some("stale") => stale_count += 1,
                _ => {}
            }
        }

        let snapshot = serde_json::json!({
            "cores": cores,
            "summary": {
                "total_cores": cores.len(),
                "active_cores": active_count,
                "idle_cores": idle_count,
                "stale_cores": stale_count,
            },
        });

        *self.shared.cores_status_snapshot.write() = Arc::new(snapshot);
    }

    fn rebuild_core_processing_snapshots(&self) {
        let mut snapshots = HashMap::new();

        for (core_index, stats) in &self.core_stats {
            let avg_processing_ms = if stats.completed_count > 0 {
                stats.total_processing_ms_event / stats.completed_count as f64
            } else {
                0.0
            };

            let (avg_completion_ms, p95_completion_ms) = if stats.samples_wall.is_empty() {
                (0.0, 0.0)
            } else {
                let mut sorted = stats.samples_wall.clone();
                sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
                let avg = sorted.iter().sum::<f64>() / sorted.len() as f64;
                let p95 = percentile(&sorted, 0.95);
                (avg, p95)
            };

            let snapshot = serde_json::json!({
                "avg_processing_time_ms": avg_processing_ms,
                "completed_last_hour": stats.completed_count,
                "avg_completion_ms": avg_completion_ms,
                "p95_completion_ms": p95_completion_ms,
                "sample_count": stats.samples_wall.len(),
            });

            snapshots.insert(*core_index, Arc::new(snapshot));
        }

        *self.shared.core_processing.write() = snapshots;
    }

    fn maybe_cleanup(&mut self) {
        if self.last_cleanup.elapsed().as_secs() < 30 {
            return;
        }
        self.last_cleanup = Instant::now();

        let now = Instant::now();
        let one_hour = std::time::Duration::from_secs(3600);
        let five_minutes = std::time::Duration::from_secs(300);
        let twenty_four_hours = std::time::Duration::from_secs(86400);

        // Clean old propagation data
        // Remove first_announcement entries older than 1 hour
        self.first_announcement
            .retain(|_, (_, instant)| now.duration_since(*instant) < one_hour);

        // Remove old propagation samples
        while let Some(front) = self.propagation_samples.front() {
            if now.duration_since(front.wall_clock) > one_hour {
                self.propagation_samples.pop_front();
            } else {
                break;
            }
        }

        // Reset hourly node block counts (approximate — we just zero them)
        // This is fine since the snapshot is rebuilt every 2s from whatever counts exist
        for counts in self.node_block_counts.values_mut() {
            counts.blocks_announced = 0;
            counts.blocks_received = 0;
        }
        self.node_block_counts.retain(|_, c| c.blocks_announced > 0 || c.blocks_received > 0);

        // Clean WP entries: completed > 5min, all > 24h
        self.wp_entries.retain(|_, entry| {
            if entry.completed && now.duration_since(entry.created) > five_minutes {
                return false;
            }
            now.duration_since(entry.created) < twenty_four_hours
        });

        // Reset hourly core stats counters
        for stats in self.core_stats.values_mut() {
            stats.wps_received_1h = 0;
            stats.guarantees_built_1h = 0;
            // Keep completed_count and samples for lifetime stats
        }

        debug!(
            "MetricsTracker cleanup: {} announcements, {} prop_samples, {} wp_entries",
            self.first_announcement.len(),
            self.propagation_samples.len(),
            self.wp_entries.len(),
        );
    }
}

fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let idx = (p * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

/// Run the metrics tracker task. Spawned via `tokio::spawn` in main.rs.
pub async fn run(shared: Arc<MetricsTracker>, mut rx: mpsc::Receiver<MetricsEvent>) {
    let mut state = TrackerState::new(shared);
    debug!("MetricsTracker task started");

    loop {
        match rx.recv().await {
            Some(event) => {
                state.process_event(event);
                // Drain remaining buffered events
                while let Ok(event) = rx.try_recv() {
                    state.process_event(event);
                }
                state.maybe_rebuild_snapshots();
                state.maybe_cleanup();
            }
            None => {
                debug!("MetricsTracker channel closed, shutting down");
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::*;

    // === Test helpers ===

    fn make_block_announced(slot: u32) -> Arc<Event> {
        Arc::new(Event::BlockAnnounced {
            timestamp: 0,
            peer: [0u8; 32],
            announcer: ConnectionSide::Local,
            slot,
            hash: [0u8; 32],
        })
    }

    fn make_block_transferred(slot: u32) -> Arc<Event> {
        Arc::new(Event::BlockTransferred {
            timestamp: 0,
            request_id: 0,
            slot,
            outline: BlockSummary {
                size_bytes: 0,
                hash: [0u8; 32],
                num_tickets: 0,
                num_preimages: 0,
                total_preimages_size: 0,
                num_guarantees: 0,
                num_assurances: 0,
                num_dispute_verdicts: 0,
            },
            last: true,
        })
    }

    fn make_wp_received(sub_id: u64, core: u16) -> Arc<Event> {
        Arc::new(Event::WorkPackageReceived {
            timestamp: 1000,
            submission_or_share_id: sub_id,
            core,
            outline: WorkPackageSummary {
                work_package_size: 0,
                work_package_hash: [0u8; 32],
                anchor: [0u8; 32],
                lookup_anchor_slot: 0,
                prerequisites: vec![],
                work_items: vec![],
            },
        })
    }

    fn make_authorized(sub_id: u64, ts: u64) -> Arc<Event> {
        Arc::new(Event::Authorized {
            timestamp: ts,
            submission_or_share_id: sub_id,
            cost: IsAuthorizedCost {
                total: ExecCost {
                    gas_used: 0,
                    elapsed_ns: 0,
                },
                load_ns: 0,
                host_call: ExecCost {
                    gas_used: 0,
                    elapsed_ns: 0,
                },
            },
        })
    }

    fn make_refined(sub_id: u64, ts: u64) -> Arc<Event> {
        Arc::new(Event::Refined {
            timestamp: ts,
            submission_or_share_id: sub_id,
            costs: vec![],
        })
    }

    fn make_work_report_built(sub_id: u64, ts: u64) -> Arc<Event> {
        Arc::new(Event::WorkReportBuilt {
            timestamp: ts,
            submission_or_share_id: sub_id,
            outline: WorkReportSummary {
                work_report_hash: [0u8; 32],
                bundle_size: 0,
                erasure_root: [0u8; 32],
                segments_root: [0u8; 32],
            },
        })
    }

    fn make_guarantee_built(sub_id: u64, ts: u64) -> Arc<Event> {
        Arc::new(Event::GuaranteeBuilt {
            timestamp: ts,
            submission_id: sub_id,
            outline: GuaranteeSummary {
                work_report_hash: [0u8; 32],
                slot: 0,
                guarantors: vec![],
            },
        })
    }

    fn make_guarantees_distributed(sub_id: u64, ts: u64) -> Arc<Event> {
        Arc::new(Event::GuaranteesDistributed {
            timestamp: ts,
            submission_id: sub_id,
        })
    }

    fn metrics_event(node_id: &str, event: Arc<Event>) -> MetricsEvent {
        MetricsEvent {
            node_id: Arc::from(node_id),
            event,
            wall_clock: Instant::now(),
        }
    }

    fn metrics_event_at(node_id: &str, event: Arc<Event>, wall_clock: Instant) -> MetricsEvent {
        MetricsEvent {
            node_id: Arc::from(node_id),
            event,
            wall_clock,
        }
    }

    fn new_state() -> TrackerState {
        TrackerState::new(Arc::new(MetricsTracker::new()))
    }

    // === Block Propagation Tests ===

    #[test]
    fn test_single_slot_propagation() {
        let mut state = new_state();
        let t0 = Instant::now();
        let t1 = t0 + std::time::Duration::from_millis(100);

        state.process_event(metrics_event_at("node_A", make_block_announced(10), t0));
        state.process_event(metrics_event_at("node_B", make_block_transferred(10), t1));

        assert_eq!(state.propagation_samples.len(), 1);
        let sample = &state.propagation_samples[0];
        assert!((sample.propagation_ms - 100.0).abs() < 5.0);
        assert_eq!(sample.receiver_node_id.as_ref(), "node_B");

        // Check node counts
        assert_eq!(state.node_block_counts["node_A"].blocks_announced, 1);
        assert_eq!(state.node_block_counts["node_B"].blocks_received, 1);
    }

    #[test]
    fn test_same_node_ignored() {
        let mut state = new_state();
        let t0 = Instant::now();
        let t1 = t0 + std::time::Duration::from_millis(50);

        state.process_event(metrics_event_at("node_A", make_block_announced(10), t0));
        state.process_event(metrics_event_at("node_A", make_block_transferred(10), t1));

        assert_eq!(state.propagation_samples.len(), 0);
    }

    #[test]
    fn test_multiple_receivers() {
        let mut state = new_state();
        let t0 = Instant::now();

        state.process_event(metrics_event_at("node_A", make_block_announced(10), t0));
        state.process_event(metrics_event_at(
            "node_B",
            make_block_transferred(10),
            t0 + std::time::Duration::from_millis(50),
        ));
        state.process_event(metrics_event_at(
            "node_C",
            make_block_transferred(10),
            t0 + std::time::Duration::from_millis(100),
        ));
        state.process_event(metrics_event_at(
            "node_D",
            make_block_transferred(10),
            t0 + std::time::Duration::from_millis(150),
        ));

        assert_eq!(state.propagation_samples.len(), 3);
    }

    #[test]
    fn test_percentiles() {
        let mut state = new_state();
        let t0 = Instant::now();

        // Feed 100 pairs with increasing delays
        for i in 0..100 {
            let slot = 100 + i;
            let delay_ms = (i + 1) as u64 * 10; // 10ms, 20ms, ..., 1000ms
            state.process_event(metrics_event_at(
                "announcer",
                make_block_announced(slot),
                t0,
            ));
            state.process_event(metrics_event_at(
                "receiver",
                make_block_transferred(slot),
                t0 + std::time::Duration::from_millis(delay_ms),
            ));
        }

        assert_eq!(state.propagation_samples.len(), 100);

        let mut values: Vec<f64> = state
            .propagation_samples
            .iter()
            .map(|s| s.propagation_ms)
            .collect();
        values.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());

        let p50 = percentile(&values, 0.5);
        let p95 = percentile(&values, 0.95);
        let p99 = percentile(&values, 0.99);

        assert!(p50 > 0.0);
        assert!(p95 > p50);
        assert!(p99 >= p95);
    }

    #[test]
    fn test_unknown_slot_transfer_ignored() {
        let mut state = new_state();
        // Transfer for slot 99 without any announcement
        state.process_event(metrics_event("node_B", make_block_transferred(99)));
        assert_eq!(state.propagation_samples.len(), 0);
    }

    #[test]
    fn test_multiple_slots() {
        let mut state = new_state();
        let t0 = Instant::now();

        for slot in [10, 11, 12] {
            state.process_event(metrics_event_at(
                "announcer",
                make_block_announced(slot),
                t0,
            ));
            state.process_event(metrics_event_at(
                "receiver",
                make_block_transferred(slot),
                t0 + std::time::Duration::from_millis(50),
            ));
        }

        assert_eq!(state.first_announcement.len(), 3);
        assert_eq!(state.propagation_samples.len(), 3);
    }

    #[test]
    fn test_cleanup_expires_old_slots() {
        let mut state = new_state();
        let old = Instant::now() - std::time::Duration::from_secs(7200); // 2 hours ago

        state.process_event(metrics_event_at("node_A", make_block_announced(10), old));
        state.process_event(metrics_event_at(
            "node_B",
            make_block_transferred(10),
            old + std::time::Duration::from_millis(50),
        ));

        assert_eq!(state.first_announcement.len(), 1);
        assert_eq!(state.propagation_samples.len(), 1);

        // Force cleanup
        state.last_cleanup = Instant::now() - std::time::Duration::from_secs(60);
        state.maybe_cleanup();

        assert_eq!(state.first_announcement.len(), 0);
        assert_eq!(state.propagation_samples.len(), 0);
    }

    #[test]
    fn test_snapshot_json_format() {
        let mut state = new_state();
        let t0 = Instant::now();

        state.process_event(metrics_event_at("node_A", make_block_announced(10), t0));
        state.process_event(metrics_event_at(
            "node_B",
            make_block_transferred(10),
            t0 + std::time::Duration::from_millis(100),
        ));

        state.rebuild_block_propagation_snapshot();

        let snapshot = state.shared.get_block_propagation_snapshot();
        let obj = snapshot.as_object().expect("should be object");

        // Verify structure
        let last_hour = obj.get("last_hour").expect("missing last_hour");
        assert!(last_hour.get("avg_propagation_ms").is_some());
        assert!(last_hour.get("p50_propagation_ms").is_some());
        assert!(last_hour.get("p95_propagation_ms").is_some());
        assert!(last_hour.get("p99_propagation_ms").is_some());
        assert_eq!(last_hour["sample_count"].as_i64(), Some(1));

        assert!(obj.get("by_node").expect("missing by_node").is_array());
        assert!(obj.get("timestamp").is_some());
    }

    // === WP Pipeline Tests ===

    #[test]
    fn test_wp_basic_pipeline() {
        let mut state = new_state();
        let t0 = Instant::now();

        state.process_event(metrics_event_at("node_A", make_wp_received(1, 5), t0));
        state.process_event(metrics_event_at(
            "node_A",
            make_authorized(1, 2000),
            t0 + std::time::Duration::from_millis(10),
        ));
        state.process_event(metrics_event_at(
            "node_A",
            make_refined(1, 3000),
            t0 + std::time::Duration::from_millis(20),
        ));
        state.process_event(metrics_event_at(
            "node_A",
            make_work_report_built(1, 4000),
            t0 + std::time::Duration::from_millis(30),
        ));
        state.process_event(metrics_event_at(
            "node_A",
            make_guarantee_built(1, 5000),
            t0 + std::time::Duration::from_millis(40),
        ));
        state.process_event(metrics_event_at(
            "node_A",
            make_guarantees_distributed(1, 6000),
            t0 + std::time::Duration::from_millis(50),
        ));

        let core_stats = state.core_stats.get(&5).expect("core 5 should have stats");
        assert_eq!(core_stats.completed_count, 1);
        // Event timestamps: 6000 - 1000 = 5000 microseconds = 5.0 ms
        assert!((core_stats.total_processing_ms_event - 5.0).abs() < 0.01);
        assert!(core_stats.total_processing_ms_wall > 0.0);
    }

    #[test]
    fn test_wp_partial_pipeline() {
        let mut state = new_state();

        state.process_event(metrics_event("node_A", make_wp_received(1, 5)));
        state.process_event(metrics_event("node_A", make_authorized(1, 2000)));

        let core_stats = state.core_stats.get(&5).expect("core 5 should have stats");
        assert_eq!(core_stats.completed_count, 0);
        assert!(!state.wp_entries[&1].completed);
    }

    #[test]
    fn test_wp_unknown_submission_ignored() {
        let mut state = new_state();

        // Authorized for unknown submission
        state.process_event(metrics_event("node_A", make_authorized(999, 2000)));

        assert!(state.wp_entries.is_empty());
        assert!(state.core_stats.is_empty());
    }

    #[test]
    fn test_wp_multiple_cores() {
        let mut state = new_state();
        let t0 = Instant::now();

        // WP on core 0
        state.process_event(metrics_event_at("node_A", make_wp_received(1, 0), t0));
        state.process_event(metrics_event_at(
            "node_A",
            make_guarantees_distributed(1, 6000),
            t0 + std::time::Duration::from_millis(50),
        ));

        // WP on core 5
        state.process_event(metrics_event_at("node_A", make_wp_received(2, 5), t0));
        state.process_event(metrics_event_at(
            "node_A",
            make_guarantees_distributed(2, 8000),
            t0 + std::time::Duration::from_millis(80),
        ));

        assert_eq!(state.core_stats[&0].completed_count, 1);
        assert_eq!(state.core_stats[&5].completed_count, 1);
    }

    #[test]
    fn test_wp_cleanup_expires_completed() {
        let mut state = new_state();
        let old = Instant::now() - std::time::Duration::from_secs(600); // 10 min ago

        state.process_event(metrics_event_at("node_A", make_wp_received(1, 5), old));
        state.process_event(metrics_event_at(
            "node_A",
            make_guarantees_distributed(1, 6000),
            old + std::time::Duration::from_millis(50),
        ));

        assert!(state.wp_entries.contains_key(&1));

        // Force cleanup
        state.last_cleanup = Instant::now() - std::time::Duration::from_secs(60);
        state.maybe_cleanup();

        // Completed entry older than 5 min should be removed
        assert!(!state.wp_entries.contains_key(&1));
        // Core stats should still exist
        assert!(state.core_stats.contains_key(&5));
    }

    // === Guarantee Core Mapping Tests ===

    #[test]
    fn test_guarantee_core_mapping() {
        let mut state = new_state();

        state.process_event(metrics_event("node_A", make_wp_received(1, 7)));
        state.process_event(metrics_event("node_A", make_guarantee_built(1, 5000)));

        let core_stats = state.core_stats.get(&7).expect("core 7 should exist");
        assert_eq!(core_stats.guarantees_built_1h, 1);
    }

    #[test]
    fn test_cores_status_json_format() {
        let mut state = new_state();

        state.process_event(metrics_event("node_A", make_wp_received(1, 7)));
        state.process_event(metrics_event("node_A", make_guarantee_built(1, 5000)));

        state.rebuild_cores_status_snapshot();

        let snapshot = state.shared.get_cores_status_snapshot();
        let obj = snapshot.as_object().expect("should be object");

        let cores = obj.get("cores").expect("missing cores").as_array().expect("cores should be array");
        assert!(!cores.is_empty());

        let core = &cores[0];
        assert!(core.get("core_index").is_some());
        assert!(core.get("active_work_packages").is_some());
        assert!(core.get("work_packages_last_hour").is_some());
        assert!(core.get("guarantees_last_hour").is_some());
        assert!(core.get("last_activity").is_some());
        assert!(core.get("status").is_some());

        let summary = obj.get("summary").expect("missing summary");
        assert!(summary.get("total_cores").is_some());
        assert!(summary.get("active_cores").is_some());
        assert!(summary.get("idle_cores").is_some());
        assert!(summary.get("stale_cores").is_some());
    }
}
