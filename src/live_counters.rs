//! Per-second sliding-window counters for real-time API endpoints.
//!
//! Replaces two slow SQL queries (2.7s and 3.2s) that scan raw events.
//! Updated by the MetricsTracker task on every event via cheap atomic operations.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering::Relaxed};

/// Ring buffer of 60 per-second buckets + slot tracking.
/// All operations are lock-free atomic — safe to read from API handlers
/// while the MetricsTracker task writes.
pub struct LiveCounters {
    seconds: [SecondBucket; 60],
    latest_slot: AtomicU32,
    finalized_slot: AtomicU32,
}

struct SecondBucket {
    /// Which unix second this bucket represents. Used to detect rollover.
    epoch_second: AtomicU64,
    events: AtomicU64,
    /// event_type = 11 (BestBlockChanged)
    blocks: AtomicU64,
    /// event_type = 12 (FinalizedBlockChanged)
    finalized: AtomicU64,
    /// event_type = 62 (BlockAnnounced)
    announcements: AtomicU64,
    /// event_type IN (80, 82, 84)
    tickets: AtomicU64,
}

impl SecondBucket {
    const fn new() -> Self {
        Self {
            epoch_second: AtomicU64::new(0),
            events: AtomicU64::new(0),
            blocks: AtomicU64::new(0),
            finalized: AtomicU64::new(0),
            announcements: AtomicU64::new(0),
            tickets: AtomicU64::new(0),
        }
    }

    fn reset(&self, new_second: u64) {
        self.events.store(0, Relaxed);
        self.blocks.store(0, Relaxed);
        self.finalized.store(0, Relaxed);
        self.announcements.store(0, Relaxed);
        self.tickets.store(0, Relaxed);
        // Store epoch_second last — readers check this to validate bucket freshness
        self.epoch_second.store(new_second, Relaxed);
    }

    fn read(&self) -> BucketSnapshot {
        BucketSnapshot {
            epoch_second: self.epoch_second.load(Relaxed),
            events: self.events.load(Relaxed),
            blocks: self.blocks.load(Relaxed),
            finalized: self.finalized.load(Relaxed),
            announcements: self.announcements.load(Relaxed),
            tickets: self.tickets.load(Relaxed),
        }
    }
}

/// Immutable snapshot of a single second's counters.
#[derive(Debug, Clone, Default)]
pub struct BucketSnapshot {
    pub epoch_second: u64,
    pub events: u64,
    pub blocks: u64,
    pub finalized: u64,
    pub announcements: u64,
    pub tickets: u64,
}

/// Aggregated counters over a time window.
#[derive(Debug, Clone, Default)]
pub struct WindowSum {
    pub events: u64,
    pub blocks: u64,
    pub finalized: u64,
    pub announcements: u64,
    pub tickets: u64,
}

// Cannot derive Default because of the array of SecondBucket (no Copy/Default for atomics)
impl LiveCounters {
    #[allow(clippy::declare_interior_mutable_const)]
    pub fn new() -> Self {
        // const initializer needed for array repeat syntax with non-Copy types
        const BUCKET_INIT: SecondBucket = SecondBucket::new();
        Self {
            seconds: [BUCKET_INIT; 60],
            latest_slot: AtomicU32::new(0),
            finalized_slot: AtomicU32::new(0),
        }
    }

    /// Record one event. Called by MetricsTracker task for every event.
    pub fn record(&self, now_secs: u64, event_type: u8, slot: Option<u32>) {
        let idx = (now_secs % 60) as usize;
        let bucket = &self.seconds[idx];

        // If this bucket belongs to a different second, reset it
        if bucket.epoch_second.load(Relaxed) != now_secs {
            bucket.reset(now_secs);
        }

        bucket.events.fetch_add(1, Relaxed);

        match event_type {
            11 => {
                bucket.blocks.fetch_add(1, Relaxed);
                if let Some(s) = slot {
                    self.latest_slot.fetch_max(s, Relaxed);
                }
            }
            12 => {
                bucket.finalized.fetch_add(1, Relaxed);
                if let Some(s) = slot {
                    self.finalized_slot.fetch_max(s, Relaxed);
                }
            }
            62 => {
                bucket.announcements.fetch_add(1, Relaxed);
            }
            80 | 82 | 84 => {
                bucket.tickets.fetch_add(1, Relaxed);
            }
            _ => {}
        }
    }

    /// Sum counters for the last `n` seconds (excluding current partial second).
    pub fn sum_last_n_seconds(&self, n: u64) -> WindowSum {
        let now = current_epoch_second();
        let mut sum = WindowSum::default();

        // Sum seconds [now-n, now-1] — skip current second (partial)
        for offset in 1..=n {
            let target_second = now.wrapping_sub(offset);
            let idx = (target_second % 60) as usize;
            let snap = self.seconds[idx].read();

            if snap.epoch_second == target_second {
                sum.events += snap.events;
                sum.blocks += snap.blocks;
                sum.finalized += snap.finalized;
                sum.announcements += snap.announcements;
                sum.tickets += snap.tickets;
            }
        }
        sum
    }

    /// Get per-second history for the last `n` seconds, ordered newest first.
    pub fn per_second_history(&self, n: u64) -> Vec<BucketSnapshot> {
        let now = current_epoch_second();
        let mut result = Vec::with_capacity(n as usize);

        for offset in 1..=n {
            let target_second = now.wrapping_sub(offset);
            let idx = (target_second % 60) as usize;
            let snap = self.seconds[idx].read();

            if snap.epoch_second == target_second {
                result.push(snap);
            } else {
                // No data for this second — emit zeros
                result.push(BucketSnapshot {
                    epoch_second: target_second,
                    ..Default::default()
                });
            }
        }
        result
    }

    pub fn latest_slot(&self) -> u32 {
        self.latest_slot.load(Relaxed)
    }

    pub fn finalized_slot(&self) -> u32 {
        self.finalized_slot.load(Relaxed)
    }

    /// Build JSON matching the existing `get_live_counters` response format.
    pub fn build_live_snapshot(
        &self,
        last_10s: &WindowSum,
        last_1m: &WindowSum,
        active_nodes: usize,
    ) -> serde_json::Value {
        serde_json::json!({
            "timestamp": chrono::Utc::now(),
            "latest_slot": self.latest_slot(),
            "finalized_slot": self.finalized_slot(),
            "active_nodes": active_nodes,
            "last_10s": {
                "events": last_10s.events,
                "blocks": last_10s.blocks,
                "finalized": last_10s.finalized,
                "events_per_second": last_10s.events as f64 / 10.0,
                "blocks_per_second": last_10s.blocks as f64 / 10.0,
            },
            "last_1m": {
                "events": last_1m.events,
                "blocks": last_1m.blocks,
                "events_per_second": last_1m.events as f64 / 60.0,
                "blocks_per_second": last_1m.blocks as f64 / 60.0,
            },
        })
    }

    /// Build JSON matching the existing `get_realtime_metrics` response format.
    /// `totals` is the fast aggregate query result (2ms), passed through from store.
    pub fn build_realtime_snapshot(
        &self,
        window_seconds: i32,
        per_second: &[BucketSnapshot],
        active_nodes: usize,
    ) -> serde_json::Value {
        let total_events: u64 = per_second.iter().map(|s| s.events).sum();
        let total_blocks: u64 = per_second.iter().map(|s| s.blocks).sum();

        let data: Vec<serde_json::Value> = per_second
            .iter()
            .map(|s| {
                serde_json::json!({
                    "timestamp": chrono::DateTime::from_timestamp(s.epoch_second as i64, 0),
                    "events": s.events,
                    "nodes": 0,  // not tracked per-second in LiveCounters
                    "blocks": s.blocks,
                    "finalized": s.finalized,
                    "announcements": s.announcements,
                    "tickets": s.tickets,
                })
            })
            .collect();

        serde_json::json!({
            "window_seconds": window_seconds,
            "timestamp": chrono::Utc::now(),
            "totals": {
                "events": total_events,
                "best_blocks": total_blocks,
                "finalized_blocks": per_second.iter().map(|s| s.finalized).sum::<u64>(),
                "authored_blocks": 0,  // not tracked in LiveCounters
                "announcements": per_second.iter().map(|s| s.announcements).sum::<u64>(),
                "active_nodes": active_nodes,
                "latest_slot": self.latest_slot(),
            },
            "rates": {
                "events_per_second": total_events as f64 / window_seconds as f64,
                "blocks_per_second": total_blocks as f64 / window_seconds as f64,
            },
            "data": data,
        })
    }
}

impl Default for LiveCounters {
    fn default() -> Self {
        Self::new()
    }
}

fn current_epoch_second() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_increments() {
        let lc = LiveCounters::new();
        let now = current_epoch_second();

        lc.record(now, 1, None); // generic event
        lc.record(now, 11, Some(100)); // BestBlockChanged
        lc.record(now, 12, Some(99)); // FinalizedBlockChanged
        lc.record(now, 62, None); // BlockAnnounced
        lc.record(now, 80, None); // ticket

        let idx = (now % 60) as usize;
        let snap = lc.seconds[idx].read();
        assert_eq!(snap.events, 5);
        assert_eq!(snap.blocks, 1);
        assert_eq!(snap.finalized, 1);
        assert_eq!(snap.announcements, 1);
        assert_eq!(snap.tickets, 1);
        assert_eq!(lc.latest_slot(), 100);
        assert_eq!(lc.finalized_slot(), 99);
    }

    #[test]
    fn test_second_rollover() {
        let lc = LiveCounters::new();
        let now = current_epoch_second();

        // Fill second N
        lc.record(now, 1, None);
        lc.record(now, 1, None);

        let idx = (now % 60) as usize;
        assert_eq!(lc.seconds[idx].read().events, 2);

        // Advance to N+1 in the SAME bucket (60 seconds later)
        let future = now + 60;
        lc.record(future, 1, None);

        let snap = lc.seconds[idx].read();
        assert_eq!(snap.epoch_second, future);
        assert_eq!(snap.events, 1); // reset + 1 new
    }

    #[test]
    fn test_sum_last_n_seconds() {
        let lc = LiveCounters::new();
        let now = current_epoch_second();

        // Fill 15 seconds of data (1 event per second in the past)
        for i in 1..=15 {
            lc.record(now - i, 1, None);
        }

        let sum_10 = lc.sum_last_n_seconds(10);
        assert_eq!(sum_10.events, 10);

        let sum_15 = lc.sum_last_n_seconds(15);
        assert_eq!(sum_15.events, 15);

        let sum_60 = lc.sum_last_n_seconds(60);
        assert_eq!(sum_60.events, 15); // only 15 seconds have data
    }

    #[test]
    fn test_slot_tracking() {
        let lc = LiveCounters::new();
        let now = current_epoch_second();

        lc.record(now, 11, Some(100));
        lc.record(now, 11, Some(105));
        lc.record(now, 11, Some(103)); // lower — should not decrease

        assert_eq!(lc.latest_slot(), 105);

        lc.record(now, 12, Some(99));
        lc.record(now, 12, Some(102));
        assert_eq!(lc.finalized_slot(), 102);
    }

    #[test]
    fn test_per_second_history() {
        let lc = LiveCounters::new();
        let now = current_epoch_second();

        // Fill 5 seconds with increasing event counts
        for i in 1..=5u64 {
            for _ in 0..i {
                lc.record(now - i, 1, None);
            }
        }

        let history = lc.per_second_history(5);
        assert_eq!(history.len(), 5);
        // newest first: offset 1 has 1 event, offset 2 has 2, etc.
        assert_eq!(history[0].events, 1);
        assert_eq!(history[1].events, 2);
        assert_eq!(history[4].events, 5);
    }

    #[test]
    fn test_snapshot_json_format() {
        let lc = LiveCounters::new();
        let now = current_epoch_second();

        for _ in 0..100 {
            lc.record(now - 1, 1, None);
        }
        lc.record(now - 1, 11, Some(42));
        lc.record(now - 1, 12, Some(40));

        let last_10s = lc.sum_last_n_seconds(10);
        let last_1m = lc.sum_last_n_seconds(60);
        let json = lc.build_live_snapshot(&last_10s, &last_1m, 859);

        assert_eq!(json["active_nodes"], 859);
        assert_eq!(json["latest_slot"], 42);
        assert_eq!(json["finalized_slot"], 40);
        assert_eq!(json["last_10s"]["events"], 102); // 100 generic + 1 block + 1 finalized
        assert_eq!(json["last_1m"]["events"], 102);
        assert!(json["timestamp"].is_string());
    }
}
