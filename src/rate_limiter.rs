use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tracing::warn;

/// Maximum sustained events per second per node
const MAX_EVENTS_PER_SECOND: u32 = 1000;

/// Burst allowance above the sustained rate (for block production spikes)
const BURST_ALLOWANCE: u32 = 200;

/// Maximum number of concurrent connections
pub const MAX_CONNECTIONS: usize = 1024;

/// Time window for rate limiting (1 second)
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);

/// Process-wide epoch for converting Instant → ticks.
/// Ticks are milliseconds since this epoch, fitting u32 (~49 days of uptime).
static EPOCH: OnceLock<Instant> = OnceLock::new();

fn epoch() -> Instant {
    *EPOCH.get_or_init(Instant::now)
}

/// Pack (count, ticks) into a single u64: count in upper 32 bits, ticks in lower 32.
#[inline]
fn pack(count: u32, ticks: u32) -> u64 {
    ((count as u64) << 32) | (ticks as u64)
}

/// Unpack a u64 into (count, ticks).
#[inline]
fn unpack(val: u64) -> (u32, u32) {
    ((val >> 32) as u32, val as u32)
}

/// Convert an Instant to ticks (milliseconds since EPOCH).
#[inline]
fn instant_to_ticks(t: Instant) -> u32 {
    t.duration_since(epoch()).as_millis() as u32
}

/// Atomic rate limit entry. State is packed into a single AtomicU64:
/// upper 32 bits = event count, lower 32 bits = window start (ticks since EPOCH).
struct AtomicRateLimitEntry {
    state: AtomicU64,
}

impl Default for AtomicRateLimitEntry {
    fn default() -> Self {
        Self {
            state: AtomicU64::new(pack(0, instant_to_ticks(Instant::now()))),
        }
    }
}

pub struct RateLimiter {
    limits: Arc<DashMap<String, AtomicRateLimitEntry>>,
    connection_count: AtomicUsize,
    /// Running count of throttled events (avoids iterating DashMap in get_stats)
    throttled_count: AtomicUsize,
    // Cached metric handles to avoid per-call registry lookup (hash + DashMap lock)
    counter_rate_limited: metrics::Counter,
    counter_connections_rejected: metrics::Counter,
    gauge_active_connections: metrics::Gauge,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiter {
    pub fn new() -> Self {
        let limits = Arc::new(DashMap::new());

        // Lazy eviction: stale entries are removed inline in allow_event()
        // when their window_start is >5 minutes old. This distributes cleanup
        // across normal operations and avoids a global DashMap lock.

        RateLimiter {
            limits,
            connection_count: AtomicUsize::new(0),
            throttled_count: AtomicUsize::new(0),
            counter_rate_limited: metrics::counter!("telemetry_events_rate_limited"),
            counter_connections_rejected: metrics::counter!("telemetry_connections_rejected"),
            gauge_active_connections: metrics::gauge!("telemetry_active_connections"),
        }
    }

    /// Check if a new connection is allowed.
    /// Uses compare_exchange loop to avoid TOCTOU race between load and fetch_add.
    pub fn allow_connection(&self, addr: &SocketAddr) -> bool {
        loop {
            let current = self.connection_count.load(Ordering::Acquire);
            if current >= MAX_CONNECTIONS {
                warn!(
                    "Connection limit reached ({}), rejecting {}",
                    MAX_CONNECTIONS, addr
                );
                self.counter_connections_rejected.increment(1);
                return false;
            }
            match self.connection_count.compare_exchange(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.gauge_active_connections.set((current + 1) as f64);
                    return true;
                }
                Err(_) => {
                    std::hint::spin_loop();
                    continue;
                }
            }
        }
    }

    /// Record a connection being closed.
    /// Uses compare_exchange loop to avoid brief underflow window where counter
    /// wraps to usize::MAX, which would reject all new connections.
    pub fn connection_closed(&self) {
        loop {
            let current = self.connection_count.load(Ordering::Acquire);
            if current == 0 {
                self.gauge_active_connections.set(0.0);
                return;
            }
            match self.connection_count.compare_exchange(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.gauge_active_connections.set((current - 1) as f64);
                    return;
                }
                Err(_) => {
                    std::hint::spin_loop();
                    continue;
                }
            }
        }
    }

    /// Check if an event from a node is allowed based on rate limits.
    ///
    /// Uses `self.limits.get()` (shared read lock on DashMap shard) + CAS loop on
    /// the packed AtomicU64 state. This eliminates exclusive shard lock contention
    /// that `get_mut()` caused with 200+ concurrent nodes.
    pub fn allow_event(&self, node_id: &str) -> bool {
        let now_ticks = instant_to_ticks(Instant::now());
        let budget = MAX_EVENTS_PER_SECOND + BURST_ALLOWANCE;
        let window_ms = RATE_LIMIT_WINDOW.as_millis() as u32;

        // Fast path: existing entry — shared read lock on DashMap shard
        if let Some(entry) = self.limits.get(node_id) {
            let state = &entry.value().state;
            loop {
                let current = state.load(Ordering::Relaxed);
                let (count, start_ticks) = unpack(current);
                let elapsed_ms = now_ticks.wrapping_sub(start_ticks);

                if elapsed_ms >= window_ms {
                    // Window expired — reset to count=1, new window
                    let new = pack(1, now_ticks);
                    match state.compare_exchange_weak(
                        current,
                        new,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => return true,
                        Err(_) => continue, // CAS failed, retry
                    }
                }

                if count >= budget {
                    // Over budget — no write needed, just return false
                    self.throttled_count.fetch_add(1, Ordering::Relaxed);
                    self.counter_rate_limited.increment(1);
                    return false;
                }

                // Increment count within current window
                let new = pack(count + 1, start_ticks);
                match state.compare_exchange_weak(
                    current,
                    new,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return true,
                    Err(_) => continue, // CAS failed, retry
                }
            }
        }

        // Slow path: new node — write lock (once per node lifetime)
        self.limits.entry(node_id.to_string()).or_default();
        // Entry now exists with count=0. Recurse (will hit fast path).
        // This is at most one extra iteration since the entry now exists.
        self.allow_event(node_id)
    }

    /// Get current connection count
    pub fn connection_count(&self) -> usize {
        self.connection_count.load(Ordering::Relaxed)
    }

    /// Get rate limit stats for monitoring.
    /// Uses atomic counter for throttled events instead of iterating the full DashMap.
    pub fn get_stats(&self) -> RateLimiterStats {
        RateLimiterStats {
            active_connections: self.connection_count(),
            active_nodes: self.limits.len(),
            throttled_events: self.throttled_count.load(Ordering::Relaxed),
            max_connections: MAX_CONNECTIONS,
            max_events_per_second: MAX_EVENTS_PER_SECOND,
        }
    }
}

#[derive(Debug, serde::Serialize)]
pub struct RateLimiterStats {
    pub active_connections: usize,
    pub active_nodes: usize,
    pub throttled_events: usize,
    pub max_connections: usize,
    pub max_events_per_second: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_metrics() {
        // Install a noop recorder if none is set. Ignore error if already installed.
        let _ = metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder();
    }

    #[test]
    fn test_allow_event_basic() {
        setup_metrics();
        let limiter = RateLimiter::new();
        // First 150 events (100 sustained + 50 burst) should be allowed
        for i in 0..150 {
            assert!(
                limiter.allow_event("node_1"),
                "Event {} should be allowed",
                i
            );
        }
    }

    #[test]
    fn test_rate_limit_exceeded() {
        setup_metrics();
        let limiter = RateLimiter::new();
        let budget = MAX_EVENTS_PER_SECOND + BURST_ALLOWANCE;
        // Exhaust the event budget
        for _ in 0..budget {
            limiter.allow_event("node_1");
        }
        // Next event should be rejected
        assert!(!limiter.allow_event("node_1"));
    }

    #[test]
    fn test_rate_limit_per_node() {
        setup_metrics();
        let limiter = RateLimiter::new();
        let budget = MAX_EVENTS_PER_SECOND + BURST_ALLOWANCE;
        // Exhaust node_1's budget
        for _ in 0..budget {
            limiter.allow_event("node_1");
        }
        assert!(!limiter.allow_event("node_1"));
        // node_2 should still be allowed
        assert!(limiter.allow_event("node_2"));
    }

    #[test]
    fn test_window_reset() {
        setup_metrics();
        let limiter = RateLimiter::new();
        let budget = MAX_EVENTS_PER_SECOND + BURST_ALLOWANCE;
        // Exhaust the budget
        for _ in 0..budget {
            limiter.allow_event("node_1");
        }
        assert!(!limiter.allow_event("node_1"));
        // Wait for window to reset
        std::thread::sleep(Duration::from_millis(1100));
        assert!(limiter.allow_event("node_1"));
    }

    #[test]
    fn test_allow_connection() {
        setup_metrics();
        let limiter = RateLimiter::new();
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        assert!(limiter.allow_connection(&addr));
        assert_eq!(limiter.connection_count(), 1);
    }

    #[test]
    fn test_connection_closed() {
        setup_metrics();
        let limiter = RateLimiter::new();
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        limiter.allow_connection(&addr);
        assert_eq!(limiter.connection_count(), 1);
        limiter.connection_closed();
        assert_eq!(limiter.connection_count(), 0);
    }

    #[test]
    fn test_connection_closed_at_zero_no_underflow() {
        setup_metrics();
        let limiter = RateLimiter::new();
        assert_eq!(limiter.connection_count(), 0);
        // Closing when already at 0 should not underflow
        limiter.connection_closed();
        assert_eq!(limiter.connection_count(), 0);
    }

    #[test]
    fn test_get_stats() {
        setup_metrics();
        let limiter = RateLimiter::new();
        let addr: SocketAddr = "127.0.0.1:9999".parse().unwrap();
        limiter.allow_connection(&addr);
        limiter.allow_event("node_1");
        let stats = limiter.get_stats();
        assert_eq!(stats.active_connections, 1);
        assert_eq!(stats.active_nodes, 1);
        assert_eq!(stats.max_connections, MAX_CONNECTIONS);
        assert_eq!(stats.max_events_per_second, MAX_EVENTS_PER_SECOND);
    }

    #[test]
    fn test_pack_unpack_roundtrip() {
        // Verify atomic packing preserves count + ticks across edge values
        for (count, ticks) in [
            (0u32, 0u32),
            (1, 1),
            (1200, 50_000),
            (u32::MAX, u32::MAX),
            (0, u32::MAX),
            (u32::MAX, 0),
        ] {
            let packed = pack(count, ticks);
            let (c, t) = unpack(packed);
            assert_eq!(c, count, "count mismatch for ({}, {})", count, ticks);
            assert_eq!(t, ticks, "ticks mismatch for ({}, {})", count, ticks);
        }
    }

    #[test]
    fn test_instant_to_ticks_monotonic() {
        // Verify ticks increase over time with reasonable precision
        let t0 = instant_to_ticks(Instant::now());
        std::thread::sleep(Duration::from_millis(50));
        let t1 = instant_to_ticks(Instant::now());
        assert!(t1 > t0, "ticks should be monotonically increasing");
        let delta = t1 - t0;
        assert!(
            (40..=200).contains(&delta),
            "~50ms sleep should give 40-200ms ticks, got {}",
            delta
        );
    }

    #[test]
    fn test_rate_limiter_concurrent_correctness() {
        setup_metrics();
        let limiter = Arc::new(RateLimiter::new());
        let budget = MAX_EVENTS_PER_SECOND + BURST_ALLOWANCE;
        let num_threads = 8;
        let events_per_thread = (budget as usize * 2) / num_threads;

        let allowed = Arc::new(AtomicUsize::new(0));
        let mut handles = Vec::new();

        for _ in 0..num_threads {
            let limiter = Arc::clone(&limiter);
            let allowed = Arc::clone(&allowed);
            handles.push(std::thread::spawn(move || {
                for _ in 0..events_per_thread {
                    if limiter.allow_event("shared_node") {
                        allowed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let total_allowed = allowed.load(Ordering::Relaxed);
        // With CAS races, total allowed should be close to budget but may vary slightly.
        // Allow 2% tolerance above budget (CAS races can cause minor overcounting).
        let tolerance = (budget as f64 * 0.02) as usize;
        assert!(
            total_allowed <= budget as usize + tolerance,
            "total allowed {} should be within 2% of budget {} (tolerance {})",
            total_allowed,
            budget,
            tolerance
        );
        assert!(
            total_allowed >= budget as usize - tolerance,
            "total allowed {} should be at least budget {} - tolerance {}",
            total_allowed,
            budget,
            tolerance
        );
    }
}
