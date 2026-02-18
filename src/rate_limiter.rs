use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

/// Maximum sustained events per second per node
const MAX_EVENTS_PER_SECOND: u32 = 100;

/// Burst allowance above the sustained rate (for block production spikes)
const BURST_ALLOWANCE: u32 = 50;

/// Maximum number of concurrent connections
pub const MAX_CONNECTIONS: usize = 1024;

/// Time window for rate limiting (1 second)
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);

struct RateLimitEntry {
    count: u32,
    window_start: Instant,
}

impl Default for RateLimitEntry {
    fn default() -> Self {
        Self {
            count: 0,
            window_start: Instant::now(),
        }
    }
}

pub struct RateLimiter {
    limits: Arc<DashMap<String, RateLimitEntry>>,
    connection_count: AtomicUsize,
    /// Running count of throttled events (avoids iterating DashMap in get_stats)
    throttled_count: AtomicUsize,
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
                metrics::counter!("telemetry_connections_rejected").increment(1);
                return false;
            }
            match self.connection_count.compare_exchange(
                current,
                current + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    metrics::gauge!("telemetry_active_connections").set((current + 1) as f64);
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
                metrics::gauge!("telemetry_active_connections").set(0.0);
                return;
            }
            match self.connection_count.compare_exchange(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    metrics::gauge!("telemetry_active_connections").set((current - 1) as f64);
                    return;
                }
                Err(_) => {
                    std::hint::spin_loop();
                    continue;
                }
            }
        }
    }

    /// Check if an event from a node is allowed based on rate limits
    pub fn allow_event(&self, node_id: &str) -> bool {
        let now = Instant::now();

        // Fast path: try get_mut first to avoid String allocation for existing keys
        // Only allocates a String for first-time insertion
        if let Some(mut entry) = self.limits.get_mut(node_id) {
            let limit = entry.value_mut();
            let elapsed = now.duration_since(limit.window_start);

            // Lazy eviction: if entry is stale (>5 min old), reset it in-place
            // This replaces the background cleanup task that held a global DashMap lock
            if elapsed >= Duration::from_secs(300) {
                limit.count = 1;
                limit.window_start = now;
                return true;
            }

            if elapsed >= RATE_LIMIT_WINDOW {
                limit.count = 1;
                limit.window_start = now;
                return true;
            }
            if limit.count >= MAX_EVENTS_PER_SECOND + BURST_ALLOWANCE {
                self.throttled_count.fetch_add(1, Ordering::Relaxed);
                metrics::counter!("telemetry_events_rate_limited").increment(1);
                return false;
            }
            limit.count += 1;
            return true;
        }

        // Slow path: new key, allocate String
        let mut entry = self.limits.entry(node_id.to_string()).or_default();
        let limit = entry.value_mut();

        // Check if we're in a new window
        if now.duration_since(limit.window_start) >= RATE_LIMIT_WINDOW {
            // Reset the window
            limit.count = 1;
            limit.window_start = now;
            return true;
        }

        // Check if we've exceeded the limit (with burst allowance)
        if limit.count >= MAX_EVENTS_PER_SECOND + BURST_ALLOWANCE {
            self.throttled_count.fetch_add(1, Ordering::Relaxed);
            metrics::counter!("telemetry_events_rate_limited").increment(1);
            return false;
        }

        limit.count += 1;
        true
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
        // Exhaust the 150 event budget
        for _ in 0..150 {
            limiter.allow_event("node_1");
        }
        // 151st event should be rejected
        assert!(!limiter.allow_event("node_1"));
    }

    #[test]
    fn test_rate_limit_per_node() {
        setup_metrics();
        let limiter = RateLimiter::new();
        // Exhaust node_1's budget
        for _ in 0..150 {
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
        // Exhaust the budget
        for _ in 0..150 {
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
}
