use parking_lot::RwLock;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::warn;

/// Maximum number of events per second per node
const MAX_EVENTS_PER_SECOND: u32 = 100;

/// Maximum number of concurrent connections
const MAX_CONNECTIONS: usize = 1024;

/// Time window for rate limiting (1 second)
const RATE_LIMIT_WINDOW: Duration = Duration::from_secs(1);

/// How often to clean up old entries
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

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
    limits: Arc<RwLock<HashMap<String, RateLimitEntry>>>,
    connection_count: Arc<RwLock<usize>>,
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

impl RateLimiter {
    pub fn new() -> Self {
        let limits = Arc::new(RwLock::new(HashMap::new()));
        let connection_count = Arc::new(RwLock::new(0));

        // Spawn cleanup task
        let limits_clone = limits.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
            loop {
                interval.tick().await;
                cleanup_old_entries(&limits_clone);
            }
        });

        RateLimiter {
            limits,
            connection_count,
        }
    }

    /// Check if a new connection is allowed
    pub fn allow_connection(&self, addr: &SocketAddr) -> bool {
        let mut count = self.connection_count.write();
        if *count >= MAX_CONNECTIONS {
            warn!(
                "Connection limit reached ({}), rejecting {}",
                MAX_CONNECTIONS, addr
            );
            metrics::counter!("telemetry_connections_rejected").increment(1);
            return false;
        }
        *count += 1;
        metrics::gauge!("telemetry_active_connections").set(*count as f64);
        true
    }

    /// Record a connection being closed
    pub fn connection_closed(&self) {
        let mut count = self.connection_count.write();
        if *count > 0 {
            *count -= 1;
        }
        metrics::gauge!("telemetry_active_connections").set(*count as f64);
    }

    /// Check if an event from a node is allowed based on rate limits
    pub fn allow_event(&self, node_id: &str) -> bool {
        let now = Instant::now();
        let mut limits = self.limits.write();

        let entry = limits.entry(node_id.to_string()).or_default();

        // Check if we're in a new window
        if now.duration_since(entry.window_start) >= RATE_LIMIT_WINDOW {
            // Reset the window
            entry.count = 1;
            entry.window_start = now;
            return true;
        }

        // Check if we've exceeded the limit
        if entry.count >= MAX_EVENTS_PER_SECOND {
            metrics::counter!("telemetry_events_rate_limited").increment(1);
            return false;
        }

        entry.count += 1;
        true
    }

    /// Get current connection count
    pub fn connection_count(&self) -> usize {
        *self.connection_count.read()
    }

    /// Get rate limit stats for monitoring
    pub fn get_stats(&self) -> RateLimiterStats {
        let limits = self.limits.read();
        let active_nodes = limits.len();
        let throttled_nodes = limits
            .values()
            .filter(|entry| entry.count >= MAX_EVENTS_PER_SECOND)
            .count();

        RateLimiterStats {
            active_connections: self.connection_count(),
            active_nodes,
            throttled_nodes,
            max_connections: MAX_CONNECTIONS,
            max_events_per_second: MAX_EVENTS_PER_SECOND,
        }
    }
}

fn cleanup_old_entries(limits: &Arc<RwLock<HashMap<String, RateLimitEntry>>>) {
    let now = Instant::now();
    let mut limits = limits.write();

    // Remove entries that haven't been updated in 5 minutes
    limits.retain(|_, entry| now.duration_since(entry.window_start) < Duration::from_secs(300));
}

#[derive(Debug, serde::Serialize)]
pub struct RateLimiterStats {
    pub active_connections: usize,
    pub active_nodes: usize,
    pub throttled_nodes: usize,
    pub max_connections: usize,
    pub max_events_per_second: u32,
}
