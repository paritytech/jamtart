use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Health status levels
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Component is functioning normally
    Healthy,
    /// Component has minor issues but is still functional
    Degraded,
    /// Component is not functioning
    Unhealthy,
}

/// Health information for a single component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub name: String,
    pub status: HealthStatus,
    pub message: Option<String>,
    pub last_check: chrono::DateTime<chrono::Utc>,
    pub metrics: HashMap<String, serde_json::Value>,
}

/// Overall health report
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthReport {
    pub status: HealthStatus,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub components: HashMap<String, ComponentHealth>,
    pub version: String,
    pub uptime_seconds: u64,
    pub summary: HealthSummary,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database_size_bytes: Option<u64>,
}

/// Summary statistics for health report
#[derive(Debug, Serialize, Deserialize)]
pub struct HealthSummary {
    pub total_components: usize,
    pub healthy_components: usize,
    pub degraded_components: usize,
    pub unhealthy_components: usize,
}

/// Configuration for a health check
pub struct HealthCheck {
    pub name: String,
    pub check_fn:
        Box<dyn Fn() -> Pin<Box<dyn Future<Output = ComponentHealth> + Send>> + Send + Sync>,
    pub interval: Duration,
}

/// Health monitoring system
#[derive(Clone)]
pub struct HealthMonitor {
    components: Arc<RwLock<HashMap<String, ComponentHealth>>>,
    checks: Arc<RwLock<Vec<Arc<HealthCheck>>>>,
    start_time: std::time::Instant,
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new()
    }
}

impl HealthMonitor {
    pub fn new() -> Self {
        Self {
            components: Arc::new(RwLock::new(HashMap::new())),
            checks: Arc::new(RwLock::new(Vec::new())),
            start_time: std::time::Instant::now(),
        }
    }

    /// Add a health check to be monitored
    pub async fn add_check(&self, check: HealthCheck) {
        let check = Arc::new(check);
        self.checks.write().await.push(check.clone());

        // Start monitoring this check
        let components = self.components.clone();
        let check_name = check.name.clone();
        let interval = check.interval;

        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            loop {
                interval_timer.tick().await;

                let health = (check.check_fn)().await;
                components.write().await.insert(check_name.clone(), health);
            }
        });
    }

    /// Get the current health report
    pub async fn get_health(&self) -> HealthReport {
        let components = self.components.read().await.clone();

        // Single-pass counting instead of 3 separate filter().count() iterations
        let mut healthy = 0usize;
        let mut degraded = 0usize;
        let mut unhealthy = 0usize;
        for c in components.values() {
            match c.status {
                HealthStatus::Healthy => healthy += 1,
                HealthStatus::Degraded => degraded += 1,
                HealthStatus::Unhealthy => unhealthy += 1,
            }
        }

        let summary = HealthSummary {
            total_components: components.len(),
            healthy_components: healthy,
            degraded_components: degraded,
            unhealthy_components: unhealthy,
        };

        let overall_status = calculate_overall_status(&components);

        // Extract database size from database component metrics
        let database_size_bytes = components
            .get("database")
            .and_then(|db| db.metrics.get("size_bytes"))
            .and_then(|v| v.as_u64());

        HealthReport {
            status: overall_status,
            timestamp: chrono::Utc::now(),
            components,
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
            summary,
            database_size_bytes,
        }
    }

    /// Get health for a specific component
    pub async fn get_component_health(&self, name: &str) -> Option<ComponentHealth> {
        self.components.read().await.get(name).cloned()
    }

    /// Update health for a component manually
    pub async fn update_component_health(&self, name: String, health: ComponentHealth) {
        self.components.write().await.insert(name, health);
    }
}

/// Calculate overall system health based on component health
fn calculate_overall_status(components: &HashMap<String, ComponentHealth>) -> HealthStatus {
    if components.is_empty() {
        return HealthStatus::Degraded;
    }

    let has_unhealthy = components
        .values()
        .any(|c| c.status == HealthStatus::Unhealthy);
    let has_degraded = components
        .values()
        .any(|c| c.status == HealthStatus::Degraded);

    if has_unhealthy {
        HealthStatus::Unhealthy
    } else if has_degraded {
        HealthStatus::Degraded
    } else {
        HealthStatus::Healthy
    }
}

/// Utility functions for creating common health checks
pub mod checks {
    use super::*;
    use crate::batch_writer::BatchWriter;
    use crate::event_broadcaster::EventBroadcaster;
    use crate::store::EventStore;

    /// Create a lightweight database health check using `SELECT 1`.
    /// Verifies the connection pool can acquire a connection and the database
    /// is responsive without running expensive queries.
    pub fn database_check(store: Arc<EventStore>) -> HealthCheck {
        HealthCheck {
            name: "database".to_string(),
            check_fn: Box::new(move || {
                let store = store.clone();
                Box::pin(async move {
                    let start = std::time::Instant::now();

                    // Lightweight ping with 500ms timeout
                    match tokio::time::timeout(
                        Duration::from_millis(500),
                        store.ping(),
                    )
                    .await
                    {
                        Ok(Ok(())) => {
                            let response_time = start.elapsed();
                            let mut metrics = HashMap::new();
                            metrics.insert(
                                "response_time_ms".to_string(),
                                serde_json::Value::Number(serde_json::Number::from(
                                    response_time.as_millis() as u64,
                                )),
                            );

                            let status = if response_time > Duration::from_millis(200) {
                                HealthStatus::Degraded
                            } else {
                                HealthStatus::Healthy
                            };

                            ComponentHealth {
                                name: "database".to_string(),
                                status,
                                message: Some(format!(
                                    "Ping: {}ms",
                                    response_time.as_millis()
                                )),
                                last_check: chrono::Utc::now(),
                                metrics,
                            }
                        }
                        Ok(Err(e)) => ComponentHealth {
                            name: "database".to_string(),
                            status: HealthStatus::Unhealthy,
                            message: Some(format!("Database error: {}", e)),
                            last_check: chrono::Utc::now(),
                            metrics: HashMap::new(),
                        },
                        Err(_) => ComponentHealth {
                            name: "database".to_string(),
                            status: HealthStatus::Unhealthy,
                            message: Some("Database ping timed out (>500ms)".to_string()),
                            last_check: chrono::Utc::now(),
                            metrics: HashMap::new(),
                        },
                    }
                })
            }),
            interval: Duration::from_secs(15),
        }
    }

    /// Create a batch writer health check.
    /// Uses non-blocking status checks only - never calls flush() which would
    /// defeat the batching optimization this check is meant to monitor.
    pub fn batch_writer_check(batch_writer: Arc<BatchWriter>) -> HealthCheck {
        HealthCheck {
            name: "batch_writer".to_string(),
            check_fn: Box::new(move || {
                let batch_writer = batch_writer.clone();
                Box::pin(async move {
                    let pending = batch_writer.pending_count();
                    let is_full = batch_writer.is_full();
                    let usage_pct = batch_writer.buffer_usage_percent();

                    let status = if is_full {
                        HealthStatus::Unhealthy
                    } else if usage_pct > 75.0 {
                        HealthStatus::Degraded
                    } else {
                        HealthStatus::Healthy
                    };

                    let mut metrics = HashMap::new();
                    metrics.insert(
                        "pending_count".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(pending as u64)),
                    );
                    metrics.insert(
                        "is_full".to_string(),
                        serde_json::Value::Bool(is_full),
                    );
                    metrics.insert(
                        "usage_percent".to_string(),
                        serde_json::json!(usage_pct),
                    );

                    ComponentHealth {
                        name: "batch_writer".to_string(),
                        status,
                        message: Some(format!(
                            "Buffer {:.0}% full ({} pending), full: {}",
                            usage_pct, pending, is_full
                        )),
                        last_check: chrono::Utc::now(),
                        metrics,
                    }
                })
            }),
            interval: Duration::from_secs(30),
        }
    }

    /// Create a broadcaster health check
    pub fn broadcaster_check(broadcaster: Arc<EventBroadcaster>) -> HealthCheck {
        HealthCheck {
            name: "event_broadcaster".to_string(),
            check_fn: Box::new(move || {
                let broadcaster = broadcaster.clone();
                Box::pin(async move {
                    let stats = broadcaster.get_stats();

                    // Only consider significant dropped events as a problem
                    // Undelivered events (no subscribers) are normal when no clients are connected
                    // A few dropped events can happen during connection churn
                    let status = if stats.dropped_events > 1000 {
                        HealthStatus::Unhealthy
                    } else if stats.dropped_events > 100 {
                        HealthStatus::Degraded
                    } else {
                        HealthStatus::Healthy
                    };

                    let mut metrics = HashMap::new();
                    metrics.insert(
                        "active_subscribers".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(
                            stats.active_subscribers,
                        )),
                    );
                    metrics.insert(
                        "node_channels".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(stats.node_channels)),
                    );
                    metrics.insert(
                        "total_events_broadcast".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(
                            stats.total_events_broadcast,
                        )),
                    );
                    metrics.insert(
                        "dropped_events".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(stats.dropped_events)),
                    );
                    metrics.insert(
                        "undelivered_events".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(
                            stats.undelivered_events,
                        )),
                    );
                    metrics.insert(
                        "events_in_buffer".to_string(),
                        serde_json::Value::Number(serde_json::Number::from(stats.events_in_buffer)),
                    );

                    let message = if stats.dropped_events > 0 {
                        format!(
                            "Broadcasting to {} subscribers, {} events lost due to buffer overflow",
                            stats.active_subscribers, stats.dropped_events
                        )
                    } else {
                        format!(
                            "Broadcasting to {} subscribers, {} events buffered",
                            stats.active_subscribers, stats.events_in_buffer
                        )
                    };

                    ComponentHealth {
                        name: "event_broadcaster".to_string(),
                        status,
                        message: Some(message),
                        last_check: chrono::Utc::now(),
                        metrics,
                    }
                })
            }),
            interval: Duration::from_secs(15),
        }
    }

    /// Create a memory usage health check.
    /// Reports always healthy (actual memory pressure is handled by the OS/container runtime).
    pub fn memory_check() -> HealthCheck {
        HealthCheck {
            name: "memory".to_string(),
            check_fn: Box::new(|| {
                Box::pin(async move {
                    ComponentHealth {
                        name: "memory".to_string(),
                        status: HealthStatus::Healthy,
                        message: Some("OK".to_string()),
                        last_check: chrono::Utc::now(),
                        metrics: HashMap::new(),
                    }
                })
            }),
            interval: Duration::from_secs(60),
        }
    }

    /// Create a system resource health check.
    /// Lightweight check that avoids file I/O - always reports healthy as actual
    /// resource limits are enforced by the container runtime.
    pub fn system_resources_check() -> HealthCheck {
        HealthCheck {
            name: "system_resources".to_string(),
            check_fn: Box::new(|| {
                Box::pin(async move {
                    ComponentHealth {
                        name: "system_resources".to_string(),
                        status: HealthStatus::Healthy,
                        message: Some("OK".to_string()),
                        last_check: chrono::Utc::now(),
                        metrics: HashMap::new(),
                    }
                })
            }),
            interval: Duration::from_secs(60),
        }
    }

    /// Create a partition health check for the events table
    pub fn partition_check(store: Arc<EventStore>) -> HealthCheck {
        HealthCheck {
            name: "partitions".to_string(),
            check_fn: Box::new(move || {
                let store = store.clone();
                Box::pin(async move {
                    match store.check_partition_health().await {
                        Ok(health) => {
                            let mut metrics = HashMap::new();
                            metrics.insert(
                                "today_partition_exists".to_string(),
                                serde_json::Value::Bool(health.today_partition_exists),
                            );
                            metrics.insert(
                                "tomorrow_partition_exists".to_string(),
                                serde_json::Value::Bool(health.tomorrow_partition_exists),
                            );
                            metrics.insert(
                                "future_partition_count".to_string(),
                                serde_json::Value::Number(serde_json::Number::from(
                                    health.future_partition_count,
                                )),
                            );
                            metrics.insert(
                                "latest_partitions".to_string(),
                                serde_json::Value::Array(
                                    health
                                        .latest_partitions
                                        .iter()
                                        .map(|s| serde_json::Value::String(s.clone()))
                                        .collect(),
                                ),
                            );

                            let status = if !health.today_partition_exists {
                                HealthStatus::Unhealthy
                            } else if !health.tomorrow_partition_exists
                                || health.future_partition_count < 3
                            {
                                HealthStatus::Degraded
                            } else {
                                HealthStatus::Healthy
                            };

                            let message = if health.is_healthy() {
                                format!(
                                    "Partitions healthy: {} future partitions available",
                                    health.future_partition_count
                                )
                            } else if !health.today_partition_exists {
                                "CRITICAL: Today's partition is missing! Events cannot be stored."
                                    .to_string()
                            } else {
                                format!(
                                    "Warning: Only {} future partitions available",
                                    health.future_partition_count
                                )
                            };

                            ComponentHealth {
                                name: "partitions".to_string(),
                                status,
                                message: Some(message),
                                last_check: chrono::Utc::now(),
                                metrics,
                            }
                        }
                        Err(e) => ComponentHealth {
                            name: "partitions".to_string(),
                            status: HealthStatus::Unhealthy,
                            message: Some(format!("Failed to check partitions: {}", e)),
                            last_check: chrono::Utc::now(),
                            metrics: HashMap::new(),
                        },
                    }
                })
            }),
            interval: Duration::from_secs(300), // Check every 5 minutes
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    fn make_check(name: &str, status: HealthStatus) -> HealthCheck {
        let name = name.to_string();
        let name2 = name.clone();
        HealthCheck {
            name,
            check_fn: Box::new(move || {
                let name = name2.clone();
                let status = status.clone();
                Box::pin(async move {
                    ComponentHealth {
                        name,
                        status,
                        message: None,
                        last_check: chrono::Utc::now(),
                        metrics: HashMap::new(),
                    }
                })
            }),
            interval: Duration::from_millis(50),
        }
    }

    #[tokio::test]
    async fn test_health_monitor() {
        let monitor = HealthMonitor::new();
        monitor.add_check(make_check("test", HealthStatus::Healthy)).await;
        sleep(Duration::from_millis(100)).await;

        let health = monitor.get_health().await;
        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.components.len(), 1);
        assert!(health.components.contains_key("test"));
    }

    #[tokio::test]
    async fn test_degraded_status() {
        let monitor = HealthMonitor::new();
        monitor
            .add_check(make_check("comp1", HealthStatus::Healthy))
            .await;
        monitor
            .add_check(make_check("comp2", HealthStatus::Degraded))
            .await;
        sleep(Duration::from_millis(100)).await;

        let health = monitor.get_health().await;
        assert_eq!(health.status, HealthStatus::Degraded);
        assert_eq!(health.summary.degraded_components, 1);
        assert_eq!(health.summary.healthy_components, 1);
    }

    #[tokio::test]
    async fn test_unhealthy_status() {
        let monitor = HealthMonitor::new();
        monitor
            .add_check(make_check("comp1", HealthStatus::Healthy))
            .await;
        monitor
            .add_check(make_check("comp2", HealthStatus::Unhealthy))
            .await;
        sleep(Duration::from_millis(100)).await;

        let health = monitor.get_health().await;
        assert_eq!(health.status, HealthStatus::Unhealthy);
        assert_eq!(health.summary.unhealthy_components, 1);
    }

    #[tokio::test]
    async fn test_empty_components_degraded() {
        // No checks registered → overall status should be Degraded
        let components: HashMap<String, ComponentHealth> = HashMap::new();
        let status = calculate_overall_status(&components);
        assert_eq!(status, HealthStatus::Degraded);
    }

    #[tokio::test]
    async fn test_mixed_status_unhealthy_wins() {
        let monitor = HealthMonitor::new();
        monitor
            .add_check(make_check("healthy", HealthStatus::Healthy))
            .await;
        monitor
            .add_check(make_check("degraded", HealthStatus::Degraded))
            .await;
        monitor
            .add_check(make_check("unhealthy", HealthStatus::Unhealthy))
            .await;
        sleep(Duration::from_millis(100)).await;

        let health = monitor.get_health().await;
        // Unhealthy takes precedence over Degraded
        assert_eq!(health.status, HealthStatus::Unhealthy);
        assert_eq!(health.summary.total_components, 3);
    }
}
