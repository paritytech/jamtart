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

        let summary = HealthSummary {
            total_components: components.len(),
            healthy_components: components
                .values()
                .filter(|c| c.status == HealthStatus::Healthy)
                .count(),
            degraded_components: components
                .values()
                .filter(|c| c.status == HealthStatus::Degraded)
                .count(),
            unhealthy_components: components
                .values()
                .filter(|c| c.status == HealthStatus::Unhealthy)
                .count(),
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

    /// Create a database health check
    pub fn database_check(store: Arc<EventStore>) -> HealthCheck {
        HealthCheck {
            name: "database".to_string(),
            check_fn: Box::new(move || {
                let store = store.clone();
                Box::pin(async move {
                    let start = std::time::Instant::now();

                    match store.get_health_metrics().await {
                        Ok(metrics) => {
                            let response_time = start.elapsed();
                            let mut component_metrics = metrics;
                            component_metrics.insert(
                                "response_time_ms".to_string(),
                                serde_json::Value::Number(serde_json::Number::from(
                                    response_time.as_millis() as u64,
                                )),
                            );

                            let status = if response_time > Duration::from_secs(5) {
                                HealthStatus::Degraded
                            } else {
                                HealthStatus::Healthy
                            };

                            ComponentHealth {
                                name: "database".to_string(),
                                status,
                                message: Some(format!(
                                    "Response time: {}ms",
                                    response_time.as_millis()
                                )),
                                last_check: chrono::Utc::now(),
                                metrics: component_metrics,
                            }
                        }
                        Err(e) => ComponentHealth {
                            name: "database".to_string(),
                            status: HealthStatus::Unhealthy,
                            message: Some(format!("Database error: {}", e)),
                            last_check: chrono::Utc::now(),
                            metrics: HashMap::new(),
                        },
                    }
                })
            }),
            interval: Duration::from_secs(30),
        }
    }

    /// Create a batch writer health check
    pub fn batch_writer_check(batch_writer: Arc<BatchWriter>) -> HealthCheck {
        HealthCheck {
            name: "batch_writer".to_string(),
            check_fn: Box::new(move || {
                let batch_writer = batch_writer.clone();
                Box::pin(async move {
                    let start = std::time::Instant::now();

                    // Test batch writer responsiveness by sending a flush command
                    match tokio::time::timeout(
                        Duration::from_secs(5),
                        batch_writer.flush()
                    ).await {
                        Ok(Ok(_)) => {
                            let response_time = start.elapsed();
                            let mut metrics = HashMap::new();
                            metrics.insert(
                                "response_time_ms".to_string(),
                                serde_json::Value::Number(serde_json::Number::from(
                                    response_time.as_millis() as u64,
                                )),
                            );
                            metrics.insert(
                                "pending_count".to_string(),
                                serde_json::Value::Number(serde_json::Number::from(
                                    batch_writer.pending_count() as u64,
                                )),
                            );
                            metrics.insert(
                                "is_full".to_string(),
                                serde_json::Value::Bool(batch_writer.is_full()),
                            );

                            let status = if response_time > Duration::from_secs(2) {
                                HealthStatus::Degraded
                            } else if batch_writer.is_full() {
                                HealthStatus::Degraded
                            } else {
                                HealthStatus::Healthy
                            };

                            ComponentHealth {
                                name: "batch_writer".to_string(),
                                status,
                                message: Some(format!(
                                    "Response time: {}ms, pending: {}, full: {}",
                                    response_time.as_millis(),
                                    batch_writer.pending_count(),
                                    batch_writer.is_full()
                                )),
                                last_check: chrono::Utc::now(),
                                metrics,
                            }
                        }
                        Ok(Err(e)) => ComponentHealth {
                            name: "batch_writer".to_string(),
                            status: HealthStatus::Unhealthy,
                            message: Some(format!("Batch writer flush failed: {}", e)),
                            last_check: chrono::Utc::now(),
                            metrics: HashMap::new(),
                        },
                        Err(_) => ComponentHealth {
                            name: "batch_writer".to_string(),
                            status: HealthStatus::Unhealthy,
                            message: Some("Batch writer timeout - task may have crashed".to_string()),
                            last_check: chrono::Utc::now(),
                            metrics: HashMap::new(),
                        },
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
                    let stats = broadcaster.get_stats().await;

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

    /// Create a memory usage health check
    pub fn memory_check() -> HealthCheck {
        HealthCheck {
            name: "memory".to_string(),
            check_fn: Box::new(|| {
                Box::pin(async move {
                    // Get memory info (simplified - would use proper system APIs in production)
                    let mut metrics = HashMap::new();

                    // For now, just check if we can allocate memory
                    let test_allocation = Vec::<u8>::with_capacity(1024 * 1024); // 1MB
                    drop(test_allocation);

                    metrics.insert("test_allocation".to_string(), serde_json::Value::Bool(true));

                    ComponentHealth {
                        name: "memory".to_string(),
                        status: HealthStatus::Healthy,
                        message: Some("Memory allocation test passed".to_string()),
                        last_check: chrono::Utc::now(),
                        metrics,
                    }
                })
            }),
            interval: Duration::from_secs(60),
        }
    }

    /// Create a system resource health check
    pub fn system_resources_check() -> HealthCheck {
        HealthCheck {
            name: "system_resources".to_string(),
            check_fn: Box::new(|| {
                Box::pin(async move {
                    let mut metrics = HashMap::new();

                    // Check available file descriptors (simplified)
                    let status = match std::fs::File::open("/dev/null") {
                        Ok(_) => {
                            metrics.insert(
                                "file_descriptor_test".to_string(),
                                serde_json::Value::Bool(true),
                            );
                            HealthStatus::Healthy
                        }
                        Err(e) => {
                            metrics.insert(
                                "file_descriptor_error".to_string(),
                                serde_json::Value::String(e.to_string()),
                            );
                            HealthStatus::Degraded
                        }
                    };

                    ComponentHealth {
                        name: "system_resources".to_string(),
                        status,
                        message: Some("System resource checks".to_string()),
                        last_check: chrono::Utc::now(),
                        metrics,
                    }
                })
            }),
            interval: Duration::from_secs(60),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_health_monitor() {
        let monitor = HealthMonitor::new();

        // Add a simple health check
        let check = HealthCheck {
            name: "test".to_string(),
            check_fn: Box::new(|| {
                Box::pin(async move {
                    ComponentHealth {
                        name: "test".to_string(),
                        status: HealthStatus::Healthy,
                        message: Some("Test check".to_string()),
                        last_check: chrono::Utc::now(),
                        metrics: HashMap::new(),
                    }
                })
            }),
            interval: Duration::from_millis(100),
        };

        monitor.add_check(check).await;

        // Wait for the check to run
        sleep(Duration::from_millis(150)).await;

        let health = monitor.get_health().await;
        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.components.len(), 1);
        assert!(health.components.contains_key("test"));
    }
}
