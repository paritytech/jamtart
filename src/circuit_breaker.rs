use parking_lot::RwLock;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    /// Normal operation - requests pass through
    Closed,
    /// Circuit is blocking requests due to failures
    Open,
    /// Testing if the service has recovered
    HalfOpen,
}

/// Configuration for circuit breaker behavior
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening circuit
    pub failure_threshold: u32,
    /// Number of successes in half-open state before closing
    pub success_threshold: u32,
    /// How long to wait before trying half-open
    pub timeout: Duration,
    /// Maximum calls allowed in half-open state
    pub half_open_max_calls: u32,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 3,
            timeout: Duration::from_secs(60),
            half_open_max_calls: 3,
        }
    }
}

/// Circuit breaker state for a single service/node
#[derive(Debug)]
struct CircuitBreakerState {
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure_time: Option<Instant>,
    half_open_calls: u32,
    consecutive_failures: u32,
    total_failures: u64,
    total_successes: u64,
}

impl CircuitBreakerState {
    fn new() -> Self {
        Self {
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            half_open_calls: 0,
            consecutive_failures: 0,
            total_failures: 0,
            total_successes: 0,
        }
    }

    fn should_attempt_reset(&self, timeout: Duration) -> bool {
        match self.last_failure_time {
            Some(time) => time.elapsed() >= timeout,
            None => false,
        }
    }

    fn transition_to_half_open(&mut self) {
        info!("Circuit breaker transitioning to half-open");
        self.state = CircuitState::HalfOpen;
        self.half_open_calls = 0;
        self.success_count = 0;
    }

    fn on_success(&mut self, config: &CircuitBreakerConfig) {
        self.total_successes += 1;
        self.consecutive_failures = 0;

        match self.state {
            CircuitState::HalfOpen => {
                self.success_count += 1;
                if self.success_count >= config.success_threshold {
                    info!("Circuit breaker closing after successful recovery");
                    self.state = CircuitState::Closed;
                    self.failure_count = 0;
                    self.last_failure_time = None;
                }
            }
            CircuitState::Closed => {
                // Reset failure count on success
                if self.failure_count > 0 {
                    self.failure_count = self.failure_count.saturating_sub(1);
                }
            }
            CircuitState::Open => {
                // Shouldn't happen, but handle gracefully
                warn!("Success recorded while circuit open");
            }
        }
    }

    fn on_failure(&mut self, config: &CircuitBreakerConfig) {
        self.total_failures += 1;
        self.consecutive_failures += 1;
        self.failure_count += 1;
        self.last_failure_time = Some(Instant::now());

        match self.state {
            CircuitState::Closed => {
                if self.failure_count >= config.failure_threshold {
                    warn!(
                        "Circuit breaker opening after {} failures",
                        self.failure_count
                    );
                    self.state = CircuitState::Open;
                }
            }
            CircuitState::HalfOpen => {
                // Single failure in half-open state reopens the circuit
                warn!("Circuit breaker reopening after failure in half-open state");
                self.state = CircuitState::Open;
                self.half_open_calls = 0;
            }
            CircuitState::Open => {
                // Already open, update failure time
                self.last_failure_time = Some(Instant::now());
            }
        }
    }
}

/// Error types for circuit breaker
#[derive(Debug, thiserror::Error)]
pub enum CircuitError<E> {
    #[error("Circuit breaker is open")]
    Open,
    #[error("Call failed: {0}")]
    CallFailed(E),
}

/// Circuit breaker for managing node connections
pub struct NodeCircuitBreaker {
    breakers: Arc<RwLock<HashMap<String, CircuitBreakerState>>>,
    config: CircuitBreakerConfig,
}

impl NodeCircuitBreaker {
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            breakers: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Execute a function with circuit breaker protection
    pub async fn call<F, Fut, T, E>(&self, node_id: &str, f: F) -> Result<T, CircuitError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        // Check circuit state
        {
            let mut breakers = self.breakers.write();
            let breaker = breakers
                .entry(node_id.to_string())
                .or_insert_with(CircuitBreakerState::new);

            match breaker.state {
                CircuitState::Open => {
                    if breaker.should_attempt_reset(self.config.timeout) {
                        breaker.transition_to_half_open();
                    } else {
                        return Err(CircuitError::Open);
                    }
                }
                CircuitState::HalfOpen => {
                    if breaker.half_open_calls >= self.config.half_open_max_calls {
                        return Err(CircuitError::Open);
                    }
                    breaker.half_open_calls += 1;
                }
                CircuitState::Closed => {}
            }
        }

        // Execute the function
        match f().await {
            Ok(result) => {
                let mut breakers = self.breakers.write();
                if let Some(breaker) = breakers.get_mut(node_id) {
                    breaker.on_success(&self.config);
                }
                Ok(result)
            }
            Err(error) => {
                let mut breakers = self.breakers.write();
                if let Some(breaker) = breakers.get_mut(node_id) {
                    breaker.on_failure(&self.config);
                }
                Err(CircuitError::CallFailed(error))
            }
        }
    }

    /// Get the current state of a circuit
    pub fn get_state(&self, node_id: &str) -> Option<CircuitState> {
        self.breakers.read().get(node_id).map(|b| b.state)
    }

    /// Get statistics for a node's circuit breaker
    pub fn get_stats(&self, node_id: &str) -> Option<CircuitBreakerStats> {
        self.breakers
            .read()
            .get(node_id)
            .map(|b| CircuitBreakerStats {
                state: b.state,
                failure_count: b.failure_count,
                success_count: b.success_count,
                consecutive_failures: b.consecutive_failures,
                total_failures: b.total_failures,
                total_successes: b.total_successes,
                last_failure_time: b.last_failure_time,
            })
    }

    /// Get all circuit breaker states
    pub fn get_all_states(&self) -> HashMap<String, CircuitBreakerStats> {
        self.breakers
            .read()
            .iter()
            .map(|(id, breaker)| {
                (
                    id.clone(),
                    CircuitBreakerStats {
                        state: breaker.state,
                        failure_count: breaker.failure_count,
                        success_count: breaker.success_count,
                        consecutive_failures: breaker.consecutive_failures,
                        total_failures: breaker.total_failures,
                        total_successes: breaker.total_successes,
                        last_failure_time: breaker.last_failure_time,
                    },
                )
            })
            .collect()
    }

    /// Reset a specific circuit breaker
    pub fn reset(&self, node_id: &str) {
        let mut breakers = self.breakers.write();
        if let Some(breaker) = breakers.get_mut(node_id) {
            info!("Resetting circuit breaker for node {}", node_id);
            *breaker = CircuitBreakerState::new();
        }
    }

    /// Reset all circuit breakers
    pub fn reset_all(&self) {
        let mut breakers = self.breakers.write();
        info!("Resetting all {} circuit breakers", breakers.len());
        breakers.clear();
    }
}

/// Statistics for monitoring circuit breaker state
#[derive(Debug, Clone, serde::Serialize)]
pub struct CircuitBreakerStats {
    pub state: CircuitState,
    pub failure_count: u32,
    pub success_count: u32,
    pub consecutive_failures: u32,
    pub total_failures: u64,
    pub total_successes: u64,
    #[serde(serialize_with = "serialize_instant_option")]
    pub last_failure_time: Option<Instant>,
}

// Custom serializer for Option<Instant>
fn serialize_instant_option<S>(instant: &Option<Instant>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match instant {
        Some(instant) => {
            // Serialize as seconds since creation (relative time)
            let duration = instant.elapsed().as_secs();
            serializer.serialize_some(&format!("{}s ago", duration))
        }
        None => serializer.serialize_none(),
    }
}

// Implement Serialize for CircuitState
impl serde::Serialize for CircuitState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let state_str = match self {
            CircuitState::Closed => "closed",
            CircuitState::Open => "open",
            CircuitState::HalfOpen => "half_open",
        };
        serializer.serialize_str(state_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_circuit_breaker_basic() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            success_threshold: 2,
            timeout: Duration::from_millis(100),
            half_open_max_calls: 2,
        };

        let cb = NodeCircuitBreaker::new(config);
        let node_id = "test_node";

        // Should start closed
        assert_eq!(cb.get_state(node_id), None);

        // First failure
        let _ = cb
            .call(node_id, || async { Result::<(), &str>::Err("error") })
            .await;

        // Should still be closed
        assert_eq!(cb.get_state(node_id), Some(CircuitState::Closed));

        // Second failure should open the circuit
        let _ = cb
            .call(node_id, || async { Result::<(), &str>::Err("error") })
            .await;

        assert_eq!(cb.get_state(node_id), Some(CircuitState::Open));

        // Calls should be rejected while open
        let result = cb
            .call(node_id, || async { Result::<(), &str>::Ok(()) })
            .await;

        assert!(matches!(result, Err(CircuitError::Open)));

        // Wait for timeout
        sleep(Duration::from_millis(150)).await;

        // Should transition to half-open on next call
        let _ = cb
            .call(node_id, || async { Result::<(), &str>::Ok(()) })
            .await;

        assert_eq!(cb.get_state(node_id), Some(CircuitState::HalfOpen));
    }
}
