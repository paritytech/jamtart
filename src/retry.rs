use rand::Rng;
use std::future::Future;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, warn};

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial backoff duration
    pub initial_backoff: Duration,
    /// Maximum backoff duration
    pub max_backoff: Duration,
    /// Base for exponential backoff calculation
    pub exponential_base: f64,
    /// Whether to add jitter to backoff
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(60),
            exponential_base: 2.0,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Create a policy for fast retries (e.g., network requests)
    pub fn fast() -> Self {
        Self {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
            exponential_base: 2.0,
            jitter: true,
        }
    }

    /// Create a policy for slow retries (e.g., database connections)
    pub fn slow() -> Self {
        Self {
            max_attempts: 10,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(300),
            exponential_base: 1.5,
            jitter: true,
        }
    }

    /// Create a policy with fixed delay
    pub fn fixed(delay: Duration, max_attempts: u32) -> Self {
        Self {
            max_attempts,
            initial_backoff: delay,
            max_backoff: delay,
            exponential_base: 1.0,
            jitter: false,
        }
    }
}

/// Retry a future with exponential backoff
pub async fn retry_with_backoff<F, Fut, T, E>(
    policy: &RetryPolicy,
    mut operation: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt = 0;
    let mut backoff = policy.initial_backoff;

    loop {
        attempt += 1;
        debug!("Retry attempt {} of {}", attempt, policy.max_attempts);

        match operation().await {
            Ok(result) => {
                if attempt > 1 {
                    debug!("Operation succeeded after {} attempts", attempt);
                }
                return Ok(result);
            }
            Err(error) if attempt >= policy.max_attempts => {
                warn!(
                    "Max retry attempts ({}) reached: {}",
                    policy.max_attempts, error
                );
                return Err(error);
            }
            Err(error) => {
                warn!(
                    "Attempt {} failed: {}, retrying in {:?}",
                    attempt, error, backoff
                );

                sleep(backoff).await;

                // Calculate next backoff
                backoff = calculate_next_backoff(backoff, policy);
            }
        }
    }
}

/// Retry with a condition to determine if the error is retryable
pub async fn retry_with_condition<F, Fut, T, E, C>(
    policy: &RetryPolicy,
    mut operation: F,
    mut is_retryable: C,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
    C: FnMut(&E) -> bool,
{
    let mut attempt = 0;
    let mut backoff = policy.initial_backoff;

    loop {
        attempt += 1;

        match operation().await {
            Ok(result) => return Ok(result),
            Err(error) => {
                if !is_retryable(&error) {
                    debug!("Error is not retryable: {}", error);
                    return Err(error);
                }

                if attempt >= policy.max_attempts {
                    warn!(
                        "Max retry attempts ({}) reached: {}",
                        policy.max_attempts, error
                    );
                    return Err(error);
                }

                warn!(
                    "Attempt {} failed: {}, retrying in {:?}",
                    attempt, error, backoff
                );

                sleep(backoff).await;
                backoff = calculate_next_backoff(backoff, policy);
            }
        }
    }
}

/// Calculate the next backoff duration
fn calculate_next_backoff(current: Duration, policy: &RetryPolicy) -> Duration {
    let mut next = current.mul_f64(policy.exponential_base);

    if next > policy.max_backoff {
        next = policy.max_backoff;
    }

    if policy.jitter {
        // Add jitter: ±20% of the backoff duration
        let jitter_range = next.as_millis() as f64 * 0.2;
        let jitter = rand::thread_rng().gen_range(-jitter_range..=jitter_range);
        let with_jitter = (next.as_millis() as f64 + jitter).max(0.0) as u64;
        next = Duration::from_millis(with_jitter);
    }

    next
}

/// Retry result that includes attempt information
#[derive(Debug)]
pub struct RetryResult<T, E> {
    pub result: Result<T, E>,
    pub attempts: u32,
    pub total_delay: Duration,
}

/// Retry with detailed result information
pub async fn retry_with_info<F, Fut, T, E>(
    policy: &RetryPolicy,
    mut operation: F,
) -> RetryResult<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let start = std::time::Instant::now();
    let mut attempt = 0;
    let mut backoff = policy.initial_backoff;

    loop {
        attempt += 1;

        match operation().await {
            Ok(result) => {
                return RetryResult {
                    result: Ok(result),
                    attempts: attempt,
                    total_delay: start.elapsed(),
                };
            }
            Err(error) if attempt >= policy.max_attempts => {
                return RetryResult {
                    result: Err(error),
                    attempts: attempt,
                    total_delay: start.elapsed(),
                };
            }
            Err(error) => {
                warn!(
                    "Attempt {} failed: {}, retrying in {:?}",
                    attempt, error, backoff
                );

                sleep(backoff).await;
                backoff = calculate_next_backoff(backoff, policy);
            }
        }
    }
}

/// Common retryable error types
pub trait RetryableError {
    fn is_retryable(&self) -> bool;
}

// Example implementation for std::io::Error
impl RetryableError for std::io::Error {
    fn is_retryable(&self) -> bool {
        use std::io::ErrorKind;
        matches!(
            self.kind(),
            ErrorKind::ConnectionRefused
                | ErrorKind::ConnectionReset
                | ErrorKind::ConnectionAborted
                | ErrorKind::NotConnected
                | ErrorKind::TimedOut
                | ErrorKind::Interrupted
                | ErrorKind::UnexpectedEof
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn test_retry_success_after_failures() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            exponential_base: 2.0,
            jitter: false,
        };

        let result = retry_with_backoff(&policy, || {
            let count = counter_clone.fetch_add(1, Ordering::SeqCst);
            async move {
                if count < 2 {
                    Err("temporary failure")
                } else {
                    Ok("success")
                }
            }
        })
        .await;

        assert_eq!(result, Ok("success"));
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_max_attempts() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let policy = RetryPolicy {
            max_attempts: 3,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(100),
            exponential_base: 2.0,
            jitter: false,
        };

        let result = retry_with_backoff(&policy, || {
            counter_clone.fetch_add(1, Ordering::SeqCst);
            async { Err::<(), &str>("always fails") }
        })
        .await;

        assert_eq!(result, Err("always fails"));
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_with_condition() {
        let policy = RetryPolicy::fast();

        let result = retry_with_condition(
            &policy,
            || async { Err::<(), &str>("not retryable") },
            |_| false, // Never retry
        )
        .await;

        assert_eq!(result, Err("not retryable"));
    }

    #[test]
    fn test_backoff_calculation() {
        let policy = RetryPolicy {
            max_attempts: 5,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            exponential_base: 2.0,
            jitter: false,
        };

        let backoff1 = calculate_next_backoff(policy.initial_backoff, &policy);
        assert_eq!(backoff1, Duration::from_millis(200));

        let backoff2 = calculate_next_backoff(backoff1, &policy);
        assert_eq!(backoff2, Duration::from_millis(400));

        // Test max backoff
        let large_backoff = Duration::from_secs(20);
        let capped = calculate_next_backoff(large_backoff, &policy);
        assert_eq!(capped, policy.max_backoff);
    }
}
