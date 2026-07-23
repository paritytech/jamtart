//! Shared histogram utilities for latency distribution tracking.
//!
//! Provides bucket indexing and percentile computation for both DA shard latency
//! (14 buckets) and convergence tracking (23 buckets). Histograms are additive:
//! SUM bucket columns across rows for merged distributions.

/// Convergence histogram bucket boundaries (ms). 23 buckets + sentinel.
/// Buckets: [0,2) [2,5) [5,10) [10,15) [15,20) [20,30) [30,50) [50,75) [75,100)
///   [100,150) [150,250) [250,500) [500,1000) [1000,2000) [2000,5000)
///   [5000,10000) [10000,15000) [15000,20000) [20000,25000) [25000,30000)
///   [30000,60000) [60000,120000) [120000,∞)
pub const CONVERGENCE_BOUNDS: [u32; 24] = [
    0, 2, 5, 10, 15, 20, 30, 50, 75, 100, 150, 250, 500, 1000, 2000, 5000, 10000, 15000, 20000,
    25000, 30000, 60000, 120000, 120000, // sentinel for overflow bucket upper bound
];

/// Number of convergence histogram buckets.
pub const CONVERGENCE_BUCKET_COUNT: usize = 23;

/// DA shard latency histogram bucket boundaries (ms). 14 buckets + sentinel.
/// Matches `da_tracker::HIST_BOUNDARIES_MS` layout.
pub const DA_BOUNDS: [u32; 15] = [
    0, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2000, 3000, 5000,
    5000, // sentinel for overflow bucket
];

/// Number of DA histogram buckets.
pub const DA_BUCKET_COUNT: usize = 14;

/// Find the convergence histogram bucket index for a delta in milliseconds.
pub fn convergence_bucket_index(delta_ms: i32) -> usize {
    let delta = delta_ms.max(0) as u32;
    for i in (0..CONVERGENCE_BUCKET_COUNT).rev() {
        if delta >= CONVERGENCE_BOUNDS[i] {
            return i;
        }
    }
    0
}

/// Compute approximate percentiles (p50, p75, p95, p99, p100) from a histogram.
///
/// Uses the **upper bound** of the bucket the percentile falls into.
/// The overflow bucket (last) reports its lower bound since it has no finite upper bound.
///
/// `bounds` must have `buckets.len() + 1` elements: N lower bounds + 1 sentinel.
/// Returns None if total is 0.
pub fn percentiles_from_histogram(
    buckets: &[u32],
    total: u32,
    bounds: &[u32],
) -> Option<(i32, i32, i32, i32, i32)> {
    if total == 0 {
        return None;
    }
    let targets = [0.50, 0.75, 0.95, 0.99, 1.0];
    let mut results = [0i32; 5];
    let n = buckets.len();
    for (i, &target) in targets.iter().enumerate() {
        let threshold = (total as f64 * target).ceil() as u32;
        let mut cumsum = 0u32;
        for (j, &count) in buckets.iter().enumerate() {
            cumsum += count;
            if cumsum >= threshold {
                if j == n - 1 {
                    // Overflow bucket: report lower bound (no finite upper)
                    results[i] = bounds[j] as i32;
                } else {
                    // Regular bucket: report upper bound
                    results[i] = bounds[j + 1] as i32;
                }
                break;
            }
        }
    }
    Some((results[0], results[1], results[2], results[3], results[4]))
}

/// Bucket an array of i32 deltas into a convergence histogram.
pub fn bucket_deltas_convergence(deltas: &[i32]) -> ([u32; CONVERGENCE_BUCKET_COUNT], u32) {
    let mut hist = [0u32; CONVERGENCE_BUCKET_COUNT];
    for &d in deltas {
        hist[convergence_bucket_index(d)] += 1;
    }
    (hist, deltas.len() as u32)
}

/// Column names for convergence histogram buckets (for SQL generation).
pub const CONVERGENCE_HIST_COLUMNS: [&str; CONVERGENCE_BUCKET_COUNT] = [
    "h_0_2",
    "h_2_5",
    "h_5_10",
    "h_10_15",
    "h_15_20",
    "h_20_30",
    "h_30_50",
    "h_50_75",
    "h_75_100",
    "h_100_150",
    "h_150_250",
    "h_250_500",
    "h_500_1000",
    "h_1000_2000",
    "h_2000_5000",
    "h_5000_10000",
    "h_10000_15000",
    "h_15000_20000",
    "h_20000_25000",
    "h_25000_30000",
    "h_30000_60000",
    "h_60000_120000",
    "h_120000_plus",
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn convergence_bucket_index_zero() {
        assert_eq!(convergence_bucket_index(0), 0);
        assert_eq!(convergence_bucket_index(1), 0); // [0,2)
    }

    #[test]
    fn convergence_bucket_index_boundaries() {
        assert_eq!(convergence_bucket_index(2), 1); // [2,5)
        assert_eq!(convergence_bucket_index(4), 1);
        assert_eq!(convergence_bucket_index(5), 2); // [5,10)
        assert_eq!(convergence_bucket_index(10), 3); // [10,15)
        assert_eq!(convergence_bucket_index(20), 5); // [20,30)
        assert_eq!(convergence_bucket_index(50), 7); // [50,75)
        assert_eq!(convergence_bucket_index(100), 9); // [100,150)
        assert_eq!(convergence_bucket_index(1000), 13); // [1000,2000)
        assert_eq!(convergence_bucket_index(30000), 20); // [30000,60000)
        assert_eq!(convergence_bucket_index(120000), 22); // [120000,∞)
        assert_eq!(convergence_bucket_index(999999), 22);
    }

    #[test]
    fn convergence_bucket_index_negative() {
        assert_eq!(convergence_bucket_index(-5), 0);
    }

    #[test]
    fn percentiles_empty() {
        let buckets = [0u32; 14];
        assert_eq!(percentiles_from_histogram(&buckets, 0, &DA_BOUNDS), None);
    }

    #[test]
    fn percentiles_upper_bound_single_bucket() {
        // 100 samples in DA bucket index 4 [10,25) → upper bound = 25
        let mut buckets = [0u32; 14];
        buckets[4] = 100;
        let (p50, p75, p95, p99, p100) =
            percentiles_from_histogram(&buckets, 100, &DA_BOUNDS).unwrap();
        assert_eq!(p50, 25);
        assert_eq!(p75, 25);
        assert_eq!(p95, 25);
        assert_eq!(p99, 25);
        assert_eq!(p100, 25);
    }

    #[test]
    fn percentiles_upper_bound_two_buckets() {
        // 50 in bucket 4 [10,25) + 50 in bucket 6 [50,100)
        let mut buckets = [0u32; 14];
        buckets[4] = 50;
        buckets[6] = 50;
        let (p50, _p75, _p95, _p99, p100) =
            percentiles_from_histogram(&buckets, 100, &DA_BOUNDS).unwrap();
        // p50 threshold = ceil(50) = 50, cumsum reaches 50 at bucket 4 → upper 25
        assert_eq!(p50, 25);
        // p100 threshold = 100, cumsum reaches 100 at bucket 6 → upper 100
        assert_eq!(p100, 100);
    }

    #[test]
    fn percentiles_upper_bound_single_sample() {
        // 1 sample in DA bucket 5 [25,50) → upper bound = 50
        let mut buckets = [0u32; 14];
        buckets[5] = 1;
        let (p50, p75, p95, p99, p100) =
            percentiles_from_histogram(&buckets, 1, &DA_BOUNDS).unwrap();
        assert_eq!(p50, 50);
        assert_eq!(p75, 50);
        assert_eq!(p95, 50);
        assert_eq!(p99, 50);
        assert_eq!(p100, 50);
    }

    #[test]
    fn percentiles_overflow_bucket() {
        // DA overflow bucket index 13 [5000,∞) → lower bound = 5000
        let mut buckets = [0u32; 14];
        buckets[13] = 10;
        let (p50, p75, p95, p99, p100) =
            percentiles_from_histogram(&buckets, 10, &DA_BOUNDS).unwrap();
        assert_eq!(p50, 5000);
        assert_eq!(p75, 5000);
        assert_eq!(p95, 5000);
        assert_eq!(p99, 5000);
        assert_eq!(p100, 5000);
    }

    #[test]
    fn percentiles_spread_ordering() {
        // Spread across multiple DA buckets — verify ordering
        let mut buckets = [0u32; 14];
        buckets[0] = 5; // [0,1)
        buckets[2] = 10; // [2,5)
        buckets[5] = 20; // [25,50)
        buckets[8] = 15; // [250,500)
        buckets[11] = 10; // [2000,3000)
        let total = 5 + 10 + 20 + 15 + 10;
        let (p50, p75, p95, p99, p100) =
            percentiles_from_histogram(&buckets, total, &DA_BOUNDS).unwrap();
        assert!(p50 <= p75, "p50 ({p50}) <= p75 ({p75})");
        assert!(p75 <= p95, "p75 ({p75}) <= p95 ({p95})");
        assert!(p95 <= p99, "p95 ({p95}) <= p99 ({p99})");
        assert!(p99 <= p100, "p99 ({p99}) <= p100 ({p100})");
    }

    #[test]
    fn convergence_percentiles_typical() {
        // Simulate 1024 validators: p50~21ms, p75~35ms, p95~56ms
        let mut buckets = [0u32; CONVERGENCE_BUCKET_COUNT];
        buckets[3] = 100; // [10,15) — fast nodes
        buckets[4] = 200; // [15,20)
        buckets[5] = 300; // [20,30) — bulk of p50
        buckets[6] = 200; // [30,50) — p75 range
        buckets[7] = 100; // [50,75) — p95 range
        buckets[8] = 50; // [75,100)
        buckets[9] = 30; // [100,150)
        buckets[10] = 20; // [150,250)
        buckets[22] = 24; // [120000,∞) — stuck nodes
        let total: u32 = buckets.iter().sum();
        let (p50, p75, p95, p99, p100) =
            percentiles_from_histogram(&buckets, total, &CONVERGENCE_BOUNDS).unwrap();
        assert_eq!(p50, 30); // upper bound of [20,30)
        assert_eq!(p75, 50); // upper bound of [30,50)
        assert!(p95 >= 75);
        assert!(p99 >= 100);
        assert_eq!(p100, 120000); // overflow lower bound
    }

    #[test]
    fn bucket_deltas_convergence_basic() {
        let deltas = vec![0, 1, 5, 10, 21, 50, 100, 1000, 200000];
        let (hist, total) = bucket_deltas_convergence(&deltas);
        assert_eq!(total, 9);
        assert_eq!(hist[0], 2); // [0,2): 0, 1
        assert_eq!(hist[2], 1); // [5,10): 5
        assert_eq!(hist[3], 1); // [10,15): 10
        assert_eq!(hist[5], 1); // [20,30): 21
        assert_eq!(hist[7], 1); // [50,75): 50
        assert_eq!(hist[9], 1); // [100,150): 100
        assert_eq!(hist[13], 1); // [1000,2000): 1000
        assert_eq!(hist[22], 1); // [120000,∞): 200000
    }
}
