use serde_json::json;
use std::thread;
use std::time::{Duration, Instant};

const SIM_START: &str = "2026-01-01T00:00:00Z";
const SIM_END: &str = "2026-01-01T00:02:00Z";
const RESULTS_TIMEOUT: Duration = Duration::from_secs(30);

#[test]
fn test_bottlenecks_detect_work_packages() {
    super::run_test("test_bottlenecks_detect_work_packages", |ctx| {
        // Above 10x, events from all nodes arrive together and their order decides which
        // timestamps TART records, so the results would differ between runs.
        ctx.run_spammer(&format!(
            "sim --params tiny --seed 42 --start {SIM_START} --duration 120 --speed 10 --quiet"
        ));

        let expected = json!([{
            "total_wps": 7,
            "failed_wps": 0,
            "failure_rate": 0.0,
            "avg_pipeline_ms": 3129.095428571429,
            "stage_timing": {
                "authorize": { "p50_ms": 49.0, "p95_ms": 71.6 },
                "refine": { "p50_ms": 219.329, "p95_ms": 255.6086 },
                "report": { "p50_ms": 8.0, "p95_ms": 11.7 },
                "guarantee": { "p50_ms": 2697.0074999999997, "p95_ms": 3854.51675 },
                "distribute": { "p50_ms": 335.5, "p95_ms": 410.0 },
                "pipeline_total": { "p50_ms": 3230.064, "p95_ms": 4509.0547 },
            },
        }]);

        let path = format!("/api/grafana/bottlenecks?start={SIM_START}&end={SIM_END}");
        // TART writes work-package tracking to the database periodically, not on every event.
        let deadline = Instant::now() + RESULTS_TIMEOUT;
        let mut actual = ctx.get_json(&path);
        while actual != expected && Instant::now() < deadline {
            thread::sleep(Duration::from_secs(1));
            actual = ctx.get_json(&path);
        }
        assert_eq!(actual, expected);
    });
}
