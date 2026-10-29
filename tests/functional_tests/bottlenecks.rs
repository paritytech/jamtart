use crate::functional_tests::TestContext;
use chrono::{SecondsFormat, Utc};
use xshell::cmd;

#[test]
fn test_bottlenecks_detect_work_packages() {
    super::run_test("test_bottlenecks_detect_work_packages", |ctx| {
        let TestContext {
            sh,
            api_bind,
            telemetry_bind,
            spammer_cmd,
        } = ctx;

        let start = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

        cmd!(
            sh,
            "{spammer_cmd} sim --params tiny --duration 120 --server {telemetry_bind}"
        )
        .run()
        .unwrap();

        let end = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

        let actual_response: serde_json::Value = reqwest::blocking::get(format!(
            "http://{api_bind}/api/grafana/bottlenecks?start={start}&end={end}"
        ))
        .unwrap()
        .json()
        .unwrap();

        let actual_total_wps = actual_response
            .as_array()
            .unwrap()
            .first()
            .unwrap()
            .as_object()
            .unwrap()
            .get("total_wps")
            .unwrap()
            .as_i64()
            .unwrap();

        assert!(actual_total_wps > 0);

        println!("actual response: {actual_response}");
    });
}
