use chrono::{SecondsFormat, Utc};

#[test]
fn test_bottlenecks_detect_work_packages() {
    super::run_test("test_bottlenecks_detect_work_packages", |ctx| {
        let start = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

        ctx.run_spammer(&["sim", "--params", "tiny", "--duration", "120"]);

        let end = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

        let response = ctx.get_json(&format!("/api/grafana/bottlenecks?start={start}&end={end}"));

        assert!(
            response[0]["total_wps"].as_i64() > Some(0),
            "expected work packages in the response: {response}"
        );
    });
}
