use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::panic::UnwindSafe;
use std::process::{Command, Stdio};
use url::Url;

mod bottlenecks;

/// A collection of parameters intended for the test scenario to use to locate TART services and
/// related tools.
pub struct TestContext {
    /// A string of `addr:port` where TART telemetry server is listening.
    ///
    /// This should be the server to write JIP-3 events to.
    telemetry_bind: String,
    /// A string of `addr:port` where TART API server is listening.
    ///
    /// This should be the server to read statistics from.
    pub api_bind: String,
    /// A filename of the CLI executable to generate input with.
    spammer_cmd: String,
}

impl TestContext {
    /// Runs `jip-3-spammer` with `args`, sending its events to the TART under test.
    pub fn run_spammer(&self, args: &[&str]) {
        let status = Command::new(&self.spammer_cmd)
            .args(args)
            .args(["--server", &self.telemetry_bind])
            .status()
            .expect("jip-3-spammer should start");
        assert!(status.success(), "jip-3-spammer failed: {status}");
    }
}

/// Runs the test scenario provided in `f`.
///
/// Before running `f`, this function prepares and launches TART. The address of the TART instance
/// is then passed to `f` using [`TestContext`].
///
/// The `unique_name` is used to derive parameters. It should be different for every test, otherwise
/// it raises a conflict when running them in parallel. It is intentionally not generated as a
/// straight-forward way to remain deterministic.
///
/// After running `f`, a cleanup of resources and a graceful shutdown of TART is performed. Because
/// `f` may panic e.g. due to a failed assertion, it has an additional constraint on [`UnwindSafe`].
/// The panic is only caught if it triggers unwind.
///
/// # Environment requirements
///
/// There are a few assumptions that need to pass in order for the test to run.
/// * ENV var `TEST_DATABASE_URL` is defined and contains a postgres URL.
/// * ENV var `TEST_SPAMMER_CMD` is defined and contains a path to executable `jip-3-spammer` binary
pub fn run_test(unique_name: &str, f: impl FnOnce(TestContext) + UnwindSafe) {
    let work_dir = env!("CARGO_MANIFEST_DIR");
    let database_url =
        std::env::var("TEST_DATABASE_URL").expect("ENV var TEST_DATABASE_URL should be set");
    let options: PgConnectOptions = database_url
        .parse()
        .expect("TEST_DATABASE_URL should be a postgres URL");

    let test_db_name = unique_name;
    let spammer_cmd =
        std::env::var("TEST_SPAMMER_CMD").expect("ENV var TEST_SPAMMER_CMD should be set");
    let tart_cmd = std::env::var("TEST_TART_CMD").expect("ENV var TEST_TART_CMD should be set");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();

    runtime
        .block_on(async {
            let mut connection = PgConnection::connect_with(&options).await?;

            sqlx::query(format!("DROP DATABASE IF EXISTS {test_db_name}").as_str())
                .execute(&mut connection)
                .await?;

            sqlx::query(format!("CREATE DATABASE {test_db_name}").as_str())
                .execute(&mut connection)
                .await?;

            Ok::<_, sqlx::Error>(())
        })
        .unwrap();

    let test_database_url = database_url_for(&database_url, test_db_name);

    // Allocate port numbers uniquely for the test
    let hash = {
        let mut hasher = DefaultHasher::new();
        unique_name.hash(&mut hasher);
        hasher.finish()
    };
    let telemetry_port = (9000 + (hash % (u16::MAX as u64 - 9000))) as u16;
    let api_port = (8080 + (hash % (u16::MAX as u64 - 8080))) as u16;

    assert_ne!(
        telemetry_port, api_port,
        "Telemetry port and API port should not match"
    );

    let telemetry_bind = format!("127.0.0.1:{telemetry_port}");
    let api_bind = format!("127.0.0.1:{api_port}");
    let telemetry_process = Command::new(&tart_cmd)
        .args(["--telemetry-bind", &telemetry_bind, "--api-bind", &api_bind])
        .env("DATABASE_URL", &test_database_url)
        .current_dir(work_dir)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    let telemetry_pid = telemetry_process.id();

    let result = std::panic::catch_unwind(|| {
        f(TestContext {
            telemetry_bind,
            api_bind,
            spammer_cmd,
        });
    });

    Command::new("kill")
        .arg("-2")
        .arg(telemetry_pid.to_string())
        .status()
        .unwrap();

    let output = telemetry_process.wait_with_output().unwrap();

    println!("{}", String::from_utf8_lossy(&output.stdout));

    runtime
        .block_on(async {
            let mut connection = PgConnection::connect(database_url.as_str()).await?;

            sqlx::query(format!("DROP DATABASE {test_db_name}").as_str())
                .execute(&mut connection)
                .await?;

            Ok::<_, sqlx::Error>(())
        })
        .unwrap();

    result.expect("test should succeed");
}

fn database_url_for(server_url: &str, database_name: &str) -> String {
    let mut url = Url::parse(server_url).expect("TEST_DATABASE_URL should be a valid URL");
    url.set_path(database_name);
    url.to_string()
}

#[test]
fn database_url_for_keeps_user_and_options_when_they_contain_the_database_name() {
    assert_eq!(
        database_url_for(
            "postgres://postgres:pw@localhost/postgres?sslmode=disable",
            "functional"
        ),
        "postgres://postgres:pw@localhost/functional?sslmode=disable"
    );
}
