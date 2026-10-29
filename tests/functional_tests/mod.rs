use sqlx::postgres::PgConnectOptions;
use sqlx::{Connection, PgConnection};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::panic::UnwindSafe;
use std::process::{Command, Stdio};
use xshell::{cmd, Shell};

mod bottlenecks;

/// A collection of parameters intended for the test scenario to use to locate TART services and
/// related tools.
pub struct TestContext {
    /// A [`Shell`] to use to run commands with.
    ///
    /// The shell may come with workdir and environment variables set.
    pub sh: Shell,
    /// A string of `addr:port` where TART telemetry server is listening.
    ///
    /// This should be the server to write JIP-3 events to.
    pub telemetry_bind: String,
    /// A string of `addr:port` where TART API server is listening.
    ///
    /// This should be the server to read statistics from.
    pub api_bind: String,
    /// A filename of the CLI executable to generate input with.
    pub spammer_cmd: String,
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
    let default_db_name = options
        .get_database()
        .expect("TEST_DATABASE_URL should contain a default database name");

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

    let test_database_url = database_url.replace(default_db_name, test_db_name);
    let sh = Shell::new()
        .unwrap()
        .with_var("DATABASE_URL", test_database_url.clone())
        .with_current_dir(work_dir);

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
    let telemetry_process = cmd!(
        sh,
        "{tart_cmd} --telemetry-bind {telemetry_bind} --api-bind {api_bind}"
    )
    .to_command()
    .stdout(Stdio::piped())
    .spawn()
    .unwrap();

    let telemetry_pid = telemetry_process.id();

    let result = std::panic::catch_unwind(|| {
        f(TestContext {
            sh,
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
