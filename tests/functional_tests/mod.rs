use nix::sys::signal::{kill, Signal};
use nix::unistd::Pid;
use sqlx::{Connection, PgConnection};
use std::fs::File;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus};
use std::sync::{Mutex, PoisonError};
use std::thread;
use std::time::{Duration, Instant};
use url::Url;

mod bottlenecks;

static ONE_TEST_AT_A_TIME: Mutex<()> = Mutex::new(());

const TART_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const TART_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(200);

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
/// The `unique_name` is used as the name of the test database, so every test starts from an empty
/// one. It is intentionally not generated as a straight-forward way to remain deterministic.
///
/// Only one test runs at a time, even when the test harness runs them on multiple threads. The
/// tests are meant to catch performance regressions, so they must not compete for resources.
///
/// TART is shut down gracefully and the test database is dropped afterwards, also when `f` panics
/// e.g. due to a failed assertion. TART's output is written to a log file in
/// `CARGO_TARGET_TMPDIR`, and printed when the test fails.
///
/// # Environment requirements
///
/// There are a few assumptions that need to pass in order for the test to run.
/// * ENV var `TEST_DATABASE_URL` is defined and contains a postgres URL.
/// * ENV var `TEST_SPAMMER_CMD` is defined and contains a path to executable `jip-3-spammer` binary
pub fn run_test(unique_name: &str, f: impl FnOnce(TestContext)) {
    let _one_test_at_a_time = ONE_TEST_AT_A_TIME
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let database = TestDatabase::create(&required_env("TEST_DATABASE_URL"), unique_name);
    let tart = Tart::start(&required_env("TEST_TART_CMD"), &database.url, unique_name);

    f(TestContext {
        telemetry_bind: tart.telemetry_bind.clone(),
        api_bind: tart.api_bind.clone(),
        spammer_cmd: required_env("TEST_SPAMMER_CMD"),
    });
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("ENV var {name} should be set"))
}

struct TestDatabase {
    server_url: String,
    name: String,
    url: String,
}

impl TestDatabase {
    fn create(server_url: &str, name: &str) -> Self {
        execute_on_server(
            server_url,
            &[
                format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"),
                format!("CREATE DATABASE {name}"),
            ],
        )
        .expect("test database should be created");
        Self {
            server_url: server_url.to_string(),
            name: name.to_string(),
            url: database_url_for(server_url, name),
        }
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let drop_database = format!("DROP DATABASE {} WITH (FORCE)", self.name);
        if let Err(error) = execute_on_server(&self.server_url, &[drop_database]) {
            println!("Could not drop test database {}: {error}", self.name);
        }
    }
}

fn execute_on_server(server_url: &str, statements: &[String]) -> Result<(), sqlx::Error> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime should start");
    runtime.block_on(async {
        let mut connection = PgConnection::connect(server_url).await?;
        for statement in statements {
            sqlx::query(statement).execute(&mut connection).await?;
        }
        Ok(())
    })
}

struct Tart {
    process: Child,
    telemetry_bind: String,
    api_bind: String,
    log_path: PathBuf,
}

impl Tart {
    fn start(tart_cmd: &str, database_url: &str, name: &str) -> Self {
        let telemetry_bind = format!("127.0.0.1:{}", free_local_port());
        let api_bind = format!("127.0.0.1:{}", free_local_port());
        let log_path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{name}-tart.log"));
        let log = File::create(&log_path).expect("TART log file should be writable");
        let process = Command::new(tart_cmd)
            .args(["--telemetry-bind", &telemetry_bind, "--api-bind", &api_bind])
            .env("DATABASE_URL", database_url)
            .current_dir(env!("CARGO_MANIFEST_DIR"))
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .expect("TART should start");
        let mut tart = Self {
            process,
            telemetry_bind,
            api_bind,
            log_path,
        };
        tart.wait_until_ready();
        tart
    }

    fn wait_until_ready(&mut self) {
        let health_url = format!("http://{}/api/health", self.api_bind);
        let deadline = Instant::now() + TART_STARTUP_TIMEOUT;
        while !reqwest::blocking::get(&health_url)
            .is_ok_and(|response| response.status().is_success())
        {
            if let Ok(Some(status)) = self.process.try_wait() {
                panic!("TART exited during startup: {status}");
            }
            assert!(
                Instant::now() < deadline,
                "TART should be ready within {TART_STARTUP_TIMEOUT:?}"
            );
            thread::sleep(POLL_INTERVAL);
        }
    }

    fn print_log(&self) {
        match std::fs::read_to_string(&self.log_path) {
            Ok(log) => println!("TART log ({}):\n{log}", self.log_path.display()),
            Err(error) => println!(
                "Could not read TART log {}: {error}",
                self.log_path.display()
            ),
        }
    }
}

impl Drop for Tart {
    fn drop(&mut self) {
        let _ = kill(Pid::from_raw(self.process.id() as i32), Signal::SIGTERM);
        let stopped_in_time = wait_for_exit(&mut self.process, TART_SHUTDOWN_TIMEOUT).is_some();
        if !stopped_in_time {
            let _ = self.process.kill();
            let _ = self.process.wait();
        }

        if thread::panicking() || !stopped_in_time {
            self.print_log();
        }
        assert!(
            stopped_in_time || thread::panicking(),
            "TART should shut down within {TART_SHUTDOWN_TIMEOUT:?}"
        );
    }
}

fn wait_for_exit(process: &mut Child, timeout: Duration) -> Option<ExitStatus> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(Some(status)) = process.try_wait() {
            return Some(status);
        }
        if Instant::now() >= deadline {
            return None;
        }
        thread::sleep(POLL_INTERVAL);
    }
}

fn free_local_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("should find a free port");
    listener.local_addr().unwrap().port()
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
