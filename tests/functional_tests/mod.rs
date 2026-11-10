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
const SPAMMER_TIMEOUT: Duration = Duration::from_secs(600);
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
    api_bind: String,
    /// A filename of the CLI executable to generate input with.
    spammer_cmd: String,
    /// Output of all spammer runs in the test, printed when the test fails.
    spammer_log_path: PathBuf,
}

impl TestContext {
    /// Runs `jip-3-spammer` with whitespace-separated `args`, sending its events to the TART under
    /// test.
    pub fn run_spammer(&self, args: &str) {
        let log = File::options()
            .append(true)
            .open(&self.spammer_log_path)
            .expect("jip-3-spammer log file should be writable");
        let mut spammer = Command::new(&self.spammer_cmd)
            .args(args.split_whitespace())
            .args(["--server", &self.telemetry_bind])
            .stdout(log.try_clone().unwrap())
            .stderr(log)
            .spawn()
            .expect("jip-3-spammer should start");
        let Some(status) = wait_for_exit(&mut spammer, SPAMMER_TIMEOUT) else {
            let _ = spammer.kill();
            panic!("jip-3-spammer should finish within {SPAMMER_TIMEOUT:?}");
        };
        assert!(status.success(), "jip-3-spammer failed: {status}");
    }

    /// Sends a GET request to the TART API, e.g. `get_json("/api/health")`.
    pub fn get_json(&self, path_and_query: &str) -> serde_json::Value {
        let url = format!("http://{}{path_and_query}", self.api_bind);
        let response =
            reqwest::blocking::get(&url).unwrap_or_else(|error| panic!("GET {url}: {error}"));
        let status = response.status();
        let body = response.text().unwrap_or_default();
        assert!(status.is_success(), "GET {url} returned {status}: {body}");
        serde_json::from_str(&body)
            .unwrap_or_else(|error| panic!("GET {url} returned invalid JSON ({error}): {body}"))
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        if thread::panicking() {
            print_log(&self.spammer_log_path);
        }
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
/// e.g. due to a failed assertion. The output of TART and of the spammer is written to log files in
/// `CARGO_TARGET_TMPDIR`, and printed when the test fails.
///
/// # Environment requirements
///
/// There are a few assumptions that need to pass in order for the test to run.
/// * ENV var `TEST_DATABASE_URL` is defined and contains a postgres URL of a TimescaleDB server.
///   Its user must be allowed to create databases.
/// * ENV var `TEST_SPAMMER_CMD` is defined and contains a path to executable `jip-3-spammer` binary
/// * ENV var `TEST_TART_CMD` is defined and contains a path to executable `tart-backend` binary
pub fn run_test(unique_name: &str, f: impl FnOnce(TestContext)) {
    let _one_test_at_a_time = ONE_TEST_AT_A_TIME
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    let database_url = required_env("TEST_DATABASE_URL");
    let tart_cmd = required_env("TEST_TART_CMD");
    let spammer_cmd = required_env("TEST_SPAMMER_CMD");

    let database = TestDatabase::create(&database_url, unique_name);
    let tart = Tart::start(&tart_cmd, &database.url, unique_name);

    let spammer_log_path = log_path(unique_name, "jip-3-spammer");
    File::create(&spammer_log_path).expect("jip-3-spammer log file should be writable");

    f(TestContext {
        telemetry_bind: tart.telemetry_bind.clone(),
        api_bind: tart.api_bind.clone(),
        spammer_cmd,
        spammer_log_path,
    });
}

fn log_path(test_name: &str, program: &str) -> PathBuf {
    Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("{test_name}-{program}.log"))
}

fn print_log(path: &Path) {
    match std::fs::read_to_string(path) {
        Ok(log) => println!("Log {}:\n{log}", path.display()),
        Err(error) => println!("Could not read log {}: {error}", path.display()),
    }
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
        let [telemetry_bind, api_bind] = free_local_addresses();
        let log_path = log_path(name, "tart");
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
            print_log(&self.log_path);
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

fn free_local_addresses<const COUNT: usize>() -> [String; COUNT] {
    let listeners =
        [(); COUNT].map(|()| TcpListener::bind("127.0.0.1:0").expect("should find a free port"));
    listeners.map(|listener| listener.local_addr().unwrap().to_string())
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
