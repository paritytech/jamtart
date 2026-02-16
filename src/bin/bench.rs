// tart-bench — Frontend-realistic benchmark for TART backend
//
// Simulates real Next.js/SWR frontend access patterns against a running backend.
// Each "browser tab" runs independent polling loops at the exact intervals the
// frontend uses, plus a persistent WebSocket connection.
//
// Usage:
//   cargo run --features bench --bin tart-bench -- --tabs 10 --duration 30
//
// Environment variables:
//   TART_BENCH_URL       Base URL        [default: http://localhost:8080]
//   TART_BENCH_TABS      Tab count       [default: 10]
//   TART_BENCH_DURATION  Duration secs   [default: 30]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{SplitSink, StreamExt};
use futures::SinkExt;
use hdrhistogram::Histogram;
use reqwest::Client;
use serde::Serialize;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// Endpoint definitions — exact intervals from frontend SWR hooks
// ---------------------------------------------------------------------------

/// (path, poll_interval_ms)
const DASHBOARD_ENDPOINTS: &[(&str, u64)] = &[
    ("/api/metrics/live", 1_000),
    ("/api/metrics/realtime?seconds=60", 1_000),
    ("/api/stats", 5_000),
    ("/api/workpackages/active", 5_000),
    ("/api/blocks", 10_000),
    ("/api/cores/status", 10_000),
    ("/api/events/search?event_types=43,44,45,46,47,11,12&limit=500", 10_000),
    ("/api/analytics/failure-rates", 12_000),
    ("/api/analytics/network-health", 12_000),
    ("/api/metrics/timeseries", 12_000),
    ("/api/analytics/block-propagation", 15_000),
    ("/api/workpackages", 15_000),
    ("/api/guarantees", 15_000),
    ("/api/da/stats", 15_000),
    ("/api/guarantees/by-guarantor", 30_000),
    ("/api/nodes", 30_000),
];

/// Additional endpoints when drilling into a core detail page
const CORE_DETAIL_ENDPOINTS: &[(&str, u64)] = &[
    ("/api/cores/{core}/validators", 15_000),
    ("/api/cores/{core}/metrics", 15_000),
    ("/api/cores/{core}/bottlenecks", 15_000),
    ("/api/cores/{core}/guarantors/enhanced", 15_000),
    ("/api/cores/{core}/work-packages", 15_000),
];

/// Additional endpoints when viewing a work package detail page
const WP_DETAIL_ENDPOINTS: &[(&str, u64)] = &[
    ("/api/workpackages/{hash}/journey/enhanced", 10_000),
    ("/api/workpackages/{hash}/audit-progress", 10_000),
];

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct BenchConfig {
    base_url: String,
    tabs: usize,
    duration: Duration,
    warmup: Duration,
    scenario: Scenario,
    core_index: u32,
    wp_hash: String,
    json_output: bool,
}

#[derive(Clone, Copy, PartialEq)]
enum Scenario {
    Dashboard,
    CoreDetail,
    WpDetail,
    HttpOnly,
    WsOnly,
}

impl Scenario {
    fn name(&self) -> &'static str {
        match self {
            Scenario::Dashboard => "dashboard",
            Scenario::CoreDetail => "core-detail",
            Scenario::WpDetail => "wp-detail",
            Scenario::HttpOnly => "http-only",
            Scenario::WsOnly => "ws-only",
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        match s {
            "dashboard" => Some(Self::Dashboard),
            "core-detail" => Some(Self::CoreDetail),
            "wp-detail" => Some(Self::WpDetail),
            "http-only" => Some(Self::HttpOnly),
            "ws-only" => Some(Self::WsOnly),
            _ => None,
        }
    }
}

fn parse_config() -> BenchConfig {
    let args: Vec<String> = std::env::args().collect();

    let mut config = BenchConfig {
        base_url: env_or_default("TART_BENCH_URL", "http://localhost:8080"),
        tabs: env_or_default("TART_BENCH_TABS", "10").parse().unwrap_or(10),
        duration: Duration::from_secs(
            env_or_default("TART_BENCH_DURATION", "30")
                .parse()
                .unwrap_or(30),
        ),
        warmup: Duration::from_secs(5),
        scenario: Scenario::Dashboard,
        core_index: 0,
        wp_hash: String::new(),
        json_output: false,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--url" => {
                i += 1;
                config.base_url = args.get(i).cloned().unwrap_or(config.base_url);
            }
            "--tabs" => {
                i += 1;
                config.tabs = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(config.tabs);
            }
            "--duration" => {
                i += 1;
                config.duration = Duration::from_secs(
                    args.get(i).and_then(|s| s.parse().ok()).unwrap_or(30),
                );
            }
            "--warmup" => {
                i += 1;
                config.warmup = Duration::from_secs(
                    args.get(i).and_then(|s| s.parse().ok()).unwrap_or(5),
                );
            }
            "--scenario" => {
                i += 1;
                if let Some(s) = args.get(i).and_then(|s| Scenario::from_str(s)) {
                    config.scenario = s;
                }
            }
            "--core" => {
                i += 1;
                config.core_index = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(0);
            }
            "--wp-hash" => {
                i += 1;
                config.wp_hash = args.get(i).cloned().unwrap_or_default();
            }
            "--json" => {
                config.json_output = true;
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            _ => {}
        }
        i += 1;
    }

    config
}

fn env_or_default(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn print_usage() {
    eprintln!(
        r#"tart-bench — Frontend-realistic benchmark for TART backend

USAGE:
    tart-bench [OPTIONS]

OPTIONS:
    --url <URL>          Target URL          [default: http://localhost:8080]
    --tabs <N>           Simulated tabs      [default: 10]
    --duration <SECS>    Measurement time    [default: 30]
    --warmup <SECS>      Warmup period       [default: 5]
    --scenario <NAME>    dashboard|core-detail|wp-detail|http-only|ws-only
    --core <INDEX>       Core index          [default: 0]
    --wp-hash <HASH>     WP hash for wp-detail scenario
    --json               Output JSON instead of table
    --help               Print this help

ENV VARS: TART_BENCH_URL, TART_BENCH_TABS, TART_BENCH_DURATION"#
    );
}

// ---------------------------------------------------------------------------
// Metrics collection
// ---------------------------------------------------------------------------

/// Latency sample: (endpoint_path, latency_microseconds, is_error)
type Sample = (String, u64, bool);

/// Per-tab metrics — no sharing, no locking during measurement
struct EndpointMetrics {
    histograms: HashMap<String, Histogram<u64>>,
    error_counts: HashMap<String, u64>,
    request_counts: HashMap<String, u64>,
}

impl EndpointMetrics {
    fn new() -> Self {
        Self {
            histograms: HashMap::new(),
            error_counts: HashMap::new(),
            request_counts: HashMap::new(),
        }
    }

    fn record(&mut self, endpoint: &str, latency_us: u64, is_error: bool) {
        let hist = self
            .histograms
            .entry(endpoint.to_string())
            .or_insert_with(|| Histogram::new_with_bounds(1, 60_000_000, 3).unwrap());
        let _ = hist.record(latency_us);
        *self.request_counts.entry(endpoint.to_string()).or_insert(0) += 1;
        if is_error {
            *self.error_counts.entry(endpoint.to_string()).or_insert(0) += 1;
        }
    }

    /// Merge another set of metrics into this one
    fn merge(&mut self, other: &EndpointMetrics) {
        for (endpoint, hist) in &other.histograms {
            let entry = self
                .histograms
                .entry(endpoint.clone())
                .or_insert_with(|| Histogram::new_with_bounds(1, 60_000_000, 3).unwrap());
            entry.add(hist).ok();
        }
        for (endpoint, count) in &other.request_counts {
            *self.request_counts.entry(endpoint.clone()).or_insert(0) += count;
        }
        for (endpoint, count) in &other.error_counts {
            *self.error_counts.entry(endpoint.clone()).or_insert(0) += count;
        }
    }
}

#[derive(Default)]
struct WsMetrics {
    connect_latency_us: u64,
    connected: bool,
    messages_received: u64,
    events_received: u64,
    stats_received: u64,
    metrics_received: u64,
}

// ---------------------------------------------------------------------------
// Main orchestration
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let config = Arc::new(parse_config());

    if !config.json_output {
        eprintln!(
            "tart-bench: {} tabs, {}s duration (+{}s warmup), scenario={}",
            config.tabs,
            config.duration.as_secs(),
            config.warmup.as_secs(),
            config.scenario.name()
        );
        eprintln!("  target: {}", config.base_url);
    }

    // Verify server is reachable
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .pool_max_idle_per_host(20)
        .tcp_keepalive(Duration::from_secs(60))
        .build()
        .expect("Failed to create HTTP client");

    match client
        .get(format!("{}/api/health", config.base_url))
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => {
            if !config.json_output {
                eprintln!("  server: healthy");
            }
        }
        Ok(resp) => {
            eprintln!(
                "ERROR: Server returned status {} for /api/health",
                resp.status()
            );
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("ERROR: Cannot reach server at {}: {}", config.base_url, e);
            std::process::exit(1);
        }
    }

    let warmup_ends = Instant::now() + config.warmup;
    let run_ends = warmup_ends + config.duration;

    // Spawn browser tab tasks
    let mut tab_handles = Vec::with_capacity(config.tabs);
    for tab_id in 0..config.tabs {
        let config = Arc::clone(&config);
        let client = client.clone();
        let handle = tokio::spawn(async move {
            // Stagger start to avoid artificial thundering herd
            tokio::time::sleep(Duration::from_millis(tab_id as u64 * 50)).await;
            run_browser_tab(tab_id, config, client, warmup_ends, run_ends).await
        });
        tab_handles.push(handle);
    }

    // Wait for run to complete
    let total_wait = config.warmup + config.duration + Duration::from_secs(2); // +2s grace
    tokio::time::sleep(total_wait).await;

    if !config.json_output {
        eprintln!("  collecting results...");
    }

    // Collect results from all tabs
    let mut merged_metrics = EndpointMetrics::new();
    let mut ws_metrics_all: Vec<WsMetrics> = Vec::new();

    for handle in tab_handles {
        match tokio::time::timeout(Duration::from_secs(5), handle).await {
            Ok(Ok((metrics, ws_metrics))) => {
                merged_metrics.merge(&metrics);
                ws_metrics_all.push(ws_metrics);
            }
            Ok(Err(e)) => eprintln!("  tab task panicked: {}", e),
            Err(_) => eprintln!("  tab task timed out during collection"),
        }
    }

    // Generate report
    if config.json_output {
        print_json_report(&config, &merged_metrics, &ws_metrics_all);
    } else {
        print_table_report(&config, &merged_metrics, &ws_metrics_all);
    }
}

// ---------------------------------------------------------------------------
// Browser tab simulation
// ---------------------------------------------------------------------------

async fn run_browser_tab(
    _tab_id: usize,
    config: Arc<BenchConfig>,
    client: Client,
    warmup_ends: Instant,
    run_ends: Instant,
) -> (EndpointMetrics, WsMetrics) {
    let (sample_tx, mut sample_rx) = mpsc::unbounded_channel::<Sample>();

    let mut poller_handles = Vec::new();

    // Spawn HTTP pollers based on scenario
    if config.scenario != Scenario::WsOnly {
        let endpoints = build_endpoint_list(&config);
        for (path, interval_ms) in endpoints {
            let client = client.clone();
            let base_url = config.base_url.clone();
            let tx = sample_tx.clone();
            let handle = tokio::spawn(run_poller(
                client,
                base_url,
                path,
                Duration::from_millis(interval_ms),
                warmup_ends,
                run_ends,
                tx,
            ));
            poller_handles.push(handle);
        }
    }

    // Spawn WebSocket task
    let ws_handle = if config.scenario != Scenario::HttpOnly {
        let ws_url = config
            .base_url
            .replace("http://", "ws://")
            .replace("https://", "wss://")
            + "/api/ws";
        Some(tokio::spawn(run_websocket(ws_url, run_ends)))
    } else {
        None
    };

    // Drop our copy of the sender so the channel closes when all pollers finish
    drop(sample_tx);

    // Collect samples into per-tab histograms
    let mut metrics = EndpointMetrics::new();
    while let Some((endpoint, latency_us, is_error)) = sample_rx.recv().await {
        metrics.record(&endpoint, latency_us, is_error);
    }

    // Wait for pollers to finish
    for handle in poller_handles {
        let _ = handle.await;
    }

    // Collect WebSocket metrics
    let ws_metrics = if let Some(handle) = ws_handle {
        match tokio::time::timeout(Duration::from_secs(3), handle).await {
            Ok(Ok(m)) => m,
            _ => WsMetrics::default(),
        }
    } else {
        WsMetrics::default()
    };

    (metrics, ws_metrics)
}

fn build_endpoint_list(config: &BenchConfig) -> Vec<(String, u64)> {
    let mut endpoints: Vec<(String, u64)> = DASHBOARD_ENDPOINTS
        .iter()
        .map(|(p, i)| (p.to_string(), *i))
        .collect();

    match config.scenario {
        Scenario::CoreDetail => {
            for (path_template, interval) in CORE_DETAIL_ENDPOINTS {
                let path = path_template.replace("{core}", &config.core_index.to_string());
                endpoints.push((path, *interval));
            }
        }
        Scenario::WpDetail => {
            if !config.wp_hash.is_empty() {
                for (path_template, interval) in WP_DETAIL_ENDPOINTS {
                    let path = path_template.replace("{hash}", &config.wp_hash);
                    endpoints.push((path, *interval));
                }
            }
        }
        _ => {}
    }

    endpoints
}

// ---------------------------------------------------------------------------
// HTTP Poller
// ---------------------------------------------------------------------------

async fn run_poller(
    client: Client,
    base_url: String,
    path: String,
    interval: Duration,
    warmup_ends: Instant,
    run_ends: Instant,
    tx: mpsc::UnboundedSender<Sample>,
) {
    let url = format!("{}{}", base_url, path);
    let mut ticker = tokio::time::interval(interval);
    let run_ends_tokio = tokio::time::Instant::from_std(run_ends);

    loop {
        // Race tick against run deadline — without this, 30s-interval pollers
        // would block for up to 30s after the run ends before checking the exit
        tokio::select! {
            _ = ticker.tick() => {}
            _ = tokio::time::sleep_until(run_ends_tokio) => break,
        }

        if Instant::now() >= run_ends {
            break;
        }

        let start = Instant::now();
        let result = client.get(&url).send().await;
        let elapsed_us = start.elapsed().as_micros() as u64;

        let is_error = match &result {
            Ok(resp) => !resp.status().is_success(),
            Err(_) => true,
        };

        // Consume body to release connection back to pool
        if let Ok(resp) = result {
            let _ = resp.bytes().await;
        }

        // Only record samples after warmup
        if Instant::now() >= warmup_ends
            && tx.send((path.clone(), elapsed_us, is_error)).is_err()
        {
            break; // receiver dropped
        }
    }
}

// ---------------------------------------------------------------------------
// WebSocket client
// ---------------------------------------------------------------------------

async fn run_websocket(ws_url: String, run_ends: Instant) -> WsMetrics {
    let mut metrics = WsMetrics::default();

    let connect_start = Instant::now();
    let ws_result = tokio_tungstenite::connect_async(&ws_url).await;
    metrics.connect_latency_us = connect_start.elapsed().as_micros() as u64;

    let (ws_stream, _) = match ws_result {
        Ok(s) => {
            metrics.connected = true;
            s
        }
        Err(e) => {
            eprintln!("  WebSocket connect failed: {}", e);
            return metrics;
        }
    };

    let (mut write, mut read) = ws_stream.split();

    // Subscribe to all events (matching frontend behavior)
    send_ws_json(
        &mut write,
        &serde_json::json!({
            "type": "Subscribe",
            "filter": { "type": "All" }
        }),
    )
    .await;

    // Subscribe to metrics channel
    send_ws_json(
        &mut write,
        &serde_json::json!({
            "type": "SubscribeMetrics",
            "interval_ms": 1000
        }),
    )
    .await;

    let mut ping_interval = tokio::time::interval(Duration::from_secs(30));
    // Consume the first immediate tick
    ping_interval.tick().await;

    loop {
        if Instant::now() >= run_ends {
            let _ = write.send(Message::Close(None)).await;
            break;
        }

        tokio::select! {
            msg = read.next() => {
                match msg {
                    Some(Ok(Message::Text(text))) => {
                        metrics.messages_received += 1;
                        if let Ok(val) = serde_json::from_str::<serde_json::Value>(&text) {
                            match val.get("type").and_then(|t| t.as_str()) {
                                Some("event") => metrics.events_received += 1,
                                Some("stats") => metrics.stats_received += 1,
                                Some("metrics") => metrics.metrics_received += 1,
                                _ => {}
                            }
                        }
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    _ => {}
                }
            }
            _ = ping_interval.tick() => {
                send_ws_json(&mut write, &serde_json::json!({"type": "Ping"})).await;
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(run_ends)) => {
                let _ = write.send(Message::Close(None)).await;
                break;
            }
        }
    }

    metrics
}

async fn send_ws_json(
    write: &mut SplitSink<
        tokio_tungstenite::WebSocketStream<
            tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
        >,
        Message,
    >,
    msg: &serde_json::Value,
) {
    let _ = write.send(Message::Text(msg.to_string())).await;
}

// ---------------------------------------------------------------------------
// Report formatting — table output
// ---------------------------------------------------------------------------

fn print_table_report(
    config: &BenchConfig,
    metrics: &EndpointMetrics,
    ws_all: &[WsMetrics],
) {
    let total_reqs: u64 = metrics.request_counts.values().sum();
    let total_errs: u64 = metrics.error_counts.values().sum();
    let err_pct = if total_reqs > 0 {
        (total_errs as f64 / total_reqs as f64) * 100.0
    } else {
        0.0
    };
    let rps = total_reqs as f64 / config.duration.as_secs_f64();

    println!();
    println!("================================================================================");
    println!("  TART Frontend Benchmark");
    println!("================================================================================");
    println!("  Target:      {}", config.base_url);
    println!("  Scenario:    {}", config.scenario.name());
    println!(
        "  Tabs:        {} simulated browser tabs",
        config.tabs
    );
    println!(
        "  Duration:    {:.1}s (+ {:.1}s warmup)",
        config.duration.as_secs_f64(),
        config.warmup.as_secs_f64()
    );
    println!("  Total Reqs:  {}", format_count(total_reqs));
    println!(
        "  Errors:      {} ({:.2}%)",
        format_count(total_errs),
        err_pct
    );
    println!("  Throughput:  {:.1} req/s", rps);
    println!("================================================================================");
    println!();

    // Sort endpoints by their poll interval (fastest first), using the defined order
    let endpoint_order = build_endpoint_list(config);
    let ordered_keys: Vec<String> = endpoint_order.iter().map(|(p, _)| p.clone()).collect();

    println!(
        "  {:<42} {:>6} {:>6} {:>7} {:>7} {:>7} {:>7}",
        "Endpoint", "Reqs", "Err%", "p50", "p95", "p99", "Max"
    );
    println!(
        "  {}",
        "\u{2500}".repeat(82)
    );

    // Aggregate histogram for the TOTAL row
    let mut total_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();

    for key in &ordered_keys {
        if let Some(hist) = metrics.histograms.get(key) {
            let reqs = metrics.request_counts.get(key).copied().unwrap_or(0);
            let errs = metrics.error_counts.get(key).copied().unwrap_or(0);
            let e_pct = if reqs > 0 {
                (errs as f64 / reqs as f64) * 100.0
            } else {
                0.0
            };

            println!(
                "  {:<42} {:>6} {:>5.1}% {:>7} {:>7} {:>7} {:>7}",
                truncate_path(key, 42),
                reqs,
                e_pct,
                format_latency(hist.value_at_quantile(0.50)),
                format_latency(hist.value_at_quantile(0.95)),
                format_latency(hist.value_at_quantile(0.99)),
                format_latency(hist.max()),
            );

            total_hist.add(hist).ok();
        }
    }

    // Also include any endpoints not in the predefined order
    for (key, hist) in &metrics.histograms {
        if !ordered_keys.contains(key) {
            let reqs = metrics.request_counts.get(key).copied().unwrap_or(0);
            let errs = metrics.error_counts.get(key).copied().unwrap_or(0);
            let e_pct = if reqs > 0 {
                (errs as f64 / reqs as f64) * 100.0
            } else {
                0.0
            };
            println!(
                "  {:<42} {:>6} {:>5.1}% {:>7} {:>7} {:>7} {:>7}",
                truncate_path(key, 42),
                reqs,
                e_pct,
                format_latency(hist.value_at_quantile(0.50)),
                format_latency(hist.value_at_quantile(0.95)),
                format_latency(hist.value_at_quantile(0.99)),
                format_latency(hist.max()),
            );
            total_hist.add(hist).ok();
        }
    }

    println!(
        "  {}",
        "\u{2500}".repeat(82)
    );

    if !total_hist.is_empty() {
        println!(
            "  {:<42} {:>6} {:>5.1}% {:>7} {:>7} {:>7} {:>7}",
            "TOTAL",
            total_reqs,
            err_pct,
            format_latency(total_hist.value_at_quantile(0.50)),
            format_latency(total_hist.value_at_quantile(0.95)),
            format_latency(total_hist.value_at_quantile(0.99)),
            format_latency(total_hist.max()),
        );
    }

    // WebSocket summary
    if !ws_all.is_empty() && config.scenario != Scenario::HttpOnly {
        let connected = ws_all.iter().filter(|m| m.connected).count();
        let total_events: u64 = ws_all.iter().map(|m| m.events_received).sum();
        let total_stats: u64 = ws_all.iter().map(|m| m.stats_received).sum();
        let total_metrics_ws: u64 = ws_all.iter().map(|m| m.metrics_received).sum();
        let total_msgs: u64 = ws_all.iter().map(|m| m.messages_received).sum();

        // Build connect latency histogram
        let mut connect_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
        for m in ws_all.iter().filter(|m| m.connected) {
            let _ = connect_hist.record(m.connect_latency_us);
        }

        println!();
        println!("  WebSocket");
        println!(
            "  {}",
            "\u{2500}".repeat(82)
        );
        println!(
            "  Connections:    {} ({} successful)",
            ws_all.len(),
            connected
        );
        if !connect_hist.is_empty() {
            println!(
                "  Connect time:   p50={:<8} p95={:<8} p99={}",
                format_latency(connect_hist.value_at_quantile(0.50)),
                format_latency(connect_hist.value_at_quantile(0.95)),
                format_latency(connect_hist.value_at_quantile(0.99)),
            );
        }
        println!(
            "  Messages rcvd:  {} total ({:.1}/tab)",
            format_count(total_msgs),
            total_msgs as f64 / ws_all.len().max(1) as f64,
        );
        println!(
            "  Events rcvd:    {} total ({:.1}/tab)",
            format_count(total_events),
            total_events as f64 / ws_all.len().max(1) as f64,
        );
        println!(
            "  Stats pushes:   {} total ({:.1}/tab)",
            format_count(total_stats),
            total_stats as f64 / ws_all.len().max(1) as f64,
        );
        println!(
            "  Metrics pushes: {} total ({:.1}/tab)",
            format_count(total_metrics_ws),
            total_metrics_ws as f64 / ws_all.len().max(1) as f64,
        );
        println!(
            "  {}",
            "\u{2500}".repeat(82)
        );
    }

    println!();
}

// ---------------------------------------------------------------------------
// Report formatting — JSON output
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct JsonReport {
    config: JsonConfig,
    summary: JsonSummary,
    endpoints: Vec<JsonEndpoint>,
    websocket: JsonWebSocket,
}

#[derive(Serialize)]
struct JsonConfig {
    url: String,
    scenario: String,
    tabs: usize,
    duration_secs: f64,
    warmup_secs: f64,
}

#[derive(Serialize)]
struct JsonSummary {
    total_requests: u64,
    total_errors: u64,
    error_rate_pct: f64,
    throughput_rps: f64,
}

#[derive(Serialize)]
struct JsonEndpoint {
    path: String,
    requests: u64,
    errors: u64,
    error_rate_pct: f64,
    p50_us: u64,
    p95_us: u64,
    p99_us: u64,
    max_us: u64,
    mean_us: f64,
}

#[derive(Serialize)]
struct JsonWebSocket {
    tabs: usize,
    connected: usize,
    connect_p50_us: u64,
    connect_p95_us: u64,
    connect_p99_us: u64,
    total_messages: u64,
    total_events: u64,
    total_stats: u64,
    total_metrics: u64,
}

fn print_json_report(
    config: &BenchConfig,
    metrics: &EndpointMetrics,
    ws_all: &[WsMetrics],
) {
    let total_reqs: u64 = metrics.request_counts.values().sum();
    let total_errs: u64 = metrics.error_counts.values().sum();

    let mut endpoints: Vec<JsonEndpoint> = Vec::new();
    let endpoint_order = build_endpoint_list(config);

    // Ordered endpoints first
    for (path, _) in &endpoint_order {
        if let Some(hist) = metrics.histograms.get(path) {
            let reqs = metrics.request_counts.get(path).copied().unwrap_or(0);
            let errs = metrics.error_counts.get(path).copied().unwrap_or(0);
            endpoints.push(JsonEndpoint {
                path: path.clone(),
                requests: reqs,
                errors: errs,
                error_rate_pct: if reqs > 0 {
                    (errs as f64 / reqs as f64) * 100.0
                } else {
                    0.0
                },
                p50_us: hist.value_at_quantile(0.50),
                p95_us: hist.value_at_quantile(0.95),
                p99_us: hist.value_at_quantile(0.99),
                max_us: hist.max(),
                mean_us: hist.mean(),
            });
        }
    }

    // Build WS metrics
    let connected = ws_all.iter().filter(|m| m.connected).count();
    let mut connect_hist = Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).unwrap();
    for m in ws_all.iter().filter(|m| m.connected) {
        let _ = connect_hist.record(m.connect_latency_us);
    }

    let report = JsonReport {
        config: JsonConfig {
            url: config.base_url.clone(),
            scenario: config.scenario.name().to_string(),
            tabs: config.tabs,
            duration_secs: config.duration.as_secs_f64(),
            warmup_secs: config.warmup.as_secs_f64(),
        },
        summary: JsonSummary {
            total_requests: total_reqs,
            total_errors: total_errs,
            error_rate_pct: if total_reqs > 0 {
                (total_errs as f64 / total_reqs as f64) * 100.0
            } else {
                0.0
            },
            throughput_rps: total_reqs as f64 / config.duration.as_secs_f64(),
        },
        endpoints,
        websocket: JsonWebSocket {
            tabs: ws_all.len(),
            connected,
            connect_p50_us: if !connect_hist.is_empty() {
                connect_hist.value_at_quantile(0.50)
            } else {
                0
            },
            connect_p95_us: if !connect_hist.is_empty() {
                connect_hist.value_at_quantile(0.95)
            } else {
                0
            },
            connect_p99_us: if !connect_hist.is_empty() {
                connect_hist.value_at_quantile(0.99)
            } else {
                0
            },
            total_messages: ws_all.iter().map(|m| m.messages_received).sum(),
            total_events: ws_all.iter().map(|m| m.events_received).sum(),
            total_stats: ws_all.iter().map(|m| m.stats_received).sum(),
            total_metrics: ws_all.iter().map(|m| m.metrics_received).sum(),
        },
    };

    println!("{}", serde_json::to_string_pretty(&report).unwrap());
}

// ---------------------------------------------------------------------------
// Formatting helpers
// ---------------------------------------------------------------------------

fn format_latency(us: u64) -> String {
    if us < 1_000 {
        format!("{}us", us)
    } else if us < 1_000_000 {
        format!("{:.1}ms", us as f64 / 1_000.0)
    } else {
        format!("{:.2}s", us as f64 / 1_000_000.0)
    }
}

fn format_count(n: u64) -> String {
    if n < 1_000 {
        n.to_string()
    } else if n < 1_000_000 {
        format!("{},{:03}", n / 1_000, n % 1_000)
    } else {
        format!(
            "{},{:03},{:03}",
            n / 1_000_000,
            (n % 1_000_000) / 1_000,
            n % 1_000
        )
    }
}

fn truncate_path(path: &str, max_len: usize) -> String {
    if path.len() <= max_len {
        path.to_string()
    } else {
        format!("...{}", &path[path.len() - (max_len - 3)..])
    }
}
