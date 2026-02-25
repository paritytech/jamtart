// tart-loadgen — TCP load generator for profiling the TART backend
//
// Simulates N nodes sending binary telemetry events at configurable rates.
// Each node connects via TCP, sends a NodeInformation handshake, then
// continuously sends events using the JIP-3 wire format.
//
// Usage:
//   cargo run --bin tart-loadgen -- --nodes 100 --rate 100 --duration 30

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tart_backend::encoding::{encode_message, EncodingError};
use tart_backend::events::{Event, NodeInformation};
use tart_backend::types::*;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

struct Config {
    host: String,
    port: u16,
    nodes: usize,
    rate: u64,
    duration: u64,
    ramp: u64,
}

fn parse_config() -> Config {
    let args: Vec<String> = std::env::args().collect();

    let mut config = Config {
        host: "127.0.0.1".to_string(),
        port: 9000,
        nodes: 100,
        rate: 100,
        duration: 30,
        ramp: 2,
    };

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--host" => {
                i += 1;
                config.host = args.get(i).cloned().unwrap_or(config.host);
            }
            "--port" => {
                i += 1;
                config.port = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(config.port);
            }
            "--nodes" => {
                i += 1;
                config.nodes = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(config.nodes);
            }
            "--rate" => {
                i += 1;
                config.rate = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(config.rate);
            }
            "--duration" => {
                i += 1;
                config.duration = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(config.duration);
            }
            "--ramp" => {
                i += 1;
                config.ramp = args
                    .get(i)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(config.ramp);
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

fn print_usage() {
    eprintln!(
        r#"tart-loadgen — TCP load generator for TART backend profiling

USAGE:
    tart-loadgen [OPTIONS]

OPTIONS:
    --host <HOST>      Target host              [default: 127.0.0.1]
    --port <PORT>      Target port              [default: 9000]
    --nodes <N>        Simulated nodes          [default: 100]
    --rate <R>         Events/sec per node      [default: 100]
    --duration <SECS>  Duration (0=infinite)    [default: 30]
    --ramp <SECS>      Connection ramp-up time  [default: 2]
    --help             Print this help"#
    );
}

// ---------------------------------------------------------------------------
// Shared counters
// ---------------------------------------------------------------------------

struct Counters {
    events_sent: AtomicU64,
    errors: AtomicU64,
    connected: AtomicU64,
}

// ---------------------------------------------------------------------------
// Event generation
// ---------------------------------------------------------------------------

fn make_handshake(node_id: usize) -> Result<Vec<u8>, EncodingError> {
    let mut peer_id = [0u8; 32];
    // Encode node_id into the first 8 bytes for uniqueness
    peer_id[..8].copy_from_slice(&(node_id as u64).to_le_bytes());

    let info = NodeInformation {
        params: ProtocolParameters {
            deposit_per_item: 10_000_000,
            deposit_per_byte: 1_000,
            deposit_per_account: 100_000_000,
            core_count: 341,
            min_turnaround_period: 10,
            epoch_period: 600,
            max_accumulate_gas: 100_000_000_000,
            max_is_authorized_gas: 50_000_000_000,
            max_refine_gas: 500_000_000_000,
            block_gas_limit: 1_500_000_000_000,
            recent_block_count: 8,
            max_work_items: 16,
            max_dependencies: 8,
            max_tickets_per_block: 20,
            max_lookup_anchor_age: 14400,
            tickets_attempts_number: 8,
            auth_window: 2,
            slot_period_sec: 6,
            auth_queue_len: 80,
            rotation_period: 10,
            max_extrinsics: 16,
            availability_timeout: 8,
            val_count: 1023,
            max_authorizer_code_size: 65536,
            max_input: 1_048_576,
            max_service_code_size: 4_194_304,
            basic_piece_len: 12,
            max_imports: 128,
            segment_piece_count: 6,
            max_report_elective_data: 1024,
            transfer_memo_size: 128,
            max_exports: 128,
            epoch_tail_start: 1,
        },
        genesis: [0xAB; 32],
        details: PeerDetails {
            peer_id,
            peer_address: PeerAddress {
                ipv6: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF, 127, 0, 0, 1],
                port: 30333 + node_id as u16,
            },
        },
        flags: 0,
        implementation_name: BoundedString::new("tart-loadgen").unwrap(),
        implementation_version: BoundedString::new("0.1.0").unwrap(),
        gp_version: BoundedString::new("0.6.3").unwrap(),
        additional_info: BoundedString::new("").unwrap(),
    };

    encode_message(&info)
}

fn now_jce_micros() -> u64 {
    let unix_micros = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64;
    (unix_micros - JCE_EPOCH_UNIX_MICROS) as u64
}

fn make_event(seq: u64, slot: u32) -> Result<Vec<u8>, EncodingError> {
    let timestamp = now_jce_micros();

    // Event mix: Status 50%, BestBlockChanged 25%, FinalizedBlockChanged 15%, SyncStatusChanged 10%
    let event = match seq % 100 {
        0..50 => Event::Status {
            timestamp,
            num_peers: 42,
            num_val_peers: 30,
            num_sync_peers: 12,
            num_guarantees: vec![0u8; 341],
            num_shards: 100,
            shards_size: 50_000,
            num_preimages: 5,
            preimages_size: 1024,
        },
        50..75 => Event::BestBlockChanged {
            timestamp,
            slot,
            hash: [0xBB; 32],
        },
        75..90 => Event::FinalizedBlockChanged {
            timestamp,
            slot: slot.saturating_sub(2),
            hash: [0xFF; 32],
        },
        _ => Event::SyncStatusChanged {
            timestamp,
            synced: true,
        },
    };

    encode_message(&event)
}

// ---------------------------------------------------------------------------
// Node task
// ---------------------------------------------------------------------------

async fn run_node(
    node_id: usize,
    addr: String,
    rate: u64,
    run_until: Instant,
    counters: Arc<Counters>,
) {
    let stream = match TcpStream::connect(&addr).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("  node {}: connect failed: {}", node_id, e);
            counters.errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
    };

    // Disable Nagle for lower latency
    let _ = stream.set_nodelay(true);
    let (_, mut writer) = tokio::io::split(stream);

    // Send handshake
    let handshake = match make_handshake(node_id) {
        Ok(h) => h,
        Err(e) => {
            eprintln!("  node {}: handshake encode failed: {}", node_id, e);
            return;
        }
    };

    if let Err(e) = writer.write_all(&handshake).await {
        eprintln!("  node {}: handshake send failed: {}", node_id, e);
        counters.errors.fetch_add(1, Ordering::Relaxed);
        return;
    }

    counters.connected.fetch_add(1, Ordering::Relaxed);

    let interval = Duration::from_micros(1_000_000 / rate);
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut seq: u64 = 0;
    let mut slot: u32 = 1000;

    loop {
        ticker.tick().await;

        if Instant::now() >= run_until {
            break;
        }

        // Advance slot roughly every 6 seconds worth of events
        if seq > 0 && seq.is_multiple_of(rate * 6) {
            slot += 1;
        }

        let msg = match make_event(seq, slot) {
            Ok(m) => m,
            Err(_) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                seq += 1;
                continue;
            }
        };

        if writer.write_all(&msg).await.is_err() {
            counters.errors.fetch_add(1, Ordering::Relaxed);
            break;
        }

        counters.events_sent.fetch_add(1, Ordering::Relaxed);
        seq += 1;
    }
}

// ---------------------------------------------------------------------------
// Stats reporter
// ---------------------------------------------------------------------------

async fn report_stats(counters: Arc<Counters>, run_until: Instant) {
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    let mut prev_events: u64 = 0;

    loop {
        ticker.tick().await;

        if Instant::now() >= run_until {
            break;
        }

        let total = counters.events_sent.load(Ordering::Relaxed);
        let connected = counters.connected.load(Ordering::Relaxed);
        let errors = counters.errors.load(Ordering::Relaxed);
        let delta = total - prev_events;
        prev_events = total;

        eprintln!(
            "  [{:>3} nodes] {:>8} events/s  total={:<10}  errors={}",
            connected, delta, total, errors,
        );
    }
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() {
    let config = parse_config();
    let addr = format!("{}:{}", config.host, config.port);

    eprintln!(
        "tart-loadgen: {} nodes, {} events/s per node, {}s duration",
        config.nodes, config.rate, config.duration,
    );
    eprintln!("  target: {}", addr);

    let counters = Arc::new(Counters {
        events_sent: AtomicU64::new(0),
        errors: AtomicU64::new(0),
        connected: AtomicU64::new(0),
    });

    let run_until = if config.duration == 0 {
        Instant::now() + Duration::from_secs(86400) // effectively infinite
    } else {
        Instant::now() + Duration::from_secs(config.duration)
    };

    // Spawn stats reporter
    let counters_clone = Arc::clone(&counters);
    let stats_handle = tokio::spawn(report_stats(counters_clone, run_until));

    // Spawn node tasks with ramp-up stagger
    let ramp_delay = if config.nodes > 1 && config.ramp > 0 {
        Duration::from_millis((config.ramp * 1000) / config.nodes as u64)
    } else {
        Duration::ZERO
    };

    let mut node_handles = Vec::with_capacity(config.nodes);
    for node_id in 0..config.nodes {
        if ramp_delay > Duration::ZERO {
            tokio::time::sleep(ramp_delay).await;
        }

        let addr = addr.clone();
        let counters = Arc::clone(&counters);
        let handle = tokio::spawn(run_node(node_id, addr, config.rate, run_until, counters));
        node_handles.push(handle);
    }

    // Handle Ctrl+C for early termination
    let shutdown = tokio::signal::ctrl_c();
    tokio::select! {
        _ = async {
            for handle in &mut node_handles {
                let _ = handle.await;
            }
        } => {},
        _ = shutdown => {
            eprintln!("\n  Ctrl+C received, shutting down...");
        }
    }

    let _ = stats_handle.await;

    // Final report to stdout (machine-readable)
    let total_events = counters.events_sent.load(Ordering::Relaxed);
    let total_errors = counters.errors.load(Ordering::Relaxed);
    let elapsed = if config.duration > 0 {
        config.duration as f64
    } else {
        run_until.elapsed().as_secs_f64()
    };
    let throughput = total_events as f64 / elapsed;

    println!("--- Load Generator Report ---");
    println!("Duration:     {:.1}s", elapsed);
    println!("Nodes:        {}", config.nodes);
    println!("Events sent:  {}", format_count(total_events));
    println!(
        "Throughput:   {} events/sec",
        format_count(throughput as u64)
    );
    println!("Errors:       {}", total_errors);
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
