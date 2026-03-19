//! Application entrypoint. Parses CLI arguments, initializes the database,
//! telemetry server, API server, and background flush tasks, then runs until
//! shutdown.

#[cfg(feature = "profiling")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(all(feature = "jemalloc", not(feature = "profiling")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tart_backend::api::{create_api_router, create_minimal_router, ApiState, MinimalApiState};
use tart_backend::health::{checks, HealthMonitor};
use tart_backend::jam_rpc::JamRpcClient;
use tart_backend::{EventStore, TelemetryServer};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser)]
#[command(name = "tart-backend", about = "TART Telemetry Backend")]
struct Cli {
    /// Skip database connection — only broadcast events via WebSocket
    #[arg(long)]
    no_database: bool,

    /// Disable rate limiting for incoming events
    #[arg(long)]
    no_rate_limit: bool,

    /// Database URL (can also be set via DATABASE_URL env var)
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// Telemetry TCP bind address
    #[arg(long, env = "TELEMETRY_BIND", default_value = "0.0.0.0:9000")]
    telemetry_bind: String,

    /// HTTP API bind address
    #[arg(long, env = "API_BIND", default_value = "0.0.0.0:8080")]
    api_bind: String,

    /// Number of dedicated ingestion runtimes for TCP connections.
    /// Each runtime is a single-thread tokio runtime with its own SO_REUSEPORT listener.
    /// 0 = legacy single-runtime mode (all tasks on main runtime).
    #[arg(long, env = "INGESTION_THREADS", default_value_t = 8)]
    ingestion_threads: usize,
}

/// Configure TCP socket buffer sizes for better performance.
///
/// Sets SO_RCVBUF and SO_SNDBUF to 256KB for improved throughput with many concurrent connections.
#[cfg(unix)]
fn configure_socket_buffers(listener: &tokio::net::TcpListener) {
    use std::os::unix::io::AsRawFd;

    let sock = listener.as_raw_fd();
    const BUFFER_SIZE: libc::c_int = 256 * 1024; // 256KB

    // SAFETY: The TcpListener obtained from bind() guarantees that the socket
    // file descriptor is valid for the lifetime of this block. The socket options
    // SO_RCVBUF and SO_SNDBUF are standard POSIX socket options that are safe to
    // set with properly-sized integer values. We're passing a correctly-aligned
    // c_int pointer with the appropriate size parameter for libc::socklen_t.
    // These calls modify only the kernel's socket buffer configuration and cannot
    // cause memory unsafety in Rust code.
    unsafe {
        let ret = libc::setsockopt(
            sock,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            &BUFFER_SIZE as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        if ret != 0 {
            warn!(
                "Failed to set SO_RCVBUF: {}",
                std::io::Error::last_os_error()
            );
        }

        let ret = libc::setsockopt(
            sock,
            libc::SOL_SOCKET,
            libc::SO_SNDBUF,
            &BUFFER_SIZE as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        if ret != 0 {
            warn!(
                "Failed to set SO_SNDBUF: {}",
                std::io::Error::last_os_error()
            );
        }

        // Enable TCP keepalive to prevent zombie connections
        let keepalive: libc::c_int = 1;
        libc::setsockopt(
            sock,
            libc::SOL_SOCKET,
            libc::SO_KEEPALIVE,
            &keepalive as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        // Set keepalive idle time (60s) - platform-specific constant name
        let idle: libc::c_int = 60;
        #[cfg(target_os = "macos")]
        libc::setsockopt(
            sock,
            libc::IPPROTO_TCP,
            libc::TCP_KEEPALIVE,
            &idle as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        #[cfg(target_os = "linux")]
        {
            libc::setsockopt(
                sock,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPIDLE,
                &idle as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            let interval: libc::c_int = 10;
            libc::setsockopt(
                sock,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPINTVL,
                &interval as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
            let count: libc::c_int = 5;
            libc::setsockopt(
                sock,
                libc::IPPROTO_TCP,
                libc::TCP_KEEPCNT,
                &count as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    #[cfg(feature = "profiling")]
    let _profiler = dhat::Profiler::new_heap();

    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tart_backend=info,tower_http=debug,sqlx=warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let args = Cli::parse();

    info!("Starting TART (Testing, Analytics and Research Telemetry) Backend");
    info!("Optimized for handling up to 1024 concurrent nodes");

    // Initialize metrics
    let prometheus_handle =
        metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;

    // Connect to database (unless --no-database)
    let store = if args.no_database {
        info!("NO-DATABASE mode: events will only be broadcast via WebSocket, no DB writes");
        None
    } else {
        let database_url = args.database_url.expect(
            "DATABASE_URL must be set (--database-url or DATABASE_URL env). Use --no-database to skip.",
        );
        let redacted_url = match url::Url::parse(&database_url) {
            Ok(mut parsed) => {
                if parsed.password().is_some() {
                    let _ = parsed.set_password(Some("***"));
                }
                parsed.to_string()
            }
            Err(_) => "***redacted***".to_string(),
        };
        info!("Connecting to database: {}", redacted_url);
        Some(Arc::new(EventStore::new(&database_url).await?))
    };

    // Create shutdown signal for TCP server
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Create metrics tracker channel and shared handle
    let metrics_tracker = Arc::new(tart_backend::metrics_tracker::MetricsTracker::new());
    let (metrics_tx, metrics_rx) =
        tokio::sync::mpsc::channel::<tart_backend::metrics_tracker::MetricsEvent>(50_000);

    // Spawn the metrics tracker task (separate from aggregator for isolation)
    {
        let tracker = Arc::clone(&metrics_tracker);
        tokio::spawn(tart_backend::metrics_tracker::run(tracker, metrics_rx));
    }

    // Start telemetry server
    let ingestion_threads = args.ingestion_threads;
    info!("Starting telemetry server on {}", args.telemetry_bind);
    let telemetry_server = Arc::new(
        TelemetryServer::with_options_and_metrics(
            &args.telemetry_bind,
            store.clone(),
            args.no_rate_limit,
            ingestion_threads,
            Some(metrics_tx),
        )
        .await?,
    );

    // Spawn TCP ingestion: either dedicated runtimes (SO_REUSEPORT) or single-runtime
    let _ingestion_handles = if ingestion_threads > 0 {
        info!(
            "Spawning {} dedicated ingestion runtimes (SO_REUSEPORT)",
            ingestion_threads
        );
        Some(telemetry_server.spawn_ingestion_runtimes(ingestion_threads, shutdown_rx))
    } else {
        let telemetry_server_clone = Arc::clone(&telemetry_server);
        tokio::spawn(async move {
            if let Err(e) = telemetry_server_clone.run_until_shutdown(shutdown_rx).await {
                error!("Telemetry server error: {}", e);
            }
        });
        None
    };

    // Get the broadcaster from telemetry server for API WebSocket connections
    let broadcaster = telemetry_server.get_broadcaster();
    let batch_writer = Arc::new(telemetry_server.get_batch_writer());

    // Spawn tracker flush tasks (SlotTracker + WpTracker + EventCounter + Enricher cleanup)
    if let Some(ref store) = store {
        let slot_tracker = telemetry_server.get_slot_tracker();
        let wp_tracker = telemetry_server.get_wp_tracker();
        let guarantee_convergence_tracker = telemetry_server.get_guarantee_convergence_tracker();
        let assurance_convergence_tracker = telemetry_server.get_assurance_convergence_tracker();
        let header_hash_lookup = telemetry_server.get_header_hash_lookup();
        let da_tracker = telemetry_server.get_da_tracker();
        let event_counter = telemetry_server.get_event_counter();
        let enricher_map = telemetry_server.get_enricher_map();
        let pool = store.pool().clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));
            let mut tick_count: u64 = 0;
            loop {
                interval.tick().await;
                tart_backend::slot_tracker::flush_slot_tracker(
                    &slot_tracker,
                    &pool,
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(60),
                ).await;
                tart_backend::wp_tracker::flush_wp_tracker(&wp_tracker, &pool).await;
                tart_backend::convergence_tracker::flush_guarantee_convergence(
                    &guarantee_convergence_tracker,
                    &pool,
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(60),
                ).await;
                tart_backend::convergence_tracker::flush_assurance_convergence(
                    &assurance_convergence_tracker,
                    &pool,
                    std::time::Duration::from_secs(10),
                    std::time::Duration::from_secs(60),
                ).await;
                tart_backend::event_counter::flush_event_counter(&event_counter, &pool).await;
                tick_count += 1;
                // Flush DA tracker every 2 ticks (10s)
                if tick_count % 2 == 0 {
                    tart_backend::da_tracker::flush_da_tracker(&da_tracker, &pool).await;
                    tart_backend::enricher::log_enricher_diagnostics(&enricher_map, 10.0);
                }
                // Evict stale header_hash_lookup entries every 6 ticks (30s)
                if tick_count % 6 == 0 {
                    tart_backend::convergence_tracker::evict_header_hash_lookup(&header_hash_lookup, 5000);
                }
                // Sweep stale enrichers every 30 ticks (~2.5 min)
                if tick_count % 30 == 0 {
                    enricher_map.retain(|_, e| !e.is_stale());
                }
                // Retention cleanup: delete convergence rows older than 7 days (every 60 ticks = 5 min)
                if tick_count % 60 == 0 {
                    let cutoff = "NOW() - INTERVAL '7 days'";
                    for table_and_col in &[
                        ("slot_convergence", "authored_at"),
                        ("guarantee_convergence", "built_at"),
                        ("guarantee_convergence_slots", "built_at"),
                        ("assurance_convergence", "first_distributed_at"),
                    ] {
                        let sql = format!(
                            "DELETE FROM {} WHERE {} < {}",
                            table_and_col.0, table_and_col.1, cutoff
                        );
                        if let Err(e) = sqlx::query(&sql).execute(&pool).await {
                            tracing::warn!("retention cleanup for {} failed: {e}", table_and_col.0);
                        }
                    }
                }
            }
        });
    }

    // Build the HTTP router: full (with DB) or minimal (WebSocket-only)
    let mut app = if let Some(ref store) = store {
        // Initialize health monitoring system
        info!("Initializing comprehensive health monitoring system");
        let health_monitor = Arc::new(HealthMonitor::new());

        health_monitor
            .add_check(checks::database_check(Arc::clone(store)))
            .await;
        health_monitor
            .add_check(checks::batch_writer_check(Arc::clone(&batch_writer)))
            .await;
        health_monitor
            .add_check(checks::broadcaster_check(Arc::clone(&broadcaster)))
            .await;
        health_monitor.add_check(checks::memory_check()).await;
        health_monitor
            .add_check(checks::system_resources_check())
            .await;
        info!("Health monitoring system initialized with 5 critical component checks");

        // Initialize JAM RPC client if configured
        // JAM_RPC_URL supports comma-separated URLs for redundancy
        let jam_rpc = match std::env::var("JAM_RPC_URL") {
            Ok(rpc_url_str) => {
                let urls: Vec<String> = rpc_url_str
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();

                if urls.is_empty() {
                    info!("JAM_RPC_URL is empty - JAM RPC endpoints will be unavailable");
                    None
                } else {
                    // Always spawn on-chain stats ingestion — it has its own reconnect loop
                    let _onchain_handles = tart_backend::onchain_stats::spawn_onchain_ingestion(
                        urls.clone(),
                        store.pool().clone(),
                        6, // default slot period, will be validated on connect
                    );
                    info!(
                        "On-chain stats ingestion spawned for {} URL(s)",
                        urls.len()
                    );

                    // Connect first URL for the existing JamRpcClient (used by /api/jam endpoints)
                    info!("Connecting to JAM node RPC at {} ({} URL(s))", urls[0], urls.len());
                    let mut client = JamRpcClient::new(&urls[0]);
                    match client.connect().await {
                        Ok(()) => {
                            let client = Arc::new(client);
                            let _subscription_handle = client.clone().start_stats_subscription();
                            info!("JAM RPC client connected and subscribed to statistics");
                            Some(client)
                        }
                        Err(e) => {
                            error!("Failed to connect to JAM RPC at {}: {}", urls[0], e);
                            info!("JAM RPC endpoints will be unavailable (on-chain ingestion will keep retrying)");
                            None
                        }
                    }
                }
            }
            Err(_) => {
                info!("JAM_RPC_URL not set - JAM RPC endpoints will be unavailable");
                info!("To enable, set JAM_RPC_URL=ws://localhost:19800");
                None
            }
        };

        // Create TTL cache for expensive analytics queries
        let cache = Arc::new(tart_backend::cache::TtlCache::new(
            std::time::Duration::from_secs(3),
        ));

        // Spawn background cache warming task
        {
            let cache_clone = Arc::clone(&cache);
            let store_clone = Arc::clone(store);
            let tracker_clone = Arc::clone(&metrics_tracker);
            let ts_clone_for_cache = Arc::clone(&telemetry_server);
            tokio::spawn(async move {
                let mut warm_interval = tokio::time::interval(std::time::Duration::from_secs(2));
                let mut evict_counter: u64 = 0;

                loop {
                    warm_interval.tick().await;
                    let first = evict_counter == 0;

                    if first {
                        info!(
                            "Warming cache with all aggregation endpoints (independent spawns)..."
                        );
                    }

                    macro_rules! spawn_warm {
                        ($key:expr, $($method:tt)+) => {{
                            let cache = Arc::clone(&cache_clone);
                            let store = Arc::clone(&store_clone);
                            let first = first;
                            tokio::spawn(async move {
                                match store.$($method)+.await {
                                    Ok(value) => {
                                        cache.insert($key.to_string(), value);
                                        if first {
                                            info!("Cache warmed: {}", $key);
                                        }
                                    }
                                    Err(e) => warn!("Cache warm failed for {}: {}", $key, e),
                                }
                            })
                        }};
                    }

                    let handles: Vec<tokio::task::JoinHandle<()>> = vec![
                        spawn_warm!("stats", get_stats("1 hour", "24 hours")),
                        spawn_warm!("workpackage_stats", get_workpackage_stats("24 hours")),
                        spawn_warm!("block_stats", get_block_stats("1 hour")),
                        spawn_warm!("guarantee_stats", get_guarantee_stats("1 hour", "24 hours")),
                        spawn_warm!("da_stats", get_da_stats()),
                        spawn_warm!("failure_rates", get_failure_rates("1 hour")),
                        // block_propagation served from in-memory MetricsTracker (Fix 5)
                        spawn_warm!("network_health", get_network_health("1 hour", "24 hours")),
                        spawn_warm!(
                            "guarantees_by_guarantor",
                            get_guarantees_by_guarantor("1 hour", "24 hours")
                        ),
                        // DISABLED: da_stats_enhanced kills DB CPU (~20k slow queries/day)
                        // See docs/issue-00--get_da_stats_enhanced.txt
                        // spawn_warm!(
                        //     "da_stats_enhanced",
                        //     get_da_stats_enhanced("1 hour", "24 hours")
                        // ),
                        spawn_warm!("execution_metrics", get_execution_metrics("1 hour")),
                        spawn_warm!(
                            "timeseries_throughput_5_1",
                            get_timeseries_metrics("throughput", 5, 1)
                        ),
                    ];

                    // Warm live_counters and realtime_60 from in-memory LiveCounters (instant, no SQL)
                    {
                        let lc = tracker_clone.live_counters();
                        let active_nodes = ts_clone_for_cache.connection_count();
                        let last_10s = lc.sum_last_n_seconds(10);
                        let last_1m = lc.sum_last_n_seconds(60);
                        cache_clone.insert(
                            "live_counters".to_string(),
                            lc.build_live_snapshot(&last_10s, &last_1m, active_nodes),
                        );
                        let per_second = lc.per_second_history(60);
                        cache_clone.insert(
                            "realtime_60".to_string(),
                            lc.build_realtime_snapshot(60, &per_second, active_nodes),
                        );
                    }

                    let anomaly_handle = {
                        let cache = Arc::clone(&cache_clone);
                        let store = Arc::clone(&store_clone);
                        tokio::spawn(async move {
                            match store.detect_anomalies().await {
                                Ok(alerts) => {
                                    cache.insert(
                                        "anomalies".to_string(),
                                        serde_json::json!({"alerts": alerts}),
                                    );
                                    if first {
                                        info!("Cache warmed: anomalies");
                                    }
                                }
                                Err(e) => warn!("Cache warm failed for anomalies: {}", e),
                            }
                        })
                    };

                    for handle in handles {
                        if let Err(e) = handle.await {
                            warn!("Cache warm task panicked: {}", e);
                        }
                    }
                    if let Err(e) = anomaly_handle.await {
                        warn!("Cache warm task panicked: {}", e);
                    }

                    if first {
                        info!("Initial cache warming complete (15 endpoints, independent spawns)");
                    }

                    evict_counter += 1;
                    if evict_counter.is_multiple_of(15) {
                        cache_clone.evict_expired();
                    }
                }
            });
        }

        let api_state = ApiState {
            store: Arc::clone(store),
            telemetry_server,
            broadcaster,
            health_monitor,
            jam_rpc,
            cache,
            metrics_tracker: Some(Arc::clone(&metrics_tracker)),
        };

        create_api_router(api_state)
    } else {
        // No-database mode: minimal router with WebSocket + health only
        let minimal_state = MinimalApiState {
            telemetry_server,
            broadcaster,
        };

        create_minimal_router(minimal_state)
    };

    // Add metrics endpoint (always available)
    app = app.route(
        "/metrics",
        axum::routing::get(move || async move { prometheus_handle.render() }),
    );

    // Start HTTP server
    let api_addr: SocketAddr = args.api_bind.parse()?;
    info!("Starting HTTP API server on {}", api_addr);
    info!("Metrics available at http://{}/metrics", api_addr);

    let listener = tokio::net::TcpListener::bind(api_addr).await?;

    #[cfg(unix)]
    configure_socket_buffers(&listener);

    info!("=== TART Backend Configuration ===");
    info!(
        "Mode: {}",
        if store.is_none() {
            "no-database (WebSocket-only)"
        } else {
            "full (DB + WebSocket)"
        }
    );
    if ingestion_threads > 0 {
        info!(
            "Ingestion: {} dedicated runtimes (SO_REUSEPORT)",
            ingestion_threads
        );
    } else {
        info!("Ingestion: single runtime (legacy)");
    }
    info!("Max concurrent connections: 1024");
    if !args.no_rate_limit {
        info!("Max events per second per node: 100 (+50 burst)");
    } else {
        info!("Rate limiting: DISABLED");
    }
    if store.is_some() {
        info!("Write batch size: 16000 events");
        info!("Write batch timeout: 100ms");
        info!("Cache TTL: 3s, warm interval: 2s (15 endpoints, independent spawns)");
    }
    info!("==========================================");

    // Graceful shutdown
    let batch_writer_shutdown = Arc::clone(&batch_writer);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let ctrl_c = tokio::signal::ctrl_c();
            #[cfg(unix)]
            let mut sigterm =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("Failed to install SIGTERM handler");
            #[cfg(unix)]
            let terminate = sigterm.recv();
            #[cfg(not(unix))]
            let terminate = std::future::pending::<Option<()>>();

            tokio::select! {
                _ = ctrl_c => info!("Received SIGINT, shutting down gracefully..."),
                _ = terminate => info!("Received SIGTERM, shutting down gracefully..."),
            }

            // Signal TCP server to stop accepting new connections
            let _ = shutdown_tx.send(true);

            // Flush batch writer to prevent data loss
            info!("Flushing batch writer...");
            if let Err(e) = batch_writer_shutdown.flush().await {
                error!("Failed to flush batch writer during shutdown: {}", e);
            }
            if let Err(e) = batch_writer_shutdown.shutdown().await {
                error!("Failed to shutdown batch writer: {}", e);
            }
            info!("Graceful shutdown complete");
        })
        .await
        .map_err(|e| anyhow::anyhow!("HTTP server error: {}", e))?;

    Ok(())
}
