#[cfg(feature = "profiling")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(all(feature = "jemalloc", not(feature = "profiling")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use std::net::SocketAddr;
use std::sync::Arc;
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::health::{checks, HealthMonitor};
use tart_backend::jam_rpc::JamRpcClient;
use tart_backend::{EventStore, TelemetryServer};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

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
                .unwrap_or_else(|_| "tart_backend=info,tower_http=info,sqlx=warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    info!("Starting TART (Testing, Analytics and Research Telemetry) Backend - OPTIMIZED");
    info!("Optimized for handling up to 1024 concurrent nodes");

    // Configuration from environment variables
    let telemetry_bind =
        std::env::var("TELEMETRY_BIND").unwrap_or_else(|_| "0.0.0.0:9000".to_string());
    let api_bind = std::env::var("API_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let database_url = std::env::var("DATABASE_URL").expect(
        "DATABASE_URL must be set. Example: postgres://user:password@localhost:5432/dbname",
    );

    // Initialize metrics
    let prometheus_handle =
        metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;

    // Initialize optimized storage
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
    let store = Arc::new(EventStore::new(&database_url).await?);

    // Create shutdown signal for TCP server
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);

    // Start optimized telemetry server
    info!("Starting optimized telemetry server on {}", telemetry_bind);
    let telemetry_server =
        Arc::new(TelemetryServer::new(&telemetry_bind, Arc::clone(&store)).await?);
    let telemetry_server_clone = Arc::clone(&telemetry_server);

    // Spawn telemetry server task with shutdown support
    tokio::spawn(async move {
        if let Err(e) = telemetry_server_clone.run_until_shutdown(shutdown_rx).await {
            error!("Telemetry server error: {}", e);
        }
    });

    // Get the broadcaster from telemetry server for API WebSocket connections
    let broadcaster = telemetry_server.get_broadcaster();
    let batch_writer = Arc::new(telemetry_server.get_batch_writer());

    // Initialize health monitoring system
    info!("Initializing comprehensive health monitoring system");
    let health_monitor = Arc::new(HealthMonitor::new());

    // Add health checks for all critical components
    health_monitor
        .add_check(checks::database_check(Arc::clone(&store)))
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
    let jam_rpc = match std::env::var("JAM_RPC_URL") {
        Ok(rpc_url) => {
            info!("Connecting to JAM node RPC at {}", rpc_url);
            let mut client = JamRpcClient::new(&rpc_url);
            match client.connect().await {
                Ok(()) => {
                    let client = Arc::new(client);
                    // Start background statistics subscription
                    let _subscription_handle = client.clone().start_stats_subscription();
                    info!("JAM RPC client connected and subscribed to statistics");
                    Some(client)
                }
                Err(e) => {
                    error!("Failed to connect to JAM RPC at {}: {}", rpc_url, e);
                    info!("JAM RPC endpoints will be unavailable");
                    None
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
    // 3s TTL with 2s warming interval means data is never more than ~2s old
    let cache = Arc::new(tart_backend::cache::TtlCache::new(
        std::time::Duration::from_secs(3),
    ));

    // Spawn background cache management task:
    // - Evicts expired entries every ~30 seconds
    // - Proactively re-warms all cached endpoints every 2 seconds (before the 3s TTL expires)
    // - Warming runs concurrently via tokio::join! so cycle time = max(query times) not sum
    // This means the cache is never cold: the HTTP server starts immediately and the first
    // user request always hits a warm cache. Slow DB queries happen in the background only.
    {
        let cache_clone = Arc::clone(&cache);
        let store_clone = Arc::clone(&store);
        tokio::spawn(async move {
            let mut warm_interval = tokio::time::interval(std::time::Duration::from_secs(2));
            let mut evict_counter: u64 = 0;

            loop {
                warm_interval.tick().await;
                let first = evict_counter == 0;

                if first {
                    info!("Warming cache with all aggregation endpoints (concurrent)...");
                }

                // Resolve core_count from JAM RPC params before concurrent queries
                // Warm all cached endpoints concurrently — cycle time = max(query) not sum(queries)
                // NOTE: cores_status is computed in the API handler (merges JAM RPC + DB)
                // and cached via cache_or_compute, so it's not warmed here.
                // Real-time endpoints (realtime_metrics, active_workpackages, events/search)
                // hit the DB directly — they must never serve stale data.
                let (
                    r_stats,
                    r_wp_stats,
                    r_block_stats,
                    r_guarantee_stats,
                    r_da_stats,
                    r_failure_rates,
                    r_block_propagation,
                    r_network_health,
                    r_guarantees_by_guarantor,
                    r_live_counters,
                    r_da_stats_enhanced,
                    r_execution_metrics,
                    r_anomalies,
                ) = tokio::join!(
                    store_clone.get_stats("1 hour", "24 hours"),
                    store_clone.get_workpackage_stats("24 hours"),
                    store_clone.get_block_stats("1 hour"),
                    store_clone.get_guarantee_stats("1 hour", "24 hours"),
                    store_clone.get_da_stats(),
                    store_clone.get_failure_rates("1 hour"),
                    store_clone.get_block_propagation("1 hour"),
                    store_clone.get_network_health("1 hour", "24 hours"),
                    store_clone.get_guarantees_by_guarantor("1 hour", "24 hours"),
                    store_clone.get_live_counters(),
                    store_clone.get_da_stats_enhanced("1 hour", "24 hours"),
                    store_clone.get_execution_metrics("1 hour"),
                    store_clone.detect_anomalies(),
                );

                // Insert results into cache, logging on first run
                macro_rules! cache_result {
                    ($key:expr, $result:expr) => {
                        match $result {
                            Ok(value) => {
                                cache_clone.insert($key.to_string(), value);
                                if first {
                                    info!("Cache warmed: {}", $key);
                                }
                            }
                            Err(e) => warn!("Failed to warm cache for {}: {}", $key, e),
                        }
                    };
                }

                cache_result!("stats", r_stats);
                cache_result!("workpackage_stats", r_wp_stats);
                cache_result!("block_stats", r_block_stats);
                cache_result!("guarantee_stats", r_guarantee_stats);
                cache_result!("da_stats", r_da_stats);
                cache_result!("failure_rates", r_failure_rates);
                cache_result!("block_propagation", r_block_propagation);
                cache_result!("network_health", r_network_health);
                cache_result!("guarantees_by_guarantor", r_guarantees_by_guarantor);
                cache_result!("live_counters", r_live_counters);
                cache_result!("da_stats_enhanced", r_da_stats_enhanced);
                cache_result!("execution_metrics", r_execution_metrics);

                // Cache anomalies: wrap Vec<Value> in a JSON object for the WS handler
                match r_anomalies {
                    Ok(alerts) => {
                        let value = serde_json::json!({"alerts": alerts});
                        cache_clone.insert("anomalies".to_string(), value);
                        if first {
                            info!("Cache warmed: anomalies");
                        }
                    }
                    Err(e) => warn!("Failed to warm cache for anomalies: {}", e),
                }

                if first {
                    info!("Initial cache warming complete (13 endpoints, concurrent)");
                }

                // Evict expired entries every ~15th cycle (roughly every 30s)
                evict_counter += 1;
                if evict_counter.is_multiple_of(15) {
                    cache_clone.evict_expired();
                }
            }
        });
    }

    // Create API state using the optimized components
    let api_state = ApiState {
        store: Arc::clone(&store),
        telemetry_server,
        broadcaster,
        health_monitor,
        jam_rpc,
        cache,
    };

    // Create HTTP API router
    let mut app = create_api_router(api_state);

    // Add metrics endpoint
    app = app.route(
        "/metrics",
        axum::routing::get(move || async move { prometheus_handle.render() }),
    );

    // Start HTTP server with optimized settings
    let api_addr: SocketAddr = api_bind.parse()?;
    info!("Starting HTTP API server on {}", api_addr);
    info!("Metrics available at http://{}/metrics", api_addr);

    let listener = tokio::net::TcpListener::bind(api_addr).await?;

    // Configure socket for better performance on Unix
    #[cfg(unix)]
    configure_socket_buffers(&listener);

    info!("=== TART Backend Optimized Configuration ===");
    info!("Max concurrent connections: 1024");
    info!("Max events per second per node: 100 (+50 burst)");
    info!("Write batch size: 2000 events");
    info!("Write batch timeout: 5ms");
    info!("Cache TTL: 3s, warm interval: 2s (13 endpoints, concurrent)");
    info!("==========================================");

    // Graceful shutdown: flush pending data on SIGTERM/SIGINT
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
