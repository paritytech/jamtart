use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::health::{checks, HealthMonitor};
use tart_backend::jam_rpc::JamRpcClient;
use tart_backend::{EventStore, TelemetryServer};
use tracing::{error, info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Parser, Debug)]
#[command(author, version, about = "TART (Testing, Analytics and Research Telemetry) Backend")]
struct Args {
    /// Disable PostgreSQL database (WebSocket forwarding only mode)
    #[arg(long)]
    no_database: bool,

    /// Disable rate limiting (allows unlimited events per node)
    #[arg(long)]
    no_rate_limit: bool,

    /// Telemetry server bind address
    #[arg(long, env = "TELEMETRY_BIND", default_value = "0.0.0.0:9000")]
    telemetry_bind: String,

    /// API server bind address
    #[arg(long, env = "API_BIND", default_value = "0.0.0.0:8080")]
    api_bind: String,

    /// Database URL (required unless --no-database is set)
    #[arg(long, env = "DATABASE_URL")]
    database_url: Option<String>,

    /// JAM RPC WebSocket URL (optional, enables JAM RPC endpoints)
    #[arg(long, env = "JAM_RPC_URL")]
    jam_rpc_url: Option<String>,
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
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "tart_backend=info,tower_http=info,sqlx=warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Parse CLI arguments
    let args = Args::parse();

    info!("Starting TART (Testing, Analytics and Research Telemetry) Backend");
    info!(
        "Optimized for handling up to {} concurrent nodes",
        tart_backend::rate_limiter::MAX_CONNECTIONS
    );

    // Initialize metrics
    let prometheus_handle =
        metrics_exporter_prometheus::PrometheusBuilder::new().install_recorder()?;

    // Initialize storage (optional based on --no-database flag)
    let store = if args.no_database {
        info!("Running in WebSocket-only mode (database disabled)");
        None
    } else {
        let database_url = args.database_url.expect(
            "DATABASE_URL must be set (or use --no-database for WebSocket-only mode)",
        );
        info!("Connecting to database: {}", database_url);
        Some(Arc::new(EventStore::new(&database_url).await?))
    };

    // Start optimized telemetry server
    info!(
        "Starting optimized telemetry server on {}",
        args.telemetry_bind
    );
    let telemetry_server = Arc::new(
        TelemetryServer::with_options(&args.telemetry_bind, store.clone(), args.no_rate_limit)
            .await?,
    );
    let telemetry_server_clone = Arc::clone(&telemetry_server);

    // Spawn telemetry server task
    tokio::spawn(async move {
        if let Err(e) = telemetry_server_clone.run().await {
            eprintln!("Telemetry server error: {}", e);
        }
    });

    // Get the broadcaster from telemetry server for API WebSocket connections
    let broadcaster = telemetry_server.get_broadcaster();
    let batch_writer = Arc::new(telemetry_server.get_batch_writer());

    // Initialize health monitoring system
    info!("Initializing comprehensive health monitoring system");
    let health_monitor = Arc::new(HealthMonitor::new());

    // Add health checks for all critical components
    // Only add database check if database is enabled
    if let Some(ref s) = store {
        health_monitor
            .add_check(checks::database_check(Arc::clone(s)))
            .await;
    }
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

    let check_count = if store.is_some() { 5 } else { 4 };
    info!(
        "Health monitoring system initialized with {} critical component checks",
        check_count
    );

    // Initialize JAM RPC client if configured
    let jam_rpc = match args.jam_rpc_url {
        Some(rpc_url) => {
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
        None => {
            info!("JAM_RPC_URL not set - JAM RPC endpoints will be unavailable");
            info!("To enable, set JAM_RPC_URL=ws://localhost:19800");
            None
        }
    };

    // Create API state using the optimized components
    let api_state = ApiState {
        store,
        telemetry_server,
        broadcaster,
        health_monitor,
        jam_rpc,
    };

    // Create HTTP API router
    let mut app = create_api_router(api_state);

    // Add metrics endpoint
    app = app.route(
        "/metrics",
        axum::routing::get(move || async move { prometheus_handle.render() }),
    );

    // Start HTTP server with optimized settings
    let api_addr: SocketAddr = args.api_bind.parse()?;
    info!("Starting HTTP API server on {}", api_addr);
    info!("Metrics available at http://{}/metrics", api_addr);

    let listener = tokio::net::TcpListener::bind(api_addr).await?;

    // Configure socket for better performance on Unix
    #[cfg(unix)]
    configure_socket_buffers(&listener);

    info!("=== TART Backend Configuration ===");
    info!(
        "Database mode: {}",
        if args.no_database {
            "disabled (WebSocket-only)"
        } else {
            "enabled"
        }
    );
    info!(
        "Rate limiting: {}",
        if args.no_rate_limit {
            "disabled"
        } else {
            "100 events/sec per node"
        }
    );
    info!(
        "Max concurrent connections: {}",
        tart_backend::rate_limiter::MAX_CONNECTIONS
    );
    if !args.no_database {
        info!("Write batch size: 10000 events");
        info!("Write batch timeout: 100ms");
        info!("Parallel writer workers: 32");
    }
    info!("===================================");

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!("HTTP server error: {}", e))?;

    Ok(())
}
