use std::net::SocketAddr;
use std::sync::Arc;
use tart_backend::api::{create_api_router, ApiState};
use tart_backend::health::{checks, HealthMonitor};
use tart_backend::{EventStore, TelemetryServer};
use tracing::{info, warn};
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
    info!("Connecting to database: {}", database_url);
    let store = Arc::new(EventStore::new(&database_url).await?);

    // Start optimized telemetry server
    info!("Starting optimized telemetry server on {}", telemetry_bind);
    let telemetry_server =
        Arc::new(TelemetryServer::new(&telemetry_bind, Arc::clone(&store)).await?);
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

    // Create API state using the optimized components
    let api_state = ApiState {
        store: Arc::clone(&store),
        telemetry_server,
        broadcaster,
        health_monitor,
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
    info!("Max events per second per node: 100");
    info!("Write batch size: 1000 events");
    info!("Write batch timeout: 100ms");
    info!("==========================================");

    axum::serve(listener, app)
        .await
        .map_err(|e| anyhow::anyhow!("HTTP server error: {}", e))?;

    Ok(())
}
