//! TCP telemetry server that accepts binary-framed connections from JAM nodes,
//! decodes event streams, enriches them with cross-event context, and forwards
//! records to the batch writer for persistence.

use crate::batch_writer::{BatchWriter, EventRecord};
use crate::decoder::{decode_message_frame, Decode, DecodingError};
use crate::enricher::EnricherMap;
use crate::event_broadcaster::{build_ws_envelope, BroadcastRecord, EventBroadcaster};
use crate::event_counter::{self, EventCounter};
use crate::events::{Event, NodeInformation};
use crate::rate_limiter::RateLimiter;
use crate::convergence_tracker::{self, AssuranceConvergenceTracker, GuaranteeConvergenceTracker, HeaderHashLookup};
use crate::da_tracker::{self, DaTracker};
use crate::slot_tracker::{self, SlotTracker};
use crate::store::EventStore;
use crate::wp_tracker::{self, WpTracker};
use bytes::{Buf, BytesMut};
use dashmap::DashMap;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, error, info, warn};

/// Create a TCP listener with SO_REUSEPORT enabled using socket2.
///
/// Multiple listeners can bind to the same address and the kernel distributes
/// incoming connections across them. This enables per-runtime accept loops
/// without a shared listener.
fn create_reuseport_listener(addr: &SocketAddr) -> Result<std::net::TcpListener, std::io::Error> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(*addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_port(true)?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;

    // Set receive buffer to 256KB (matching existing configure_socket_receive_buffer)
    socket.set_recv_buffer_size(256 * 1024)?;

    socket.bind(&(*addr).into())?;
    socket.listen(1024)?;

    Ok(socket.into())
}

/// Configure TCP socket receive buffer size for better performance.
///
/// Sets SO_RCVBUF to 256KB for improved throughput with many concurrent node connections.
#[cfg(unix)]
fn configure_socket_receive_buffer(listener: &TcpListener) {
    use std::os::unix::io::AsRawFd;

    let sock = listener.as_raw_fd();
    const BUFFER_SIZE: libc::c_int = 256 * 1024; // 256KB

    // SAFETY: The TcpListener obtained from bind() guarantees that the socket
    // file descriptor is valid for the lifetime of this block. SO_RCVBUF is a
    // standard POSIX socket option that is safe to set with a properly-sized
    // integer value. We're passing a correctly-aligned c_int pointer with the
    // appropriate size parameter. This call modifies only kernel socket buffer
    // configuration and cannot cause memory unsafety in Rust code.
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
                "Failed to set SO_RCVBUF for telemetry socket: {}",
                std::io::Error::last_os_error()
            );
        }
    }
}

/// Maximum buffer size per connection (1MB)
const MAX_BUFFER_SIZE: usize = 1024 * 1024;

/// Maximum message size (100KB)
const MAX_MESSAGE_SIZE: u32 = 100 * 1024;

/// Initial buffer allocation for new connections (8KB)
const INITIAL_BUFFER_SIZE: usize = 8192;

/// Frequency of connection stats updates (every N events)
const CONNECTION_STATS_UPDATE_FREQUENCY: u64 = 1000;

/// Represents an active connection from a JAM/Polkadot node.
#[derive(Clone)]
pub struct NodeConnection {
    pub id: String,
    pub address: SocketAddr,
    pub info: NodeInformation,
    pub connected_at: chrono::DateTime<chrono::Utc>,
    pub last_event_at: chrono::DateTime<chrono::Utc>,
    pub event_count: u64,
}

/// High-performance TCP telemetry server for JAM/Polkadot blockchain nodes.
///
/// Accepts binary telemetry data on TCP port 9000 from up to 1024 concurrent nodes.
/// Features include:
/// - Rate limiting (100 events/sec per node)
/// - Batch writing for optimal database performance
/// - Real-time event broadcasting to WebSocket clients
/// - Connection tracking and health monitoring
///
/// # Example
/// ```no_run
/// use tart_backend::{TelemetryServer, EventStore};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let store = Arc::new(EventStore::new("postgres://localhost/tart").await?);
/// let server = TelemetryServer::new("0.0.0.0:9000", store).await?;
/// server.run().await?;
/// # Ok(())
/// # }
/// ```
pub struct TelemetryServer {
    /// Listener for single-runtime mode. None in multi-runtime (SO_REUSEPORT) mode.
    listener: Option<TcpListener>,
    /// Bind address, kept for spawning SO_REUSEPORT listeners in multi-runtime mode.
    bind_address: SocketAddr,
    connections: Arc<DashMap<String, NodeConnection>>,
    /// EventStore reference - owned by BatchWriter but kept here for lifetime management
    /// and potential future direct queries. None in --no-database mode.
    #[allow(dead_code)]
    store: Option<Arc<EventStore>>,
    batch_writer: BatchWriter,
    rate_limiter: Arc<RateLimiter>,
    broadcaster: Arc<EventBroadcaster>,
    /// When true, rate limiting is disabled (--no-rate-limit flag)
    rate_limit_disabled: bool,
    enricher_map: EnricherMap,
    event_counter: EventCounter,
    slot_tracker: SlotTracker,
    wp_tracker: WpTracker,
    guarantee_convergence_tracker: GuaranteeConvergenceTracker,
    assurance_convergence_tracker: AssuranceConvergenceTracker,
    header_hash_lookup: HeaderHashLookup,
    da_tracker: DaTracker,
    connection_watch: Arc<tokio::sync::watch::Sender<usize>>,
    /// Kept alive so the watch channel stays open (senders fail when all receivers drop)
    #[allow(dead_code)]
    _connection_watch_rx: tokio::sync::watch::Receiver<usize>,
}

impl TelemetryServer {
    pub async fn new(bind_address: &str, store: Arc<EventStore>) -> Result<Self, std::io::Error> {
        Self::with_options(bind_address, Some(store), false, 0).await
    }

    /// Create a new TelemetryServer.
    ///
    /// If `ingestion_threads > 0`, the server will NOT bind a listener here — instead,
    /// `spawn_ingestion_runtimes()` creates N SO_REUSEPORT listeners, each on its own
    /// single-thread tokio runtime. This eliminates work-stealing contention when handling
    /// 1024+ TCP connections.
    ///
    /// If `ingestion_threads == 0`, behaves as before: single listener on current runtime.
    pub async fn with_options(
        bind_address: &str,
        store: Option<Arc<EventStore>>,
        no_rate_limit: bool,
        ingestion_threads: usize,
    ) -> Result<Self, std::io::Error> {
        Self::with_options_and_metrics(bind_address, store, no_rate_limit, ingestion_threads, None)
            .await
    }

    pub async fn with_options_and_metrics(
        bind_address: &str,
        store: Option<Arc<EventStore>>,
        no_rate_limit: bool,
        ingestion_threads: usize,
        metrics_tx: Option<tokio::sync::mpsc::Sender<crate::metrics_tracker::MetricsEvent>>,
    ) -> Result<Self, std::io::Error> {
        let bind_addr: SocketAddr = bind_address.parse().map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("bad address: {e}"),
            )
        })?;

        let listener = if ingestion_threads == 0 {
            let listener = TcpListener::bind(bind_addr).await?;
            info!("Telemetry server listening on {}", bind_address);

            #[cfg(unix)]
            configure_socket_receive_buffer(&listener);

            Some(listener)
        } else {
            info!(
                "Telemetry server will use {} dedicated ingestion runtimes on {}",
                ingestion_threads, bind_address
            );
            None
        };

        // Initialize metrics
        metrics::describe_counter!(
            "telemetry_events_received",
            "Total number of telemetry events received"
        );
        metrics::describe_counter!(
            "telemetry_events_dropped",
            "Number of events dropped due to backpressure"
        );
        metrics::describe_gauge!(
            "telemetry_active_connections",
            "Number of active telemetry connections"
        );
        metrics::describe_gauge!(
            "telemetry_buffer_pending",
            "Number of events pending in write buffer"
        );

        let batch_writer = BatchWriter::new(store.clone());

        if no_rate_limit {
            info!("Rate limiting DISABLED - nodes can send unlimited events");
        }

        let (connection_watch, connection_watch_rx) = tokio::sync::watch::channel(0usize);

        let broadcaster = Arc::new(EventBroadcaster::with_metrics_tx(metrics_tx));
        broadcaster.start_aggregator();

        Ok(Self {
            listener,
            bind_address: bind_addr,
            connections: Arc::new(DashMap::new()),
            batch_writer,
            rate_limiter: Arc::new(RateLimiter::new()),
            broadcaster,
            store,
            rate_limit_disabled: no_rate_limit,
            enricher_map: crate::enricher::new_enricher_map(),
            event_counter: event_counter::new_event_counter(),
            slot_tracker: slot_tracker::new_slot_tracker(),
            wp_tracker: wp_tracker::new_wp_tracker(),
            guarantee_convergence_tracker: convergence_tracker::new_guarantee_convergence_tracker(),
            assurance_convergence_tracker: convergence_tracker::new_assurance_convergence_tracker(),
            header_hash_lookup: convergence_tracker::new_header_hash_lookup(),
            da_tracker: da_tracker::new_da_tracker(),
            connection_watch: Arc::new(connection_watch),
            _connection_watch_rx: connection_watch_rx,
        })
    }

    pub async fn run(&self) -> Result<(), std::io::Error> {
        let listener = self
            .listener
            .as_ref()
            .expect("run() requires single-runtime mode (ingestion_threads=0)");
        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    self.spawn_connection(stream, addr);
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }

    /// Run the server until a shutdown signal is received (single-runtime mode).
    pub async fn run_until_shutdown(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), std::io::Error> {
        let listener = self
            .listener
            .as_ref()
            .expect("run_until_shutdown() requires single-runtime mode (ingestion_threads=0)");
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, addr)) => {
                            self.spawn_connection(stream, addr);
                        }
                        Err(e) => {
                            error!("Failed to accept connection: {}", e);
                        }
                    }
                }
                _ = shutdown.changed() => {
                    info!("TCP server received shutdown signal, stopping accept loop");
                    break;
                }
            }
        }
        Ok(())
    }

    /// Spawn N dedicated ingestion runtimes, each with its own SO_REUSEPORT listener.
    ///
    /// Each runtime is a single-thread tokio runtime running on its own OS thread.
    /// The kernel distributes incoming TCP connections across the listeners.
    /// Returns the join handles for the OS threads (for shutdown coordination).
    pub fn spawn_ingestion_runtimes(
        &self,
        n: usize,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Vec<std::thread::JoinHandle<()>> {
        let mut handles = Vec::with_capacity(n);

        for i in 0..n {
            let bind_addr = self.bind_address;
            let mut shutdown = shutdown.clone();
            let connections = Arc::clone(&self.connections);
            let batch_writer = self.batch_writer.clone();
            let rate_limiter = Arc::clone(&self.rate_limiter);
            let broadcaster = Arc::clone(&self.broadcaster);
            let rate_limit_disabled = self.rate_limit_disabled;
            let enricher_map = Arc::clone(&self.enricher_map);
            let event_counter = Arc::clone(&self.event_counter);
            let slot_tracker = Arc::clone(&self.slot_tracker);
            let wp_tracker = Arc::clone(&self.wp_tracker);
            let guarantee_convergence_tracker = Arc::clone(&self.guarantee_convergence_tracker);
            let assurance_convergence_tracker = Arc::clone(&self.assurance_convergence_tracker);
            let header_hash_lookup = Arc::clone(&self.header_hash_lookup);
            let da_tracker = Arc::clone(&self.da_tracker);
            let connection_watch = Arc::clone(&self.connection_watch);

            let handle = std::thread::Builder::new()
                .name(format!("ingestion-{i}"))
                .spawn(move || {
                    let rt = tokio::runtime::Builder::new_current_thread()
                        .enable_all()
                        .build()
                        .unwrap_or_else(|e| panic!("ingestion runtime {i}: {e}"));

                    rt.block_on(async move {
                        let std_listener = match create_reuseport_listener(&bind_addr) {
                            Ok(l) => l,
                            Err(e) => {
                                error!("ingestion-{i}: failed to bind {bind_addr}: {e}");
                                return;
                            }
                        };

                        let listener = TcpListener::from_std(std_listener)
                            .expect("ingestion: TcpListener::from_std");

                        info!("ingestion-{i}: listening on {bind_addr} (SO_REUSEPORT)");

                        loop {
                            tokio::select! {
                                result = listener.accept() => {
                                    match result {
                                        Ok((stream, addr)) => {
                                            // Check connection limit
                                            if !rate_limiter.allow_connection(&addr) {
                                                drop(stream);
                                                continue;
                                            }

                                            info!(
                                                "New connection from {} ({}/{})",
                                                addr,
                                                rate_limiter.connection_count(),
                                                crate::rate_limiter::MAX_CONNECTIONS
                                            );

                                            let rl = Arc::clone(&rate_limiter);
                                            let ctx = ConnectionContext {
                                                connections: Arc::clone(&connections),
                                                batch_writer: batch_writer.clone(),
                                                rate_limiter: rl.clone(),
                                                broadcaster: Arc::clone(&broadcaster),
                                                rate_limit_disabled,
                                                enricher_map: Arc::clone(&enricher_map),
                                                event_counter: Arc::clone(&event_counter),
                                                slot_tracker: Arc::clone(&slot_tracker),
                                                wp_tracker: Arc::clone(&wp_tracker),
                                                guarantee_convergence_tracker: Arc::clone(&guarantee_convergence_tracker),
                                                assurance_convergence_tracker: Arc::clone(&assurance_convergence_tracker),
                                                header_hash_lookup: Arc::clone(&header_hash_lookup),
                                                da_tracker: Arc::clone(&da_tracker),
                                                connection_watch: Arc::clone(&connection_watch),
                                            };

                                            tokio::spawn(async move {
                                                let result = handle_connection_optimized(stream, addr, ctx).await;
                                                rl.connection_closed();
                                                if let Err(e) = result {
                                                    error!("Connection error from {}: {}", addr, e);
                                                }
                                            });
                                        }
                                        Err(e) => {
                                            error!("ingestion-{i}: accept error: {e}");
                                        }
                                    }
                                }
                                _ = shutdown.changed() => {
                                    info!("ingestion-{i}: shutdown signal received");
                                    break;
                                }
                            }
                        }
                    });
                })
                .unwrap_or_else(|e| panic!("failed to spawn ingestion thread {i}: {e}"));

            handles.push(handle);
        }

        handles
    }

    fn spawn_connection(&self, stream: TcpStream, addr: SocketAddr) {
        // Check connection limit
        if !self.rate_limiter.allow_connection(&addr) {
            // Close connection immediately
            drop(stream);
            return;
        }

        info!(
            "New connection from {} ({}/{})",
            addr,
            self.rate_limiter.connection_count(),
            crate::rate_limiter::MAX_CONNECTIONS
        );

        let rate_limiter = Arc::clone(&self.rate_limiter);
        let ctx = ConnectionContext {
            connections: Arc::clone(&self.connections),
            batch_writer: self.batch_writer.clone(),
            rate_limiter: rate_limiter.clone(),
            broadcaster: Arc::clone(&self.broadcaster),
            rate_limit_disabled: self.rate_limit_disabled,
            enricher_map: Arc::clone(&self.enricher_map),
            event_counter: Arc::clone(&self.event_counter),
            slot_tracker: Arc::clone(&self.slot_tracker),
            wp_tracker: Arc::clone(&self.wp_tracker),
            guarantee_convergence_tracker: Arc::clone(&self.guarantee_convergence_tracker),
            assurance_convergence_tracker: Arc::clone(&self.assurance_convergence_tracker),
            header_hash_lookup: Arc::clone(&self.header_hash_lookup),
            da_tracker: Arc::clone(&self.da_tracker),
            connection_watch: Arc::clone(&self.connection_watch),
        };

        tokio::spawn(async move {
            let result = handle_connection_optimized(stream, addr, ctx).await;

            // Always decrement connection count
            rate_limiter.connection_closed();

            if let Err(e) = result {
                error!("Connection error from {}: {}", addr, e);
            }
        });
    }

    pub fn get_connections(&self) -> Vec<NodeConnection> {
        self.connections
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get connection count without cloning all NodeConnection structs.
    ///
    /// Much more efficient than `get_connections().len()` for stats queries.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Get list of connected node IDs without cloning NodeConnection structs.
    ///
    /// More efficient than `get_connections()` when only IDs are needed.
    pub fn get_connection_ids(&self) -> Vec<String> {
        self.connections
            .iter()
            .map(|entry| entry.value().id.clone())
            .collect()
    }

    pub fn get_stats(&self) -> ServerStats {
        ServerStats {
            active_connections: self.connections.len(),
            pending_writes: self.batch_writer.pending_count(),
            rate_limiter_stats: self.rate_limiter.get_stats(),
        }
    }

    pub fn get_broadcaster(&self) -> Arc<EventBroadcaster> {
        Arc::clone(&self.broadcaster)
    }

    pub fn get_batch_writer(&self) -> BatchWriter {
        self.batch_writer.clone()
    }

    pub fn get_enricher_map(&self) -> EnricherMap {
        Arc::clone(&self.enricher_map)
    }

    pub fn get_event_counter(&self) -> EventCounter {
        Arc::clone(&self.event_counter)
    }

    pub fn get_slot_tracker(&self) -> SlotTracker {
        Arc::clone(&self.slot_tracker)
    }

    pub fn get_wp_tracker(&self) -> WpTracker {
        Arc::clone(&self.wp_tracker)
    }

    pub fn get_guarantee_convergence_tracker(&self) -> GuaranteeConvergenceTracker {
        Arc::clone(&self.guarantee_convergence_tracker)
    }

    pub fn get_assurance_convergence_tracker(&self) -> AssuranceConvergenceTracker {
        Arc::clone(&self.assurance_convergence_tracker)
    }

    pub fn get_header_hash_lookup(&self) -> HeaderHashLookup {
        Arc::clone(&self.header_hash_lookup)
    }

    pub fn get_da_tracker(&self) -> DaTracker {
        Arc::clone(&self.da_tracker)
    }

    /// Flush slot_tracker, wp_tracker, and event_counter to database on-demand.
    /// For testing only — in production these flush every 5s via the periodic task.
    pub async fn flush_trackers(&self) {
        if let Some(ref store) = self.store {
            crate::slot_tracker::flush_slot_tracker(
                &self.slot_tracker,
                store.pool(),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(60),
            )
            .await;
            crate::wp_tracker::flush_wp_tracker(&self.wp_tracker, store.pool()).await;
            convergence_tracker::flush_guarantee_convergence(
                &self.guarantee_convergence_tracker,
                store.pool(),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(60),
            )
            .await;
            convergence_tracker::flush_assurance_convergence(
                &self.assurance_convergence_tracker,
                store.pool(),
                std::time::Duration::from_secs(10),
                std::time::Duration::from_secs(60),
            )
            .await;
            da_tracker::flush_da_tracker(&self.da_tracker, store.pool()).await;
            event_counter::flush_event_counter(&self.event_counter, store.pool()).await;
        }
    }

    /// Test-only flush: bypasses the age gate so slot_tracker entries flush immediately.
    pub async fn flush_trackers_for_test(&self) {
        if let Some(ref store) = self.store {
            crate::slot_tracker::flush_slot_tracker(
                &self.slot_tracker,
                store.pool(),
                std::time::Duration::ZERO,
                std::time::Duration::from_secs(3600),
            )
            .await;
            crate::wp_tracker::flush_wp_tracker(&self.wp_tracker, store.pool()).await;
            convergence_tracker::flush_guarantee_convergence(
                &self.guarantee_convergence_tracker,
                store.pool(),
                std::time::Duration::ZERO,
                std::time::Duration::from_secs(3600),
            )
            .await;
            convergence_tracker::flush_assurance_convergence(
                &self.assurance_convergence_tracker,
                store.pool(),
                std::time::Duration::ZERO,
                std::time::Duration::from_secs(3600),
            )
            .await;
            da_tracker::flush_da_tracker(&self.da_tracker, store.pool()).await;
            event_counter::flush_event_counter(&self.event_counter, store.pool()).await;
        }
    }

    /// Flush all pending batch writes to database
    ///
    /// **For testing only**: Forces immediate flush of all buffered
    /// data to PostgreSQL. Necessary in tests to ensure data is written
    /// before queries execute.
    ///
    /// In production, this can be used for graceful shutdown but should
    /// NOT be called during normal operation.
    /// Returns the local address the server is listening on.
    ///
    /// Useful in tests that bind to port 0 to discover the assigned port.
    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        match &self.listener {
            Some(listener) => listener.local_addr(),
            None => Ok(self.bind_address),
        }
    }

    pub async fn flush_writes(&self) -> anyhow::Result<()> {
        self.batch_writer.flush().await
    }

    /// Wait until the server has exactly `expected` active connections.
    /// Returns immediately if the condition is already met.
    pub async fn wait_for_connections(&self, expected: usize) {
        let mut rx = self.connection_watch.subscribe();
        while *rx.borrow_and_update() != expected {
            rx.changed().await.expect("connection watch closed");
        }
    }
}

/// Shared server state passed to each connection handler.
struct ConnectionContext {
    connections: Arc<DashMap<String, NodeConnection>>,
    batch_writer: BatchWriter,
    rate_limiter: Arc<RateLimiter>,
    broadcaster: Arc<EventBroadcaster>,
    rate_limit_disabled: bool,
    enricher_map: EnricherMap,
    event_counter: EventCounter,
    slot_tracker: SlotTracker,
    wp_tracker: WpTracker,
    guarantee_convergence_tracker: GuaranteeConvergenceTracker,
    assurance_convergence_tracker: AssuranceConvergenceTracker,
    header_hash_lookup: HeaderHashLookup,
    da_tracker: DaTracker,
    connection_watch: Arc<tokio::sync::watch::Sender<usize>>,
}

async fn handle_connection_optimized(
    mut stream: TcpStream,
    addr: SocketAddr,
    ctx: ConnectionContext,
) -> Result<(), Box<dyn std::error::Error>> {
    let ConnectionContext {
        connections,
        batch_writer,
        rate_limiter,
        broadcaster,
        rate_limit_disabled,
        enricher_map,
        event_counter,
        slot_tracker,
        wp_tracker,
        guarantee_convergence_tracker,
        assurance_convergence_tracker,
        header_hash_lookup,
        da_tracker,
        connection_watch,
    } = ctx;
    // Set TCP nodelay for lower latency
    stream.set_nodelay(true)?;

    let mut buffer = BytesMut::with_capacity(INITIAL_BUFFER_SIZE);
    let mut event_count = 0u64;

    // First message should be NodeInformation
    let node_info = loop {
        let n = stream.read_buf(&mut buffer).await?;
        if n == 0 {
            return Err("Connection closed before receiving node information".into());
        }

        // Prevent buffer overflow
        if buffer.len() > MAX_BUFFER_SIZE {
            return Err("Buffer overflow - node sending too much data".into());
        }

        match decode_message_frame(&buffer) {
            Ok((size, msg_data)) => {
                if size > MAX_MESSAGE_SIZE {
                    return Err(format!("Message too large: {} bytes", size).into());
                }

                let mut cursor = Cursor::new(msg_data);
                match NodeInformation::decode(&mut cursor) {
                    Ok(info) => {
                        buffer.advance(4 + size as usize);
                        break info;
                    }
                    Err(e) => {
                        return Err(format!("Failed to decode node information: {}", e).into());
                    }
                }
            }
            Err(DecodingError::InsufficientData { .. }) => {
                continue;
            }
            Err(e) => {
                return Err(format!("Failed to decode message frame: {}", e).into());
            }
        }
    };

    // Extract core_count from ProtocolParameters for JIP-3 fixed-size arrays
    let core_count = node_info.params.core_count;

    // Generate node ID from peer ID — Arc<str> for zero-cost cloning in hot path
    let node_id_str: Arc<str> = Arc::from(hex::encode(node_info.details.peer_id));

    info!(
        "Node {} connected: {} v{} - {}",
        node_id_str,
        node_info.implementation_name.as_str().unwrap_or("unknown"),
        node_info
            .implementation_version
            .as_str()
            .unwrap_or("unknown"),
        node_info.additional_info.as_str().unwrap_or("")
    );

    // Store connection info (DashMap key is String; one-time alloc per connection)
    let connection = NodeConnection {
        id: node_id_str.to_string(),
        address: addr,
        info: node_info.clone(),
        connected_at: chrono::Utc::now(),
        last_event_at: chrono::Utc::now(),
        event_count: 0,
    };
    connections.insert(node_id_str.to_string(), connection);
    let _ = connection_watch.send(connections.len());

    // Queue node connection event
    info!("Queueing node connection for {}", node_id_str);
    match batch_writer
        .node_connected(node_id_str.clone(), node_info, addr.to_string())
        .await
    {
        Ok(_) => info!("Successfully queued node connection for {}", node_id_str),
        Err(e) => {
            error!("Failed to queue node connection for {}: {}", node_id_str, e);
            return Err(e.into());
        }
    }

    // Track dropped events
    let mut dropped_events = 0u64;

    // Cache metric handles before the event loop (one-time registry lookup).
    // Each metrics::counter!/gauge! macro does a full hash + DashMap lock to find the
    // handle. At 1023 connections x 600 events/sec, this caused ~13% CPU in DashMap
    // spin contention. Caching eliminates it.
    let counter_events_received = metrics::counter!("telemetry_events_received");
    let counter_events_dropped = metrics::counter!("telemetry_events_dropped");
    let gauge_buffer_pending = metrics::gauge!("telemetry_buffer_pending");

    // Per-wakeup batch accumulators. Reused across loop iterations to avoid
    // repeated allocation. Events are collected during the inner decode loop
    // and sent as batches after it completes — one channel send per TCP read
    // wakeup instead of one per event.
    let mut broadcast_batch: Vec<BroadcastRecord> = Vec::with_capacity(64);
    let mut db_batch: Vec<EventRecord> = Vec::with_capacity(64);
    let mut ws_buf: Vec<u8> = Vec::with_capacity(4096);

    // Read events
    loop {
        // Process all complete messages already in buffer before blocking on read.
        // This handles leftover bytes from the node info read and coalesced TCP segments.
        let mut batch_received: u64 = 0;

        while buffer.len() >= 4 {
            match decode_message_frame(&buffer) {
                Ok((size, msg_data)) => {
                    if size > MAX_MESSAGE_SIZE {
                        warn!("Message too large from {}: {} bytes", node_id_str, size);
                        buffer.advance(4 + size as usize);
                        continue;
                    }

                    let mut cursor = Cursor::new(msg_data);
                    match Event::decode_event(&mut cursor, core_count) {
                        Ok(event) => {
                            // EventId assignment: post-increment (JIP-3 §implicit IDs).
                            // First event = ID 0, each subsequent = previous + 1,
                            // except Dropped events advance by `num`.
                            let this_event_id = event_count;
                            match &event {
                                Event::Dropped { num, .. } => {
                                    event_count += num;
                                }
                                _ => {
                                    event_count += 1;
                                }
                            }

                            // Apply rate limiting (unless disabled)
                            if !rate_limit_disabled && !rate_limiter.allow_event(&node_id_str) {
                                dropped_events += 1;
                                counter_events_dropped.increment(1);
                                buffer.advance(4 + size as usize);
                                continue;
                            }

                            // Wrap event in Arc once to share between broadcaster and batch writer
                            let event = Arc::new(event);

                            // Pre-serialize Event JSON once (shared by broadcaster + DB writer)
                            let event_json: Arc<[u8]> = Arc::from(
                                serde_json::to_vec(&*event).unwrap_or_else(|_| b"{}".to_vec()),
                            );

                            // Enrich event with cross-event correlation (core, services, wp_hash)
                            let enriched = {
                                let mut enricher = enricher_map
                                    .entry(node_id_str.clone())
                                    .or_default();
                                enricher.process(&event, this_event_id)
                            };

                            // Update SlotTracker for block propagation convergence
                            if let Some(slot) = enriched.slot {
                                let evt_ts = event.timestamp();
                                let et_raw = event.event_type() as u16;
                                match et_raw {
                                    42 => { // Authored
                                        slot_tracker.entry(slot)
                                            .and_modify(|s| {
                                                s.authored_at = Some(evt_ts);
                                                s.record(et_raw, evt_ts);
                                            })
                                            .or_insert_with(|| {
                                                crate::slot_tracker::SlotState::new(et_raw, evt_ts, Some(evt_ts))
                                            });
                                        // Populate HeaderHashLookup for assurance convergence
                                        if let Event::Authored { outline, .. } = &*event {
                                            header_hash_lookup.insert(outline.hash, slot);
                                        }
                                    }
                                    11 | 12 | 40 | 43 => { // BestBlockChanged, FinalizedBlockChanged, Authoring, Importing
                                        slot_tracker.entry(slot)
                                            .and_modify(|s| {
                                                s.record(et_raw, evt_ts);
                                            })
                                            .or_insert_with(|| {
                                                crate::slot_tracker::SlotState::new(et_raw, evt_ts, None)
                                            });
                                        // Populate HeaderHashLookup for assurance convergence
                                        if let Event::Importing { outline, .. } = &*event {
                                            header_hash_lookup.insert(outline.hash, slot);
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            // Update WpTracker for work package pipeline tracking
                            {
                                let et_raw = event.event_type() as u16;
                                let evt_ts = event.timestamp();
                                match et_raw {
                                    94 => { // WorkPackageReceived
                                        if let Some(hash) = enriched.wp_hash {
                                            let core = enriched.core.unwrap_or(0);
                                            let sids = enriched.service_ids.clone().unwrap_or_default();
                                            let nid = node_id_str.clone();
                                            wp_tracker.entry(hash)
                                                .and_modify(|s| {
                                                    if s.received_nodes.insert(nid.clone()) {
                                                        s.received_by += 1;
                                                    }
                                                    s.last_updated = evt_ts;
                                                    s.dirty = true;
                                                    s.last_activity = std::time::Instant::now();
                                                })
                                                .or_insert_with(|| {
                                                    let mut received_nodes = std::collections::HashSet::new();
                                                    received_nodes.insert(nid.clone());
                                                    crate::wp_tracker::WpState {
                                                        first_seen: evt_ts,
                                                        last_updated: evt_ts,
                                                        core,
                                                        service_ids: sids,
                                                        received_by: 1,
                                                        received_nodes,
                                                        stage: 0,
                                                        received_at: Some(evt_ts),
                                                        dirty: true,
                                                        node_id: Some(nid),
                                                        ..Default::default()
                                                    }
                                                });
                                        }
                                    }
                                    92 => { // WorkPackageFailed
                                        if let Some(hash) = enriched.wp_hash {
                                            // Extract failure reason from event
                                            let reason = if let crate::events::Event::WorkPackageFailed { ref reason, .. } = *event {
                                                reason.as_str().ok().map(|s| s.to_string())
                                            } else {
                                                None
                                            };
                                            wp_tracker.entry(hash).and_modify(|s| {
                                                s.mark_failed(evt_ts);
                                                if s.failure_reason.is_none() {
                                                    s.failure_reason = reason;
                                                }
                                            });
                                        }
                                    }
                                    105 => { // GuaranteeBuilt — also count guarantors
                                        if let Some(hash) = enriched.wp_hash {
                                            wp_tracker.entry(hash).and_modify(|s| {
                                                if s.guaranteed_nodes.insert(node_id_str.clone()) {
                                                    s.guaranteed_by += 1;
                                                }
                                                s.update_stage(4, evt_ts);
                                            });
                                        }
                                    }
                                    101 => { // Refined — extract gas_used
                                        if let Some(hash) = enriched.wp_hash {
                                            let gas = if let crate::events::Event::Refined { ref costs, .. } = *event {
                                                let total: u64 = costs.iter().map(|c| c.total.gas_used).sum();
                                                if total > 0 { Some(total as i64) } else { None }
                                            } else {
                                                None
                                            };
                                            wp_tracker.entry(hash).and_modify(|s| {
                                                s.update_stage(2, evt_ts);
                                                if s.refine_gas_used.is_none() {
                                                    s.refine_gas_used = gas;
                                                }
                                            });
                                        }
                                    }
                                    95 | 102 | 109 => {
                                        if let Some(hash) = enriched.wp_hash {
                                            let ordinal = crate::wp_tracker::event_type_to_ordinal(et_raw);
                                            wp_tracker.entry(hash).and_modify(|s| {
                                                s.update_stage(ordinal, evt_ts);
                                            });
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            // Update GuaranteeConvergenceTracker for guarantee propagation convergence
                            {
                                let et_raw = event.event_type() as u16;
                                let evt_ts = event.timestamp();
                                match et_raw {
                                    105 => { // GuaranteeBuilt — anchor event
                                        if let Event::GuaranteeBuilt { outline, .. } = &*event {
                                            let wrh = outline.work_report_hash;
                                            let slot = outline.slot;
                                            guarantee_convergence_tracker.entry(wrh)
                                                .and_modify(|s| {
                                                    if s.built_at.is_none() {
                                                        s.built_at = Some(evt_ts);
                                                    }
                                                    if s.slot.is_none() {
                                                        s.slot = Some(slot);
                                                    }
                                                    if s.core.is_none() {
                                                        s.core = enriched.core;
                                                    }
                                                    if s.wp_hash.is_none() {
                                                        s.wp_hash = enriched.wp_hash;
                                                    }
                                                    if s.builder_node_id.is_none() {
                                                        s.builder_node_id = Some(node_id_str.clone());
                                                    }
                                                    s.dirty = true;
                                                    s.last_event = std::time::Instant::now();
                                                })
                                                .or_insert_with(|| {
                                                    crate::convergence_tracker::GuaranteeConvergenceState {
                                                        built_at: Some(evt_ts),
                                                        slot: Some(slot),
                                                        core: enriched.core,
                                                        wp_hash: enriched.wp_hash,
                                                        builder_node_id: Some(node_id_str.clone()),
                                                        received_timestamps: Vec::new(),
                                                        last_event: std::time::Instant::now(),
                                                        flushed: false,
                                                        dirty: true,
                                                    }
                                                });
                                        }
                                    }
                                    112 => { // GuaranteeReceived — measured event
                                        if let Event::GuaranteeReceived { outline, .. } = &*event {
                                            let wrh = outline.work_report_hash;
                                            guarantee_convergence_tracker.entry(wrh)
                                                .and_modify(|s| {
                                                    s.received_timestamps.push(evt_ts);
                                                    s.dirty = true;
                                                    s.last_event = std::time::Instant::now();
                                                })
                                                .or_insert_with(|| {
                                                    crate::convergence_tracker::GuaranteeConvergenceState {
                                                        built_at: None,
                                                        slot: None,
                                                        core: None,
                                                        wp_hash: None,
                                                        builder_node_id: None,
                                                        received_timestamps: vec![evt_ts],
                                                        last_event: std::time::Instant::now(),
                                                        flushed: false,
                                                        dirty: true,
                                                    }
                                                });
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            // Update AssuranceConvergenceTracker for assurance propagation convergence
                            {
                                let et_raw = event.event_type() as u16;
                                let evt_ts = event.timestamp();
                                match et_raw {
                                    126 => { // DistributingAssurance — sender begins distribution
                                        if let Event::DistributingAssurance { statement, .. } = &*event {
                                            let anchor = statement.anchor;
                                            let slot = header_hash_lookup.get(&anchor).map(|s| *s);
                                            let sender_id = node_id_str.clone();
                                            assurance_convergence_tracker.entry(anchor)
                                                .and_modify(|s| {
                                                    if s.slot.is_none() {
                                                        s.slot = slot;
                                                    }
                                                    // Drain pending_received for this sender
                                                    let distributed_at = evt_ts;
                                                    let mut resolved = Vec::new();
                                                    s.pending_received.retain(|(sid, ts)| {
                                                        if *sid == sender_id {
                                                            let delta = (*ts as i64 - distributed_at as i64) / 1000;
                                                            resolved.push(delta.max(0) as i32);
                                                            false
                                                        } else {
                                                            true
                                                        }
                                                    });
                                                    let sender_state = s.senders.entry(sender_id.clone())
                                                        .or_insert_with(|| crate::convergence_tracker::SenderAssuranceState {
                                                            distributed_at,
                                                            deltas_ms: Vec::new(),
                                                        });
                                                    sender_state.deltas_ms.extend(resolved);
                                                    s.dirty = true;
                                                    s.last_event = std::time::Instant::now();
                                                })
                                                .or_insert_with(|| {
                                                    let mut senders = std::collections::HashMap::new();
                                                    senders.insert(sender_id, crate::convergence_tracker::SenderAssuranceState {
                                                        distributed_at: evt_ts,
                                                        deltas_ms: Vec::new(),
                                                    });
                                                    crate::convergence_tracker::AnchorState {
                                                        slot,
                                                        senders,
                                                        pending_received: Vec::new(),
                                                        last_event: std::time::Instant::now(),
                                                        flushed: false,
                                                        dirty: true,
                                                    }
                                                });
                                        }
                                    }
                                    131 => { // AssuranceReceived — validator received an assurance
                                        if let Event::AssuranceReceived { anchor, sender, .. } = &*event {
                                            let sender_node_id: Arc<str> = Arc::from(hex::encode(sender));
                                            assurance_convergence_tracker.entry(*anchor)
                                                .and_modify(|s| {
                                                    if s.slot.is_none() {
                                                        s.slot = header_hash_lookup.get(anchor).map(|sl| *sl);
                                                    }
                                                    if let Some(sender_state) = s.senders.get_mut(&sender_node_id) {
                                                        let delta = (evt_ts as i64 - sender_state.distributed_at as i64) / 1000;
                                                        sender_state.deltas_ms.push(delta.max(0) as i32);
                                                    } else {
                                                        // Sender not yet seen — buffer for later resolution
                                                        s.pending_received.push((sender_node_id.clone(), evt_ts));
                                                    }
                                                    s.dirty = true;
                                                    s.last_event = std::time::Instant::now();
                                                })
                                                .or_insert_with(|| {
                                                    crate::convergence_tracker::AnchorState {
                                                        slot: header_hash_lookup.get(anchor).map(|sl| *sl),
                                                        senders: std::collections::HashMap::new(),
                                                        pending_received: vec![(sender_node_id, evt_ts)],
                                                        last_event: std::time::Instant::now(),
                                                        flushed: false,
                                                        dirty: true,
                                                    }
                                                });
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            // Update DaTracker for shard distribution and preimage events
                            {
                                let et_raw = event.event_type() as u16;
                                let evt_ts = event.timestamp();
                                match et_raw {
                                    120 => { // SendingShardRequest — assurer initiates
                                        da_tracker.entry(node_id_str.clone())
                                            .and_modify(|s| {
                                                s.shard_requests_sent += 1;
                                                s.assurer_pending.insert(this_event_id, evt_ts);
                                                s.dirty = true;
                                                s.last_activity = std::time::Instant::now();
                                            })
                                            .or_insert_with(|| {
                                                let mut s = da_tracker::DaNodeState::default();
                                                s.shard_requests_sent = 1;
                                                s.assurer_pending.insert(this_event_id, evt_ts);
                                                s.dirty = true;
                                                s
                                            });
                                    }
                                    121 => { // ReceivingShardRequest — guarantor receives
                                        da_tracker.entry(node_id_str.clone())
                                            .and_modify(|s| {
                                                s.shard_requests_received += 1;
                                                s.guarantor_pending.insert(this_event_id, evt_ts);
                                                s.dirty = true;
                                                s.last_activity = std::time::Instant::now();
                                            })
                                            .or_insert_with(|| {
                                                let mut s = da_tracker::DaNodeState::default();
                                                s.shard_requests_received = 1;
                                                s.guarantor_pending.insert(this_event_id, evt_ts);
                                                s.dirty = true;
                                                s
                                            });
                                    }
                                    122 => { // ShardRequestFailed — check both pending maps
                                        if let Event::ShardRequestFailed { request_id, .. } = &*event {
                                            da_tracker.entry(node_id_str.clone()).and_modify(|s| {
                                                s.shard_failures += 1;
                                                // Compute delta and record in histogram
                                                if let Some(sent_ts) = s.assurer_pending.remove(request_id) {
                                                    let delta_us = evt_ts.saturating_sub(sent_ts);
                                                    let delta_ms = (delta_us / 1000) as i32;
                                                    s.assurer_latency_sum_us += delta_us;
                                                    s.assurer_latency_count += 1;
                                                    let idx = da_tracker::hist_bucket_index(delta_ms);
                                                    s.assurer_hist[idx] += 1;
                                                    s.assurer_hist_total += 1;
                                                    s.assurer_hist_failed += 1;
                                                } else if let Some(recv_ts) = s.guarantor_pending.remove(request_id) {
                                                    let delta_us = evt_ts.saturating_sub(recv_ts);
                                                    let delta_ms = (delta_us / 1000) as i32;
                                                    s.guarantor_latency_sum_us += delta_us;
                                                    s.guarantor_latency_count += 1;
                                                    let idx = da_tracker::hist_bucket_index(delta_ms);
                                                    s.guarantor_hist[idx] += 1;
                                                    s.guarantor_hist_total += 1;
                                                    s.guarantor_hist_failed += 1;
                                                }
                                                s.dirty = true;
                                                s.last_activity = std::time::Instant::now();
                                            });
                                        }
                                    }
                                    123 => { // ShardRequestSent
                                        da_tracker.entry(node_id_str.clone())
                                            .and_modify(|s| {
                                                s.shard_sent_confirmed += 1;
                                                s.dirty = true;
                                                s.last_activity = std::time::Instant::now();
                                            })
                                            .or_insert_with(|| {
                                                let mut s = da_tracker::DaNodeState::default();
                                                s.shard_sent_confirmed = 1;
                                                s.dirty = true;
                                                s
                                            });
                                    }
                                    124 => { // ShardRequestReceived — guarantor side completion
                                        if let Event::ShardRequestReceived { request_id, shard, .. } = &*event {
                                            da_tracker.entry(node_id_str.clone())
                                                .and_modify(|s| {
                                                    s.shard_received_confirmed += 1;
                                                    s.active_shards.insert(*shard);
                                                    if let Some(recv_ts) = s.guarantor_pending.remove(request_id) {
                                                        let delta_us = evt_ts.saturating_sub(recv_ts);
                                                        let delta_ms = (delta_us / 1000) as i32;
                                                        s.guarantor_latency_sum_us += delta_us;
                                                        s.guarantor_latency_count += 1;
                                                        let idx = da_tracker::hist_bucket_index(delta_ms);
                                                        s.guarantor_hist[idx] += 1;
                                                        s.guarantor_hist_total += 1;
                                                    }
                                                    s.dirty = true;
                                                    s.last_activity = std::time::Instant::now();
                                                })
                                                .or_insert_with(|| {
                                                    let mut s = da_tracker::DaNodeState::default();
                                                    s.shard_received_confirmed = 1;
                                                    s.active_shards.insert(*shard);
                                                    s.dirty = true;
                                                    s
                                                });
                                        }
                                    }
                                    125 => { // ShardsTransferred — assurer side completion ONLY
                                        if let Event::ShardsTransferred { request_id, .. } = &*event {
                                            da_tracker.entry(node_id_str.clone())
                                                .and_modify(|s| {
                                                    s.shards_transferred += 1;
                                                    if let Some(sent_ts) = s.assurer_pending.remove(request_id) {
                                                        let delta_us = evt_ts.saturating_sub(sent_ts);
                                                        let delta_ms = (delta_us / 1000) as i32;
                                                        s.assurer_latency_sum_us += delta_us;
                                                        s.assurer_latency_count += 1;
                                                        let idx = da_tracker::hist_bucket_index(delta_ms);
                                                        s.assurer_hist[idx] += 1;
                                                        s.assurer_hist_total += 1;
                                                    }
                                                    s.dirty = true;
                                                    s.last_activity = std::time::Instant::now();
                                                })
                                                .or_insert_with(|| {
                                                    let mut s = da_tracker::DaNodeState::default();
                                                    s.shards_transferred = 1;
                                                    s.dirty = true;
                                                    s
                                                });
                                        }
                                    }
                                    190 => { // PreimageAnnouncementFailed
                                        da_tracker.entry(node_id_str.clone())
                                            .and_modify(|s| {
                                                s.preimage_ann_failures += 1;
                                                s.dirty = true;
                                                s.last_activity = std::time::Instant::now();
                                            })
                                            .or_insert_with(|| {
                                                let mut s = da_tracker::DaNodeState::default();
                                                s.preimage_ann_failures = 1;
                                                s.dirty = true;
                                                s
                                            });
                                    }
                                    191 => { // PreimageAnnounced
                                        da_tracker.entry(node_id_str.clone())
                                            .and_modify(|s| {
                                                s.preimages_announced += 1;
                                                s.dirty = true;
                                                s.last_activity = std::time::Instant::now();
                                            })
                                            .or_insert_with(|| {
                                                let mut s = da_tracker::DaNodeState::default();
                                                s.preimages_announced = 1;
                                                s.dirty = true;
                                                s
                                            });
                                    }
                                    192 => { // AnnouncedPreimageForgotten
                                        da_tracker.entry(node_id_str.clone())
                                            .and_modify(|s| {
                                                s.preimages_forgotten += 1;
                                                s.dirty = true;
                                                s.last_activity = std::time::Instant::now();
                                            })
                                            .or_insert_with(|| {
                                                let mut s = da_tracker::DaNodeState::default();
                                                s.preimages_forgotten = 1;
                                                s.dirty = true;
                                                s
                                            });
                                    }
                                    _ => {}
                                }
                            }

                            // Pre-aggregate high-volume events into in-memory counters
                            let et_val = event.event_type() as u16;
                            if event_counter::is_pre_aggregated(et_val) {
                                let unix_micros = crate::types::JCE_EPOCH_UNIX_MICROS
                                    + event.timestamp() as i64;
                                event_counter::record_event(
                                    &event_counter,
                                    &event,
                                    &enriched,
                                    unix_micros,
                                    &node_id_str,
                                    event.event_type(),
                                );
                            }

                            // Build WS envelope in ingestion thread (parallelized across 8 runtimes)
                            let id = broadcaster.next_event_id();
                            let event_type = event.event_type() as u8;
                            let timestamp = chrono::Utc::now();
                            let ws_json = build_ws_envelope(
                                id,
                                &node_id_str,
                                event_type,
                                &event_json,
                                timestamp,
                                &mut ws_buf,
                            );

                            // Accumulate for batch send after inner loop
                            broadcast_batch.push(BroadcastRecord {
                                node_id: node_id_str.clone(),
                                event: Arc::clone(&event),
                                event_json: Arc::clone(&event_json),
                                id,
                                event_type,
                                timestamp,
                                ws_json,
                            });
                            db_batch.push(EventRecord {
                                node_id: node_id_str.clone(),
                                event_id: this_event_id,
                                event,
                                event_json,
                                enriched,
                            });
                            batch_received += 1;

                            buffer.advance(4 + size as usize);
                        }
                        Err(e) => {
                            let event_type_hint = if msg_data.len() > 8 {
                                format!(" (event_type={})", msg_data[8])
                            } else {
                                String::new()
                            };
                            let hex_preview: String = msg_data
                                .iter()
                                .take(32)
                                .map(|b| format!("{:02x}", b))
                                .collect::<Vec<_>>()
                                .join(" ");
                            warn!(
                                "Failed to decode event from {}{}: {} [msg_len={}, hex={}]",
                                node_id_str,
                                event_type_hint,
                                e,
                                msg_data.len(),
                                hex_preview
                            );
                            buffer.advance(4 + size as usize);
                        }
                    }
                }
                Err(DecodingError::InsufficientData { .. }) => {
                    break;
                }
                Err(e) => {
                    error!("Failed to decode message frame from {}: {}", node_id_str, e);
                    return Err(e.into());
                }
            }
        }

        // Batch-send accumulated events (1 channel send per TCP read, not per event)
        if !broadcast_batch.is_empty() {
            let cap = broadcast_batch.capacity();
            let events = std::mem::replace(&mut broadcast_batch, Vec::with_capacity(cap));
            broadcaster.send_event_batch(events);
        }

        if !db_batch.is_empty() {
            let cap = db_batch.capacity();
            let events = std::mem::replace(&mut db_batch, Vec::with_capacity(cap));
            match batch_writer.write_event_batch(events) {
                Ok(_) => {
                    counter_events_received.increment(batch_received);

                    // Update connection stats periodically
                    if event_count.is_multiple_of(CONNECTION_STATS_UPDATE_FREQUENCY) {
                        if let Some(mut conn) = connections.get_mut(&*node_id_str) {
                            conn.last_event_at = chrono::Utc::now();
                            conn.event_count = event_count;
                        }
                    }
                }
                Err(_) => {
                    dropped_events += batch_received;
                    counter_events_dropped.increment(batch_received);
                    if dropped_events.is_multiple_of(500) {
                        debug!("Write buffer full, dropping events total: {dropped_events}",);
                    }
                }
            }
        }

        // Read more data from stream
        let n = stream.read_buf(&mut buffer).await?;
        if n == 0 {
            info!(
                "Node {} disconnected (received {} events, dropped {})",
                node_id_str, event_count, dropped_events
            );
            break;
        }

        // Prevent buffer overflow
        if buffer.len() > MAX_BUFFER_SIZE {
            warn!("Buffer overflow for node {} - disconnecting", node_id_str);
            break;
        }

        // Update metrics
        gauge_buffer_pending.set(batch_writer.pending_count() as f64);
    }

    // Clean up
    connections.remove(&*node_id_str);
    broadcaster.remove_node_channel(&node_id_str).await;
    let _ = connection_watch.send(connections.len());
    batch_writer.node_disconnected(node_id_str).await?;

    Ok(())
}

#[derive(Debug, serde::Serialize)]
pub struct ServerStats {
    pub active_connections: usize,
    pub pending_writes: usize,
    pub rate_limiter_stats: crate::rate_limiter::RateLimiterStats,
}
