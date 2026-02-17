use crate::batch_writer::BatchWriter;
use crate::decoder::{decode_message_frame, Decode, DecodingError};
use crate::event_broadcaster::EventBroadcaster;
use crate::events::{Event, NodeInformation};
use crate::rate_limiter::RateLimiter;
use crate::store::EventStore;
use bytes::BytesMut;
use dashmap::DashMap;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{timeout, Duration};
use tracing::{debug, error, info, warn};

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
const CONNECTION_STATS_UPDATE_FREQUENCY: u64 = 100;

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
    bind_address: String,
    connections: Arc<DashMap<String, NodeConnection>>,
    /// EventStore reference - owned by BatchWriter but kept here for lifetime management
    /// and potential future direct queries
    #[allow(dead_code)]
    store: Arc<EventStore>,
    batch_writer: BatchWriter,
    rate_limiter: Arc<RateLimiter>,
    broadcaster: Arc<EventBroadcaster>,
}

impl TelemetryServer {
    pub async fn new(bind_address: &str, store: Arc<EventStore>) -> Result<Self, std::io::Error> {
        // Test bind to make sure address is valid
        let _ = TcpListener::bind(bind_address).await?;
        info!("Telemetry server configured for {}", bind_address);

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

        Ok(Self {
            bind_address: bind_address.to_string(),
            connections: Arc::new(DashMap::new()),
            batch_writer: BatchWriter::new(Arc::clone(&store)),
            rate_limiter: Arc::new(RateLimiter::new()),
            broadcaster: Arc::new(EventBroadcaster::new()),
            store,
        })
    }

    pub async fn run(&self) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(&self.bind_address).await?;
        info!(
            "Optimized telemetry server listening on {}",
            self.bind_address
        );

        // Set TCP socket options for better performance on Unix
        #[cfg(unix)]
        configure_socket_receive_buffer(&listener);

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

    /// Run the server until a shutdown signal is received.
    pub async fn run_until_shutdown(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), std::io::Error> {
        let listener = TcpListener::bind(&self.bind_address).await?;
        info!(
            "Optimized telemetry server listening on {}",
            self.bind_address
        );

        #[cfg(unix)]
        configure_socket_receive_buffer(&listener);

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
            1024
        );

        let connections = Arc::clone(&self.connections);
        let batch_writer = self.batch_writer.clone();
        let rate_limiter = Arc::clone(&self.rate_limiter);
        let broadcaster = Arc::clone(&self.broadcaster);

        tokio::spawn(async move {
            let result = handle_connection_optimized(
                stream,
                addr,
                connections,
                batch_writer,
                rate_limiter.clone(),
                broadcaster,
            )
            .await;

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

    /// Flush all pending batch writes to database
    ///
    /// **For testing only**: Forces immediate flush of all buffered
    /// data to PostgreSQL. Necessary in tests to ensure data is written
    /// before queries execute.
    ///
    /// In production, this can be used for graceful shutdown but should
    /// NOT be called during normal operation.
    pub async fn flush_writes(&self) -> anyhow::Result<()> {
        self.batch_writer.flush().await
    }
}

async fn handle_connection_optimized(
    mut stream: TcpStream,
    addr: SocketAddr,
    connections: Arc<DashMap<String, NodeConnection>>,
    batch_writer: BatchWriter,
    rate_limiter: Arc<RateLimiter>,
    broadcaster: Arc<EventBroadcaster>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Set TCP nodelay for lower latency
    stream.set_nodelay(true)?;

    let mut buffer = BytesMut::with_capacity(INITIAL_BUFFER_SIZE);
    let mut event_count = 0u64;

    // First message should be NodeInformation
    let node_info = loop {
        let n = match timeout(Duration::from_secs(30), stream.read_buf(&mut buffer)).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => return Err(e.into()),
            Err(_) => return Err("Timeout waiting for node information".into()),
        };
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

    // Generate node ID from peer ID
    let node_id_str = hex::encode(node_info.details.peer_id);

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

    // Queue node connection event first - only insert into DashMap on success
    info!("Queueing node connection for {}", node_id_str);
    match batch_writer
        .node_connected(node_id_str.clone(), node_info.clone(), addr.to_string())
        .await
    {
        Ok(_) => {
            info!("Successfully queued node connection for {}", node_id_str);
            // Store connection info only after successful batch_writer registration
            let connection = NodeConnection {
                id: node_id_str.clone(),
                address: addr,
                info: node_info,
                connected_at: chrono::Utc::now(),
                last_event_at: chrono::Utc::now(),
                event_count: 0,
            };
            connections.insert(node_id_str.clone(), connection);
        }
        Err(e) => {
            error!("Failed to queue node connection for {}: {}", node_id_str, e);
            return Err(e.into());
        }
    }

    // Track dropped events
    let mut dropped_events = 0u64;

    // Read events
    loop {
        // Process all complete messages already in buffer before blocking on read.
        // This handles leftover bytes from the node info read and coalesced TCP segments.
        while buffer.len() >= 4 {
            match decode_message_frame(&buffer) {
                Ok((size, msg_data)) => {
                    if size > MAX_MESSAGE_SIZE {
                        warn!("Message too large from {}: {} bytes", node_id_str, size);
                        buffer.advance(4 + size as usize);
                        continue;
                    }

                    let mut cursor = Cursor::new(msg_data);
                    match Event::decode(&mut cursor) {
                        Ok(event) => {
                            event_count += 1;

                            // Apply rate limiting
                            if !rate_limiter.allow_event(&node_id_str) {
                                dropped_events += 1;
                                metrics::counter!("telemetry_events_dropped").increment(1);
                                buffer.advance(4 + size as usize);
                                continue;
                            }

                            debug!(
                                "Received event {} from node {}",
                                event.event_type() as u8,
                                node_id_str
                            );

                            // Wrap event in Arc once to share between broadcaster and batch writer
                            let event = Arc::new(event);

                            // Broadcast event for real-time subscribers (non-blocking, sync)
                            let broadcast_result =
                                broadcaster.broadcast_event(&node_id_str, Arc::clone(&event));

                            if let Ok(event_id) = broadcast_result {
                                debug!("Broadcast event {} from node {}", event_id, node_id_str);
                            }

                            // Try to queue event for batch writing
                            // Unwrap Arc — broadcaster already has its clone
                            let event_owned = Arc::try_unwrap(event)
                                .unwrap_or_else(|arc| (*arc).clone());
                            match batch_writer.write_event(node_id_str.clone(), event_count, event_owned) {
                                Ok(_) => {
                                    metrics::counter!("telemetry_events_received").increment(1);

                                    // Update connection stats periodically
                                    if event_count.is_multiple_of(CONNECTION_STATS_UPDATE_FREQUENCY)
                                    {
                                        if let Some(mut conn) = connections.get_mut(&node_id_str) {
                                            conn.last_event_at = chrono::Utc::now();
                                            conn.event_count = event_count;
                                        }
                                    }
                                }
                                Err(_) => {
                                    // Buffer is full, apply backpressure
                                    dropped_events += 1;
                                    metrics::counter!("telemetry_events_dropped").increment(1);
                                    warn!("Write buffer full, dropping event from {}", node_id_str);
                                }
                            }

                            buffer.advance(4 + size as usize);
                        }
                        Err(e) => {
                            // Timestamp is u64 (8 bytes LE), discriminator is at byte 8
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

        // Update metrics periodically (every 100 events) to reduce overhead in hot loop
        if event_count.is_multiple_of(100) {
            metrics::gauge!("telemetry_buffer_pending").set(batch_writer.pending_count() as f64);
        }

        // Read more data from the stream (120s idle timeout for dead connections)
        let n = match timeout(Duration::from_secs(120), stream.read_buf(&mut buffer)).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                info!("Node {} read error: {}", node_id_str, e);
                break;
            }
            Err(_) => {
                warn!("Node {} timed out after 120s idle", node_id_str);
                break;
            }
        };
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
    }

    // Clean up
    connections.remove(&node_id_str);
    batch_writer.node_disconnected(node_id_str).await?;

    Ok(())
}

trait BytesMutExt {
    fn advance(&mut self, cnt: usize);
}

impl BytesMutExt for BytesMut {
    fn advance(&mut self, cnt: usize) {
        let _ = self.split_to(cnt);
    }
}

#[derive(Debug, serde::Serialize)]
pub struct ServerStats {
    pub active_connections: usize,
    pub pending_writes: usize,
    pub rate_limiter_stats: crate::rate_limiter::RateLimiterStats,
}
