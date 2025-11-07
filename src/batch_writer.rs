use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::interval;
use tracing::{debug, error, info};

use crate::events::{Event, NodeInformation};
use crate::store::EventStore;

/// Maximum number of events to buffer before forcing a flush
/// Increased from 100 to 500 to handle higher throughput (50k events/sec @ 10ms interval)
const MAX_BATCH_SIZE: usize = 500;

/// Maximum time to wait before flushing a batch
/// Decreased from 20ms to 10ms for better throughput and lower latency
const BATCH_TIMEOUT: Duration = Duration::from_millis(10);

/// Number of events that can be buffered in the channel
/// Increased from 100k to 500k to handle bursts from 1024 concurrent nodes
const CHANNEL_SIZE: usize = 500_000;

#[derive(Clone)]
pub struct BatchWriter {
    sender: Sender<WriterCommand>,
}

enum WriterCommand {
    NodeConnected {
        node_id: String,
        info: NodeInformation,
    },
    NodeDisconnected {
        node_id: String,
    },
    Event {
        node_id: String,
        event_id: u64,
        event: Event,
    },
    Flush {
        response: tokio::sync::oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

impl BatchWriter {
    pub fn new(store: Arc<EventStore>) -> Self {
        let (sender, receiver) = mpsc::channel(CHANNEL_SIZE);

        // Spawn the background writer task
        // Note: This task is critical. If it fails, the entire process should restart
        // because:
        // 1. The channel receiver is consumed and cannot be recreated without
        //    invalidating all existing BatchWriter sender references
        // 2. A failed batch writer means NO events are persisted (silent data loss)
        // 3. Failing fast is better than silently appearing to work
        // 4. The health check (batch_writer_check) will detect unresponsiveness
        //    before panic, allowing monitoring/alerting systems to act
        tokio::spawn(async move {
            info!("Batch writer task started");
            match batch_writer_loop(receiver, store).await {
                Ok(_) => {
                    info!("Batch writer task completed normally");
                }
                Err(e) => {
                    error!(
                        "CRITICAL: Batch writer task failed - events will not be persisted: {}",
                        e
                    );
                    error!(
                        "This is a fatal error. The process should be restarted by the supervisor."
                    );
                    // Panic to trigger process restart via systemd/k8s
                    panic!("Batch writer task failed: {}. Process restart required.", e);
                }
            }
        });

        BatchWriter { sender }
    }

    /// Queue a node connection event (async for reliability)
    pub async fn node_connected(&self, node_id: String, info: NodeInformation) -> Result<()> {
        self.sender
            .send(WriterCommand::NodeConnected { node_id, info })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send node connection: {}", e))?;
        Ok(())
    }

    /// Queue a node disconnection event (async for reliability)
    pub async fn node_disconnected(&self, node_id: String) -> Result<()> {
        self.sender
            .send(WriterCommand::NodeDisconnected { node_id })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send node disconnection: {}", e))?;
        Ok(())
    }

    /// Queue an event for writing (non-blocking)
    pub fn write_event(&self, node_id: String, event_id: u64, event: Event) -> Result<()> {
        self.sender
            .try_send(WriterCommand::Event {
                node_id,
                event_id,
                event,
            })
            .map_err(|e| anyhow::anyhow!("Channel full: {}", e))?;
        Ok(())
    }

    /// Check if the writer can accept more events
    pub fn is_full(&self) -> bool {
        self.sender.capacity() == 0
    }

    /// Get the number of events currently buffered
    pub fn pending_count(&self) -> usize {
        CHANNEL_SIZE - self.sender.capacity()
    }

    /// Shutdown the batch writer
    pub async fn shutdown(&self) -> Result<()> {
        self.sender
            .send(WriterCommand::Shutdown)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send shutdown command: {}", e))?;
        Ok(())
    }

    /// Flush all pending writes to database
    ///
    /// **For testing only**: This method forces immediate flush of all
    /// batched events and node updates to the database. It's necessary
    /// in tests to ensure data is written before queries, since the
    /// batch writer runs asynchronously in the background.
    ///
    /// In production, the batch writer automatically flushes based on:
    /// - Time-based intervals (20ms)
    /// - Batch size limits (1000 events)
    /// - Node connection events (immediate flush)
    ///
    /// This method is public (not #[cfg(test)]) because it's useful
    /// for graceful shutdown and debugging, but should NOT be called
    /// in normal operation as it defeats the purpose of batching.
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.sender
            .send(WriterCommand::Flush { response: tx })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send flush command: {}", e))?;
        let result = rx
            .await
            .map_err(|e| anyhow::anyhow!("Flush response channel closed: {}", e))?;
        result.map_err(|e| anyhow::anyhow!("Flush failed: {}", e))
    }
}

async fn batch_writer_loop(
    mut receiver: Receiver<WriterCommand>,
    store: Arc<EventStore>,
) -> Result<()> {
    let mut interval = interval(BATCH_TIMEOUT);
    let mut event_batch = Vec::with_capacity(MAX_BATCH_SIZE);
    let mut node_updates: Vec<(String, Option<NodeInformation>)> = Vec::new();

    info!("Batch writer started");

    // Consume the first tick which fires immediately
    interval.tick().await;

    // Single processing loop - no two-phase initialization
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Periodic flush
                if !event_batch.is_empty() || !node_updates.is_empty() {
                    debug!("Periodic flush: {} events, {} node updates", event_batch.len(), node_updates.len());
                    if let Err(e) = flush_batch(&store, &mut event_batch, &mut node_updates).await {
                        error!("Periodic flush error: {}", e);
                    }
                }
            }
            Some(command) = receiver.recv() => {
                match command {
                    WriterCommand::Event { node_id, event_id, event } => {
                        event_batch.push((node_id, event_id, event));

                        // Flush if batch is full
                        if event_batch.len() >= MAX_BATCH_SIZE {
                            debug!("Batch size flush: {} events", event_batch.len());
                            if let Err(e) = flush_batch(&store, &mut event_batch, &mut node_updates).await {
                                error!("Batch size flush error: {}", e);
                            }
                        }
                    }
                    WriterCommand::NodeConnected { node_id, info } => {
                        info!("Node connected: {}, flushing immediately", node_id);
                        node_updates.push((node_id, Some(info)));
                        // Force flush node updates immediately to avoid race conditions
                        if let Err(e) = flush_batch(&store, &mut event_batch, &mut node_updates).await {
                            error!("Node connection flush error: {}", e);
                        }
                    }
                    WriterCommand::NodeDisconnected { node_id } => {
                        info!("Node disconnected: {}, flushing immediately", node_id);
                        node_updates.push((node_id, None));
                        // Flush disconnections immediately too (symmetry with connections)
                        if let Err(e) = flush_batch(&store, &mut event_batch, &mut node_updates).await {
                            error!("Node disconnection flush error: {}", e);
                        }
                    }
                    WriterCommand::Flush { response } => {
                        debug!("Manual flush requested: {} events, {} node updates", event_batch.len(), node_updates.len());
                        let result = flush_batch(&store, &mut event_batch, &mut node_updates).await;
                        if let Err(ref e) = result {
                            error!("Manual flush error: {}", e);
                        }
                        let _ = response.send(result);
                    }
                    WriterCommand::Shutdown => {
                        info!("Batch writer shutting down, performing final flush");
                        if let Err(e) = flush_batch(&store, &mut event_batch, &mut node_updates).await {
                            error!("Shutdown flush error: {}", e);
                        }
                        break;
                    }
                }
            }
        }
    }

    info!("Batch writer stopped");
    Ok(())
}

async fn flush_batch(
    store: &EventStore,
    event_batch: &mut Vec<(String, u64, Event)>,
    node_updates: &mut Vec<(String, Option<NodeInformation>)>,
) -> Result<()> {
    let event_count = event_batch.len();
    let node_count = node_updates.len();

    if event_count == 0 && node_count == 0 {
        return Ok(());
    }

    debug!(
        "Flushing batch: {} events, {} node updates",
        event_count, node_count
    );

    // Process node updates first
    // Note: Using drain() for simplicity. If an error occurs, we fail fast (panic)
    // since the batch writer task failure will restart the process anyway.
    for (node_id, info) in node_updates.drain(..) {
        match info {
            Some(info) => {
                debug!("Storing node connection for {}", node_id);
                store
                    .store_node_connected(&node_id, &info)
                    .await
                    .map_err(|e| {
                        error!("Failed to store node connection {}: {}", node_id, e);
                        anyhow::anyhow!("Node connection storage failed: {}", e)
                    })?;
                debug!("Successfully stored node connection for {}", node_id);
            }
            None => {
                debug!("Storing node disconnection for {}", node_id);
                store.store_node_disconnected(&node_id).await.map_err(|e| {
                    error!("Failed to store node disconnection {}: {}", node_id, e);
                    anyhow::anyhow!("Node disconnection storage failed: {}", e)
                })?;
            }
        }
    }

    // Process events using batch insert for optimal performance
    if !event_batch.is_empty() {
        let batch = std::mem::take(event_batch);
        store.store_events_batch(batch).await.map_err(|e| {
            error!("Failed to store event batch: {}", e);
            anyhow::anyhow!("Event batch storage failed: {}", e)
        })?;
    }

    // Update metrics
    metrics::counter!("telemetry_events_flushed").increment(event_count as u64);
    metrics::counter!("telemetry_node_updates_flushed").increment(node_count as u64);

    debug!(
        "Flush completed successfully: {} events, {} nodes",
        event_count, node_count
    );
    Ok(())
}
