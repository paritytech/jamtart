use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::{interval, timeout};
use tracing::{debug, error, info, warn};

use crate::events::{Event, NodeInformation};
use crate::store::EventStore;

/// Maximum number of events to buffer before forcing a flush
const MAX_BATCH_SIZE: usize = 100;

/// Maximum time to wait before flushing a batch
const BATCH_TIMEOUT: Duration = Duration::from_millis(20);

/// Number of events that can be buffered in the channel
const CHANNEL_SIZE: usize = 100_000;

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
        response: tokio::sync::oneshot::Sender<()>,
    },
    Shutdown,
}

impl BatchWriter {
    pub fn new(store: Arc<EventStore>) -> Self {
        let (sender, receiver) = mpsc::channel(CHANNEL_SIZE);

        // Spawn the background writer task
        tokio::spawn(async move {
            if let Err(e) = batch_writer_loop(receiver, store).await {
                error!("Batch writer error: {}", e);
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
    pub async fn shutdown(&self) {
        let _ = self.sender.send(WriterCommand::Shutdown).await;
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
        rx.await
            .map_err(|e| anyhow::anyhow!("Flush response error: {}", e))?;
        Ok(())
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

    // Small delay to allow all initial connections to queue
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    // Try to receive initial messages immediately with timeout
    let mut initial_count = 0;
    loop {
        match timeout(Duration::from_millis(10), receiver.recv()).await {
            Ok(Some(command)) => {
                initial_count += 1;
                info!(
                    "Processing initial command: {:?}",
                    match &command {
                        WriterCommand::NodeConnected { node_id, .. } =>
                            format!("NodeConnected({})", node_id),
                        WriterCommand::NodeDisconnected { node_id } =>
                            format!("NodeDisconnected({})", node_id),
                        WriterCommand::Event { node_id, .. } => format!("Event({})", node_id),
                        WriterCommand::Flush { .. } => "Flush".to_string(),
                        WriterCommand::Shutdown => "Shutdown".to_string(),
                    }
                );
                match command {
                    WriterCommand::NodeConnected { node_id, info } => {
                        info!("Received NodeConnected command for {}", node_id);
                        node_updates.push((node_id, Some(info)));
                        // Force flush node updates immediately to avoid race conditions
                        flush_batch(&store, &mut event_batch, &mut node_updates).await;
                    }
                    WriterCommand::Event {
                        node_id,
                        event_id,
                        event,
                    } => {
                        event_batch.push((node_id, event_id, event));
                        if event_batch.len() >= MAX_BATCH_SIZE {
                            flush_batch(&store, &mut event_batch, &mut node_updates).await;
                        }
                    }
                    WriterCommand::NodeDisconnected { node_id } => {
                        node_updates.push((node_id, None));
                    }
                    WriterCommand::Flush { response } => {
                        flush_batch(&store, &mut event_batch, &mut node_updates).await;
                        let _ = response.send(());
                    }
                    WriterCommand::Shutdown => {
                        info!("Batch writer shutting down");
                        flush_batch(&store, &mut event_batch, &mut node_updates).await;
                        return Ok(());
                    }
                }
            }
            Ok(None) => {
                // Channel closed
                warn!("Batch writer channel closed");
                return Ok(());
            }
            Err(_) => {
                // Timeout, no more initial messages
                break;
            }
        }
    }

    info!("Processed {} initial commands", initial_count);

    // Main processing loop
    loop {
        tokio::select! {
            _ = interval.tick() => {
                // Timeout reached, flush any pending events
                if !event_batch.is_empty() || !node_updates.is_empty() {
                    flush_batch(&store, &mut event_batch, &mut node_updates).await;
                }
            }
            Some(command) = receiver.recv() => {
                match command {
                    WriterCommand::Event { node_id, event_id, event } => {
                        event_batch.push((node_id, event_id, event));

                        // Flush if batch is full
                        if event_batch.len() >= MAX_BATCH_SIZE {
                            flush_batch(&store, &mut event_batch, &mut node_updates).await;
                        }
                    }
                    WriterCommand::NodeConnected { node_id, info } => {
                        info!("Received NodeConnected command for {}", node_id);
                        node_updates.push((node_id, Some(info)));
                        // Force flush node updates immediately to avoid race conditions
                        flush_batch(&store, &mut event_batch, &mut node_updates).await;
                    }
                    WriterCommand::NodeDisconnected { node_id } => {
                        node_updates.push((node_id, None));
                    }
                    WriterCommand::Flush { response } => {
                        flush_batch(&store, &mut event_batch, &mut node_updates).await;
                        let _ = response.send(());
                    }
                    WriterCommand::Shutdown => {
                        info!("Batch writer shutting down");
                        // Final flush
                        flush_batch(&store, &mut event_batch, &mut node_updates).await;
                        break;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn flush_batch(
    store: &EventStore,
    event_batch: &mut Vec<(String, u64, Event)>,
    node_updates: &mut Vec<(String, Option<NodeInformation>)>,
) {
    let event_count = event_batch.len();
    let node_count = node_updates.len();

    if event_count == 0 && node_count == 0 {
        return;
    }

    info!(
        "Flushing batch: {} events, {} node updates",
        event_count, node_count
    );

    // Process node updates first
    for (node_id, info) in node_updates.drain(..) {
        match info {
            Some(info) => {
                info!("Storing node connection for {}", node_id);
                match store.store_node_connected(&node_id, &info).await {
                    Ok(_) => info!("Successfully stored node connection for {}", node_id),
                    Err(e) => error!("Failed to store node connection {}: {}", node_id, e),
                }
            }
            None => {
                debug!("Storing node disconnection for {}", node_id);
                if let Err(e) = store.store_node_disconnected(&node_id).await {
                    error!("Failed to store node disconnection {}: {}", node_id, e);
                }
            }
        }
    }

    // Process events using batch insert for optimal performance
    if !event_batch.is_empty() {
        let batch: Vec<(String, u64, Event)> = std::mem::take(event_batch);
        if let Err(e) = store.store_events_batch(batch).await {
            error!("Failed to store event batch: {}", e);
        }
    }

    // Update metrics
    metrics::counter!("telemetry_events_flushed").increment(event_count as u64);
    metrics::counter!("telemetry_node_updates_flushed").increment(node_count as u64);
}
