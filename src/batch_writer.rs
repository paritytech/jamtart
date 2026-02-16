use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::events::{Event, NodeInformation};
use crate::store::EventStore;

/// Type alias for batch event tuples stored in the channel and flush buffer
pub(crate) type BatchEvent = (String, u64, Arc<Event>);

/// Maximum number of events to buffer before forcing a flush
/// 2000 matches real throughput at 100K events/sec with 20ms intervals
const MAX_BATCH_SIZE: usize = 2000;

/// Maximum time to wait before flushing a batch
/// 5ms keeps data fresh for real-time dashboard queries while still batching efficiently
/// (~500 events/batch at 100K events/sec is well within Postgres's optimal batch INSERT range)
const BATCH_TIMEOUT: Duration = Duration::from_millis(5);

/// Number of events that can be buffered in the channel
/// 200K provides ~2 seconds of headroom at 100K events/sec processing rate
const CHANNEL_SIZE: usize = 200_000;

#[derive(Clone)]
pub struct BatchWriter {
    sender: Sender<WriterCommand>,
}

enum WriterCommand {
    NodeConnected {
        node_id: String,
        info: Box<NodeInformation>,
    },
    NodeDisconnected {
        node_id: String,
    },
    Event {
        node_id: String,
        event_id: u64,
        event: Arc<Event>,
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
            .send(WriterCommand::NodeConnected {
                node_id,
                info: Box::new(info),
            })
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

    /// Queue an event for writing (non-blocking).
    /// Accepts &str for node_id to avoid cloning in the caller's hot path;
    /// allocates String only at the channel boundary.
    pub fn write_event(&self, node_id: &str, event_id: u64, event: Arc<Event>) -> Result<()> {
        self.sender
            .try_send(WriterCommand::Event {
                node_id: node_id.to_string(),
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

    /// Get buffer usage as a percentage (0-100)
    pub fn buffer_usage_percent(&self) -> f64 {
        (self.pending_count() as f64 / CHANNEL_SIZE as f64) * 100.0
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
    let mut event_batch: Vec<BatchEvent> = Vec::with_capacity(MAX_BATCH_SIZE);
    let mut node_updates: Vec<(String, Option<NodeInformation>)> = Vec::new();

    info!("Batch writer started");

    // Consume the first tick which fires immediately
    interval.tick().await;

    // Single processing loop - no two-phase initialization
    loop {
        tokio::select! {
            // Prefer draining the channel over timer ticks under load
            biased;

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
                        info!("Node connected: {}", node_id);
                        node_updates.push((node_id, Some(*info)));
                        // Accumulate node updates and flush with next batch or when >100 pending
                        // This reduces transaction count during node churn by 10-100x
                        if node_updates.len() > 100 {
                            if let Err(e) = flush_batch(&store, &mut event_batch, &mut node_updates).await {
                                error!("Node connection batch flush error: {}", e);
                            }
                        }
                    }
                    WriterCommand::NodeDisconnected { node_id } => {
                        info!("Node disconnected: {}", node_id);
                        node_updates.push((node_id, None));
                        if node_updates.len() > 100 {
                            if let Err(e) = flush_batch(&store, &mut event_batch, &mut node_updates).await {
                                error!("Node disconnection batch flush error: {}", e);
                            }
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
            _ = interval.tick() => {
                // Periodic flush
                if !event_batch.is_empty() || !node_updates.is_empty() {
                    debug!("Periodic flush: {} events, {} node updates", event_batch.len(), node_updates.len());
                    if let Err(e) = flush_batch(&store, &mut event_batch, &mut node_updates).await {
                        error!("Periodic flush error: {}", e);
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
    event_batch: &mut Vec<BatchEvent>,
    node_updates: &mut Vec<(String, Option<NodeInformation>)>,
) -> Result<()> {
    let event_count = event_batch.len();
    let node_count = node_updates.len();

    if event_count == 0 && node_count == 0 {
        return Ok(());
    }

    let flush_start = std::time::Instant::now();

    debug!(
        "Flushing batch: {} events, {} node updates",
        event_count, node_count
    );

    // Process node updates using batch operations (1 query per type instead of N)
    if !node_updates.is_empty() {
        let mut connections: Vec<(String, NodeInformation)> = Vec::new();
        let mut disconnections: Vec<String> = Vec::new();

        for (node_id, info) in node_updates.drain(..) {
            match info {
                Some(info) => connections.push((node_id, info)),
                None => disconnections.push(node_id),
            }
        }

        if !connections.is_empty() {
            debug!("Batch storing {} node connections", connections.len());
            store
                .store_nodes_connected_batch(&connections)
                .await
                .map_err(|e| {
                    error!("Failed to store node connections batch: {}", e);
                    anyhow::anyhow!("Node connection batch storage failed: {}", e)
                })?;
        }

        if !disconnections.is_empty() {
            debug!("Batch storing {} node disconnections", disconnections.len());
            store
                .store_nodes_disconnected_batch(&disconnections)
                .await
                .map_err(|e| {
                    error!("Failed to store node disconnections batch: {}", e);
                    anyhow::anyhow!("Node disconnection batch storage failed: {}", e)
                })?;
        }
    }

    // Process events using batch insert for optimal performance
    // Events are only cleared after successful write to prevent data loss
    if !event_batch.is_empty() {
        // Collect node_ids for stats update before passing event_batch to store
        let node_ids: Vec<&str> = event_batch.iter().map(|(id, _, _)| id.as_str()).collect();

        // Pre-compute stats update data before mutably borrowing event_batch
        let mut counts: std::collections::HashMap<&str, i64> = std::collections::HashMap::new();
        for id in &node_ids {
            *counts.entry(id).or_insert(0) += 1;
        }
        let unique_ids: Vec<String> = counts.keys().map(|s| s.to_string()).collect();
        let increments: Vec<i64> = unique_ids.iter().map(|id| counts[id.as_str()]).collect();
        drop(node_ids);
        drop(counts);

        store.store_events_batch(event_batch).await.map_err(|e| {
            error!("Failed to store event batch: {}", e);
            anyhow::anyhow!("Event batch storage failed: {}", e)
        })?;

        // Only clear after successful write
        event_batch.clear();

        // Update node stats in batch (replaces per-row trigger)
        if let Err(e) = store
            .update_node_stats_batch_precomputed(&unique_ids, &increments)
            .await
        {
            error!("Failed to update node stats batch: {}", e);
            // Non-fatal: events were already stored, stats will catch up
        }
    }

    // Update metrics
    let flush_duration = flush_start.elapsed();
    metrics::counter!("telemetry_events_flushed").increment(event_count as u64);
    metrics::counter!("telemetry_node_updates_flushed").increment(node_count as u64);
    metrics::histogram!("telemetry_batch_flush_duration_ms")
        .record(flush_duration.as_millis() as f64);

    if flush_duration.as_millis() > 100 {
        warn!(
            "Slow flush: {}ms for {} events, {} nodes",
            flush_duration.as_millis(),
            event_count,
            node_count
        );
    }

    debug!(
        "Flush completed in {}ms: {} events, {} nodes",
        flush_duration.as_millis(),
        event_count,
        node_count
    );
    Ok(())
}
