use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::events::{Event, NodeInformation};
use crate::store::EventStore;

/// Number of parallel DB writer tasks (work-stealing pool).
/// More workers = more concurrent COPY operations in flight while waiting on DB I/O.
const NUM_WRITERS: usize = 32;

/// Maximum number of events to buffer per writer before flushing.
/// Increased from 3,000 to 10,000 for TimescaleDB hypertable efficiency.
const MAX_BATCH_SIZE: usize = 16_000;

/// Maximum time to wait for events to accumulate before flushing.
/// After receiving the first event, the worker will keep draining for up to
/// this duration (or until MAX_BATCH_SIZE is reached), preventing tiny 1-event
/// flushes when many workers compete for a trickle of events.
const BATCH_TIMEOUT: Duration = Duration::from_millis(100);

/// Total channel capacity shared across all writers.
const CHANNEL_SIZE: usize = 5_000_000;

/// Interval for flushing per-node event counts to the database.
/// Replaces the per-row trigger which is catastrophic at 3M events/s.
const NODE_STATS_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct BatchWriter {
    sender: Sender<WriterCommand>,
}

enum WriterCommand {
    NodeConnected {
        node_id: String,
        info: NodeInformation,
        address: String,
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
    /// Create a new BatchWriter with N parallel writer workers (work-stealing pool).
    ///
    /// The receiver is wrapped in `Arc<Mutex<Receiver>>` and shared across N workers.
    /// Each worker locks the mutex briefly to drain events, then releases it and
    /// performs the slow DB write without holding the lock. This provides natural
    /// work-stealing: idle workers pick up events while busy workers are blocked on I/O.
    pub fn new(store: Arc<EventStore>) -> Self {
        let (sender, receiver) = mpsc::channel(CHANNEL_SIZE);
        let shared_rx = Arc::new(Mutex::new(receiver));
        let shared_node_counts: Arc<Mutex<HashMap<String, u64>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Single dedicated task for flushing node stats to DB.
        // Aggregates counts from all writers, preventing deadlocks from concurrent UPDATEs.
        {
            let counts = shared_node_counts.clone();
            let store = store.clone();
            tokio::spawn(async move {
                let mut tick = interval(NODE_STATS_INTERVAL);
                tick.tick().await;
                loop {
                    tick.tick().await;
                    let batch = {
                        let mut map = counts.lock().await;
                        if map.is_empty() {
                            continue;
                        }
                        std::mem::take(&mut *map)
                    };
                    if let Err(e) = store.update_node_stats(&batch).await {
                        warn!("Stats flusher failed to update node stats: {}", e);
                    }
                }
            });
        }

        for id in 0..NUM_WRITERS {
            let rx = shared_rx.clone();
            let store = store.clone();
            let node_counts = shared_node_counts.clone();
            tokio::spawn(async move {
                info!("Writer worker {} started", id);
                match writer_worker(id, rx, store, node_counts).await {
                    Ok(_) => {
                        info!("Writer worker {} completed normally", id);
                    }
                    Err(e) => {
                        error!(
                            "CRITICAL: Writer worker {} failed - events may not be persisted: {}",
                            id, e
                        );
                        panic!(
                            "Writer worker {} failed: {}. Process restart required.",
                            id, e
                        );
                    }
                }
            });
        }

        BatchWriter { sender }
    }

    /// Queue a node connection event (async for reliability)
    pub async fn node_connected(&self, node_id: String, info: NodeInformation, address: String) -> Result<()> {
        self.sender
            .send(WriterCommand::NodeConnected { node_id, info, address })
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

    /// Shutdown all writer workers
    pub async fn shutdown(&self) -> Result<()> {
        self.sender
            .send(WriterCommand::Shutdown)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send shutdown command: {}", e))?;
        Ok(())
    }

    /// Flush all pending writes to database.
    ///
    /// **For testing only**: Forces immediate flush. Sends a flush command to
    /// every worker so all local buffers are drained, then waits for all to complete.
    pub async fn flush(&self) -> Result<()> {
        let mut receivers = Vec::with_capacity(NUM_WRITERS);
        for _ in 0..NUM_WRITERS {
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.sender
                .send(WriterCommand::Flush { response: tx })
                .await
                .map_err(|e| anyhow::anyhow!("Failed to send flush command: {}", e))?;
            receivers.push(rx);
        }
        for rx in receivers {
            let result = rx
                .await
                .map_err(|e| anyhow::anyhow!("Flush response channel closed: {}", e))?;
            result.map_err(|e| anyhow::anyhow!("Flush failed: {}", e))?;
        }
        Ok(())
    }
}

/// Individual writer worker task. Multiple instances share the same receiver
/// via Arc<Mutex<Receiver>>, implementing implicit work-stealing.
///
/// Each worker:
/// 1. Locks the mutex briefly to drain events via try_recv() (microseconds)
/// 2. Releases the mutex
/// 3. Flushes the event batch to DB (milliseconds, no lock held)
/// 4. Node updates are flushed separately on a timer (decoupled from event path)
/// 5. Other workers drain events while this one is blocked on I/O
async fn writer_worker(
    id: usize,
    receiver: Arc<Mutex<Receiver<WriterCommand>>>,
    store: Arc<EventStore>,
    shared_node_counts: Arc<Mutex<HashMap<String, u64>>>,
) -> Result<()> {
    let mut event_batch: Vec<(String, u64, Event)> = Vec::with_capacity(MAX_BATCH_SIZE);
    let mut node_connects: Vec<(String, NodeInformation, String)> = Vec::new();
    let mut node_disconnects: Vec<String> = Vec::new();
    let mut node_counts: HashMap<String, u64> = HashMap::new();

    let mut stats_interval = interval(NODE_STATS_INTERVAL);
    stats_interval.tick().await;

    loop {
        // Phase 1: Acquire lock and drain available events into local batch.
        // Hold lock only for try_recv() calls (microseconds), release before DB I/O.
        let mut flush_response: Option<tokio::sync::oneshot::Sender<Result<()>>> = None;
        let should_shutdown = drain_from_channel(
            &receiver,
            &mut event_batch,
            &mut node_connects,
            &mut node_disconnects,
            &mut node_counts,
            &mut flush_response,
        )
        .await;

        // Phase 2: Flush EVENT batch to DB (slow, milliseconds — NO lock held)
        if !event_batch.is_empty() {
            let result = flush_events(&store, &mut event_batch).await;
            if let Err(e) = result {
                error!("Writer {} event flush error: {}", id, e);
            }
        }

        // Phase 3: Flush node connect/disconnect immediately
        // These are rare events (~1024 total) so no need to batch on a timer
        if !node_connects.is_empty() {
            let connects = std::mem::take(&mut node_connects);
            if let Err(e) = store.store_nodes_connected_batch(&connects).await {
                warn!("Writer {} failed to flush node connects: {}", id, e);
            }
        }
        if !node_disconnects.is_empty() {
            let disconnects = std::mem::take(&mut node_disconnects);
            if let Err(e) = store.store_nodes_disconnected_batch(&disconnects).await {
                warn!("Writer {} failed to flush node disconnects: {}", id, e);
            }
        }

        // Send flush response after everything is written
        if let Some(response) = flush_response.take() {
            let _ = response.send(Ok(()));
        }

        // Phase 4: Merge local node counts into shared aggregator (every 5 seconds)
        // A single dedicated task flushes the shared map to DB, preventing deadlocks
        if tokio::time::timeout(Duration::ZERO, stats_interval.tick())
            .await
            .is_ok()
            && !node_counts.is_empty()
        {
            let local = std::mem::take(&mut node_counts);
            let mut shared = shared_node_counts.lock().await;
            for (node_id, count) in local {
                *shared.entry(node_id).or_default() += count;
            }
        }

        if should_shutdown {
            // Final event flush
            if let Err(e) = flush_events(&store, &mut event_batch).await {
                error!("Writer {} shutdown event flush error: {}", id, e);
            }
            // Final node updates flush
            if !node_connects.is_empty() {
                let _ = store.store_nodes_connected_batch(&node_connects).await;
            }
            if !node_disconnects.is_empty() {
                let _ = store
                    .store_nodes_disconnected_batch(&node_disconnects)
                    .await;
            }
            // Merge remaining node counts into shared aggregator
            if !node_counts.is_empty() {
                let mut shared = shared_node_counts.lock().await;
                for (node_id, count) in &node_counts {
                    *shared.entry(node_id.clone()).or_default() += count;
                }
            }
            info!("Writer worker {} stopped", id);
            break;
        }
    }

    Ok(())
}

/// Drain events from the shared channel into local buffers.
/// Returns true if a Shutdown command was received.
///
/// Strategy: block-wait for the first event, then keep draining for up to
/// BATCH_TIMEOUT (or until MAX_BATCH_SIZE). This prevents tiny 1-event flushes
/// when 32 workers compete for events on a lightly-loaded channel.
async fn drain_from_channel(
    receiver: &Arc<Mutex<Receiver<WriterCommand>>>,
    event_batch: &mut Vec<(String, u64, Event)>,
    node_connects: &mut Vec<(String, NodeInformation, String)>,
    node_disconnects: &mut Vec<String>,
    node_counts: &mut HashMap<String, u64>,
    flush_response: &mut Option<tokio::sync::oneshot::Sender<Result<()>>>,
) -> bool {
    let mut rx = receiver.lock().await;

    // Phase 1: Block-wait for the first event (no point spinning with empty batch)
    match rx.recv().await {
        Some(cmd) => match handle_command(
            cmd,
            event_batch,
            node_connects,
            node_disconnects,
            node_counts,
            flush_response,
        ) {
            CommandAction::Shutdown => return true,
            CommandAction::Flush => return false,
            CommandAction::Continue => {}
        },
        None => return true, // channel closed
    }

    // Phase 2: Drain as many events as possible within BATCH_TIMEOUT.
    // This lets events accumulate into larger batches instead of flushing
    // after every tiny handful of events.
    let deadline = tokio::time::Instant::now() + BATCH_TIMEOUT;

    loop {
        if event_batch.len() >= MAX_BATCH_SIZE || flush_response.is_some() {
            return false;
        }

        match rx.try_recv() {
            Ok(cmd) => match handle_command(
                cmd,
                event_batch,
                node_connects,
                node_disconnects,
                node_counts,
                flush_response,
            ) {
                CommandAction::Continue => {}
                CommandAction::Shutdown => return true,
                CommandAction::Flush => return false,
            },
            Err(_) => {
                // Channel empty — wait for more events or timeout
                match tokio::time::timeout_at(deadline, rx.recv()).await {
                    Ok(Some(cmd)) => match handle_command(
                        cmd,
                        event_batch,
                        node_connects,
                        node_disconnects,
                        node_counts,
                        flush_response,
                    ) {
                        CommandAction::Continue => {}
                        CommandAction::Shutdown => return true,
                        CommandAction::Flush => return false,
                    },
                    Ok(None) => return true, // channel closed
                    Err(_) => return false,   // timeout — flush what we have
                }
            }
        }
    }
}

enum CommandAction {
    Continue,
    Shutdown,
    Flush,
}

fn handle_command(
    cmd: WriterCommand,
    event_batch: &mut Vec<(String, u64, Event)>,
    node_connects: &mut Vec<(String, NodeInformation, String)>,
    node_disconnects: &mut Vec<String>,
    node_counts: &mut HashMap<String, u64>,
    flush_response: &mut Option<tokio::sync::oneshot::Sender<Result<()>>>,
) -> CommandAction {
    match cmd {
        WriterCommand::Event {
            node_id,
            event_id,
            event,
        } => {
            *node_counts.entry(node_id.clone()).or_default() += 1;
            event_batch.push((node_id, event_id, event));
            CommandAction::Continue
        }
        WriterCommand::NodeConnected { node_id, info, address } => {
            node_connects.push((node_id, info, address));
            CommandAction::Continue
        }
        WriterCommand::NodeDisconnected { node_id } => {
            node_disconnects.push(node_id);
            CommandAction::Continue
        }
        WriterCommand::Flush { response } => {
            *flush_response = Some(response);
            CommandAction::Flush
        }
        WriterCommand::Shutdown => CommandAction::Shutdown,
    }
}

/// Flush only events to database (node updates are decoupled).
async fn flush_events(
    store: &Arc<EventStore>,
    event_batch: &mut Vec<(String, u64, Event)>,
) -> Result<()> {
    let event_count = event_batch.len();

    if event_count == 0 {
        return Ok(());
    }

    let start = std::time::Instant::now();

    debug!("Flushing {} events", event_count);

    // Process events using batch insert
    let batch = std::mem::take(event_batch);
    store.store_events_batch(batch).await.map_err(|e| {
        error!("Failed to store event batch: {}", e);
        anyhow::anyhow!("Event batch storage failed: {}", e)
    })?;

    // Update metrics
    metrics::counter!("telemetry_events_flushed").increment(event_count as u64);

    debug!(
        "Flush completed: {} events in {:?}",
        event_count,
        start.elapsed()
    );
    Ok(())
}
