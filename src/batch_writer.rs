use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::sync::Mutex;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

use crate::enricher::EnrichedFields;
use crate::events::{Event, NodeInformation};
use crate::store::EventStore;
use crate::types::JCE_EPOCH_UNIX_MICROS;

/// Shared string type for node IDs in hot paths.
/// Arc<str> clone is a single atomic increment vs 64-byte heap allocation for String.
pub type NodeId = Arc<str>;

/// Event record with pre-serialized JSON and enriched fields.
/// Used throughout the write pipeline (batch_writer → store).
pub struct EventRecord {
    pub node_id: NodeId,
    pub event_id: u64,
    pub event: Arc<Event>,
    pub event_json: Arc<[u8]>,
    pub enriched: EnrichedFields,
}

/// Number of parallel DB writer tasks (work-stealing pool).
/// More workers = more concurrent COPY operations in flight while waiting on DB I/O.
const NUM_WRITERS: usize = 8;

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
        node_id: NodeId,
        info: Box<NodeInformation>,
        address: String,
    },
    NodeDisconnected {
        node_id: NodeId,
    },
    Event(EventRecord),
    EventBatch {
        events: Vec<EventRecord>,
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
    ///
    /// When `store` is `None` (--no-database mode), workers still drain the channel
    /// to prevent backpressure/OOM but skip all DB writes.
    pub fn new(store: Option<Arc<EventStore>>) -> Self {
        let (sender, receiver) = mpsc::channel(CHANNEL_SIZE);
        let shared_rx = Arc::new(Mutex::new(receiver));
        let shared_node_counts: Arc<Mutex<HashMap<NodeId, u64>>> =
            Arc::new(Mutex::new(HashMap::new()));

        // Single dedicated task for flushing node stats to DB.
        // Aggregates counts from all writers, preventing deadlocks from concurrent UPDATEs.
        if let Some(ref store) = store {
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
    pub async fn node_connected(
        &self,
        node_id: NodeId,
        info: NodeInformation,
        address: String,
    ) -> Result<()> {
        self.sender
            .send(WriterCommand::NodeConnected {
                node_id,
                info: Box::new(info),
                address,
            })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send node connection: {}", e))?;
        Ok(())
    }

    /// Queue a node disconnection event (async for reliability)
    pub async fn node_disconnected(&self, node_id: NodeId) -> Result<()> {
        self.sender
            .send(WriterCommand::NodeDisconnected { node_id })
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send node disconnection: {}", e))?;
        Ok(())
    }

    /// Queue an event for writing (non-blocking)
    pub fn write_event(&self, record: EventRecord) -> Result<()> {
        self.sender
            .try_send(WriterCommand::Event(record))
            .map_err(|e| anyhow::anyhow!("Channel full: {}", e))?;
        Ok(())
    }

    /// Queue a batch of events for writing (non-blocking, single channel send).
    /// Reduces mpsc contention by sending N events in one `try_send` instead of N calls.
    pub fn write_event_batch(&self, events: Vec<EventRecord>) -> Result<()> {
        self.sender
            .try_send(WriterCommand::EventBatch { events })
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

    /// Get buffer usage as a percentage (0.0 - 100.0)
    pub fn buffer_usage_percent(&self) -> f64 {
        (self.pending_count() as f64 / CHANNEL_SIZE as f64) * 100.0
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
    store: Option<Arc<EventStore>>,
    shared_node_counts: Arc<Mutex<HashMap<NodeId, u64>>>,
) -> Result<()> {
    let mut event_batch: Vec<EventRecord> = Vec::with_capacity(MAX_BATCH_SIZE);
    let mut node_connects: Vec<(NodeId, NodeInformation, String)> = Vec::new();
    let mut node_disconnects: Vec<NodeId> = Vec::new();
    let mut node_counts: HashMap<NodeId, u64> = HashMap::new();

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
        // In no-database mode, just discard the events (channel was drained to prevent OOM)
        if !event_batch.is_empty() {
            if let Some(ref store) = store {
                let result = flush_events(store, &mut event_batch).await;
                if let Err(e) = result {
                    error!("Writer {} event flush error: {}", id, e);
                }
            } else {
                event_batch.clear();
            }
        }

        // Phase 3: Flush node connect/disconnect immediately
        // These are rare events (~1024 total) so no need to batch on a timer
        if let Some(ref store) = store {
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
        } else {
            node_connects.clear();
            node_disconnects.clear();
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
            if let Some(ref store) = store {
                if let Err(e) = flush_events(store, &mut event_batch).await {
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
    event_batch: &mut Vec<EventRecord>,
    node_connects: &mut Vec<(NodeId, NodeInformation, String)>,
    node_disconnects: &mut Vec<NodeId>,
    node_counts: &mut HashMap<NodeId, u64>,
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
                    Err(_) => return false,  // timeout — flush what we have
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
    event_batch: &mut Vec<EventRecord>,
    node_connects: &mut Vec<(NodeId, NodeInformation, String)>,
    node_disconnects: &mut Vec<NodeId>,
    node_counts: &mut HashMap<NodeId, u64>,
    flush_response: &mut Option<tokio::sync::oneshot::Sender<Result<()>>>,
) -> CommandAction {
    match cmd {
        WriterCommand::Event(record) => {
            *node_counts.entry(record.node_id.clone()).or_default() += 1;
            event_batch.push(record);
            CommandAction::Continue
        }
        WriterCommand::EventBatch { events } => {
            for record in events {
                *node_counts.entry(record.node_id.clone()).or_default() += 1;
                event_batch.push(record);
            }
            CommandAction::Continue
        }
        WriterCommand::NodeConnected {
            node_id,
            info,
            address,
        } => {
            node_connects.push((node_id, *info, address));
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

/// Event types that generate rows in event_services junction table.
const SERVICE_EVENT_TYPES: &[u16] = &[
    47,  // BlockExecuted (direct, not enriched)
    92,  // WorkPackageFailed
    93,  // DuplicateWorkPackage
    94,  // WorkPackageReceived
    95,  // Authorized
    96,  // ExtrinsicDataReceived
    97,  // ImportsReceived
    98,  // SharingWorkPackage
    99,  // WorkPackageSharingFailed
    100, // BundleSent
    101, // Refined
    102, // WorkReportBuilt
    103, // WorkReportSignatureSent
    104, // WorkReportSignatureReceived
    105, // GuaranteeBuilt
    109, // GuaranteesDistributed
    160, // WorkPackageHashMapped
    161, // SegmentsRootMapped
    168, // ReconstructingSegments
    170, // SegmentsReconstructed
    172, // SegmentsVerified
];

/// Flush only events to database (node updates are decoupled).
async fn flush_events(store: &Arc<EventStore>, event_batch: &mut Vec<EventRecord>) -> Result<()> {
    let event_count = event_batch.len();

    if event_count == 0 {
        return Ok(());
    }

    let start = std::time::Instant::now();

    debug!("Flushing {} events", event_count);

    // Collect event_services rows and node_stats rows before consuming the batch
    let mut service_rows: Vec<(i64, String, i16, i32, Option<i64>)> = Vec::new();
    let mut stats_rows: Vec<(i64, String, i32, i32, i32, i32, i64, i32, i32, i16, i16, f32, i16)> =
        Vec::new();

    for record in event_batch.iter() {
        let et = record.event.event_type() as u16;
        let unix_micros = JCE_EPOCH_UNIX_MICROS + record.event.timestamp() as i64;

        // event_services: enriched events with service_ids
        if SERVICE_EVENT_TYPES.contains(&et) && et != 47 {
            if let Some(ref sids) = record.enriched.service_ids {
                let gas = record.event.gas_per_service_item(sids.len());
                for (i, sid) in sids.iter().enumerate() {
                    service_rows.push((
                        unix_micros,
                        record.node_id.to_string(),
                        et as i16,
                        *sid as i32,
                        gas.get(i).copied().flatten(),
                    ));
                }
            }
        }

        // event_services: BlockExecuted (direct, no enricher needed)
        if et == 47 {
            if let crate::events::Event::BlockExecuted {
                accumulate_costs, ..
            } = &*record.event
            {
                for (service_id, cost) in accumulate_costs {
                    service_rows.push((
                        unix_micros,
                        record.node_id.to_string(),
                        47i16,
                        *service_id as i32,
                        Some(cost.total.gas_used as i64),
                    ));
                }
            }
        }

        // node_stats: Status events
        if et == 10 {
            if let crate::events::Event::Status {
                num_peers,
                num_val_peers,
                num_sync_peers,
                num_guarantees,
                num_shards,
                shards_size,
                num_preimages,
                preimages_size,
                ..
            } = &*record.event
            {
                let min_g = num_guarantees.iter().copied().min().unwrap_or(0) as i16;
                let max_g = num_guarantees.iter().copied().max().unwrap_or(0) as i16;
                let avg_g = if num_guarantees.is_empty() {
                    0.0
                } else {
                    num_guarantees.iter().map(|&v| v as f32).sum::<f32>()
                        / num_guarantees.len() as f32
                };
                let zero_g = num_guarantees.iter().filter(|&&v| v == 0).count() as i16;

                stats_rows.push((
                    unix_micros,
                    record.node_id.to_string(),
                    *num_peers as i32,
                    *num_val_peers as i32,
                    *num_sync_peers as i32,
                    *num_shards as i32,
                    *shards_size as i64,
                    *num_preimages as i32,
                    *preimages_size as i32,
                    min_g,
                    max_g,
                    avg_g,
                    zero_g,
                ));
            }
        }
    }

    // Flush events to DB
    let batch = std::mem::take(event_batch);
    store.store_events_batch(batch).await.map_err(|e| {
        error!("Failed to store event batch: {}", e);
        anyhow::anyhow!("Event batch storage failed: {}", e)
    })?;

    // Flush event_services (independent, failure doesn't roll back events)
    if !service_rows.is_empty() {
        let refs: Vec<(i64, &str, i16, i32, Option<i64>)> = service_rows
            .iter()
            .map(|(ts, nid, et, sid, g)| (*ts, nid.as_str(), *et, *sid, *g))
            .collect();
        if let Err(e) = store.store_event_services_batch(&refs).await {
            warn!("Failed to flush event_services: {}", e);
        }
    }

    // Flush node_stats (independent)
    if !stats_rows.is_empty() {
        let refs: Vec<(i64, &str, i32, i32, i32, i32, i64, i32, i32, i16, i16, f32, i16)> =
            stats_rows
                .iter()
                .map(|(ts, nid, a, b, c, d, e, f, g, h, i, j, k)| {
                    (*ts, nid.as_str(), *a, *b, *c, *d, *e, *f, *g, *h, *i, *j, *k)
                })
                .collect();
        if let Err(e) = store.store_node_stats_batch(&refs).await {
            warn!("Failed to flush node_stats: {}", e);
        }
    }

    // Update metrics
    metrics::counter!("telemetry_events_flushed").increment(event_count as u64);

    debug!(
        "Flush completed: {} events in {:?}",
        event_count,
        start.elapsed()
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::Event;
    use crate::types::*;
    use std::sync::Arc;

    fn zero_exec() -> ExecCost {
        ExecCost { gas_used: 0, elapsed_ns: 0 }
    }

    fn zero_refine_host() -> RefineHostCallCost {
        RefineHostCallCost {
            lookup: zero_exec(), vm: zero_exec(), mem: zero_exec(),
            invoke: zero_exec(), other: zero_exec(),
        }
    }

    fn zero_accum_host() -> AccumulateHostCallCost {
        AccumulateHostCallCost {
            state: zero_exec(), lookup: zero_exec(), preimage: zero_exec(),
            service: zero_exec(), transfer: zero_exec(), transfer_dest_gas: 0,
            other: zero_exec(),
        }
    }

    /// Test: BlockExecuted gas extraction produces correct service_rows
    #[test]
    fn test_service_rows_from_block_executed() {
        let event = Event::BlockExecuted {
            timestamp: 1000,
            authoring_or_importing_id: 1,
            accumulate_costs: vec![
                (10, AccumulateCost {
                    num_calls: 1, num_transfers: 0, num_items: 1,
                    total: ExecCost { gas_used: 500, elapsed_ns: 100 },
                    load_ns: 50, host_call: zero_accum_host(),
                }),
                (20, AccumulateCost {
                    num_calls: 2, num_transfers: 1, num_items: 3,
                    total: ExecCost { gas_used: 1200, elapsed_ns: 200 },
                    load_ns: 60, host_call: zero_accum_host(),
                }),
            ],
        };

        // Simulate the extraction pattern from flush_events
        let mut service_rows: Vec<(i32, Option<i64>)> = Vec::new();
        if let Event::BlockExecuted { accumulate_costs, .. } = &event {
            for (service_id, cost) in accumulate_costs {
                service_rows.push((*service_id as i32, Some(cost.total.gas_used as i64)));
            }
        }

        assert_eq!(service_rows.len(), 2);
        assert_eq!(service_rows[0], (10, Some(500)));
        assert_eq!(service_rows[1], (20, Some(1200)));
    }

    /// Test: Authorized gas assigns to first service only
    #[test]
    fn test_authorized_gas_first_only() {
        let event = Event::Authorized {
            timestamp: 1000,
            submission_or_share_id: 100,
            cost: IsAuthorizedCost {
                total: ExecCost { gas_used: 999, elapsed_ns: 50 },
                load_ns: 10,
                host_call: ExecCost { gas_used: 100, elapsed_ns: 20 },
            },
        };

        let gas = event.gas_per_service_item(3);
        assert_eq!(gas.len(), 3);
        assert_eq!(gas[0], Some(999));
        assert_eq!(gas[1], None);
        assert_eq!(gas[2], None);
    }

    /// Test: Status event fields are correctly extracted for node_stats rows
    #[test]
    fn test_node_stats_from_status() {
        let event = Event::Status {
            timestamp: 1000,
            num_peers: 50,
            num_val_peers: 30,
            num_sync_peers: 10,
            num_guarantees: vec![0, 3, 5, 0, 2],
            num_shards: 100,
            shards_size: 50000,
            num_preimages: 20,
            preimages_size: 1000,
        };

        // Simulate the extraction pattern from flush_events
        if let Event::Status {
            num_peers, num_val_peers, num_sync_peers,
            num_guarantees, num_shards, shards_size,
            num_preimages, preimages_size, ..
        } = &event {
            let min_g = num_guarantees.iter().copied().min().unwrap_or(0) as i16;
            let max_g = num_guarantees.iter().copied().max().unwrap_or(0) as i16;
            let avg_g = if num_guarantees.is_empty() {
                0.0
            } else {
                num_guarantees.iter().map(|&v| v as f32).sum::<f32>()
                    / num_guarantees.len() as f32
            };
            let zero_g = num_guarantees.iter().filter(|&&v| v == 0).count() as i16;

            assert_eq!(min_g, 0);
            assert_eq!(max_g, 5);
            assert!((avg_g - 2.0).abs() < f32::EPSILON);
            assert_eq!(zero_g, 2);
            assert_eq!(*num_peers, 50);
            assert_eq!(*num_val_peers, 30);
            assert_eq!(*num_sync_peers, 10);
            assert_eq!(*num_shards, 100);
            assert_eq!(*shards_size, 50000);
            assert_eq!(*num_preimages, 20);
            assert_eq!(*preimages_size, 1000);
        } else {
            panic!("Expected Status event");
        }
    }
}
