//! Broadcast channel for fan-out of decoded telemetry events to WebSocket
//! subscribers and other real-time consumers.

use crate::events::Event;
use crate::metrics_tracker::MetricsEvent;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::trace;

/// Size of the main broadcast channel
/// 500K provides ~5 seconds of buffer at peak throughput (100K events/sec)
const BROADCAST_CHANNEL_SIZE: usize = 500_000;

/// Size of per-node channels (smaller since filtered)
const NODE_CHANNEL_SIZE: usize = 10_000;

/// Maximum number of events to retain in memory for instant replay
const MAX_RETAINED_EVENTS: usize = 10_000;

/// Size of the MPSC aggregation channel.
/// Matches BROADCAST_CHANNEL_SIZE to handle the same burst capacity.
const AGGREGATION_CHANNEL_SIZE: usize = 500_000;

/// Max events per aggregator drain cycle before yielding to tokio.
/// At 600K events/s this gives ~60 yields/s = yield every ~16ms,
/// preventing WS loop starvation (observed 700ms stalls without this).
const AGGREGATOR_DRAIN_LIMIT: usize = 10_000;

/// Size of the command channel for WS subscribe/unsubscribe requests.
const COMMAND_CHANNEL_SIZE: usize = 256;

/// Commands sent from WS handlers to the aggregator task.
/// The aggregator owns the node channels HashMap — all access goes through here.
enum AggregatorCommand {
    SubscribeNode {
        node_id: String,
        reply: oneshot::Sender<Option<broadcast::Receiver<Arc<BroadcastEvent>>>>,
    },
    SubscribeNodes {
        node_ids: Vec<String>,
        reply: oneshot::Sender<Vec<(String, broadcast::Receiver<Arc<BroadcastEvent>>)>>,
    },
    SubscribeAllNodes {
        reply: oneshot::Sender<Vec<(String, broadcast::Receiver<Arc<BroadcastEvent>>)>>,
    },
    RemoveNodeChannel {
        node_id: String,
    },
}

/// Event record produced by ingestion threads with pre-serialized WS JSON.
/// Sent via MPSC to the aggregator which does pure routing (no serialization).
pub struct BroadcastRecord {
    pub node_id: Arc<str>,
    pub event: Arc<Event>,
    pub event_json: Arc<[u8]>,
    pub id: u64,
    pub event_type: u8,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    /// Pre-serialized WS envelope JSON (built in ingestion thread)
    pub ws_json: Option<Arc<str>>,
}

/// Batch of events sent from a connection handler to the aggregator task via MPSC.
/// One channel message per TCP read wakeup instead of one per event.
/// The `Arc<[u8]>` carries pre-serialized Event JSON (serialized once in server.rs).
struct IncomingBatch {
    events: Vec<BroadcastRecord>,
}

/// Typed struct for direct WebSocket JSON serialization (used in tests to verify RawValue equivalence).
#[cfg(test)]
#[derive(Serialize)]
struct WsBroadcast<'a> {
    r#type: &'static str,
    data: WsBroadcastData<'a>,
    timestamp: chrono::DateTime<chrono::Utc>,
}

/// Inner data payload for WsBroadcast (used in tests to verify RawValue equivalence).
#[cfg(test)]
#[derive(Serialize)]
struct WsBroadcastData<'a> {
    id: u64,
    node_id: &'a str,
    event_type: u8,
    event: &'a Event,
}

/// WsBroadcast variant using pre-serialized Event JSON via RawValue.
/// Avoids re-serializing the Event enum — the RawValue is embedded verbatim.
#[derive(Serialize)]
pub(crate) struct WsBroadcastRaw<'a> {
    pub r#type: &'static str,
    pub data: WsBroadcastDataRaw<'a>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

/// Inner data payload using RawValue for the pre-serialized Event.
#[derive(Serialize)]
pub(crate) struct WsBroadcastDataRaw<'a> {
    pub id: u64,
    pub node_id: &'a str,
    pub event_type: u8,
    pub event: &'a serde_json::value::RawValue,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BroadcastEvent {
    pub id: u64,
    pub node_id: Arc<str>,
    pub event: Arc<Event>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub event_type: u8,
    /// Pre-serialized JSON for WebSocket delivery (serialize once, send to all subscribers)
    #[serde(skip)]
    pub serialized_json: Option<Arc<str>>,
    /// Pre-serialized standalone Event JSON (serialized once in server.rs)
    #[serde(skip)]
    pub event_json: Option<Arc<[u8]>>,
}

/// Statistics for monitoring broadcaster performance
#[derive(Debug, Serialize)]
pub struct BroadcasterStats {
    pub total_events_broadcast: u64,
    pub active_subscribers: usize,
    pub node_channels: usize,
    pub events_in_buffer: usize,
    pub dropped_events: u64,
    pub undelivered_events: u64, // Events with no subscribers but still in buffer
}

/// Build the WS envelope JSON from pre-serialized Event JSON.
/// Uses RawValue to embed the event verbatim (no re-serialization).
/// `ws_buf` is a reusable buffer owned by the caller (avoids per-event allocation).
pub(crate) fn build_ws_envelope(
    id: u64,
    node_id: &str,
    event_type: u8,
    event_json: &[u8],
    timestamp: chrono::DateTime<chrono::Utc>,
    ws_buf: &mut Vec<u8>,
) -> Option<Arc<str>> {
    let raw_str = std::str::from_utf8(event_json).ok()?;
    let raw_value = serde_json::value::RawValue::from_string(raw_str.to_string()).ok()?;
    let ws_response = WsBroadcastRaw {
        r#type: "event",
        data: WsBroadcastDataRaw {
            id,
            node_id,
            event_type,
            event: &raw_value,
        },
        timestamp,
    };
    ws_buf.clear();
    serde_json::to_writer(&mut *ws_buf, &ws_response).ok()?;
    Some(Arc::from(unsafe { std::str::from_utf8_unchecked(ws_buf) }))
}

/// High-performance event broadcaster designed for 1024+ nodes.
///
/// Node channels are owned exclusively by the aggregator task (no shared lock).
/// WS handlers communicate via a command channel (mpsc) with oneshot replies.
pub struct EventBroadcaster {
    /// MPSC sender for connection handlers to submit event batches to the aggregator.
    /// `try_send()` is lock-free — no contention between 1023 connection tasks.
    event_sender: mpsc::Sender<IncomingBatch>,

    /// MPSC receiver, wrapped in Mutex<Option<>> so `start_aggregator()` can take it once.
    event_receiver: Mutex<Option<mpsc::Receiver<IncomingBatch>>>,

    /// Main broadcast channel for all events
    sender: broadcast::Sender<Arc<BroadcastEvent>>,

    /// Command channel for WS handlers to subscribe/unsubscribe from node channels.
    command_sender: mpsc::Sender<AggregatorCommand>,

    /// Command receiver, taken once by `start_aggregator()`.
    command_receiver: Mutex<Option<mpsc::Receiver<AggregatorCommand>>>,

    /// Ring buffer of recent events for new connections
    /// Uses parking_lot::Mutex for fast sync access (no await points in critical section)
    recent_events: Arc<Mutex<VecDeque<Arc<BroadcastEvent>>>>,

    /// Event counter for unique IDs
    event_counter: Arc<AtomicU64>,

    /// Node channel count — updated by aggregator, read by get_stats()
    node_channel_count: Arc<AtomicUsize>,

    /// Statistics
    total_broadcast: Arc<AtomicU64>,
    dropped_events: Arc<AtomicU64>,
    undelivered_events: Arc<AtomicU64>,

    /// Optional sender for metrics tracker task (filtered events only)
    metrics_tx: Option<mpsc::Sender<MetricsEvent>>,
}

impl Default for EventBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBroadcaster {
    pub fn new() -> Self {
        Self::with_metrics_tx(None)
    }

    pub fn with_metrics_tx(metrics_tx: Option<mpsc::Sender<MetricsEvent>>) -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CHANNEL_SIZE);
        let (event_sender, event_receiver) = mpsc::channel(AGGREGATION_CHANNEL_SIZE);
        let (command_sender, command_receiver) = mpsc::channel(COMMAND_CHANNEL_SIZE);

        Self {
            event_sender,
            event_receiver: Mutex::new(Some(event_receiver)),
            sender,
            command_sender,
            command_receiver: Mutex::new(Some(command_receiver)),
            recent_events: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_RETAINED_EVENTS))),
            event_counter: Arc::new(AtomicU64::new(0)),
            node_channel_count: Arc::new(AtomicUsize::new(0)),
            total_broadcast: Arc::new(AtomicU64::new(0)),
            dropped_events: Arc::new(AtomicU64::new(0)),
            undelivered_events: Arc::new(AtomicU64::new(0)),
            metrics_tx,
        }
    }

    /// Route a pre-built event record to subscribers and ring buffer.
    /// Called only from the aggregator task — no serialization, pure routing.
    fn broadcast_event(
        &self,
        record: BroadcastRecord,
        node_channels: &mut HashMap<String, broadcast::Sender<Arc<BroadcastEvent>>>,
    ) {
        let main_receivers = self.sender.receiver_count();
        let node_has_receivers = node_channels
            .get(&*record.node_id)
            .is_some_and(|s| s.receiver_count() > 0);

        let broadcast_event = Arc::new(BroadcastEvent {
            id: record.id,
            node_id: record.node_id.clone(),
            event_type: record.event_type,
            timestamp: record.timestamp,
            event: record.event,
            serialized_json: record.ws_json,
            event_json: if main_receivers > 0 || node_has_receivers {
                Some(record.event_json)
            } else {
                None
            },
        });

        if main_receivers > 0 {
            let _ = self.sender.send(broadcast_event.clone());
        }
        self.total_broadcast.fetch_add(1, Ordering::Relaxed);

        // Dispatch to per-node channel (create on first event for this node)
        if let Some(sender) = node_channels.get(&*record.node_id) {
            let _ = sender.send(broadcast_event.clone());
        } else {
            let (tx, _) = broadcast::channel(NODE_CHANNEL_SIZE);
            let _ = tx.send(broadcast_event.clone());
            node_channels.insert(record.node_id.to_string(), tx);
            self.node_channel_count
                .store(node_channels.len(), Ordering::Relaxed);
        }

        // Ring buffer for API recent events
        {
            let mut recent = self.recent_events.lock();
            if recent.len() >= MAX_RETAINED_EVENTS {
                recent.pop_front();
            }
            recent.push_back(broadcast_event.clone());
        }

        // Forward all events to MetricsTracker task (non-blocking)
        if let Some(ref tx) = self.metrics_tx {
            let _ = tx.try_send(MetricsEvent {
                node_id: broadcast_event.node_id.clone(),
                event: broadcast_event.event.clone(),
                event_type: broadcast_event.event_type,
                wall_clock: Instant::now(),
            });
        }
    }

    /// Submit an event to the aggregation channel for processing.
    ///
    /// This is the public API for connection handlers. Events are funnelled through
    /// an MPSC channel to a single aggregator task, eliminating lock contention on
    /// the broadcast channel and ring buffer.
    ///
    /// Uses `try_send()` which is lock-free and will return an error if the channel
    /// is full (backpressure).
    /// Returns `true` if the event was submitted, `false` if the channel is full.
    pub fn send_event(&self, node_id: Arc<str>, event: Arc<Event>, event_json: Arc<[u8]>) -> bool {
        let id = self.next_event_id();
        let event_type = event.event_type() as u8;
        self.event_sender
            .try_send(IncomingBatch {
                events: vec![BroadcastRecord {
                    node_id,
                    event_type,
                    id,
                    timestamp: chrono::Utc::now(),
                    ws_json: None,
                    event,
                    event_json,
                }],
            })
            .is_ok()
    }

    /// Submit a batch of events in a single channel `try_send`.
    /// At 600K ev/s with ~50 events per TCP read, this turns 600K channel sends
    /// into ~12K, dramatically reducing atomic CAS contention in `Tx::find_block`.
    pub fn send_event_batch(&self, events: Vec<BroadcastRecord>) -> bool {
        if events.is_empty() {
            return true;
        }
        self.event_sender.try_send(IncomingBatch { events }).is_ok()
    }

    /// Assign a unique event ID. Called from ingestion threads.
    /// `fetch_add` is always atomic — `Relaxed` only affects cross-variable ordering.
    pub fn next_event_id(&self) -> u64 {
        self.event_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Spawn the aggregator task that drains the MPSC channel and calls `broadcast_event()`.
    ///
    /// This must be called exactly once after construction. The aggregator is the sole
    /// caller of `broadcast_event()`, which eliminates contention on the broadcast mutex
    /// and ring buffer RwLock.
    pub fn start_aggregator(self: &Arc<Self>) {
        let mut receiver = self
            .event_receiver
            .lock()
            .take()
            .expect("start_aggregator() must be called exactly once");

        let mut cmd_receiver = self
            .command_receiver
            .lock()
            .take()
            .expect("start_aggregator() must be called exactly once");

        let this = Arc::clone(self);

        tokio::spawn(async move {
            // Node channels owned exclusively by this task — no lock needed
            let mut node_channels: HashMap<String, broadcast::Sender<Arc<BroadcastEvent>>> =
                HashMap::with_capacity(1024);

            // Debug stats
            let mut debug_last_log = tokio::time::Instant::now();
            let mut debug_events_total = 0u64;
            let mut debug_drain_max_events = 0u64;
            let mut debug_drain_max_us = 0u64;
            let mut debug_drain_count = 0u64;

            loop {
                tokio::select! {
                    Some(batch) = receiver.recv() => {
                        let drain_start = tokio::time::Instant::now();
                        let mut drain_events = 0u64;

                        for record in batch.events {
                            this.broadcast_event(record, &mut node_channels);
                            drain_events += 1;
                        }

                        // Drain additional buffered batches, but yield after AGGREGATOR_DRAIN_LIMIT
                        while (drain_events as usize) < AGGREGATOR_DRAIN_LIMIT {
                            match receiver.try_recv() {
                                Ok(batch) => {
                                    for record in batch.events {
                                        this.broadcast_event(record, &mut node_channels);
                                        drain_events += 1;
                                    }
                                }
                                Err(_) => break,
                            }
                        }

                        let drain_us = drain_start.elapsed().as_micros() as u64;
                        debug_events_total += drain_events;
                        debug_drain_count += 1;
                        if drain_events > debug_drain_max_events {
                            debug_drain_max_events = drain_events;
                        }
                        if drain_us > debug_drain_max_us {
                            debug_drain_max_us = drain_us;
                        }

                        if debug_last_log.elapsed() >= std::time::Duration::from_secs(2) {
                            let elapsed = debug_last_log.elapsed().as_secs_f64();
                            trace!(
                                "Aggregator stats: {:.0} events/s, drains={}, max_drain_events={}, max_drain_us={}, mpsc_lag={}",
                                debug_events_total as f64 / elapsed,
                                debug_drain_count,
                                debug_drain_max_events,
                                debug_drain_max_us,
                                receiver.len(),
                            );
                            debug_events_total = 0;
                            debug_drain_max_events = 0;
                            debug_drain_max_us = 0;
                            debug_drain_count = 0;
                            debug_last_log = tokio::time::Instant::now();
                        }
                    }

                    Some(cmd) = cmd_receiver.recv() => {
                        match cmd {
                            AggregatorCommand::SubscribeNode { node_id, reply } => {
                                let rx = node_channels.get(&node_id).map(|s| s.subscribe());
                                let _ = reply.send(rx);
                            }
                            AggregatorCommand::SubscribeNodes { node_ids, reply } => {
                                let subs = node_ids.iter()
                                    .filter_map(|id| node_channels.get(id).map(|s| (id.clone(), s.subscribe())))
                                    .collect();
                                let _ = reply.send(subs);
                            }
                            AggregatorCommand::SubscribeAllNodes { reply } => {
                                let subs = node_channels.iter()
                                    .map(|(id, s)| (id.clone(), s.subscribe()))
                                    .collect();
                                let _ = reply.send(subs);
                            }
                            AggregatorCommand::RemoveNodeChannel { node_id } => {
                                if let Some(sender) = node_channels.get(&node_id) {
                                    if sender.receiver_count() == 0 {
                                        node_channels.remove(&node_id);
                                        this.node_channel_count.store(node_channels.len(), Ordering::Relaxed);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
    }

    /// Subscribe to all events
    pub fn subscribe_all(&self) -> broadcast::Receiver<Arc<BroadcastEvent>> {
        self.sender.subscribe()
    }

    /// Subscribe to a specific node's channel via aggregator command.
    /// Returns None if node_id hasn't connected yet (no events received).
    pub async fn subscribe_node(
        &self,
        node_id: &str,
    ) -> Option<broadcast::Receiver<Arc<BroadcastEvent>>> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_sender
            .send(AggregatorCommand::SubscribeNode {
                node_id: node_id.to_string(),
                reply: tx,
            })
            .await;
        rx.await.ok().flatten()
    }

    /// Subscribe to multiple nodes via aggregator command.
    /// Node IDs that haven't connected yet are silently skipped.
    pub async fn subscribe_nodes(
        &self,
        node_ids: &[String],
    ) -> Vec<(String, broadcast::Receiver<Arc<BroadcastEvent>>)> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_sender
            .send(AggregatorCommand::SubscribeNodes {
                node_ids: node_ids.to_vec(),
                reply: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Subscribe to all currently-known node channels via aggregator command.
    pub async fn subscribe_all_nodes(
        &self,
    ) -> Vec<(String, broadcast::Receiver<Arc<BroadcastEvent>>)> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .command_sender
            .send(AggregatorCommand::SubscribeAllNodes { reply: tx })
            .await;
        rx.await.unwrap_or_default()
    }

    /// Remove a node's broadcast channel if no WS clients are subscribed.
    /// Called when a node's TCP connection drops.
    pub async fn remove_node_channel(&self, node_id: &str) {
        let _ = self
            .command_sender
            .send(AggregatorCommand::RemoveNodeChannel {
                node_id: node_id.to_string(),
            })
            .await;
    }

    /// Get recent events for catch-up on new connections
    /// Returns up to `limit` most recent events
    pub fn get_recent_events(&self, limit: Option<usize>) -> Vec<Arc<BroadcastEvent>> {
        let recent = self.recent_events.lock();
        let limit = limit.unwrap_or(recent.len()).min(recent.len());

        if limit == 0 {
            return Vec::new();
        }

        // Return most recent events (VecDeque iterator is efficient)
        let skip = recent.len().saturating_sub(limit);
        recent.iter().skip(skip).cloned().collect()
    }

    /// Get recent events filtered by node
    pub fn get_recent_events_by_node(
        &self,
        node_id: &str,
        limit: usize,
    ) -> Vec<Arc<BroadcastEvent>> {
        let recent = self.recent_events.lock();
        // Single pass: collect from reverse iterator, then reverse the result
        let mut result = Vec::with_capacity(limit.min(recent.len()));
        for event in recent.iter().rev() {
            if &*event.node_id == node_id {
                result.push(Arc::clone(event));
                if result.len() >= limit {
                    break;
                }
            }
        }
        result.reverse();
        result
    }

    /// Get broadcaster statistics
    pub fn get_stats(&self) -> BroadcasterStats {
        BroadcasterStats {
            total_events_broadcast: self.total_broadcast.load(Ordering::Relaxed),
            active_subscribers: self.sender.receiver_count(),
            node_channels: self.node_channel_count.load(Ordering::Relaxed),
            events_in_buffer: self.recent_events.lock().len(),
            dropped_events: self.dropped_events.load(Ordering::Relaxed),
            undelivered_events: self.undelivered_events.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_event(_node_id: &str) -> Arc<Event> {
        Arc::new(Event::BestBlockChanged {
            timestamp: 1_000_000,
            slot: 42,
            hash: [0xAA; 32],
        })
    }

    fn make_test_json(event: &Event) -> Arc<[u8]> {
        Arc::from(serde_json::to_vec(event).unwrap())
    }

    fn node_id(s: &str) -> Arc<str> {
        Arc::from(s)
    }

    /// Helper: call broadcast_event with a local node_channels HashMap (no aggregator needed).
    fn broadcast_direct(
        broadcaster: &EventBroadcaster,
        nid: &str,
        event: Arc<Event>,
        json: &[u8],
        node_channels: &mut HashMap<String, broadcast::Sender<Arc<BroadcastEvent>>>,
    ) {
        let record = BroadcastRecord {
            id: broadcaster.next_event_id(),
            node_id: node_id(nid),
            event_type: event.event_type() as u8,
            timestamp: chrono::Utc::now(),
            ws_json: None,
            event,
            event_json: Arc::from(json),
        };
        broadcaster.broadcast_event(record, node_channels);
    }

    /// Helper: start aggregator and send events via MPSC (for subscribe tests).
    async fn send_via_aggregator(broadcaster: &Arc<EventBroadcaster>, nid: &str) {
        let event = make_test_event(nid);
        let json: Arc<[u8]> = Arc::from(serde_json::to_vec(&*event).unwrap());
        broadcaster.send_event(Arc::from(nid), event, json);
        // Give aggregator time to process
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    #[tokio::test]
    async fn test_broadcaster_scale() {
        let broadcaster = Arc::new(EventBroadcaster::new());
        broadcaster.start_aggregator();

        // Create channels by sending events for 1024 nodes via aggregator
        for i in 0..1024 {
            let nid = format!("node_{}", i);
            let event = make_test_event(&nid);
            let json: Arc<[u8]> = Arc::from(serde_json::to_vec(&*event).unwrap());
            broadcaster.send_event(Arc::from(nid.as_str()), event, json);
        }
        // Give aggregator time to process all events
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Subscribe to all nodes via command
        let subs = broadcaster.subscribe_all_nodes().await;
        assert_eq!(subs.len(), 1024);

        let stats = broadcaster.get_stats();
        assert_eq!(stats.node_channels, 1024);
    }

    #[tokio::test]
    async fn test_broadcast_and_receive() {
        let broadcaster = EventBroadcaster::new();
        let mut rx = broadcaster.subscribe_all();
        let mut nc = HashMap::new();

        let event = make_test_event("node_1");
        let event_json = make_test_json(&event);
        broadcast_direct(&broadcaster, "node_1", event, &event_json, &mut nc);

        let received = rx.recv().await.unwrap();
        assert_eq!(received.id, 0);
        assert_eq!(&*received.node_id, "node_1");
    }

    #[tokio::test]
    async fn test_subscribe_all_multiple_subscribers() {
        let broadcaster = EventBroadcaster::new();
        let mut rx1 = broadcaster.subscribe_all();
        let mut rx2 = broadcaster.subscribe_all();
        let mut nc = HashMap::new();

        let event = make_test_event("node_1");
        let event_json = make_test_json(&event);
        broadcast_direct(&broadcaster, "node_1", event, &event_json, &mut nc);

        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        assert_eq!(e1.id, e2.id);
    }

    #[tokio::test]
    async fn test_subscribe_node_filters() {
        let broadcaster = Arc::new(EventBroadcaster::new());
        broadcaster.start_aggregator();

        // Send event to create node_1 channel
        send_via_aggregator(&broadcaster, "node_1").await;

        // Subscribe to node_1 via command
        let mut rx_node1 = broadcaster
            .subscribe_node("node_1")
            .await
            .expect("channel should exist");

        // Send more events
        send_via_aggregator(&broadcaster, "node_1").await;
        send_via_aggregator(&broadcaster, "node_2").await;

        let received = rx_node1.recv().await.unwrap();
        assert_eq!(&*received.node_id, "node_1");

        // Trying to receive again should timeout (no more messages for node_1)
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), rx_node1.recv()).await;
        assert!(result.is_err(), "Should timeout — no more node_1 events");
    }

    #[tokio::test]
    async fn test_subscribe_nodes_multi() {
        let broadcaster = Arc::new(EventBroadcaster::new());
        broadcaster.start_aggregator();

        // Create channels via aggregator
        for nid in &["node_a", "node_b", "node_c"] {
            send_via_aggregator(&broadcaster, nid).await;
        }

        // Subscribe to node_a and node_b
        let subs = broadcaster
            .subscribe_nodes(&["node_a".to_string(), "node_b".to_string()])
            .await;
        assert_eq!(subs.len(), 2);

        // Subscribe to non-existent node — should be skipped
        let subs2 = broadcaster
            .subscribe_nodes(&["node_a".to_string(), "nonexistent".to_string()])
            .await;
        assert_eq!(subs2.len(), 1);

        // subscribe_all_nodes should return all 3
        let all = broadcaster.subscribe_all_nodes().await;
        assert_eq!(all.len(), 3);
    }

    #[tokio::test]
    async fn test_recent_events() {
        let broadcaster = EventBroadcaster::new();
        let mut nc = HashMap::new();

        // Broadcast 5 events
        for i in 0..5 {
            let nid = format!("node_{}", i);
            let event = make_test_event(&nid);
            let event_json = make_test_json(&event);
            broadcast_direct(&broadcaster, &nid, event, &event_json, &mut nc);
        }

        // Get last 3
        let recent = broadcaster.get_recent_events(Some(3));
        assert_eq!(recent.len(), 3);
        assert_eq!(recent[0].id, 2);
        assert_eq!(recent[2].id, 4);

        // Get all
        let all = broadcaster.get_recent_events(None);
        assert_eq!(all.len(), 5);
    }

    #[tokio::test]
    async fn test_recent_events_by_node() {
        let broadcaster = EventBroadcaster::new();
        let mut nc = HashMap::new();

        for _ in 0..3 {
            let event = make_test_event("node_a");
            let event_json = make_test_json(&event);
            broadcast_direct(&broadcaster, "node_a", event, &event_json, &mut nc);
        }
        for _ in 0..2 {
            let event = make_test_event("node_b");
            let event_json = make_test_json(&event);
            broadcast_direct(&broadcaster, "node_b", event, &event_json, &mut nc);
        }

        let node_a_events = broadcaster.get_recent_events_by_node("node_a", 10);
        assert_eq!(node_a_events.len(), 3);
        assert!(node_a_events.iter().all(|e| &*e.node_id == "node_a"));

        let node_b_events = broadcaster.get_recent_events_by_node("node_b", 10);
        assert_eq!(node_b_events.len(), 2);
    }

    #[tokio::test]
    async fn test_recent_events_ring_buffer() {
        let broadcaster = EventBroadcaster::new();
        let mut nc = HashMap::new();

        for _ in 0..(MAX_RETAINED_EVENTS + 100) {
            let event = make_test_event("node_1");
            let event_json = make_test_json(&event);
            broadcast_direct(&broadcaster, "node_1", event, &event_json, &mut nc);
        }

        let recent = broadcaster.get_recent_events(None);
        assert_eq!(recent.len(), MAX_RETAINED_EVENTS);
        assert_eq!(recent[0].id, 100);
    }

    #[tokio::test]
    async fn test_get_stats() {
        let broadcaster = EventBroadcaster::new();
        let _rx = broadcaster.subscribe_all();
        let mut nc = HashMap::new();

        let event = make_test_event("node_1");
        let event_json = make_test_json(&event);
        broadcast_direct(&broadcaster, "node_1", event, &event_json, &mut nc);

        let stats = broadcaster.get_stats();
        assert_eq!(stats.total_events_broadcast, 1);
        assert_eq!(stats.active_subscribers, 1);
        assert_eq!(stats.events_in_buffer, 1);
    }

    #[tokio::test]
    async fn test_raw_value_ws_broadcast_matches_original() {
        let event = Event::BestBlockChanged {
            timestamp: 1_000_000,
            slot: 42,
            hash: [0xAA; 32],
        };
        let event_json = serde_json::to_vec(&event).unwrap();
        let now = chrono::Utc::now();

        let original = WsBroadcast {
            r#type: "event",
            data: WsBroadcastData {
                id: 1,
                node_id: "node_1",
                event_type: 0,
                event: &event,
            },
            timestamp: now,
        };
        let original_json = serde_json::to_string(&original).unwrap();

        let raw_str = std::str::from_utf8(&event_json).unwrap();
        let raw_value = serde_json::value::RawValue::from_string(raw_str.to_string()).unwrap();
        let raw = WsBroadcastRaw {
            r#type: "event",
            data: WsBroadcastDataRaw {
                id: 1,
                node_id: "node_1",
                event_type: 0,
                event: &raw_value,
            },
            timestamp: now,
        };
        let raw_json = serde_json::to_string(&raw).unwrap();

        assert_eq!(original_json, raw_json);
    }

    #[tokio::test]
    async fn test_broadcast_with_preserialized_json() {
        let broadcaster = EventBroadcaster::new();
        let _rx = broadcaster.subscribe_all(); // need a subscriber so event_json is stored
        let mut nc = HashMap::new();
        let mut ws_buf = Vec::new();

        let event = make_test_event("node_1");
        let event_json = make_test_json(&event);
        let id = broadcaster.next_event_id();
        let event_type = event.event_type() as u8;
        let timestamp = chrono::Utc::now();
        let ws_json = build_ws_envelope(
            id,
            "node_1",
            event_type,
            &event_json,
            timestamp,
            &mut ws_buf,
        );

        let record = BroadcastRecord {
            id,
            node_id: node_id("node_1"),
            event_type,
            timestamp,
            ws_json,
            event,
            event_json: Arc::from(&*event_json),
        };
        broadcaster.broadcast_event(record, &mut nc);

        let recent = broadcaster.get_recent_events(Some(1));
        assert_eq!(recent.len(), 1);
        let be = &recent[0];

        // serialized_json should be present (built by build_ws_envelope)
        assert!(be.serialized_json.is_some());
        let parsed: serde_json::Value =
            serde_json::from_str(be.serialized_json.as_ref().unwrap()).unwrap();
        assert_eq!(parsed["type"], "event");
        assert_eq!(parsed["data"]["node_id"], "node_1");

        // event_json should be stored (we had a subscriber)
        assert!(be.event_json.is_some());
        assert_eq!(&**be.event_json.as_ref().unwrap(), &*event_json);
    }
}
