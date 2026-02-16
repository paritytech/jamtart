use crate::events::Event;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

/// Size of the main broadcast channel
/// 500K provides ~5 seconds of buffer at peak throughput (100K events/sec)
const BROADCAST_CHANNEL_SIZE: usize = 500_000;

/// Size of per-node channels (smaller since filtered)
const NODE_CHANNEL_SIZE: usize = 10_000;

/// Maximum number of events to retain in memory for instant replay
const MAX_RETAINED_EVENTS: usize = 10_000;

/// Maximum number of node-specific channels to maintain
const MAX_NODE_CHANNELS: usize = 2048;

/// How often to clean up inactive channels (seconds)
const CHANNEL_CLEANUP_INTERVAL: u64 = 30;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BroadcastEvent {
    pub id: u64,
    pub node_id: String,
    pub event: Event,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub event_type: u8,
    /// Pre-serialized JSON for WebSocket delivery (serialize once, send to all subscribers)
    #[serde(skip)]
    pub serialized_json: Option<Arc<str>>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum EventFilter {
    All,
    Node(String),
    EventType(u8),
    EventTypeRange(u8, u8),
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

/// High-performance event broadcaster designed for 1024+ nodes.
///
/// Uses `parking_lot::RwLock` instead of `tokio::sync::RwLock` for the recent_events
/// ring buffer and node_channels map because the critical sections contain no await
/// points. This avoids async lock overhead on the hottest path.
pub struct EventBroadcaster {
    /// Main broadcast channel for all events
    sender: broadcast::Sender<Arc<BroadcastEvent>>,

    /// Per-node broadcast channels for filtered subscriptions
    node_channels: Arc<RwLock<HashMap<String, broadcast::Sender<Arc<BroadcastEvent>>>>>,

    /// Ring buffer of recent events for new connections
    /// Uses parking_lot::RwLock for fast sync access (no await points in critical section)
    recent_events: Arc<RwLock<VecDeque<Arc<BroadcastEvent>>>>,

    /// Event counter for unique IDs
    event_counter: Arc<AtomicU64>,

    /// Statistics
    total_broadcast: Arc<AtomicU64>,
    dropped_events: Arc<AtomicU64>,
    undelivered_events: Arc<AtomicU64>,
}

impl Default for EventBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBroadcaster {
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(BROADCAST_CHANNEL_SIZE);

        let broadcaster = Self {
            sender,
            node_channels: Arc::new(RwLock::new(HashMap::with_capacity(MAX_NODE_CHANNELS))),
            recent_events: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_RETAINED_EVENTS))),
            event_counter: Arc::new(AtomicU64::new(0)),
            total_broadcast: Arc::new(AtomicU64::new(0)),
            dropped_events: Arc::new(AtomicU64::new(0)), // Note: Real drops happen inside receivers
            undelivered_events: Arc::new(AtomicU64::new(0)),
        };

        // Start cleanup task for inactive channels
        broadcaster.start_cleanup_task();

        broadcaster
    }

    /// Broadcast an event to all subscribers
    /// Optimized for high throughput with minimal latency.
    /// Accepts `Arc<Event>` to avoid cloning in the caller's hot path.
    pub fn broadcast_event(
        &self,
        node_id: &str,
        event: Arc<Event>,
    ) -> Result<u64, broadcast::error::SendError<Arc<BroadcastEvent>>> {
        // Generate unique event ID
        let id = self.event_counter.fetch_add(1, Ordering::Relaxed);

        let event_type = event.event_type() as u8;
        let mut broadcast_event = BroadcastEvent {
            id,
            node_id: node_id.to_string(),
            event_type,
            timestamp: chrono::Utc::now(),
            event: (*event).clone(),
            serialized_json: None,
        };

        // Pre-serialize in WebSocketResponse format so the WS handler can send it directly.
        // Must match the {"type": "event", "data": {...}, "timestamp": "..."} envelope
        // that clients (frontend) expect.
        if self.sender.receiver_count() > 0 {
            let ws_response = serde_json::json!({
                "type": "event",
                "data": {
                    "id": broadcast_event.id,
                    "node_id": &broadcast_event.node_id,
                    "event_type": broadcast_event.event_type,
                    "event": &broadcast_event.event,
                },
                "timestamp": broadcast_event.timestamp,
            });
            if let Ok(json) = serde_json::to_string(&ws_response) {
                broadcast_event.serialized_json = Some(Arc::from(json.as_str()));
            }
        }

        let broadcast_event = Arc::new(broadcast_event);

        // Broadcast to main channel (fast path)
        let receiver_count = self.sender.receiver_count();
        match self.sender.send(broadcast_event.clone()) {
            Ok(_) => {
                debug!("Broadcast event {} to {} receivers", id, receiver_count);
                self.total_broadcast.fetch_add(1, Ordering::Relaxed);
                if receiver_count == 0 {
                    // No receivers currently listening, but event is still in buffer
                    self.undelivered_events.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => {
                // In tokio broadcast, send() only fails if there are no receivers
                // (not even lagged ones). This is different from "lagged receivers"
                // which handle lag internally by dropping old messages
                self.total_broadcast.fetch_add(1, Ordering::Relaxed);
                if receiver_count == 0 {
                    self.undelivered_events.fetch_add(1, Ordering::Relaxed);
                } else {
                    // This shouldn't happen with active receivers
                    warn!(
                        "Unexpected broadcast error with {} active receivers",
                        receiver_count
                    );
                }
            }
        }

        // Broadcast to node-specific channel if exists (sync read lock - fast path)
        {
            let node_channels = self.node_channels.read();
            if let Some(sender) = node_channels.get(node_id) {
                let _ = sender.send(broadcast_event.clone());
            }
        }

        // Add to recent events ring buffer (O(1) operations with VecDeque)
        {
            let mut recent = self.recent_events.write();
            if recent.len() >= MAX_RETAINED_EVENTS {
                // This is normal ring buffer rotation, not data loss
                // The event was already broadcast to all subscribers
                recent.pop_front(); // O(1) with VecDeque instead of O(n) with Vec
            }
            recent.push_back(broadcast_event); // O(1) append
        }

        Ok(id)
    }

    /// Subscribe to all events
    pub fn subscribe_all(&self) -> broadcast::Receiver<Arc<BroadcastEvent>> {
        self.sender.subscribe()
    }

    /// Subscribe to events from a specific node
    /// Creates a dedicated channel for this node if it doesn't exist
    pub fn subscribe_node(&self, node_id: &str) -> broadcast::Receiver<Arc<BroadcastEvent>> {
        // Check if channel exists (read lock - fast path)
        {
            let channels = self.node_channels.read();
            if let Some(sender) = channels.get(node_id) {
                return sender.subscribe();
            }
        }

        // Channel doesn't exist, need to create it (write lock)
        let mut channels = self.node_channels.write();

        // Check again in case another task created it
        if let Some(sender) = channels.get(node_id) {
            return sender.subscribe();
        }

        // Enforce maximum channel limit to prevent memory exhaustion
        if channels.len() >= MAX_NODE_CHANNELS {
            warn!(
                "Maximum node channels ({}) reached, using main channel",
                MAX_NODE_CHANNELS
            );
            return self.sender.subscribe();
        }

        // Create new channel for this node
        let (tx, rx) = broadcast::channel(NODE_CHANNEL_SIZE);
        channels.insert(node_id.to_string(), tx);

        info!("Created dedicated channel for node {}", node_id);
        rx
    }

    /// Subscribe with a custom filter
    /// For complex filters, subscribers should use the main channel and filter client-side
    pub fn subscribe_filtered(
        &self,
        filter: EventFilter,
    ) -> broadcast::Receiver<Arc<BroadcastEvent>> {
        match filter {
            EventFilter::All => self.subscribe_all(),
            EventFilter::Node(node_id) => self.subscribe_node(&node_id),
            // For other filters, use main channel and filter client-side
            _ => self.subscribe_all(),
        }
    }

    /// Get recent events for catch-up on new connections
    /// Returns up to `limit` most recent events
    pub fn get_recent_events(&self, limit: Option<usize>) -> Vec<Arc<BroadcastEvent>> {
        let recent = self.recent_events.read();
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
        let recent = self.recent_events.read();
        // Single pass: collect from reverse iterator, then reverse the result
        let mut result = Vec::with_capacity(limit.min(recent.len()));
        for event in recent.iter().rev() {
            if event.node_id == node_id {
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
            node_channels: self.node_channels.read().len(),
            events_in_buffer: self.recent_events.read().len(),
            dropped_events: self.dropped_events.load(Ordering::Relaxed),
            undelivered_events: self.undelivered_events.load(Ordering::Relaxed),
        }
    }

    /// Start background task to clean up inactive node channels
    fn start_cleanup_task(&self) {
        let node_channels = self.node_channels.clone();

        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_secs(CHANNEL_CLEANUP_INTERVAL));

            loop {
                interval.tick().await;

                let mut channels = node_channels.write();
                let before = channels.len();

                // Remove channels with no receivers
                channels.retain(|node_id, sender| {
                    let receiver_count = sender.receiver_count();
                    if receiver_count == 0 {
                        debug!("Removing inactive channel for node {}", node_id);
                        false
                    } else {
                        true
                    }
                });

                let removed = before - channels.len();
                if removed > 0 {
                    info!("Cleaned up {} inactive node channels", removed);
                }
            }
        });
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

    #[tokio::test]
    async fn test_broadcaster_scale() {
        let broadcaster = EventBroadcaster::new();

        // Simulate 1024 nodes
        let mut receivers = Vec::new();
        for i in 0..1024 {
            let rx = broadcaster.subscribe_node(&format!("node_{}", i));
            receivers.push(rx);
        }

        // Verify we can handle the load
        assert!(receivers.len() == 1024);

        let stats = broadcaster.get_stats();
        assert!(stats.node_channels <= MAX_NODE_CHANNELS);
    }

    #[tokio::test]
    async fn test_broadcast_and_receive() {
        let broadcaster = EventBroadcaster::new();
        let mut rx = broadcaster.subscribe_all();

        let event = make_test_event("node_1");
        let id = broadcaster.broadcast_event("node_1", event).unwrap();
        assert_eq!(id, 0);

        let received = rx.recv().await.unwrap();
        assert_eq!(received.id, 0);
        assert_eq!(received.node_id, "node_1");
    }

    #[tokio::test]
    async fn test_subscribe_all_multiple_subscribers() {
        let broadcaster = EventBroadcaster::new();
        let mut rx1 = broadcaster.subscribe_all();
        let mut rx2 = broadcaster.subscribe_all();

        let event = make_test_event("node_1");
        broadcaster.broadcast_event("node_1", event).unwrap();

        let e1 = rx1.recv().await.unwrap();
        let e2 = rx2.recv().await.unwrap();
        assert_eq!(e1.id, e2.id);
    }

    #[tokio::test]
    async fn test_subscribe_node_filters() {
        let broadcaster = EventBroadcaster::new();
        let mut rx_node1 = broadcaster.subscribe_node("node_1");

        // Broadcast to node_1 — should be received
        let event1 = make_test_event("node_1");
        broadcaster.broadcast_event("node_1", event1).unwrap();

        // Broadcast to node_2 — should NOT be received on node_1 channel
        let event2 = make_test_event("node_2");
        broadcaster.broadcast_event("node_2", event2).unwrap();

        let received = rx_node1.recv().await.unwrap();
        assert_eq!(received.node_id, "node_1");

        // Trying to receive again should timeout (no more messages for node_1)
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), rx_node1.recv()).await;
        assert!(result.is_err(), "Should timeout — no more node_1 events");
    }

    #[tokio::test]
    async fn test_subscribe_filtered_all() {
        let broadcaster = EventBroadcaster::new();
        let mut rx = broadcaster.subscribe_filtered(EventFilter::All);

        let event = make_test_event("node_1");
        broadcaster.broadcast_event("node_1", event).unwrap();

        let received = rx.recv().await.unwrap();
        assert_eq!(received.node_id, "node_1");
    }

    #[tokio::test]
    async fn test_recent_events() {
        let broadcaster = EventBroadcaster::new();

        // Broadcast 5 events
        for i in 0..5 {
            let event = make_test_event(&format!("node_{}", i));
            broadcaster
                .broadcast_event(&format!("node_{}", i), event)
                .unwrap();
        }

        // Get last 3
        let recent = broadcaster.get_recent_events(Some(3));
        assert_eq!(recent.len(), 3);
        // Should be the last 3 (ids 2, 3, 4)
        assert_eq!(recent[0].id, 2);
        assert_eq!(recent[2].id, 4);

        // Get all
        let all = broadcaster.get_recent_events(None);
        assert_eq!(all.len(), 5);
    }

    #[tokio::test]
    async fn test_recent_events_by_node() {
        let broadcaster = EventBroadcaster::new();

        // Broadcast from multiple nodes
        for _ in 0..3 {
            let event = make_test_event("node_a");
            broadcaster.broadcast_event("node_a", event).unwrap();
        }
        for _ in 0..2 {
            let event = make_test_event("node_b");
            broadcaster.broadcast_event("node_b", event).unwrap();
        }

        let node_a_events = broadcaster.get_recent_events_by_node("node_a", 10);
        assert_eq!(node_a_events.len(), 3);
        assert!(node_a_events.iter().all(|e| e.node_id == "node_a"));

        let node_b_events = broadcaster.get_recent_events_by_node("node_b", 10);
        assert_eq!(node_b_events.len(), 2);
    }

    #[tokio::test]
    async fn test_recent_events_ring_buffer() {
        let broadcaster = EventBroadcaster::new();

        // Broadcast more than MAX_RETAINED_EVENTS
        for _ in 0..(MAX_RETAINED_EVENTS + 100) {
            let event = make_test_event("node_1");
            broadcaster.broadcast_event("node_1", event).unwrap();
        }

        let recent = broadcaster.get_recent_events(None);
        assert_eq!(recent.len(), MAX_RETAINED_EVENTS);
        // First event in buffer should be #100 (oldest were evicted)
        assert_eq!(recent[0].id, 100);
    }

    #[tokio::test]
    async fn test_get_stats() {
        let broadcaster = EventBroadcaster::new();
        let _rx = broadcaster.subscribe_all();

        let event = make_test_event("node_1");
        broadcaster.broadcast_event("node_1", event).unwrap();

        let stats = broadcaster.get_stats();
        assert_eq!(stats.total_events_broadcast, 1);
        assert_eq!(stats.active_subscribers, 1);
        assert_eq!(stats.events_in_buffer, 1);
    }
}
