use crate::events::Event;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Size of the main broadcast channel
/// Must be large enough to handle bursts from 1024 nodes
const BROADCAST_CHANNEL_SIZE: usize = 100_000;

/// Size of per-node channels (smaller since filtered)
const NODE_CHANNEL_SIZE: usize = 10_000;

/// Maximum number of events to retain in memory for instant replay
const MAX_RETAINED_EVENTS: usize = 10_000;

/// Maximum number of node-specific channels to maintain
const MAX_NODE_CHANNELS: usize = 2048;

/// How often to clean up inactive channels (seconds)
const CHANNEL_CLEANUP_INTERVAL: u64 = 300;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BroadcastEvent {
    pub id: u64,
    pub node_id: String,
    pub event: Event,
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub event_type: u8,
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

/// High-performance event broadcaster designed for 1024+ nodes
pub struct EventBroadcaster {
    /// Main broadcast channel for all events
    sender: broadcast::Sender<Arc<BroadcastEvent>>,

    /// Per-node broadcast channels for filtered subscriptions
    /// Using RwLock for better read performance with many subscribers
    node_channels: Arc<RwLock<HashMap<String, broadcast::Sender<Arc<BroadcastEvent>>>>>,

    /// Ring buffer of recent events for new connections
    /// Arc<RwLock> for concurrent reads, sequential writes
    /// Using VecDeque for O(1) push_back/pop_front operations
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
    /// Optimized for high throughput with minimal latency
    pub async fn broadcast_event(
        &self,
        node_id: String,
        event: Event,
    ) -> Result<u64, broadcast::error::SendError<Arc<BroadcastEvent>>> {
        // Generate unique event ID
        let id = self.event_counter.fetch_add(1, Ordering::Relaxed);

        let broadcast_event = Arc::new(BroadcastEvent {
            id,
            node_id: node_id.clone(),
            event_type: event.event_type() as u8,
            timestamp: chrono::Utc::now(),
            event,
        });

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

        // Broadcast to node-specific channel if exists (optimized for read-heavy workload)
        {
            let node_channels = self.node_channels.read().await;
            if let Some(sender) = node_channels.get(&node_id) {
                let _ = sender.send(broadcast_event.clone());
            }
        }

        // Add to recent events ring buffer (O(1) operations with VecDeque)
        {
            let mut recent = self.recent_events.write().await;
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
    pub async fn subscribe_node(&self, node_id: &str) -> broadcast::Receiver<Arc<BroadcastEvent>> {
        // Check if channel exists (read lock - fast path)
        {
            let channels = self.node_channels.read().await;
            if let Some(sender) = channels.get(node_id) {
                return sender.subscribe();
            }
        }

        // Channel doesn't exist, need to create it (write lock)
        let mut channels = self.node_channels.write().await;

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
    pub async fn subscribe_filtered(
        &self,
        filter: EventFilter,
    ) -> broadcast::Receiver<Arc<BroadcastEvent>> {
        match filter {
            EventFilter::All => self.subscribe_all(),
            EventFilter::Node(node_id) => self.subscribe_node(&node_id).await,
            // For other filters, use main channel and filter client-side
            _ => self.subscribe_all(),
        }
    }

    /// Get recent events for catch-up on new connections
    /// Returns up to `limit` most recent events
    pub async fn get_recent_events(&self, limit: Option<usize>) -> Vec<Arc<BroadcastEvent>> {
        let recent = self.recent_events.read().await;
        let limit = limit.unwrap_or(recent.len()).min(recent.len());

        if limit == 0 {
            return Vec::new();
        }

        // Return most recent events (VecDeque iterator is efficient)
        let skip = recent.len().saturating_sub(limit);
        recent.iter().skip(skip).cloned().collect()
    }

    /// Get recent events filtered by node
    pub async fn get_recent_events_by_node(
        &self,
        node_id: &str,
        limit: usize,
    ) -> Vec<Arc<BroadcastEvent>> {
        let recent = self.recent_events.read().await;
        recent
            .iter()
            .rev()
            .filter(|e| e.node_id == node_id)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// Get broadcaster statistics
    pub async fn get_stats(&self) -> BroadcasterStats {
        BroadcasterStats {
            total_events_broadcast: self.total_broadcast.load(Ordering::Relaxed),
            active_subscribers: self.sender.receiver_count(),
            node_channels: self.node_channels.read().await.len(),
            events_in_buffer: self.recent_events.read().await.len(),
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

                let mut channels = node_channels.write().await;
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

    #[tokio::test]
    async fn test_broadcaster_scale() {
        let broadcaster = EventBroadcaster::new();

        // Simulate 1024 nodes
        let mut receivers = Vec::new();
        for i in 0..1024 {
            let rx = broadcaster.subscribe_node(&format!("node_{}", i)).await;
            receivers.push(rx);
        }

        // Verify we can handle the load
        assert!(receivers.len() == 1024);

        let stats = broadcaster.get_stats().await;
        assert!(stats.node_channels <= MAX_NODE_CHANNELS);
    }
}
