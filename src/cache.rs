use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Maximum number of cache entries to prevent unbounded growth
const MAX_CACHE_ENTRIES: usize = 10_000;

/// Simple in-memory TTL cache for expensive query results.
///
/// Uses `parking_lot::RwLock` for fast concurrent reads with infrequent writes.
/// Cache entries expire after the configured TTL and are lazily evicted.
/// Values are stored behind `Arc` to avoid deep JSON cloning on reads.
pub struct TtlCache {
    data: RwLock<HashMap<String, CacheEntry>>,
    /// Per-key notify for stampede prevention: concurrent requests for the same
    /// missing key wait on a Notify instead of all hitting the database.
    inflight: RwLock<HashMap<String, Arc<Notify>>>,
    ttl: Duration,
}

struct CacheEntry {
    value: Arc<serde_json::Value>,
    inserted_at: Instant,
}

impl TtlCache {
    /// Create a new cache with the given TTL for entries.
    pub fn new(ttl: Duration) -> Self {
        Self {
            data: RwLock::new(HashMap::with_capacity(16)),
            inflight: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Get a cached value if it exists and hasn't expired.
    /// Returns a cheap Arc clone instead of deep-cloning the JSON.
    pub fn get(&self, key: &str) -> Option<Arc<serde_json::Value>> {
        let data = self.data.read();
        if let Some(entry) = data.get(key) {
            if entry.inserted_at.elapsed() < self.ttl {
                return Some(Arc::clone(&entry.value));
            }
        }
        None
    }

    /// Insert a value into the cache, wrapping it in Arc.
    pub fn insert(&self, key: String, value: serde_json::Value) {
        self.insert_arc(key, Arc::new(value));
    }

    /// Insert a pre-wrapped Arc value into the cache.
    pub fn insert_arc(&self, key: String, value: Arc<serde_json::Value>) {
        let mut data = self.data.write();
        // Enforce max entry count to prevent unbounded growth
        if data.len() >= MAX_CACHE_ENTRIES && !data.contains_key(&key) {
            // Evict expired entries first
            let ttl = self.ttl;
            data.retain(|_, entry| entry.inserted_at.elapsed() < ttl);
            // If still at limit, evict oldest entry
            if data.len() >= MAX_CACHE_ENTRIES {
                if let Some(oldest_key) = data
                    .iter()
                    .min_by_key(|(_, entry)| entry.inserted_at)
                    .map(|(k, _)| k.clone())
                {
                    data.remove(&oldest_key);
                }
            }
        }
        data.insert(
            key.clone(),
            CacheEntry {
                value,
                inserted_at: Instant::now(),
            },
        );
        // Wake any waiters for this key (stampede prevention)
        let inflight = self.inflight.read();
        if let Some(notify) = inflight.get(&key) {
            notify.notify_waiters();
        }
    }

    /// Delete a specific cache entry (for invalidation).
    pub fn delete(&self, key: &str) {
        let mut data = self.data.write();
        data.remove(key);
    }

    /// Get the number of entries in the cache.
    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    /// Check if cache is empty.
    pub fn is_empty(&self) -> bool {
        self.data.read().is_empty()
    }

    /// Register interest in a key for stampede prevention.
    /// Returns Some(Notify) if another request is already computing this key,
    /// meaning the caller should wait. Returns None if the caller should compute.
    pub fn register_inflight(&self, key: &str) -> Option<Arc<Notify>> {
        // Check if already inflight
        {
            let inflight = self.inflight.read();
            if let Some(notify) = inflight.get(key) {
                return Some(Arc::clone(notify));
            }
        }
        // Register ourselves as the inflight request
        let mut inflight = self.inflight.write();
        // Double-check after acquiring write lock
        if let Some(notify) = inflight.get(key) {
            return Some(Arc::clone(notify));
        }
        let notify = Arc::new(Notify::new());
        inflight.insert(key.to_string(), notify);
        None
    }

    /// Remove inflight registration after computation completes.
    pub fn clear_inflight(&self, key: &str) {
        let mut inflight = self.inflight.write();
        inflight.remove(key);
    }

    /// Remove expired entries. Called periodically to prevent unbounded growth.
    pub fn evict_expired(&self) {
        let mut data = self.data.write();
        let ttl = self.ttl;
        data.retain(|_, entry| entry.inserted_at.elapsed() < ttl);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_insert_and_get() {
        let cache = TtlCache::new(Duration::from_secs(10));
        cache.insert("key1".to_string(), serde_json::json!({"hello": "world"}));
        let val = cache.get("key1").unwrap();
        assert_eq!(*val, serde_json::json!({"hello": "world"}));
    }

    #[test]
    fn test_get_missing_key() {
        let cache = TtlCache::new(Duration::from_secs(10));
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_get_expired() {
        let cache = TtlCache::new(Duration::from_millis(50));
        cache.insert("key1".to_string(), serde_json::json!(1));
        assert!(cache.get("key1").is_some());
        thread::sleep(Duration::from_millis(60));
        assert!(cache.get("key1").is_none());
    }

    #[test]
    fn test_delete() {
        let cache = TtlCache::new(Duration::from_secs(10));
        cache.insert("key1".to_string(), serde_json::json!(1));
        assert!(cache.get("key1").is_some());
        cache.delete("key1");
        assert!(cache.get("key1").is_none());
    }

    #[test]
    fn test_evict_expired() {
        let cache = TtlCache::new(Duration::from_millis(50));
        cache.insert("a".to_string(), serde_json::json!(1));
        cache.insert("b".to_string(), serde_json::json!(2));
        assert_eq!(cache.len(), 2);
        thread::sleep(Duration::from_millis(60));
        cache.evict_expired();
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn test_len_and_is_empty() {
        let cache = TtlCache::new(Duration::from_secs(10));
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        cache.insert("a".to_string(), serde_json::json!(1));
        assert!(!cache.is_empty());
        assert_eq!(cache.len(), 1);
        cache.insert("b".to_string(), serde_json::json!(2));
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_insert_arc() {
        let cache = TtlCache::new(Duration::from_secs(10));
        let value = Arc::new(serde_json::json!({"pre": "wrapped"}));
        cache.insert_arc("key1".to_string(), value.clone());
        let got = cache.get("key1").unwrap();
        assert_eq!(*got, serde_json::json!({"pre": "wrapped"}));
    }

    #[test]
    fn test_overwrite_existing_key() {
        let cache = TtlCache::new(Duration::from_secs(10));
        cache.insert("key1".to_string(), serde_json::json!(1));
        cache.insert("key1".to_string(), serde_json::json!(2));
        assert_eq!(*cache.get("key1").unwrap(), serde_json::json!(2));
        assert_eq!(cache.len(), 1);
    }

    #[tokio::test]
    async fn test_stampede_register_first_caller() {
        let cache = TtlCache::new(Duration::from_secs(10));
        // First caller should get None (they compute)
        let result = cache.register_inflight("key1");
        assert!(result.is_none());
        cache.clear_inflight("key1");
    }

    #[tokio::test]
    async fn test_stampede_second_caller_gets_notify() {
        let cache = TtlCache::new(Duration::from_secs(10));
        // First caller registers
        let first = cache.register_inflight("key1");
        assert!(first.is_none());
        // Second caller gets a Notify to wait on
        let second = cache.register_inflight("key1");
        assert!(second.is_some());
        cache.clear_inflight("key1");
    }

    #[tokio::test]
    async fn test_stampede_notify_on_insert() {
        let cache = Arc::new(TtlCache::new(Duration::from_secs(10)));
        // First caller registers inflight
        let first = cache.register_inflight("key1");
        assert!(first.is_none());

        // Second caller gets Notify
        let notify = cache.register_inflight("key1").unwrap();

        // Spawn a task that waits on the notify
        let cache2 = Arc::clone(&cache);
        let handle = tokio::spawn(async move {
            notify.notified().await;
            // After notification, value should be in cache
            cache2.get("key1").unwrap()
        });

        // Small delay, then insert (which wakes waiters)
        tokio::time::sleep(Duration::from_millis(10)).await;
        cache.insert("key1".to_string(), serde_json::json!(42));
        cache.clear_inflight("key1");

        let val = handle.await.unwrap();
        assert_eq!(*val, serde_json::json!(42));
    }
}
