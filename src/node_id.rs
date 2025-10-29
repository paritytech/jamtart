use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Type-safe node identifier.
///
/// Wraps a hex-encoded peer ID with zero-cost abstraction and cheap cloning.
/// Uses Arc<str> internally for efficient sharing across async tasks.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(Arc<str>);

impl NodeId {
    /// Creates a NodeId from a 32-byte peer ID by hex-encoding it.
    pub fn from_peer_id(peer_id: &[u8; 32]) -> Self {
        Self(hex::encode(peer_id).into())
    }

    /// Creates a NodeId from a pre-validated hex string.
    ///
    /// # Panics
    /// Panics in debug mode if the string is not 64 hex characters.
    pub fn from_hex(hex_str: impl Into<Arc<str>>) -> Self {
        let arc_str = hex_str.into();
        debug_assert_eq!(
            arc_str.len(),
            64,
            "NodeId must be 64 hex characters (32 bytes encoded)"
        );
        debug_assert!(
            arc_str.chars().all(|c| c.is_ascii_hexdigit()),
            "NodeId must contain only hex characters"
        );
        Self(arc_str)
    }

    /// Returns the node ID as a string slice.
    ///
    /// Prefer this over `to_string()` to avoid allocations.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl AsRef<str> for NodeId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

impl From<String> for NodeId {
    fn from(s: String) -> Self {
        Self(s.into())
    }
}

impl From<&str> for NodeId {
    fn from(s: &str) -> Self {
        Self(s.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_from_peer_id() {
        let peer_id = [0u8; 32];
        let node_id = NodeId::from_peer_id(&peer_id);
        assert_eq!(node_id.as_str().len(), 64);
        assert!(node_id.as_str().chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_node_id_cheap_clone() {
        let node_id1 = NodeId::from_hex("0".repeat(64));
        let node_id2 = node_id1.clone();
        // Arc clones are cheap - just pointer + ref count increment
        assert_eq!(node_id1, node_id2);
    }

    #[test]
    fn test_node_id_display() {
        let node_id = NodeId::from_hex("a".repeat(64));
        let displayed = format!("{}", node_id);
        assert_eq!(displayed.len(), 64);
    }
}
