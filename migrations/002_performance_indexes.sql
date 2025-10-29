-- Additional performance indexes based on audit findings
-- Migration: 002_performance_indexes
-- Purpose: Add indexes to optimize common query patterns

-- Index for efficient node_id filtering in get_node_events queries
-- This supports the pattern: SELECT * FROM events WHERE node_id = $1 ORDER BY created_at DESC LIMIT N
-- Already have idx_events_node_timestamp_type, but this is more specific for this pattern
CREATE INDEX IF NOT EXISTS idx_events_node_created_at
    ON events(node_id, created_at DESC);

-- Index for efficient event type filtering with pagination
-- Supports queries like: SELECT * FROM events WHERE event_type IN (10, 11) ORDER BY timestamp DESC
CREATE INDEX IF NOT EXISTS idx_events_type_created_at
    ON events(event_type, created_at DESC);

-- Note: Partial index with CURRENT_TIMESTAMP removed due to PostgreSQL IMMUTABLE requirement
-- The above indexes provide sufficient performance for common queries

-- Covering index for node statistics queries
-- Speeds up queries that need node metadata with connection status
CREATE INDEX IF NOT EXISTS idx_nodes_connected_metadata
    ON nodes(is_connected, last_seen_at DESC, event_count)
    WHERE is_connected = true;

-- Index on peer_id for lookups by peer identifier
CREATE INDEX IF NOT EXISTS idx_nodes_peer_id
    ON nodes(peer_id);

-- Statistics: Add index for faster stats aggregation
-- Supports COUNT(*), SUM(event_count) queries with filters
CREATE INDEX IF NOT EXISTS idx_nodes_stats
    ON nodes(is_connected, created_at)
    INCLUDE (event_count, total_events);

-- Performance comments
COMMENT ON INDEX idx_events_node_created_at IS 'Optimized for per-node event queries with time-based ordering';
COMMENT ON INDEX idx_events_type_created_at IS 'Optimized for event type filtering with pagination';
COMMENT ON INDEX idx_nodes_stats IS 'Covering index for aggregate statistics queries';
