-- Migration: 003_frontend_analytics_indexes
-- Purpose: Add indexes to optimize new frontend analytics queries
-- Based on frontend team data gap analysis

-- Index for block propagation analysis (BlockAnnounced events)
-- Supports queries correlating announcements with transfers by slot
CREATE INDEX IF NOT EXISTS idx_block_announced_slot
    ON events ((data->'BlockAnnounced'->>'slot'))
    WHERE event_type = 62;

-- Index for block transfer correlation
CREATE INDEX IF NOT EXISTS idx_block_transferred_slot
    ON events ((data->'BlockTransferred'->>'slot'))
    WHERE event_type = 68;

-- Index for failure event analysis
-- Optimizes failure rate queries by category
CREATE INDEX IF NOT EXISTS idx_failed_events
    ON events (event_type, node_id, timestamp)
    WHERE event_type IN (41, 44, 46, 81, 94, 97, 99, 102, 109, 110, 112, 113);

-- Index for guarantee analysis by core
-- Supports get_core_guarantors and validator-to-core mapping queries
CREATE INDEX IF NOT EXISTS idx_guarantee_built_core
    ON events ((CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER)), node_id)
    WHERE event_type = 105;

-- Index for shard operations (DA stats)
-- Optimizes read/write operation counting
CREATE INDEX IF NOT EXISTS idx_shard_operations
    ON events (event_type, node_id, created_at)
    WHERE event_type IN (121, 123, 124);

-- Index for work package events by hash (journey tracking)
-- Supports get_workpackage_journey queries
CREATE INDEX IF NOT EXISTS idx_wp_events_hash
    ON events ((COALESCE(
        data->'WorkPackageSubmitted'->>'hash',
        data->'Refined'->>'hash',
        data->'GuaranteeBuilt'->'outline'->>'hash',
        data->>'hash'
    )))
    WHERE event_type BETWEEN 90 AND 113;

-- Index for preimage events
CREATE INDEX IF NOT EXISTS idx_preimage_events
    ON events (event_type, node_id)
    WHERE event_type IN (190, 191, 192);

-- Index for ticket events
CREATE INDEX IF NOT EXISTS idx_ticket_events
    ON events (event_type, node_id, timestamp)
    WHERE event_type IN (80, 81, 82, 84);

-- Performance comments
COMMENT ON INDEX idx_block_announced_slot IS 'Optimized for block propagation analysis - announcement tracking';
COMMENT ON INDEX idx_block_transferred_slot IS 'Optimized for block propagation analysis - transfer tracking';
COMMENT ON INDEX idx_failed_events IS 'Optimized for failure rate analytics queries';
COMMENT ON INDEX idx_guarantee_built_core IS 'Optimized for core guarantor and validator mapping queries';
COMMENT ON INDEX idx_shard_operations IS 'Optimized for DA read/write operation counting';
COMMENT ON INDEX idx_wp_events_hash IS 'Optimized for work package journey tracking';
COMMENT ON INDEX idx_preimage_events IS 'Optimized for preimage pool queries';
COMMENT ON INDEX idx_ticket_events IS 'Optimized for ticket generation analysis';
