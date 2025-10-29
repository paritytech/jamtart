-- PostgreSQL optimized schema for TART Backend
-- Designed for high-throughput event ingestion from 1023+ nodes
-- with efficient analytics on rapid event streams

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS btree_gin;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Nodes table with connection tracking
CREATE TABLE nodes (
    node_id TEXT PRIMARY KEY,
    peer_id TEXT NOT NULL,
    implementation_name TEXT NOT NULL,
    implementation_version TEXT NOT NULL,
    node_info JSONB NOT NULL,
    connected_at TIMESTAMPTZ NOT NULL,
    disconnected_at TIMESTAMPTZ,
    last_seen_at TIMESTAMPTZ NOT NULL,
    is_connected BOOLEAN DEFAULT true,
    event_count BIGINT DEFAULT 0,
    total_events BIGINT DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Optimized indexes for nodes table
CREATE INDEX idx_nodes_connected ON nodes(is_connected, last_seen_at DESC) WHERE is_connected = true;
CREATE INDEX idx_nodes_last_seen ON nodes(last_seen_at DESC);

-- Events table with time-based partitioning
-- Parent table for partitioning by time (daily partitions for efficient archival)
CREATE TABLE events (
    id BIGSERIAL,
    node_id TEXT NOT NULL,
    event_id BIGINT NOT NULL,
    event_type INTEGER NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, created_at)
) PARTITION BY RANGE (created_at);

-- Create initial partitions (current day and next 7 days)
-- In production, use pg_partman or similar for automatic partition management
DO $$
DECLARE
    partition_date DATE;
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    FOR i IN 0..7 LOOP
        partition_date := CURRENT_DATE + (i || ' days')::INTERVAL;
        partition_name := 'events_' || TO_CHAR(partition_date, 'YYYY_MM_DD');
        start_date := partition_date;
        end_date := partition_date + INTERVAL '1 day';

        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );
    END LOOP;
END $$;

-- Indexes on events table (applied to all partitions)
CREATE INDEX idx_events_node_timestamp_type ON events(node_id, timestamp DESC, event_type);
CREATE INDEX idx_events_type_timestamp ON events(event_type, timestamp DESC);
CREATE INDEX idx_events_timestamp ON events(timestamp DESC);
CREATE INDEX idx_events_created_at ON events(created_at DESC);

-- GIN index for JSONB queries on event data
CREATE INDEX idx_events_data_gin ON events USING GIN (data);

-- Unique constraint for event deduplication (per node)
-- For partitioned tables, unique constraints must include the partition key (created_at)
-- This ensures idempotent inserts: same node_id + event_id + timestamp = same event
-- Different timestamps allow event_id reuse across connection sessions
CREATE UNIQUE INDEX idx_events_node_event_id ON events(node_id, event_id, created_at);

-- Foreign key constraint
ALTER TABLE events ADD CONSTRAINT fk_events_nodes FOREIGN KEY (node_id) REFERENCES nodes(node_id) ON DELETE CASCADE;

-- Specialized table for node status events (event_type = 10)
-- Optimized for fast queries on status metrics
CREATE TABLE node_status (
    id BIGSERIAL PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    validator_peers INTEGER NOT NULL,
    non_validator_peers INTEGER NOT NULL,
    block_announcement_peers INTEGER NOT NULL,
    num_shards INTEGER NOT NULL,
    total_shard_size BIGINT NOT NULL,
    announced_preimages INTEGER NOT NULL,
    preimages_in_pool INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_node_status_node_id ON node_status(node_id, timestamp DESC);
CREATE INDEX idx_node_status_timestamp ON node_status(timestamp DESC);

-- Specialized table for block events
CREATE TABLE blocks (
    id BIGSERIAL PRIMARY KEY,
    node_id TEXT NOT NULL REFERENCES nodes(node_id) ON DELETE CASCADE,
    timestamp TIMESTAMPTZ NOT NULL,
    slot BIGINT NOT NULL,
    header_hash TEXT NOT NULL,
    block_type TEXT NOT NULL CHECK (block_type IN ('best', 'finalized')),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_blocks_node_id ON blocks(node_id, timestamp DESC);
CREATE INDEX idx_blocks_slot ON blocks(slot DESC);
CREATE INDEX idx_blocks_timestamp ON blocks(timestamp DESC);
CREATE INDEX idx_blocks_type ON blocks(block_type, slot DESC);

-- Stats cache table for pre-computed aggregations
CREATE TABLE stats_cache (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_stats_cache_updated ON stats_cache(updated_at DESC);

-- Materialized view for real-time statistics (refreshed periodically)
CREATE MATERIALIZED VIEW event_stats_hourly AS
SELECT
    DATE_TRUNC('hour', created_at) AS hour,
    node_id,
    event_type,
    COUNT(*) AS event_count,
    MIN(timestamp) AS first_event,
    MAX(timestamp) AS last_event
FROM events
WHERE created_at >= NOW() - INTERVAL '7 days'
GROUP BY DATE_TRUNC('hour', created_at), node_id, event_type
WITH DATA;

CREATE INDEX idx_event_stats_hourly ON event_stats_hourly(hour DESC, node_id, event_type);

-- Function to refresh materialized view
CREATE OR REPLACE FUNCTION refresh_event_stats() RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY event_stats_hourly;
END;
$$ LANGUAGE plpgsql;

-- Function to create new partition for next day
CREATE OR REPLACE FUNCTION create_next_partition() RETURNS void AS $$
DECLARE
    partition_date DATE;
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    partition_date := CURRENT_DATE + INTERVAL '8 days';
    partition_name := 'events_' || TO_CHAR(partition_date, 'YYYY_MM_DD');
    start_date := partition_date;
    end_date := partition_date + INTERVAL '1 day';

    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I PARTITION OF events FOR VALUES FROM (%L) TO (%L)',
        partition_name, start_date, end_date
    );
END;
$$ LANGUAGE plpgsql;

-- Function to archive old partitions (older than 30 days)
CREATE OR REPLACE FUNCTION archive_old_partitions() RETURNS void AS $$
DECLARE
    partition_record RECORD;
    cutoff_date DATE;
BEGIN
    cutoff_date := CURRENT_DATE - INTERVAL '30 days';

    FOR partition_record IN
        SELECT tablename FROM pg_tables
        WHERE schemaname = 'public'
        AND tablename LIKE 'events_____\_\_\_\_\_\_\_\_'
        AND tablename < 'events_' || TO_CHAR(cutoff_date, 'YYYY_MM_DD')
    LOOP
        -- Detach partition (PostgreSQL 12+)
        EXECUTE format('ALTER TABLE events DETACH PARTITION %I', partition_record.tablename);
        -- Optionally: Move to archive schema or drop
        -- EXECUTE format('DROP TABLE %I', partition_record.tablename);
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Trigger to update last_seen_at on event insert
CREATE OR REPLACE FUNCTION update_node_last_seen() RETURNS TRIGGER AS $$
BEGIN
    UPDATE nodes
    SET last_seen_at = NEW.created_at,
        event_count = event_count + 1
    WHERE node_id = NEW.node_id;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger to automatically update node stats on event insert
-- This eliminates the need for separate UPDATE queries
CREATE TRIGGER trg_update_node_last_seen
    AFTER INSERT ON events
    FOR EACH ROW
    EXECUTE FUNCTION update_node_last_seen();

-- Performance tuning comments
COMMENT ON TABLE events IS 'Main event log partitioned by created_at for efficient archival. Use bulk INSERT for best performance.';
COMMENT ON INDEX idx_events_node_timestamp_type IS 'Composite index covering most common query patterns: filter by node, sort by time, filter by event type';
COMMENT ON INDEX idx_events_data_gin IS 'GIN index for fast JSONB queries on event payloads';
COMMENT ON MATERIALIZED VIEW event_stats_hourly IS 'Pre-computed hourly statistics. Refresh every 5-15 minutes for real-time dashboards.';