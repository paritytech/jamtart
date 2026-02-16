-- no transaction
-- TimescaleDB schema for TART Backend
-- Designed for high-throughput event ingestion: 3M events/sec from 1024+ nodes
-- Features: automatic chunking, continuous aggregates, compression, retention policies

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Nodes table (regular PostgreSQL table - low cardinality, ~1024 rows)
CREATE TABLE IF NOT EXISTS nodes (
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
    address TEXT,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_nodes_connected ON nodes(is_connected, last_seen_at DESC) WHERE is_connected = true;
CREATE INDEX IF NOT EXISTS idx_nodes_last_seen ON nodes(last_seen_at DESC);

-- Events hypertable (main time-series data)
--
-- Schema differences from the original PostgreSQL schema (v0.2.0):
--   id BIGSERIAL PRIMARY KEY  →  event_id BIGINT (no PK)
--     Hypertables don't support BIGSERIAL PKs; unique constraints require
--     the partition column. At 3M events/s dedup is too expensive anyway.
--   created_at TIMESTAMPTZ    →  received_at TIMESTAMPTZ
--     Clarifies semantics: this is server receive time, not event creation time.
--   timestamp BIGINT          →  time TIMESTAMPTZ
--     Native timestamp used as hypertable partition key (1-hour chunks).
--     The original stored unix millis which required casting in every query.
--   event_type INTEGER        →  event_type SMALLINT
--     130 event types fit in 2 bytes; saves ~2GB/day at full throughput.
--
CREATE TABLE IF NOT EXISTS events (
    time         TIMESTAMPTZ NOT NULL,
    node_id      TEXT        NOT NULL,
    event_id     BIGINT      NOT NULL,
    event_type   SMALLINT    NOT NULL,
    data         JSONB       NOT NULL,
    received_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Convert to hypertable with 1-hour chunks
SELECT create_hypertable('events', 'time',
    chunk_time_interval => INTERVAL '1 hour',
    create_default_indexes => FALSE,
    if_not_exists => TRUE
);

-- Space partitioning on node_id (32 hash buckets for write distribution)
SELECT add_dimension('events', by_hash('node_id', 32), if_not_exists => TRUE);

-- Minimal indexes (each index costs write throughput at 3M events/s)
CREATE INDEX IF NOT EXISTS idx_events_node_time ON events (node_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_events_type_time ON events (event_type, time DESC);

-- Stats cache table for pre-computed aggregations
CREATE TABLE IF NOT EXISTS stats_cache (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- 1-minute continuous aggregate
CREATE MATERIALIZED VIEW IF NOT EXISTS event_stats_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', time) AS bucket,
    node_id,
    event_type,
    COUNT(*)  AS event_count,
    MIN(time) AS first_event,
    MAX(time) AS last_event
FROM events
GROUP BY bucket, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_stats_1m',
    start_offset    => INTERVAL '10 minutes',
    end_offset      => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes',
    if_not_exists => TRUE
);

-- 1-hour continuous aggregate (hierarchical, from 1-minute)
CREATE MATERIALIZED VIEW IF NOT EXISTS event_stats_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id,
    event_type,
    SUM(event_count)  AS event_count,
    MIN(first_event)  AS first_event,
    MAX(last_event)   AS last_event
FROM event_stats_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_stats_1h',
    start_offset    => INTERVAL '4 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- Compression policy (compress chunks older than 2 hours)
ALTER TABLE events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'time DESC'
);

SELECT add_compression_policy('events', INTERVAL '2 hours', if_not_exists => TRUE);

-- Retention policies
SELECT add_retention_policy('events', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('event_stats_1m', INTERVAL '30 days', if_not_exists => TRUE);
SELECT add_retention_policy('event_stats_1h', INTERVAL '365 days', if_not_exists => TRUE);
