-- Migration 020: Unified event architecture.
--
-- Expands count tables to cover ALL 115 event types, adds wp_hash hot column
-- to ingested_raw_events, rebuilds UNION views to reference only count tables,
-- drops old continuous aggregates, and sets 1h retention on raw events.
--
-- After this migration:
--   - All 115 types → count tables (long-term aggregation, single source)
--   - All 115 types → ingested_raw_events (1h browsing store, hot columns)
--   - event_stats_30s/1m/1h and core_stats_1m → DROPPED
--   - UNION views → rebuilt with 14 count table branches (no continuous aggregate branches)

-- ============================================================
-- 1. status_counts (types 0, 10-13)
--    Dropped, Status, BestBlockChanged, FinalizedBlockChanged, SyncStatusChanged
-- ============================================================
CREATE TABLE status_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    slot INT
);
SELECT create_hypertable('status_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE status_counts ADD CHECK (event_type = 0 OR event_type BETWEEN 10 AND 13);
CREATE INDEX ON status_counts (node_id, event_type, bucket DESC);
ALTER TABLE status_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('status_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('status_counts', INTERVAL '3 days');

CREATE MATERIALIZED VIEW status_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM status_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('status_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('status_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW status_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM status_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('status_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('status_counts_1h', INTERVAL '365 days');

-- ============================================================
-- 2. connection_counts (types 20-28)
--    ConnectionRefused through PeerMisbehaved
-- ============================================================
CREATE TABLE connection_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    reason TEXT
);
SELECT create_hypertable('connection_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE connection_counts ADD CHECK (event_type BETWEEN 20 AND 28);
CREATE INDEX ON connection_counts (node_id, event_type, bucket DESC);
ALTER TABLE connection_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('connection_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('connection_counts', INTERVAL '3 days');

CREATE MATERIALIZED VIEW connection_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM connection_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('connection_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('connection_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW connection_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM connection_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('connection_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('connection_counts_1h', INTERVAL '365 days');

-- ============================================================
-- 3. block_counts (types 40-47)
--    Authoring through BlockExecuted
-- ============================================================
CREATE TABLE block_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    slot INT,
    reason TEXT
);
SELECT create_hypertable('block_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE block_counts ADD CHECK (event_type BETWEEN 40 AND 47);
CREATE INDEX ON block_counts (node_id, event_type, bucket DESC);
ALTER TABLE block_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('block_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('block_counts', INTERVAL '3 days');

CREATE MATERIALIZED VIEW block_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM block_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('block_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('block_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW block_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM block_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('block_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('block_counts_1h', INTERVAL '365 days');

-- ============================================================
-- 4. ticket_low_counts (types 80-82)
--    GeneratingTickets, TicketGenerationFailed, TicketsGenerated
-- ============================================================
CREATE TABLE ticket_low_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    reason TEXT
);
SELECT create_hypertable('ticket_low_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE ticket_low_counts ADD CHECK (event_type BETWEEN 80 AND 82);
CREATE INDEX ON ticket_low_counts (node_id, event_type, bucket DESC);
ALTER TABLE ticket_low_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('ticket_low_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('ticket_low_counts', INTERVAL '3 days');

CREATE MATERIALIZED VIEW ticket_low_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM ticket_low_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ticket_low_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('ticket_low_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW ticket_low_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM ticket_low_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ticket_low_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('ticket_low_counts_1h', INTERVAL '365 days');

-- ============================================================
-- 5. wp_pipeline_counts (types 90-105)
--    WorkPackageSubmission through GuaranteeBuilt
--    Core is nullable — enrichment may fail for types 90, 91, 103
--    Types 106-109 go to guarantee_sending_counts (CHECK extended below)
-- ============================================================
CREATE TABLE wp_pipeline_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    core SMALLINT,
    reason TEXT
);
SELECT create_hypertable('wp_pipeline_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE wp_pipeline_counts ADD CHECK (event_type BETWEEN 90 AND 105);
CREATE INDEX ON wp_pipeline_counts (node_id, event_type, bucket DESC);
ALTER TABLE wp_pipeline_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('wp_pipeline_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('wp_pipeline_counts', INTERVAL '3 days');

CREATE MATERIALIZED VIEW wp_pipeline_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM wp_pipeline_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('wp_pipeline_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('wp_pipeline_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW wp_pipeline_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM wp_pipeline_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('wp_pipeline_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('wp_pipeline_counts_1h', INTERVAL '365 days');

-- ============================================================
-- 5b. Extend guarantee_sending_counts to include type 109 (GuaranteesDistributed)
--     Previously type 109 was not pre-aggregated; now it goes to guarantee_sending_counts.
-- ============================================================
ALTER TABLE guarantee_sending_counts DROP CONSTRAINT IF EXISTS guarantee_sending_counts_event_type_check;
ALTER TABLE guarantee_sending_counts ADD CHECK (event_type BETWEEN 106 AND 109);

-- ============================================================
-- 6. wp_hash hot column on ingested_raw_events
--    Enables /grafana/wp/{hash} journey drilldown without JSONB chains
-- ============================================================
ALTER TABLE ingested_raw_events ADD COLUMN IF NOT EXISTS wp_hash BYTEA;
CREATE INDEX IF NOT EXISTS idx_ire_wp_hash
    ON ingested_raw_events (wp_hash, timestamp DESC) WHERE wp_hash IS NOT NULL;

-- ============================================================
-- 7. Drop old UNION views (must drop before dropping underlying aggregates)
-- ============================================================
DROP VIEW IF EXISTS all_event_stats_30s CASCADE;
DROP VIEW IF EXISTS all_event_stats_1m CASCADE;
DROP VIEW IF EXISTS all_event_stats_1h CASCADE;
DROP VIEW IF EXISTS all_core_stats_1m CASCADE;

-- ============================================================
-- 8. Drop old continuous aggregates
--    Count tables are now the single aggregation source.
-- ============================================================
-- Must drop hierarchical aggregates top-down (1h depends on 1m, 1m depends on 30s)
DROP MATERIALIZED VIEW IF EXISTS event_stats_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS event_stats_1m CASCADE;
DROP MATERIALIZED VIEW IF EXISTS event_stats_30s CASCADE;
DROP MATERIALIZED VIEW IF EXISTS core_stats_1m CASCADE;

-- ============================================================
-- 9. Rebuild UNION views — count tables only (14 branches)
-- ============================================================

-- 30s: raw count tables
CREATE VIEW all_event_stats_30s AS
  SELECT bucket, node_id, event_type, event_count FROM status_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM connection_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_low_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM wp_pipeline_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_distribution_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_sending_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_receiving_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM shard_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM assurance_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM bundle_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM segment_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM preimage_counts;

-- 1m: aggregated continuous aggregates
CREATE VIEW all_event_stats_1m AS
  SELECT bucket, node_id, event_type, event_count FROM status_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM connection_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_low_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM wp_pipeline_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_distribution_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_sending_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_receiving_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM shard_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM assurance_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM bundle_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM segment_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM preimage_counts_1m;

-- 1h: aggregated continuous aggregates
CREATE VIEW all_event_stats_1h AS
  SELECT bucket, node_id, event_type, event_count FROM status_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM connection_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_low_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM wp_pipeline_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_distribution_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_sending_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_receiving_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM shard_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM assurance_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM bundle_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM segment_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM preimage_counts_1h;

-- Core-aware UNION view (for timeseries?group_by=core and core=X filter)
-- Only tables with a core column participate.
-- Uses raw count tables (not _1m aggregates) because the continuous aggregates
-- drop the core dimension (they GROUP BY node_id, event_type only).
CREATE VIEW all_core_stats_1m AS
  SELECT bucket, event_type, core, event_count
    FROM guarantee_sending_counts WHERE core IS NOT NULL
  UNION ALL SELECT bucket, event_type, core, event_count
    FROM segment_counts WHERE core IS NOT NULL
  UNION ALL SELECT bucket, event_type, core, event_count
    FROM wp_pipeline_counts WHERE core IS NOT NULL;

-- ============================================================
-- 10. Set 1h retention on ingested_raw_events
--     Table is now a pure browsing store — no aggregation depends on it.
-- ============================================================
SELECT remove_retention_policy('ingested_raw_events', if_exists => TRUE);
SELECT add_retention_policy('ingested_raw_events', INTERVAL '1 hour');

-- ============================================================
-- 11. Drop the backward-compatibility VIEW alias for 'events'
--     (created in migration 015, no longer needed)
-- ============================================================
DROP VIEW IF EXISTS events_view CASCADE;
-- Keep the 'events' VIEW alias for now — legacy endpoints still reference it
-- until they are removed in later phases.
