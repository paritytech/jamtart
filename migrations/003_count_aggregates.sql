-- Hierarchical continuous aggregates over the count tables:
-- <group>_1m (from raw, 30d retention) and <group>_1h (from _1m, 365d retention).
-- guarantee_sending and segment aggregates keep the `core` dimension.

-- ============================================================
-- block_distribution_counts
-- ============================================================
CREATE MATERIALIZED VIEW block_distribution_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM block_distribution_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('block_distribution_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('block_distribution_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW block_distribution_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM block_distribution_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('block_distribution_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('block_distribution_counts_1h', INTERVAL '365 days');

-- ============================================================
-- ticket_counts
-- ============================================================
CREATE MATERIALIZED VIEW ticket_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM ticket_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ticket_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('ticket_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW ticket_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM ticket_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('ticket_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('ticket_counts_1h', INTERVAL '365 days');

-- ============================================================
-- guarantee_sending_counts
-- ============================================================
CREATE MATERIALIZED VIEW guarantee_sending_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type, core,
    SUM(event_count) AS event_count
FROM guarantee_sending_counts
GROUP BY 1, node_id, event_type, core
WITH NO DATA;

SELECT add_continuous_aggregate_policy('guarantee_sending_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('guarantee_sending_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW guarantee_sending_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type, core,
    SUM(event_count) AS event_count
FROM guarantee_sending_counts_1m
GROUP BY 1, node_id, event_type, core
WITH NO DATA;

SELECT add_continuous_aggregate_policy('guarantee_sending_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('guarantee_sending_counts_1h', INTERVAL '365 days');

-- ============================================================
-- guarantee_receiving_counts
-- ============================================================
CREATE MATERIALIZED VIEW guarantee_receiving_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM guarantee_receiving_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('guarantee_receiving_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('guarantee_receiving_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW guarantee_receiving_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM guarantee_receiving_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('guarantee_receiving_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('guarantee_receiving_counts_1h', INTERVAL '365 days');

-- ============================================================
-- shard_counts
-- ============================================================
CREATE MATERIALIZED VIEW shard_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM shard_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('shard_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('shard_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW shard_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM shard_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('shard_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('shard_counts_1h', INTERVAL '365 days');

-- ============================================================
-- assurance_counts
-- ============================================================
CREATE MATERIALIZED VIEW assurance_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM assurance_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('assurance_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('assurance_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW assurance_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM assurance_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('assurance_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('assurance_counts_1h', INTERVAL '365 days');

-- ============================================================
-- bundle_counts
-- ============================================================
CREATE MATERIALIZED VIEW bundle_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM bundle_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('bundle_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('bundle_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW bundle_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM bundle_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('bundle_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('bundle_counts_1h', INTERVAL '365 days');

-- ============================================================
-- segment_counts
-- ============================================================
CREATE MATERIALIZED VIEW segment_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type, core,
    SUM(event_count) AS event_count
FROM segment_counts
GROUP BY 1, node_id, event_type, core
WITH NO DATA;

SELECT add_continuous_aggregate_policy('segment_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('segment_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW segment_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type, core,
    SUM(event_count) AS event_count
FROM segment_counts_1m
GROUP BY 1, node_id, event_type, core
WITH NO DATA;

SELECT add_continuous_aggregate_policy('segment_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('segment_counts_1h', INTERVAL '365 days');

-- ============================================================
-- preimage_counts
-- ============================================================
CREATE MATERIALIZED VIEW preimage_counts_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM preimage_counts
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('preimage_counts_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes');
SELECT add_retention_policy('preimage_counts_1m', INTERVAL '30 days');

CREATE MATERIALIZED VIEW preimage_counts_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count
FROM preimage_counts_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('preimage_counts_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour');
SELECT add_retention_policy('preimage_counts_1h', INTERVAL '365 days');

-- ============================================================
-- status_counts
-- ============================================================
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
-- connection_counts
-- ============================================================
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
-- block_counts
-- ============================================================
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
-- ticket_low_counts
-- ============================================================
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
-- wp_pipeline_counts
-- ============================================================
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
-- Real-time aggregation (materialized_only = false) on all 28 aggregates:
-- the query planner appends a live tail scan over the un-materialized
-- window (last 2-4 min), so recent data is visible in Grafana panels and
-- every branch of the all_event_stats_* UNION views has the same freshness.
--
-- PERFORMANCE WARNING (1024-validator networks): the tail scan reads the
-- raw count table for the un-materialized window on every query. If
-- aggregate queries become slow, this setting is the first thing to check.
-- To revert a single aggregate:
--   ALTER MATERIALIZED VIEW <view_name> SET (timescaledb.materialized_only = true);
-- ============================================================
ALTER MATERIALIZED VIEW status_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW status_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW connection_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW connection_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW block_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW block_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW ticket_low_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW ticket_low_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW wp_pipeline_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW wp_pipeline_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW block_distribution_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW block_distribution_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW ticket_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW ticket_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW guarantee_sending_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW guarantee_sending_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW guarantee_receiving_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW guarantee_receiving_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW shard_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW shard_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW assurance_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW assurance_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW bundle_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW bundle_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW segment_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW segment_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW preimage_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW preimage_counts_1h SET (timescaledb.materialized_only = false);

-- ============================================================
-- (event_type, bucket DESC) indexes: aggregate queries filter by event_type
-- after bucket range narrowing; without these they sequential-scan.
-- ============================================================
CREATE INDEX IF NOT EXISTS idx_block_distribution_counts_1m_et
    ON block_distribution_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_block_distribution_counts_1h_et
    ON block_distribution_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_counts_1m_et
    ON ticket_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_counts_1h_et
    ON ticket_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_sending_counts_1m_et
    ON guarantee_sending_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_sending_counts_1h_et
    ON guarantee_sending_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_receiving_counts_1m_et
    ON guarantee_receiving_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_receiving_counts_1h_et
    ON guarantee_receiving_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_shard_counts_1m_et
    ON shard_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_shard_counts_1h_et
    ON shard_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_assurance_counts_1m_et
    ON assurance_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_assurance_counts_1h_et
    ON assurance_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_bundle_counts_1m_et
    ON bundle_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_bundle_counts_1h_et
    ON bundle_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_segment_counts_1m_et
    ON segment_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_segment_counts_1h_et
    ON segment_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_preimage_counts_1m_et
    ON preimage_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_preimage_counts_1h_et
    ON preimage_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_status_counts_1m_et
    ON status_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_status_counts_1h_et
    ON status_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_connection_counts_1m_et
    ON connection_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_connection_counts_1h_et
    ON connection_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_block_counts_1m_et
    ON block_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_block_counts_1h_et
    ON block_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_low_counts_1m_et
    ON ticket_low_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_low_counts_1h_et
    ON ticket_low_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_wp_pipeline_counts_1m_et
    ON wp_pipeline_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_wp_pipeline_counts_1h_et
    ON wp_pipeline_counts_1h (event_type, bucket DESC);
