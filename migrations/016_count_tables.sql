-- Per-protocol-group count tables for pre-aggregated high-volume events.
-- Events are counted in-memory (DashMap) and flushed every 5s via COPY BINARY.
-- Append-only: multiple rows per logical key are correct because all query paths
-- do SUM(event_count) GROUP BY ...

-- ============================================================
-- 1. block_distribution_counts (types 60-68)
-- ============================================================
CREATE TABLE block_distribution_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    slot INT,
    reason TEXT
);
SELECT create_hypertable('block_distribution_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE block_distribution_counts ADD CHECK (event_type BETWEEN 60 AND 68);
CREATE INDEX ON block_distribution_counts (node_id, event_type, bucket DESC);
ALTER TABLE block_distribution_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('block_distribution_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('block_distribution_counts', INTERVAL '3 days');

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
-- 2. ticket_counts (types 83-84)
-- ============================================================
CREATE TABLE ticket_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    reason TEXT,
    from_proxy BOOLEAN,
    epoch INT
);
SELECT create_hypertable('ticket_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE ticket_counts ADD CHECK (event_type BETWEEN 83 AND 84);
CREATE INDEX ON ticket_counts (node_id, event_type, bucket DESC);
ALTER TABLE ticket_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('ticket_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('ticket_counts', INTERVAL '3 days');

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
-- 3. guarantee_sending_counts (types 106-108)
-- ============================================================
CREATE TABLE guarantee_sending_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    core SMALLINT,
    reason TEXT
);
SELECT create_hypertable('guarantee_sending_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE guarantee_sending_counts ADD CHECK (event_type BETWEEN 106 AND 108);
CREATE INDEX ON guarantee_sending_counts (node_id, event_type, bucket DESC);
ALTER TABLE guarantee_sending_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('guarantee_sending_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('guarantee_sending_counts', INTERVAL '3 days');

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
-- 4. guarantee_receiving_counts (types 110-113)
-- ============================================================
CREATE TABLE guarantee_receiving_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    slot INT,
    reason TEXT
);
SELECT create_hypertable('guarantee_receiving_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE guarantee_receiving_counts ADD CHECK (event_type BETWEEN 110 AND 113);
CREATE INDEX ON guarantee_receiving_counts (node_id, event_type, bucket DESC);
-- Partial index for /guarantee-discards endpoint (no node_id leading column)
CREATE INDEX ON guarantee_receiving_counts (event_type, bucket DESC) WHERE reason IS NOT NULL;
ALTER TABLE guarantee_receiving_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('guarantee_receiving_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('guarantee_receiving_counts', INTERVAL '3 days');

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
-- 5. shard_counts (types 120-125)
-- ============================================================
CREATE TABLE shard_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    reason TEXT
);
SELECT create_hypertable('shard_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE shard_counts ADD CHECK (event_type BETWEEN 120 AND 125);
CREATE INDEX ON shard_counts (node_id, event_type, bucket DESC);
ALTER TABLE shard_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('shard_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('shard_counts', INTERVAL '3 days');

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
-- 6. assurance_counts (types 126-131)
-- ============================================================
CREATE TABLE assurance_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    reason TEXT
);
SELECT create_hypertable('assurance_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE assurance_counts ADD CHECK (event_type BETWEEN 126 AND 131);
CREATE INDEX ON assurance_counts (node_id, event_type, bucket DESC);
ALTER TABLE assurance_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('assurance_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('assurance_counts', INTERVAL '3 days');

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
-- 7. bundle_counts (types 140-153)
-- ============================================================
CREATE TABLE bundle_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    reason TEXT,
    kind SMALLINT
);
SELECT create_hypertable('bundle_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE bundle_counts ADD CHECK (event_type BETWEEN 140 AND 153);
CREATE INDEX ON bundle_counts (node_id, event_type, bucket DESC);
ALTER TABLE bundle_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('bundle_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('bundle_counts', INTERVAL '3 days');

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
-- 8. segment_counts (types 160-178)
-- ============================================================
CREATE TABLE segment_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    core SMALLINT,
    reason TEXT,
    kind SMALLINT
);
SELECT create_hypertable('segment_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE segment_counts ADD CHECK (event_type BETWEEN 160 AND 178);
CREATE INDEX ON segment_counts (node_id, event_type, bucket DESC);
ALTER TABLE segment_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('segment_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('segment_counts', INTERVAL '3 days');

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
-- 9. preimage_counts (types 190-199)
-- ============================================================
CREATE TABLE preimage_counts (
    bucket TIMESTAMPTZ NOT NULL,
    node_id TEXT NOT NULL,
    event_type SMALLINT NOT NULL,
    event_count BIGINT NOT NULL,
    reason TEXT,
    service_id INT
);
SELECT create_hypertable('preimage_counts', 'bucket', chunk_time_interval => INTERVAL '1 day');
ALTER TABLE preimage_counts ADD CHECK (event_type BETWEEN 190 AND 199);
CREATE INDEX ON preimage_counts (node_id, event_type, bucket DESC);
ALTER TABLE preimage_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('preimage_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('preimage_counts', INTERVAL '3 days');

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
-- UNION views: transparent query interface combining raw + pre-aggregated
-- ============================================================

-- 30s: raw event_stats_30s + raw count tables
CREATE VIEW all_event_stats_30s AS
  SELECT bucket, node_id, event_type, event_count FROM event_stats_30s
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_distribution_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_sending_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_receiving_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM shard_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM assurance_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM bundle_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM segment_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM preimage_counts;

-- 1m: aggregated
CREATE VIEW all_event_stats_1m AS
  SELECT bucket, node_id, event_type, event_count FROM event_stats_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_distribution_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_sending_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_receiving_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM shard_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM assurance_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM bundle_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM segment_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM preimage_counts_1m;

-- 1h: aggregated
CREATE VIEW all_event_stats_1h AS
  SELECT bucket, node_id, event_type, event_count FROM event_stats_1h
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
-- core_stats_1m has no node_id column, so we project without it.
CREATE VIEW all_core_stats_1m AS
  SELECT bucket, event_type, core, event_count FROM core_stats_1m
  UNION ALL SELECT bucket, event_type, core, event_count
    FROM guarantee_sending_counts_1m WHERE core IS NOT NULL
  UNION ALL SELECT bucket, event_type, core, event_count
    FROM segment_counts_1m WHERE core IS NOT NULL;
