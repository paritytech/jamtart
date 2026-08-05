-- Per-protocol-group count tables for pre-aggregated events (all 115 types).
-- Events are counted in-memory (DashMap) and flushed every 5s via COPY BINARY.
-- Append-only: multiple rows per logical key are correct because all query paths
-- do SUM(event_count) GROUP BY ...

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
-- sync_timeline queries status_counts by event_type without node_id
CREATE INDEX IF NOT EXISTS idx_status_counts_et
    ON status_counts (event_type, bucket DESC) WHERE slot IS NOT NULL;
ALTER TABLE status_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('status_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('status_counts', INTERVAL '3 days');

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
-- connections_timeline queries connection_counts by event_type without node_id
CREATE INDEX IF NOT EXISTS idx_connection_counts_et
    ON connection_counts (event_type, bucket DESC);
ALTER TABLE connection_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('connection_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('connection_counts', INTERVAL '3 days');

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

-- ============================================================
-- 5. wp_pipeline_counts (types 90-105)
--    WorkPackageSubmission through GuaranteeBuilt
--    Core is nullable — enrichment may fail for types 90, 91, 103
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
-- all_core_stats_1m queries raw tables with core filter (partial: many rows have NULL core)
CREATE INDEX IF NOT EXISTS idx_wp_pipeline_counts_core
    ON wp_pipeline_counts (core, event_type, bucket DESC) WHERE core IS NOT NULL;
ALTER TABLE wp_pipeline_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('wp_pipeline_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('wp_pipeline_counts', INTERVAL '3 days');

-- ============================================================
-- 6. block_distribution_counts (types 60-68)
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

-- ============================================================
-- 7. ticket_counts (types 83-84)
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

-- ============================================================
-- 8. guarantee_sending_counts (types 106-109)
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
ALTER TABLE guarantee_sending_counts ADD CHECK (event_type BETWEEN 106 AND 109);
CREATE INDEX ON guarantee_sending_counts (node_id, event_type, bucket DESC);
-- all_core_stats_1m queries raw tables with core filter (partial: many rows have NULL core)
CREATE INDEX IF NOT EXISTS idx_guarantee_sending_counts_core
    ON guarantee_sending_counts (core, event_type, bucket DESC) WHERE core IS NOT NULL;
ALTER TABLE guarantee_sending_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('guarantee_sending_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('guarantee_sending_counts', INTERVAL '3 days');

-- ============================================================
-- 9. guarantee_receiving_counts (types 110-113)
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

-- ============================================================
-- 10. shard_counts (types 120-125)
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

-- ============================================================
-- 11. assurance_counts (types 126-131)
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

-- ============================================================
-- 12. bundle_counts (types 140-153)
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

-- ============================================================
-- 13. segment_counts (types 160-178)
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
-- all_core_stats_1m queries raw tables with core filter (partial: many rows have NULL core)
CREATE INDEX IF NOT EXISTS idx_segment_counts_core
    ON segment_counts (core, event_type, bucket DESC) WHERE core IS NOT NULL;
ALTER TABLE segment_counts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id, event_type',
    timescaledb.compress_orderby = 'bucket DESC'
);
SELECT add_compression_policy('segment_counts', compress_after => INTERVAL '2 hours');
SELECT add_retention_policy('segment_counts', INTERVAL '3 days');

-- ============================================================
-- 14. preimage_counts (types 190-199)
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
