-- Node stats (Status event extraction) and per-service junction table,
-- each with a 1-minute continuous aggregate.

-- ============================================================
-- node_stats: extracts Status (event 10) fields at ingestion time.
-- Avoids JSONB queries for peer counts, DA storage, preimages.
-- ~512 rows/sec (Status fires every 2s, 1,023 nodes). Tiny rows (~50 bytes).
-- ============================================================
CREATE TABLE IF NOT EXISTS node_stats (
    timestamp       TIMESTAMPTZ NOT NULL,
    node_id         TEXT        NOT NULL,
    num_peers       INT         NOT NULL,
    num_val_peers   INT         NOT NULL,
    num_sync_peers  INT         NOT NULL,
    num_shards      INT         NOT NULL,
    shards_size     BIGINT      NOT NULL,
    num_preimages   INT         NOT NULL,
    preimages_size  INT         NOT NULL,
    -- Guarantee pool: scalar summaries only (array dropped)
    min_guarantees       SMALLINT    NOT NULL,
    max_guarantees       SMALLINT    NOT NULL,
    avg_guarantees       REAL        NOT NULL,
    zero_guarantee_cores SMALLINT    NOT NULL
);

SELECT create_hypertable('node_stats', 'timestamp',
    chunk_time_interval => INTERVAL '1 hour',
    create_default_indexes => FALSE,
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_node_stats_node_time ON node_stats (node_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_node_stats_time ON node_stats (timestamp DESC);

ALTER TABLE node_stats SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('node_stats', INTERVAL '2 hours', if_not_exists => TRUE);
SELECT add_retention_policy('node_stats', INTERVAL '7 days', if_not_exists => TRUE);

-- Node stats aggregate: AVG/MIN/MAX per node per minute.
-- For longer time ranges and network-wide views.
CREATE MATERIALIZED VIEW IF NOT EXISTS node_stats_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    node_id,
    AVG(num_peers)::INT AS avg_peers,
    MIN(num_peers) AS min_peers,
    MAX(num_peers) AS max_peers,
    AVG(num_val_peers)::INT AS avg_val_peers,
    MIN(num_val_peers) AS min_val_peers,
    MAX(num_val_peers) AS max_val_peers,
    AVG(num_sync_peers)::INT AS avg_sync_peers,
    MIN(num_sync_peers) AS min_sync_peers,
    MAX(num_sync_peers) AS max_sync_peers,
    AVG(num_shards)::INT AS avg_shards,
    MIN(num_shards) AS min_shards,
    MAX(num_shards) AS max_shards,
    AVG(shards_size)::BIGINT AS avg_shards_size,
    MAX(shards_size) AS max_shards_size,
    AVG(num_preimages)::INT AS avg_preimages,
    MAX(num_preimages) AS max_preimages,
    AVG(preimages_size)::INT AS avg_preimages_size,
    MAX(preimages_size) AS max_preimages_size,
    -- Guarantee pool scalars (from pre-computed columns)
    AVG(avg_guarantees) AS avg_guarantees,
    MIN(min_guarantees) AS min_guarantees,
    MAX(max_guarantees) AS max_guarantees,
    MAX(zero_guarantee_cores) AS max_zero_guarantee_cores,
    COUNT(*) AS status_count
FROM node_stats
GROUP BY bucket, node_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('node_stats_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes',
    if_not_exists => TRUE);

SELECT add_retention_policy('node_stats_1m', INTERVAL '30 days', if_not_exists => TRUE);

ALTER MATERIALIZED VIEW node_stats_1m SET (timescaledb.materialized_only = false);

-- node_stats_1m: per-node drill-down queries (1024 nodes, huge selectivity gain)
CREATE INDEX IF NOT EXISTS idx_node_stats_1m_node
    ON node_stats_1m (node_id, bucket DESC);

-- ============================================================
-- event_services: service junction table, one row per service per event.
-- Only low-volume pipeline events written (~13.5K rows/slot, ~2.3K rows/sec).
-- gas_used populated for gas-bearing events (Authorized=95, Refined=101, BlockExecuted=47).
-- elapsed_ns/load_ns: execution timing for types 47, 95, 101.
-- ============================================================
CREATE TABLE IF NOT EXISTS event_services (
    timestamp    TIMESTAMPTZ NOT NULL,
    node_id      TEXT        NOT NULL,
    event_type   SMALLINT    NOT NULL,
    service_id   INT         NOT NULL,
    gas_used     BIGINT,     -- NULL for events without gas data
    elapsed_ns   BIGINT,     -- total wall-clock execution time (from ExecCost.ns)
    load_ns      BIGINT      -- PVM code loading/compilation time
);

SELECT create_hypertable('event_services', 'timestamp',
    chunk_time_interval => INTERVAL '1 hour',
    create_default_indexes => FALSE,
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_event_services_service ON event_services (service_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_event_services_time ON event_services (timestamp DESC);

-- Retention matches raw events' historical value (no compression on purpose)
SELECT add_retention_policy('event_services', INTERVAL '7 days', if_not_exists => TRUE);

-- Service stats aggregate: per-service event counts and gas totals per minute.
CREATE MATERIALIZED VIEW IF NOT EXISTS service_stats_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    service_id,
    event_type,
    COUNT(*) AS event_count,
    SUM(gas_used) AS total_gas
FROM event_services
GROUP BY bucket, service_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('service_stats_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes',
    if_not_exists => TRUE);

SELECT add_retention_policy('service_stats_1m', INTERVAL '30 days', if_not_exists => TRUE);

ALTER MATERIALIZED VIEW service_stats_1m SET (timescaledb.materialized_only = false);

-- service_stats_1m: queries filter by service_id
CREATE INDEX IF NOT EXISTS idx_service_stats_1m_svc
    ON service_stats_1m (service_id, bucket DESC);
