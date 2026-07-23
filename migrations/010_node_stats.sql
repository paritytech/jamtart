-- Node stats table: extracts Status (event 10) fields at ingestion time.
-- Avoids JSONB queries for peer counts, DA storage, preimages.
-- ~512 rows/sec (Status fires every 2s, 1,023 nodes). Tiny rows (~50 bytes).

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

-- Compression
ALTER TABLE node_stats SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'node_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);

SELECT add_compression_policy('node_stats', INTERVAL '2 hours', if_not_exists => TRUE);
SELECT add_retention_policy('node_stats', INTERVAL '7 days', if_not_exists => TRUE);
