-- On-chain statistics tables (from JAM RPC `statistics(header_hash)`)
-- Three hypertables: core, service, validator stats per block.
-- Partitioned on timestamp (consistent with all existing hypertables).
-- No unique constraints (dedup in application layer via in-memory LRU).

-- Core stats: 341 rows/block, ~22 KB/block
CREATE TABLE onchain_core_stats (
    timestamp       TIMESTAMPTZ NOT NULL,
    slot            INT         NOT NULL,
    header_hash     BYTEA       NOT NULL,  -- 32 bytes
    core            SMALLINT    NOT NULL,
    gas_used        BIGINT      NOT NULL DEFAULT 0,
    da_load         INT         NOT NULL DEFAULT 0,
    popularity      SMALLINT    NOT NULL DEFAULT 0,
    imports         SMALLINT    NOT NULL DEFAULT 0,
    extrinsic_count SMALLINT    NOT NULL DEFAULT 0,
    extrinsic_size  INT         NOT NULL DEFAULT 0,
    exports         SMALLINT    NOT NULL DEFAULT 0,
    bundle_size     INT         NOT NULL DEFAULT 0,
    on_best_chain   BOOLEAN     NOT NULL DEFAULT TRUE
);
SELECT create_hypertable('onchain_core_stats', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    create_default_indexes => FALSE,
    if_not_exists => TRUE);
CREATE INDEX idx_ocs_ts_core ON onchain_core_stats (timestamp DESC, core);
CREATE INDEX idx_ocs_slot ON onchain_core_stats (slot DESC);

-- Service stats: ~50 rows/block, ~4 KB/block
CREATE TABLE onchain_service_stats (
    timestamp         TIMESTAMPTZ NOT NULL,
    slot              INT         NOT NULL,
    header_hash       BYTEA       NOT NULL,
    service_id        INT         NOT NULL,
    provided_count    SMALLINT    NOT NULL DEFAULT 0,
    provided_size     INT         NOT NULL DEFAULT 0,
    refinement_count  INT         NOT NULL DEFAULT 0,
    refinement_gas    BIGINT      NOT NULL DEFAULT 0,
    imports           INT         NOT NULL DEFAULT 0,
    extrinsic_count   INT         NOT NULL DEFAULT 0,
    extrinsic_size    INT         NOT NULL DEFAULT 0,
    exports           INT         NOT NULL DEFAULT 0,
    accumulate_count  INT         NOT NULL DEFAULT 0,
    accumulate_gas    BIGINT      NOT NULL DEFAULT 0,
    on_best_chain     BOOLEAN     NOT NULL DEFAULT TRUE
);
SELECT create_hypertable('onchain_service_stats', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    create_default_indexes => FALSE,
    if_not_exists => TRUE);
CREATE INDEX idx_oss_ts_svc ON onchain_service_stats (timestamp DESC, service_id);
CREATE INDEX idx_oss_slot ON onchain_service_stats (slot DESC);

-- Validator stats: 1024 rows/block, ~62 KB/block (epoch-cumulative values)
CREATE TABLE onchain_validator_stats (
    timestamp        TIMESTAMPTZ NOT NULL,
    slot             INT         NOT NULL,
    header_hash      BYTEA       NOT NULL,
    validator_index  SMALLINT    NOT NULL,
    blocks_produced  INT         NOT NULL DEFAULT 0,
    tickets          INT         NOT NULL DEFAULT 0,
    preimages        INT         NOT NULL DEFAULT 0,
    preimages_size   INT         NOT NULL DEFAULT 0,
    guarantees       INT         NOT NULL DEFAULT 0,
    assurances       INT         NOT NULL DEFAULT 0,
    on_best_chain    BOOLEAN     NOT NULL DEFAULT TRUE
);
SELECT create_hypertable('onchain_validator_stats', 'timestamp',
    chunk_time_interval => INTERVAL '1 day',
    create_default_indexes => FALSE,
    if_not_exists => TRUE);
CREATE INDEX idx_ovs_ts_val ON onchain_validator_stats (timestamp DESC, validator_index);
CREATE INDEX idx_ovs_slot ON onchain_validator_stats (slot DESC);

-- Enable compression on all three tables
ALTER TABLE onchain_core_stats SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'core',
    timescaledb.compress_orderby = 'timestamp DESC'
);
ALTER TABLE onchain_service_stats SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'service_id',
    timescaledb.compress_orderby = 'timestamp DESC'
);
ALTER TABLE onchain_validator_stats SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'validator_index',
    timescaledb.compress_orderby = 'timestamp DESC'
);

-- Compression after 7 days, retention 90 days (all three)
SELECT add_compression_policy('onchain_core_stats', INTERVAL '7 days');
SELECT add_compression_policy('onchain_service_stats', INTERVAL '7 days');
SELECT add_compression_policy('onchain_validator_stats', INTERVAL '7 days');
SELECT add_retention_policy('onchain_core_stats', INTERVAL '90 days');
SELECT add_retention_policy('onchain_service_stats', INTERVAL '90 days');
SELECT add_retention_policy('onchain_validator_stats', INTERVAL '90 days');

-- Finalization tracker (singleton row)
CREATE TABLE onchain_finalization (
    id             INT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    finalized_slot INT         NOT NULL DEFAULT 0,
    finalized_hash BYTEA       NOT NULL DEFAULT '\x00',
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
INSERT INTO onchain_finalization DEFAULT VALUES;
