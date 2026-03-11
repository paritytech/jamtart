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
