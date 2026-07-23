-- Core stats aggregate: per-core event counts per minute.
-- Depends on hot column `core` from migration 004.

CREATE MATERIALIZED VIEW IF NOT EXISTS core_stats_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    core, event_type,
    COUNT(*) AS event_count
FROM events
WHERE core IS NOT NULL
GROUP BY bucket, core, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('core_stats_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes',
    if_not_exists => TRUE);

SELECT add_retention_policy('core_stats_1m', INTERVAL '30 days', if_not_exists => TRUE);
