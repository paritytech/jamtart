-- 30-second continuous aggregate: finest granularity for debugging
-- Only this aggregate scans raw events. 1m and 1h are hierarchical (from 30s/1m).

CREATE MATERIALIZED VIEW IF NOT EXISTS event_stats_30s
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('30 seconds', timestamp) AS bucket,
    node_id, event_type,
    COUNT(*) AS event_count,
    MIN(timestamp) AS first_event,
    MAX(timestamp) AS last_event
FROM events
GROUP BY bucket, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_stats_30s',
    start_offset => INTERVAL '5 minutes',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

SELECT add_retention_policy('event_stats_30s', INTERVAL '3 days', if_not_exists => TRUE);
