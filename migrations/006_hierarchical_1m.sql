-- Rebuild event_stats_1m as hierarchical aggregate FROM event_stats_30s
-- and event_stats_1h FROM event_stats_1m.
-- This eliminates the double raw-event scan (previously both 1m and 30s scanned raw).
-- Chain: raw events -> 30s -> 1m -> 1h

-- Drop existing aggregates (1h depends on 1m, so drop 1h first)
DROP MATERIALIZED VIEW IF EXISTS event_stats_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS event_stats_1m CASCADE;

-- Recreate 1m from 30s (hierarchical)
CREATE MATERIALIZED VIEW event_stats_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count,
    MIN(first_event) AS first_event,
    MAX(last_event) AS last_event
FROM event_stats_30s
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_stats_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes',
    if_not_exists => TRUE);

SELECT add_retention_policy('event_stats_1m', INTERVAL '30 days', if_not_exists => TRUE);

-- Recreate 1h from 1m (hierarchical, unchanged logic)
CREATE MATERIALIZED VIEW event_stats_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', bucket) AS bucket,
    node_id, event_type,
    SUM(event_count) AS event_count,
    MIN(first_event) AS first_event,
    MAX(last_event) AS last_event
FROM event_stats_1m
GROUP BY 1, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_stats_1h',
    start_offset => INTERVAL '4 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE);

SELECT add_retention_policy('event_stats_1h', INTERVAL '365 days', if_not_exists => TRUE);
