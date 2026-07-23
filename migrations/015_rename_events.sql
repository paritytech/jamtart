-- Rename raw events hypertable: events → ingested_raw_events.
-- Pre-aggregated event types will be written to per-group count tables (migration 016).
-- The VIEW alias preserves backward compatibility for sqlx migrations that reference 'events'.
--
-- DB will be dropped and re-created, so no data migration needed.

-- 1. Drop continuous aggregates that reference 'events' directly.
--    (event_stats_1m/1h are hierarchical from 30s, but core_stats_1m scans 'events' directly)
DROP MATERIALIZED VIEW IF EXISTS event_stats_1h CASCADE;
DROP MATERIALIZED VIEW IF EXISTS event_stats_1m CASCADE;
DROP MATERIALIZED VIEW IF EXISTS event_stats_30s CASCADE;
DROP MATERIALIZED VIEW IF EXISTS core_stats_1m CASCADE;

-- Also drop the events_view (from migration 002) that joins events with event_types
DROP VIEW IF EXISTS events_view CASCADE;

-- 2. Rename the hypertable
ALTER TABLE events RENAME TO ingested_raw_events;

-- 3. Create VIEW alias for backward compatibility
--    (earlier migrations reference 'events' — VIEW satisfies those references)
CREATE VIEW events AS SELECT * FROM ingested_raw_events;

-- 4. Recreate event_stats_30s on ingested_raw_events
CREATE MATERIALIZED VIEW event_stats_30s
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('30 seconds', timestamp) AS bucket,
    node_id, event_type,
    COUNT(*) AS event_count,
    MIN(timestamp) AS first_event,
    MAX(timestamp) AS last_event
FROM ingested_raw_events
GROUP BY bucket, node_id, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('event_stats_30s',
    start_offset => INTERVAL '5 minutes',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 minute',
    if_not_exists => TRUE);

SELECT add_retention_policy('event_stats_30s', INTERVAL '3 days', if_not_exists => TRUE);

-- 5. Recreate event_stats_1m (hierarchical from 30s)
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

-- 6. Recreate event_stats_1h (hierarchical from 1m)
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

-- 7. Recreate core_stats_1m on ingested_raw_events
CREATE MATERIALIZED VIEW core_stats_1m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', timestamp) AS bucket,
    core, event_type,
    COUNT(*) AS event_count
FROM ingested_raw_events
WHERE core IS NOT NULL
GROUP BY bucket, core, event_type
WITH NO DATA;

SELECT add_continuous_aggregate_policy('core_stats_1m',
    start_offset => INTERVAL '10 minutes',
    end_offset => INTERVAL '2 minutes',
    schedule_interval => INTERVAL '2 minutes',
    if_not_exists => TRUE);

SELECT add_retention_policy('core_stats_1m', INTERVAL '30 days', if_not_exists => TRUE);

-- 8. Recreate events_view (from migration 002)
CREATE VIEW events_view AS
    SELECT e.*, et.name AS event_type_name, et.group_name
    FROM ingested_raw_events e
    LEFT JOIN event_types et ON e.event_type = et.id;
