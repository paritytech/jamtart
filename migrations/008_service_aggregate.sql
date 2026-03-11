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
