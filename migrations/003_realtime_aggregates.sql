-- Tighten continuous aggregate refresh policy.
-- 30s end_offset + 30s schedule_interval ensures data is materialized quickly.
-- We keep materialized_only=true (the default when views were created WITH NO DATA)
-- because real-time mode (materialized_only=false) forces every aggregate query to
-- also scan unmaterialized raw events, which is catastrophic under high write load.
-- A 30-60s lag in aggregate data is acceptable for dashboard analytics.

SELECT remove_continuous_aggregate_policy('event_stats_1m', if_exists => TRUE);
SELECT add_continuous_aggregate_policy('event_stats_1m',
    start_offset    => INTERVAL '10 minutes',
    end_offset      => INTERVAL '30 seconds',
    schedule_interval => INTERVAL '30 seconds',
    if_not_exists => TRUE
);
