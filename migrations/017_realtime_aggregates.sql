-- Enable real-time aggregation on all continuous aggregates.
--
-- With materialized_only = false, TimescaleDB appends a live tail scan on the
-- source table for the un-materialized time window (last 2-4 minutes). This
-- eliminates the gap where recent data is invisible in Grafana panels.
--
-- PERFORMANCE WARNING (1024-validator networks):
-- If aggregate queries become slow, this setting is the first thing to check.
-- The tail scan reads raw data for the un-materialized window on every query.
-- Post count-table refactoring the cost is low (ingested_raw_events only has
-- low-volume event types), but under extreme load it may add latency.
--
-- To revert a single aggregate:
--   ALTER MATERIALIZED VIEW <view_name> SET (timescaledb.materialized_only = true);

-- Original aggregates (from migrations 005/006/009/008/011/015)
ALTER MATERIALIZED VIEW event_stats_30s SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW event_stats_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW event_stats_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW core_stats_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW service_stats_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW node_stats_1m SET (timescaledb.materialized_only = false);

-- Count table aggregates (from migration 016) — 9 groups × 2 tiers
ALTER MATERIALIZED VIEW block_distribution_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW block_distribution_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW ticket_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW ticket_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW guarantee_sending_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW guarantee_sending_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW guarantee_receiving_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW guarantee_receiving_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW shard_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW shard_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW assurance_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW assurance_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW bundle_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW bundle_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW segment_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW segment_counts_1h SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW preimage_counts_1m SET (timescaledb.materialized_only = false);
ALTER MATERIALIZED VIEW preimage_counts_1h SET (timescaledb.materialized_only = false);
