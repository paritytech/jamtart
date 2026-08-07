-- UNION views: transparent query interface over the 14 count-table groups.
-- Grafana endpoints auto-select the tier (30s raw / 1m / 1h) by time range.

-- 30s: raw count tables
CREATE VIEW all_event_stats_30s AS
  SELECT bucket, node_id, event_type, event_count FROM status_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM connection_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_low_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM wp_pipeline_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_distribution_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_sending_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_receiving_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM shard_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM assurance_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM bundle_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM segment_counts
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM preimage_counts;

-- 1m: aggregated continuous aggregates
CREATE VIEW all_event_stats_1m AS
  SELECT bucket, node_id, event_type, event_count FROM status_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM connection_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_low_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM wp_pipeline_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_distribution_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_sending_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_receiving_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM shard_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM assurance_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM bundle_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM segment_counts_1m
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM preimage_counts_1m;

-- 1h: aggregated continuous aggregates
CREATE VIEW all_event_stats_1h AS
  SELECT bucket, node_id, event_type, event_count FROM status_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM connection_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_low_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM wp_pipeline_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM block_distribution_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM ticket_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_sending_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM guarantee_receiving_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM shard_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM assurance_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM bundle_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM segment_counts_1h
  UNION ALL SELECT bucket, node_id, event_type, event_count FROM preimage_counts_1h;

-- Core-aware UNION view (for timeseries?group_by=core and core=X filter).
-- Only the three groups carrying a core dimension participate. Reads the
-- _1m aggregates, so core queries get their 30-day retention.
CREATE VIEW all_core_stats_1m AS
  SELECT bucket, event_type, core, event_count
    FROM guarantee_sending_counts_1m WHERE core IS NOT NULL
  UNION ALL SELECT bucket, event_type, core, event_count
    FROM segment_counts_1m WHERE core IS NOT NULL
  UNION ALL SELECT bucket, event_type, core, event_count
    FROM wp_pipeline_counts_1m WHERE core IS NOT NULL;
