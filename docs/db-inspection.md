# Database Inspection Runbook

psql runbook for verifying schema, data flow, compression, and retention after deployment.

## 1. Schema verification

```sql
-- Tables exist
\dt+ events event_services node_stats nodes stats_cache

-- Hypertables
SELECT hypertable_name, num_dimensions, num_chunks
FROM timescaledb_information.hypertables;

-- Hot columns exist
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'events' AND column_name IN ('slot', 'core', 'submission_id');

-- Indexes
SELECT indexname, indexdef FROM pg_indexes
WHERE tablename = 'events' ORDER BY indexname;
```

## 2. Continuous aggregates

```sql
-- All aggregates exist and are refreshing
SELECT view_name, materialization_hypertable_name,
    view_definition IS NOT NULL AS has_definition
FROM timescaledb_information.continuous_aggregates;

-- Refresh policies active
SELECT application_name, schedule_interval, config
FROM timescaledb_information.jobs
WHERE application_name LIKE 'Refresh%';

-- Freshness check: latest bucket in each aggregate
SELECT 'event_stats_30s' AS agg, MAX(bucket) FROM event_stats_30s
UNION ALL SELECT 'event_stats_1m', MAX(bucket) FROM event_stats_1m
UNION ALL SELECT 'event_stats_1h', MAX(bucket) FROM event_stats_1h
UNION ALL SELECT 'core_stats_1m', MAX(bucket) FROM core_stats_1m
UNION ALL SELECT 'service_stats_1m', MAX(bucket) FROM service_stats_1m
UNION ALL SELECT 'node_stats_1m', MAX(bucket) FROM node_stats_1m;
```

## 3. Data flow

```sql
-- Hot columns populated
SELECT slot, core, submission_id FROM events WHERE slot IS NOT NULL LIMIT 5;

-- event_services populated
SELECT * FROM event_services ORDER BY timestamp DESC LIMIT 5;

-- node_stats populated
SELECT * FROM node_stats ORDER BY timestamp DESC LIMIT 5;

-- wp_tracking
SELECT encode(wp_hash, 'hex'), stage, received_by FROM wp_tracking ORDER BY first_seen DESC LIMIT 10;

-- slot_convergence
SELECT * FROM slot_convergence ORDER BY authored_at DESC LIMIT 10;
```

## 4. Compression

```sql
SELECT hypertable_name, number_compressed_chunks,
    before_compression_total_bytes, after_compression_total_bytes,
    ROUND(1 - after_compression_total_bytes::numeric / NULLIF(before_compression_total_bytes, 0), 2) AS ratio
FROM chunk_compression_stats('events')
UNION ALL
SELECT hypertable_name, number_compressed_chunks,
    before_compression_total_bytes, after_compression_total_bytes,
    ROUND(1 - after_compression_total_bytes::numeric / NULLIF(before_compression_total_bytes, 0), 2)
FROM chunk_compression_stats('node_stats');
```

## 5. Retention

```sql
-- Check retention policies
SELECT hypertable_name, schedule_interval, config
FROM timescaledb_information.jobs
WHERE proc_name = 'policy_retention';

-- Manual cleanup for regular tables
DELETE FROM wp_tracking WHERE first_seen < NOW() - INTERVAL '7 days';
DELETE FROM slot_convergence WHERE authored_at < NOW() - INTERVAL '7 days';
```

## 6. Size overview

```sql
SELECT hypertable_name,
    pg_size_pretty(total_bytes) as total,
    pg_size_pretty(table_bytes) as table_size,
    pg_size_pretty(index_bytes) as index_size
FROM hypertable_detailed_size('events')
UNION ALL
SELECT hypertable_name, pg_size_pretty(total_bytes), pg_size_pretty(table_bytes), pg_size_pretty(index_bytes)
FROM hypertable_detailed_size('node_stats')
UNION ALL
SELECT hypertable_name, pg_size_pretty(total_bytes), pg_size_pretty(table_bytes), pg_size_pretty(index_bytes)
FROM hypertable_detailed_size('event_services');
```

## 7. Performance

```sql
-- Slowest queries from pg_stat_statements
SELECT query, calls, mean_exec_time, total_exec_time
FROM pg_stat_statements
ORDER BY mean_exec_time DESC LIMIT 10;

-- Active queries
SELECT pid, now() - pg_stat_activity.query_start AS duration, query
FROM pg_stat_activity
WHERE state = 'active' AND query NOT LIKE '%pg_stat%'
ORDER BY duration DESC;
```
