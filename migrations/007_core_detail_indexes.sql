-- Targeted index for the core_wp_ids CTE used by ALL core detail queries.
-- Matches: WHERE event_type = 94 AND CAST(data->'WorkPackageReceived'->>'core' AS INTEGER) = $1
-- The existing idx_events_core_index uses COALESCE(...) which PG can't match to bare CAST.
CREATE INDEX IF NOT EXISTS idx_events_wp_received_core
ON events ((CAST(data->'WorkPackageReceived'->>'core' AS INTEGER)), created_at DESC)
WHERE event_type = 94;
