-- Migration 005: Performance optimizations
--
-- Removes the per-row trigger on events table that fires an individual
-- UPDATE nodes for every inserted row. This is the single biggest write
-- throughput bottleneck: a 500-row batch insert fires 500 separate UPDATEs.
-- Node stats are now updated in batch by the application after each flush.

-- Drop the per-row trigger
DROP TRIGGER IF EXISTS trg_update_node_last_seen ON events;

-- Keep the function around for reference but it's no longer called by trigger
-- DROP FUNCTION IF EXISTS update_node_last_seen();
