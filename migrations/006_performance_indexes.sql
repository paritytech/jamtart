-- Migration 006: Performance indexes for query optimization
--
-- Expression index for workpackage hash lookups (replaces data::text LIKE scan)
-- Partial index for failure event types
-- Partial index for node status events

-- Expression index for fast work package hash lookups across all relevant JSONB fields
CREATE INDEX IF NOT EXISTS idx_events_wp_hash ON events (
    (COALESCE(
        data->'WorkPackageSubmitted'->>'hash',
        data->'WorkPackageReceived'->>'hash',
        data->'WorkPackageReceived'->'outline'->>'hash',
        data->'DuplicateWorkPackage'->>'hash',
        data->'WorkPackageHashMapped'->>'work_package_hash',
        data->'Refined'->>'hash',
        data->'GuaranteeBuilt'->'outline'->>'hash',
        data->'GuaranteeReceived'->>'hash'
    ))
) WHERE event_type BETWEEN 90 AND 113;

-- Partial index for failure event types (quick failure rate queries)
CREATE INDEX IF NOT EXISTS idx_events_failures ON events (event_type, created_at DESC)
WHERE event_type IN (41, 92, 113);

-- Partial index for node status events (fast node status lookups)
CREATE INDEX IF NOT EXISTS idx_events_node_status ON events (node_id, created_at DESC)
WHERE event_type = 10;
