-- Migration 004: Frontend search indexes for new API endpoints
-- Supports: /api/events/search, /api/slots/:slot, service_id timeseries

-- Expression index for event search by core_index (extracted from JSONB)
CREATE INDEX IF NOT EXISTS idx_events_core_index ON events (
    (COALESCE(
        CAST(data->'WorkPackageReceived'->>'core' AS INTEGER),
        CAST(data->'GuaranteeBuilt'->'outline'->>'core' AS INTEGER)
    ))
) WHERE event_type BETWEEN 90 AND 113;

-- Expression index for slot lookup across event types
CREATE INDEX IF NOT EXISTS idx_events_slot ON events (
    (COALESCE(
        CAST(data->'Authoring'->>'slot' AS INTEGER),
        CAST(data->'BestBlockChanged'->>'slot' AS INTEGER),
        CAST(data->'BlockAnnounced'->>'slot' AS INTEGER),
        CAST(data->'BlockTransferred'->>'slot' AS INTEGER)
    ))
) WHERE event_type IN (11, 12, 40, 42, 43, 62, 68);

-- Expression index for service_id timeseries
CREATE INDEX IF NOT EXISTS idx_events_service_id ON events (
    (data->'Refined'->'costs'->>'service_id'), created_at DESC
) WHERE event_type = 101;
