-- Hot columns: promote frequently-queried JSONB fields to top-level columns
-- Nullable, no default — instant ALTER. NULL for existing rows, populated going forward.
-- 7-day retention ages out NULLs.

ALTER TABLE events ADD COLUMN IF NOT EXISTS slot INT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS core SMALLINT;
ALTER TABLE events ADD COLUMN IF NOT EXISTS submission_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_events_slot ON events (slot, timestamp DESC) WHERE slot IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_core ON events (core, timestamp DESC) WHERE core IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_events_submission_id ON events (node_id, submission_id, timestamp DESC) WHERE submission_id IS NOT NULL;
