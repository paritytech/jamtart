-- Slot convergence: pre-computed per-slot block propagation stats.
-- Populated at ingestion time by SlotTracker — no raw event scans needed.
-- ~4 rows per slot (authored, announced, imported, executed).
-- At 7-day retention: ~400K rows. Regular table (not hypertable).

CREATE TABLE IF NOT EXISTS slot_convergence (
    slot         INT NOT NULL,
    event_type   SMALLINT NOT NULL,
    node_count   SMALLINT NOT NULL,
    p50_ms       INT NOT NULL,
    p99_ms       INT NOT NULL,
    p100_ms      INT NOT NULL,
    authored_at  TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (slot, event_type)
);

CREATE INDEX IF NOT EXISTS idx_slot_convergence_time ON slot_convergence (authored_at DESC);
