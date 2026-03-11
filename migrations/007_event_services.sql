-- Service junction table: one row per service per event.
-- Only low-volume pipeline events written (~13.5K rows/slot, ~2.3K rows/sec).
-- gas_used populated for gas-bearing events (Authorized=95, Refined=101, BlockExecuted=47).

CREATE TABLE IF NOT EXISTS event_services (
    timestamp    TIMESTAMPTZ NOT NULL,
    node_id      TEXT        NOT NULL,
    event_type   SMALLINT    NOT NULL,
    service_id   INT         NOT NULL,
    gas_used     BIGINT      -- NULL for events without gas data
);

SELECT create_hypertable('event_services', 'timestamp',
    chunk_time_interval => INTERVAL '1 hour',
    create_default_indexes => FALSE,
    if_not_exists => TRUE
);

CREATE INDEX IF NOT EXISTS idx_event_services_service ON event_services (service_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_event_services_time ON event_services (timestamp DESC);

-- Retention matches raw events
SELECT add_retention_policy('event_services', INTERVAL '7 days', if_not_exists => TRUE);
