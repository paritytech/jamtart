-- Work package tracking: unique WP counting and pipeline funnel.
-- Regular table (NOT hypertable) — wp_hash is the true unique key.
-- At ~57 rows/sec with 7-day retention: ~34M rows max.

CREATE TABLE IF NOT EXISTS wp_tracking (
    wp_hash          BYTEA PRIMARY KEY,
    first_seen       TIMESTAMPTZ NOT NULL,
    last_updated     TIMESTAMPTZ NOT NULL,
    core             SMALLINT NOT NULL,
    service_ids      INT[] NOT NULL,
    -- Pipeline stage timestamps (NULL = not reached yet)
    received_at      TIMESTAMPTZ,
    authorized_at    TIMESTAMPTZ,
    refined_at       TIMESTAMPTZ,
    report_built_at  TIMESTAMPTZ,
    guarantee_built_at TIMESTAMPTZ,
    distributed_at   TIMESTAMPTZ,
    failed_at        TIMESTAMPTZ,
    -- Counts
    received_by      SMALLINT DEFAULT 0,
    guaranteed_by    SMALLINT DEFAULT 0,
    -- Pipeline stage as explicit ordinal (NOT event_type number)
    -- 0=received, 1=authorized, 2=refined, 3=report_built, 4=guarantee_built, 5=distributed
    stage            SMALLINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_wp_tracking_time ON wp_tracking (first_seen DESC);
CREATE INDEX IF NOT EXISTS idx_wp_tracking_core ON wp_tracking (core, first_seen DESC);
CREATE INDEX IF NOT EXISTS idx_wp_tracking_stage ON wp_tracking (stage, first_seen DESC);
