-- Migration 018: Convergence tables + expanded percentiles
--
-- Adds guarantee convergence tracking (per work_report_hash + per-slot summary)
-- and expands existing slot_convergence with p75/p95 percentile columns.

-- ── Expanded percentiles for existing slot_convergence ──────────────────
ALTER TABLE slot_convergence ADD COLUMN IF NOT EXISTS p75_ms INT;
ALTER TABLE slot_convergence ADD COLUMN IF NOT EXISTS p95_ms INT;

-- ── Guarantee convergence (per work_report_hash) ────────────────────────
-- One row per guarantee. Populated by convergence_tracker flush.
-- Measures: GuaranteeBuilt(105) → GuaranteeReceived(112) propagation latency.
CREATE TABLE IF NOT EXISTS guarantee_convergence (
    work_report_hash  BYTEA NOT NULL PRIMARY KEY,
    slot              INT NOT NULL,
    core              SMALLINT,          -- nullable: NULL when guarantor not connected to telemetry
    wp_hash           BYTEA,
    node_count        SMALLINT NOT NULL,
    p50_ms            INT NOT NULL,
    p75_ms            INT,
    p95_ms            INT,
    p99_ms            INT NOT NULL,
    p100_ms           INT NOT NULL,
    built_at          TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_guarantee_convergence_time
    ON guarantee_convergence (built_at DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_convergence_core
    ON guarantee_convergence (core, built_at DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_convergence_wp
    ON guarantee_convergence (wp_hash, built_at DESC);

-- ── Guarantee convergence per-slot summary ──────────────────────────────
-- One row per slot. Aggregates all guarantees in the slot.
-- Used by /guarantee-convergence overview endpoint.
CREATE TABLE IF NOT EXISTS guarantee_convergence_slots (
    slot              INT NOT NULL PRIMARY KEY,
    slot_timestamp    TIMESTAMPTZ,
    guarantee_count   SMALLINT NOT NULL,
    node_count        SMALLINT NOT NULL,
    p50_ms            INT,
    p75_ms            INT,
    p95_ms            INT,
    p99_ms            INT,
    p100_ms           INT,
    built_at          TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_guarantee_conv_slots_time
    ON guarantee_convergence_slots (built_at DESC);

-- ── Assurance convergence per-anchor summary ────────────────────────────
-- One row per block anchor. Aggregates all senders' assurance propagation.
-- Measures: DistributingAssurance(126) → AssuranceReceived(131) per sender.
-- Also tracks distribution start spread (how quickly validators begin distributing).
CREATE TABLE IF NOT EXISTS assurance_convergence (
    anchor              BYTEA NOT NULL PRIMARY KEY,
    slot                INT,
    slot_timestamp      TIMESTAMPTZ,
    sender_count        SMALLINT NOT NULL,
    receiver_count      INT NOT NULL,
    -- Reception convergence (distribution→reception deltas, clamped to >= 0)
    p50_ms              INT NOT NULL,
    p75_ms              INT,
    p95_ms              INT,
    p99_ms              INT NOT NULL,
    p100_ms             INT NOT NULL,
    -- Distribution start spread (relative to first distributor)
    dist_start_p50_ms   INT,
    dist_start_p95_ms   INT,
    dist_start_p99_ms   INT,
    dist_start_p100_ms  INT,
    first_distributed_at TIMESTAMPTZ,
    last_distributed_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_assurance_convergence_slot
    ON assurance_convergence (slot DESC);

-- ── Assurance convergence per-sender detail ─────────────────────────────
-- For debugging individual node assurance propagation.
-- Hypertable: ~1023 senders × ~14.4k anchors/day ≈ ~14.7M rows/day at full load.
-- INSERT-only (no unique constraint — cross-chunk uniqueness impractical on hypertables).
CREATE TABLE IF NOT EXISTS assurance_convergence_senders (
    distributed_at      TIMESTAMPTZ NOT NULL,
    anchor              BYTEA NOT NULL,
    sender_node_id      TEXT NOT NULL,
    node_count          SMALLINT NOT NULL,
    p50_ms              INT NOT NULL,
    p75_ms              INT,
    p95_ms              INT,
    p99_ms              INT NOT NULL,
    p100_ms             INT NOT NULL
);

SELECT create_hypertable('assurance_convergence_senders', 'distributed_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_assurance_conv_senders_node
    ON assurance_convergence_senders (sender_node_id, distributed_at DESC);
