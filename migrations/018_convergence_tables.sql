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
