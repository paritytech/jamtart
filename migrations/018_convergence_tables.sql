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

-- ── DA node stats ───────────────────────────────────────────────────────
-- Per-node DA operational stats: shard event counts, latency averages, shard inventory.
-- Populated by da_tracker flush every 10s. One row per active node per flush.
CREATE TABLE IF NOT EXISTS da_node_stats (
    ts                        TIMESTAMPTZ NOT NULL,
    node_id                   TEXT NOT NULL,
    shard_requests_sent       INT DEFAULT 0,
    shard_requests_received   INT DEFAULT 0,
    shard_sent_confirmed      INT DEFAULT 0,
    shard_received_confirmed  INT DEFAULT 0,
    shards_transferred        INT DEFAULT 0,
    shard_failures            INT DEFAULT 0,
    preimage_ann_failures     INT DEFAULT 0,
    preimages_announced       INT DEFAULT 0,
    preimages_forgotten       INT DEFAULT 0,
    assurer_avg_latency_ms    REAL,
    assurer_latency_samples   INT DEFAULT 0,
    guarantor_avg_latency_ms  REAL,
    guarantor_latency_samples INT DEFAULT 0,
    active_shards             INT DEFAULT 0
);

SELECT create_hypertable('da_node_stats', 'ts', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_da_node_stats_node
    ON da_node_stats (node_id, ts DESC);

-- ── Shard latency histogram ─────────────────────────────────────────────
-- Latency distribution for shard requests. 14 buckets (ms): [0,1), [1,2), [2,5), [5,10),
-- [10,25), [25,50), [50,100), [100,250), [250,500), [500,1000), [1000,2000), [2000,3000),
-- [3000,5000), [5000,∞). Side: 0=assurer (120→125), 1=guarantor (121→124).
-- Histograms are mergeable: SUM bucket columns across nodes/time for combined distribution.
CREATE TABLE IF NOT EXISTS shard_latency_hist (
    ts              TIMESTAMPTZ NOT NULL,
    node_id         TEXT NOT NULL,
    side            SMALLINT NOT NULL,
    b_0_1           INT DEFAULT 0,
    b_1_2           INT DEFAULT 0,
    b_2_5           INT DEFAULT 0,
    b_5_10          INT DEFAULT 0,
    b_10_25         INT DEFAULT 0,
    b_25_50         INT DEFAULT 0,
    b_50_100        INT DEFAULT 0,
    b_100_250       INT DEFAULT 0,
    b_250_500       INT DEFAULT 0,
    b_500_1000      INT DEFAULT 0,
    b_1000_2000     INT DEFAULT 0,
    b_2000_3000     INT DEFAULT 0,
    b_3000_5000     INT DEFAULT 0,
    b_5000_plus     INT DEFAULT 0,
    total_count     INT DEFAULT 0,
    failed_count    INT DEFAULT 0
);

SELECT create_hypertable('shard_latency_hist', 'ts', if_not_exists => TRUE);
