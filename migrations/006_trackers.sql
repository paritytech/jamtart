-- Tracker-populated tables: slot/guarantee/assurance convergence, work-package
-- pipeline tracking, and DA operational stats + latency histograms.
--
-- Histogram columns use CONVERGENCE_BOUNDS (23 buckets, 0ms-120s):
--   [0,2) [2,5) [5,10) [10,15) [15,20) [20,30) [30,50) [50,75) [75,100)
--   [100,150) [150,250) [250,500) [500,1k) [1k,2k) [2k,5k) [5k,10k)
--   [10k,15k) [15k,20k) [20k,25k) [25k,30k) [30k,60k) [60k,120k) [120k,+inf)

-- ============================================================
-- Slot convergence: pre-computed per-slot block propagation stats.
-- Populated at ingestion time by SlotTracker — no raw event scans needed.
-- ~4 rows per slot. Regular table (not hypertable).
-- ============================================================
CREATE TABLE IF NOT EXISTS slot_convergence (
    slot         INT NOT NULL,
    event_type   SMALLINT NOT NULL,
    node_count   SMALLINT NOT NULL,
    p50_ms       INT NOT NULL,
    p99_ms       INT NOT NULL,
    p100_ms      INT NOT NULL,
    authored_at  TIMESTAMPTZ NOT NULL,
    p75_ms       INT,
    p95_ms       INT,
    PRIMARY KEY (slot, event_type)
);

CREATE INDEX IF NOT EXISTS idx_slot_convergence_time ON slot_convergence (authored_at DESC);

-- ============================================================
-- Work package tracking: unique WP counting and pipeline funnel.
-- Regular table (NOT hypertable) — wp_hash is the true unique key.
-- ============================================================
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
    stage            SMALLINT NOT NULL,
    -- node_id: which node first received this WP (from WorkPackageReceived event)
    node_id          TEXT,
    -- refine_gas_used: total gas from Refined event (SUM of costs[].total.gas_used)
    refine_gas_used  BIGINT,
    -- failure_reason: from WorkPackageFailed event reason field
    failure_reason   TEXT,
    -- discard_reason: from GuaranteeDiscarded event via guarantee_convergence wp_hash mapping
    discard_reason   TEXT
);

CREATE INDEX IF NOT EXISTS idx_wp_tracking_time ON wp_tracking (first_seen DESC);
CREATE INDEX IF NOT EXISTS idx_wp_tracking_core ON wp_tracking (core, first_seen DESC);
CREATE INDEX IF NOT EXISTS idx_wp_tracking_stage ON wp_tracking (stage, first_seen DESC);
-- Partial index for wp-active queries: only rows that haven't completed or failed
CREATE INDEX IF NOT EXISTS idx_wp_tracking_active
    ON wp_tracking (first_seen DESC)
    WHERE distributed_at IS NULL AND failed_at IS NULL;

-- ============================================================
-- Guarantee convergence (per work_report_hash).
-- Measures: GuaranteeBuilt(105) -> GuaranteeReceived(112) propagation latency.
-- ============================================================
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
    built_at          TIMESTAMPTZ NOT NULL,
    -- builder_node_id: for per-guarantor analysis
    builder_node_id   TEXT,
    h_0_2          INT DEFAULT 0,
    h_2_5          INT DEFAULT 0,
    h_5_10         INT DEFAULT 0,
    h_10_15        INT DEFAULT 0,
    h_15_20        INT DEFAULT 0,
    h_20_30        INT DEFAULT 0,
    h_30_50        INT DEFAULT 0,
    h_50_75        INT DEFAULT 0,
    h_75_100       INT DEFAULT 0,
    h_100_150      INT DEFAULT 0,
    h_150_250      INT DEFAULT 0,
    h_250_500      INT DEFAULT 0,
    h_500_1000     INT DEFAULT 0,
    h_1000_2000    INT DEFAULT 0,
    h_2000_5000    INT DEFAULT 0,
    h_5000_10000   INT DEFAULT 0,
    h_10000_15000  INT DEFAULT 0,
    h_15000_20000  INT DEFAULT 0,
    h_20000_25000  INT DEFAULT 0,
    h_25000_30000  INT DEFAULT 0,
    h_30000_60000  INT DEFAULT 0,
    h_60000_120000 INT DEFAULT 0,
    h_120000_plus  INT DEFAULT 0,
    hist_total      INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_guarantee_convergence_time
    ON guarantee_convergence (built_at DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_convergence_core
    ON guarantee_convergence (core, built_at DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_convergence_wp
    ON guarantee_convergence (wp_hash, built_at DESC);

-- ============================================================
-- Guarantee convergence per-slot summary.
-- ============================================================
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

-- ============================================================
-- Assurance convergence per-anchor summary.
-- Measures: DistributingAssurance(126) -> AssuranceReceived(131) per sender.
-- ============================================================
CREATE TABLE IF NOT EXISTS assurance_convergence (
    anchor              BYTEA NOT NULL PRIMARY KEY,
    slot                INT,
    slot_timestamp      TIMESTAMPTZ,
    sender_count        SMALLINT NOT NULL,
    receiver_count      INT NOT NULL,
    -- Reception convergence (distribution->reception deltas, clamped to >= 0)
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
    last_distributed_at  TIMESTAMPTZ,
    h_0_2          INT DEFAULT 0,
    h_2_5          INT DEFAULT 0,
    h_5_10         INT DEFAULT 0,
    h_10_15        INT DEFAULT 0,
    h_15_20        INT DEFAULT 0,
    h_20_30        INT DEFAULT 0,
    h_30_50        INT DEFAULT 0,
    h_50_75        INT DEFAULT 0,
    h_75_100       INT DEFAULT 0,
    h_100_150      INT DEFAULT 0,
    h_150_250      INT DEFAULT 0,
    h_250_500      INT DEFAULT 0,
    h_500_1000     INT DEFAULT 0,
    h_1000_2000    INT DEFAULT 0,
    h_2000_5000    INT DEFAULT 0,
    h_5000_10000   INT DEFAULT 0,
    h_10000_15000  INT DEFAULT 0,
    h_15000_20000  INT DEFAULT 0,
    h_20000_25000  INT DEFAULT 0,
    h_25000_30000  INT DEFAULT 0,
    h_30000_60000  INT DEFAULT 0,
    h_60000_120000 INT DEFAULT 0,
    h_120000_plus  INT DEFAULT 0,
    hist_total      INT DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_assurance_convergence_slot
    ON assurance_convergence (slot DESC);
-- queries filter by first_distributed_at
CREATE INDEX IF NOT EXISTS idx_assurance_convergence_time
    ON assurance_convergence (first_distributed_at);

-- ============================================================
-- Assurance convergence per-sender detail (hypertable, default chunking).
-- INSERT-only (no unique constraint — cross-chunk uniqueness impractical).
-- ============================================================
CREATE TABLE IF NOT EXISTS assurance_convergence_senders (
    distributed_at      TIMESTAMPTZ NOT NULL,
    anchor              BYTEA NOT NULL,
    sender_node_id      TEXT NOT NULL,
    node_count          SMALLINT NOT NULL,
    p50_ms              INT NOT NULL,
    p75_ms              INT,
    p95_ms              INT,
    p99_ms              INT NOT NULL,
    p100_ms             INT NOT NULL,
    h_0_2          INT DEFAULT 0,
    h_2_5          INT DEFAULT 0,
    h_5_10         INT DEFAULT 0,
    h_10_15        INT DEFAULT 0,
    h_15_20        INT DEFAULT 0,
    h_20_30        INT DEFAULT 0,
    h_30_50        INT DEFAULT 0,
    h_50_75        INT DEFAULT 0,
    h_75_100       INT DEFAULT 0,
    h_100_150      INT DEFAULT 0,
    h_150_250      INT DEFAULT 0,
    h_250_500      INT DEFAULT 0,
    h_500_1000     INT DEFAULT 0,
    h_1000_2000    INT DEFAULT 0,
    h_2000_5000    INT DEFAULT 0,
    h_5000_10000   INT DEFAULT 0,
    h_10000_15000  INT DEFAULT 0,
    h_15000_20000  INT DEFAULT 0,
    h_20000_25000  INT DEFAULT 0,
    h_25000_30000  INT DEFAULT 0,
    h_30000_60000  INT DEFAULT 0,
    h_60000_120000 INT DEFAULT 0,
    h_120000_plus  INT DEFAULT 0,
    hist_total      INT DEFAULT 0
);

SELECT create_hypertable('assurance_convergence_senders', 'distributed_at', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_assurance_conv_senders_node
    ON assurance_convergence_senders (sender_node_id, distributed_at DESC);

-- ============================================================
-- DA node stats: per-node shard event counts, latency averages, shard inventory.
-- Populated by da_tracker flush every 10s (hypertable, default chunking).
-- ============================================================
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

-- ============================================================
-- Shard latency histogram. 14 buckets (ms): [0,1) [1,2) [2,5) [5,10) [10,25)
-- [25,50) [50,100) [100,250) [250,500) [500,1000) [1000,2000) [2000,3000)
-- [3000,5000) [5000,inf). Side: 0=assurer (120->125), 1=guarantor (121->124).
-- (Hypertable, default chunking; deliberately no secondary index.)
-- ============================================================
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

-- ============================================================
-- bundle_latency_hist: side 0=shard_req(140->145), 1=shard_resp(141->145), 2=full_req(148->153), 3=full_resp(149->153), 4=reconstruct(146->147), 5=e2e(140->147)
-- ============================================================
CREATE TABLE IF NOT EXISTS bundle_latency_hist (
    ts              TIMESTAMPTZ NOT NULL,
    node_id         TEXT NOT NULL,
    side            SMALLINT NOT NULL,
    h_0_2          INT DEFAULT 0,
    h_2_5          INT DEFAULT 0,
    h_5_10         INT DEFAULT 0,
    h_10_15        INT DEFAULT 0,
    h_15_20        INT DEFAULT 0,
    h_20_30        INT DEFAULT 0,
    h_30_50        INT DEFAULT 0,
    h_50_75        INT DEFAULT 0,
    h_75_100       INT DEFAULT 0,
    h_100_150      INT DEFAULT 0,
    h_150_250      INT DEFAULT 0,
    h_250_500      INT DEFAULT 0,
    h_500_1000     INT DEFAULT 0,
    h_1000_2000    INT DEFAULT 0,
    h_2000_5000    INT DEFAULT 0,
    h_5000_10000   INT DEFAULT 0,
    h_10000_15000  INT DEFAULT 0,
    h_15000_20000  INT DEFAULT 0,
    h_20000_25000  INT DEFAULT 0,
    h_25000_30000  INT DEFAULT 0,
    h_30000_60000  INT DEFAULT 0,
    h_60000_120000 INT DEFAULT 0,
    h_120000_plus  INT DEFAULT 0,
    total_count     INT DEFAULT 0,
    failed_count    INT DEFAULT 0
);
SELECT create_hypertable('bundle_latency_hist', 'ts', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_bundle_latency_hist_node ON bundle_latency_hist (node_id, ts DESC);

-- ============================================================
-- segment_latency_hist: side 0=shard_req(162->167), 1=shard_resp(163->167), 2=full_req(173->178), 3=full_resp(174->178), 4=reconstruct(168->170)
-- ============================================================
CREATE TABLE IF NOT EXISTS segment_latency_hist (
    ts              TIMESTAMPTZ NOT NULL,
    node_id         TEXT NOT NULL,
    side            SMALLINT NOT NULL,
    h_0_2          INT DEFAULT 0,
    h_2_5          INT DEFAULT 0,
    h_5_10         INT DEFAULT 0,
    h_10_15        INT DEFAULT 0,
    h_15_20        INT DEFAULT 0,
    h_20_30        INT DEFAULT 0,
    h_30_50        INT DEFAULT 0,
    h_50_75        INT DEFAULT 0,
    h_75_100       INT DEFAULT 0,
    h_100_150      INT DEFAULT 0,
    h_150_250      INT DEFAULT 0,
    h_250_500      INT DEFAULT 0,
    h_500_1000     INT DEFAULT 0,
    h_1000_2000    INT DEFAULT 0,
    h_2000_5000    INT DEFAULT 0,
    h_5000_10000   INT DEFAULT 0,
    h_10000_15000  INT DEFAULT 0,
    h_15000_20000  INT DEFAULT 0,
    h_20000_25000  INT DEFAULT 0,
    h_25000_30000  INT DEFAULT 0,
    h_30000_60000  INT DEFAULT 0,
    h_60000_120000 INT DEFAULT 0,
    h_120000_plus  INT DEFAULT 0,
    total_count     INT DEFAULT 0,
    failed_count    INT DEFAULT 0
);
SELECT create_hypertable('segment_latency_hist', 'ts', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_segment_latency_hist_node ON segment_latency_hist (node_id, ts DESC);

-- ============================================================
-- preimage_latency_hist: side 0=req(193->198), 1=resp(194->198)
-- ============================================================
CREATE TABLE IF NOT EXISTS preimage_latency_hist (
    ts              TIMESTAMPTZ NOT NULL,
    node_id         TEXT NOT NULL,
    side            SMALLINT NOT NULL,
    h_0_2          INT DEFAULT 0,
    h_2_5          INT DEFAULT 0,
    h_5_10         INT DEFAULT 0,
    h_10_15        INT DEFAULT 0,
    h_15_20        INT DEFAULT 0,
    h_20_30        INT DEFAULT 0,
    h_30_50        INT DEFAULT 0,
    h_50_75        INT DEFAULT 0,
    h_75_100       INT DEFAULT 0,
    h_100_150      INT DEFAULT 0,
    h_150_250      INT DEFAULT 0,
    h_250_500      INT DEFAULT 0,
    h_500_1000     INT DEFAULT 0,
    h_1000_2000    INT DEFAULT 0,
    h_2000_5000    INT DEFAULT 0,
    h_5000_10000   INT DEFAULT 0,
    h_10000_15000  INT DEFAULT 0,
    h_15000_20000  INT DEFAULT 0,
    h_20000_25000  INT DEFAULT 0,
    h_25000_30000  INT DEFAULT 0,
    h_30000_60000  INT DEFAULT 0,
    h_60000_120000 INT DEFAULT 0,
    h_120000_plus  INT DEFAULT 0,
    total_count     INT DEFAULT 0,
    failed_count    INT DEFAULT 0
);
SELECT create_hypertable('preimage_latency_hist', 'ts', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_preimage_latency_hist_node ON preimage_latency_hist (node_id, ts DESC);
