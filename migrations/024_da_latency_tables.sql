-- Migration 024: DA latency histogram tables for bundle reconstruction,
-- segment fetching, and preimage transfers.
--
-- Uses CONVERGENCE_BOUNDS (23 buckets, 0ms–120s):
--   [0,2) [2,5) [5,10) [10,15) [15,20) [20,30) [30,50) [50,75) [75,100)
--   [100,150) [150,250) [250,500) [500,1k) [1k,2k) [2k,5k) [5k,10k)
--   [10k,15k) [15k,20k) [20k,25k) [25k,30k) [30k,60k) [60k,120k) [120k,+inf)
--
-- Side encoding per table:
--   bundle_latency_hist:  0=shard_req(140->145), 1=shard_resp(141->145),
--                         2=full_req(148->153), 3=full_resp(149->153),
--                         4=reconstruct(146->147), 5=e2e(140->147)
--   segment_latency_hist: 0=shard_req(162->167), 1=shard_resp(163->167),
--                         2=full_req(173->178), 3=full_resp(174->178),
--                         4=reconstruct(168->170)
--   preimage_latency_hist: 0=req(193->198), 1=resp(194->198)

-- ── Bundle reconstruction latency ────────────────────────────────────
CREATE TABLE IF NOT EXISTS bundle_latency_hist (
    ts              TIMESTAMPTZ NOT NULL,
    node_id         TEXT NOT NULL,
    side            SMALLINT NOT NULL,
    h_0_2           INT DEFAULT 0,
    h_2_5           INT DEFAULT 0,
    h_5_10          INT DEFAULT 0,
    h_10_15         INT DEFAULT 0,
    h_15_20         INT DEFAULT 0,
    h_20_30         INT DEFAULT 0,
    h_30_50         INT DEFAULT 0,
    h_50_75         INT DEFAULT 0,
    h_75_100        INT DEFAULT 0,
    h_100_150       INT DEFAULT 0,
    h_150_250       INT DEFAULT 0,
    h_250_500       INT DEFAULT 0,
    h_500_1000      INT DEFAULT 0,
    h_1000_2000     INT DEFAULT 0,
    h_2000_5000     INT DEFAULT 0,
    h_5000_10000    INT DEFAULT 0,
    h_10000_15000   INT DEFAULT 0,
    h_15000_20000   INT DEFAULT 0,
    h_20000_25000   INT DEFAULT 0,
    h_25000_30000   INT DEFAULT 0,
    h_30000_60000   INT DEFAULT 0,
    h_60000_120000  INT DEFAULT 0,
    h_120000_plus   INT DEFAULT 0,
    total_count     INT DEFAULT 0,
    failed_count    INT DEFAULT 0
);
SELECT create_hypertable('bundle_latency_hist', 'ts', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_bundle_latency_hist_node ON bundle_latency_hist (node_id, ts DESC);

-- ── Segment fetching latency ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS segment_latency_hist (
    ts              TIMESTAMPTZ NOT NULL,
    node_id         TEXT NOT NULL,
    side            SMALLINT NOT NULL,
    h_0_2           INT DEFAULT 0,
    h_2_5           INT DEFAULT 0,
    h_5_10          INT DEFAULT 0,
    h_10_15         INT DEFAULT 0,
    h_15_20         INT DEFAULT 0,
    h_20_30         INT DEFAULT 0,
    h_30_50         INT DEFAULT 0,
    h_50_75         INT DEFAULT 0,
    h_75_100        INT DEFAULT 0,
    h_100_150       INT DEFAULT 0,
    h_150_250       INT DEFAULT 0,
    h_250_500       INT DEFAULT 0,
    h_500_1000      INT DEFAULT 0,
    h_1000_2000     INT DEFAULT 0,
    h_2000_5000     INT DEFAULT 0,
    h_5000_10000    INT DEFAULT 0,
    h_10000_15000   INT DEFAULT 0,
    h_15000_20000   INT DEFAULT 0,
    h_20000_25000   INT DEFAULT 0,
    h_25000_30000   INT DEFAULT 0,
    h_30000_60000   INT DEFAULT 0,
    h_60000_120000  INT DEFAULT 0,
    h_120000_plus   INT DEFAULT 0,
    total_count     INT DEFAULT 0,
    failed_count    INT DEFAULT 0
);
SELECT create_hypertable('segment_latency_hist', 'ts', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_segment_latency_hist_node ON segment_latency_hist (node_id, ts DESC);

-- ── Preimage transfer latency ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS preimage_latency_hist (
    ts              TIMESTAMPTZ NOT NULL,
    node_id         TEXT NOT NULL,
    side            SMALLINT NOT NULL,
    h_0_2           INT DEFAULT 0,
    h_2_5           INT DEFAULT 0,
    h_5_10          INT DEFAULT 0,
    h_10_15         INT DEFAULT 0,
    h_15_20         INT DEFAULT 0,
    h_20_30         INT DEFAULT 0,
    h_30_50         INT DEFAULT 0,
    h_50_75         INT DEFAULT 0,
    h_75_100        INT DEFAULT 0,
    h_100_150       INT DEFAULT 0,
    h_150_250       INT DEFAULT 0,
    h_250_500       INT DEFAULT 0,
    h_500_1000      INT DEFAULT 0,
    h_1000_2000     INT DEFAULT 0,
    h_2000_5000     INT DEFAULT 0,
    h_5000_10000    INT DEFAULT 0,
    h_10000_15000   INT DEFAULT 0,
    h_15000_20000   INT DEFAULT 0,
    h_20000_25000   INT DEFAULT 0,
    h_25000_30000   INT DEFAULT 0,
    h_30000_60000   INT DEFAULT 0,
    h_60000_120000  INT DEFAULT 0,
    h_120000_plus   INT DEFAULT 0,
    total_count     INT DEFAULT 0,
    failed_count    INT DEFAULT 0
);
SELECT create_hypertable('preimage_latency_hist', 'ts', chunk_time_interval => INTERVAL '1 hour', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_preimage_latency_hist_node ON preimage_latency_hist (node_id, ts DESC);
