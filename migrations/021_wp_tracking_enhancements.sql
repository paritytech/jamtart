-- Migration 021: Enhance wp_tracking with node_id, gas, failure/discard reasons.
--
-- Adds columns needed by /grafana/wp-active, /grafana/wp/{hash}, and
-- /grafana/cores/:core_id (extended WP list).
--
-- node_id: which node first received this WP (from WorkPackageReceived event)
-- refine_gas_used: total gas from Refined event (SUM of costs[].total.gas_used)
-- failure_reason: from WorkPackageFailed event reason field
-- discard_reason: from GuaranteeDiscarded event via guarantee_convergence wp_hash mapping

ALTER TABLE wp_tracking ADD COLUMN IF NOT EXISTS node_id TEXT;
ALTER TABLE wp_tracking ADD COLUMN IF NOT EXISTS refine_gas_used BIGINT;
ALTER TABLE wp_tracking ADD COLUMN IF NOT EXISTS failure_reason TEXT;
ALTER TABLE wp_tracking ADD COLUMN IF NOT EXISTS discard_reason TEXT;

-- Partial index for wp-active queries: matches the exact WHERE clause
-- of "in-flight WPs" queries. Only indexes rows that haven't completed
-- or failed — keeps the index small and fast.
CREATE INDEX IF NOT EXISTS idx_wp_tracking_active
    ON wp_tracking (first_seen DESC)
    WHERE distributed_at IS NULL AND failed_at IS NULL;
