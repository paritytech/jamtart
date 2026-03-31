-- Add indexes on continuous aggregates and convergence tables to speed up
-- Grafana endpoint queries. Continuous aggregates only had TimescaleDB's
-- default bucket index — queries filtering by event_type, service_id, or
-- node_id were doing sequential scans after bucket range narrowing.

-- ============================================================
-- Tier 1: _1m continuous aggregates — (event_type, bucket DESC)
-- Speeds up ~15 Grafana endpoints that query all_event_stats_1m
-- with: WHERE bucket >= $1 AND bucket < $2 AND event_type = ANY($3)
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_status_counts_1m_et
    ON status_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_connection_counts_1m_et
    ON connection_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_block_counts_1m_et
    ON block_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_low_counts_1m_et
    ON ticket_low_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_wp_pipeline_counts_1m_et
    ON wp_pipeline_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_block_distribution_counts_1m_et
    ON block_distribution_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_counts_1m_et
    ON ticket_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_sending_counts_1m_et
    ON guarantee_sending_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_receiving_counts_1m_et
    ON guarantee_receiving_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_shard_counts_1m_et
    ON shard_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_assurance_counts_1m_et
    ON assurance_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_bundle_counts_1m_et
    ON bundle_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_segment_counts_1m_et
    ON segment_counts_1m (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_preimage_counts_1m_et
    ON preimage_counts_1m (event_type, bucket DESC);

-- assurance_convergence: only had (slot DESC), but queries filter by first_distributed_at
CREATE INDEX IF NOT EXISTS idx_assurance_convergence_time
    ON assurance_convergence (first_distributed_at);

-- ============================================================
-- Tier 2: _1h continuous aggregates — (event_type, bucket DESC)
-- Same pattern for all_event_stats_1h (time ranges > 30 days)
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_status_counts_1h_et
    ON status_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_connection_counts_1h_et
    ON connection_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_block_counts_1h_et
    ON block_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_low_counts_1h_et
    ON ticket_low_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_wp_pipeline_counts_1h_et
    ON wp_pipeline_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_block_distribution_counts_1h_et
    ON block_distribution_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_counts_1h_et
    ON ticket_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_sending_counts_1h_et
    ON guarantee_sending_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_guarantee_receiving_counts_1h_et
    ON guarantee_receiving_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_shard_counts_1h_et
    ON shard_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_assurance_counts_1h_et
    ON assurance_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_bundle_counts_1h_et
    ON bundle_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_segment_counts_1h_et
    ON segment_counts_1h (event_type, bucket DESC);
CREATE INDEX IF NOT EXISTS idx_preimage_counts_1h_et
    ON preimage_counts_1h (event_type, bucket DESC);

-- service_stats_1m: queries filter by service_id
CREATE INDEX IF NOT EXISTS idx_service_stats_1m_svc
    ON service_stats_1m (service_id, bucket DESC);

-- node_stats_1m: per-node drill-down queries (1024 nodes, huge selectivity gain)
CREATE INDEX IF NOT EXISTS idx_node_stats_1m_node
    ON node_stats_1m (node_id, bucket DESC);

-- ============================================================
-- Tier 3: Raw 30s count tables — write cost tradeoff
-- These receive COPY BINARY every 5s but have 3-day retention.
-- ============================================================

-- all_core_stats_1m queries raw tables with core filter (partial: many rows have NULL core)
CREATE INDEX IF NOT EXISTS idx_guarantee_sending_counts_core
    ON guarantee_sending_counts (core, event_type, bucket DESC) WHERE core IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_segment_counts_core
    ON segment_counts (core, event_type, bucket DESC) WHERE core IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_wp_pipeline_counts_core
    ON wp_pipeline_counts (core, event_type, bucket DESC) WHERE core IS NOT NULL;

-- sync_timeline queries status_counts by event_type without node_id
CREATE INDEX IF NOT EXISTS idx_status_counts_et
    ON status_counts (event_type, bucket DESC) WHERE slot IS NOT NULL;

-- connections_timeline queries connection_counts by event_type without node_id
CREATE INDEX IF NOT EXISTS idx_connection_counts_et
    ON connection_counts (event_type, bucket DESC);
