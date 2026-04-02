//! CLI feature flags to selectively disable ingestion pipeline subsystems.
//! Used for memory debugging — disable subsystems one-by-one to isolate leaks.

/// Feature flags controlling which ingestion pipeline subsystems are active.
/// All flags default to `false` (all subsystems enabled).
#[derive(Clone, Copy, Debug, Default)]
pub struct FeatureFlags {
    /// Skip enricher cross-event correlation (core, services, wp_hash propagation)
    pub disable_enricher: bool,
    /// Skip slot tracker (block propagation convergence)
    pub disable_slot_tracker: bool,
    /// Skip work package pipeline tracking
    pub disable_wp_tracker: bool,
    /// Skip guarantee + assurance convergence tracking + header_hash_lookup
    pub disable_convergence: bool,
    /// Skip DA tracker + DA latency tracker
    pub disable_da_tracker: bool,
    /// Skip pre-aggregated event counters
    pub disable_event_counter: bool,
    /// Skip WebSocket broadcast (no real-time events to WS clients)
    pub disable_ws_broadcast: bool,
    /// Skip batch writer entirely (no raw event persistence, no channel/workers)
    pub disable_db_writes: bool,
    /// Skip metrics tracker task (no in-memory metrics snapshots)
    pub disable_metrics_tracker: bool,
    /// Skip on-chain stats ingestion
    pub disable_onchain: bool,
    /// Skip cache warming task
    pub disable_cache_warmer: bool,
}

impl FeatureFlags {
    /// Log all disabled subsystems at startup.
    pub fn log_disabled(&self) {
        let flags = [
            (self.disable_enricher, "enricher"),
            (self.disable_slot_tracker, "slot-tracker"),
            (self.disable_wp_tracker, "wp-tracker"),
            (self.disable_convergence, "convergence"),
            (self.disable_da_tracker, "da-tracker"),
            (self.disable_event_counter, "event-counter"),
            (self.disable_ws_broadcast, "ws-broadcast"),
            (self.disable_db_writes, "db-writes"),
            (self.disable_metrics_tracker, "metrics-tracker"),
            (self.disable_onchain, "onchain"),
            (self.disable_cache_warmer, "cache-warmer"),
        ];
        let disabled: Vec<&str> = flags.iter().filter(|(f, _)| *f).map(|(_, n)| *n).collect();
        if disabled.is_empty() {
            tracing::info!("Feature flags: all subsystems ENABLED");
        } else {
            tracing::warn!("Feature flags: DISABLED subsystems: {}", disabled.join(", "));
        }
    }
}
