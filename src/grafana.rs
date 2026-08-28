//! Grafana integration endpoints. Exposes time-series and aggregate query routes
//! consumed by Grafana dashboards for network-wide telemetry visualization.
//!
//! OpenAPI spec is auto-generated from these annotations and served at
//! `GET /api/docs/openapi.json`.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use utoipa::{IntoParams, OpenApi};

use crate::api::ApiState;
use crate::grafana_types::*;
use crate::onchain_types::*;

#[derive(OpenApi)]
#[openapi(
    paths(
        timeseries,
        stats,
        cores_summary,
        core_detail,
        blocks_convergence,
        blocks_contents,
        services,
        services_timeseries,
        nodes,
        node_stats,
        node_stats_aggregate,
        db_stats,
        bottlenecks,
        wp_funnel,
        guarantee_convergence,
        guarantee_convergence_detail,
        assurance_convergence,
        assurance_convergence_senders,
        da_stats,
        shard_latency,
        wp_funnel_timeseries,
        bottlenecks_timeseries,
        event_types,
        events,
        guarantee_discards,
        // Phase 3
        failure_rates,
        sync_timeline,
        connections_timeline,
        guarantees,
        guarantees_by_guarantor,
        wp_stats,
        validators_cores,
        network_health,
        // Phase 4
        wp_active,
        wp_detail,
        wp_batch,
        blocks_summary,
        core_metrics,
        core_validators,
        // Phase 5
        execution_metrics,
        onchain_cores_summary,
        onchain_cores_timeseries,
        onchain_core_detail,
        onchain_services_summary,
        onchain_services_timeseries,
        onchain_service_detail,
        onchain_validators_summary,
        onchain_validators_timeseries,
        onchain_validator_detail,
        // Validator profiling
        validator_profiling,
        validator_profiling_timeseries,
        // DA latency
        bundle_latency,
        segment_latency,
        preimage_latency,
    ),
    components(schemas(
        TimeseriesRow,
        StatsResponse,
        CoreSummary,
        CoreDetail,
        WpTrackingRow,
        BlockConvergenceRow,
        BlockContentsRow,
        ServiceRow,
        ServiceTimeseriesRow,
        NodeRow,
        NodeStatsRow,
        NodeStatsAggregateRow,
        DbStatsResponse,
        TableSize,
        RowCount,
        CompressionInfo,
        BottlenecksResponse,
        StageTiming,
        Percentiles,
        WpFunnelResponse,
        GuaranteeConvergenceSlotRow,
        GuaranteeConvergenceDetailRow,
        AssuranceConvergenceRow,
        AssuranceConvergenceSenderRow,
        ConvergenceTimeseriesRow,
        DaStatsRow,
        ShardLatencyRow,
        WpFunnelTimeseriesRow,
        BottlenecksTimeseriesRow,
        EventRow,
        GuaranteeDiscardRow,
        crate::event_type_meta::EventTypeMeta,
        OnchainCoreSummary,
        OnchainCoreTimeseries,
        OnchainCoreTimeseriesAgg,
        OnchainCoreDetail,
        OnchainServiceSummary,
        OnchainServiceTimeseries,
        OnchainServiceDetail,
        OnchainValidatorSummary,
        OnchainValidatorTimeseries,
        OnchainValidatorTimeseriesAgg,
        OnchainValidatorDetail,
        // Phase 0
        EventsSearchResponse,
        PaginationMeta,
        // Phase 3
        FailureRatesResponse,
        FailureOverall,
        FailureCategory,
        FailureByNode,
        RecentFailure,
        SyncTimelineRow,
        ConnectionsTimelineResponse,
        ConnectionsBucket,
        ConnectionHealthStats,
        GuaranteesResponse,
        GuaranteeTotals,
        GuaranteeSuccessRates,
        GuarantorBreakdownResponse,
        GuarantorRow,
        WpStatsResponse,
        WpStageTotals,
        WpCoreCount,
        ValidatorCoreRow,
        NetworkHealthResponse,
        HealthComponent,
        HealthAlert,
        // Phase 4
        WpActiveResponse,
        WpActiveRow,
        WpActiveSummary,
        WpReachedCounts,
        WpStageDurations,
        FailureBreakdownEntry,
        WpDetailResponse,
        BlocksSummaryResponse,
        BlockTotals,
        ChainState,
        AuthoringByNode,
        CoreMetricsResponse,
        CoreValidatorsResponse,
        CoreValidatorRow,
        // Phase 5
        ExecutionMetricsResponse,
        ExecutionPhaseStats,
        ServiceExecutionRow,
        // Validator profiling
        ValidatorProfilingResponse,
        ValidatorProfilingRow,
        ValidatorProfilingTimeseriesRow,
        // DA latency
        BundleLatencyRow,
        SegmentLatencyRow,
        PreimageLatencyRow,
    )),
    tags(
        (name = "grafana", description = "Grafana dashboard API — time-series, aggregates, and metadata"),
        (name = "onchain", description = "On-chain statistics API — the chain's own per-block activity statistics for cores, services and validators")
    )
)]
pub struct GrafanaApiDoc;

pub fn router() -> Router<ApiState> {
    Router::new()
        .route("/timeseries", get(timeseries))
        .route("/stats", get(stats))
        .route("/cores", get(cores_summary))
        .route("/cores/:core_id", get(core_detail))
        .route("/blocks/convergence", get(blocks_convergence))
        .route("/blocks/contents", get(blocks_contents))
        .route("/services", get(services))
        .route("/services/timeseries", get(services_timeseries))
        .route("/nodes", get(nodes))
        .route("/node-stats", get(node_stats))
        .route("/node-stats-aggregate", get(node_stats_aggregate))
        .route("/db-stats", get(db_stats))
        .route("/bottlenecks", get(bottlenecks))
        .route("/wp-funnel", get(wp_funnel))
        .route("/guarantee-convergence", get(guarantee_convergence))
        .route(
            "/guarantee-convergence/detail",
            get(guarantee_convergence_detail),
        )
        .route("/assurance-convergence", get(assurance_convergence))
        .route(
            "/assurance-convergence/senders",
            get(assurance_convergence_senders),
        )
        .route("/da-stats", get(da_stats))
        .route("/shard-latency", get(shard_latency))
        .route("/bundle-latency", get(bundle_latency))
        .route("/segment-latency", get(segment_latency))
        .route("/preimage-latency", get(preimage_latency))
        .route("/wp-funnel-timeseries", get(wp_funnel_timeseries))
        .route("/bottlenecks-timeseries", get(bottlenecks_timeseries))
        .route("/event-types", get(event_types))
        .route("/events", get(events))
        .route("/guarantee-discards", get(guarantee_discards))
        // Phase 3: new endpoints
        .route("/failure-rates", get(failure_rates))
        .route("/sync-timeline", get(sync_timeline))
        .route("/connections-timeline", get(connections_timeline))
        .route("/guarantees", get(guarantees))
        .route("/guarantees/by-guarantor", get(guarantees_by_guarantor))
        .route("/wp-stats", get(wp_stats))
        .route("/validators/cores", get(validators_cores))
        .route("/network-health", get(network_health))
        // Phase 4: moderate endpoints
        .route("/wp-active", get(wp_active))
        .route("/wp/:wp_hash", get(wp_detail))
        .route("/wp/batch", post(wp_batch))
        .route("/blocks/summary", get(blocks_summary))
        .route("/cores/:core_id/metrics", get(core_metrics))
        .route("/cores/:core_id/validators", get(core_validators))
        // Validator profiling
        .route("/validator-profiling", get(validator_profiling))
        .route(
            "/validator-profiling-timeseries",
            get(validator_profiling_timeseries),
        )
        // Phase 5
        .route("/execution", get(execution_metrics))
        .nest("/onchain", onchain_router())
}

fn onchain_router() -> Router<ApiState> {
    Router::new()
        .route("/cores", get(onchain_cores_summary))
        .route("/cores/timeseries", get(onchain_cores_timeseries))
        .route("/cores/:core_id", get(onchain_core_detail))
        .route("/services", get(onchain_services_summary))
        .route("/services/timeseries", get(onchain_services_timeseries))
        .route("/services/:service_id", get(onchain_service_detail))
        .route("/validators", get(onchain_validators_summary))
        .route("/validators/timeseries", get(onchain_validators_timeseries))
        .route("/validators/:validator_idx", get(onchain_validator_detail))
}

/// Map sqlx errors to appropriate HTTP status codes.
/// Protocol errors (used for validation) → 400, everything else → 500.
fn map_sqlx_error(context: &str, e: sqlx::Error) -> StatusCode {
    if matches!(&e, sqlx::Error::Protocol(_)) {
        tracing::warn!("{context} bad request: {e}");
        StatusCode::BAD_REQUEST
    } else {
        tracing::error!("{context} error: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    }
}

// ── Query parameter structs ────────────────────────────────────────────

/// Parameters for time-series queries with flexible grouping and filtering.
#[derive(Deserialize, IntoParams)]
pub struct TimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Grouping column. Allowed: node_id, event_type, core
    pub group_by: Option<String>,
    /// Filter to a single node_id
    pub node: Option<String>,
    /// Comma-separated event type codes, group names, or event names. Supports Grafana {a,b} syntax.
    pub event_types: Option<String>,
    /// Filter to a single core index
    pub core: Option<i16>,
}

/// Common time range + optional filters used by most endpoints.
#[derive(Deserialize, IntoParams)]
pub struct TimeRangeQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Filter to a single node_id (or comma-separated list with Grafana {a,b} support)
    pub node: Option<String>,
    /// Filter to a single core index
    pub core: Option<i16>,
    /// Filter to a single event type code
    pub event_type: Option<i16>,
}

/// Parameters for service endpoints.
#[derive(Deserialize, IntoParams)]
pub struct ServiceQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Comma-separated service IDs (decimal or 0x hex). Supports Grafana {a,b} syntax.
    pub service: Option<String>,
}

/// Parameters for service timeseries endpoint.
#[derive(Deserialize, IntoParams)]
pub struct ServiceTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width (same values as /timeseries)
    pub interval: Option<String>,
    /// Comma-separated service IDs (decimal or 0x hex). Supports Grafana {a,b} syntax.
    pub service: Option<String>,
}

/// Parameters for event-types metadata endpoint.
#[derive(Deserialize, IntoParams)]
pub struct EventTypesParams {
    /// Filter to a single event group name (e.g. "blocks", "wp_pipeline", "failures")
    pub group: Option<String>,
}

/// Parameters for browsing individual telemetry events, with optional filtering
/// and pagination.
#[derive(Deserialize, IntoParams)]
pub struct EventsQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Comma-separated event type codes, group names, or event names. Optional — if omitted, returns all types.
    pub event_types: Option<String>,
    /// Maximum number of events to return (default: 500, max: 2000)
    pub limit: Option<i64>,
    /// Offset for pagination (default: 0)
    pub offset: Option<i64>,
    /// Filter to a single node_id
    pub node: Option<String>,
    /// Filter to a single core index
    pub core: Option<i16>,
    /// Filter to a single work package hash (hex-encoded, with or without `0x`)
    pub wp_hash: Option<String>,
}

/// Parameters for convergence endpoints that support both per-slot and histogram modes.
#[derive(Deserialize, IntoParams)]
pub struct ConvergenceQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width for histogram mode. When present, returns percentile timeseries from merged histograms instead of per-slot rows.
    pub interval: Option<String>,
}

/// Parameters for assurance convergence senders query.
#[derive(Deserialize, IntoParams)]
pub struct AssuranceConvergenceSendersQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Filter to a single block anchor (hex-encoded)
    pub anchor: Option<String>,
    /// Filter to a single sender node_id
    pub node: Option<String>,
    /// Bucket width. When present, the response becomes one percentile row per bucket instead of one row per sender, and the anchor filter is ignored.
    pub interval: Option<String>,
}

/// Parameters for guarantee convergence detail query.
#[derive(Deserialize, IntoParams)]
pub struct GuaranteeConvergenceDetailQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Filter to a single core index
    pub core: Option<i16>,
    /// Filter to a single work package hash (hex-encoded)
    pub wp_hash: Option<String>,
}

/// Parameters for WP pipeline timeseries queries (funnel + bottlenecks).
#[derive(Deserialize, IntoParams)]
pub struct WpTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Filter to a single core index
    pub core: Option<i16>,
}

/// Parameters for guarantee discards query.
#[derive(Deserialize, IntoParams)]
pub struct GuaranteeDiscardsQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width (same values as /timeseries)
    pub interval: Option<String>,
}

/// Parameters for validator profiling summary query.
#[derive(Deserialize, IntoParams)]
pub struct ValidatorProfilingQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Filter to a single core index
    pub core: Option<i16>,
    /// Maximum number of nodes to return.
    /// `network_avg_total_ms` still reflects all nodes regardless of limit.
    pub limit: Option<u32>,
    /// Sort order for `avg_total_ms`: `desc` (slowest first, default) or `asc` (fastest first).
    pub sort: Option<String>,
}

/// Parameters for validator profiling timeseries query.
#[derive(Deserialize, IntoParams)]
pub struct ValidatorProfilingTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Filter to a single core index
    pub core: Option<i16>,
    /// Filter to a single node (hex-encoded 32-byte public key). When provided,
    /// returns every bucket for that guarantor only. When omitted, returns the 20
    /// guarantors with the highest average total pipeline duration over the range.
    pub node: Option<String>,
}

// ── Helper functions ───────────────────────────────────────────────────

/// Strip Grafana multi-select curly-brace wrapper: `{a,b}` → `a,b`.
fn strip_grafana_braces(s: &str) -> &str {
    s.strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .unwrap_or(s)
}

/// Parse comma-separated validator indices, stripping Grafana curly-brace wrapper.
fn parse_validator_indices(s: &str) -> Vec<i16> {
    strip_grafana_braces(s)
        .split(',')
        .filter_map(|v| v.trim().parse().ok())
        .collect()
}

/// Parse comma-separated node names, stripping Grafana curly-brace wrapper.
fn parse_node_list(s: &str) -> Vec<String> {
    strip_grafana_braces(s)
        .split(',')
        .map(|s| s.trim().to_string())
        .collect()
}

// ── Handlers ───────────────────────────────────────────────────────────

/// Telemetry event counts over time, grouped by event type, core or node.
///
/// One row per time bucket and group value. Exactly one of `event_type`, `core`
/// or `node_id` is populated, following the `group_by` parameter (default
/// `event_type`; also accepts `core` and `node_id`/`node`). Counts are
/// pre-aggregated and the bucket resolution is chosen from the interval: 30 s
/// for intervals below one minute, 1 min below one hour, 1 h from there on;
/// core grouping and the `core` filter always read 1-minute resolution.
/// Requesting a finer interval than the available resolution adds no detail,
/// and older ranges only have coarser resolution left — from roughly 3 days
/// back the finest is 1 min, from roughly 30 days back 1 h. Interval values
/// outside the supported set (6 s up to 1 d) snap to the nearest supported one.
///
/// The `event_types` parameter takes a comma-separated mix of numeric JIP-3
/// event IDs, canonical event names such as `Authored`, and event group names
/// (the `wp_pipeline` group, for instance), with Grafana `{a,b}` multi-select
/// syntax; entries it does not recognise are ignored. The `node` filter has no
/// effect once `group_by=core` or a `core` filter is in play, since only
/// core-attributed events are counted there.
///
/// Answers: how did telemetry volume develop over the range, and which event
/// types, cores or nodes account for it?
#[utoipa::path(
    get,
    path = "/api/grafana/timeseries",
    params(TimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket and group value, ascending by bucket, each with the bucket start, the event count and the single populated grouping field.", body = [TimeseriesRow]),
        (status = 400, description = "`group_by` is not one of `event_type`, `core`, `node_id`"),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn timeseries(
    Query(q): Query<TimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    let event_types: Option<Vec<i16>> = q
        .event_types
        .map(|s| crate::event_type_meta::expand_event_types(&s))
        .filter(|v| !v.is_empty());

    state
        .store
        .grafana_timeseries(
            q.start,
            q.end,
            interval,
            q.group_by.as_deref(),
            q.node.as_deref(),
            event_types.as_deref(),
            q.core,
        )
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/timeseries", e))
}

/// Headline network counters for a dashboard summary row.
///
/// Over the requested range: how many GuaranteeBuilt(105), WorkPackageFailed(92)
/// and WorkPackageReceived(94) reports arrived, plus `slot_events` — the largest
/// number of Authored(42) reports seen in any single minute of the range, so the
/// busiest minute of block authoring rather than a total.
///
/// The remaining fields describe the present moment and ignore the time range:
/// how many nodes are currently connected to the telemetry collector, telemetry
/// events and BestBlockChanged(11) reports per second over the last 10 s, the
/// highest slot numbers seen in BestBlockChanged(11) and
/// FinalizedBlockChanged(12) reports so far, and the number of open node
/// connections. Everything except the connected-node count is absent when live
/// metrics collection is switched off.
///
/// Answers: is the network alive right now, and how much block, guarantee and
/// work-package activity did the range see?
#[utoipa::path(
    get,
    path = "/api/grafana/stats",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with the range's authoring, guarantee, failure and work-package counters plus the live rate, slot and connection fields when available.", body = StatsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn stats(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut result = state
        .store
        .grafana_stats(q.start, q.end)
        .await
        .map_err(|e| map_sqlx_error("grafana/stats", e))?;

    // Merge real-time data from LiveCounters (events_per_sec, blocks_per_sec, slots)
    if let Some(ref tracker) = state.metrics_tracker {
        let lc = tracker.live_counters();
        let last_10s = lc.sum_last_n_seconds(10);
        let active_nodes = state.telemetry_server.connection_count();
        result.events_per_sec_10s = Some(last_10s.events as f64 / 10.0);
        result.blocks_per_sec_10s = Some(last_10s.blocks as f64 / 10.0);
        result.best_slot = Some(lc.latest_slot());
        result.finalized_slot = Some(lc.finalized_slot());
        result.active_nodes = Some(active_nodes);
    }

    Ok(Json(result))
}

/// Per-core work-package activity: receptions, guarantees and failures.
///
/// One row per core that saw any core-attributed activity in the range, counting
/// WorkPackageReceived(94), GuaranteeBuilt(105) and WorkPackageFailed(92). These
/// are event counts, not distinct work packages — every guarantor of a work
/// package reports its own reception and guarantee, so one work package
/// contributes once per guarantor. `last_activity` is the newest work package
/// first observed on the core since the range start and is not capped at the
/// range end.
///
/// Answers: which cores are carrying work-package load, and where are work
/// packages failing?
#[utoipa::path(
    get,
    path = "/api/grafana/cores",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array of per-core rows, ascending by core index, each with the core's work-package reception, guarantee and failure counts plus its last observed work-package activity.", body = [CoreSummary]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn cores_summary(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_cores_summary(q.start, q.end, q.core)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/cores", e))
}

/// One core's work-package activity, with its most recent work-package timelines.
///
/// The same counters as `/cores` for this core — WorkPackageReceived(94),
/// GuaranteeBuilt(105) and WorkPackageFailed(92) event counts — plus up to 100
/// work packages first observed on the core in the range, newest first, each with
/// its pipeline timeline from reception through guarantee distribution or failure.
/// A core with no activity in the range comes back with zero counters and an empty
/// list.
///
/// Answers: what is this one core doing, and how far did its most recent work
/// packages get?
#[utoipa::path(
    get,
    path = "/api/grafana/cores/{core_id}",
    params(
        ("core_id" = i16, Path, description = "Core index (0-based)"),
        TimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Single object with the core's activity counters and up to 100 of its recent work-package pipeline timelines, newest first.", body = CoreDetail),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn core_detail(
    Path(core_id): Path<i16>,
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_core_detail(q.start, q.end, core_id)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/cores/detail", e))
}

/// How fast a newly authored block reaches the rest of the network, per slot.
///
/// One row per slot and per block event. Every offset is measured from
/// Authored(42) on the block's author to the same slot's event on each other
/// node, and the offsets of all reporting nodes are pooled before the
/// percentiles are taken, so they are percentiles over nodes. The `event_type`
/// filter picks which step to look at — BestBlockChanged(11),
/// FinalizedBlockChanged(12), Authoring(40), Authored(42) or Importing(43);
/// without it every step observed for the slot is returned. Authoring(40) and
/// Authored(42) come from the author itself, so their offsets sit at or below
/// zero. A slot whose author never reported Authored(42) has no rows at all,
/// and a slot's rows keep being refined while late reports for it arrive.
///
/// Answers: how quickly does a new block propagate across the network, and in
/// which slots does propagation degrade?
#[utoipa::path(
    get,
    path = "/api/grafana/blocks/convergence",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array of per-slot rows, ascending by slot and then by event type, each carrying the propagation-offset percentiles across the nodes that reported that block event for the slot.", body = [BlockConvergenceRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn blocks_convergence(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_blocks_convergence(q.start, q.end, q.event_type)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/blocks/convergence", e))
}

/// What each authored block contained, one row per authored block.
///
/// Each row is one Authored(42) report from the node that authored the block:
/// the slot, the author, and the block outline it reported — how many
/// guarantees, assurances, preimages, tickets and dispute verdicts the block
/// carried, plus the block's size in bytes. Built from recent raw events, which
/// are retained for about an hour, so older parts of a range come back empty.
///
/// Answers: how full are the blocks being authored, and which extrinsic types
/// are actually making it on chain?
#[utoipa::path(
    get,
    path = "/api/grafana/blocks/contents",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array of rows, one per authored block, ascending by slot, each with the author and the per-extrinsic-type counts from the block outline. Only covers the ~1 hour raw-event retention window.", body = [BlockContentsRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn blocks_contents(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_blocks_contents(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/blocks/contents", e))
}

/// What each service did in the range, and the gas it spent doing it.
///
/// One row per service that any node attributed work to. The counters are
/// event attributions, not distinct work packages or blocks: a service is
/// counted once for every WorkPackageReceived(94), Authorized(95),
/// Refined(101) and BlockExecuted(47) report that names it, and since each
/// guarantor and each node executing a block reports for itself, one work
/// package or block contributes once per reporting node. The gas figures are
/// the reported costs of the service's own code — the is-authorized call for
/// Authorized(95), the per-work-item refine calls for Refined(101) and the
/// per-service accumulate calls for BlockExecuted(47). Service IDs come back
/// zero-padded hex; the `service` filter accepts decimal or `0x` hex IDs and
/// Grafana `{a,b}` multi-select syntax.
///
/// Answers: which services are consuming the network's compute, and how much
/// gas does each spend on authorization, refinement and accumulation?
#[utoipa::path(
    get,
    path = "/api/grafana/services",
    params(ServiceQuery),
    responses(
        (status = 200, description = "Array with one row per service, ascending by service ID, each with the service's work-package, authorization, refinement and accumulation counters and the gas consumed in each of the three phases.", body = [ServiceRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn services(
    Query(q): Query<ServiceQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let services: Option<Vec<DbServiceId>> = q.service.map(|s| DbServiceId::parse_list(&s));
    state
        .store
        .grafana_services(q.start, q.end, services.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/services", e))
}

/// Per-service work-package load and gas usage over time.
///
/// The same per-service attribution as `/services`, split into buckets of the
/// requested `interval` (default 1 minute, snapped to the nearest supported
/// width from 6s up to 1d). Each bucket carries the WorkPackageReceived(94)
/// count for the service and the gas its code used in each phase —
/// Authorized(95) for authorization, Refined(101) for refinement,
/// BlockExecuted(47) for accumulation. The counts behind it have one-minute
/// resolution, so 1 minute is the finest interval that carries real detail;
/// shorter buckets land everything on the minute boundaries. Service IDs come
/// back zero-padded hex and the `service` filter takes Grafana `{a,b}`
/// multi-select syntax.
///
/// Answers: how does each service's work-package load and gas consumption
/// evolve over time?
#[utoipa::path(
    get,
    path = "/api/grafana/services/timeseries",
    params(ServiceTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket and service, ascending by bucket and then service ID, each with the bucket's work-package count and its authorization, refinement and accumulation gas.", body = [ServiceTimeseriesRow]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn services_timeseries(
    Query(q): Query<ServiceTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    let services: Option<Vec<DbServiceId>> = q.service.map(|s| DbServiceId::parse_list(&s));

    state
        .store
        .grafana_services_timeseries(q.start, q.end, interval, services.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/services/timeseries", e))
}

/// Every node that has ever reported telemetry, with its identity and current session state.
///
/// One row per node, whether or not it is still reporting. The connection state
/// (`is_connected`, `connected_at`, `disconnected_at`, `last_seen_at`) describes the
/// node's telemetry session with this collector, not its JAM peer connections — for
/// those see `/connections-timeline`. Identity, implementation and protocol
/// parameters come from the JIP-3 node information message sent at handshake, and
/// `total_event_count` covers every event the node has reported across all of its
/// sessions. Nodes currently reporting come first, then the most recently heard from.
/// Takes no time range: the answer is always the present state.
///
/// Answers: which nodes are reporting right now, what software are they running,
/// and when was each one last heard from?
#[utoipa::path(
    get,
    path = "/api/grafana/nodes",
    responses(
        (status = 200, description = "Array with one row per known node, the ones currently reporting first and then the most recently seen, each with the node's identity, implementation, session timestamps and lifetime event count.", body = [NodeRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn nodes(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_nodes()
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/nodes", e))
}

/// Individual node status snapshots, exactly as reported in Status(10).
///
/// One row per Status(10) event. Nodes emit one roughly every 2 seconds, so an
/// unfiltered query over a wide range returns a great many rows — use
/// `/node-stats-aggregate` when a trend is enough. Each row carries the reporting
/// node's peer counts, its availability-store shard holdings and preimage pool, and
/// how many guarantees its guarantee pool held per core, summarised as the minimum,
/// maximum and mean across cores plus the number of cores holding none. The `node`
/// parameter accepts a comma-separated list with Grafana `{a,b}` multi-select
/// syntax; without it, every reporting node is included.
///
/// Answers: what did a specific node's peer count, availability-store occupancy and
/// guarantee pool look like at each moment?
#[utoipa::path(
    get,
    path = "/api/grafana/node-stats",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array of one row per Status(10) report in the range, ascending by time, each with the reporting node's peer counts, shard and preimage holdings and per-core guarantee-pool summary.", body = [NodeStatsRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn node_stats(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let nodes: Option<Vec<String>> = q.node.map(|n| parse_node_list(&n));
    state
        .store
        .grafana_node_stats(q.start, q.end, nodes.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/node-stats", e))
}

/// Node status metrics from Status(10), condensed into 1-minute buckets.
///
/// The same measurements as `/node-stats`, pre-aggregated to one row per minute so
/// that long ranges stay cheap. Without a `node` filter each row is network-wide:
/// the mean of the per-node means, and the lowest and highest value any reporting
/// node showed in that minute. With a `node` filter each row is one node in one
/// minute. `status_count` is how many Status(10) reports went into the row, which
/// distinguishes a fully reported minute from a partial one. One minute is the
/// finest resolution available here — use `/node-stats` for individual reports. The
/// `node` parameter accepts comma-separated IDs with Grafana `{a,b}` multi-select
/// syntax.
///
/// Answers: how are peer counts, availability-store occupancy and guarantee-pool
/// depth trending, network-wide or for particular nodes?
#[utoipa::path(
    get,
    path = "/api/grafana/node-stats-aggregate",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array of rows, one per minute bucket network-wide, or one per minute and node when a node filter is given, ascending by time, each with mean/lowest/highest peer, shard, preimage and guarantee-pool figures and the number of Status(10) reports behind them.", body = [NodeStatsAggregateRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn node_stats_aggregate(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let nodes: Option<Vec<String>> = q.node.map(|n| parse_node_list(&n));
    state
        .store
        .grafana_node_stats_aggregate(q.start, q.end, nodes.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/node-stats-aggregate", e))
}

/// Storage state of the telemetry collector itself — an operational endpoint.
///
/// Reports jam-tart's own TimescaleDB state for running the collector; it says
/// nothing about the JAM network. For a fixed set of tables it returns the byte
/// breakdown (total, table, index, toast) from `hypertable_detailed_size()`, row
/// counts — estimated via `approximate_row_count()` on the hypertables, exact on
/// the small tables such as `wp_tracking`, `slot_convergence` and `nodes` — and
/// the chunk compression totals before and after compression, from
/// `chunk_compression_stats()`, for the raw event and node status hypertables.
/// It takes no parameters and always describes the current state.
///
/// Answers: how much storage is the collector using, and is compression keeping
/// up?
#[utoipa::path(
    get,
    path = "/api/grafana/db-stats",
    responses(
        (status = 200, description = "Single object with per-table byte sizes, row counts and compression figures for the collector's own storage.", body = DbStatsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn db_stats(State(state): State<ApiState>) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_db_stats()
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/db-stats", e))
}

/// Where time goes inside the guarantor work-package pipeline.
///
/// Median and 95th-percentile durations of each stage a work package passes
/// through on its guarantors: authorize (WorkPackageReceived(94) →
/// Authorized(95)), refine (→ Refined(101)), report (→ WorkReportBuilt(102)),
/// guarantee (→ GuaranteeBuilt(105)), distribute (→ GuaranteesDistributed(109)),
/// plus the total from reception to distribution. Work packages are selected by
/// when they were first observed anywhere in the network; one that never reached
/// distribution contributes the time up to its last observed pipeline event.
/// `failure_rate` is the share of them for which a WorkPackageFailed(92) was
/// reported. The optional `core` filter narrows to the work packages assigned to
/// one core.
///
/// Answers: which pipeline stage dominates work-package latency, and how often do
/// work packages fail outright?
#[utoipa::path(
    get,
    path = "/api/grafana/bottlenecks",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array holding a single object: per-stage median and p95 durations in milliseconds, the number of work packages considered, how many of them failed, and the resulting failure rate.", body = [BottlenecksResponse]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn bottlenecks(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_bottlenecks(q.start, q.end, q.core)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/bottlenecks", e))
}

/// How many work packages reached each stage of the guarantor pipeline.
///
/// Counts over the work packages first observed in the range: received
/// (WorkPackageReceived(94)), authorized (Authorized(95)), refined (Refined(101)),
/// report_built (WorkReportBuilt(102)), guarantee_built (GuaranteeBuilt(105)),
/// distributed (GuaranteesDistributed(109)) and failed (WorkPackageFailed(92)).
/// A work package counts towards a stage as soon as any of its guarantors reported
/// that stage, so the gap between two consecutive counts is the number that
/// stopped progressing there. `distributed` means the primary guarantor finished
/// sending the guarantee out.
///
/// Answers: at which pipeline stage do work packages get stuck or lost?
#[utoipa::path(
    get,
    path = "/api/grafana/wp-funnel",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with one work-package count per pipeline stage for the whole range.", body = WpFunnelResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_funnel(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_wp_funnel(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp-funnel", e))
}

/// How fast guarantees reach the validator set, summarised per slot.
///
/// One row per slot. The latency measured is GuaranteeBuilt(105) on the
/// guarantor until GuaranteeReceived(112) on each validator that received the
/// guarantee. Latencies for every guarantee built in the slot are pooled before
/// the percentiles are taken, so they are true cross-core percentiles and not an
/// average of per-guarantee ones.
///
/// Answers: how quickly does a guarantee propagate to the rest of the validator
/// set, and in which slots does propagation degrade?
#[utoipa::path(
    get,
    path = "/api/grafana/guarantee-convergence",
    params(ConvergenceQuery),
    responses(
        (status = 200, description = "Array of per-slot rows, ascending by slot, each carrying the propagation-latency percentiles for that slot. With `interval`: one row per time bucket instead, with percentiles taken over the latencies of all guarantees in the bucket.", body = [GuaranteeConvergenceSlotRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantee_convergence(
    Query(q): Query<ConvergenceQuery>,
    State(state): State<ApiState>,
) -> Result<Response, StatusCode> {
    if let Some(interval) = &q.interval {
        state
            .store
            .grafana_guarantee_convergence_hist(q.start, q.end, interval)
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/guarantee-convergence", e))
    } else {
        state
            .store
            .grafana_guarantee_convergence(q.start, q.end)
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/guarantee-convergence", e))
    }
}

/// Per-work-report guarantee propagation — drill-down behind the per-slot summary.
///
/// One row per work report, identified by its work-report hash. Each row names
/// the guarantor that emitted GuaranteeBuilt(105) and the core the report was
/// built for, and gives the spread of latencies until GuaranteeReceived(112) on
/// the validators that received it. Optional `core` and `wp_hash` filters narrow
/// the drill-down.
///
/// Answers: which individual guarantees propagated slowly, and which guarantor
/// and core produced them?
#[utoipa::path(
    get,
    path = "/api/grafana/guarantee-convergence/detail",
    params(GuaranteeConvergenceDetailQuery),
    responses(
        (status = 200, description = "Array of rows, one per work report, ascending by slot, each with its guarantor, core and GuaranteeBuilt(105) → GuaranteeReceived(112) latency percentiles. Narrowed by the optional core and work-package-hash filters.", body = [GuaranteeConvergenceDetailRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantee_convergence_detail(
    Query(q): Query<GuaranteeConvergenceDetailQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let wp_hash_bytes = q.wp_hash.as_deref().and_then(|h| hex::decode(h).ok());
    state
        .store
        .grafana_guarantee_convergence_detail(q.start, q.end, q.core, wp_hash_bytes.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/guarantee-convergence/detail", e))
}

/// How fast assurances reach the validator set, summarised per assurance anchor.
///
/// One row per anchor — the header hash of the block an availability statement
/// refers to. Reception latency runs from DistributingAssurance(126) on a sender
/// to AssuranceReceived(131) on each validator that received that sender's
/// assurance; the latencies of every sender for the anchor are pooled before the
/// percentiles are taken. A second set of percentiles gives the distribution
/// start spread — how much later the remaining validators began distributing for
/// this anchor than the first one did. Assurances only count towards
/// availability while the report is still pending, a 5-slot (30 s) window.
///
/// Answers: do assurances for a block reach the validator set well inside the
/// 5-slot availability window, and which anchors converge slowly?
#[utoipa::path(
    get,
    path = "/api/grafana/assurance-convergence",
    params(ConvergenceQuery),
    responses(
        (status = 200, description = "Array of per-anchor rows, ascending by slot, each with the pooled DistributingAssurance(126) → AssuranceReceived(131) percentiles and the distribution start spread for that anchor. With `interval`: one row per time bucket instead, with percentiles over the latencies of all assurances in the bucket (approximate, latency-histogram based).", body = [AssuranceConvergenceRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn assurance_convergence(
    Query(q): Query<ConvergenceQuery>,
    State(state): State<ApiState>,
) -> Result<Response, StatusCode> {
    if let Some(interval) = &q.interval {
        state
            .store
            .grafana_assurance_convergence_hist(q.start, q.end, interval)
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/assurance-convergence", e))
    } else {
        state
            .store
            .grafana_assurance_convergence(q.start, q.end)
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/assurance-convergence", e))
    }
}

/// Per-sender assurance propagation — drill-down behind the per-anchor summary.
///
/// One row per (anchor, sender): the validator that emitted
/// DistributingAssurance(126) for that anchor, how many validators received its
/// assurance, and the spread of latencies until AssuranceReceived(131) on them.
/// Optional `anchor` and `node` filters isolate a single block or a single
/// suspect validator.
///
/// Answers: which validator's assurances propagate slowly, and to how many of
/// its peers?
#[utoipa::path(
    get,
    path = "/api/grafana/assurance-convergence/senders",
    params(AssuranceConvergenceSendersQuery),
    responses(
        (status = 200, description = "Array of rows, one per anchor and sender, ascending by the time that sender started distributing, each with its receiving-validator count and DistributingAssurance(126) → AssuranceReceived(131) latency percentiles. With `interval`: one row per time bucket instead, with percentiles over all sender latencies in the bucket, and the anchor filter no longer applies.", body = [AssuranceConvergenceSenderRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn assurance_convergence_senders(
    Query(q): Query<AssuranceConvergenceSendersQuery>,
    State(state): State<ApiState>,
) -> Result<Response, StatusCode> {
    if let Some(interval) = &q.interval {
        state
            .store
            .grafana_assurance_convergence_senders_hist(q.start, q.end, interval, q.node.as_deref())
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/assurance-convergence/senders", e))
    } else {
        let anchor_bytes = q.anchor.as_deref().and_then(|h| hex::decode(h).ok());
        state
            .store
            .grafana_assurance_convergence_senders(
                q.start,
                q.end,
                anchor_bytes.as_deref(),
                q.node.as_deref(),
            )
            .await
            .map(|rows| Json(rows).into_response())
            .map_err(|e| map_sqlx_error("grafana/assurance-convergence/senders", e))
    }
}

/// Per-node data-availability activity: shard request counts, average shard
/// latency and shard inventory over the requested time range.
///
/// One row per node, totalled over the range. Shard work is counted from both
/// ends: requests the node made as an assurer fetching its shards
/// (SendingShardRequest(120), ShardRequestSent(123), ShardsTransferred(125)) and
/// requests it took in as a guarantor holding them (ReceivingShardRequest(121),
/// ShardRequestReceived(124)), alongside failures (ShardRequestFailed(122)) and
/// preimage announcement activity (PreimageAnnouncementFailed(190),
/// PreimageAnnounced(191), AnnouncedPreimageForgotten(192)). The two average
/// latencies cover the same two perspectives and include requests that ended in
/// failure, measured up to the failure.
///
/// Answers: which nodes carry the data-availability load, and which are slow or
/// failing at serving shards?
#[utoipa::path(
    get,
    path = "/api/grafana/da-stats",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array of per-node rows, busiest shard transferrer first, each with its shard and preimage event totals, sample-weighted average latency per perspective in milliseconds, and peak distinct-shard count.", body = [DaStatsRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn da_stats(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_da_stats(q.start, q.end, q.node.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/da-stats", e))
}

/// Shard transfer latency percentiles over time, seen from both ends of the
/// transfer.
///
/// One row per time bucket, with the two perspectives measured separately: the
/// assurer's round-trip from SendingShardRequest(120) to ShardsTransferred(125),
/// and the guarantor's time to take the request in, ReceivingShardRequest(121) to
/// ShardRequestReceived(124). Latencies from all reporting nodes are pooled per
/// bucket, so the percentiles are network-wide and approximate: values are
/// rounded up to a latency-bucket edge and saturate at 5 s. Requests that ended
/// in ShardRequestFailed(122) are included, measured up to the failure, and also
/// reported as a separate count.
///
/// Answers: is shard fetching slow because assurers are waiting on the network,
/// or because guarantors are slow to serve their shards?
#[utoipa::path(
    get,
    path = "/api/grafana/shard-latency",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket, each with approximate p50/p75/p95/p99/p100 latencies in milliseconds for the assurer round-trip and the guarantor request-intake side, their sample counts, and how many measurements ended in failure.", body = [ShardLatencyRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn shard_latency(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_shard_latency_raw(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/shard-latency", e))
}

/// Audit bundle recovery latency percentiles over time.
///
/// One row per time bucket. Auditors fetch erasure-coded shards from assurers
/// and reconstruct the original work-package bundle; each leg of that recovery is
/// reported separately — requesting and serving shards
/// (SendingBundleShardRequest(140) / ReceivingBundleShardRequest(141) →
/// BundleShardTransferred(145)), requesting and serving a whole bundle
/// (SendingBundleRequest(148) / ReceivingBundleRequest(149) →
/// BundleTransferred(153)), local reconstruction (ReconstructingBundle(146) →
/// BundleReconstructed(147)) and end-to-end recovery per audit. Latencies from
/// all reporting nodes are pooled per bucket; see BundleLatencyRow for the exact
/// event pairing behind each field.
///
/// Answers: how long does recovering an audit bundle take, and which leg of the
/// recovery dominates?
#[utoipa::path(
    get,
    path = "/api/grafana/bundle-latency",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket, each with approximate percentiles in milliseconds for shard request and serve, full-bundle request and serve, reconstruction and end-to-end recovery, their sample counts, and how many measurements ended in failure.", body = [BundleLatencyRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn bundle_latency(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_bundle_latency(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/bundle-latency", e))
}

/// Import segment fetching latency percentiles over time.
///
/// One row per time bucket. A guarantor fetches a work package's import segments
/// before refinement, so these latencies sit directly in the work-package
/// pipeline. Each leg is reported separately — requesting and serving segment
/// shards (SendingSegmentShardRequest(162) / ReceivingSegmentShardRequest(163) →
/// SegmentShardsTransferred(167)), requesting and serving whole segments
/// (SendingSegmentRequest(173) / ReceivingSegmentRequest(174) →
/// SegmentsTransferred(178)) and local reconstruction
/// (ReconstructingSegments(168) → SegmentsReconstructed(170)). Latencies from all
/// reporting nodes are pooled per bucket.
///
/// Answers: is import segment fetching delaying refinement, and is the delay in
/// the network or in reconstruction?
#[utoipa::path(
    get,
    path = "/api/grafana/segment-latency",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket, each with approximate percentiles in milliseconds for segment shard request and serve, whole-segment request and serve, and reconstruction, their sample counts, and how many measurements ended in failure.", body = [SegmentLatencyRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn segment_latency(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_segment_latency(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/segment-latency", e))
}

/// Preimage transfer latency percentiles over time.
///
/// One row per time bucket, split by role: the requestor's round-trip from
/// SendingPreimageRequest(193) to PreimageTransferred(198), and the responder's
/// local handling from ReceivingPreimageRequest(194) to PreimageTransferred(198).
/// Latencies from all reporting nodes are pooled per bucket.
///
/// Answers: how quickly do nodes obtain the preimages a service needs, and is a
/// slow transfer the requestor's or the responder's problem?
#[utoipa::path(
    get,
    path = "/api/grafana/preimage-latency",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket, each with approximate requestor-side and responder-side percentiles in milliseconds, their sample counts, and how many transfers ended in failure.", body = [PreimageLatencyRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn preimage_latency(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_preimage_latency(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/preimage-latency", e))
}

/// Work-package pipeline funnel over time, one row per time bucket.
///
/// The same per-stage counts as `/wp-funnel` — WorkPackageReceived(94),
/// Authorized(95), Refined(101), WorkReportBuilt(102), GuaranteeBuilt(105),
/// GuaranteesDistributed(109) and WorkPackageFailed(92) — with each work package
/// attributed to the bucket in which it was first observed, so its later stages
/// are counted in that same bucket even if they happened afterwards. The optional
/// `core` filter narrows to one core.
///
/// Answers: when did the pipeline start losing work packages, and at which stage?
#[utoipa::path(
    get,
    path = "/api/grafana/wp-funnel-timeseries",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket in ascending order, each with the per-stage work-package counts for that bucket. Narrowed by the optional core filter.", body = [WpFunnelTimeseriesRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_funnel_timeseries(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_wp_funnel_timeseries(q.start, q.end, interval, q.core)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp-funnel-timeseries", e))
}

/// Per-stage work-package pipeline durations over time, one row per time bucket.
///
/// The same stage measurements as `/bottlenecks` — authorize
/// (WorkPackageReceived(94) → Authorized(95)) through distribute
/// (GuaranteeBuilt(105) → GuaranteesDistributed(109)), plus the total from
/// reception to distribution — with each work package attributed to the bucket in
/// which it was first observed. A stage's percentiles are null in buckets where no
/// work package reached that stage. The optional `core` filter narrows to one core.
///
/// Answers: when did a pipeline stage start slowing down, and which one?
#[utoipa::path(
    get,
    path = "/api/grafana/bottlenecks-timeseries",
    params(WpTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket in ascending order, each with the per-stage median and p95 durations in milliseconds plus the bucket's work-package and failure counts. Narrowed by the optional core filter.", body = [BottlenecksTimeseriesRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn bottlenecks_timeseries(
    Query(q): Query<WpTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_bottlenecks_timeseries(q.start, q.end, interval, q.core)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/bottlenecks-timeseries", e))
}

/// Per-guarantor work-package pipeline performance.
///
/// One row per guarantor, over the work packages first observed in the range that
/// finished — reached GuaranteesDistributed(109) or WorkPackageFailed(92); ones
/// still in flight are left out. Each stage average is the duration between two
/// consecutive pipeline stages (WorkPackageReceived(94) → Authorized(95) →
/// Refined(101) → WorkReportBuilt(102) → GuaranteeBuilt(105) →
/// GuaranteesDistributed(109)) in milliseconds, taken only over the work packages
/// that did get distributed, while `failure_rate` covers every finished one.
/// `slowdown_factor` is the guarantor's average total divided by
/// `network_avg_total_ms`, itself the unweighted mean of the per-guarantor
/// averages; above roughly 1.5 the guarantor is an outlier. `core` narrows to the
/// work packages on one core, `sort` picks slowest-first (default) or
/// fastest-first, and `limit` truncates after sorting.
///
/// **Caveat:** a work package is attributed to the guarantor that first reported
/// WorkPackageReceived(94) for it, but every later stage timestamp is the earliest
/// report from *any* of its guarantors. A row therefore measures the fastest
/// observed progress of that guarantor's work packages, which can blend several
/// guarantors, rather than that one node's own processing.
///
/// Answers: which guarantors are slow or failing compared with the rest of the
/// network?
#[utoipa::path(
    get,
    path = "/api/grafana/validator-profiling",
    params(ValidatorProfilingQuery),
    responses(
        (status = 200, description = "Single object with the network-wide average total pipeline duration and the per-guarantor rows, sorted by average total duration, slowest first unless `sort=asc`. The network average always reflects every guarantor with finished work packages, not just the rows returned under `limit`.", body = ValidatorProfilingResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn validator_profiling(
    Query(q): Query<ValidatorProfilingQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let ascending = q.sort.as_deref() == Some("asc");
    state
        .store
        .grafana_validator_profiling(q.start, q.end, q.core, q.limit, ascending)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/validator-profiling", e))
}

/// Per-guarantor work-package pipeline performance over time.
///
/// The time-bucketed form of `/validator-profiling`: one row per bucket and
/// guarantor, with each work package placed in the bucket in which it was first
/// observed. `wp_count` here counts every work package attributed to that
/// guarantor in the bucket, including ones still in flight, while the stage
/// averages (WorkPackageReceived(94) → Authorized(95) → Refined(101) →
/// WorkReportBuilt(102) → GuaranteeBuilt(105) → GuaranteesDistributed(109)) cover
/// only those that reached distribution. With `node` set, every bucket for that
/// one guarantor is returned; without it, the 20 guarantors with the highest
/// average total duration over the whole range are chosen once and all their
/// buckets returned, so the same nodes appear throughout.
///
/// The attribution caveat of `/validator-profiling` applies here too: stage
/// timestamps are the earliest report from any of a work package's guarantors.
///
/// Answers: when did a guarantor start slowing down, and at which stage?
#[utoipa::path(
    get,
    path = "/api/grafana/validator-profiling-timeseries",
    params(ValidatorProfilingTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket and guarantor, ascending by bucket and then node, each with the bucket's work-package and failure counts and its per-stage average durations in milliseconds. With `node`: only that guarantor; without it: the 20 slowest guarantors over the whole range.", body = [ValidatorProfilingTimeseriesRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn validator_profiling_timeseries(
    Query(q): Query<ValidatorProfilingTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    state
        .store
        .grafana_validator_profiling_timeseries(q.start, q.end, interval, q.core, q.node.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/validator-profiling-timeseries", e))
}

/// Catalogue of the 115 JIP-3 telemetry event types this API understands.
///
/// One entry per event type: its numeric ID as defined in JIP-3, its canonical
/// name — `Authored` for 42, for instance — and the event group it belongs to.
/// The event groups are `wp_pipeline`, `guarantee_receiving`, `assurances`,
/// `shards`, `segments`, `bundles`, `preimages`, `blocks`,
/// `block_distribution`, `tickets`, `connections`, `status` and `system`, plus
/// the virtual group `failures`, which gathers every failure, discard and
/// duplicate event from across the others. The `group` parameter narrows the
/// listing to one of them.
///
/// The IDs, names and group names listed here are exactly the values that the
/// `event_types` parameter of the other endpoints accepts. The catalogue is
/// fixed and describes no observed traffic.
///
/// Answers: which telemetry event types exist, what are they called, and which
/// group does each belong to?
#[utoipa::path(
    get,
    path = "/api/grafana/event-types",
    params(EventTypesParams),
    responses(
        (status = 200, description = "Array of event type entries — numeric ID, canonical name and event group — covering every type, or only the requested group.", body = [crate::event_type_meta::EventTypeMeta]),
    ),
    tag = "grafana"
)]
async fn event_types(Query(params): Query<EventTypesParams>) -> impl IntoResponse {
    let all = crate::event_type_meta::event_type_metadata();
    if let Some(ref group) = params.group {
        let ids = crate::event_type_meta::expand_event_types(group);
        let filtered: Vec<_> = all
            .iter()
            .filter(|m| ids.contains(&m.id))
            .cloned()
            .collect();
        Json(filtered)
    } else {
        Json(all.to_vec())
    }
}

/// Individual telemetry events exactly as the nodes reported them, newest first.
///
/// Every one of the 115 JIP-3 event types is browsable here, each with the full
/// payload the reporting node sent. Raw events are retained for about an hour,
/// so a range reaching further back returns nothing for the older part. Filters
/// narrow by event type, reporting node, core and work-package hash (hex, with
/// or without `0x`); the core and work-package filters only match events whose
/// core or work package could be determined.
///
/// The `event_types` parameter is optional — omitted, every type is returned.
/// It takes a comma-separated mix of numeric JIP-3 event IDs, canonical event
/// names such as `Authored`, and event group names such as `wp_pipeline`, with
/// Grafana `{a,b}` multi-select syntax; entries it does not recognise are
/// ignored. Results are paginated: `limit` defaults to 500 and is capped at
/// 2000, `offset` skips ahead, and the response reports how many events match
/// in total.
///
/// Answers: what exactly did the nodes report in the last hour, for a given
/// event type, node, core or work package?
#[utoipa::path(
    get,
    path = "/api/grafana/events",
    params(EventsQuery),
    responses(
        (status = 200, description = "Single object with one page of matching events, newest first, plus pagination metadata carrying the total number of matches.", body = EventsSearchResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn events(
    Query(q): Query<EventsQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let event_types = q
        .event_types
        .as_deref()
        .map(crate::event_type_meta::expand_event_types)
        .unwrap_or_default();
    let limit = q.limit.unwrap_or(500);
    let offset = q.offset.unwrap_or(0);

    // Parse wp_hash from hex string to bytes
    let wp_hash_bytes = q.wp_hash.as_deref().and_then(|h| {
        let h = h.strip_prefix("0x").unwrap_or(h);
        hex::decode(h).ok()
    });

    state
        .store
        .grafana_events(
            q.start,
            q.end,
            &event_types,
            limit,
            offset,
            q.node.as_deref(),
            q.core,
            wp_hash_bytes.as_deref(),
        )
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/events", e))
}

/// Guarantees dropped from validators' local guarantee pools over time, split by reason.
///
/// Counts GuaranteeDiscarded(113) per time bucket and per reason. The reasons are
/// the JIP-3 discard reasons: PackageReportedOnChain(0) — the work package was
/// already reported on-chain, ReplacedByBetter(1), CannotReportOnChain(2),
/// TooManyGuarantees(3), Other(4). Only reason 0 means the guarantee reached its
/// intended end; the others mean guarantor work was thrown away.
///
/// Answers: why are guarantees leaving the pool without being reported on-chain,
/// and is that getting worse over time?
#[utoipa::path(
    get,
    path = "/api/grafana/guarantee-discards",
    params(GuaranteeDiscardsQuery),
    responses(
        (status = 200, description = "Array of (bucket timestamp, discard reason, count) rows, ordered by bucket then reason; one row per reason seen in each bucket.", body = [GuaranteeDiscardRow]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantee_discards(
    Query(q): Query<GuaranteeDiscardsQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("30s");
    state
        .store
        .grafana_guarantee_discards(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/guarantee-discards", e))
}

// ── Phase 3: New grafana endpoints ───────────────────────────────────────

/// How often each part of the protocol is failing, network-wide, per node, and most recently.
///
/// Six categories, each weighing the failures a node reported against the
/// corresponding successes over the time range:
///
/// - **block_authoring** — AuthoringFailed(41), BlockVerificationFailed(44),
///   BlockExecutionFailed(46) against Authoring(40) and Authored(42)
/// - **tickets** — TicketGenerationFailed(81), TicketTransferFailed(83) against
///   GeneratingTickets(80), TicketsGenerated(82), TicketTransferred(84)
/// - **work_packages** — WorkPackageFailed(92), WorkPackageSharingFailed(99) against
///   WorkPackageReceived(94)
/// - **guarantees** — GuaranteeSendFailed(107), GuaranteeReceiveFailed(111),
///   GuaranteeDiscarded(113) against GuaranteeBuilt(105), GuaranteeSent(108),
///   GuaranteesDistributed(109)
/// - **shards** — ShardRequestFailed(122) against SendingShardRequest(120) and
///   ShardsTransferred(125)
/// - **assurances** — AssuranceSendFailed(127) against DistributingAssurance(126)
///
/// Each rate is failures over all events counted for the category, so it is a share
/// of observed events, not of distinct protocol operations — one block or work
/// package normally reports several events on its way through. `overall` pools the
/// same failure events across all six categories, but its denominator leaves out
/// TicketsGenerated(82) and TicketTransferred(84), so it is not exactly the sum of
/// the per-category figures. `by_node` names the 20 nodes with the most failures.
/// Note that GuaranteeDiscarded(113) counts as a failure here even though its most
/// common reason is the work package already being reported on-chain — see
/// `/guarantee-discards` for the reason split. `recent_failures` lists the last 20
/// individual failure events from the last 5 minutes only, regardless of the
/// requested range, since individual events are retained for about an hour.
///
/// Answers: what is failing across the network, in which part of the protocol, and
/// on which nodes?
#[utoipa::path(
    get,
    path = "/api/grafana/failure-rates",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with the pooled failure rate for the range, one entry per failure category, the 20 nodes with the most failures, and a short list of the most recent individual failure events.", body = FailureRatesResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn failure_rates(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_failure_rates(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/failure-rates", e))
}

/// How much of the network is keeping up with the chain tip, over time.
///
/// One row per time bucket. Each node's highest best-block slot in the bucket
/// comes from its BestBlockChanged(11) reports; `network_slot` is the highest
/// slot any node reported, and a node counts as synced when its own best slot is
/// within 2 slots (about 12 s) of it. Only nodes that reported
/// BestBlockChanged(11) in the bucket are counted, so a node that stops
/// reporting disappears from the row instead of showing up as behind. This is an
/// observed measure and is independent of the node's own subjective
/// SyncStatusChanged(13) flag. `interval` defaults to 5m and is snapped to a
/// supported bucket width.
///
/// Answers: is the network in sync, and how many nodes are lagging behind the
/// chain tip?
#[utoipa::path(
    get,
    path = "/api/grafana/sync-timeline",
    params(TimeseriesQuery),
    responses(
        (status = 200, description = "Array of per-bucket rows, ascending by time, each with the highest best-block slot seen in the network and how many reporting nodes were at the tip versus behind it.", body = [SyncTimelineRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn sync_timeline(
    Query(q): Query<TimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("5m");
    state
        .store
        .grafana_sync_timeline(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/sync-timeline", e))
}

/// Peer connections established and dropped across the network, over time.
///
/// One row per time bucket, counting the peer links nodes reported completing —
/// ConnectedIn(23) for inbound plus ConnectedOut(26) for outbound — against
/// Disconnected(27), together with how many distinct nodes reported any of those
/// three in the bucket. Attempts that never completed are not counted here:
/// ConnectionRefused(20), ConnectingIn(21), ConnectInFailed(22), ConnectingOut(24)
/// and ConnectOutFailed(25) are excluded. `health_stats` is not part of the
/// timeline and ignores the time range — it is the current tally of nodes ever seen
/// by telemetry and how many are reporting right now. `interval` defaults to 5m and
/// snaps to a supported width; the underlying counts have 30-second resolution, so
/// 30s is the finest interval that carries real detail.
///
/// Answers: is peer connectivity across the network stable, or are nodes churning
/// connections?
#[utoipa::path(
    get,
    path = "/api/grafana/connections-timeline",
    params(TimeseriesQuery),
    responses(
        (status = 200, description = "Single object with an array of per-bucket connection, disconnection and active-node counts ascending by time, plus a current, range-independent tally of known and reporting nodes.", body = ConnectionsTimelineResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn connections_timeline(
    Query(q): Query<TimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("5m");
    state
        .store
        .grafana_connections_timeline(q.start, q.end, interval)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/connections-timeline", e))
}

/// Network-wide guarantee counts for every stage of guarantee distribution, plus success rates.
///
/// Counts each guaranteeing event over the time range: GuaranteeBuilt(105),
/// SendingGuarantee(106), GuaranteeSendFailed(107), GuaranteeSent(108),
/// GuaranteesDistributed(109) on the guarantor side, and ReceivingGuarantee(110),
/// GuaranteeReceiveFailed(111), GuaranteeReceived(112), GuaranteeDiscarded(113)
/// on the receiving side. Send success is GuaranteeSent(108) over all send
/// attempts (106 + 107 + 108), receive success is GuaranteeReceived(112) over all
/// receive attempts (110 + 111 + 112); both are 1.0 when there was no activity.
///
/// Answers: how much guaranteeing traffic is the network carrying, and what
/// fraction of guarantee transfers succeeds?
#[utoipa::path(
    get,
    path = "/api/grafana/guarantees",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with a per-event-type count block and a send/receive success-rate block for the whole time range.", body = GuaranteesResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantees(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_guarantees(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/guarantees", e))
}

/// Guaranteeing activity per guarantor node, with the cores each one was seen guaranteeing for.
///
/// One row per node that emitted GuaranteeBuilt(105) in the time range, with how
/// many guarantees it built, when it last built one, and the set of cores those
/// guarantees were for. Nodes that built no guarantees do not appear. Guarantee
/// propagation records are retained for 7 days, which bounds how far back the
/// time range can reach.
///
/// **Caveat:** the node→core association is what was observed, not the protocol's
/// validator→core assignment. JAM rotates core assignments every 10 slots and
/// reshuffles them each epoch, so a node legitimately appears on several cores
/// over any range longer than one rotation.
///
/// Answers: which nodes are actually guaranteeing, for which cores, and how
/// evenly is guaranteeing work spread across them?
#[utoipa::path(
    get,
    path = "/api/grafana/guarantees/by-guarantor",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object holding the guarantor count and an array of per-node rows, ordered by guarantees built, most active first.", body = GuarantorBreakdownResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn guarantees_by_guarantor(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_guarantees_by_guarantor(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/guarantees/by-guarantor", e))
}

/// Work-package pipeline totals for the range, with a per-core breakdown.
///
/// `totals` combines two views. The stage counts say how many work packages
/// reached each guarantor pipeline stage, from received (WorkPackageReceived(94))
/// through distributed (GuaranteesDistributed(109)) and failed
/// (WorkPackageFailed(92)). The pre-pipeline figures count the
/// WorkPackageSubmission(90), WorkPackageBeingShared(91) and
/// DuplicateWorkPackage(93) events reported by all nodes — these are event counts,
/// not distinct work packages, and a duplicate is reported instead of a reception,
/// so duplicates never appear in the stage counts. `by_core` counts the work
/// packages first observed in the range per core they were assigned to.
///
/// Answers: how much work-package traffic did the network handle, how far did it
/// get, and how is it spread across cores?
#[utoipa::path(
    get,
    path = "/api/grafana/wp-stats",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with the pre-pipeline and per-stage totals for the range plus one count per core.", body = WpStatsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_stats(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_wp_stats(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp-stats", e))
}

/// Which core each guaranteeing node was seen working on.
///
/// One row per node that emitted GuaranteeBuilt(105) in the range, with the total
/// number of guarantees it built across all cores; a node that built none does not
/// appear. `primary_core` names a single core even when the node guaranteed for
/// several, so it is only meaningful over ranges shorter than one core rotation —
/// use `/guarantees/by-guarantor` for a node's full core set, or
/// `/cores/{core_id}/validators` for the view from one core. Guarantee propagation
/// records are kept for 7 days, which bounds how far back the range can reach.
///
/// **Caveat:** the node→core association is observed from guaranteeing behaviour,
/// not the protocol's validator→core assignment. JAM rotates core assignments
/// every 10 slots and reshuffles them each epoch, so a node legitimately shows up
/// on several cores over any range longer than one rotation.
///
/// Answers: which node is active on which core?
#[utoipa::path(
    get,
    path = "/api/grafana/validators/cores",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Array of one row per guaranteeing node, ordered by guarantees built with the most active first, each naming a core the node was seen guaranteeing for.", body = [ValidatorCoreRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn validators_cores(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_validators_cores(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/validators/cores", e))
}

/// One network health score for the range, broken down into five protocol subsystems.
///
/// Each component is a success share over the requested range, scored 0–100:
///
/// - **block_production** — Authored(42) over Authored(42) plus AuthoringFailed(41),
///   BlockVerificationFailed(44) and BlockExecutionFailed(46). Healthy at 95 and above.
/// - **work_packages** — WorkPackageReceived(94) over that plus WorkPackageFailed(92)
///   and WorkPackageSharingFailed(99). Healthy at 95 and above.
/// - **data_availability** — ShardsTransferred(125) over that plus
///   ShardRequestFailed(122). Healthy at 95 and above.
/// - **connectivity** — the share of all nodes ever seen by telemetry that are
///   reporting right now; this one ignores the time range. Healthy at 90 and above.
/// - **event_throughput** — 100 if any of the above events arrived at all in the
///   range, 0 if none did.
///
/// A component with no activity at all scores 100 rather than 0, so an idle network
/// looks healthy. The overall score is the plain mean of the five: healthy at 90 and
/// above, degraded from 70, unhealthy below that. Only block_production raises
/// alerts — a warning below 95 and an error below 80.
///
/// Answers: is the network healthy overall, and which subsystem is dragging the
/// score down?
#[utoipa::path(
    get,
    path = "/api/grafana/network-health",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with the overall score and status label, one entry per health component, and any alerts raised.", body = NetworkHealthResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn network_health(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_network_health(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/network-health", e))
}

// ── Phase 4: Moderate endpoints ──────────────────────────────────────────

/// Recent work packages, with a pipeline health summary of the whole range.
///
/// `work_packages` lists the most recently started work packages of the range (at
/// most 200, newest first), each with its stage timestamps from
/// WorkPackageReceived(94) through GuaranteesDistributed(109), the gas its
/// Refined(101) reported, how many guarantors received it and how many built a
/// guarantee for it, and the reason from WorkPackageFailed(92) where one was
/// reported. The summaries alongside cover every work package first observed in the
/// range, not only the listed ones: `summary` counts the work packages whose
/// furthest reached stage is each stage, so it shows where work stalled; `reached`
/// counts those that ever reached each stage; `stage_duration_percentiles` gives
/// per-stage median and p95 durations; `failure_breakdown` groups failures by
/// reported reason.
///
/// Answers: which work packages ran recently, where are they stalling, and why are
/// they failing?
#[utoipa::path(
    get,
    path = "/api/grafana/wp-active",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with the recent work-package list and the range-wide stage counts, per-stage duration percentiles and failure-reason breakdown.", body = WpActiveResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_active(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_wp_active(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp-active", e))
}

/// Everything known about one work package, looked up by its hash.
///
/// `summary` is that work package's pipeline timeline — the timestamps of
/// WorkPackageReceived(94), Authorized(95), Refined(101), WorkReportBuilt(102),
/// GuaranteeBuilt(105), GuaranteesDistributed(109) and WorkPackageFailed(92), the
/// services it touched, and how many guarantors received and guaranteed it — and
/// stays available long after the work package finished. `events` is the raw
/// telemetry timeline for the same work package in emission order; raw events are
/// retained for about an hour, so for older work packages this array is empty while
/// the summary remains. The hash is hex-encoded, with or without a `0x` prefix.
///
/// Answers: what exactly happened to this one work package, and where did it stop?
#[utoipa::path(
    get,
    path = "/api/grafana/wp/{wp_hash}",
    params(
        ("wp_hash" = String, Path, description = "Work package hash (hex-encoded)")
    ),
    responses(
        (status = 200, description = "Single object with the work package's pipeline timeline (null if the hash is unknown) and its raw event timeline, oldest event first.", body = WpDetailResponse),
        (status = 400, description = "Work package hash is not valid hex"),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_detail(
    Path(wp_hash): Path<String>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let hash_str = wp_hash.strip_prefix("0x").unwrap_or(&wp_hash);
    let hash_bytes = hex::decode(hash_str).map_err(|_| StatusCode::BAD_REQUEST)?;

    state
        .store
        .grafana_wp_detail(&hash_bytes)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp/{hash}", e))
}

/// Pipeline timelines for several work packages in one request.
///
/// The request body is a JSON array of hex-encoded work-package hashes, with or
/// without `0x` prefixes. Each response row is one work package's pipeline
/// timeline, the same summary `/wp/{wp_hash}` returns: the stage timestamps from
/// WorkPackageReceived(94) through GuaranteesDistributed(109), WorkPackageFailed(92)
/// where reported, and the guarantor counts. Hashes that are unknown or not valid
/// hex are simply absent, so the response can be shorter than the request.
///
/// Answers: how far through the pipeline did each of these specific work packages
/// get?
#[utoipa::path(
    post,
    path = "/api/grafana/wp/batch",
    request_body = Vec<String>,
    responses(
        (status = 200, description = "Array of pipeline timelines, one per work package that was found, newest first.", body = [WpTrackingRow]),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn wp_batch(
    State(state): State<ApiState>,
    Json(hashes): Json<Vec<String>>,
) -> Result<impl IntoResponse, StatusCode> {
    let hash_bytes: Vec<Vec<u8>> = hashes
        .iter()
        .filter_map(|h| {
            let h = h.strip_prefix("0x").unwrap_or(h);
            hex::decode(h).ok()
        })
        .collect();

    state
        .store
        .grafana_wp_batch(&hash_bytes)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/wp/batch", e))
}

/// Block production and import health for the range, plus the current chain tips.
///
/// `totals` counts the block lifecycle events reported by the whole network in
/// the range: Authoring(40), AuthoringFailed(41), Authored(42), Importing(43),
/// BlockVerificationFailed(44), BlockVerified(45), BlockExecutionFailed(46),
/// BlockExecuted(47), plus BestBlockChanged(11) and FinalizedBlockChanged(12).
/// Every node reports its own import of a block, so the import-side counts scale
/// with the number of nodes, while `authored` is one per block. `chain` gives the
/// current best and finalized slot as of the request, not over the range, and is
/// null when live chain tracking is unavailable. `authoring_by_node` ranks the 50
/// most active authors by Authored(42) count.
///
/// Answers: are blocks being produced, verified and executed successfully, and
/// which nodes are authoring them?
#[utoipa::path(
    get,
    path = "/api/grafana/blocks/summary",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with the range-wide block-event totals, the current best and finalized slot, and the 50 most active authoring nodes.", body = BlocksSummaryResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn blocks_summary(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let mut result = state
        .store
        .grafana_blocks_summary(q.start, q.end)
        .await
        .map_err(|e| map_sqlx_error("grafana/blocks/summary", e))?;

    // Overlay LiveCounters for current slot numbers
    if let Some(ref tracker) = state.metrics_tracker {
        let lc = tracker.live_counters();
        result.chain.best_slot = Some(lc.latest_slot() as i32);
        result.chain.finalized_slot = Some(lc.finalized_slot() as i32);
    }

    Ok(Json(result))
}

/// One core's work-package throughput, latency and gas usage.
///
/// `processing_efficiency_pct` is the share of Refined(101) among Refined(101)
/// plus WorkPackageFailed(92) reports on the core, and is 100 when neither was
/// reported. The latency percentiles run from WorkPackageReceived(94) to
/// GuaranteesDistributed(109) over the work packages first observed on this core in
/// the range, falling back to the last pipeline event seen for ones that never got
/// distributed, whereas `average_completion_time_ms` averages only the ones that
/// did. `total_gas_used` sums the refine gas reported in Refined(101), and
/// `work_packages_processed` counts WorkPackageReceived(94) reports, one per
/// guarantor.
///
/// Answers: is this core keeping up, and is it slow, failing or gas-heavy?
#[utoipa::path(
    get,
    path = "/api/grafana/cores/{core_id}/metrics",
    params(
        ("core_id" = i16, Path, description = "Core index (0–340)"),
        TimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Single object with this core's processing efficiency, pipeline latency percentiles, average completion time, refine gas total and work-package reception count.", body = CoreMetricsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn core_metrics(
    Path(core_id): Path<i16>,
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_core_metrics(q.start, q.end, core_id)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/cores/{id}/metrics", e))
}

/// Which nodes built guarantees for one core, and what software they run.
///
/// One row per node that emitted GuaranteeBuilt(105) for this core in the range,
/// with how many guarantees it built, when it last built one, and the node's
/// implementation name, version and current connection state. A validator assigned
/// to the core that never built a guarantee does not appear, and `total_active`
/// counts only the nodes that did. Guarantee propagation records are kept for
/// 7 days, which bounds how far back the range can reach. Per-guarantor data
/// availability figures come from `/da-stats?node=…` instead.
///
/// **Caveat:** this is the observed guarantor set, not the protocol's
/// validator→core assignment. JAM rotates core assignments every 10 slots and
/// reshuffles them each epoch, so over a longer range more nodes appear here than
/// are assigned to the core at any one time.
///
/// Answers: which nodes are actually guaranteeing for this core?
#[utoipa::path(
    get,
    path = "/api/grafana/cores/{core_id}/validators",
    params(
        ("core_id" = i16, Path, description = "Core index (0–340)"),
        TimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Single object with the core index, how many nodes built guarantees for it, and the per-node rows ordered by guarantees built with the most active first.", body = CoreValidatorsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn core_validators(
    Path(core_id): Path<i16>,
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_core_validators(q.start, q.end, core_id)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/cores/{id}/validators", e))
}

// ── Phase 5: Hard rewrites ──────────────────────────────────────────────

/// Gas and execution time for each of the three service-execution phases.
///
/// The phases are the three points where service code actually runs: the
/// is-authorized call of a work package, reported by Authorized(95); the
/// refine call of each work item, reported by Refined(101); and the
/// accumulate call of each service touched by a block, reported by
/// BlockExecuted(47). Each phase gives how many executions were reported,
/// their total and mean gas, and the mean wall-clock and code-load times.
/// `by_service` splits the same numbers per service and phase, highest gas
/// first. Guarantors and block-executing nodes report independently, so an
/// execution is counted once per reporting node rather than once per work
/// package or block. Only about the last week of executions is kept.
///
/// Answers: which execution phase and which services dominate gas usage and
/// execution time?
#[utoipa::path(
    get,
    path = "/api/grafana/execution",
    params(TimeRangeQuery),
    responses(
        (status = 200, description = "Single object with a gas and timing summary for each of the authorization, refinement and accumulation phases, plus the 50 highest-gas service-and-phase combinations.", body = ExecutionMetricsResponse),
        (status = 500, description = "Database error"),
    ),
    tag = "grafana"
)]
async fn execution_metrics(
    Query(q): Query<TimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .grafana_execution(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("grafana/execution", e))
}

// ── On-chain statistics query params ─────────────────────────────────────

/// Parameters for on-chain time range queries.
#[derive(Deserialize, IntoParams)]
pub struct OnchainTimeRangeQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
}

/// Parameters for on-chain timeseries queries.
#[derive(Deserialize, IntoParams)]
pub struct OnchainTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
}

/// Parameters for on-chain core timeseries queries (with optional core filter).
#[derive(Deserialize, IntoParams)]
pub struct OnchainCoreTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Core index to filter by. Without this, returns network-wide aggregates.
    pub core: Option<i16>,
}

/// Parameters for on-chain validator timeseries queries (with optional validator filter).
#[derive(Deserialize, IntoParams)]
pub struct OnchainValidatorTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Comma-separated validator indices. Supports Grafana {a,b} syntax.
    pub validator: Option<String>,
}

/// Parameters for on-chain service queries (with optional service filter).
#[derive(Deserialize, IntoParams)]
pub struct OnchainServiceQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Comma-separated service IDs (decimal or 0x hex). Supports Grafana {a,b} syntax.
    pub service: Option<String>,
}

/// Parameters for on-chain service timeseries queries (with optional service filter).
#[derive(Deserialize, IntoParams)]
pub struct OnchainServiceTimeseriesQuery {
    /// Start of time range (ISO 8601)
    pub start: DateTime<Utc>,
    /// End of time range (ISO 8601)
    pub end: DateTime<Utc>,
    /// Bucket width. Supported: 6s, 12s, 18s, 24s, 30s, 1m, 2m, 5m, 10m, 15m, 30m, 1h–1d. Unsupported values are snapped to nearest valid.
    pub interval: Option<String>,
    /// Comma-separated service IDs (decimal or 0x hex). Supports Grafana {a,b} syntax.
    pub service: Option<String>,
}

// ── On-chain cores ──────────────────────────────────────────────────────

/// What each core did on chain over a time range, one row per core.
///
/// The JAM chain keeps per-core activity statistics in every block: the gas
/// consumed by the work reported on the core, the segments its work items
/// imported and exported, the extrinsics they referenced, the work-bundle bytes
/// and the total bytes the core placed into data availability, and how many
/// validators assured the core. Those are per-block figures in the protocol, so
/// each row adds them up over every block of the range — `popularity_avg`
/// excepted, which is a mean. Statistics are read from the chain once per block,
/// blocks that later lost to a fork are left out, and history reaches back
/// 90 days.
///
/// Answers: how is reported work spread across cores, and which cores carry the
/// most gas and data-availability load?
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/cores",
    params(OnchainTimeRangeQuery),
    responses(
        (status = 200, description = "Array with one row per core, ascending by core index, \
            each totalling the core's gas, imports, exports, extrinsic and data-availability \
            figures over the range, plus its mean assurance popularity.",
            body = [OnchainCoreSummary]),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_cores_summary(
    Query(q): Query<OnchainTimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .onchain_cores_summary(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/cores", e))
}

/// Per-core on-chain activity over time.
///
/// The same per-block core statistics as `/onchain/cores`, split into buckets of
/// the requested `interval` (default 1 minute; widths outside the supported set
/// are snapped up to the nearest supported one, between 6 s — one slot — and
/// 1 day). Without a `core` filter each row covers all cores together, with
/// popularity averaged; with one, each row covers that single core and carries
/// its index.
///
/// Answers: how does core load develop over time, and when did gas or
/// data-availability usage spike?
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/cores/timeseries",
    params(OnchainCoreTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows ascending by bucket: without the core \
            filter, one row per bucket covering all cores together; with it, one row per bucket \
            for the requested core.",
            body = [OnchainCoreTimeseriesAgg]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_cores_timeseries(
    Query(q): Query<OnchainCoreTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    if let Some(core) = q.core {
        let rows = state
            .store
            .onchain_cores_timeseries(q.start, q.end, interval, core)
            .await
            .map_err(|e| map_sqlx_error("onchain/cores/timeseries", e))?;
        Ok(Json(serde_json::to_value(rows).unwrap()))
    } else {
        let rows = state
            .store
            .onchain_cores_timeseries_agg(q.start, q.end, interval)
            .await
            .map_err(|e| map_sqlx_error("onchain/cores/timeseries", e))?;
        Ok(Json(serde_json::to_value(rows).unwrap()))
    }
}

/// One core's on-chain activity block by block.
///
/// One row per block in which the chain reported statistics for this core,
/// newest first, capped at 1000 rows and never aggregated — each row is what the
/// core did in that single block: gas, imports, exports, extrinsics, work-bundle
/// bytes, bytes placed into data availability, and the number of validators
/// assuring it.
///
/// Answers: what exactly did this core do in each recent block?
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/cores/{core_id}",
    params(
        ("core_id" = i16, Path, description = "Core index (0-based)"),
        OnchainTimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Array of per-block rows for this core, newest first, \
            at most 1000 rows.",
            body = [OnchainCoreDetail]),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_core_detail(
    Path(core_id): Path<i16>,
    Query(q): Query<OnchainTimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .onchain_core_detail(core_id, q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/cores/{id}", e))
}

// ── On-chain services ───────────────────────────────────────────────────

/// What each service did on chain over a time range, one row per service.
///
/// The JAM chain keeps per-service activity statistics in every block: preimages
/// provided to the service and their total size, work items refined for it and
/// the refinement gas they used, work items accumulated and the accumulation gas,
/// the segments its work items imported and exported, and the extrinsics they
/// referenced. Those are per-block figures, so each row adds them up over the
/// range, and only services that were active somewhere in the range appear. The
/// `service` filter takes decimal or 0x-hex service IDs and Grafana `{a,b}`
/// multi-select syntax. Statistics are read from the chain once per block, blocks
/// that later lost to a fork are left out, and history reaches back 90 days.
///
/// Answers: which services are consuming the network's gas and
/// data-availability capacity?
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/services",
    params(OnchainServiceQuery),
    responses(
        (status = 200, description = "Array with one row per service that was active in the \
            range, ascending by service ID, each totalling its preimage, refinement, \
            accumulation, segment and extrinsic figures.",
            body = [OnchainServiceSummary]),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_services_summary(
    Query(q): Query<OnchainServiceQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let services: Option<Vec<DbServiceId>> = q.service.map(|s| DbServiceId::parse_list(&s));
    state
        .store
        .onchain_services_summary(q.start, q.end, services.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/services", e))
}

/// Per-service on-chain activity over time.
///
/// The same per-block service statistics as `/onchain/services`, split into
/// buckets of the requested `interval` (default 1 minute; unsupported widths are
/// snapped up to the nearest supported one, between 6 s — one slot — and 1 day).
/// One row per bucket and service; without a `service` filter every service
/// active in the bucket is returned, so the response grows with the number of
/// active services.
///
/// Answers: how do a service's refinement and accumulation gas and its
/// data-availability traffic develop over time?
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/services/timeseries",
    params(OnchainServiceTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows, one per time bucket and service, \
            ascending by bucket and then service ID, each with that service's figures for \
            the bucket.",
            body = [OnchainServiceTimeseries]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_services_timeseries(
    Query(q): Query<OnchainServiceTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    let services: Option<Vec<DbServiceId>> = q.service.map(|s| DbServiceId::parse_list(&s));
    state
        .store
        .onchain_services_timeseries(q.start, q.end, interval, services.as_deref())
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/services/timeseries", e))
}

/// One service's on-chain activity block by block.
///
/// One row per block in which the chain reported statistics for this service,
/// newest first, capped at 1000 rows and never aggregated. The path accepts the
/// service ID in decimal or 0x-hex form.
///
/// Answers: in which blocks was this service refined or accumulated, and what
/// did each of those blocks cost it in gas?
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/services/{service_id}",
    params(
        ("service_id" = String, Path, description = "Service ID (decimal or 0x hex)"),
        OnchainTimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Array of per-block rows for this service, newest first, \
            at most 1000 rows.",
            body = [OnchainServiceDetail]),
        (status = 400, description = "Invalid service ID"),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_service_detail(
    Path(service_id): Path<String>,
    Query(q): Query<OnchainTimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let ids = DbServiceId::parse_list(&service_id);
    let id = ids.first().copied().ok_or(StatusCode::BAD_REQUEST)?;
    state
        .store
        .onchain_service_detail(id, q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/services/{id}", e))
}

// ── On-chain validators ─────────────────────────────────────────────────

/// Each validator's on-chain activity tallies, at their peak within a time range.
///
/// The JAM chain accumulates six tallies per validator over an epoch — blocks
/// authored, tickets introduced, preimages introduced and their total size, work
/// reports guaranteed, and availability assurances made — and resets them when
/// the next epoch starts. Each field is the highest value that tally reached
/// inside the requested range, so a range that sits inside one epoch shows that
/// epoch's progress, while a range crossing an epoch boundary shows the older
/// epoch's final value. Statistics are read from the chain once per block, blocks
/// that later lost to a fork are left out, and history reaches back 90 days.
///
/// Answers: which validators are authoring blocks, guaranteeing reports and
/// assuring availability, and which are contributing nothing?
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/validators",
    params(OnchainTimeRangeQuery),
    responses(
        (status = 200, description = "Array with one row per validator, ascending by validator \
            index, each holding the peak of that validator's epoch tallies within the range.",
            body = [OnchainValidatorSummary]),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_validators_summary(
    Query(q): Query<OnchainTimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .onchain_validators_summary(q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/validators", e))
}

/// Validator activity tallies over time.
///
/// With a `validator` filter (comma-separated indices, Grafana `{a,b}`
/// multi-select syntax): one row per bucket and validator, holding the highest
/// value that validator's epoch tallies reached in the bucket, so each series
/// steps up through an epoch and falls back to zero at the epoch boundary.
/// Without the filter: one row per bucket adding up the tallies every validator
/// reported in every block of the bucket — since those tallies are
/// epoch-cumulative, that sum is a relative measure of participation, not a count
/// of what happened during the bucket. Bucket width comes from `interval`
/// (default 1 minute, snapped up to the nearest supported width between 6 s —
/// one slot — and 1 day).
///
/// Answers: how does validator participation build up through an epoch, and
/// which validators stop making progress?
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/validators/timeseries",
    params(OnchainValidatorTimeseriesQuery),
    responses(
        (status = 200, description = "Array of rows ascending by bucket: without the validator \
            filter, one row per bucket adding up all validators' tallies; with it, one row per \
            bucket and selected validator holding that validator's peak tallies for the bucket.",
            body = [OnchainValidatorTimeseriesAgg]),
        (status = 400, description = "Invalid interval"),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_validators_timeseries(
    Query(q): Query<OnchainValidatorTimeseriesQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    let interval = q.interval.as_deref().unwrap_or("1m");
    if let Some(ref validator_str) = q.validator {
        let validators = parse_validator_indices(validator_str);
        let rows = state
            .store
            .onchain_validators_timeseries(q.start, q.end, interval, &validators)
            .await
            .map_err(|e| map_sqlx_error("onchain/validators/timeseries", e))?;
        Ok(Json(serde_json::to_value(rows).unwrap()))
    } else {
        let rows = state
            .store
            .onchain_validators_timeseries_agg(q.start, q.end, interval)
            .await
            .map_err(|e| map_sqlx_error("onchain/validators/timeseries", e))?;
        Ok(Json(serde_json::to_value(rows).unwrap()))
    }
}

/// One validator's activity tallies block by block.
///
/// One row per block, newest first, capped at 1000 rows, each holding the
/// validator's epoch tallies as of that block. The values climb block by block
/// through the epoch and restart at zero once the next epoch begins.
///
/// Answers: block by block, when did this validator author, guarantee or assure,
/// and at which block did its tallies stop advancing?
#[utoipa::path(
    get,
    path = "/api/grafana/onchain/validators/{validator_idx}",
    params(
        ("validator_idx" = i16, Path, description = "Validator index (0-based)"),
        OnchainTimeRangeQuery,
    ),
    responses(
        (status = 200, description = "Array of per-block rows for this validator, newest first, \
            at most 1000 rows, each with the epoch tallies as of that block.",
            body = [OnchainValidatorDetail]),
        (status = 500, description = "Database error"),
    ),
    tag = "onchain"
)]
async fn onchain_validator_detail(
    Path(validator_idx): Path<i16>,
    Query(q): Query<OnchainTimeRangeQuery>,
    State(state): State<ApiState>,
) -> Result<impl IntoResponse, StatusCode> {
    state
        .store
        .onchain_validator_detail(validator_idx, q.start, q.end)
        .await
        .map(Json)
        .map_err(|e| map_sqlx_error("onchain/validators/{idx}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_service_ids_basic() {
        assert_eq!(
            DbServiceId::parse_list("10,20"),
            vec![DbServiceId(10), DbServiceId(20)]
        );
    }

    #[test]
    fn test_parse_service_ids_hex() {
        assert_eq!(
            DbServiceId::parse_list("0xa,0x14"),
            vec![DbServiceId(10), DbServiceId(20)]
        );
    }

    #[test]
    fn test_parse_service_ids_hex_overflow() {
        // 0xea9f727c = 3936318076 as u32, wraps to -358649220 as i32
        let result = DbServiceId::parse_list("0xea9f727c");
        assert_eq!(result, vec![DbServiceId(0xea9f727c_u32 as i32)]);
    }

    #[test]
    fn test_parse_service_ids_curly_braces() {
        assert_eq!(
            DbServiceId::parse_list("{0xa,0x14}"),
            vec![DbServiceId(10), DbServiceId(20)]
        );
    }

    #[test]
    fn test_parse_service_ids_mixed() {
        assert_eq!(
            DbServiceId::parse_list("10,0x14"),
            vec![DbServiceId(10), DbServiceId(20)]
        );
    }

    #[test]
    fn test_db_service_id_display() {
        assert_eq!(DbServiceId(10).to_string(), "0x0000000a");
        assert_eq!(DbServiceId(0xea9f727c_u32 as i32).to_string(), "0xea9f727c");
    }

    #[test]
    fn test_db_service_id_serialize() {
        let id = DbServiceId(255);
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, "\"0x000000ff\"");
    }

    #[test]
    fn test_parse_node_list_basic() {
        assert_eq!(parse_node_list("node1,node2"), vec!["node1", "node2"]);
    }

    #[test]
    fn test_parse_node_list_curly_braces() {
        assert_eq!(parse_node_list("{node1,node2}"), vec!["node1", "node2"]);
    }
}
