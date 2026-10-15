# Endpoint documentation conventions

The OpenAPI spec (swagger page, `/api/docs/openapi.json`) is generated from `utoipa`
doc comments in the source:

- `src/grafana.rs` — `///` on handlers (first line = summary, rest = description) and
  `description = "..."` strings inside `#[utoipa::path(...)]`, plus query-param structs.
- `src/grafana_types.rs`, `src/onchain_types.rs` — `///` on `ToSchema` structs and fields.

Everything in those doc comments is public API documentation. Write it for a consumer
of the API, at the JAM/JIP-3 level of abstraction — never for a reader of tart's code.
(Established by [issue #21](https://github.com/paritytech/jamtart/issues/21).)

## The four rules

1. **JIP-3 level of abstraction.** Describe endpoints in protocol terms — nodes,
   validators, cores, services, slots, work packages, guarantees, assurances, shards,
   segments, preimages, blocks — never in terms of how tart stores or aggregates data.
2. **`EventName(ID)` nomenclature.** Reference telemetry events as
   `WorkPackageReceived(94)`, `AssuranceReceived(131)`, etc. Canonical name↔ID table:
   `src/event_type_meta.rs`. On-chain endpoints reference no events — that data comes
   from the chain's state, so say so instead of inventing an event.
3. **No database internals.** No table names, SQL, column names, aggregate/rollup
   names, TimescaleDB/Postgres/JSONB/hypertable mentions, or storage types (i16/i32).
4. **State the question answered.** Every endpoint description ends with
   `Answers: <the operator question this data answers>.`

## Handler doc template

```rust
/// <One-line summary: what the endpoint returns, protocol vocabulary.>
///
/// <1–3 sentences: what each row represents, which events feed it as
///  EventName(ID), and semantics the consumer needs: windows, thresholds,
///  what "failed" or "converged" means.>
///
/// Answers: <the question>.
```

Before/after (from the #21 rewrite):

```rust
// BAD — documents the SQL
/// Work package pipeline bottleneck analysis with percentile timings.
///
/// Queries `wp_tracking` table using `percentile_cont(0.5)` and
/// `percentile_cont(0.95)` on the inter-stage timestamp deltas ...

// GOOD — documents the meaning
/// Where time goes inside the guarantor work-package pipeline.
///
/// Median and 95th-percentile durations of each stage a work package passes
/// through on its guarantors: authorize (WorkPackageReceived(94) →
/// Authorized(95)), refine (→ Refined(101)), ...
///
/// Answers: which pipeline stage dominates work-package latency, and how
/// often do work packages fail outright?
```

Response descriptions (`responses(description = ...)`) get one sentence about the
response shape ("Array of per-core rows, ascending by slot"), including mode switches
("with `interval`: one row per time bucket instead"). Schema struct docs say what one
instance represents; field docs give units and semantics ("milliseconds from X to Y").

## Translation table

| Instead of | Say |
|---|---|
| `wp_tracking` table | "per-work-package pipeline tracking" |
| `event_stats_1m` / "continuous aggregate" | "pre-aggregated counts (30 s / 1 min / 1 h resolution, auto-selected from the range)" — mention resolution only when it affects the consumer |
| `ingested_raw_events` | "recent raw events (retained ~1 hour)" — retention IS consumer-relevant, keep it |
| `guarantee_convergence` etc. | describe the measurement: "per-report guarantee propagation" |
| `SUM FILTER`, `COUNT(*)`, `time_bucket(...)` | drop; describe the result, not the computation |
| "JSONB payload" | "full event payload" |
| "stored as signed i32 in PostgreSQL" | drop |

Not leaks — keep them:

- Event **group** names (`status`, `blocks`, `wp_pipeline`, `assurances`, ...) — public
  API vocabulary returned by `/event-types` and accepted by `event_types` parameters,
  even where they coincide with table names. Make the context explicit: "the
  `wp_pipeline` event group".
- Protocol constants (5-slot availability window, 6 s slots), percentiles, and
  measurement precision ("percentiles are approximate, histogram-based").
- Consumer-facing formats: hex service IDs, accepted input formats, Grafana `{a,b}`
  multi-select syntax, pagination and caps, sort orders.
- Consumer-visible behavior phrased as behavior: retention windows, aggregation
  resolution, sampling cadence — never as a property of a named table.

## Special case

`GET /api/grafana/db-stats` is an operational endpoint about the collector's own
storage — naming TimescaleDB, hypertables and compression there is its subject
matter, not a leak. Its docs must open by saying it reports tart's internal storage
state, not JAM protocol data.

## Golden rule

Docs describe what the code **does**, not what it was meant to do. If they disagree,
fix the code or document the actual behavior — never document the intent.
