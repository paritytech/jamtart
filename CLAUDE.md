# TART — JAM Telemetry Backend

Rust/Axum backend that ingests telemetry from JAM blockchain nodes (binary TCP, JIP-3),
stores in TimescaleDB, and serves via REST API + WebSocket. Grafana dashboards visualize everything.
Target: 3M events/s from 1024 nodes — performance-critical design decisions are intentional.

API: `localhost:8080` (local dev only — the real stack runs remotely at `https://jamtoaster.network/api`; a stale local instance can look healthy while serving old data)
TEST-DB: `postgres://tart:tart_password@localhost:5432/tart_test`
DB: `postgres://tart:tart_password@127.0.0.1:5432/tart_telemetry`

## Key paths

- `src/grafana.rs` + `src/grafana_store.rs` — Grafana endpoints (active development)
- `src/api.rs` — REST/WS routes, `src/store.rs` + `src/batch_writer.rs` — legacy endpoints, don't touch unless asked
- `src/server.rs` — TCP ingestion, `src/enricher.rs` — cross-event correlation
- `grafana/provisioning/dashboards/` — dashboard JSONs (Infinity plugin, uid: `jamtart-api`)
- `docs/grafana-guide.md` — dashboard recipes and shared query types
- `docs/endpoint-doc-conventions.md` — rules for endpoint/schema doc comments (they become the public OpenAPI docs)
- `/api/docs/openapi.json` (served by the backend) — endpoint source of truth: every endpoint self-describes with its feeding events and the question it answers

## Grafana dashboards

All panels use the Infinity plugin to HTTP GET `localhost:8080/api/grafana/*` endpoints.

- **Curl the endpoint first** to see response shape before writing/editing panel config.
- The `tart-backend` skill has the live backend URLs and a debugging endpoint index.
- **Edit panels surgically** with the Edit tool — don't rewrite entire files or create Python scripts.
- Grafana variables: `$node`, `$core`, `$service`, `$interval`
- Time macros: `${__from:date:iso}`, `${__to:date:iso}`
- Read `docs/grafana-guide.md` for dashboard recipes; endpoint specs live in the OpenAPI doc.

## Key architecture assumptions

- **Dual-write ingestion:** every event is written to both `ingested_raw_events` (1h retention, browsing store with hot columns) and in-memory DashMap counters that flush every 5s to 14 per-group count tables (e.g. `status_counts`, `assurance_counts`, `segment_counts`). All 115 event types go through both paths.
- **Aggregate hierarchy:** no continuous aggregates over raw events — the count tables are the single aggregation source, with `_1m`/`_1h` continuous aggregates on top. UNION views (`all_event_stats_30s/1m/1h`, `all_core_stats_1m`) combine the 14 groups. Grafana endpoints auto-select tier based on time range. Migrations are a squashed 7-file baseline (2026-08); never drop a continuous aggregate created by an earlier migration (scheduler deadlock on fresh DBs).
- **`ingested_raw_events`** has 1h retention — queries against it (via the `events` view alias) only return the last hour of data. `store.rs` endpoints that query `events` without time bounds effectively get ≤1h of data.
- **Enricher** (`src/enricher.rs`): per-node stateful correlation. WorkPackageReceived is the source event — core, service_ids, submission_id propagate to ~30 downstream events via ID chains. Enriched fields are DB-only (not on WS broadcast path).
- **Hot columns** on `ingested_raw_events`: `slot`, `core`, `submission_id` — populated at ingestion, avoid JSONB queries.
- **Separate tables:** `event_services` (service×event junction with gas), `node_stats` (extracted Status fields), `wp_tracking`, `slot_convergence` — all written at ingestion time.

## External references (sibling checkouts of this repo)
- Telemetry events: `../polkajam/crates/jam-std-common/src/telemetry.rs`
- JIP-3 spec: `../JIPs/JIP-3.md` (public: https://github.com/polkadot-fellows/JIPs/blob/main/JIP-3.md)
- polkajam implementation (node): `../polkajam/`
- Infinity plugin source (when stuck): `../grafana-infinity-datasource/`

## Testing

Run `./run-tests.sh` — it sets up the test DB and runs all tests serially. It's slow, so run it once and read the full output. Don't re-run it repeatedly to grep/filter/count.

## Working style

- **Plan before coding.** Explain the problem and proposed approach before editing files; use plan mode to prepare the plan.
- **Research before guessing.** Read docs, curl endpoints — don't trial-and-error.
