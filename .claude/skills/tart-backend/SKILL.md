---
name: tart-backend
description: Query the TART telemetry backend to debug a JAM network. Triggers when investigating stuck or failing work packages, missing/slow blocks, guarantee or assurance propagation problems, data-availability latency, nodes not reporting telemetry, or any "what is the network actually doing" question about a JAM deployment.
---

# TART — debugging a JAM network through its telemetry

## When to use this

You are debugging a JAM network (e.g. jamtoaster) and need evidence: is the network
producing blocks, where do work packages stall, which nodes or cores misbehave, is
data availability keeping up. TART has the answers as queryable JSON — prefer curling
it over reading node logs.

## Mental model

JAM produces a block every 6-second slot. Compute happens off-chain on **cores**: a
**work package** is sent to a core's **guarantors**, who authorize and **refine** it into a
work report, sign it (**guarantee**) and distribute it; validators then **assure** that the
report's data is available (within a 5-slot window); once enough assurances land
on-chain, the result is **accumulated** into the state of a **service**. Validators author
blocks, guarantee, and assure.

Every polkajam node pushes a binary telemetry event stream (JIP-3 protocol, TCP port
9000) to TART: 115 event types covering block authoring/import, the work-package
pipeline, guarantee/assurance distribution, DA shard traffic, connections and node
status. TART ingests, correlates (e.g. attaching core and service to downstream
events of a work package) and aggregates them, and serves everything as JSON under
`/api/grafana/*`. It also samples the chain's own on-chain activity statistics per
block, served under `/api/grafana/onchain/*` — the on-chain truth to compare the
telemetry view against.

## Backends

| URL | What |
|---|---|
| `https://jamtoaster.network/api` | Public read-only deployment watching the jamtoaster network |
| `http://192.168.20.34:8080/api` | Same backend, direct on the LAN |
| `http://localhost:8080/api` | Local dev instance |

First commands:

```bash
BASE=https://jamtoaster.network/api
curl -s $BASE/health                       # is TART up
curl -s "$BASE/grafana/stats?start=$(date -u -d '5 min ago' +%FT%TZ)&end=$(date -u +%FT%TZ)"
```

`connected_nodes` > 0 and a moving `best_slot` mean the network is alive and reporting.

## How to work

- Nearly every endpoint requires `start` and `end` (ISO 8601, e.g. `2026-08-20T10:00:00Z`).
- Start wide (`/grafana/network-health`, `/grafana/stats`, `/grafana/failure-rates`),
  then drill into the subsystem that looks wrong using the index below.
- The full spec is self-describing: `curl -s $BASE/docs/openapi.json` — every endpoint's
  description states which JIP-3 events feed it and ends with the question it answers.
- Event catalogue: `/grafana/event-types` (id, name, group). Docs and API use
  `EventName(ID)` naming, e.g. `WorkPackageReceived(94)`.

## Endpoint index — pick by the question you're asking

**Is the network alive and healthy?**
- `/grafana/stats` — headline counters: nodes reporting, best/finalized slot, event rates
- `/grafana/network-health` — one score, broken into five protocol subsystems
- `/grafana/failure-rates` — what is failing, in which part of the protocol, on which nodes
- `/grafana/sync-timeline` — how many nodes are keeping up with the chain tip
- `/grafana/connections-timeline` — is peer connectivity stable or churning

**Are blocks being produced and propagating?**
- `/grafana/blocks/summary` — produced, verified, executed; which nodes author
- `/grafana/blocks/convergence` — how fast a new block reaches the network, per slot
- `/grafana/blocks/contents` — how full blocks are, which extrinsic types land on chain

**Where do work packages stall?**
- `/grafana/wp-funnel` (+ `-timeseries`) — how many WPs reached each pipeline stage
- `/grafana/bottlenecks` (+ `-timeseries`) — which stage dominates latency
- `/grafana/wp-active` — recent WPs: where stalling, why failing
- `/grafana/wp/{wp_hash}`, POST `/grafana/wp/batch` — full story of specific WPs
- `/grafana/wp-stats` — WP traffic totals with per-core breakdown

**Are guarantees flowing?**
- `/grafana/guarantees` — guaranteeing traffic and transfer success rates
- `/grafana/guarantee-convergence` (+ `/detail`) — propagation speed to the validator set
- `/grafana/guarantee-discards` — why guarantees leave the pool unreported
- `/grafana/guarantees/by-guarantor` — which nodes guarantee, for which cores

**Is data availability keeping up?**
- `/grafana/assurance-convergence` (+ `/senders`) — do assurances land inside the 5-slot window
- `/grafana/da-stats` — which nodes carry the DA load, which are slow or failing
- `/grafana/shard-latency`, `/grafana/bundle-latency`, `/grafana/segment-latency`,
  `/grafana/preimage-latency` — where transfer time is spent, requester vs responder side

**Which core / validator / node is the problem?**
- `/grafana/cores`, `/grafana/cores/{id}` (+ `/metrics`, `/validators`) — per-core load, latency, gas
- `/grafana/validator-profiling` (+ `-timeseries`) — slow or failing guarantors vs the rest
- `/grafana/validators/cores` — observed node→core mapping
- `/grafana/nodes` — every node ever seen: version, last heard from
- `/grafana/node-stats` (+ `-aggregate`) — Status(10) snapshots: peers, DA store, guarantee pool

**Which service is expensive?**
- `/grafana/services` (+ `/timeseries`) — per-service activity and gas
- `/grafana/execution` — gas and time per execution phase (authorize/refine/accumulate)

**What does the chain itself say?** (on-chain truth, not telemetry)
- `/grafana/onchain/cores|services|validators` (+ `/timeseries`, `/{id}`) — the chain's own
  per-block activity statistics; compare against the telemetry view above

**Raw evidence**
- `/grafana/events` — individual events as nodes reported them (last ~1 h only),
  filterable by type, node, core, work package
- `/grafana/timeseries` — event counts over time, grouped by type, core or node

## Digging deeper: the JIP-3 spec

The telemetry protocol itself is specified in **JIP-3**:
https://raw.githubusercontent.com/polkadot-fellows/JIPs/main/JIP-3.md
(often also checked out locally as a `JIPs/` sibling of this repo).

Don't read it up front — the endpoint docs and `/grafana/event-types` cover normal
debugging. Reach for it when the question moves from "which endpoint" to "what does
this event actually mean":

- exact payload fields of an event and their encoding
- when and by which node an event is emitted (author vs importer, primary vs
  secondary guarantor, sender vs receiver side of a transfer)
- semantics of enum values, e.g. guarantee discard reasons or failure kinds
- handshake / node-information fields, universal event fields (slot, timestamps)

The emitting side lives in polkajam (`crates/jam-std-common/src/telemetry.rs`). If the
sibling checkout is missing or the GitHub source isn't reachable, fall back to the public
docs: https://docs.rs/jam-std-common/latest/jam_std_common/

## Gotchas

- **Raw events are retained ~1 hour.** `/grafana/events` and the event drill-down in
  `/grafana/wp/{wp_hash}` go empty beyond that; aggregated endpoints keep history.
  Guarantee/assurance propagation records keep ~7 days.
- Counts are **per reporting node**: several guarantors report the same work package,
  author and importers all report the same block — totals are telemetry-observed
  events, not distinct protocol objects.
- Timeseries resolution is pre-aggregated (30 s / 1 min / 1 h picked from your range);
  finer `interval` values add no detail and unsupported ones snap to the nearest.
- A node that "cannot reach port 9000" usually speaks JIP-3 v0 — TART requires the
  v1 handshake and rejects v0 at connect time.
- Service IDs are zero-padded hex in responses (`"0x0000000a"`); inputs accept decimal
  or hex.
