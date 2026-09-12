# Antfly Status Subsystem

This document describes the design of the runtime status subsystem: how it
keeps status observation off the hot data path, how it represents freshness
and topology, and how owners publish state as they do work. Most of the shape
described here is implemented; genuinely open items are listed under
[Open work](#open-work) at the end.

## Goals

Status and metrics must be outside the hot data path.

Status endpoints must not make foreground requests slower by opening DBs,
running catch-up, draining async workers, syncing indexes, or depending on
search/read responsiveness. A node should be able to answer status from a cheap
snapshot even when the DB is under load, when a local shard is not present, or
when a remote owner is slow.

For distributed antfly, status must also be topology-aware. A node answering an
API request cannot assume all table groups are local or fresh. Missing runtime
data should be represented explicitly instead of being silently aggregated away.

## Current Shape

The current code already has the beginning of a status plane:

- `pkg/antfly/src/api/runtime_status.zig`
  - Defines `LocalTableRuntimeStatus`, `LocalTableRuntimeStatuses`,
    `TableRuntimeSnapshot`, and `TableRuntimeSnapshotCache`.
  - The cache stores in-memory per-table snapshots and supports full replace,
    group upsert, table invalidation, and summary counters.

- `pkg/antfly/src/api/provisioned_storage.zig`
  - Owns a shared `runtime_status_cache`.
  - Wires that cache into both provisioned read and write sources.

- `pkg/antfly/src/api/http_server.zig`
  - Index list/get handlers call `localTableRuntimeStatuses()`.
  - The HTTP layer first asks the read source for local runtime statuses.
  - The write source is not consulted from the request path. Missing read-side
    status is encoded as missing, stale, synthetic, or remote status from the
    status plane rather than repaired by poking a live writer. This is
    intentional for hot-path performance.

- `pkg/antfly/src/api/table_reads.zig`
  - `ProvisionedTableReadSource.localRuntimeStatuses()` prefers
    `runtime_status_cache.snapshot(table_name)`.
  - It falls back to read-cache snapshots when no shared runtime-status cache is
    available.
  - `HostedProvisionedTableReadSource.localRuntimeStatuses()` returns `null`,
    so status does not fan out to remote shard owners from the request path.

- `pkg/antfly/src/api/table_writes.zig`
  - Write/startup paths publish snapshots with
    `publishRuntimeStatusSnapshot*()`.
  - The write source also has best-effort helpers for cached writers, startup
    catch-up, replay debt, and managed writer overlays.
  - Those helpers are publisher/background paths. Public status handlers must
    not call them directly because they can inspect live writers, take apply
    locks, or finish pending index work.
  - Publishers can overlay cheap live counters onto retained group status,
    but establishing fresh source counts or index inventory requires
    `DB.runtimeStatusStatsConsistentIfAvailable()`. Contention preserves the
    cached observation or defers a cold publication; operational `DB.stats()`
    telemetry does not establish this authority.

- `pkg/antfly/src/data/runtime.zig`
  - `DataServer.runRuntimeStatusRefresh()` is the main background refresh path.
  - It inspects metadata, determines locally owned groups, collects local
    snapshots, and replaces `runtime_status_cache`.
  - It avoids opening an actively catching-up group and can reuse cached or
    managed-writer snapshots when opening the DB would be unsafe or expensive.

## Design Rationale And Known Constraints

The subsections below record the problems that shaped the current design.
Most have since been addressed by the phases described later in this
document; each notes where. The dated E2E observations and the
request-path-repair rule remain live constraints.

### Dated E2E Observations

Observed on 2026-05-01:

- `e2e/antfly/test_distributed_status.py::test_non_host_api_reports_remote_index_status_from_metadata_heartbeat`
  is the clearest status-subsystem failure. The data owner publishes runtime
  status into metadata heartbeat, but the API-only process still reports
  `runtime_source = "synthetic_config"` with `expected_groups = 0` and
  `reported_groups = 0`. That means the in-process/unit-level distributed
  status contract is not yet proven in the real split-process heartbeat path.
- Managed embedding index lifecycle failures are status-plane publisher
  failures until proven otherwise. The indexes can often answer queries or make
  progress, but index detail readiness does not reliably reflect that progress
  after rate-limit recovery, provider pacing, delete/recreate, or artifact
  corruption recovery.
- The schema migration full-text rebuild failure has the same status-plane
  shape: `full_text_index_v1` is created, but readiness does not reach the
  expected state in the public status path before timeout. The next diagnostic
  step is to determine whether rebuild work is missing, stuck, or complete but
  unpublished.
- CDC failures are metadata status-summary failures, not table runtime-status
  failures. Snapshot import and streaming changes succeed, but `/status`
  counters such as `projected_replication_source_statuses_streaming` and
  `projected_replication_source_statuses_terminal_failed` do not match the
  projected replication source records exposed elsewhere.
- `test_occ_conflict_detection` returned HTTP 500 on the first stateless commit
  in the full E2E run, but focused transaction reruns now pass. If it recurs,
  it belongs to transaction correctness/error mapping, not runtime status
  publishing.

### Missing Shards Are Not First-Class

Index status aggregation only sees the runtime statuses supplied to the encoder.
If an expected table group has no cached runtime status, the encoder aggregates
the groups it does see. That can make "unknown" look like "ready" or "partially
ready" depending on the remaining groups.

Distributed status needs to distinguish:

- group is expected and fresh
- group is expected but stale
- group is expected but no local snapshot is available
- group is remote and unknown to this node
- group is being opened or catching up
- group is known failed

This gap is addressed: index status responses report `expected_groups`,
`fresh_groups`, `stale_groups`, `missing_groups`, and `unknown_remote_groups`
explicitly (see API Semantics below).

### Freshness Is Not Explicit

`LocalTableRuntimeStatus` carries `group_id` and `DBStats`, but not status-plane
metadata such as:

- source store id / node id
- topology or placement generation
- LSM root generation
- snapshot generation
- updated timestamp
- freshness/staleness reason
- whether the status is synthetic, cached, live-writer-published, or opened by a
  background refresher

Without this metadata, consumers cannot tell whether a status is current,
stale, from a previous owner, or synthesized from index configuration.

This gap is addressed: the runtime status record carries source, freshness,
generation, and update-time metadata (see Data Model below).

### Publisher Coverage Is Incomplete

Important runtime transitions can fail to publish promptly into
`runtime_status_cache`, especially managed enrichment/replay transitions:

- retryable enrichment failures
- partial retrying backfill
- replay debt becoming visible
- replay debt being cleared
- artifact rebuild progress
- startup catch-up phase changes

When those transitions are not published, the read-side status path correctly
avoids the live DB, but it returns stale data.

This gap is addressed: managed enrichment retry/progress, replay debt, and
startup catch-up phase changes all publish into `runtime_status_cache` (see
Publisher Coverage below).

### Request-Path Repair Is the Wrong Direction

Some recent fixes explored using status reads to drive replay catch-up, drain
cached writers, or open DBs for status. That can fix individual e2e timing
issues, but it is not the long-term shape. Status reads should observe
background work, not perform it.

Request-path status must not:

- call `db.stats()` on a hot DB
- open a managed DB just to answer status
- run `db.runUntilIdle()`
- call index sync/flush
- trigger replay catch-up
- block behind enrichment retry/backoff
- depend on local shard ownership

### Cluster Scope Is Local-Only

Today the status cache is process-local. In a distributed deployment, a node can
only answer for the shards it owns or has locally refreshed. Hosted read sources
do not gather remote shard statuses from the request path, which is good for
latency, but the public API response does not yet expose that limitation
cleanly.

This gap is largely addressed: owners publish compact runtime summaries into
the metadata store heartbeat, and API nodes merge local cache with propagated
remote records without request-time fanout (see Distributed Status
Propagation below). Live split-process E2E proof of that path is still open
(see [Open work](#open-work)).

## Production Shape

The status subsystem is an observability plane.

Runtime components publish status as they do work. HTTP, metrics, readiness, and
admin endpoints read cheap snapshots from that plane. Background workers perform
repair and catch-up. Request handlers never repair state just to make status
look current.

### Durable safety and ephemeral activity are separate planes

Admission-critical facts remain in the metadata Raft projection: reporter
incarnation/fence, repair state, native-restore identity, and the durable
coverage/checkpoint identities used by readiness. That codec negotiates only
the exact v12 profile released by v0.2.0 or current v15. Every other numeric
version is an unreleased development artifact and is rejected.

Embedding work telemetry is a versioned, readiness-neutral heartbeat. The data
owner publishes exact phase transitions and coalesced counter progress; the
metadata leader retains matching store, group, index, generation, and config
identities in a bounded, sharded TTL cache. Report order and per-index sample
order are fenced separately from durable status generation, so activity-only
updates do not create Raft writes. Cached liveness heartbeats deliberately omit
the observation bit and cannot extend activity freshness. Local best-effort
snapshot contention retains the last incarnation-matched sample under the same
TTL instead of treating one missed lock as owner disappearance. Retained local
samples remain visible to standalone clients but are not forwarded as fresh
heartbeats, so repeated polling cannot extend another hop's TTL. An observed
owner/incarnation replacement clears immediately; otherwise activity expires to
unavailable after the TTL or a leader change. Status must never infer work from
coverage debt, and activity must never authorize a query or lifecycle transition.

### Data Model

Introduce a richer status record around the existing DB stats:

```zig
const RuntimeStatusSource = enum {
    synthetic_config,
    cached_snapshot,
    live_writer_publish,
    background_refresh,
    startup_catch_up,
    remote_store,
};

const RuntimeStatusFreshness = enum {
    fresh,
    stale,
    missing,
    remote_unknown,
    opening,
    catching_up,
    failed,
};

const TableGroupRuntimeStatus = struct {
    table_name: []const u8,
    group_id: u64,
    store_id: u64,
    node_id: []const u8,
    topology_generation: u64,
    lsm_root_generation: u64,
    status_generation: u64,
    updated_at_ns: u64,
    source: RuntimeStatusSource,
    freshness: RuntimeStatusFreshness,
    freshness_reason: []const u8,
    stats: DBStats,
};
```

The exact field names can differ, but the contract should include ownership,
generation, update time, source, and freshness.

### Publishing

Status should be published by runtime owners when state changes:

- write path after commits enqueue derived or enrichment work
- enrichment runtime when retrying, progressing, succeeding, or failing
- replay/catch-up worker when replay debt changes
- index manager when visibility or artifact state changes
- startup/reopen catch-up worker when phase or progress changes
- background refresh when it samples a shard

Publishing must be cheap and best-effort. Failure to publish should not fail the
write/query path; it should increment a metric and leave the previous snapshot
marked stale by age.

### DB Stats Contract

Split operational status from diagnostics:

- `DB.stats()` is the operational stats API.
- `DB.runtimeStatusStatsConsistentIfAvailable()` is the nonblocking coherent
  source-count/index-inventory API for runtime publication.
- `DB.diagnosticStats()` is the deep inspection API.

`DB.stats()` must be cheap and bounded.
It should assemble a snapshot from already-maintained in-memory counters,
published index visibility, replay watermarks, async worker state, resource
manager snapshots, and lightweight persisted metadata that can be read with a
point lookup. It is allowed to allocate the returned `DBStats` tree, but it
must not perform unbounded storage/index work.

When apply-lock admission fails, `DB.stats()` can return partial telemetry
without index rows or source cardinality. That is a missing observation, not
an observed empty table. It must not replace the published inventory or be
labelled as a fresh live-writer observation. A DB lease pins lifetime; it does
not prove that these facts were observed.

Runtime publishers use `runtimeStatusStatsConsistentIfAvailable()` and preserve
cached facts or defer publication when it returns `null`. A caller deliberately
owning a blocking observation boundary can use `runtimeStatusStatsConsistent()`.
Both coherent APIs retain bounded inventory/counter work; they do not authorize
scans, maintenance, or cold opens. Public HTTP handlers continue to read the
immutable status cache. Cached overlays retain their existing authority unless
a coherent observation establishes new facts.

`DB.stats()` must not:

- scan primary documents for cardinality
- open read snapshots or range cursors just to count status
- estimate rebuild progress by walking persisted rebuild state
- load or cold-open indexes
- run replay, enrichment, text merge, TTL cleanup, or transaction recovery
- finish bulk ingest sessions or publish pending HBC state
- take long apply locks or wait behind foreground writer/index work

`DB.diagnosticStats()` owns the current expensive behavior. It may take apply
locks, enumerate live index internals, inspect HBC/text/sparse/graph structures,
estimate rebuild progress, sample storage/cache state, or scan when explicitly
requested by admin/debug tooling. It should not be called from public status,
health, metrics, or normal runtime-status publication.

The intended caller split is:

- HTTP table/index status: `runtime_status_cache` and metadata heartbeat only.
- Runtime-status publishers: coherent runtime snapshots, with cached fallback
  or deferred publication on contention.
- Benchmarks that need operational status: `DB.stats()`.
- Debug/admin tools and tests that need deep validation: `DB.diagnosticStats()`.
- Embedded/C API status surfaces should prefer `DB.stats()` unless explicitly
  documented as diagnostic endpoints.

Any missing counter needed by `DB.stats()` should be added as an explicit
durable or in-memory maintained counter. Do not silently fall back to scans in
the operational path.

### Refresh And Repair

`DataServer.runRuntimeStatusRefresh()` should remain the background owner of
expensive sampling, but it should become topology-aware:

- enumerate expected table groups from metadata
- collect local owned groups only
- preserve valid cached status for groups that are busy or actively catching up
- synthesize explicit `missing`/`remote_unknown` entries for expected groups
  that cannot be sampled locally
- avoid opening DBs unless running in a background refresh budget
- never run replay or enrichment catch-up as part of an HTTP request

Replay and async repair should be handled by separate background workers:

- startup/reopen catch-up discovers debt from durable replay journals
- owner-side maintenance drains pending replay/enrichment work
- status refresh observes and publishes progress

### API Semantics

Index status responses should be explicit about partial knowledge.

For an index across N expected groups:

- `status.shards` should include one entry per expected group when debug or
  shard view is requested.
- Aggregate fields should be computed from fresh known groups.
- The response should include counts such as `expected_groups`,
  `fresh_groups`, `stale_groups`, `missing_groups`, and `unknown_remote_groups`.
- Readiness should require every expected group to be fresh and ready.
- Missing/stale groups should not be treated as ready.

This preserves backward-compatible simple fields where possible while making the
distributed reality visible.

Embeddings status additionally separates lifecycle truth from work telemetry:

- `incarnation`, `target_revision`, and `published_revision` identify the
  generation and its captured/published replay boundary.
- `milestones.queryable` and `milestones.complete` each contain `reached` plus
  blockers specific to that milestone. Clients should wait on the requested
  milestone instead of interpreting a generic state string.
- `source_coverage` reports generation-scoped source outcomes. Exact coverage
  requires a complete, fresh observation of every expected group; otherwise
  `pending` is `null` and `observation_incomplete_reasons` says why.
- `searchable_vectors` reports physical query-visible entries. It is not a
  source-document counter and may be larger for chunked indexes.
- Dense embeddings expose `publication.target_vectors`,
  `publication.searchable_vectors`, and `publication.complete`. The object is
  emitted only when an exact durable target exists for the current
  incarnation; completion requires equality, not a lower-bound comparison
  with source outcomes. Other index types should define publication facts in
  their own physical units rather than reuse vector semantics.
- `activity` reports volatile, incarnation-scoped work. Its counters are
  maintained by the owning enrichment runtime and aggregated only from current
  shard observations. A client may calculate throughput only across samples
  whose opaque activity `epoch` is unchanged. Owners report an authoritative
  phase independently of counters; aggregation reduces those phases with
  `waiting_retry > embedding > publishing > preparing > idle`. A counter from
  a lower-priority owner cannot mask a higher-priority phase.
- Serving authority, convergence authority, and activity freshness are
  independent. A cached published incarnation may remain queryable when the
  owner heartbeat is absent. `complete` additionally requires the group owner
  to have observed the latest accepted target; until then status reports
  `target_observation` without discarding the last coverage or publication
  counters.

These dimensions have a strict dependency direction:

```text
durable source outcomes + publication/repair/topology facts -> milestones
runtime work                                                -> activity only
```

Status encoders and waiters must never derive readiness from activity counters,
timestamps, or a worker appearing idle. Conversely, a process restart may reset
activity and its epoch without changing durable coverage or the queryable
publication. This keeps status useful for UX while preserving fail-closed
admission and bounded request-path performance.

For progressive dense indexes, restart queryability is certified by the exact
published physical count at the checkpoint's sequence and incarnation. A newer
artifact target describes work after that safe publication and does not revoke
it. Lazy posting centroid or quantized-payload debt is diagnostic maintenance,
not repair state: queries use the exact fallback while bounded background work
refreshes those caches. Structural generation faults remain repair blockers.

Wait clients select an explicit useful outcome such as `complete`,
`source-covered=N%`, or `searchable-artifacts=N`. Threshold waits also require
query admission for the same incarnation. The v0.2.0 response has no milestone
map, so clients use its historical fully-settled readiness rules only as a
separate compatibility path.

There is no request-time live overlay. A table commit marks the cached target
observation pending in a small monotonic status watermark, and the runtime owner
publishes replay target, coverage, physical artifact counters, and serving
state together. Each publication carries the target revision captured before
sampling, so an older in-flight publisher cannot clear a newer commit fence.
HTTP reads only clone that immutable snapshot. Writer/apply
lock contention can therefore delay convergence or activity observation, but
cannot revoke a published serving generation or reset its counters. Explicit
root replacement, exact-index mutation, and corruption fences remain the only
paths that can revoke the corresponding serving authority.

Local writable owners publish independent serving and coverage stamps, scoped
by the enclosing exact index identity and table root. A stamp contains a local
owner epoch, observation revision, and the applied replay prefix sampled before
the corresponding facts. Captures are serialized on the owner, not on HTTP
readers. New authoritative revisions replace facts even when counts decrease;
counts are not ordering tokens. Reusing a stamp with different facts is rejected.
Metadata relabeling, cloning, and replay-target overlays cannot mint stamps.

A successor owner also supplies an admitted, config-matching durable checkpoint
prefix. It may replace a previous payload only after that recovery proof covers
the payload's applied prefix. Older local owners cannot replace their successor.
Read-only/status-only observations cannot claim writable-owner succession.
These local stamps are not persisted or sent through the runtime wire codec;
distributed reporter fencing remains a separate authority.

Commit callbacks merge the maximum target sequence and maximum reducing
sequence independently. An older delete callback must not be discarded merely
because an additive callback arrived first. The merge is idempotent and uses
constant space per exact index/group. A callback watermark does not authorize
publication: an owner may publish intermediate progress before the newest
accepted delete is applied. Physical topology and coverage bucket counts are
not generally monotonic, even in the absence of source deletes.

Chunk retirement is reducing work for each vector projection bound to that
chunk source, even when surviving chunks reuse cached embeddings and emit no
new vectors. Replay eligibility and delete-key collection use the same source
scope. Generated projections delete chunk identities; multi-source projections
delete their configured embedding-artifact identities. Those identities are
derived from catalog bindings, not from already-deleted artifact rows, and
unrelated source projections remain untouched.

Distributed publication has two released profiles: v12 for v0.2.0 peers and
v15 for current admission/restore safety facts. V13 and v14 were development
artifacts and are rejected rather than negotiated. During rolling upgrades,
writers emit v12 until every metadata voter advertises v15; repair state,
reporter fences, native-restore identity, and exact publication targets then
activate together and remain mandatory/fail-closed. Embedding activity is not projected through this codec:
its separately versioned TTL heartbeat remains optional. Capability proofs are
scoped to the metadata incarnation, membership fingerprint, and required
profile.

### Metrics

Expose status-plane health separately from table/index health:

- runtime status cache table/group/index counts
- stale status count and max age
- missing expected group count
- last refresh duration and failures
- publisher failures
- remote status propagation lag, once remote propagation exists
- replay debt counts and backlog from cached status only

## Delivery Phases

The subsections below describe how the current contract was built up. They
are ordered as delivered; later phases depend on the metadata added by
earlier ones.

### Stabilizing The Status Contract

HTTP status handling stayed read-source-first throughout (`ApiHttpServer`
does not merge in live write-source statuses or drain/catch-up a DB from the
request path; see Current Shape above). The runtime cache record carries
`updated_at_ns`, source, generation, and freshness metadata (see Data Model
above). `DB.stats()` and `DB.diagnosticStats()` are split as described in the
DB Stats Contract above, so operational status stays bounded while deep
inspection lives behind the diagnostic API. Index status encoding is
topology-aware: it reports expected groups explicitly and does not let
missing or stale groups read as ready (see API Semantics above). Absence
semantics are covered by unit tests in `table_writes.zig`.

### Publisher Coverage

Local-owner publishers cover:

- Managed enrichment runtime status changes notify the DB visibility hook
  after retry, failure, progress, and idle status writes.
- Managed DB visibility/status hooks publish into `runtime_status_cache` from
  the owner DB handle already in memory. They do not open DBs or drain work
  from HTTP status reads.
- Owner write paths publish best-effort status snapshots after local writes
  and committed transaction resolution, so replay debt creation is observable
  in the cache when the owner has a live DB handle.
- Startup catch-up publishes opening, catch-up, artifact rebuild, and idle
  phases with the runtime status metadata described above.
- Publish failures invalidate the table runtime snapshot rather than
  preserving stale ready state.

Enrichment retry, replay debt, and startup catch-up transitions each have
unit test coverage in `table_writes.zig` confirming they update the cache.

### Background Refresh Discipline

Local refresh coverage:

- `DataServer.runRuntimeStatusRefresh()` runs through an explicit DB-open
  budget. The default refresh worker uses a bounded per-run budget, and tests
  can exercise lower budgets directly.
- Refresh publishes cached stale status or synthetic configured placeholders
  when the DB-open budget is exhausted instead of stampeding every local
  group.
- Refresh publishes explicit `missing` synthetic status for expected local
  groups whose DB path is absent, instead of silently dropping the group from
  the cache.
- Refresh preserves cached owner-published status while it reports active
  background work such as enrichment retry, replay debt, startup catch-up, or
  dense catch-up. That prevents a background sampler from opening a second DB
  over a shard that is already doing the work that status should observe.
- Refresh samples DB stats with a direct `DB.open(.status_only)` configured
  with index workers, TTL cleanup, transaction recovery, and text merge
  disabled. It does not route through metadata-driven managed index
  reconciliation for status sampling.
- Health metrics expose the most recent refresh DB opens, skipped DB opens,
  and placeholder group count alongside table/group/duration counters.

Refresh, repair, and observation stay separate: refresh samples and
publishes; startup/replay/enrichment maintenance repairs; HTTP reads observe.

### Distributed Status Propagation

The distributed status plane uses the existing metadata store heartbeat path.
Store records carry `runtime_statuses` separately from placement-oriented
`group_statuses`, keyed by table/group/store/node identity.

- Data owners publish compact runtime summaries from their in-memory
  `runtime_status_cache`. Heartbeats do not open DBs or trigger repair work;
  they serialize already-published owner status.
- Runtime summaries contain table/group identity, freshness/source metadata,
  topology/status generations, target-observation authority, compact
  table/enrichment state, and per-index counters needed by index status
  responses.
- API nodes merge local read-cache status with propagated remote store
  records. Local status wins for a group; remote records fill groups the API
  node cannot observe locally. There is no request-time fanout to data
  owners.
- Raft metadata encoding appends the new runtime summary payload after
  existing store group-status fields, so older persisted store records decode
  with empty runtime status.
- Focused in-process API tests cover the distributed status contract:
  propagated remote status is used by non-owner API paths, status from a
  removed owner is ignored once placement changes, and missing remote shard
  status remains not-ready instead of being treated as success.

Live, split-process E2E proof of this contract is not yet passing (see
[Open work](#open-work)): an API-only process has been observed serving
synthetic configured status even after the data owner published runtime
status into the metadata heartbeat.

## Open work

- Fix the live split-process heartbeat merge path so an API-only node
  consumes propagated owner runtime status before falling back to synthetic
  configured status (see Distributed Status Propagation above).
- Confirm schema migration and versioned full-text rebuild progress publish
  enough runtime status for the public index detail path to distinguish
  missing work from unpublished completion.
- Add live split-process e2e coverage for the real metadata heartbeat path.
  Existing Zig multi-node tests cover routing and API aggregation behavior,
  but they do not prove that a separate API-only process can answer index
  status from a data owner's propagated heartbeat.
- Tune heartbeat payload size if many indexes per table or many groups per
  store. The current payload is compact enough for normal tables, but very
  large index counts can make store heartbeats too large or too frequent. The
  scalable shape is to keep the regular heartbeat summary bounded, then add
  pagination, deltas, or a detail endpoint for rare high-cardinality status
  inspection.
- Add status-plane health metrics for remote propagation: heartbeat
  runtime-status bytes, runtime-status group/index counts, dropped summaries,
  encode/decode failures, propagation age, and max stale age per store.
- Decide an explicit expiry policy for propagated runtime status. API nodes
  already ignore status from stores that do not own the current placement,
  but production should also age out stale owner records by store lease,
  topology generation, or heartbeat timestamp so old metadata snapshots
  cannot report a dead shard as healthy.
- Split summary from detail if heartbeat size grows. Index list/get readiness
  only needs per-index counters and freshness; more verbose diagnostic state
  should live behind an admin/debug path or be fetched on demand from the
  status plane, not attached to every store heartbeat indefinitely.
- Add integration coverage for degraded cases once cluster process
  orchestration is stable: owner process stopped, stale heartbeat, table
  placement moved, and API-only node with no local shard. These should assert
  explicit missing or stale status, not readiness.
- Diagnose the dated E2E gaps recorded under Dated E2E Observations above
  (observed 2026-05-01, not yet confirmed resolved).

## Non-Goals

- Do not make HTTP status endpoints a maintenance trigger.
- Do not do request-time remote fanout for status.
- Do not make every status request open every local group DB.
- Do not use full table scans for table/index coverage counts.
- Do not treat missing runtime status as success.
