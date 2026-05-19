# Antfly Status Subsystem

This document describes where the runtime status subsystem is today, what the
production shape should be, and the changes needed to get there.

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
  - Publishers should refresh an existing cached group status by overlaying
    cheap live counters. Full `DB.stats()` is a cold-start/deep-stats fallback,
    not the steady-state hot publish mechanism.

- `pkg/antfly/src/data/runtime.zig`
  - `DataServer.runRuntimeStatusRefresh()` is the main background refresh path.
  - It inspects metadata, determines locally owned groups, collects local
    snapshots, and replaces `runtime_status_cache`.
  - It avoids opening an actively catching-up group and can reuse cached or
    managed-writer snapshots when opening the DB would be unsafe or expensive.

## Current Problems

The current shape is close, but the contract is still implicit and incomplete.

### Current E2E Failure Map

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

## Desired Production Shape

The long-term design is an observability plane.

Runtime components publish status as they do work. HTTP, metrics, readiness, and
admin endpoints read cheap snapshots from that plane. Background workers perform
repair and catch-up. Request handlers never repair state just to make status
look current.

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
- `DB.diagnosticStats()` is the deep inspection API.

`DB.stats()` must be cheap, bounded, and safe for background status publishers.
It should assemble a snapshot from already-maintained in-memory counters,
published index visibility, replay watermarks, async worker state, resource
manager snapshots, and lightweight persisted metadata that can be read with a
point lookup. It is allowed to allocate the returned `DBStats` tree, but it
must not perform unbounded storage/index work.

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
- Runtime-status publishers: `DB.stats()`.
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

### Metrics

Expose status-plane health separately from table/index health:

- runtime status cache table/group/index counts
- stale status count and max age
- missing expected group count
- last refresh duration and failures
- publisher failures
- remote status propagation lag, once remote propagation exists
- replay debt counts and backlog from cached status only

## Implementation Plan

### Phase 1: Stabilize The Current Contract

1. Keep HTTP status request handling cheap.
   - Preserve the read-source-first behavior in `ApiHttpServer`.
   - Do not merge in live write-source statuses from the request path.
   - Remove request-path DB drains/catch-up from status code.

2. Add freshness metadata to runtime cache entries.
   - Extend `LocalTableRuntimeStatus` or wrap it in a new status-plane record.
   - Include `updated_at_ns`, source, generation, and freshness.
   - Keep `DBStats` ownership/deinit rules clear.

3. Split operational and diagnostic DB stats.
   - Move the current expensive `DB.stats()` implementation to
     `DB.diagnosticStats()`.
   - Rebuild `DB.stats()` as a bounded operational snapshot.
   - Make `DB.stats()` avoid primary scans, snapshot/range opens, rebuild-state
     walks, cold index loads, replay drains, and bulk-session finishing.
   - Add or wire maintained counters for any field needed by operational
     status instead of falling back to a scan.
   - Move debug/admin callers that need deep validation to
     `DB.diagnosticStats()`.

4. Make the encoder topology-aware.
   - Teach index status encoding about expected groups from metadata or a
     precomputed table runtime topology snapshot.
   - Represent missing expected groups explicitly.
   - Ensure aggregate readiness does not ignore missing/stale groups.

5. Add tests for absence semantics.
   - One fresh group plus one missing expected group must not encode as ready.
   - Stale status should be visible and should block ready.
   - Synthetic configured status should report configured indexes but not fake
     completion.

### Phase 2: Improve Publisher Coverage

Current Phase 2 local-owner coverage:

- Managed enrichment runtime status changes notify the DB visibility hook after
  retry, failure, progress, and idle status writes.
- Managed DB visibility/status hooks publish into `runtime_status_cache` from
  the owner DB handle already in memory. They do not open DBs or drain work from
  HTTP status reads.
- Owner write paths publish best-effort status snapshots after local writes and
  committed transaction resolution, so replay debt creation is observable in the
  cache when the owner has a live DB handle.
- Startup catch-up publishes opening, catch-up, artifact rebuild, and idle
  phases with the runtime status metadata added in Phase 1.
- Publish failures invalidate the table runtime snapshot rather than preserving
  stale ready state.

1. Publish managed enrichment retry transitions.
   - When enrichment records retryable errors, publish/update cached runtime
     status with `backfill_state=retrying` inputs.
   - Do not require status endpoint polling to open or drain the DB.

2. Publish replay debt transitions.
   - When replay debt is created or cleared, update the cache.
   - The replay journal remains the durable source of work; cache is the
     observable projection.

3. Publish startup catch-up progress.
   - Continue using the active catch-up preservation path, but include explicit
     phase/freshness/source metadata.

4. Add unit tests around publishing.
   - Enrichment retry updates cache.
   - Replay debt updates cache.
   - Catch-up active group is preserved during refresh.

### Phase 3: Background Refresh Discipline

Current Phase 3 local refresh coverage:

- `DataServer.runRuntimeStatusRefresh()` now runs through an explicit DB-open
  budget. The default refresh worker uses a bounded per-run budget, and tests
  can exercise lower budgets directly.
- Refresh publishes cached stale status or synthetic configured placeholders
  when the DB-open budget is exhausted instead of stampeding every local group.
- Refresh publishes explicit `missing` synthetic status for expected local
  groups whose DB path is absent, instead of silently dropping the group from
  the cache.
- Refresh preserves cached owner-published status while it reports active
  background work such as enrichment retry, replay debt, startup catch-up, or
  dense catch-up. That prevents a background sampler from opening a second DB
  over a shard that is already doing the work that status should observe.
- Refresh samples DB stats with a direct `DB.open(.status_only)` configured
  with index workers, TTL cleanup, transaction recovery, and text merge
  disabled. It no longer routes through metadata-driven managed index
  reconciliation for status sampling.
- Health metrics expose the most recent refresh DB opens, skipped DB opens, and
  placeholder group count alongside table/group/duration counters.

1. Budget DB opens in `runRuntimeStatusRefresh()`.
   - Keep DB opens out of HTTP handlers.
   - Bound refresh work per interval so large clusters do not stampede local
     disks.

2. Make refresh topology-aware.
   - Emit placeholder status for expected groups that are non-local or missing.
   - Preserve valid cached state when local group ownership is ambiguous.

3. Separate refresh from repair.
   - Refresh samples and publishes.
   - Startup/replay/enrichment maintenance repairs.
   - HTTP reads observe.

### Phase 4: Distributed Status Propagation

Current Phase 4 implementation shape:

- The distributed status plane uses the existing metadata store heartbeat path.
  Store records now carry `runtime_statuses` separately from placement-oriented
  `group_statuses`, keyed by table/group/store/node identity.
- Data owners publish compact runtime summaries from their in-memory
  `runtime_status_cache`. Heartbeats do not open DBs or trigger repair work;
  they serialize already-published owner status.
- Runtime summaries contain table/group identity, freshness/source metadata,
  topology/status generations, compact table/enrichment state, and per-index
  counters needed by index status responses.
- API nodes merge local read-cache status with propagated remote store records.
  Local status wins for a group; remote records fill groups the API node cannot
  observe locally. There is still no request-time fanout to data owners.
- Raft metadata encoding appends the new runtime summary payload after existing
  store group-status fields, so older persisted store records decode with empty
  runtime status.
- Focused in-process API tests cover the intended distributed status contract:
  propagated remote status is used by non-owner API paths, status from a
  removed owner is ignored once placement changes, and missing remote shard
  status remains not-ready instead of being treated as success.
- Live split-process E2E does not currently satisfy that contract. The current
  failure is an API-only process serving synthetic configured status even after
  the data owner has published runtime status into the metadata heartbeat.
  Treat the live heartbeat/metadata-snapshot/API merge path as active work, not
  as production-complete.

1. Add a cluster-visible status plane.
   Options:
   - metadata store records keyed by `(table_id, group_id, store_id)`
   - store heartbeat payloads containing compact runtime status summaries
   - lightweight gossip between data servers

2. Owner nodes publish their shard statuses.
   - Include topology generation and owner identity.
   - Expire old-owner statuses by generation/lease.

3. API nodes aggregate local cache plus propagated remote records.
   - No request-time fanout.
   - Missing remote records remain explicit.

4. Add distributed tests.
   - Non-owner API node can report remote shard status from propagated cache.
   - Removed owner status is ignored after topology generation changes.
   - Missing remote shard status is reported as unknown, not ready.

## Remaining Production Work

The current implementation has the right shape for the status subsystem:
request handlers read cheap snapshots, owners publish runtime state as they do
work, and API nodes consume propagated owner status without request-time fanout.
The remaining work is production hardening around scale, observability, and
failure handling.

- Add live split-process e2e coverage for the real metadata heartbeat path.
  Existing Zig multi-node tests cover routing and API aggregation behavior, but
  they do not prove that a separate API-only process can answer index status
  from a data owner's propagated heartbeat.

- Tune heartbeat payload size if many indexes per table or many groups per
  store. The current payload is compact enough for normal tables, but very large
  index counts can make store heartbeats too large or too frequent. The
  scalable shape is to keep the regular heartbeat summary bounded, then add
  pagination, deltas, or a detail endpoint for rare high-cardinality status
  inspection.

- Add status-plane health metrics for remote propagation. Track heartbeat
  runtime-status bytes, runtime-status group/index counts, dropped summaries,
  encode/decode failures, propagation age, and max stale age per store. These
  metrics should describe status subsystem health without opening table DBs.

- Decide an explicit expiry policy for propagated runtime status. API nodes
  already ignore status from stores that do not own the current placement, but
  production should also age out stale owner records by store lease, topology
  generation, or heartbeat timestamp so old metadata snapshots cannot report a
  dead shard as healthy.

- Split summary from detail if heartbeat size grows. Index list/get readiness
  only needs per-index counters and freshness. More verbose diagnostic state
  should live behind an admin/debug path or be fetched on demand from the
  status plane, not attached to every store heartbeat indefinitely.

- Add integration coverage for degraded cases once cluster process orchestration
  is stable: owner process stopped, stale heartbeat, table placement moved, and
  API-only node with no local shard. These should assert explicit missing or
  stale status, not readiness.

## Near-Term Recommendation For The Current Bug

The managed embedding, schema migration, and distributed-status E2E failures are
all status-plane gaps until a focused diagnostic proves otherwise. They are not
a reason to make status calls drive the DB.

The correct fix is:

1. Keep `ApiHttpServer.localTableRuntimeStatuses()` read-source-first.
2. Ensure the live managed writer/enrichment runtime publishes retry/progress
   status into `runtime_status_cache` when retry state changes.
3. Ensure schema migration and versioned full-text rebuild progress publish
   enough runtime status for the public index detail path to distinguish
   missing work from unpublished completion.
4. Fix the live split-process heartbeat merge path so API-only nodes consume
   propagated owner runtime status before falling back to synthetic configured
   status.
5. Ensure background refresh preserves live-published status while the
   writer is active or retrying.
6. Make missing/stale runtime data explicit in the status response.

That preserves production performance while making status accurate enough for
e2e tests and operators.

## Non-Goals

- Do not make HTTP status endpoints a maintenance trigger.
- Do not do request-time remote fanout for status.
- Do not make every status request open every local group DB.
- Do not use full table scans for table/index coverage counts.
- Do not treat missing runtime status as success.
