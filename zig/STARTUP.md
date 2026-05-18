# Startup Status And Provisioning

## Goal

Startup, health, and public status paths must stay cheap even when a node holds a large local shard. They must not reopen a table DB, replay pending derived work, or trigger close-time flush/compaction just to answer diagnostic or provisioning questions.

## Request-Path Contract

The request path for:

- `GET /api/v1/tables/{name}`
- `GET /api/v1/tables/{name}/indexes/{index}`
- health/status endpoints
- metadata store-status heartbeats

must not call `DB.open()`.

These endpoints may return:

- a last-known-good local runtime snapshot
- a partial snapshot
- `unknown` / `null` when the shard is not open locally

They must not reopen shard state synchronously.

## Provisioning Contract

Metadata provisioning must not do full:

- `DB.open()`
- `replayPendingDerivedBatches()`
- `DB.close()`

on the hot metadata loop.

Provisioning should reconcile from cached runtime state or durable summaries first, then enqueue background reconcile/open work when needed.

## Long-Term Shape

### 1. Node-local runtime registry

Maintain a node-local registry keyed by local shard/group that owns:

- long-lived open local DB handles for actively served shards
- immutable runtime snapshots
- freshness timestamps
- topology/config generation or fingerprint

Runtime status consumers read snapshots only.

### 2. Split runtime status from provisioning

Two separate responsibilities:

- `ShardRuntimeRegistry`
  - cheap snapshot reads
  - best-effort, stale-safe
  - no open/replay on demand
- `ShardProvisioner`
  - background reconcile queue
  - rate-limited open/reconcile/update work
  - never on request handlers
  - never on the metadata hot loop

### 3. Durable summaries for unopened shards

Persist small summaries during normal write/maintenance flow:

- doc count
- per-index doc count
- HBC node count
- applied/replay sequence watermark
- maintenance timestamps

After restart, status and provisioning can answer most questions from these summaries without opening the whole DB.

### 4. Generation-based invalidation

All cached snapshots need conservative invalidation on:

- topology epoch changes
- local shard generation changes
- index-config/schema fingerprint changes
- replay/apply watermark changes

Stale is acceptable. Invented freshness is not.

### 5. Explicit DB open modes

`DB.open()` must stop smuggling writer-side recovery into read/status paths.

Long-term contract:

- `DB.open(..., .query_readonly)`
  - mounts durable primary/index state
  - does not run `replayPendingDerivedBatches()`
  - does not start derived/index workers
  - does not start optional maintenance runtimes
- `DB.open(..., .status_only)`
  - cheaper summary/status open
  - no replay
  - no workers
  - no optional runtimes
- `DB.open(..., .writer)`
  - may replay pending derived work
  - may start workers and optional runtimes

Read/status correctness then comes from explicit replay debt, not from forcing open-time repair.

### 6. Replay debt must be durable and visible

Per derived index we need durable watermarks/status such as:

- `applied_sequence`
- `pending_sequence` or equivalent derived target
- `catch_up_required`

Then query/status opens can:

- read durable state as-is
- expose replay debt in status/metrics
- optionally schedule background catch-up
- never block request paths on journal replay

### 7. Replay must become chunked streaming apply

Replay should stop building a large process-wide unique-string window before apply.

Target shape:

- stream journal records in bounded chunks
- apply directly into index-specific catch-up sessions
- dedupe within the chunk only
- checkpoint progress every chunk or every bounded byte budget

This keeps:

- startup latency bounded
- replay memory bounded
- replay accounting local to the chunk instead of global-window sized

### 8. Metadata projected status must be incremental

`MetadataHttpService.runRound()` must not repeatedly rebuild projected state from storage scans such as:

- `listProjectedTables`
- `listProjectedPlacementIntents`
- repeated `DocStore.scanPrefix`
- repeated LSM merge-cursor rebuilds

Instead:

- maintain a node-local projected metadata snapshot in memory
- update it incrementally on Raft apply / metadata head change
- publish immutable snapshots to readers
- rebuild from storage only on startup or explicit repair

`runRound()` should read projections, not reconstruct them.

### 9. Replay accounting must be incremental

Replay resource accounting must reserve/release on chunk boundaries and adjust incrementally on append/capacity growth.

It must not recompute whole-window tracked bytes on each append.

Preferred shape:

- reserve a local chunk budget up front, e.g. 4-16 MiB
- consume from that budget during replay append
- refill or release only at chunk boundaries
- release accumulated bytes once on chunk teardown

## First Slice

The first implementation slice is intentionally small:

- public status paths use already-open cached DB handles only
- if a shard/table is not already cached locally, runtime status returns `null`
- hosted sources do not reopen local DBs for status

This keeps correctness simple:

- no request-path replay
- no request-path open/close side effects
- no false status synthesis

It also gives a clean base for the proper runtime registry without keeping the current reopen-on-status behavior alive.

## Second Slice

The next implementation slice keeps request paths pure but makes cold startup useful:

- `ProvisionedGroupStorage` owns a shared runtime snapshot cache keyed by table name
- read/write status sources consult:
  1. live cached DB handles
  2. then shared runtime snapshots
  3. then `null`
- `DataServer` warms that shared snapshot cache in the background from metadata snapshots
- the warmer uses lightweight `DB.open()` settings:
  - no derived replay
  - no index workers
  - no TTL cleanup
  - no transaction recovery
  - no text merge

This is still not the full node-local runtime registry. It is a conservative bridge:

- request/health paths still never open shard DBs
- stale snapshots remain acceptable
- cold loaded startup can return real runtime status once the background warm finishes
- provisioning and runtime status are still separate concerns

## Open Performance Plan

Cold open performance should improve without changing correctness by default.
Relax open-time replay semantics only where the caller can expose replay debt
and gate readiness explicitly.

Constraints:

- keep default `DB.open()` behavior stable until measurements show replay is the
  dominant remaining cost
- prefer latency work that does not widen correctness risk:
  - instrumentation
  - parallel index open
  - cache warming
- do not make swarm readiness depend on assumptions that are not already
  represented in current health wiring

### Open Path Instrumentation

Opt-in structured timing covers:

- `DB.open()` phase timings
- `IndexManager.load()` per-index open and backfill timings
- derived catch-up collection and apply timings

`bench/storage/open_bench.zig` makes those timings comparable across changes.

Status: complete.

### Parallel Index Load

`IndexManager.load()` may open configured indexes in bounded detached jobs, then
merge results serially in catalog order.

Guardrails:

- no change to `add()` / `addAllNoBackfill()`
- pre-create index directories before parallel work
- bound parallelism
- keep merge deterministic

Status: in progress.

Current benchmark signal:

- `bench/storage/open_bench.zig --docs 200 --batch-size 25 --indexes-text 2 --indexes-dense 1 --indexes-sparse 1 --stage-backlog --index-open-parallelism 1`
  - `open_ms=9.056`
- `bench/storage/open_bench.zig --docs 200 --batch-size 25 --indexes-text 2 --indexes-dense 1 --indexes-sparse 1 --stage-backlog`
  - `open_ms=5.622`

That is roughly a `1.6x` improvement on the replay-heavy reopen case before
touching replay semantics.

### Provisioned Cache Warmup

After store registration, the data server warms owned tables through existing
provisioned read/write caches:

1. warm writer DBs
2. warm read/query DBs
3. refresh best-effort runtime status so cached replay-debt metrics are
   available after warmup

This shifts `DB.open()` cost off the first request without changing request
semantics.

Status: in progress.

Current progress:

- swarm startup requests provisioned-cache warmup immediately after store
  registration
- warmup opens writer caches before read/query caches for local table groups
- `bench/storage/provisioned_warmup_bench.zig` compares cold vs warmed first
  lookup/write paths against the real provisioned cache warmup flow
- `bench/storage/raft_apply_bench.zig` measures the adjacent raft-backed
  apply-store path on the same document workload shape
- `bench/storage/managed_host_wal_bench.zig` measures the full managed-host
  proposal path with `.replica_state_backend = .wal`
- `data/runtime.zig` exports warmup and cached replay-debt metrics:
  - `antfly_data_api_requests_total`
  - `antfly_data_api_first_request_elapsed_ms`
  - `antfly_data_provisioned_warmup_*`
  - `antfly_data_provisioned_{read,write}_cache_{hits,misses}_total`
  - `antfly_data_runtime_status_refresh_*`
  - `antfly_data_replay_debt_*`
  - `antfly_data_runtime_status_*`

Current warmup-bench signal on
`--docs 200 --batch-size 25 --body-repeat 8`:

- first lookup: `9.750 ms` cold -> `0.134 ms` warmed
- first write batch: `113.307 ms` cold -> `6.740 ms` warmed

Current raft-apply-bench signal on the same workload:

- raft apply total: `8.199 ms`
- max raft apply batch: `1.183 ms`
- reopen latest batch + state read: `0.066 ms` reopen,
  `0.247 ms` group-state scan

Current managed-host-wal-bench signal on the same workload:

- leader election: `3.225 ms`
- propose + commit + apply: `30.830 ms` total, `4.964 ms` max batch
- restart: `1.506 ms`
- WAL/apply indexes after restart: `201` persisted, `201` applied,
  `201` latest commit index

### Open Performance Follow-Through

Only continue into replay-mode changes if new data still shows replay backlog
dominating cold open after instrumentation, parallel index load, and cache
warmup.

Follow-through order:

1. re-measure after warmup and parallel index load
2. add a swarm-specific background replay mode for provisioned writer-cache
   opens first
3. keep default `DB.open()` behavior blocking until readiness semantics are
   proven
4. use per-index replay debt/runtime status to build a data-server readiness
   view
5. do not attach replay-debt readiness directly to metadata-only swarm health
   without an explicit composite readiness design
6. move post-replay text merge off the critical path if replay still leaves
   expensive text merge work on startup

First-slice non-goals:

- no parallel blocking replay yet
- no broad thread-pool abstraction yet
- no global change to `DB.open()` replay defaults
- no readiness contract change in the first slice

## Startup Execution Order

Implementation order:

1. remove `replayPendingDerivedBatches()` from query/status open modes
2. add durable per-index replay watermarks and replay-debt status
3. convert derived replay from global window collection to chunked streaming apply
4. make metadata projected status incremental/in-memory instead of scan-based
5. finish incremental replay accounting cleanup

Current status:

- step 1 is implemented
- step 2 is implemented and exposed in runtime/index status
- step 3 is in progress:
  - `catchUpIndex` now streams matching replay records in bounded chunks
  - chunk-local dedupe replaces the large global replay window on the worker path
  - replay debt probing also uses bounded streaming scans
  - chunk byte tracking now feeds the shared `ResourceManager`
  - the old coalesced window helpers have been removed from the live path and deleted from `change_journal.zig`
  - focused replay verification now reaches source, worker, DB status, runtime-summary, and API aggregation layers
  - remaining work is optional broader end-to-end replay verification once repo-wide test noise is clear
- step 4 is in progress:
  - metadata round logic now has an initial shared projection-input path for provisioning/schema refresh
  - `MetadataHttpService` now keeps an immutable projected snapshot cache for tables/ranges/stores/placements/schema-progress/restore-progress/replication-statuses/transitions
  - round-local capture now clones directly from that cached snapshot instead of bouncing through scan-backed projected-list paths
  - local restore/schema refresh now uses cached projected progress slices from that same snapshot
  - remaining work is only to extend the snapshot if some new projected read later proves hot enough to justify it

### Step 3 Task List: Chunked Streaming Replay

- done: `DB.open(..., .query_readonly/.status_only)` no longer runs pending derived replay
- done: durable replay debt/status is exposed per index
- done: `catchUpIndex` streams bounded matching replay records into chunk-local batches
- done: replay-debt probing uses bounded matching-record scans instead of materializing a replay window
- done: the dead `replay_source.collectWindow(...)` wrapper is removed
- done: streaming replay now enforces an initial per-chunk byte budget before admitting another record
- done: chunk-budgeted replay accounting now feeds the shared `ResourceManager`
- done: focused replay verification now covers stop/max-match behavior on both replay sources, byte-budget chunking on the worker path, and worker replay against the primary-store source
- done: old coalesced replay-window helpers are gone from `replay_stream` and `change_journal`
- done: focused replay verification now covers stop/max-match behavior on both replay sources, byte-budget chunking on the worker path, worker replay against the primary-store source, DB replay debt/status on reopen, runtime snapshot replay-debt summary, and API index-status aggregation
- remaining: optional broader end-to-end replay verification once unrelated repo compile blockers are clear

### Step 4 Task List: Incremental Metadata Projection

- done: one round-local `group_ids + tables + ranges` snapshot is reused across schema/provisioning refresh
- done: one round-local placement/transition snapshot is reused across reconcile-gated refresh work
- done: restore-intent completion reuses the same round-local tables/ranges/placement snapshot
- done: store-status sync reuses the `adminSnapshot()` projected stores/tables/ranges/placements/transitions it already captured
- done: `MetadataHttpService` now maintains an immutable projected snapshot cache updated by projection/placement/transition epochs
- done: round-local projection/placement/transition capture now clones directly from the cached projected snapshot
- done: schema-progress, restore-progress, replication-source-status, and shuffle-join-lease reads now also come from the cached projected snapshot
- done: local provisioning/schema refresh now consumes cached projected progress instead of issuing separate projected-progress reads
- done: restore-intent completion now reuses round-local projected restore progress instead of re-reading it
- done: `MetadataHttpService.adminSnapshot()` now clones projected core state directly from the cached snapshot instead of rebuilding that core slice through the generic capture helper
- done: HTTP local schema-progress refresh now reuses round-local projected schema progress instead of re-reading it
- done: HTTP store-status reporting now clones projected stores directly from the cached snapshot instead of going back through generic projected-list helpers
- done: local bootstrap status remains a direct host-memory read rather than projected-snapshot state; it is already in-memory and does not benefit from projection-store caching
- remaining: extend the cached snapshot only if some other repeated projected read shows up hot enough to justify it
- remaining: leave storage-scan rebuild as startup/repair-only fallback

Expected payoff:

- biggest latency win: step 1
- biggest control-plane CPU win: step 4
- biggest memory/safety win: step 3
- cleanup/perf polish: step 5
