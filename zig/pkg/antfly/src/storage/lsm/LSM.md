# LSM Backend Performance Plan

This document tracks the near-term LSM backend performance work derived from the review of:

- `pkg/antfly/src/storage/lsm_backend.zig`
- `pkg/antfly/src/storage/lsm_backend/cache.zig`
- `pkg/antfly/src/storage/lsm_backend/recovery.zig`
- `pkg/antfly/src/storage/lsm_backend/storage_io.zig`

The goal is to pull in the highest-leverage lessons from RocksDB and Pebble without forcing an immediate table-format rewrite.

## Preferred Design

The LSM backend is the durable storage engine for performance-sensitive table
and index metadata. It should behave like an observability-friendly Pebble-style
engine: foreground writes append to WAL and mutate memory, while table-file
creation, compaction, cleanup, and status publication happen through bounded
background maintenance with short critical sections.

This is the source of truth for the desired implementation shape. If code and
this document disagree, treat the code path as transitional implementation debt
unless a later design note explicitly says otherwise.

### Foreground Write Contract

- Foreground commits append framed WAL records, sync according to the backend
  durability policy, and apply mutations to the active mutable memtable.
- Threshold-crossing commits may rotate the mutable memtable into the immutable
  queue, but they should not build table files or compact runs while holding DB
  apply locks or the backend mutex.
- Writers may perform a bounded maintenance assist only under hard write
  pressure. The assist must have a fixed work budget and must not turn into an
  unbounded compaction loop.
- WAL-backed stores must keep one physical writable owner for each root. Query,
  status, and read-only opens must be physically read-only and must never create
  or publish table files.

### Immutable Memtable Lifecycle

Immutable flush is a three-phase operation:

1. Under the backend mutex, detach or select one immutable generation, pin it
   against reclamation, and reserve any run IDs needed by the output.
2. Outside the backend mutex, build persisted table files or in-memory runs from
   the pinned immutable state.
3. Under the backend mutex, validate that the immutable generation is still the
   publish target, install the output runs, retire the pinned state, update WAL
   checkpoint metadata, and publish or mark the manifest.

Failures before publication leave the immutable generation replayable from
memory/WAL. Failures after publication must recover either the previous manifest
view or the newly published view; orphaned output files are cleanup debt, not
logical state.

### Compaction Lifecycle

Compaction follows the same short-critical-section pattern:

1. Under the backend mutex, choose a run-set, reserve output run IDs, and retain
   the selected input run snapshots.
2. Outside the backend mutex, build compacted output runs from those snapshots.
3. Under the backend mutex, validate that the selected run IDs still match,
   publish output runs, queue obsolete inputs, and update manifest state.

Persistent backends should use the unlocked-build path. Locked-only compaction
is acceptable only for simple in-memory or test backends that do not perform
slow storage IO.

### Lock Policy

- The backend mutex protects mutable metadata publication: run lists,
  immutable queue pointers, manifest flags, reader retention, caches, and
  counters that must stay consistent with those structures.
- Read transactions and scans must not rotate the mutable memtable. Replay
  scans that intentionally use the current mutable view must not clone it or
  create immutable flush debt on the writer path.
- Slow work must not run under the backend mutex: table encoding, file writes,
  manifest file replacement, WAL scanning, directory traversal, block reads for
  status, or compaction input/output construction.
- Lock wait loops must be bounded/adaptive. A contended backend must yield or
  park instead of burning cores in an unbounded `tryLock` spin loop.
- DB apply locks should guard DB/index catalog consistency. They should not be
  held while LSM table files are encoded, flushed, compacted, or deleted.

### Background Runtime And Backpressure

- LSM maintenance jobs use `backend_runtime` as their normal execution lane.
  That includes immutable flush, compaction build, cleanup, and manifest
  persistence where publication semantics allow it.
- Each backend may have at most one immutable flush publisher active at a time.
  Compaction concurrency is controlled by the compaction scheduler and
  ResourceManager budgets.
- Soft pressure schedules or accelerates background maintenance. Hard pressure
  can delay writes or require one bounded writer-assist step.
- Metrics must make pressure explicit: mutable bytes, immutable bytes, L0
  runs/bytes, compaction grants/denials, WAL retention/checkpoint lag, and
  maintenance job queue state.

### Status And Metrics Policy

Status is an observability plane, not a repair mechanism.

- Request-path status and metrics read cached snapshots only.
- Status handlers must not open DBs, run catch-up, drain workers, force index
  sync, call `DB.stats()` on hot writers, or open LSM read transactions just to
  answer an HTTP request.
- Writers and background workers publish status snapshots as they make progress.
  Missing or stale cached data should be reported as missing/stale, not repaired
  by probing live state from the request path.

### Acceptance Criteria

After changes to WAL, flush, compaction, manifest publication, HBC publish, or
ResourceManager pressure:

- `zig build lsm-backend-test` passes.
- 100k and 300k public-query guardrails complete without `InvalidTableFile` or
  manifest/table entry-count mismatch warnings.
- During 300k ingest, replay should keep making progress and must not remain at
  the same applied-batch count for an entire 25k-doc load window.
- Status and metrics requests stay cheap under load and do not appear in samples
  as live LSM read/repair work.
- Samples at 175k, 225k, 275k, and post-load should not show backend-lock waits
  dominating derived replay.

## Principles

- Make the read path keyed, not scan-based.
- Coordinate concurrent cache misses without spin/yield loops.
- Keep lock hold time short and move slow syscalls out from under hot locks.
- Evict from maintained shard-local state instead of rescanning the whole cache.
- Treat table metadata and block skipping as a separate phase because they touch the table format and reader contract.
- Treat compaction as background debt management, not as an all-or-nothing foreground write tax.
- Measure L0/run debt directly so write stalls, compaction scheduling, and bulk-ingest policy can be tuned from metrics instead of inferred from disk growth.

## Current Performance Checklist

Status: active

Use this checklist for the next performance loop: measure a baseline, implement
one bounded slice, rerun the same harness, and keep the change only if the
metric movement matches the expected mechanism.

### Baseline Commands

Read/scan path:

- `zig build lsm-backend-bench -- --samples 5 --keys 20000 --storage host --cache both > /tmp/lsm-read-before.jsonl`
- `zig build lsm-backend-bench -- --samples 5 --keys 20000 --storage host --cache both --concurrent-read-threads 16 --concurrent-read-keys 1024 --concurrent-read-repeats 8 > /tmp/lsm-read-concurrent-before.jsonl`
- `zig build lsm-backend-bench-compare -- --before /tmp/lsm-read-before.jsonl --after /tmp/lsm-read-after.jsonl`

Write path:

- `zig build lsm-write-bench -- --samples 5 --keys 20000 --storage host --mode both > /tmp/lsm-write-before.jsonl`
- `zig build lsm-write-bench-compare -- --before /tmp/lsm-write-before.jsonl --after /tmp/lsm-write-after.jsonl`
- Add `--wal-sync-on-commit` when measuring WAL sync latency and retention
  behavior under durable commit pressure.

Large-ingest guardrails:

- Run the 50k and 1M dense public/provisioned guardrails after any change that
  touches WAL, flush, compaction, manifest publication, HBC publish, or
  ResourceManager pressure.

### Read And Scan Work

1. [x] Move block-cache singleflight state from one global `pending_loads` map
   to shard-local pending maps.
   - Expected signal: lower cache pending mutex contention under concurrent
     miss-heavy reads; cache `waits` should still count coalesced followers.
   - Compare with `lsm-backend-bench` cache-on miss/reopen workloads.
2. [x] Add a benchmark mode that stresses concurrent point reads across cold
   cached blocks and records p50/p95/p99, cache waits, run probes, bloom
   negatives, and block loads.
   - [x] Surface existing table parse/block load and shared/local block-cache
     hit/miss counters in the read benchmark JSON and comparison output.
3. [ ] Implement a block-window cursor for persisted scans in the shared-cache
   path, matching the no-shared-cache table-index/block-window shape.
   - Expected signal: full/short scan throughput improves and cache pollution
     drops because scans stop pinning or materializing whole tables.
   - [x] First slice: when the shared block cache is configured, merge cursors
     now hold one cache block handle per source instead of duplicating cached
     block bytes into cursor-owned memory.
4. [x] Replace linear forward winner selection with heap-backed source
   selection for merge cursors.
   - Expected signal: scans with many sources improve in rows/sec and CPU per
     row; correctness must preserve tombstone/source precedence.
   - The existing loser-tree helper is integer-keyed, so the first production
     slice uses cursor-owned byte-slice heap state with the same ordering.
5. [ ] Return borrowed scan values until `next()` instead of allocating and
   copying every visible row.
   - Expected signal: scan CPU and allocator traffic fall, especially for wide
     rows.
   - [x] First slice: merge cursors now reuse cursor-owned scratch for source
     advancement, removing the per-row stable-key allocation while preserving
     block-release safety.
   - [x] Snapshot cursor-plan `getManySorted` now returns values borrowed from
     retained cache block handles or stable snapshot states instead of copying
     every hit into transaction-owned value buffers.
   - [x] Current/probe reads can now retain cached run-block handles for
     path-backed point hits, so persisted values survive lock release without a
     transaction-owned value copy.
   - [x] Live mutable merge cursors now reuse cursor-owned entry scratch across
     source movement, avoiding one allocation/free cycle per visible mutable row
     while preserving the valid-until-next cursor contract.
6. [ ] Cache per-cursor source layout (`runs`, L0 groups, lower levels, and
   immutable pointer slice) across repeated seeks while the cursor snapshot is
   valid.
7. [ ] Add table block smallest-key metadata and use block min/max bounds to
   skip non-overlapping range-scan blocks.
   - [x] First slice: table footer metadata now records per-block smallest
     key/namespace and exact point reads reject out-of-block candidates by
     min/max bounds before loading the block.
   - [x] Bounded forward scans can now pass an upper bound into erased cursors;
     persisted LSM merge cursors stop before that bound and skip later table
     blocks whose smallest key is already outside the scan range.
8. [ ] Add sequential scan readahead/prefetch hints for full-run scans.
   - [x] First slice: persisted merge cursors warm the next table block in
     the shared cache after loading the current block, while respecting scan
     upper bounds.
9. [x] Add prefix extractor and prefix bloom metadata for structured key
   families.
   - Table codec version 8 now records a first-separator prefix extractor,
     run-level prefix bloom, and per-block prefix blooms.
   - Persisted forward scans use prefix blooms to skip block loads when the
     scan upper bound proves the cursor cannot leave the extracted prefix.

### Point Read Work

1. [ ] Split point lookup into a bloom/range precheck phase followed by a read
   phase for surviving runs.
   - Existing run and block blooms are already consulted before block loads in
     the block-index path; the missing piece is avoiding one synchronous
     miss-at-a-time across candidate runs.
   - [x] First slice: point reads now consult the manifest-carried run bloom
     before loading a persisted run table index/block from `getFromRunIndices`.
2. [ ] Issue concurrent block reads for surviving point-read candidates where
   precedence allows it.
   - L0/tombstone semantics require resolving by source order, not by first
     completed read.
3. [ ] Add a borrowed-value point-read mode that can hold cache block handles
   until transaction end instead of duplicating every returned value.
   - [x] First slice: snapshot point-batch reads can return slices borrowed
     from retained block handles or immutable snapshots instead of copying every
     result into `held_values`. Current/live locked helpers still copy because
     they do not own a transaction-level block lifetime.
   - [x] Cursor scratch now uses one aligned backing allocation for all
     per-source arrays instead of allocating positions, heap state, block
     handles, and entry slots separately for every cursor open.
   - [x] Current/probe point reads now borrow values from retained cache block
     handles for path-backed run hits when the transaction owns a held-block
     list; live mutable hits still copy until their generation lifetime is
     explicitly pinned.
4. [ ] Make sorted `getManySorted` keep per-run cursor state across keys so
   batch reads resume inside the current block where possible.
   - [x] First slice: cached sorted-by-run reads now keep a per-run forward
     entry hint and bounded-scan from the previous hit before falling back to
     exact block lookup.
5. [x] Tune bloom defaults once the benchmark can show false-positive block
   loads.
   - Run-level and per-block LSM filters now default to 14 bits/key. The option
     remains overrideable through `Options.bloom` for stores that prefer smaller
     filters over lower false-positive rates.

### Write And Maintenance Work

1. [ ] Add explicit WAL checkpoint/retention metadata and retire covered
   segments incrementally instead of relying on clean full resets.
2. [ ] Export retained WAL bytes, oldest uncheckpointed segment, WAL truncation
   lag, immutable-memtable bytes, and WAL sync latency through status/metrics.
   - [x] First slice: backend write stats now expose WAL sync latency alongside
     sync record counts, while maintenance stats expose retained WAL segments,
     retained bytes, checkpoint lag, and replay retention.
   - [x] Bench slice: `lsm-write-bench` and `lsm-write-bench-compare` now emit
     and compare WAL append/sync/reset deltas plus retained-WAL after-state,
     with a `--wal-sync-on-commit` workload flag.
3. [ ] Replace recovery replay allocation churn with a bounded recovery
   allocation model that can release whole chunks after flush.
   - [x] First slice: state WAL recovery now reads segment chunks directly
     into the reusable pending replay buffer instead of allocating one chunk
     per read and retaining those chunks until segment replay exits.
4. [ ] Add final-state HBC bulk publication for sustained ingest so large loads
   avoid persisting every intermediate online mutation.
5. [x] Add background IO admission budgeting for maintenance work.
   - First slice: immutable flushes and scheduled compactions now reserve from
     a per-step background IO byte budget, can defer when the budget is
     exhausted, and expose budget/reserved/denied/oversized counters in
     maintenance stats.
6. [ ] Raise compaction concurrency only after the scheduler can prove selected
   jobs are non-overlapping or otherwise safe to run in parallel.
   - [x] Policy slice: scheduled maintenance now has a default-off
     `max_compaction_input_bytes` cap. Plan selection can skip oversized
     compactions and choose eligible smaller work instead of repeatedly
     admitting or remembering a plan larger than the configured policy budget.
7. [ ] Consider memtable structure changes after byte-budgeted WAL/flush and
   recovery allocation work are measured; the current active memtable appends
   plus hash-indexes writes and sorts on freeze/flush, so the main costs are
   flush sort, range iteration, immutable lookup, and memory layout rather than
   ordered-insert shifts.
8. [ ] Add table key-prefix compression with restart points after the scan and
   WAL/flush bottlenecks are under control, because it is a table-format change.
   - [x] First slice: table blocks can now be stored as prefix-compressed key
     deltas with restart offsets, optionally followed by Snappy. The current
     reader expands blocks back to the existing full-entry layout before search.
   - [x] Decode cleanup: prefix-block materialization reuses key scratch while
     expanding entries, avoiding one temporary key allocation per entry.
   - [x] Direct-search primitive: prefix-compressed block payloads can be
     searched by restart point and scanned within the restart window without
     expanding the full logical block. Runtime integration needs a cache policy
     that does not displace hot decoded blocks on mixed workloads.

### Comparison Rules

- Keep the same command, sample count, key count, value pattern, cache size, and
  storage mode across before/after runs.
- Compare medians first, then inspect p95/p99 for new tail regressions.
- Treat these as primary read metrics: `ns_per_op`, `storage_read_range`,
  `read_run_probes`, `read_bloom_negatives`, cache block hit rate, and cache
  waits.
- Treat these as primary write metrics: ingest ops/sec, `flush_ms`,
  `compaction_ms`, manifest write count/bytes, WAL append/sync time, L0
  runs/bytes, retained WAL bytes, and ResourceManager slices.
- For correctness-sensitive changes, run `zig build lsm-backend-test` before
  benchmark comparison.

## Pebble Gap: Write Path And Compaction

Status: in progress

The latest VectorDBBench runs exposed a write-path gap that is separate from
the read-cache work above. Go uses Pebble for the main DB and HBC index DBs, so
it gets Pebble's memtables, immutable-memtable queue, background flushes,
background compactions, L0 pressure handling, write stalls, a block-buffered
SST writer, and a storage-engine WAL that makes foreground commits append-only
before later table flush. The Zig LSM has table files, run metadata, compaction
primitives, bulk-ingest modes, and now a bounded node-round maintenance
scheduler, but it still lacks Pebble's dedicated foreground WAL, immutable
memtable queue, background worker pool, and mature stall policy.

Current symptoms:

- Successful 50k dense runs can report `compaction_ms=0` while producing tens
  of GB of table data. That means compaction is not falling behind; for that
  path, it is not being scheduled.
- Runs that do compact do it from write/finalize paths, which reduces space
  amplification but pushes compaction latency directly into insert timing.
- HBC online mutation produces many intermediate `nodes`, `quant`, `range`, and
  `vecs` updates. Deferring compaction without coalescing final state preserves
  those intermediate versions as disk debt.
- Table-file encoding streams many small append calls into the native atomic
  writer. On native storage each append can become a small positional write,
  so flush time can be high even before compaction.
- Foreground commits still publish through mutable-state flushes into table
  files once thresholds are reached. Pebble instead appends commit records to
  its WAL, applies them to an in-memory memtable, and lets background flush turn
  immutable memtables into SSTs later.
- Derived replay over the primary store used to reopen snapshot read txns on
  hot ingest. On the Zig LSM backend, `beginReadTxn()` clones the active mutable
  memtable, so replay workers could drive multi-GB Activity Monitor footprint
  even on 50k vector runs.

Task list:

1. [x] Add a buffered table-file writer between `encodeWithFilterToSink` and
   native/host `AtomicWriteSink`, preserving `writeAt` patching for the table
   header.
2. [x] Add LSM maintenance/debt stats: mutable entries, total runs/bytes, L0
   runs/bytes, compactable run count, obsolete path count, and manifest dirty
   state.
3. [x] Export the maintenance/debt stats through HBC benchmark write logs so
   dense-index runs can answer whether compaction is idle, running, or
   backlogged.
4. [ ] Export the same maintenance/debt stats through DB status and Prometheus.
5. [x] Add a node-level LSM maintenance scheduler. It should pick backends by
   score/debt, run compaction outside foreground request handlers, and publish
   manifests safely.
6. [x] Add write pressure/backpressure policy. Soft limits should schedule or
   accelerate background work; hard limits should bound L0/run debt with either
   retryable overload, write delay, or inline cleanup depending on sync level.
7. [x] Convert flush policy from mostly entry-count thresholds to byte-budgeted
   memtable/run thresholds, with HBC-specific defaults for large vector values.
8. [x] Add a native append-only LSM WAL plus immutable-memtable queue. Foreground
   commits should append a framed/checksummed mutation batch, sync according to
   the backend sync policy, apply to the mutable memtable, and defer table-file
   creation to background flush.
   - [x] Commit-path flush deferral now supports WAL-backed entry-threshold
     flushes as an opt-in mode and keeps the existing byte-threshold deferral.
     Threshold-crossing commits rotate the mutable memtable into the immutable
     queue and leave table-file creation to maintenance, with a bounded
     immutable-queue backpressure limit.
   - [x] Added `BackendHandle`, a heap-owned backend owner that keeps a stable
     `*Backend` address for future internal workers while preserving the
     existing by-value `Backend` API during migration.
   - [x] Migrated runtime DB/store LSM owner construction to `BackendHandle`
     across persistent indexes, HBC, DB primary stores, WAL-backed stores,
     graph reverse stores, raft apply stores, and auth stores.
   - [ ] A dedicated internal flush thread can now be built on `BackendHandle`
     and enabled after its stop/join, wakeup, and write-pressure behavior is
     covered by tests.
9. [x] Add WAL-aware recovery and manifest checkpoints. Recovery should load
   durable runs from the manifest, replay WAL records after the last checkpoint
   into mutable/immutable memory state, and safely truncate or recycle WAL files
   only after a published flush checkpoint.
10. [ ] Add WAL metrics and pressure hooks: bytes appended, sync latency,
   records replayed, oldest uncheckpointed LSN, immutable-memtable bytes, and
   WAL truncation lag.
11. [ ] Add incremental WAL checkpoints and segment retirement. Durable flush +
   manifest publication should retire covered WAL segments without waiting for
   a full backend reset.
12. [ ] Export startup open phases and retained-WAL debt. LSM-backed stores
   should report whether they are opening the manifest, replaying WAL, mounting
   runs/indexes, or doing higher-level catch-up/rebuild work.
13. [ ] Add per-backend WAL retention policy for index stores. Dense, sparse,
   and graph backends should checkpoint aggressively after successful bulk
   finalize, startup recovery, and large catch-up sessions so retained WAL stays
   bounded across restarts.
14. [ ] Add final-state HBC bulk publication for empty or sustained ingest so
   large loads do not persist every intermediate online mutation.
15. [x] Add LSM table-block compression. Start with adaptive per-block Snappy
   because the repo has a pure Zig codec today, keep the policy configurable per
   backend/store, and store blocks uncompressed when the compressed payload does
   not clear a savings threshold. Add zstd/lz4 policies later when encoder
   support is available and benchmarked.
16. [ ] Keep dense/sparse embeddings in their binary artifact format rather
   than relying on table compression to shrink JSON float arrays.
17. [ ] Re-run 50k and 1M VectorDBBench with samples and compare:
   `logical_bytes`, `table_file_bytes`, `l0_runs`, `compaction_debt`,
   `flush_ms`, `compaction_ms`, `wal_append_ms`, `wal_sync_ms`, and search
   p95/p99.

Design target:

- Foreground writes should publish durable mutable state quickly.
- Background maintenance should reduce L0/obsolete/version debt continuously.
- If background work cannot keep up, the resource manager should make that
  pressure visible and apply bounded backpressure before disk usage explodes.
- Bulk HBC ingest should write final index state where possible, not a stream of
  online mutation history.

## Foreground Publish Versus Maintenance Debt

Status: first backend slice implemented

The latest 1M public guardrail showed a specific remaining architectural bug:
dense query-visible publish is still coupled to LSM cleanup. The public
auto-bulk path asks for `compact = false`, but it also passes `flush = true`
and `max_deferred_l0_runs = 64`. In the LSM backend, that path still drains all
mutable/immutable state and then loops in `compactDeferredL0RunsToLimit()` under
the backend lock until L0 is below the requested limit. Samples from the failed
1M run showed the hot stack in:

- `IndexManager.finishDenseBulkIngestEntryWithOptions`
- `HBCIndex.finishBulkIngestSessionWithOptions`
- `Backend.finishBulkIngestSessionWithOptions`
- `Backend.compactDeferredL0RunsToLimit`
- `compaction.compactPlanAt`
- persisted run cursor/table read/write work

That explains the large publish windows and the "visible count advances in huge
chunks" behavior. The HBC publish made progress, but every publish window could
inherit a foreground L0 compaction loop.

This is not the Pebble/RocksDB shape. Pebble and RocksDB make foreground write
visibility depend on WAL + memtable publication and, when needed, memtable
flush. They do not make normal write visibility depend on compacting L0 back to
a target. L0/level cleanup is background maintenance debt. If debt exceeds hard
limits, writes can be slowed, stalled, or rejected by policy, but that is an
explicit pressure response rather than an implicit cost hidden inside every
publish.

### Contract

Separate the three concepts that are currently blurred:

1. Visibility publish.
   - Make the latest accepted state query-visible.
   - Publish metadata/manifests needed for readers to find the new state.
   - Keep the work bounded by bytes/runs/time.

2. Durability checkpoint.
   - Ensure WAL coverage, flushed runs, and manifest state satisfy the selected
     durability contract.
   - Retire WAL only for data that has been durably covered.
   - This applies to all LSM-backed stores and indexes, not just dense.

3. Maintenance debt reduction.
   - Flush queued immutable memtables.
   - Compact L0 and lower levels.
   - Delete obsolete files after reader safety windows.
   - Run from background maintenance under resource-manager budgets.

Visibility publish may create debt. It must not be required to pay all of that
debt before returning.

### Target Behavior

- `.write` and `.propose` batches append to durable journal/WAL state and return
  without waiting for dense/full-text/sparse/graph compaction.
- `.full_text`, `.aknn`, and `.full_index` can wait for derived visibility, but
  they still should not require unbounded LSM compaction unless the requested
  contract explicitly includes storage cleanup.
- Dense catch-up publishes query-visible HBC state in bounded windows.
- The same LSM publish/maintenance split is available to primary, dense,
  full-text, sparse, and graph stores.
- Status reports:
  - query-visible sequence/doc count
  - LSM mutable/immutable bytes
  - L0 runs/bytes
  - compaction debt
  - whether writes are stalled or slowed by hard limits
- Health and cached status stay responsive while maintenance runs.

### Implementation Plan

1. [x] Rename and tighten finish options.
   - Split `BulkIngestFinishOptions.max_deferred_l0_runs` into an explicit
     foreground cleanup budget, not a target that loops until satisfied.
   - Add fields shaped like:
     - `max_foreground_compaction_steps`
     - `max_foreground_compaction_input_bytes`
     - `max_foreground_compaction_ns`
   - Default publish paths should use zero foreground compaction steps.

2. [x] Make `finishBulkIngestSessionWithOptions()` publish-only by default for
   `compact = false`.
   - Drain mutable/immutable state only when required for visibility or
     durability.
   - Persist the manifest if new runs must be visible after reopen.
   - Refresh maintenance debt hints.
   - Do not call `compactDeferredL0RunsToLimit()` unless an explicit bounded
     foreground budget is present.

3. [x] Add a one-step scheduled foreground compaction primitive for explicit
   bounded cleanup.
   - Keep the existing loop only for tests or explicit full-cleanup calls.
   - Add a one-step bounded variant used by explicit foreground cleanup.
   - Use the existing compaction scheduler and resource-manager
     `lsm_compaction_work` budget for input bytes.

4. [x] Add hard-limit pressure policy outside publish.
   - Soft limits schedule maintenance.
   - Hard limits can apply write delay/stall/overload before accepting more
     work.
   - Hard-limit enforcement should be visible in metrics; it should not appear
     as a mysterious multi-minute publish window.

5. [x] Checkpoint WAL independently from compaction.
   - After a successful flush + manifest publication, advance WAL coverage for
     the covered state.
   - Retire covered segments incrementally.
   - Do not require L0 compaction before WAL checkpointing.

6. [x] Release and account transient LSM memory eagerly.
   - After mutable rotation, immutable flush, and manifest publish, update
     `lsm_in_memory_state`.
   - Ensure table-builder, WAL staging, and compaction scratch allocations have
     resource-manager slices or short-lived ownership that actually releases.
   - The expected post-publish footprint should be retained caches plus real
     L0/run metadata, not stale build buffers.

7. [x] Keep dense/HBC publish bounded at both layers for foreground L0 cleanup.
   - HBC split/publish windows remain bounded by
     `max_deferred_hbc_leaf_splits_per_publish`.
   - The underlying LSM publish window must also be bounded and must not run an
     unbounded compaction loop after HBC finishes its own bounded work.

8. [x] Make benchmark/client failures distinguishable.
   - Public guardrail should retry a single stale closed HTTP connection during
     query startup.
   - A retry hides keepalive races, not server failures; repeated connection
     close/refuse still fails the run.

### Required Tests

1. Bulk finish with `compact = false` and no foreground compaction budget
   publishes pending data but does not reduce L0 to a target.
2. Bulk finish with a one-step foreground compaction budget performs at most one
   compaction step.
3. Background maintenance can later reduce the same L0 debt to the soft target.
4. Hard L0 limits trigger explicit pressure accounting rather than hidden
   publish-time compaction.
5. WAL checkpoint/segment retirement works after flush + manifest publication
   without requiring compaction.
6. Dense auto-bulk publish advances query-visible status while leaving LSM
   compaction debt for maintenance.
7. Full-text/sparse/graph LSM stores inherit the same publish-versus-maintenance
   behavior through the backend contract.

### Validation

Re-run the public guardrails with samples and metrics:

- `50k`, `.write`
- `1M`, `.write`
- optional `.propose` comparison after `.write` is stable

Expected signals:

- health/status/metrics remain reachable during load and catch-up
- dense `published_doc_count` advances in bounded windows
- `bulk_finish_max_window_ns` drops materially
- no foreground stack dominated by `compactDeferredL0RunsToLimit`
- `l0_runs`, `l0_bytes`, and compaction debt may rise during load, then fall
  under background maintenance
- `rm_lsm_in_memory_mb` drops after publish/flush instead of retaining GBs of
  stale mutable/immutable/build state

WAL design note:

- Pebble's WAL does not replace SST/table files. It moves the foreground durable
  write from "create/publish a sorted table now" to "append a mutation record
  now, flush sorted tables later".
- Reusing `pkg/antfly/src/storage/wal.zig` directly is not enough if the WAL is
  backed by the LSM backend, because that would store the WAL inside the same
  table/manifest system and preserve the small-file problem. The LSM needs a
  native append-log file under the backend's storage root, with record framing,
  checksums, rotation, replay, and checkpoint/truncation tied to manifest
  publication.
- The read path must merge mutable memtable state, queued immutable memtables,
  and durable runs. The current mutable-plus-runs merge shape is close, but a
  WAL-backed design needs immutable memtables to remain visible while background
  flush is writing their table files.

Implemented WAL slice:

- The storage abstraction now has an append-file operation with native,
  memory-storage, and fallback implementations. Native storage appends to the
  existing file and can optionally sync the file handle.
- Each durable LSM backend now writes committed mutable transaction batches to
  segmented `wal/NNNN.log` files as framed, checksummed records before
  publishing them to the in-memory mutable state. `wal/index` records the active
  segment and segments rotate by byte budget.
- Recovery loads the manifest, replays legacy `wal.log` if present, then replays
  numbered WAL segments in order using bounded range reads. Torn trailing records
  are ignored; corrupt complete records fail recovery. Replayed mutations are
  restored into mutable memory state; preserving the previous immutable queue
  shape across restart is not required for correctness.
- Manifest publication resets the WAL only when mutable state and queued
  immutable memtables are empty, so a crash between table flush and manifest
  publish can still recover from WAL.
- Write stats now include WAL append, replay, reset, and sync counters.

Implemented immutable-memtable slice:

- Durable byte-budgeted backends now rotate mutable state into an oldest-first
  immutable-memtable queue instead of synchronously writing run tables when the
  threshold is crossed.
- Read snapshots merge durable runs, queued immutable memtables, and current
  mutable state so rotated writes remain visible while background flush catches
  up.
- Bounded maintenance can flush one immutable memtable into run tables, then
  continue normal L0/level compaction and manifest publication. Explicit sync,
  split, close, and bulk-finalization paths drain immutable memtables before
  relying on manifest-only recovery.
- In-memory and entry-threshold-only test profiles keep the older direct flush
  behavior, which keeps small unit tests deterministic while production HBC
  profiles use the WAL-backed byte-budgeted path.

Immutable memtable task list:

1. [x] Add a backend-owned immutable memtable queue, oldest first.
2. [x] Make read snapshots include mutable plus queued immutable memtables, so
   committed writes stay visible after foreground rotation and before table
   flush.
3. [x] Rotate mutable into the immutable queue when byte/entry thresholds are
   crossed instead of writing run tables in the foreground commit path.
4. [x] Teach the maintenance scheduler to flush one immutable memtable per
   bounded step, then continue normal L0/level compaction debt handling.
5. [x] Make `sync`, close, split preparation, and bulk-ingest finalization drain
   immutable memtables before relying on manifest-only recovery.
6. [x] Keep WAL truncation gated on both mutable and immutable queues being
   empty after manifest publication.

## Replay Read Path

Status: in progress

Replay rows are append-only and sequence-ordered. They are not a general query
workload, so they should not pay for the full snapshot-read machinery that the
LSM exposes for arbitrary scans.

The old replay path used:

- `DocStore.beginReadTxn()`
- `txn.openCursor()`
- a stable merged snapshot over mutable + immutable + runs

That is correct for read-only scans, but it is the wrong shape for hot replay.
On the LSM backend, opening that snapshot clones the active mutable memtable.
Under sustained ingest, derived workers repeatedly reopened those snapshots and
inflated process footprint far beyond the actual steady-state working set.

Current direction:

- Keep general snapshot reads for query/search/scan code.
- Keep probe transactions point-read only for current-tip lookups.
- Add a replay-specific live scan path for append-only replay rows.
- Make replay workers consume replay lanes from the current durable tip using a
  dedicated current-scan contract instead of snapshot cursors or dense
  point-probe loops.

This is deliberately different from the generic snapshot contract:

- replay only needs forward iteration by sequence
- replay rows are append-only
- the DB is single-writer
- the hot path does not need a long-lived stable view of arbitrary keyspace

That means the efficient API is not "snapshot + cursor" and it is also not
"probe + hidden cursor". It is:

- `ProbeTxn`: current-tip point reads only
- `CurrentScanTxn`: ordered current-tip replay scans only
- `ReadTxn`: general snapshot reads and arbitrary scans

Near-term task list:

1. [x] Remove long-lived primary-store replay cursors that pin LSM snapshots.
2. [x] Add a `DocStore` replay-specific live scan path for hint-filtered replay
   reads.
3. [x] Switch derived replay workers to that live scan path instead of
   `beginReadTxn()`.
4. [x] Move full-text derived point-read document fetches off snapshot reads and
   onto a probe path.
5. [x] Keep `ProbeTxn` point-read only by splitting replay scans onto a
   dedicated current-scan contract.
6. [ ] Add native LSM/runtime replay-lane iteration so replay workers no longer
   scan the replay-all lane and decode hint masks in userland.
7. [ ] Export replay-live scan metrics so we can compare:
   - replay sequences scanned
   - replay scan batches
   - replay hint-filter skips
   - replay clone bytes avoided

Design target:

- Query/search paths keep snapshot reads.
- Probe paths stay point-read only.
- Replay paths get a dedicated live scan contract.
- Activity Monitor footprint during ingest should be dominated by real write
  working set, HBC finish state, and caches, not by cloned mutable snapshots
  held open for replay.

Segmented WAL task list:

1. [x] Replace `wal.log` as the active production format with `wal/index` plus
   numbered segment files under `wal/`.
2. [x] Rotate segments by byte budget.
3. [x] Replay segments in order with bounded range reads instead of loading the
   full WAL into memory.
4. [x] Keep legacy `wal.log` replay long enough to recover data written by the
   first WAL slice.
5. [x] Reset/checkpoint by publishing a clean segment index and deleting obsolete
   segment files after manifest publication.

Still open:

- WAL metrics currently cover append/replay/reset/sync activity and immutable
  memtable bytes, but do not yet expose an oldest-uncheckpointed-LSN or WAL
  truncation-lag gauge.
- WAL retention is still reset-based instead of checkpoint-based. A backend can
  retain multi-GB segmented WAL debt after an interrupted or partially-complete
  run, and startup must replay that entire tail before higher-level catch-up
  becomes visible.
- The next HBC-specific ingest slice is final-state bulk publication so large
  sustained loads avoid persisting every intermediate online mutation.

### Current 1M Recovery Findings

Recent loaded-root reopen runs are now instrumented enough to be explicit about
the remaining gaps:

- Dense-index reopen spends about 33s in `DB.open()`, almost entirely in LSM
  WAL replay.
- The dense backend reports about 4.26GB replayed and about 4.34GB still
  retained for replay on this root.
- After open, higher-level dense catch-up is active but barely progressing.
- Process RSS can climb into multi-GB territory while the existing
  `ResourceManager` slices remain near zero, which means startup replay/open
  memory still sits outside the tracked cache slices.

That means there are still two independent problems to fix:

1. old retained WAL tails must be retired sooner after durable recovery/catch-up
2. startup replay/open memory must be explicitly accounted and eventually
   pressure-limited, instead of being inferred from cache metrics

### Near-term recovery/memory task list

1. [x] Surface startup open metrics: configured/opened indexes, index-load time,
   WAL replay records/entries/bytes/ns, and retained WAL debt.
2. [x] Add a resource-manager slice for LSM in-memory replay/open state using
   backend mutable + immutable bytes.
3. [x] Export and test the new in-memory-state slice through status/metrics on
   loaded-root startup paths.
4. [x] Make startup/open progress publish from the actual recovery worker
   instead of leaving public status frozen at an outer `opening_db` snapshot.
5. [ ] Read back `wal_replay_*`, `lsm.in_memory_state`, and startup/open phase
   on the same loaded root after the recovery-flush changes, then compare them
   against the earlier multi-GB replay runs.
6. [ ] Verify the second restart cost drops further once a run reaches a clean
   post-recovery checkpoint, rather than replaying the same retained bytes.
7. [ ] Replace recovery's general-allocator entry churn with a bounded recovery
   allocation model.
   - Current evidence from `vmmap` on the live `1M` root:
     - physical footprint can reach about `14.1G`
     - RSS stays under `500M`
     - mapped files are only about `389M`
     - malloc zones account for about `13.0G` allocated / `13.8G` swapped
   - This means the remaining memory problem is process-private heap growth and
     allocator retention during recovery, not primarily mapped-file residency.
   - The target fix is recovery-specific chunk/arena allocation for replayed
     state so flush can drop whole chunks instead of retaining millions of
     medium/small heap objects.
8. [ ] Add a distinct startup/recovery working-set slice for higher-level dense
   rebuild/catch-up transient buffers if physical footprint still materially
   exceeds the new bounded recovery heap plus tracked caches.
9. [ ] Diagnose why dense catch-up stalls after open on the `1M` root even
   after WAL replay completes, because that still blocks proving post-fix WAL
   retirement on restart.

### Immediate loaded-root follow-up

Status: active

The loaded `1M` root is now making real progress again:

- reopen is cheap (`wal_replay_bytes = 0`, `load_indexes_ns ~= 4.5s`)
- startup reaches `artifact_rebuild`
- dense rebuild advances steadily instead of stalling at `1007 / applied=0`
- footprint is bounded in the low-GB range rather than the old runaway shape

The next work should be executed in this order:

1. [ ] Add a local loaded-root artifact-rebuild benchmark.
   - Reopen a partially rebuilt root and measure:
     - `load_indexes_ns`
     - time to first applied entry
     - steady-state applied entries/sec
     - peak RSS / physical footprint
     - tracked resource slices
   - Cover `50k`, `250k`, and `1M` fixtures.
   - The key regression case is a root with replay debt cleared but dense
     artifact rebuild still required.
2. [ ] Implement metadata-lookup reuse for dense apply.
   - Current steady-state sample is dominated by:
     - `IndexManager.applyDenseEmbeddingWritesEntry`
     - `HBCIndex.getMetadata`
     - metadata cache insert/remove churn
   - Reuse or preload current vector-id metadata per rebuild chunk instead of
     repeated point reads through HBC/LSM.
3. [ ] Rerun the local artifact-rebuild benchmark and compare throughput,
   memory, and cache usage.
4. [ ] Let the loaded `1M` root finish end to end on the improved binary.
5. [ ] Restart immediately after clean completion and confirm reopen remains
   cheap without rebuilding the old retained-WAL/open-time debt.
10. [ ] Add dense catch-up diagnostics for the post-open stall window:
   - external vector cache hits/misses
   - docstore artifact/document load counts and bytes
   - recompute-leaf calls and member-vector reloads
   - per-window watchdog logs when `applied_entries` does not move
11. [ ] Add ResourceManager coverage for the remaining untracked dense/docstore
    working sets:
   - session-local external vector memo bytes
   - centroid recompute scratch
   - docstore decode/materialization buffers
12. [ ] Re-run the loaded `1M` root with the new dense diagnostics and record:
   - startup phase
   - replay sequence progress
   - cache hit/miss deltas
   - resource slices vs `vmmap` / physical footprint
13. [ ] Revisit startup dense cache caps once the new diagnostics are in hand.
   The current startup defaults still clamp HBC caches to `nodes=128` and
   `vectors=2048`, and recent samples still show `loadExternalVectorCached()`
   missing on most `getVectorScratch()` calls during the same catch-up window.

Implemented next slice:

- Recovery-time WAL replay is moving to a bounded Pebble-style model instead of
  "replay the whole retained tail into one mutable memtable, then maintain."
- The first executable step is incremental recovery flushing:
  - WAL replay can call back into the backend after each applied record
  - once recovered mutable state crosses the normal flush threshold, recovery
    rotates and flushes immediately
  - WAL checkpoint/reset is deferred until replay completes, so unread later
    segments are never retired early
- Required regression:
  - reopen over a multi-segment retained WAL tail must flush incrementally,
    keep post-open mutable/immutable state bounded, and make the second reopen
    avoid replaying the same retained bytes again
- Remaining gap from live validation:
  - recovery now flushes incrementally, but the long-running `1M` reopen still
    accumulates a very large malloc footprint in private heap pages
  - the next executable slice is allocator-model work, not more cache tuning

## WAL Retention And Startup Replay

Status: planned

Recent 1M loaded-root runs showed the next backend-level gap clearly:

- dense startup catch-up may appear "stuck at zero" because the store is still
  in `DB.open() -> LSM WAL replay`, before the higher-level index catch-up
  phases start
- retained index WAL can grow to multi-GB across interrupted runs
- the current LSM reset path only deletes WAL segments after a manifest
  publication with no mutable state and no queued immutable memtables
- derived replay already has an applied watermark + truncation path; the LSM
  WAL does not

This is not dense-specific. Any LSM-backed index backend can inherit the same
startup replay tax if it retains large WAL segments between runs.

### Design target

- Opening an LSM-backed store should replay only the uncovered WAL tail, not the
  full retained history.
- Durable flush + manifest publication should advance an explicit WAL checkpoint
  and retire covered WAL segments incrementally.
- Startup/status should report LSM open phases separately from higher-level
  replay or index backfill so the node does not look idle while it is still
  paying WAL replay debt.
- Dense, sparse, and graph index stores should all inherit the same retention
  guarantees from the LSM layer.

### Current status

1. Add explicit WAL checkpoint metadata to the backend.
   - Implemented:
     - current segment
     - oldest uncheckpointed segment
     - retained WAL bytes/segments
     - checkpoint lag in sealed segments before the active WAL segment
   - Surfaced through backend maintenance stats and Prometheus metrics.
   - Remaining:
     - last checkpointed mutation sequence or equivalent durable flush marker

2. Add incremental segment retirement after durable publication.
   - Implemented: when a flush + manifest publication durably covers WAL through
     segment `N`, retire segments `<= N` immediately.
   - Keep the full-reset path for the totally clean case, but do not require a
     full reset to reclaim historical WAL.

3. Split "checkpoint" from "reset".
   - Implemented:
     - `checkpoint`: advance durable coverage and retire covered segments while
       keeping the current WAL live for new writes
     - `reset`: clean-slate path when mutable + immutable state are both empty

4. Add WAL pressure policy.
   - Implemented:
     - optional soft/hard WAL segment and byte limits on `Options`
     - retained WAL pressure feeds backend maintenance score
     - soft WAL pressure makes maintenance flush/checkpoint a live mutable
       memtable before the normal flush threshold
   - Remaining:
     - hard-threshold write throttling/stalling
     - resource-manager pressure integration

### Remaining work

1. Export startup/open phases for LSM-backed stores.
   - Suggested phases:
     - `opening_manifest`
     - `replaying_wal`
     - `mounting_runs`
     - `starting_index_runtime`
     - `higher_level_catch_up`
   - Status/metrics should distinguish LSM replay debt from derived replay debt
     and from index rebuild/backfill work.

2. Add aggressive checkpoint triggers for index stores.
   - After successful bulk finalize
   - After successful startup repair/rebuild
   - After large catch-up sessions
   - After sustained write bursts that rotated segments

3. Re-benchmark loaded-root restart behavior.
   - Measure time to:
     - LSM open complete
     - first visible higher-level catch-up progress
     - steady-state query readiness
   - Compare retained WAL bytes before/after checkpoint-retirement changes

### Required test coverage

The goal is to make WAL retention behavior a backend contract, not a workload
accident. Add focused tests at the LSM layer plus one integration-style restart
test through DB/index open.

Core backend tests:

1. Checkpoint retires covered segments.
   - Write enough state to create multiple WAL segments.
   - Flush + publish durable runs.
   - Assert covered segments are retired while the active tail remains.

2. Restart replays only uncovered segments.
   - Create several segments.
   - Advance the checkpoint through an interior segment.
   - Reopen and assert replay starts after the checkpointed coverage.

3. Full reset still works.
   - Reach the empty mutable + immutable state case.
   - Assert reset removes obsolete segments and reinitializes the WAL index.

4. Interrupted flush preserves correctness but bounds replay debt.
   - Simulate a crash after WAL append and before or during publish.
   - Reopen and assert data correctness.
   - After a successful later checkpoint, assert old retained segments are
     retired.

5. Repeated open/close does not accumulate retained WAL indefinitely.
   - Drive several write / flush / reopen cycles.
   - Assert retained WAL bytes/segments stay bounded.

Index-facing integration tests:

6. Dense startup after successful checkpoint does not replay historical WAL.
7. Sparse startup after successful checkpoint does not replay historical WAL.
8. Graph startup after successful checkpoint does not replay historical WAL.

Those do not need three separate codepaths if the harness can parameterize the
LSM-backed index kind, but the behavior needs explicit coverage for all three.

Observability tests:

9. Status/metrics expose:
   - retained WAL segments
   - retained WAL bytes
   - oldest uncheckpointed segment
   - startup phase = `replaying_wal` during open replay
   - startup phase transitions once replay completes

## Compression Direction

RocksDB and Pebble compress table blocks, not whole logical databases. Antfly
should follow that shape first: each table block carries a small compression
header, the reader decompresses only blocks it touches, and block-cache/accounting
can separately budget compressed bytes on disk and uncompressed bytes in memory.

Near-term compression order:

1. Add adaptive LSM table-block compression as a per-store option. The first
   implementation uses Snappy-style block framing because it is available in
   pure Zig in this repository; zstd/lz4 should be added as additional policies
   once encoder support and CPU/ratio benchmarks justify them.
   - `Backend.WriteStats` records table logical entry bytes, physical entry
     bytes, raw block count, compressed block count, and a compression codec
     mask for each backend/store.
   - `MaintenanceStats` records the same logical/physical totals for the active
     run set so benchmark logs can distinguish live compressed bytes from
     obsolete files or repeated table publication.
2. Keep dense and sparse embedding artifacts binary. Binary vector payloads are
   the format fix; table compression is only a secondary byte-reduction layer.
3. Preserve byte-based mutable flush thresholds and per-store LSM configs as
   first-class work. Compression reduces bytes, but it does not fix flushing too
   often or persisting intermediate HBC states.
4. Treat HBC quantized/vector-like blocks as adaptive: if compression does not
   win by a threshold, store the block raw.

MAYBE/later:

- Add primary document and chunk codec envelopes. Small JSON/text values can
  remain raw; larger JSON/text values can be zstd/lz4-compressed behind an
  explicit versioned header.
- Add zstd/lz4 LSM block compression policies. These should be configurable per
  store, not a global format switch, because primary JSON/text rows, full-text
  metadata, HBC metadata, and vector-like payloads have different CPU/ratio
  tradeoffs.
- Add value separation for very large values if table-block compression plus
  byte-based flush policy still leaves high compaction rewrite cost.

Implemented scheduler slice:

- Each LSM backend now publishes a maintenance score derived from L0 run/byte
  debt, lower-level overflow, and dirty manifest state.
- `DataServer.runRound()` runs one bounded maintenance step through the
  provisioned write-cache, choosing the cached table DB with the highest LSM
  score. The DB then chooses primary-store or index LSM debt and runs one
  compaction/publish step under the DB apply lock.
- Soft L0 limits are now scheduler debt. The maintenance step compacts toward
  the soft limit when foreground writes have not crossed the hard limit.
- Hard L0 limits are foreground guardrails. After a mutable flush, a backend
  that crosses the hard run/byte limit does bounded inline cleanup so L0 debt
  cannot grow without bound while background rounds catch up.
- Dense HBC LSM defaults now use a byte flush threshold and byte/run L0 limits,
  while legacy entry-count thresholds remain available for small-value stores
  and tests.
- Primary document, full-text main/WAL metadata, and graph reverse LSM defaults
  now also use byte-based mutable flush thresholds and byte/run L0 limits. Small
  unit tests can still opt into entry-count flushes explicitly, but production
  defaults should not emit one run per tiny write batch.

## Phase 1: In-Flight Load Coordination

Status: implemented

Problem:

- `pending_loads` was an `ArrayList` scanned on every miss.
- Waiters busy-looped with `yield`, which amplifies CPU burn under block-cache misses.

Implemented:

- Replaced `pending_loads` with a keyed hash map over the full block/table cache key.
- Added wait/broadcast load coordination so one loader owns a miss and followers block until the load finishes.
- Kept a fallback sleep/relock path for environments where the pthread-backed wait path is unavailable.

Why this matches RocksDB/Pebble:

- Both engines aggressively avoid duplicate miss work and avoid thundering-herd behavior around shared read structures.
- The important change is not just hashing the key. It is coalescing the miss itself.

## Phase 2: Native FD Cache Structure

Status: implemented

Problem:

- The old fd cache used one global mutex and one linear entry array.
- Misses held the cache mutex across `openat`.
- Exact-path invalidation had to scan the entire cache.

Implemented:

- Replaced the single array with a sharded fd cache.
- Each shard now uses hashed buckets keyed by path hash, with collision lists for exact path matching.
- `openat` now happens outside the shard lock, followed by recheck-on-insert.
- Each shard maintains local LRU order.
- Exact-path invalidation now goes directly to the hashed bucket for that path.
- Duplicate path entries remain supported so invalidated pinned fds do not block reopening the same path.

Why this matches RocksDB/Pebble:

- This moves the design toward a real table-cache shape: shard first, hash lookup second, syscall outside the hottest lock.
- The implementation is intentionally simpler than RocksDB's table cache, but it fixes the same class of contention.

## Phase 3: Block Cache Eviction

Status: implemented

Problem:

- The block cache was sharded for locking but still evicted by globally rescanning every shard and every entry.
- The previous `key_hash` change reduced comparison cost but did not change the O(n) victim search.

Implemented:

- Replaced shard entry arrays with keyed shard maps.
- Added shard-local LRU lists grouped by eviction priority.
- Eviction now walks shard-local maintained state instead of rebuilding victim choice from a full cache scan.
- Retain, put, release, and invalidate paths now update LRU state directly.

Why this matches RocksDB/Pebble:

- Sharded caches only pay off if lookup and eviction both operate on shard-local indexed state.
- This is the minimum structure needed to make sharding materially useful under pressure.

## Phase 4: Metadata Read Bundling

Status: implemented for new table files, with legacy fallback retained

Problem:

- Loading a `TableIndex` still requires multiple small reads:
  - header
  - entry offsets
  - bloom length
  - bloom bytes
- The current v3 table layout places entry data between offsets and bloom bytes, so one contiguous metadata read is not possible without either over-reading entry data or knowing more about file length/layout.

Implemented in this pass:

- The block-read index path now reuses cached full-table raw bytes when they are already present, decoding the index from cached raw data instead of falling back to the fragmented `readFileRangeAlloc` sequence.
- This does not eliminate the multi-read metadata path when raw bytes are not cached, but it removes redundant metadata I/O when range/full-table reads have already populated the raw-table cache.
- Added `fileSize` to the storage abstraction and switched the v3 index loader to derive bloom length from EOF, reducing the uncached v3 metadata path from four reads to two range reads plus one size lookup.
- Added a new v4 run-table format with a fixed footer at EOF. The footer points to one contiguous metadata bundle containing entry offsets and bloom bytes.
- New table files now load indexes via:
  - one fixed-size footer trailer read
  - one contiguous metadata read
- Legacy v2/v3 table files remain readable through the older decode path.
- Bloom-negative reads now require and use manifest-carried `encoded_bloom_filter` bytes, avoiding whole-table I/O on common negative probes after reopen.
- Once a manifest bloom has been decoded for a live run, later read snapshots now borrow that decoded filter instead of re-decoding it per transaction.
- Added a backend-local `TableIndex` cache for no-shared-cache readers, and point reads now use `footer metadata + one data block read` instead of loading the full table on first access after reopen.
- Added a small backend-local run-block cache for no-shared-cache readers so repeated point reads in the same backend can reuse the previously fetched data block without additional file I/O.
- Added a new v5 table format that stores per-block upper-bound metadata in the footer bundle. Point reads now use that metadata to jump directly to a single candidate data block instead of binary-searching across entry offsets and potentially touching multiple blocks.
- Added per-block bloom filters to the footer metadata. If a point lookup survives the run-level bloom filter but is still absent from the candidate block, the reader now rejects it before issuing any data-block read.
- Added per-block hash slots to the footer metadata. Once a candidate block is loaded, exact point lookups now use a block-local hash index instead of binary-searching inside the block.
- The raw-table seek path now uses block bounds to jump `lowerBound` and `seekAtOrAfter` directly to the first candidate block instead of searching the full table entry space.
- Cursor/range iteration now stays table-backed for persisted runs in more places:
  - reverse cursor paths (`last`, `prev`, `seekAtOrBefore`) now use table-backed helpers instead of forcing run-state materialization
  - namespace read cursors now use the merge cursor directly instead of materializing a full visible-state snapshot before iteration
- Persisted forward scans in the no-shared-cache path now stay on the backend-local `TableIndex` plus owned block windows:
  - `seekAtOrAfter` uses footer/index metadata to jump directly into the candidate block
  - forward `next()` iteration reuses the current block window until it is exhausted, then loads only the next block instead of materializing the full run state or reopening the whole table

Candidate implementation options:

1. Add a native-only table metadata cache keyed by `(path, run_id, generation)`.
2. Extend native or host storage with a direct trailer read helper so the v4 footer can be fetched without an explicit `fileSize` round trip.
3. Optionally move more reader-open metadata into the footer bundle if future table properties are added.

Why this is not in Phase 1:

- This crosses the storage abstraction and, in the best version, the on-disk table format.
- The cache and fd-cache changes were higher-confidence wins that did not require a format migration.

## Phase 5: Longer-Term Block/Property Skipping

Status: planned longer term

This is where the bigger RocksDB/Pebble lessons live.

### 1. Data-Block Hash Index

RocksDB can trade a small amount of extra space for much faster random reads by adding a hash-assisted in-block lookup structure.

For Antfly LSM:

- Add an optional per-block mini-index for exact key lookup inside cached data blocks.
- Use it only for exact point reads, not for range iteration.
- Keep the existing ordered entry layout so iterators still work.

Expected benefit:

- Cached block hits stop reparsing/scanning from the block start for many point-lookups.

Expected cost:

- Table format change.
- More bytes per block.
- More writer complexity.

### 2. Block Property Filters

Pebble's block-property collectors/filters let the iterator skip whole tables, index blocks, or data blocks when a user-defined property proves they cannot match.

For Antfly LSM:

- Persist per-table and per-block properties such as namespace bounds, smallest/largest key, tombstone-only ranges, and possibly lightweight prefix/domain summaries.
- Teach point and range readers to consult those properties before loading a block.

Expected benefit:

- Fewer block loads for namespace-scoped reads.
- Lower read amplification for mixed-keyspace workloads.

Expected cost:

- Format and compaction changes.
- New collector logic at write time.
- Reader-side predicate plumbing.

## Suggested Order From Here

1. Keep the current Phase 1-3 changes and benchmark them under miss-heavy point-lookups and reopen-heavy workloads.
2. Benchmark the v4 footer path against the v3 fallback path under reopen-heavy point-lookups.
3. Only after measuring Phase 4, decide whether the next format revision should add:
   - a trailer read helper in the storage layer
   - a block hash index
   - block property collectors

## What Landed In This Pass

- Pending load coordination is now keyed and blocking instead of scan-plus-yield.
- The native fd cache is now sharded and hashed, and it no longer holds the hot lock across `openat`.
- The block cache now evicts from maintained shard-local LRU state instead of globally rescanning the cache.
- New run tables now use a footer-backed v4 metadata layout, while legacy table versions still decode through the compatibility path.

## Validation

Validated with:

- `zig test pkg/antfly/src/storage/lsm_backend/storage_io.zig`
- `zig build root-test -- --test-filter "lsm backend"`

## Benchmark Harness

For before/after comparisons on this read-path work, use:

- `zig build lsm-backend-bench -- --samples 5 --keys 20000 --storage host --cache both > /tmp/lsm-bench.jsonl`

The harness emits JSONL with:

- scenario labels such as `host_nocache` and `host_cache`
- warm hit/miss/short-scan/full-scan timings
- reopen-heavy open/get/miss/short-scan timings
- mixed read/write timings
- storage read counters (`read_file`, `read_range`, `read_trailer`, `file_size`)
- backend read-stat deltas (`point_gets`, `run_probes`, `bloom_negatives`, etc.)
- shared-cache hit/miss deltas when cache is enabled

Run the same command on two revisions and diff the JSON lines by `scenario + workload`.

To compare two runs directly:

- `zig build lsm-backend-bench-compare -- --before /tmp/lsm-before.jsonl --after /tmp/lsm-after.jsonl`

The compare tool:

- groups by `scenario + workload`
- aggregates medians across samples
- prints `ns/op`, `ops/s`, storage I/O counters, run probes, bloom negatives, and block-hit rate deltas
- tolerates the human-readable header line that the benchmark runner prints before the JSON records

## Immutable Memtable And WAL Efficiency

Status: implemented in the current LSM slice; keep covered by tests because these are correctness-sensitive ownership paths.

Task list:

1. [x] Make immutable flush non-destructive without cloning the whole memtable first. Flush now builds borrowed table entries from the immutable state and only retires the immutable memtable after the new runs are installed.
2. [x] Avoid cloning all immutable memtables into every read transaction. Read snapshots now keep a small newest-to-oldest pointer slice under the reader guard, while only the mutable state is cloned.
3. [x] Replace O(n) immutable queue front removal. The queue now advances a head index and compacts the active suffix after retirement.
4. [x] Add a small-segment WAL rotation test hook. Production segment sizing remains the default, while tests can force rotation without writing large files.

Follow-up watch points:

- If immutable backlog becomes deep, cursor initialization now has more logical sources. The maintenance scheduler should keep that depth low.
- If reads are held for a long time, retired immutable memtables remain pinned until the last reader exits. That is intentional snapshot behavior; resource pressure metrics should make it visible.
