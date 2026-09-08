# Direct thread migration assessment

Audited `origin/main` at `c5ab97691fb8361503f301e8b4e0bedd6bf062e8` on
2026-09-07, in `.worktrees/std-io-migration-audit`, branch
`codex/std-io-migration-audit`. The inventory below records that original
assessment; the implemented follow-ups are described next.

## Current implementation status

This worktree now includes the complete Raft migration, all 237 original
test/helper spawn sites, and nine additional runtime spawn sites. The latter
are the six data-server workers, internal LSM flush, quarantine retry, and
parallel index opening. The benchmark follow-up also converts all 19 original
benchmark spawn sites. The serverless/inference follow-up converts six more
runtime sites and retires the two unused legacy implementations. The latest
follow-ups convert LMDB, linalg, both CUDA worker sites, all remaining direct
yield calls, and the three HTTP control loops. **One original spawn site remains
as an intentional exception: the hard-shutdown watchdog. No `Thread.yield`
calls remain.** The inventory and migration
table below are historical, rather than a list of work still outstanding.

The test-suite follow-up converts the remaining 227 test/helper sites across
61 files to guaranteed-concurrent futures. Worker arrays drain partially
started batches; blocked workers receive their release/stop signal before
cleanup awaits them. Tests retain explicit awaits at observation boundaries.
The metadata HTTP simulation retains its custom stack size in a bounded
two-task executor. A scraping HTTP fixture now consumes request headers before
sending and closing its response to avoid TCP resets from unread request data.
A graph traversal fixture also supplies the routing callbacks now required by
the catalog interface; production routing behavior is unchanged.

Data-server warmup, startup catch-up, and root refresh use owner-scoped durable
jobs. Each submitted job has a closeable owner, drained before its DataServer
state is released. Maintenance reserves one runtime-owned worker, status
reserves two, and auto-bulk finishing reserves its own worker. This keeps status
independent of expensive storage work. Closing admission precedes draining jobs
and releasing leases; startup failure preserves active flags and retry state.
Manual runtimes use the explicit foreground paths rather than submitting a
recursive pipeline inline while its admission lock is held.

Durable-owner drains wait for callbacks and payload destruction without holding
the lane-wide reaper lock, allowing these jobs to close child database owners.
Only completed futures are reaped under that lock; runtime shutdown first waits
for all owners to become idle. Concurrent drains retain the same owner completion
barrier, and draining an open owner never waits for newly admitted work under
the reaper lock.

The internal LSM flush worker accepts borrowed scheduling Io, reuses an owned
backend runtime when present, and otherwise owns a one-task executor. Io mutex
and event waits replace idle polling while preserving obsolete-file deadlines.
Stop without draining now exits without running an extra maintenance pass;
explicit drain still runs final maintenance. Failed startup releases backend
resources and writer ownership. Quarantine retry borrows the DB executor and
uses a stop event while retaining stable-address startup and manual test mode.
Read-only index opening uses bounded Group.async work with safe inline fallback
and per-index result/quarantine ownership.

## Yield, LMDB, linalg, and CUDA follow-up

All 62 direct `Thread.yield` calls remaining after the previous follow-up are
removed. The replacement depends on the wait's purpose:

- WAL commit/coordinator waiters, CUDA staging producers/consumer, background owner close, and embedded provider draining use Io mutexes and condition notifications. Publication happens under the wait mutex, and owner teardown waits for final release notification to finish.
- Atomic lock helpers retain their synchronous ABI and bounded spin/backoff where applicable. The platform compatibility handoff now uses a zero-duration Io wait; the existing longer lock backoff uses a 100-microsecond Io wait. These use the non-scheduling Threaded context when no owner Io is available, with cancellation protection for non-cancelable APIs and freestanding processor hints preserved.
- Existing borrowed-Io file-lock waits retain cancellation checks and their staged backoff. Test polling uses testing Io. Writer-lock retry helpers now honor their configured Io delays instead of relying on the removed `Thread.sleep` compatibility branch.

The LMDB commit worker owns one guaranteed-concurrent scheduling slot, separate
from its optional worker-initialized async I/O runtime. Io mutexes/conditions
replace its pthread handshake, preserving readiness/error publication,
serialized synchronous submissions, pending-work drain, and destruction after
completion. Scheduling refusal still maps to the existing OutOfMemory error
contract. Environment resource/statistics/mapping locks also use Io mutexes.

The synchronous linalg API retains a process-lifetime bounded owner on Linux,
with at most seven background workers plus caller participation. Io groups
replace the raw worker slots, futex initialization/parking, and completion
counter. Concurrent Sync submissions remain serialized; excess job counts,
capacity pressure, and allocation failure can execute inline. Non-Linux and
single-threaded Sync paths retain their sequential fallback. Io-aware callers
continue to use their borrowed executor. The `have_futex` implementation flag
is replaced with `supports_sync_parallelism` at its caller. Process-lifetime
retention is unchanged; local-owner tests explicitly drain the pool.

CUDA staging now owns guaranteed-concurrent futures on a private executor
bounded to the configured producer count. Startup failure stops and wakes all
started producers before awaiting them. Normal/error teardown drains producers
before the existing GPU stream fence, retaining pinned slots and destinations
until all transfers complete. Tests exercise zero/partial capacity and stopped
producers using full staging slots without requiring a GPU.

Prepared-shard prefetch accepts borrowed Io and uses bounded Group.async work,
retaining source/shard identity checks, shared work indexing, byte accounting,
and cleanup after every worker completes. Nested per-shard reads stay serial.
The Linux prepared-pack test checks normal and zero-capacity inline execution.
There are no direct Thread references left in the CUDA source directory.

At that stage four direct spawn sites remained. The HTTP control follow-up
below converts three; the hard-shutdown watchdog remains an explicit exception.

Validation for the yield/LMDB/linalg/CUDA follow-up:

- `zig build lmdb-test storage-lmdb-test -j1` passed; the final `lmdb-test` rerun includes concurrent submitters with and without the worker-owned I/O runtime, plus unavailable scheduling capacity.
- `zig test zig/lib/linalg/src/mod.zig -lc`: all 37 tests passed. Local pool tests exercise concurrent callers, zero capacity, repeated submissions, and inline completion while the only worker is occupied.
- The broader storage runs completed 3,439 tests with eight skips and no failures or leaks, including DB core, WAL grouping, retry, and storage concurrency regressions. The initial combined invocation also encountered the corrected linalg capability-name compile error in the production target and sandbox-blocked serverless listeners; its successful storage results are reported separately. Both affected targets passed their later runs.
- `zig build lib-storage-test -j1 -- 'backend runtime' 'lane lease'`: 47 passed, no skips, failures, or leaks, including owner-close draining and recursive submission rejection.
- `zig build lib-standalone-runtime-test serverless-test -j2` with loopback access: 95 standalone tests and 140 serverless tests passed, with two serverless skips and no failures or leaks. Initial sandbox failures came from local TLS/HTTP listeners.
- `lib-vectorindex-test`: all 58 tests passed. Platform synchronization tests: both passed.
- `zig build inference-test -Dcuda=true -j1 -- 'CUDA A4B pipeline' 'prepared pack'`: five passed, one Linux-only skip, plus four Python source-policy tests. All three staging tests execute: final-ready-slot ordering, ready/stop notifications, and zero/partial/full worker startup and teardown. The default build excludes CUDA tests, so its success is not treated as staging validation.
- Default focused inference checks also passed: 22 parallel/prefetch/control tests, then 15 prepared-pack/prefetch/control tests with one Linux skip after the CUDA spawn migration.
- Linalg pool and prepared-pack tests cross-compile for `aarch64-linux-musl`. Linux execution, Linux kernel performance qualification, and GPU transfer execution were unavailable locally.
- Final `zig build antfly -j1` and `zig build antfly -Dcuda=true -j1` both passed, along with CLI `--help` smoke checks. The CUDA-enabled build validates the full production loading path; GPU transfer execution still requires hardware.
- Optional `zig build install-wasm -j1` does not pass: it reports broader freestanding/32-bit compilation problems, including unsupported PATH_MAX, 64-bit atomics, architecture constants, and unrelated missing members. No successful WASM validation is claimed.

## Backend runtime worker ownership

`BackendRuntime.acquireWorkers` reserves an exclusive, heap-stable executor with
async scheduling disabled. The configurable aggregate `worker_capacity` defaults
to 256. These reservations are separate from API, inference, control-request,
and durable-job capacity. Failed acquisition rolls back accounting; manual
runtimes reject automatic workers. Worker capacity, current/peak reservations,
and active leases are exposed in lane stats and metadata/data health metrics.

Owners stop and join leaf workers before releasing their leases. Runtime
shutdown closes all lane admission and waits for leases before destroying any
executor. The lease gate holds its drain mutex before publishing a final release,
so teardown cannot overtake that release's final notification.

Raft HTTP hosts reserve frame senders, snapshot senders, acceptance, peer
observation, and artifact maintenance before constructing the transport. Node
entry points separately reserve Raft progress capacity. This retains independent
progress and stop-before-drain semantics without importing database runtime types
into transport, observer, or progress-driver implementations.

### Worker ownership validation after integrating main

Validated on macOS ARM64, Zig 0.16.0, after integrating PR head `80f499cfe`:

- `zig build -Dmetal=false -j1`: production build passed.
- `zig build lib-storage-test -Dmetal=false -j1 -- storage.background_runtime`:
  37 passed, including capacity isolation/refusal, allocation rollback, manual
  mode, shutdown waiting, and the deterministic final-release race regression.
- The combined `raft-runtime-test raft-transport-test raft-storage-test
  lib-data-runtime-test api-http-runtime-test lib-httpx-test
  lib-standalone-runtime-test serverless-test -Dmetal=false -j1` build passed
  with loopback access: 149 DataServer, 16 Raft runtime, six Ready continuation,
  43 transport, 135 snapshot-storage, 101 API HTTP, 95 standalone, and 140
  serverless tests passed. One snapshot-suite external soak and two serverless
  tests skipped; no failures or leaks. The standalone httpx suite also passed.
- `zig build lib-hash-test -j1`: passed after converting the concurrent CPU
  feature-cache test introduced by the main merge to futures and an event.
- Formatting and whitespace checks passed. No new Linux/GPU execution or
  aggregate all-unit validation is claimed by this follow-up.

The nested-owner drain correction also passed all 39 background-runtime tests
and 149 DataServer tests on the same platform. Its deterministic regression
covers nested closes from job callbacks and payload destructors during owner
drain, owner close, and runtime shutdown, plus concurrent drains of one owner.
The nested-close regression fails with the former locking restored in an isolated
source copy.

## HTTP control executor follow-up

The std HTTP accept loop, peer/deadline observer, and httpx cancellation observer
each run one guaranteed-concurrent future with capacity independent of requests.
Application runtime owners now acquire dedicated `BackendRuntime.WorkerLease`
reservations and pass borrowed scheduling `Io` to these components. The httpx
`HttpRuntime` owns a bounded observer executor when no capability is injected;
standalone std HTTP components retain their bounded fallback. Configured accept/httpx stack sizes and the peer observer's 4 MiB
stack floor are preserved. The accept loop still uses the request executor for
socket I/O and connection handoff, including inline fallback when that executor
has zero concurrent capacity. Neither observer consumes request pool capacity.

Start/stop serialize access to futures and executor destruction. Observer
registration checks atomic lifecycle publication instead of reading a mutable
future. Failed startup releases executors, kernel queues, and listener resources;
retry creates fresh control owners. Listener stop publishes stopping, wakes retry
waits and the accept socket, drains acceptance, shuts down accepted sockets,
drains connection work, then releases the observer and server. The control
executor remains alive through this sequence.

Idle observer waits use latched Io stop events. Active poll/kevent/WSAPoll calls
retain their bounded 25 ms kernel timeouts: stop/await does not depend on future
cancellation interrupting those raw syscalls. Registrations stay multiplexed,
with existing deadline/failure publication, descriptor retirement, and distinct
peer-observer FIN versus httpx half-close semantics preserved. This is a Threaded
backend migration, not a claim of arbitrary evented-backend support.

The hard-shutdown watchdog is the sole direct-spawn exception. Keeping it outside
Io preserves its stronger guarantee: expiration remains independent of faults in
Io scheduling, waiting, and destruction, as well as exhausted application pools.
The source now identifies that exception explicitly.

Validation on macOS ARM64 with Zig 0.16.0:

- `zig build common-http-test lib-httpx-test -j2 --summary all` passed with local socket access: 126 common HTTP tests passed, one optional external-endpoint soak skipped; all 522 httpx tests passed. No failures or leaks. Initial sandbox runs failed at prohibited local binds; the unrestricted runs passed.
- New tests force control-capacity refusal, check rollback and retry, verify the one-worker ceiling, and race two stop callers through repeated listener/httpx observer restarts. Existing tests cover zero-capacity request Io, deadline storms, peer cancellation, HTTP half-close behavior, blocked-read shutdown, and watchdog expiration outside Io.
- The httpx observer tests cross-compile for `aarch64-linux-musl` and `x86_64-windows-gnu`. These are compile checks; Linux/Windows execution and Linux-only resource-stability checks still need those platforms.
- Final `zig build antfly -j1` and the production CLI `--help` smoke check passed.
- Source scan finds one direct spawn (the watchdog) and zero direct yields.

## Serverless, inference, and legacy retirement follow-up

Serverless management and publication loops borrow scheduling Io from their
owners. Concurrent futures have serialized start/stop, explicit stop events,
and interruptible interval waits. The persistent range cache retains its
existing mutex/condition queue and drain-on-close policy, with a future on its
one-worker owned executor.

The inference prefetch queue owns one worker slot independently of request
capacity. Io mutexes and a latched wake event replace its atomic spin lock and
idle polling. Weight-pin callers borrow the queue's synchronization context;
shutdown stops the worker and clears those handles before destroying the queue.
Priority, processing with or without the queue lock, and deterministic manual
test draining remain intact.

CLIP preprocessing and file prefetch accept scheduling Io and use bounded
Group.async fan-out with safe inline fallback. CLIP retains caller participation
and the eight-worker/CPU cap; the synchronous embedding pipeline owns a bounded
executor for only its preprocessing phase. File prefetch retains the one-to-eight
worker clamp, disjoint reads, eight-MiB worker buffers, and backend-specific
pread/fadvise implementation. Groups drain before results, file descriptors,
or worker state can be released. Existing server callers supply Io; the already
serial inner CUDA shard read uses the single-threaded Io context.

The old derived worker and httpx executor implementations are deleted. Shared
derived callback declarations live in `runtime_types.zig`; active manual and
Io-derived execution retain the same callback contracts. Removing the exported
`db.async_runtime` and httpx `executor`/`Executor`/`Task` symbols is an intentional
API removal. Serverless constructors, CLIP batch preprocessing, and file
prefetch now require scheduling Io; their in-repo callers are updated.

At this stage eight direct spawn sites remained. The LMDB/linalg/CUDA follow-up
above removes four more. CPU-count and thread-identity queries remain separate
platform operations.

Validation for this follow-up:

- `zig build serverless-test lib-httpx-test -j2`: passed; loopback listeners require sandbox escalation.
- Final `zig build serverless-test -j1`: 140 passed, two skipped, no failures or leaks. Lifecycle coverage includes capacity refusal, duplicate start, restart, and prompt stop with a one-minute publisher interval; existing range-cache drain tests also pass.
- `zig build inference-test -j1 -- prefetch lazy clip CLIP`: 64 passed, 14 platform/model-dependent skips; four Python source-policy tests also passed. Covers real queue worker wake/stop/restart, priority/manual drain, zero-capacity startup, CLIP inline/concurrent equality and invalid-image cleanup, file-prefetch inline/concurrent execution and worker bounds, and native lazy-weight paths.
- `zig build lib-storage-test -j1 -- 'io threaded'`: nine active derived-runtime tests passed, no skips, failures, or leaks.
- Standalone queue tests: four passed. Standalone file-prefetch test: one passed on macOS using the common pread/group implementation.
- The file-prefetch test cross-compiles for `aarch64-linux-musl`. No Linux execution or GPU run was performed; hardware/model-dependent inference tests retain their skips.
- `zig build antfly -j1` and the resulting binary's `--help` smoke passed.
- Formatting, `git diff --check`, and source scans passed. Eight direct spawn sites remained at that stage. The next follow-up handles the remaining yield calls; CLIP CPU discovery remains a platform query.

## Benchmark implementation follow-up

All 19 benchmark spawn sites now use guaranteed-concurrent Io futures. Each
sample owns finite capacity matching its worker count, so completed samples do
not retain a larger worker pool into subsequent measurements. Startup barriers
use Io events. Partial startup releases barriers or stops persistent readers,
then awaits every started future before freeing contexts or results. Early
worker errors also drain the rest of their batch.

Public-query runs reserve room for all query workers plus four pollers in an
executor separate from request handling. Pollers retain 512 KiB stacks and the
recall heartbeat retains its independent 256 KiB stack. Standalone load pollers
now register cleanup before the first task starts. Two ingest-harness delay
helpers also use Io sleep instead of direct thread yielding.

The httpx comparison now reports sequential requests, concurrent futures on a
fresh Io owner per sample, and the existing concurrency API on a shared Io
owner. The former OS-thread column is no longer an OS-thread baseline. Worker
counts and existing timing boundaries are retained; results need rebaselining
because Io scheduling, event waits, and executor teardown change overhead.
Local smoke measurements are correctness checks, not performance qualification.

Benchmark validation:

- `zig build wal-bench derived-log-bench -j1`: both plain and grouped runs.
- `zig build benchmark-io-test wal-bench derived-log-bench -j1`: both startup regressions passed with zero and one available worker slot, followed by normal benchmark runs.
- `zig build lsm-backend-bench -j1 -- --samples 1 --keys 64 --value-size 32 --hit-repeats 1 --miss-repeats 1 --short-scan-len 4 --short-scan-repeats 1 --full-scan-repeats 1 --reopen-repeats 1 --mixed-repeats 1 --concurrent-read-threads 2 --concurrent-read-keys 16 --concurrent-read-repeats 2 --storage memory --cache both`: 22 workload rows, including concurrent reads with and without cache.
- `zig build lsm-write-bench -j1 -- --samples 1 --keys 64 --hot-keys 16 --overwrite-rounds 2 --value-size 32 --batch-size 16 --readers 2 --storage memory --mode both`: 14 workload rows; overwrite readers reported zero errors.
- `zig build rw-lock-bench -j1 -- --docs 64 --write-batches 4 --batch-size 8 --search-threads 2 --body-repeat 2`: concurrent searches and writes, zero failed searches.
- From `zig/lib/httpx`, `zig build bench-concurrency -j1`: all sequential/concurrent batches, any, and race checks completed against the local echo server.
- `zig build public-query-standalone-guardrail-build recall-harness-build -j1`: passed.
- `zig build public-query-standalone-guardrail -j1 -- --mode standalone --docs 64 --dims 16 --queries 4 --repeats 2 --k 4 --batch-size 16 --search-threads 2 --poll-interval-ms 20`: passed against a temporary local production binary, with zero health/metrics/status failures during both load and query phases.
- `zig build recall-harness -j1 -- --dataset-dir /private/tmp --dataset std-io-heartbeat-smoke`: heartbeat startup and teardown passed with an empty case selection; this does not validate dataset recall.
- Both `dense-ingest-guardrail-build` and `provisioned-dense-ingest-guardrail-build` compiled successfully.
- `zig build dense-stack-bench -j1 -- --docs 64 --dims 16 --queries 4 --repeats 2 --k 4 --batch-size 16 --search-threads 2`: passed direct, concurrent, packed C API, and wire C API phases after releasing the direct DB before reopening the root.
- Final formatting, whitespace checks, and a source scan passed: no direct `Thread` references remain in the migrated benchmark sources, and 16 inventoried runtime/library spawn sites remained at that stage.

Benchmark build wiring also supplies existing missing CAPI, reader-config,
and httpx JSON imports. The old direct/local public-query benchmark still
depends on the removed `ApiHttpServer.init` / `executor` compatibility API;
its build failure predates this migration. The standalone variant excludes
that obsolete compatibility lane and passed its build and smoke run. The
dense-stack benchmark now releases the direct DB before the C API opens the
same root, preserving resource counters before that close and retaining the
separate timing of each phase.

## Validation of the test and data/storage follow-ups

The production build (`zig build antfly -j1`) and CLI `--help` smoke passed.
The combined storage, metadata, data-runtime, and standalone checks passed
4,129 tests with eight skips and no failures or allocator leaks:

```sh
zig build unit-storage-test unit-metadata-test lib-data-runtime-test lib-standalone-runtime-test -j2
```

The migrated metadata election/backup simulation passed with its custom stack
size. Standalone Prometheus tests passed (37 tests), linalg passed (35 tests,
one platform skip), and the scraping suite passed after the HTTP fixture fix.
The remaining focused batch covered root/API HTTP, production table writes,
secrets, user management, httpx, MCP, serverless, inference, and data runtime:

```sh
zig build root-test common-http-test api-http-runtime-test lib-api-graph-snapshot-test api-table-writes-production-regression-test lib-common-secrets-test lib-usermgr-test lib-httpx-test lib-mcp-test serverless-test inference-test lib-data-runtime-test -j1
```

Every target passed except the existing graph fixture's missing routing
callbacks. After fixing the fixture, `zig build lib-api-graph-snapshot-test -j1`
passed all 12 tests. The final data-runtime run passed 149 tests, including the
new reserved-capacity and owner-close regression. Inference's largest suite
passed 3,411 tests with 22 skips. All-unit runs are not claimed: intermediate
attempts were superseded by these focused checks. Linux/GPU and performance
validation remain outside this local macOS run. The final four test-helper
polling regressions also passed: artifact finalization, post-delete readers,
active text-merge shutdown, and torn lifecycle-ledger recovery. Formatting and
`git diff --check` pass for the final tree.

## Raft implementation follow-up

The worktree now migrates all direct `std.Thread` references in `lib/raft/`,
`pkg/antfly/src/raft/`, and the data and metadata `storage/raft_apply_store.zig`
files. Snapshot building, artifact maintenance, Raft progress, frame sending,
and snapshot sending use owned `Io.Future` lifetimes. Progress and sender
executors have separate bounded capacity; their stop/wake/await ordering keeps
their resources alive until work drains. Snapshot startup rollback publishes
its stopped state only after all worker and executor cleanup finishes.

Catalog and inbound-host locking use Io mutexes, with borrowed synchronization
contexts. Test helpers use concurrent futures and Io events/waits. New failure
injection tests exercise partial sender startup, rollback, restart, and worker
allocation failure; existing progress and peer-isolation tests now exercise a
caller executor with no concurrency capacity. This removes 15 spawn sites
(five implementation sites and ten test/helper sites) from the original inventory.

Focused validation commands, run from `zig/` unless shown otherwise:

```sh
zig build raft-test raft-transport-test -j2
zig build lib-data-storage-test -j1 -- 'data raft'
zig build unit-metadata-test -j1 -- 'lifecycle listener detach drains callbacks and preserves unrelated listeners'
# From the repository root:
zig test zig/lib/raft/src/root.zig
```

The broad Raft HTTP checks need permission to bind local loopback sockets.
The optional external wrong-route endpoint soak is not enabled by these commands.
The remaining assessment and source inventory below describe the original base
commit, including the implementation sites now converted above.

Migrating application-owned thread lifetimes to `std.Io` is feasible on the
currently pinned Zig **0.16.0**; no compiler upgrade is required. Most of the
infrastructure already exists. Completing this safely requires ownership,
admission, and shutdown changes, rather than a global spawn/join substitution.
Eliminating every `std.Thread` reference is a broader project than migrating
thread creation: CPU discovery and OS thread identity have no equivalent on the
0.16 `Io` interface, and synchronization/backoff needs separate treatment.

## Inventory

The scan covers tracked Zig source throughout the repository; all matching
files are under `zig/`. Counts are source call sites, not running thread counts.
One site can create many workers or execute for many database instances.

| Spawn category | Sites |
| --- | ---: |
| Runtime/library implementations, including two legacy implementations | 30 |
| Tests and test-only helpers | 237 |
| Benchmarks | 19 |
| **Total, across 89 files** | **286** |

Of these, 283 spell `std.Thread.spawn`; three use the `Thread` alias in httpx.
The complete call-site list, with original line numbers and source text, is in
[std-thread-inventory.json](docs/std-thread-inventory.json).
Tests were identified by enclosing test blocks and manual review of helper
functions. Runtime/library classification does not imply every optional backend
or exported library implementation is exercised by the default executable.

Separately, a source scan excluding comments and quoted strings finds **587
direct `std.Thread` references across 115 files**: 283 `spawn`, 210 `yield`,
78 type/namespace uses, seven `getCpuCount`, five `sleep`, three `getCurrentId`,
and one `SpawnConfig`. The alias uses are additional. Some sleep calls are
guarded compatibility paths; Zig 0.16's `Thread` no longer exports `sleep`.

### Runtime and library worklist

Paths below are relative to `zig/`. Line numbers refer to the audited commit.

| Owner / source | Spawn lines | Required work |
| --- | --- | --- |
| **Implemented** `lib/raft/src/runtime/multi_raft.zig` — snapshot builder | 345 | Already owns `Io.Threaded`, `Io.Mutex`, and `Io.Condition`. Replace the handle with an owned future; preserve source cancellation, wake, result cleanup, and drain before executor destruction. |
| **Implemented** `pkg/antfly/src/raft/runtime_loop.zig` — progress driver | 112 | Already accepts `Io` and uses events. Use a concurrent future with capacity reserved independently of request work; retain cadence, failure publication, stall detection, and one-shot lifecycle. |
| **Implemented** `pkg/antfly/src/raft/transport/http_driver.zig` — frame senders | 197 | Replace the bounded handle array with a group or futures. Keep queue bounds, worker indexing, partial-start rollback, and stop/wake before drain. |
| **Implemented** `pkg/antfly/src/raft/transport/http_snapshot.zig` — snapshot senders | 546 | Same, retaining per-peer admission and the starting/running/closing handshake. Serialize group/future teardown just as handle initialization and joining are serialized today. |
| **Implemented** `pkg/antfly/src/raft/storage/file_snapshot_store.zig` — artifact maintenance | 928 | Already owns an executor and event. Convert the handle; retain final maintenance/cleanup ordering and finite capacity. |
| **Implemented** `pkg/antfly/src/serverless/runtime/manager.zig` — management loop | 93 | Add borrowed scheduling `Io` to init/start and its bootstrap callers; replace polling sleeps with stop-aware waits. |
| **Implemented** `pkg/antfly/src/serverless/build/coordinator.zig` — publisher | 48 | Add `Io`, convert the loop and wait mechanism, preserve publication and shutdown behavior. |
| **Implemented** `pkg/antfly/src/serverless/query/lake_parquet_rowgroup.zig` — persistent range cache | 301 | Already owns `Io.Threaded` and an Io condition/mutex. Convert the worker, explicitly bound concurrency, preserve queued-write drain and cache-state lifetime. |
| **Implemented** `pkg/antfly/src/data/runtime.zig` — LSM maintenance, cache warmup, runtime status, startup catch-up, root refresh, local-group status | 7829, 14293, 15427, 15543, 15547, 16267 | Convert six entry points and their lifecycle fields/callers. Use existing owner-scoped durable jobs for appropriate finite work; use concurrent futures for persistent loops. Keep status and Raft progress independent of expensive work, generation checks, coalescing, and teardown ordering. |
| **Implemented** `pkg/antfly/src/storage/lsm_backend.zig` — internal flush worker | 6228 | Supply scheduling `Io`, replace polling/wake state with Io waits, preserve `drain_on_stop` and obsolete-file reclamation. Scope executor ownership above individual backends where practical. |
| **Implemented** `pkg/antfly/src/storage/db/db.zig` — quarantine retry | 23824 | Use the database's background ownership machinery or a concurrent future. Preserve stable-address startup, best-effort spawn failure, stop/join, and deterministic manual test mode. |
| **Implemented** `pkg/antfly/src/storage/db/catalog/index_manager.zig` — parallel index open | 5886 | Plumb scheduling `Io`; bounded finite fan-out can use `Group.async` if inline execution is safe. Preserve read-only-only parallelism, per-index errors/quarantine, partial startup cleanup, and result ownership. |
| **Implemented** `pkg/antfly/src/common/http/std_http_listener.zig` — accept loop | 266 | Use a future on a control executor with explicit capacity and stack sizing. Preserve accept wakeup, connection cancellation, observer teardown, and borrowed/owned executor lifetimes. See the zero-capacity contract below. |
| **Implemented** `pkg/antfly/src/common/http/peer_disconnect_observer.zig` | 161 | Add Io ownership but retain one multiplexed observer task. Raw poll/kqueue and nanosleep need an explicit backend/wakeup design for portable cancellation. |
| **Implemented** `lib/httpx/src/server/cancellation_observer.zig` | 123 | Same observer constraints; preserve startup-error reporting and configurable stack size. |
| **Retained exception** `pkg/antfly/src/common/runtime_lifecycle.zig` — hard shutdown watchdog | 68 | Requires a separately owned, isolated watchdog executor or a documented exception. Never schedule this on an executor it is timing out. |
| **Implemented** `pkg/antfly/src/lmdb/env.zig` — commit worker | 182 | Add a bounded scheduling owner; review pthread mutex/conditions and synchronous submitters together. Preserve serialization, readiness handshake, optional worker-owned async runtime, C error mapping, and transaction/thread assumptions. |
| **Implemented** `lib/linalg/src/pool.zig` — process-wide Sync kernel pool | 172 | Prefer existing `dispatchJobsIo` at callers. Decide whether Sync APIs become sequential or borrow an explicitly owned compute runtime. Removing the global futex pool requires performance validation. |
| **Implemented** `pkg/inference/src/runtime/tier/prefetch.zig` — generic prefetch queue | 97 | Plumb `Io` through queue owners and mutex callers. Replace the no-op signal plus idle polling with a condition/event; preserve priority, processing-under-lock option, and manual test draining. |
| **Implemented** `pkg/inference/src/pipelines/image.zig` — CLIP preprocessing | 1134 | Add scheduling `Io` through preprocessing calls; bounded independent CPU jobs can use group async with caller participation and identical result/error cleanup. |
| **Implemented** `pkg/inference/src/util/c_file.zig` — parallel prefetch | 538 | Add scheduling `Io`; keep worker/read-buffer bounds. Blocking pread remains backend-specific until migrated to Io file operations. |
| **Implemented** `pkg/inference/src/ops/cuda/a4b_prepared_pack.zig` — shard warming | 544 | Add `Io` and bounded finite fan-out; preserve independent read streams and cleanup. |
| **Implemented** `pkg/inference/src/ops/cuda/cuda_compute.zig` — A4B staging pipeline | 2997 | Use guaranteed concurrency: workers and upload consumer depend on each other. Convert polling to notifications where practical; retain stop-before-drain and GPU completion fences before freeing pinned staging buffers. |
| **Retired** `pkg/antfly/src/storage/db/derived/async_runtime.zig` — legacy derived worker | 191 | Removed implementation and export; shared declarations now live in `runtime_types.zig`, used by the existing manual/Io executor paths. |
| **Retired** `lib/httpx/src/concurrency/executor.zig` — legacy exported executor | 107 | Removed implementation and its `executor`, `Executor`, and `Task` exports as requested. No in-repo consumers remain; external users of those exports must use the existing Io-based concurrency API. |

## Migration rules and concrete constraints

**Choose the scheduling guarantee deliberately.** A single thread handle usually
becomes `?Io.Future(void)` (or a future carrying the actual result); a bounded
worker set can become `Io.Group`. Use `io.concurrent`/`group.concurrent` for
listeners, background loops, producer/consumer workers, and contention tests.
They must allow the initiating caller to continue. Use `async` for independent
finite work that remains correct when executed inline. It may execute before
returning, so invoking it while holding a lock the task takes can deadlock.
Group tasks return `void` or `Io.Cancelable!void`; other errors need explicit
result storage or futures. These semantics are documented in the official
[Zig 0.16 release notes](https://ziglang.org/download/0.16.0/release-notes.html#Future).

**Preserve admission independently of the runtime ceiling.** `Io.Threaded`
retains its pool until deinit. Migrating spawn/join to concurrent/await does
not by itself reduce OS threads; short-lived spikes can become retained pool
capacity. Existing service/inference limits are 256/64 in
`pkg/antfly/src/common/threaded_io_limits.zig`. Account for persistent workers
and nested fan-out, keep workload admission below those ceilings, and avoid
making every component create a default unlimited executor. Control, durable
work, and request lanes already have useful separation in
`storage/background_runtime.zig`. A finite ceiling is not a reservation: work
that must start despite request saturation needs a separate owner/capacity plan.

**The HTTP listener has an existing zero-capacity contract.** Tests around
`std_http_listener.zig:2304`, `:2367`, and `:2489` create a shared runtime with
both limits set to `.nothing` and still expect the listener to start. The
dedicated accept thread allows inline request fallback and deadline observation
to work. A direct replacement with `self.io_impl.io().concurrent(serve, ...)`
would fail startup. Preserve this with a separate bounded control executor,
or deliberately revise the contract and its tests. Keep one observer task per
observer, rather than a task per registered request/socket.

**Keep stack constraints.** `Io` has no per-task `SpawnConfig`; stack size is an
`Io.Threaded.InitOptions` setting. The repo documents a 4 MiB infrastructure
floor for the partitioned Linux executable in `runtime_thread_config.zig`,
while the std HTTP request stack defaults to 8 MiB. Moving tasks between pools
must preserve those requirements. Small benchmark stacks should likewise be
accounted for when comparing memory results.

**Drain before freeing.** Publish stop state, wake blocked workers, await all
started tasks, then free queues/resources and finally deinit owned executors.
Future and group await/cancel are not safe for multiple simultaneous teardown
callers without owner synchronization. Preserve existing partial-spawn rollback
and generation/state machines. `cancel` also waits; it does not forcibly kill
CPU code, raw pthread waits, polling loops, or foreign calls. Audit broad
`catch {}` paths so newly surfaced `error.Canceled` is not silently consumed.
Durable writes/flushes need explicit completion policy and, where required,
uncancelable cleanup rather than a blanket replacement of join with cancel.

**The watchdog must remain independent.** Its source explicitly promises a hard
deadline even if the application executors cannot drain. For literal removal
of direct spawning, provision a private bounded `Io.Threaded` watchdog owner,
with its own task/wakeup and lifecycle, before application teardown. Demonstrate
that it still expires with service pools exhausted or stuck. This preserves
independence from service executors, but changes the current contract of
independence from Io itself; retaining that stronger contract requires an
exception or an external supervisor design. The normal task group is unsuitable.

**Sync and foreign interfaces need an explicit policy.** The linalg pool already
offers `dispatchJobsIo`; the raw pool exists because `*Sync` entry points have no
Io and need low-overhead Linux parallelism. Move production callers to Io-aware
paths first, then decide the Sync fallback. LMDB, prefetch, and C file helpers
likewise need Io propagation or owned runtime boundaries. For a future evented
backend, additionally audit raw pthread/futex waits, OS poll/pread, thread-local
state, and CUDA/LMDB thread-affine operations. Merely changing task creation is
compatible with keeping `Io.Threaded`, but does not prove arbitrary-backend safety.

**Handle non-spawn references separately.** Replace waiting spin loops with
`Io.Mutex`, `Condition`, `Event`, or timed waits as appropriate, carrying the
same owner Io through lock/unlock and wait/signal. Do not translate the 210
`Thread.yield` sites into unconditional sleeps: some are lock backoff, others
test barriers or GPU progress loops. Tiny atomic-only critical sections can
retain a reviewed spin strategy. Five sleep sites need Io-aware cleanup,
including compatibility fallbacks. CPU discovery and profiling/debug thread
IDs can stay in a small platform boundary; if zero namespace references is a
strict acceptance criterion, inject CPU budgets and use explicit operation IDs
or platform APIs. Those are not `Io` task-creation equivalents.

## Suggested delivery sequence

1. **Ownership and representative conversions.** Establish explicit scheduling
   Io and control-lane capacity. Convert the snapshot builder, artifact
   maintenance, and persistent range cache, which already use Io synchronization.
   Cover concurrency exhaustion, partial startup, restart, and drain ordering.
2. **Raft, serverless, and data/storage owners.** Convert the remaining service
   loops and senders, reusing durable-job ownership for finite work. Retire the
   legacy derived runtime after extracting shared declarations. Validate recovery,
   maintenance, status responsiveness, and shutdown under saturated admission.
3. **HTTP and deadline infrastructure.** Migrate accept/observer tasks with the
   zero-capacity and stack contracts preserved. Resolve the watchdog isolation
   design and legacy httpx export. Validate disconnects, stalled headers/bodies,
   low-FD operation, hard deadlines, and repeated startup/teardown on Linux/macOS.
4. **Inference, compute, and foreign boundaries.** Propagate Io through linalg,
   CLIP, prefetch, LMDB, and CUDA loading. Replace synchronization alongside task
   ownership where needed. Measure Linux Sync-kernel performance, CPU/GPU loading,
   memory retention, and synchronous/native/wasm compatibility.
5. **Tests, benchmarks, and prevention.** Migrate all 237 test/helper and 19
   benchmark sites. Race tests must use guaranteed concurrent work and explicit
   event barriers so they still exercise overlapping operations. Rebaseline
   benchmarks with equivalent worker/admission/stack budgets. Add a CI source
   check for direct spawn/handle usage (including aliases), with only consciously
   documented exceptions; separately track remaining yield/platform queries.

This is a multi-PR refactor. The 30 runtime/library sites are tractable; the
largest uncertainties are synchronous API propagation, watchdog isolation,
HTTP fallback behavior, and performance validation. A calendar estimate should
follow the first representative conversions and the decision on literal zero
`std.Thread` references versus zero application-owned thread lifetimes.

## Validation performed and still required

Checked the pinned toolchain policy, installed Zig 0.16.0 `Io.zig` and
`Io/Threaded.zig`, runtime owners, existing Io-based implementations, and all
286 direct/aliased spawn locations. Raw tracked-source spawn totals agree with
the categorized inventory. The four standalone
[API probes](scratch/std_io_migration_probe.zig) passed on macOS ARM64:

```sh
zig test zig/scratch/std_io_migration_probe.zig -lc
```

They exercise inline async fallback, concurrency refusal without starting a
task, stop/await across worker restarts, and draining after partial group
startup failure. They validate migration mechanics, not application behavior.
At the original audit stage, no application migration, full build, full test
suite, Linux/GPU run, or performance measurement had been performed. The
implementation and validation results above supersede that initial status.

For implementation, run focused existing targets such as `common-http-test`,
`lib-httpx-test`, `lib-raft-test`, `serverless-test`, `db-test`, and `wal-test`
from `zig/` as each owner changes, then the relevant inference tests and the
aggregate/real-binary checks described in [TESTING.md](TESTING.md). The
partitioned Linux binary, saturation/teardown tests, and applicable CPU/GPU
benchmarks are necessary evidence before calling the migration complete.
