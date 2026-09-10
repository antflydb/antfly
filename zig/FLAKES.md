# Zig runtime flakes

See also the [E2E flake history](e2e/FLAKES.md). Record the original evidence,
reproduction conditions, deterministic regression, and before/after results;
a passing soak alone does not establish a failure's cause.

## Standalone routing watch deadline (#689, #694)

[PR #689, run 34418061842, job 102687782332](https://github.com/antflydb/antfly/actions/runs/34418061842/job/102687782332)
failed `standalone routing watch does not report absence after one probe` with
`expected .authoritative_absence, found .retry`. The real-clock test used a 60 ms
deadline; scheduling delay can exhaust the confirmation budget, so the
deadline-aware mutex correctly returns a retry instead of certifying absence.

[PR #694](https://github.com/antflydb/antfly/pull/694) accepts that retry only after
deadline expiry and retains the minimum-wait and unexpected-change assertions.
The watch and mutex deadline checks share an injectable monotonic clock. Manual
time requires successful confirmation before expiry and checks an overshooting
sleep, an already-expired caller budget, and mutex contention. Production keeps
the same monotonic time, sleep, and yield operations.

Validation on macOS ARM64: all 108 standalone runtime tests passed without
leaks. Setting the confirmation budget to zero temporarily made the new test
fail with the original expected/actual mismatch; the mutation was reverted.
Both routing-watch tests passed in #694's subsequent Linux x86_64 CI run
34423487352. That run failed in the unrelated dense rollback test below.

## Dense generation rollback activation budget (#694, #695)

[PR #694, run 34423487352, job 102704099568](https://github.com/antflydb/antfly/actions/runs/34423487352/job/102704099568)
failed `db failed activated dense generation rolls back to retained predecessor`.
The second repair returned `failed=1`, `unresolved=1`, and remaining debt instead
of reaching the injected `TestCrashBeforeReplacementValidation` error.

The functional rollback test used the 250 ms production activation budget.
A temporary 350 ms pause after persisting the activating phase reproduced the
CI signature on macOS ARM64. [PR #695](https://github.com/antflydb/antfly/pull/695),
commit `d36b3492d`, uses the existing five-second functional-test options for the
predecessor build, crash-injected replacement, and final recovery rebuild. It
also asserts that the predecessor completed successfully before testing
rollback. The production limit and crash, rollback, and search assertions stay
in place. The fix is also included in #694.

The injected-pause case passed after the fix. After removing the temporary hook,
all 68 tests in `zig build dense-index-lifecycle-regression-test -j4` passed
without leaks. Formatting and diff checks passed. The Linux x86_64 unit job on
#695 also passed ([run 34427566807, job 102716322007](https://github.com/antflydb/antfly/actions/runs/34427566807/job/102716322007)).
The same 68 tests passed again after inclusion in #694 with current main.

## Backup seed forwarding exhausts the control executor (#694)

[PR #694, run 34423487352, job 102714559943](https://github.com/antflydb/antfly/actions/runs/34423487352/job/102714559943)
failed the three-by-three backup E2E case while seeding its three documents:
HTTP 409 `write outcome unknown`. The artifact contained only the executable,
so the original CI log does not identify the underlying transport error.

On macOS ARM64, a native Debug build based on main `9f192f9be` reproduced the
same seed failure in **1/30 concurrent runs**. Adding error-path diagnostics
then reproduced it in **7/60 runs**, all with `ConcurrencyUnavailable` inside
the forwarded group batch HTTP client. The client conservatively translates
transport errors after its send boundary into an ambiguous write outcome;
the test correctly fails instead of blindly replaying the batch.

Both known-leader and placement-fallback forwarding used `dataRaftIo()`, which
selects the runtime's eight-worker control executor. HTTP forwarding submits
nested request, deadline, and connection tasks. Concurrent transaction
participants compete with control work for those eight slots, and admission
failure can occur after a request starts. Forwarding now uses the existing
bounded outbound Raft network executor. Control waits retain their executor,
and borrowed runtimes retain their caller-supplied transport authority. Neither
the aggregate worker budget nor the write deadline is increased.

The deterministic regression occupies every control slot, verifies that one
more task is rejected, then sends a real forwarded batch to a loopback peer.
With the old executor it fails with `RaftBatchWriteTransportOutcomeUnknown`
and underlying `ConcurrencyUnavailable`; with the fix it receives HTTP 201 and
verifies exactly one request. The borrowed-VoprIo and forwarding-error
classification regressions also pass. Diagnostics now retain the underlying
HTTP error, delivery phase, and timeout without logging request bodies.
The broader HTTP client regression had three stale expectations for transport
failures after #692 introduced the distinct internal error; they now require
`RaftBatchWriteTransportOutcomeUnknown`. Peer-reported ambiguity still requires
`RaftBatchWriteOutcomeUnknown`, and definite pre-send failure remains distinct.

The E2E fixture also declares its process resource explicitly: before the fix,
pytest collection classified this six-process cluster as `light`; afterward it
uses `antfly-process`. See the [E2E entry](e2e/FLAKES.md#three-by-three-backup-seed-batch-unknown-outcome-694)
for final soak results and remaining CI validation.

## Autograph second-document write timeout (#690)

[CI run 34395199129, job 102623777993](https://github.com/antflydb/antfly/actions/runs/34395199129/job/102623777993?pr=690)
failed the first of three low-descriptor repetitions of
`test_resolution.py::test_multinode_autograph_resolves_promotes_and_hydrates_entities`.
The `doc:b` batch timed out after 30 seconds. Data node 101 reported
`target=4 applied=3`, `diagnostics=raft_mutex_contended`, and an unknown outcome
after proposal. Resolution also reported `ReadIndexTimeout`. GDB attachment was
denied by the runner's ptrace policy, leaving no usable native stacks.

The worktree starts at `origin/main` commit `26e332ed8c335d75eecd875548c6c36bcb47d183`.
The original ReleaseFast CI executable reproduces the same `doc:b` timeout and
Raft diagnostics under concurrent Linux load: **5 failures in 30 runs**. Three
serial Linux runs passed. The reproduction uses the runner image, a 50 GiB
`standard-rwo` disk, eight-CPU affinity, and `RLIMIT_NOFILE=256`. A disposable
test launcher enables same-UID GDB attachment; the original artifact is stripped,
so those stacks contain addresses without Antfly symbols.

### Cause and fix

A symbolized, unmodified `main` ReleaseFast build reproduced the failure in
**1 of 30 runs**. The native stacks established this cycle:

1. `runProvisionedRootRefresh` holds a group refresh activity and calls
   `reconcileReplicaRootTablesWithWriteCache` → `backfillResolverCorpus`.
2. Resolver catch-up calls the distributed candidate source, whose cross-shard
   scan waits for `data_raft_mutex` to submit ReadIndex.
3. The Raft progress thread holds `data_raft_mutex` in `applyReady` and waits
   inside `beginReplicatedApplyOperationLocked` for that same refresh activity.

Managed metadata refresh now persists resolver configuration and durable backfill
cursors without running resolution or promotion inline. The resolution worker
owns bounded cursor scans and replay, wakes for catalog-only changes, and reloads
pending cursors after reopen. Cached cold opens defer resolver workers until the
stable cache entry has its distributed candidate source, entity sink, and
promotion-owner callbacks installed.

Catalog changes use nonblocking resolution/promotion callback fences while a
managed refresh owns a group activity. A busy worker causes the refresh to yield
and retry. Material configuration changes atomically reset both durable cursors
with the catalog, so a previous scan cannot clear a new generation's work.
Promotion checks the live resolver generation under the same callback fence and
skips queued artifacts from superseded configurations.
Removal retains the catalog until artifact retirement and graph replay finish;
retries do not enqueue deletes for already absent artifacts. Follower retirement
fences callbacks without waiting for leader-only promotion work. Embedded callers
retain their synchronous resolver API.

Committed Raft apply also tries the read-compatible group activity once, including
nonblocking acquisition of its bookkeeping mutex. Contention returns
`RaftApplyWriterUnavailable` before document mutation or cache invalidation. The
state-machine checkpoint preserves exactly-once document effects across retries.
The original nonblocking fix still returned from the entire host drain on this
error: a review probe showed three rounds with zero healthy read completions or
transport sends until the blocked group was released.

MultiRaft now classifies replay-safe apply backpressure explicitly and defers only
the affected group. It preserves that group's snapshot, entry, and ReadState order
while a persistent cursor gives other groups turns, including with a one-task
budget. Persisted transport can flush during deferral. Fatal apply/persistence
errors still stop the turn. Async storage-apply acknowledgements and snapshot
compaction are emitted only after the corresponding task completes. Legacy apply
queues retain their completed-prefix contract unless they opt into this scheduling.

Regressions hold real refresh activity and its bookkeeping mutex while applying a
committed increment. Attempts must return before release, leave the document and
watermark unchanged, and keep write/read waiters pending. The production router
must complete another group's read while refresh remains held. After release, the
increment applies exactly once. MultiRaft tests separately cover transport, bounded
fairness, snapshot order, fatal errors, opt-in queues, and async acknowledgements.
DB tests cover durable worker-only backfill after deferred activation/reopen,
configuration fencing and cursor reset, and resolver retirement on leaders and
followers. Watchdogs release held owners before joining on regression failure.

### Read deadline defect

`DataServer.waitDataReadSafeWithCancellation` registered its waiter, acquired
`data_raft_mutex` with an unbounded wait, and only then started its five-second
read timeout. Resolution's cross-shard reads therefore could neither time out
nor cancel while waiting for a contended Raft owner. Managed writer/worker
dependencies can keep that wait alive beyond the public write deadline.

The read now uses one absolute deadline across owner-lock admission and the
matching quorum/apply barrier. Lock contention yields through the runtime's
`std.Io`, checks cancellation and remaining time, and releases waiter ownership
on failure. It rechecks the deadline after acquiring the mutex before submitting
ReadIndex. Quorum, applied-index, restart-fencing, and write-success requirements
remain in force; neither the test's write timeout nor its retry policy changes.

The deterministic regression holds the owner mutex, starts a read, and verifies
that timeout or cancellation completes before the owner is released. A watchdog
always releases the owner before joining, so the original code fails without
hanging the runner. It also checks expired requests, pre-cancellation, lock
release, and waiter cleanup. The original implementation fails this regression;
the fixed implementation passes. The test is selected by the default data-runtime
suite.

### Diagnostics and validation

The regression script's existing `ANTFLY_E2E_NATIVE_STACKS=1` now enables an
exec launcher for the multi-node fixture on Linux. Only disposable test children
opt into sibling debugger attachment; host ptrace policy and production startup
are unchanged. The launcher preserves server PIDs and arguments.

Validation on 2026-09-09:

- Original native Debug: 160/160 passes at 256 descriptors, including six
  concurrent workers. This did not reproduce the Linux failure.
- Descriptor probes: 9/9 passes at 192 and 9/9 at 160. At 128, three runs failed
  with descriptor-exhausted node exits, a different signature.
- Deadline-only native Debug: 30/30 concurrent runs passed.
- Deadline-only Linux Debug: 30/30 concurrent runs passed.
- Final code: all 109 data-Raft tests passed, as did the production DataServer
  VOPR merge/read-barrier scenario. Both deterministic regressions fail on
  the original implementation and pass with the fix. No leaks were reported.
- Fifteen Python launcher/resolution checks passed; sibling GDB attachment was
  also verified on the Linux runner.
- Deadline-only Linux ReleaseFast: 60/60 concurrent runs passed.
- Final nonblocking apply + deadline Linux ReleaseFast: **60/60 concurrent
  runs passed** at 256 descriptors with eight-CPU affinity.
- Final Linux ReleaseFast: the regression script’s default schema-migration
  and automatic shard-split cases both passed under the same resource limits.

Reproduce from the worktree root with a built executable:

```sh
SKIP_BUILD=1 ANTFLY_E2E_ENV_LOADED=1 ANTFLY_E2E_NOFILE_LIMIT=256 \
ANTFLY_E2E_REGRESSION_WORKERS=3 ANTFLY_E2E_REGRESSION_REPEATS=10 \
scripts/ci/zig-e2e-regression-loop.sh \
  e2e/antfly/test_resolution.py::test_multinode_autograph_resolves_promotes_and_hydrates_entities
```

On Linux, prefix the script with `taskset -c 0-7` when those CPUs are available.
Build with `-Doptimize=ReleaseFast -Dstrip=false` for symbolized reproduction.
Local investigation logs are retained under `/private/tmp/antfly-ci690-*`.

Run the deterministic regressions from `zig/`:

```sh
zig build antfly-data-runtime-test -- \
  --test-filter 'data raft apply defers refresh contention' \
  --test-filter 'data raft read safety' \
  --test-filter 'data raft retry checkpoints' \
  --test-filter 'production DataServer replicated merge actions run on VoprIo'
```

### Architectural follow-up validation (#692)

The review-driven worker ownership and per-group apply changes are tracked in
[PR #692](https://github.com/antflydb/antfly/pull/692).

- Full standalone Raft suite: 391 tests passed.
- Full data-runtime suite: 173 tests passed, including production VOPR cases.
- Resolver/promotion suite: 38 tests passed, including deferred worker
  activation/reopen, configuration fencing, stale promotion rejection, and
  follower removal. The final focused data-Raft/VOPR suite also passed 110 tests.
- Managed writer-cache/restore lifecycle suite: 218 tests passed.
- Python launcher/resolution checks: 15 passed.
- Final Linux ReleaseFast soak: **60/60 passed**, with three workers,
  eight CPUs (`0-6,8`), and 256 descriptors.
- The same final Linux build passed both default regression-loop cases:
  full-text schema migration and automatic shard splitting.
  Executable SHA-256: `a3c6e7987c0eadc0f633b58373d18ec0e13e0e6529fded12eaf37d966fe317f1`.

Run the durable backfill and catalog lifecycle checks from `zig/`:

```sh
zig build antfly-storage-db-test -- --test-filter resolver --test-filter reresolve \
  --test-filter PromotionRuntime --test-filter processResolutionArtifact \
  --test-filter catchUpWindow --test-filter 'db promotes'
```

Follow-up validation logs use `/private/tmp/antfly-pr692-*`.
