# Zig runtime flakes

See also the [E2E flake history](e2e/FLAKES.md). Record the original evidence,
reproduction conditions, deterministic regression, and before/after results;
a passing soak alone does not establish a failure's cause.

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

Committed Raft apply now tries the read-compatible group activity once, including
nonblocking acquisition of its bookkeeping mutex. Contention returns
`RaftApplyWriterUnavailable` before any document mutation or cache invalidation.
The existing state-machine checkpoint retains the committed entry and lets the
progress owner release its mutex and retry. Structural/generation exclusion and
read compatibility are preserved. A deferred apply publishes neither success nor
an applied watermark; matching read barriers still require successful apply.
The independent read deadline below bounds quorum/apply backpressure as well.

The regression reserves a real refresh activity while applying a committed
increment, then repeats with the activity mutex held. Both attempts must return
before the reservation is released, leave the document and watermark unchanged,
and keep write/read waiters pending. After release, retry completes the write and
read barrier; replaying it again leaves the increment applied exactly once. A
watchdog releases the reservation before joining so the original code fails
without hanging. This regression is included in the default data-runtime suite.

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
