# Linux storage test reassessment, PR #790

Investigation baseline commit: `3136b34eb0fc08749a91da6f8c465dbd5d5c425c`.
CI run: https://github.com/antflydb/antfly/actions/runs/35304355356

## Correct the baseline comparison

The 462.13s graph / 170.41s HBC baseline and 27.94s / 22.31s follow-up
were **macOS ARM64 Debug** measurements. Comparing the former directly with
Linux CI's 205.84s / 143.70s is not a valid before/after comparison.
The earlier Linux CI run 35277188581 recorded 288.7s graph / 192.5s HBC.
The new Linux CI observations are approximately 29% / 25% lower, but these
are shared-runner observations, not controlled performance benchmarks.

The baseline run’s main unit build/test step passed in 27m16s; the full job took
41m02s. Its selection audit reports 16,543 named tests/executions and no
repeats. Scale tests were excluded. The aggregate graph/HBC totals are sums
of test runtimes, not workflow elapsed time.

## Controlled filesystem comparison

Cross-compiled the current focused tests with Zig 0.16.0, Debug,
`aarch64-linux-gnu`; ran the same executable in local native ARM64 Linux
containers. Allocation traces were unset; timing/work profiles were enabled.
Normal sync calls and all assertions remained enabled. Tests ran sequentially.
The disk is Docker overlay storage; RAM-backed storage is tmpfs. This isolates
a filesystem effect locally; it does not measure the CI runner's exact disk.
The host had other builds running, so small CPU-time differences are noise.

| Fixture | Linux disk seconds | Linux tmpfs seconds |
| --- | ---: | ---: |
| Portable archive, 4,100 schema epochs | 35.58 | 3.97 |
| Applied-state, 1,025 flushed overwrites | 17.20 | 1.85 |
| Relational clean coalescing | 11.91 | 3.92 |
| HBC randomized insert/delete churn | 8.72 | 1.13 |
| Reopened PageRank, 90 scheduler steps | 18.46 | 4.80 |
| Wide external-vector update/reopen | 21.07 | 21.30 |
| Three-server VOPR history plus replay | 17.01 | 11.02 |

Current CI times for overwrite and clean-coalescing are 59.41s and 34.68s;
the local paired results isolate the filesystem effect, but do not predict
the exact result on CI's different hardware/storage/concurrency.

**Path detail:** HBC's `TestPath` and graph's `TestHelpers.tempPath` hard-code
`/tmp`. `std.testing.tmpDir` and `TestDirectory` use `.zig-cache/tmp` under
the working directory. Changing just the working directory or `TMPDIR`
does not move every fixture. The controlled graph/HBC rerun mounted both
`/tmp` and the working directory on tmpfs.

PageRank's disk close phase was 11.44s of 18.46s. On tmpfs it was 1.19s;
open was 1.99s, worker 0.74s, coordinator 0.74s. The fixture still completed
all 90 steps. The prior reaper wake fix therefore did not remove all close
costs; the remaining filesystem-dependent costs are material.

Wide-vector work on tmpfs: insertion 13.39s, persistence 2.69s, query 4.52s,
checkpoint 0.14s, close 0.036s, reopen 0.009s. Counters: 2,048 routes,
2,041 centroid recomputations, 261,694 centroid members scanned. Reopening
is not the bottleneck, and faster fixture storage does not fix this workload.

Relational selection on tmpfs: 19.30s total, setup 1.88s, columnar 2.96s,
reference primary scans 14.05s, dense validation 0.39s. This is predominantly
the repeated reference-query workload, not durable setup.

A separate `strace -f -c` run of archive, overwrites, clean coalescing, and
reopened PageRank passed all four tests and counted **48,675 fsync calls and
4,405 fdatasync calls** (53,080 sync calls), plus 43,852 pwrite64 calls.
This confirms a large durability-call workload. Do not treat strace's default
CPU-time summary as I/O wait time. Ptrace substantially inflated wall time
(PageRank alone rose from 18.46s to 100.68s), so traced durations are excluded
from the comparison table.

## Recommended work, preserving correctness boundaries

1. **Graph/HBC lifecycle fixtures:** use a bounded RAM-backed unit-test
   workspace in Linux CI and a common test-path helper. Preserve reopen,
   publication, eviction, split/merge and old-reader assertions. Keep explicit
   physical-storage durability/fault tests on their appropriate backend;
   tmpfs cannot validate power-loss durability. Do not globally disable fsync
   or change production durability defaults. Capacity must fit the existing
   pod/runtime memory budget; do not mount the compiler cache on tmpfs.
2. **Archive history:** batch the 4,100 seed records in test setup, preserving
   the original epoch count, decoded-cache eviction, duplicate rejection,
   public export, and both import modes. Individual seed commits are not the
   boundary this test asserts. Both `importMetadataBatch` and
   `validateAndImportMetadataBatchPayload` already use `putBatch`; the obvious
   per-record commit loop is test setup, not those production import paths.
3. **Flushed apply-state overwrites:** preserve the 1,025-flush case and its
   work assertions on fast fixture storage. Add a smaller disk-backed
   correctness case only after deriving the compaction/level transition it
   must reach; do not simply lower the loop count. Treat the current test as
   a forced durable-write workload, not evidence of repeated no-op writes.
4. **Relational selection:** separate the projection/filter/dirty-overlay
   comparison matrix from the physical-plan regression. Run semantic cases
   on small fixtures; retain the 768-row wide fixture for explicit multi-block,
   late-materialization and sequential-read checks. The earlier 512-row
   substitution failed the physical-plan assertion and must not be repeated.
5. **Clean coalescing:** retain both backends, typed/null/JSON cells, zero
   primary reads during the clean merge, and reopened results. Use fast
   fixture storage first; if reducing rows, expose the relevant internal block
   budget and prove that at least two underfilled ranges actually merge.
6. **Replicated merge/split/failover:** retain recording **and replay**, all
   three production DataServers, faults, restart, and public API assertions.
   VOPR virtualizes scheduling/network/time but this fixture explicitly uses
   host-filesystem LSM/Raft backends. Count Raft rounds and durable writes by
   phase before changing the history; do not assume its time is simulated sleep.
   The focused implementation test passed on both filesystems: 17.01s on disk
   and 11.02s on tmpfs, so storage accounts for part, not all, of its runtime.
   The helper already uses a DebugAllocator with zero stack-trace frames.
   This is not another obvious allocation-backtrace fix. The remaining work
   is real production-server orchestration and replay; capture per-phase
   round/write counts before proposing an algorithmic change.
7. **Wide-vector updates:** production algorithm work is warranted. Design
   version-aware unnormalized centroid sums/deltas and covering-radius
   maintenance, coalescing compatible updates per leaf. Preserve prior/current
   external-vector revision handling, cosine/L2/IP semantics, and routing
   changes after split/merge. A normalized cosine centroid is insufficient to
   recover the old sum. Guard member scans/recomputations as well as results.

## Implemented follow-up

Correctness fixtures now opt into `ANTFLY_TEST_WORKSPACE` through shared
path helpers, covering graph, HBC, the selected DB/archive tests, and the
three-server history. Without the variable, fixtures remain disk-backed.
The ordinary `TestDirectory.init` and `std.testing.tmpDir` defaults remain
unchanged. The HBC explicit no-sync durability test explicitly selects disk.
Production sync calls and durability settings are unchanged.

CI requests a 512 MiB tmpfs for fixture data only, with unconditional cleanup.
If an ARC pod denies the mount, CI reports a warning and runs the complete
gate on disk. Runner mount capability therefore determines whether this
acceleration is available. Workflows evaluated from trusted main will only
receive this setup after the workflow change lands. Compiler caches remain
on disk, scale selection is unchanged, and no tests are excluded.

Archive setup now batches all 4,100 seed writes while retaining every epoch,
cache-eviction assertion, duplicate rejection, export, and both imports.
The apply-state fixture still executes all 1,025 flushes. Clean coalescing
still exercises both backends, typed/null/JSON values, zero primary reads,
and reopening.

Relational selection now separates all 280 semantic comparisons onto a
32-row fixture from eight physical-plan comparisons on the original
768-row wide fixture. The smaller fixture retains the high keys used by
filters and dirty overlays. Multi-block, plan reuse, late-materialization,
and dense sequential-read assertions remain in the large case.

The vector production change maintains one bounded, transaction-local f64
unnormalized sum for consecutive external-vector relocations from a source
leaf. Reuse requires matching leaf identity, mutation version and member
count. In-place updates and other intervening mutations invalidate it;
no sum survives a batch boundary. Cancellation/nonfinite sums trigger a
full recomputation. Center movement conservatively expands covering radii;
inner product keeps its existing unknown-radius behavior. Regression tests
cover two external revisions, an intervening in-place update, cosine/L2/IP,
and conservative radii. Existing wide-vector query/reopen assertions remain.

The wide fixture’s measured centroid work fell from 2,041 recomputations /
261,694 scanned members to 924 / 119,530 (54% fewer scanned members).
Its regression now requires delta reuse and fewer than 160,000 scanned
members, rather than relying on a tight wall-clock deadline.

The three-server history retains all stages and recording/replay. Opt-in
work profiling reports per-stage elapsed time and cumulative scheduler
transitions, node rounds, and filesystem-interface calls. Recording and
replay each executed 15,105 transitions and rounds `{1204, 1206, 1205}`.
The wrapped runtime filesystem interface counted 22 syncs and 9 positional
writes in each; these are **not total process I/O counts**, because adapters
can call the OS directly. Diagnostic host time never feeds scheduling.

### Post-change local Linux observations

Native ARM64 Linux Debug, disk working directory, with only opted-in fixtures
redirected through `ANTFLY_TEST_WORKSPACE=/ram`. Other host builds were active;
these observations are not controlled CI speedup forecasts.

| Fixture | Seconds |
| --- | ---: |
| Portable archive, all epochs and both imports | 0.68 |
| Applied-state, all 1,025 flushed overwrites | 1.89 |
| Relational physical-plan case | 2.80 |
| Relational full semantic matrix | 0.80 |
| Clean coalescing | 4.56 |
| Reopened PageRank | 4.08 |
| HBC insert/delete churn | 0.71 |
| Wide-vector update/reopen | 14.21 |
| Three-server recording plus replay (separate executable) | 5.67 |

The focused Linux executable passed all 22 selected/import tests, including
explicit disk durability and the centroid invalidation/radius regression.
The same 22-test Linux selection also passed under an exact 512 MiB tmpfs
limit. The native focused run passed 76 tests. Vector-library tests passed
211 tests with 3 skips, and the VOPR determinism audit passed. Full storage
validation additionally exposed three pre-existing undeclared error logs in
intentional HBC/native-key and posting-publication fault tests. Those tests
now declare exactly one expected error each; the focused strict-runner check
passed with three expected and zero unexpected error logs. The complete
`unit-storage-test` rerun succeeded (20/20 build steps): 4,574 passed, 35
skipped, no failed tests or leaks, and zero unexpected error logs.

Remaining work is to observe the actual CI filesystem choice and timings,
and use the new phase/work counts to prioritize further production changes.
Wide-vector member scans and full three-server orchestration still incur
real work; neither is claimed to be eliminated.

To use an existing fast workspace locally, create its absolute directory and
set `ANTFLY_TEST_WORKSPACE` when invoking the normal test target. The variable
does not mount storage itself. Leave it unset to verify the disk fallback.
