# Storage unit follow-up after PR #774

Baseline: `b46f42c780` (PR #774 merge), native macOS ARM64, Zig 0.16.0,
Debug, metal/CUDA disabled. Compile time is excluded from the focused
measurements. These are local correctness-test timings, not Linux CI forecasts
or application throughput benchmarks.

## Six expensive workloads

| Fixture | Baseline local seconds | Follow-up local seconds | Change / finding |
| --- | ---: | ---: | --- |
| Portable archive history | 17.45 | 2.64 | Allocation traces opt-in; all 4,100 epochs, cache eviction, duplicate rejection, public export, and both import modes retained |
| Flushed apply-state overwrites | 25.22 | 1.67 | Allocation traces opt-in; all 1,025 flushes and durable reopen retained |
| Relational clean coalescing | 31.31 | 2.54 | Allocation traces opt-in; both backends, typed cells, zero primary reads during clean merge, and reopen retained |
| Relational selection / projection | 12.10 | 12.54 | No claimed speedup; retain the physical layout and all 280 reference comparisons |
| Wide-vector update / reopen | 13.67 | 13.76 | No claimed speedup; insertion dominates, not reopening |
| Reopened PageRank scheduler | 5.35 | 4.22 | Production reaper shutdown wakes its idle wait instead of awaiting a 10ms polling sleep |

Leak checking remains enabled. `ANTFLY_TEST_ALLOCATOR_TRACES=1` restores
allocation/free backtraces in the adjusted fixtures, including the shared
`storage/test_allocator.zig` policy used by graph, HBC, vector-payload, and
algebraic-index tests. Samples of
flushed overwrites and clean coalescing showed stack-unwinding overhead before
the change. No fixture sizes were reduced and no tests moved to the scale gate.

The overwrite test additionally asserts exactly 1,025 flushes and at most two
manifest publications per flush. The observed run had 118 compactions and
1,124 manifest writes; compaction/publication timing can vary, so the test does
not require those exact latter counts.

### Production shutdown change

`ThreadedDurableJobLane` previously set a shutdown boolean and joined a reaper
that could still be sleeping for its 10ms polling interval. It now uses a
persistent `Io.Event`: shutdown signals it, and the idle loop waits with the
same timeout. Signaling before or during enrollment is safe; the event is
never reset. Maintenance probe cadence, backlog draining, owner completion,
and payload destruction remain unchanged. No jobs are canceled to speed close.

The graph fixture still runs 90 worker/coordinator steps and reopens both DB
handles each step. Its close phase fell from 2.20s to 1.01s; opening remained
about 2.09s, worker work 0.53s, and coordinator work 0.51s. This is a general
runtime shutdown fix, not a graph-algorithm change.

Tests cover a modeled idle cycle with exactly one event wait and zero
unconditional sleeps, shutdown before enrollment, a real threaded enrollment
race, and the existing nested-owner/drain/payload lifecycle cases.

### Remaining vector and relational work

`ANTFLY_TEST_WORK_PROFILE=1` now prints cumulative phase timings for these
fixtures. It does not enforce wall-clock deadlines.

The 4,096-vector, 1,536-dimensional update/reopen fixture measured insertion
8.08s, persistence 1.96s, all query comparisons 3.39s, checkpoint plus byte
comparison 0.061s, close 0.010s, and reopen/activation 0.001s. Insertion includes
input preparation; query phases include correctness checks. A sample during
updates found full source-leaf centroid and covering-radius recomputation.
Further improvement needs an algorithmic work contract for repeated updates
of one leaf, including previous/current external revisions and split/merge
routing. Subtracting from a normalized cosine centroid cannot reconstruct its
unnormalized sum. Do not weaken the fixture or change routing arithmetic solely
to make this test faster.

The relational fixture measured setup 1.07s, columnar scans 2.06s, reference
primary scans 9.17s, and dense validation 0.22s. An experimental 512-row fixture
failed its late-materialization assertion (expected eight rows, observed zero)
because the physical layout selected sequential reads. The original 768 rows
are retained. A future split into small semantic comparisons and explicit
physical-plan regressions must preserve all projections, dirty overlays,
backend coverage, hashes, and the late-versus-sequential decision.

## Suite scheduling reassessment

The complete partition logs from CI run
[35277188581](https://github.com/antflydb/antfly/actions/runs/35277188581)
contained 141 category-lane executions totaling 231.60s and 1,182 complement
executions totaling 886.89s. These include anonymous import declarations;
named-test ownership is audited separately.

Moving `relational columnar` and `source vector` to the existing category lane
assigns the same recorded executions as follows:

| Lane | Previous recorded seconds | Reassigned recorded seconds | Executions after reassignment |
| --- | ---: | ---: | ---: |
| Category | 231.60 | 575.68 | 221 |
| Complement | 886.89 | 542.80 | 1,102 |

This is a replay of recorded durations, not a measured parallel speedup. It
adds no compiler process or execution lane. The complement uses the same
filters as exclusions, preserving disjoint selection. The allocator and
shutdown improvements are not included in this forecast.

Storage-support's largest recorded groups were graph metric runtime (288.7s),
HBC adapter (192.5s), archive validation (67.4s), and derived apply state (56.6s).
The latter two are addressed here; repeated DB closure also benefits from the
runtime fix. Keep storage-support's concurrency unchanged until the updated CI
run establishes its remaining critical path. Storage-engine runs after support
under the existing runtime memory policy; all three artifacts can compile
independently. Summed test durations are not aggregate CI elapsed time.

### Compile scheduling

The local bounded `-j2` run exposed a second scheduling problem: support and
engine compiled, then support ran while DB-core compilation had not started.
A build-runner backtrace showed recursive `Group.async` eager dispatch inside
`makeStep`/`stepReady`, with another worker idle. The default storage gate now
waits for its existing three-artifact compile step before starting support or
the DB-core partitions. Engine retains its dependency on support. This removes
the long test run from the compiler dispatch path, without new artifacts,
additional execution lanes, or a different memory budget. Focused non-default
storage selections retain their existing behavior.

## Validation

The final complete `unit-storage-test` gate succeeded (20/20 build steps):
4,571 passed, 35 skipped, zero failed tests or leaks. Both the final inventory
and the execution log contain 4,606 unique tests and zero duplicate executions.
The prior unique inventory is preserved, with one new HBC routing regression.
The standalone gate's six imported enrichment/spool duplicates are now excluded
from DB-core and remain owned by storage-support. Scale fixtures were excluded.

The standalone vector-index library also passed 198 tests with three skips.
Four focused regressions passed again with allocation traces enabled using the
final compiled artifact (payload fault sweep, HLL contention, HBC relocation,
and quantized-payload ownership). Both previously failing relational fixtures
passed under the concurrent full gate. Formatting and whitespace checks passed.

Final local DB-core lanes summed to 449.37s (221 executions) and 357.48s (1,108).
The build reported support at 2m, engine at 5m, and partitioned DB-core at 7m;
these rounded run-step durations exclude compilation. No new Linux CI run has
been measured for this branch. Existing diagnostic error-log counts are noted
below; strict unexpected-error-log mode was not enabled.

Earlier focused validation for the six-workload part of this branch:

The focused Debug run passed 71 executions (the six target tests, 54
background-runtime tests, and imported anonymous declarations), with no leaks.
Both new reaper regressions also passed from the compiled storage-support
artifact. The partition runner and ownership auditor Python checks passed eight
tests. Replaying the recorded DB-core names through the new filters retained
all 1,323 named tests exactly once.

Before the four-group follow-up below, the local storage-support artifact
completed 2,183 executions: 2,179
passed, four skipped, no failed tests or leaks. Summed test duration was
908.19s. It logged the same ten errors (seven declared expected, three
undeclared) as the successful baseline CI artifact; strict error-log mode was
not enabled. This standalone storage target has a broader inventory than its
ownership-filtered clone in the aggregate unit gate.

Its leading local groups were graph metric runtime (97 tests, 462.13s), HBC
(291, 170.41s), vector payload storage (63, 84.10s), algebraic index (86, 72.95s),
and DB-split VOPR (two, 43.68s). Archive validation was 4.41s across 34 tests;
derived apply state was 2.47s across 15. Preserve the exhaustive failure and
restart coverage when addressing the remaining groups. These native Debug
measurements ran alongside other local work and must not be used as Linux CI
speedup claims.

## Four remaining storage groups

The final storage-support run passed 2,180 tests, skipped four, and reported
zero failures/leaks (2,184 executions, including one new routing regression).
Its summed test durations were 165.60s, compared with 908.19s before these four
group changes. Both measurements are native Debug runs under concurrent local
work; the change is not a controlled Linux CI latency forecast.

| Group | Earlier local seconds | Final local seconds | Final executions |
| --- | ---: | ---: | ---: |
| Graph metric runtime | 462.13 | 27.94 | 97 |
| HBC adapter | 170.41 | 22.31 | 292 |
| Vector payload storage | 84.10 | 5.89 | 63 |
| Algebraic index | 72.95 | 0.94 | 86 |


The follow-up applies the same opt-in allocation-backtrace policy to graph
runtime, HBC, vector-payload, and algebraic-index fixture allocators. Every
fixture retains a DebugAllocator and checks its deinitialization for leaks;
`ANTFLY_TEST_ALLOCATOR_TRACES=1` selects the original testing allocator.
Tests that inject allocation failures still use FailingAllocator and verify
all failure outcomes. Allocation backtraces are diagnostic overhead, not a
production graph/index bottleneck.

A same-binary comparison of the reopened HITS worker pool and HITS oracle
fixtures measured 36.10s with traces versus 1.68s without. All 97 graph-runtime
tests passed in 22.35s without reducing inputs, numerical iterations, restart
boundaries, or publication assertions. Separate payload/algebraic runs passed
150 executions in 5.76s (63 payload tests: 5.26s; 87 algebraic declarations:
0.51s). The complete storage-support measurement above is the final comparison;
these focused runs are diagnostic, not additive CI savings.

### Payload failure sweep

The publication regression formerly rebuilt its database 128 times in each of
two modes. It now advances the failure index until a complete operation has
`has_induced_failure == false`. A handled allocation failure does not terminate
the sweep. Every attempt still checks the old snapshot and resolves the current
payload through two independent reopens. The two modes needed 60 and 61 attempts,
removing 135 redundant attempts while retaining all preceding failure points.

### Deterministic HLL concurrency

The previous 200-document/40-batch fixture only inserted, despite a comment
claiming it deleted old documents. Inserts did not dirty the HLL sketch, so
its 21.65s runtime did not establish the intended background rebuild race.
The replacement inserts three documents and deletes one before scheduling a
real threaded maintenance job. Per-index test-only events park that job with
the write lock held; a foreground insert observes contention at its actual
lock boundary. After release/join/drain, the test asserts exactly one maintenance
pass, no dirty marker or extra maintenance work, and cardinality three (the two
survivors plus the foreground insert). It uses no scheduling sleeps. With the
original allocator the corrected fixture measured 0.43s, before the additional
backtrace reduction.

### HBC setup and production routing

The retained-scratch and block-pre-admission fixtures now bulk-build the same
192 and 128 vectors. Leaf sizes, branching, centroid block sizes and memory
limits remain unchanged. Assertions still require directory blocks larger than
a leaf, pre-allocation rejection, bounded peak bytes, and scratch reclamation.
Incremental insertion/split regressions remain separate, including the existing
production-scale binary-fanout fixture outside the unit gate.

The batch update path also routed relocating vectors twice without an intervening
mutation and repeated the transform. It now passes the current item's resolved
source/target postings and transformed vector to the mutation helper. This state
is never reused for a later item; centroid repair, merges, splits and persistence
retain their existing order. A regression compares batched and serial external
updates under cosine, L2 and inner-product metrics, checks membership/centroids/
radii, and requires one routing traversal per mutation. The existing native
update/reopen tests also enforce a per-batch routing budget.

The wide-vector fixture remains around 14s: its diagnostic run recorded 2,048
update routes, 2,041 centroid rebuilds and 261,694 member visits. Reopen itself
remained about 3ms. This change does not claim to solve that numerical bottleneck.
A further incremental-centroid design must retain authoritative unnormalized
sums and version membership, including missing previous external revisions,
splits and merges; subtracting from normalized cosine centroids is incorrect.
Deferring all repairs would also change the tree used to route later mutations.

### Relational fixture stability

Two single-pass semantic tests failed during the concurrent full-gate run:
payload-free existence/null projection and adaptive admission across restart and
clock skew. Their assertions depended on completing a build inside the real
50ms maintenance quantum. The existing test-only deadline override now isolates
these two semantic fixtures from CPU scheduling. Fake admission time, row/block
limits, payload-read assertions and adaptive policy checks remain enabled.
Separate bounded/resumable maintenance tests retain their existing coverage.

## Remaining work

1. Measure the new Linux CI critical path; local summed test time is not gate
   elapsed time. The storage scheduler already reuses the three compiled
   artifacts and retains disjoint DB-core lanes.
2. Design and benchmark version-aware incremental centroid/radius maintenance
   for wide-vector relocation. The current work counters provide a baseline.
3. Split relational reference-query semantic coverage from physical-plan
   fixtures only with explicit late-materialization/sequential-plan assertions;
   the earlier 512-row reduction selected a different plan and was reverted.

## Reproduction

From `zig/`, run the storage unit gate with timing artifacts:

```sh
ANTFLY_TEST_TIMINGS=1 ANTFLY_TEST_LOG_DIR=/tmp/storage-followup-suites \
  python3 tools/run_bounded_zig_build.py --max-rss-cap 25769803776 -- \
  build unit-storage-test -Doptimize=Debug -Dmetal=false -Dcuda=false -j2 --summary all
```

Set `ANTFLY_TEST_WORK_PROFILE=1` when running an already compiled test binary
to collect phase diagnostics. Compare the same filters, optimizer, allocator,
hardware, filesystem, and concurrency. Keep partition logs separate from the
aggregate log to avoid double-counting their records.
