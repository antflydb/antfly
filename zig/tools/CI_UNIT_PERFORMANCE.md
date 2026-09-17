# Unit gate performance investigation

Measured locally on macOS ARM64, Zig 0.16.0, Debug, Metal/CUDA disabled.
These are per-test/process observations, not a prediction of Linux CI wall time.
The broad baseline used commit `964c396146`; follow-up measurements use this
change. Concurrent compiler activity introduces noise.

## Finetuning: share compilation, retain every entrypoint

At the profiling base, the registry contained 66 command entrypoints. Previously the finetuning gate
compiled each as a separate executable; the measured compile steps summed to
1,077 seconds, excluding the actual test executables and auxiliary tools.
Summed compile durations are not elapsed gate time.

The root `inference-finetune-test` and standalone `test-finetune` gate now use
35 compile/link groups for 68 entrypoints after merging the two new GLiNER2.5
commands from main (the measured change initially used 34 groups for 66). Groups must have identical named imports, asset owner,
native linkage, libc setting, and release metadata. The generated dispatcher
selects a CLI at runtime, keeping every registered `main` reachable for semantic
analysis, code generation, and linking. The checks are not installed or executed.
Individual CLI targets retain their original modules and run behavior.

Five entrypoints require isolated checks because they import another CLI through
relative paths; Zig rejects those files appearing in two modules. Their registry
entries set `shared_check = false`. A future conflicting entrypoint fails the
build instead of silently disappearing from coverage. New registry entries are
automatically checked. The ordinary finetuning test executables remain in the gate.

`python3 -m unittest tools/test_finetune_command_checks.py` checks that runtime
dispatch rejects semantic and linker failures in an unexecuted second entrypoint.
`zig build inference-finetune-command-check -Dmetal=false -Dcuda=false` runs only
the command checks. The standalone name is `test-finetune-command-check`.

This change halves the number of command compilations. A clean Linux CI run is
still needed to establish the resulting gate wall-time reduction; local follow-up
builds reuse caches and should not be presented as cold-build speedups.

## Ownership and boundaries

| Work | Before | After |
| --- | ---: | ---: |
| Sparse root, including imported tests | 914 tests / 334 s | 20 sparse + 38 foundation tests / 1.0 s |
| Sparse graph metric vector chunks | 4,097 dictionary entries / 186 s | 257 entries / 0.6 s |
| Same full-size graph fixture, traces disabled | 186 s | 6.1 s in scale suite |
| Graph scheduler reopening every worker/coordinator step | 97 s | 5.7 s |
| Five configured graph metrics through runUntilIdle | 84 s | 3.0 s |
| Relational bound-selection semantics | 88 s | about 13 s; same 280 comparisons |
| Relational repeated bound-scan benchmark | 64 s in unit gate | retained in scale suite; 1.3 s one-pass work contract in unit gate |
| PDF inherited-font exhaustive allocation failures | 23.5 s | 0.29 s |
| Entire PDF executable | 463 tests / 42 s | same 463 tests / 19 s |
| Extractor resolution exhaustive allocation failures | 9.7 s | about 1.1 s |

Sparse no longer reruns imported LSM integration tests. The explicit
`storage-foundation-test` target retains the otherwise-uncovered common workers,
backend runtime, simulator primitives, and LMDB lifecycle tests in
`antfly-unit-test`. Comparing the old inventory with the new inventory plus
existing owners left only `compaction phase handoff production scale` outside
the unit union; `release-scale-test` owns that case. Tests made reachable only by
removed imported tests now have explicit imports in the sparse root.

The small graph fixture still crosses the real 256-entry vector chunk boundary,
has empty producer partitions and two active partitions, reopens mid-build,
checks PageRank/eigenvector/HITS/degree scores, and verifies a warm rebuild.
`graph metric sparse vector chunks production scale` retains the original
4,097-entry dictionary. No production partition or codec thresholds changed.

The relational semantics fixture retains 768 rows and all combinations of two
backends, clean/dirty state, seven projections, five filters, and two limits.
Its physical block layout is necessary for the asserted late-materialization
path. The one-pass bound-scan work contract uses 512 rows, retains sparse and
dense scenarios, and asserts plan counts, primary reads, result counts, and
allocation balance. The original 768-row, eight-round benchmark remains in
`release-scale-test` with median timings and counters.

## Allocation safety and production work

Selected expensive fixtures use DebugAllocator with allocation/free backtraces
disabled. Safety checks and leak detection remain enabled; the exhaustive PDF
and extractor tests still call `checkAllAllocationFailures` on the same tiny
fixtures. `ANTFLY_TEST_ALLOCATOR_TRACES=1` restores detailed backtraces.
This is a diagnostic cost reduction, not evidence that production is 30–80x faster.

Graph control-key construction now allocates its exact final size once, encodes
components directly, and formats the job identifier into a stack buffer.
`graph metric control namespace keys allocate exactly once` rejects a second
allocation or any resize and verifies byte compatibility for escaped names,
maximum job identifiers, and topology task namespaces. This removes real
allocation work without changing stored keys; it is not the explanation for
the large tracing-related fixture speedups.

## Validation and remaining work

Before the latest main merge, validation passed for all 34 command groups and the finetuning gate, 99 storage
work-contract tests, 20 sparse tests, 38 foundation tests, the retained graph and
relational scale cases, and 10 selected graph release-blocker tests. The PDF and
focused extractor build reported 486/486 tests passed; the Python checks passed
five tests. The small graph fixture also passed with allocation traces enabled
(17.1 s). The standalone inference build exposes the new command-check target. CI on the
merged revision also covers main's two new GLiNER2.5 entrypoints.

Run the changed gate and bounded-work regressions:

```sh
ANTFLY_TEST_TIMINGS=1 python3 tools/run_bounded_zig_build.py \
  --max-rss-cap 23622320128 -- build inference-finetune-test \
  storage-work-contract-test sparse-test storage-foundation-test \
  -Dmetal=false -Dcuda=false -j2 --summary all

ANTFLY_TEST_TIMINGS=1 python3 tools/run_bounded_zig_build.py \
  --max-rss-cap 12884901888 -- build release-scale-test \
  -Dmetal=false -Dcuda=false -j1 -- \
  --test-filter 'graph metric sparse vector chunks production scale' \
  --test-filter 'relational columnar bound scan benchmark'
```

The broad baseline separately exercised library, inference, finetuning, and
Antfly unit targets. Listener tests blocked by the sandbox were rerun with
native socket access. Five pre-existing borrowed-VoprIo crashes were excluded
from profiling only; this change does not add CI exclusions for them. Consequently
this investigation does not claim a clean, unmodified end-to-end CI run.

Remaining measured candidates include the wide external-vector reopen test
(70 s), relational deferred-discovery scheduler (56 s), physical-churn benchmark
(39 s), and canceled compaction staging (38 s). Serverless graph routing still
uses 131,073 scores to cross real routing/footer boundaries. Preserve those
boundaries when separating small correctness fixtures from full-size cases.
Measure with allocator traces disabled before identifying production bottlenecks.

## Follow-up after the GLiNER2.5 main merge

The September 16 local reassessment uses `ec74554b7f` (macOS ARM64, Debug,
Metal/CUDA disabled). The SDK CI failure was the Python formatter rejecting
`test_finetune_command_checks.py`; the exact repository formatting check and
all five Python regression tests pass after formatting. The restarted SDK job
also passed.

The expanded inference executable now selects 4,294 tests: 3,907 passed and
387 skipped. Three runs took 478, 481, and 480 seconds. The older 75-second
measurement predates the new training tests and is not the current baseline.
Compilation peaked at 7.2 GiB, above its old 7 GiB estimate; the build now
reserves 9 GiB for this artifact. The inference gate passed with that estimate.

### Training test diagnostics: initial experiments

Controlled experiments replaced the allocator in the following fixtures with
`DebugAllocator(.{ .stack_trace_frames = 0, .resize_stack_traces = false })`,
asserting leak-free deinitialization. Dimensions, iterations, ownership checks,
failure-injection loops, cancellation checks, and resume assertions stayed the
same. These experimental source edits were reverted after measuring; they are
historical measurements. The follow-up below now applies and validates these changes.

| Test | Current full-suite profile | Traces disabled, focused run |
| --- | ---: | ---: |
| Native trainer regional recomputation / cancellation / partial resume | 70.37 s | 0.844 s |
| Native trainer full and heads / cancellation / durable resume | 55.60 s | 0.568 s |
| Native trainer replay attention / partial resume identity | 54.62 s | 0.518 s |
| Recomputed training runtime allocation failures / retry | 51.98 s | 0.430 s |
| Recomputed training compilation at every allocation failure | 22.97 s | 0.254 s |
| Recomputed head admission compilation allocation failures | 2.25 s | 0.036 s |

The focused executables all passed. The first five cases account for about
256 seconds in the full inference run and 2.6 seconds in the experiments.
That suggests roughly four minutes of avoidable diagnostic overhead; it is
not a measured new whole-suite time or a production throughput improvement.
Sampling the runtime-failure test confirmed repeated graph compilation and
allocator stack unwinding on its hot path.

The follow-up applies the existing opt-in allocation-backtrace convention to these
fixtures, preserving their exhaustive failure coverage, and also measures the
remaining training ownership and manifest tests: regional/staged-head ownership failure
coverage took 36.64 seconds, inactive-adapter accumulation/resume 29.35 seconds,
and GLiNER boundary manifest loading/listing 24.80 seconds. Do not remove
allocation failures or shrink the integration scenarios before measuring the
same diagnostics change.

### Other completed measurements

All 35 finetuning command groups and the finetuning gate passed. Their compile
steps summed to 647 seconds in this cache-reusing run; this is not a cold-build
comparison against the earlier 1,077-second baseline.

The library gate passed. The full PDF executable took 18.99 seconds (463 tests),
HTTP 11.38 seconds (586 tests), and image decode 9.87 seconds (269 tests).
Serverless took 195.35 seconds, including exact workflow replay at 49.47 seconds
and graph-routing fixtures at 28.99 and 17.29 seconds. Vector payload tests took
74.33 seconds; publication allocation failures account for 13.13 seconds and
mark-workspace admission for 10.57 seconds. These remain separate candidates
for allocator-diagnostics comparisons and boundary-preserving fixture work.

The graph release-blocker gate passed in 227.17 seconds, versus 417.62 seconds
in the earlier local baseline. Its vector-chunk boundary case now takes about
1 second. The remaining HITS paired-worker fixture takes 23.73 seconds; a sample
caught setup adding edges individually and committing/flushing LSM state.
Measure tracing separately before changing the setup to batches, and retain
the multi-page/paired-worker boundary.

Storage support passed in 866.23 seconds (2,173 passed, 3 skipped), versus
1,090.90 seconds in the older baseline. Storage engine passed in 301.62 seconds
(1,065 passed, 23 skipped). The TTL and transaction-recovery borrowed-VoprIo
tests pass without the temporary profiling exclusions used before main's
fiber-unwinding fix.

Two further allocator experiments, also reverted after passing, clarify the
next storage work:

| Fixture, unchanged workload | Current profile | Traces disabled |
| --- | ---: | ---: |
| Relational deferred-discovery scheduler, 8,192 rows | 55.70 s | 1.408 s |
| Wide external-vector updates and reopen, 4,096 vectors x 1,536 dimensions | 68.28 s | 20.820 s |

The scheduler's diagnostic change is now applied. The vector follow-up below
profiles its remaining work and fixes a production relocation inefficiency.
The original wide-vector workload remains in the unit gate: the narrow-vector
variant does not establish the same cache and native-generation coverage.

### Completed broad validation and CI status

The fresh local four-gate run completed with **422/422 build steps succeeded**.
DB-core's two partitions passed in 873.05 seconds (1,322 passed, 8 skipped),
versus 1,045.96 seconds in the earlier profile. All five previously excluded
borrowed-VoprIo cases now pass; this run added no test exclusions. The timing
log contains 11,094 Antfly test executions, with 3,238.30 seconds of summed
per-test time (not parallel gate wall time). Inference's separate per-test
profile covers all 4,294 selected tests.

The correct PR #774 follow-up CI run is
https://github.com/antflydb/antfly/actions/runs/35168448026. Its unit step passed
in **35 minutes 16 seconds**; SDKs, both architecture jobs, and base E2E passed.
The overall run did not pass: required VOPR qualification was canceled before
receiving a runner, and the optional >1M-chunk scale job exhausted its 60-minute
job timeout while still building E2E binaries. These are distinct from unit-test
assertion failures. The reusable VOPR workflow keys concurrency by `github.ref`,
which is `refs/heads/main` for these approved-PR dispatches, so unrelated PRs
share its pending queue. PR #691 entered that queue at the same time #774's
pending qualification was canceled.

The workflow-only timing setting did not reach PR CI because reusable workflow
definitions are loaded from main. The checked-out `make unit-test` command now
defaults `ANTFLY_TEST_TIMINGS` to 1 in CI, while preserving an explicit override
and leaving local timing output opt-in. Its environment selection was checked
for local, CI, explicit-on, and explicit-off invocations. The current PR's CI
log therefore cannot yet provide the new per-test records; do not substitute
the unrelated PR #773 run's 52-minute unit measurement for this PR's result.

## Unique ownership across the default unit gate

The compiled inventory audit now covers `lib-test`, `antfly-unit-test`,
`inference-test`, and `inference-finetune-test`, including linked consumer
executables, standard Zig protocol runners, inference runtime filters, and
both DB-core partitions. With CPU backends on macOS Debug, the before/after
comparison is **17,613 named executions → 16,357**, preserving exactly the same
**16,357 distinct named tests**: **1,256 repeated executions removed**, zero
missing or additional names. Namespace exclusions leave anonymous `.test_0`
reachability probes in the remaining artifacts. They are excluded from
name-based ownership claims because their names can refer to different files.

The aggregate no longer schedules the focused Lite, portable-backup,
vector-payload, Raft transition/read-gate, or HTTP client artifacts whose
coverage already has an owner. Four import-only finetuning roots are owned by
`inference-test` in the combined gate; standalone package finetuning still
includes them. This removes another 55 executions, including repeated data
and pipeline imports within those roots. Backend and consumer/provider
coverage remains: consumer and implementation test namespaces are distinct,
and the finetuning wrappers use the same selected backend configuration.

The remaining overlaps use explicit aggregate-only exclusions in
`pkg/antfly/build/unit_test_ownership_rules.zig`. Focused targets retain their
original selections. Run nodes share compiler artifacts and preserve their
environment, output checks, memory reservations, and ordering dependencies.
The audit compares original and reduced runtime selections and rejects lost,
added, or multiply-owned named tests. It runs alongside tests, so it adds no
compile-all barrier before test execution. DB inventory output is emitted as
complete records instead of interleaved progress chunks.

`make unit-test` includes the audit, even while approved PR workflows still
load their definitions from main. To inspect ownership without executing test
bodies, run:

```sh
zig build unit-test-inventory -Dmetal=false -Dcuda=false
```

The report is `zig-out/unit-test-inventory.json`. New overlap is a build failure;
`-Dunit-test-inventory-allow-overlap=true` is available for diagnosis. The full
before/after comparison also included the removed focused artifacts, rather
than assuming that identical executable names imply identical coverage.

## Ranked remaining work

1. **Continue evidence-based diagnostics comparisons.** The training and six
   storage fixtures in the follow-up below now keep allocator backtraces opt-in.
   Leak checks, exhaustive failure injection, cancellation, durable resume, and
   fixture sizes remain intact. Apply the same comparison to other expensive
   ownership tests before changing their coverage.
2. **Keep aggregate ownership enforced.** The compiled audit above removes
   1,256 repeated named executions across all four gates and rejects new
   overlap. The earlier Antfly timing sample estimated about 211 seconds of
   duplicate process work; that is not a promised CI wall-time saving. Focused
   targets remain available, with correctness and configuration coverage
   preserved in the aggregate.
3. **Preserve correctness boundaries; classify by purpose, not runtime.**
   Pagination, canceled compaction, sequential selection, and graph restore
   remain in the unit gate with their original fixtures. Their former 30–42 s
   durations were primarily allocator diagnostics (see follow-up). Physical
   churn also checks pinned-reader correctness and payload ownership, despite
   its benchmark name. Do not relocate it solely because it reports timings.
   The wide-vector follow-up below preserves all 4,096 x 1,536 values and
   identifies repeated source-leaf quantization during relocation.
4. **Optimize demonstrated production work.** Profile the remaining workload
   after tracing is off. For graph setup, distinguish individual edge commits
   from metric calculation. For vectors, measure update/quantization/search
   and reopen separately. For relational maintenance, measure scanned rows,
   passes, flushes, and manifest publications. Turn confirmed improvements
   into work-count contracts rather than tight elapsed-time assertions.
5. **Measure build and scheduling separately.** Command grouping is already
   implemented. The local grouped compile durations reuse caches and cannot
   establish cold CI speedups. Use the next CI per-test records alongside
   compile-step durations and memory observations to identify the critical
   path. Audit the blanket 6 GiB run reservations before increasing parallelism;
   do not lower reservations without peak-memory evidence or treat sums of
   overlapping durations as elapsed time.

The largest local executables are DB-core (873 s), storage support (866 s),
inference (480 s), storage engine (302 s), graph release-blocker (227 s), and
serverless (195 s). These are individual process/partition-step observations,
not additive wall time. The diagnostic and duplicate-work estimates also need
to be recomputed after each change so overlapping savings are not counted twice.

## Follow-up: retain regression boundaries and make diagnostics opt-in

Local macOS ARM64, Zig 0.16.0, Debug, Metal/CUDA disabled. The same compiled
executables were run with `ANTFLY_TEST_ALLOCATOR_TRACES=1` and `=0`.
Backtraces are now opt-in in these fixtures; DebugAllocator safety and leak
checks remain enabled. Exhaustive allocation-failure loops, original data
sizes, cancellation checks, and persisted-state assertions are unchanged.
No test was removed, renamed, or moved to a scale suite.

### Training

These are individual process durations including startup and cleanup, not
parallel CI wall time. The inactive-adapter row first isolates diagnostics;
its additional initializer improvement is described below.

| Fixture | Backtraces on | Backtraces off |
| --- | ---: | ---: |
| Regional recomputation / cancellation / resume | 70.31 s | 0.87 s |
| Full and heads / cancellation / resume | 55.10 s | 0.59 s |
| Replay attention / cancellation / resume | 54.33 s | 0.55 s |
| Runtime allocation failures / retry | 51.53 s | 0.41 s |
| Compilation allocation failures | 22.67 s | 0.26 s |
| Head admission allocation failures | 2.13 s | 0.04 s |
| Regional / staged-head ownership failures | 36.03 s | 0.31 s |
| Inactive adapter accumulation / resume | 29.41 s | 13.97 s |
| Manifest loading / listing failures | 24.23 s | 1.80 s |

The nine unchanged workloads sum to 345.75 s with traces and 18.80 s without.
Sampling the remaining inactive-adapter time found repeated parameter-name
classification inside the per-weight initializer. Hoisting those invariant
checks preserves every generated value and reduces that fixture from 13.97 s
to 9.34 s without backtraces (25.24 s with them). The remaining Debug sample
is dominated by native `primDotGeneralOp` / `rank2TensorRead`, not stack
unwinding. Its classifier keeps the published 384-wide dimensions; they were
not reduced merely to improve test time.
The unchanged final fixture passes in ReleaseFast in 0.61 s, compared with
9.34 s in Debug. The Debug matrix-multiplication hotspot alone therefore does
not establish a production bottleneck. The ReleaseFast build finished 14/14
steps; the bounded runner recovered an underestimated 64 MiB build-helper
run reservation (observed 102.78 MB). No production kernel was changed.

### Storage

Durations are from the runner's per-test TIMING records, including cleanup.

| Fixture | Backtraces on | Backtraces off | Preserved boundary |
| --- | ---: | ---: | --- |
| Physical churn | 38.98 s | 4.37 s | Both GC settings; 16 update rounds; pinned reader and payload ownership |
| Sequential selection | 33.08 s | 4.33 s | 768 wide rows; both backends; dirty owners, bounds and all limits |
| Canceled compaction | 35.73 s | 1.89 s | 800 original + 800 inserted rows; two-block quanta; race, cancellation and reopen |
| Deferred-discovery scheduler | 55.92 s | 1.47 s | 8,192 rows / 32 blocks; bounded passes and unchanged idle commit count |
| Graph artifact restore | 30.39 s | 0.81 s | 2,050 edges cross the 2,048-item page; rollback and parse-once assertions |
| Dense pagination | 41.52 s | 0.74 s | 2,200 vectors; offset 1,024; exact candidate and hit-count assertions |

The six cases sum to 235.62 s with backtraces and 13.61 s without. This is
predominantly diagnostic overhead, not evidence that production operations
normally take 30–55 seconds. Selection still reconstructs and compares wide
JSON documents; churn still compresses, checksums, and persists real LSM data.
Samples identify those as remaining work but do not establish redundant work
or production regressions. Benchmark optimized builds before changing those
paths. The existing work-count and correctness contracts remain in the unit
gate; a benchmark name or elapsed duration alone does not make a scale test.

Validation: the focused inference build passed 14/14 steps (24 selected test
bodies passed, four Metal cases skipped, plus 23 build-helper tests); storage
passed 20/20 steps and all six selected bodies. All nine changed training
fixtures and all six storage fixtures also passed with backtraces enabled.
The initializer change passed both diagnostic modes with the original
cancellation, accumulation, and durable-resume assertions.

## Wide-vector relocation and paired HITS follow-up

Both regressions retain their original input sizes and assertions. Allocation
backtraces are opt-in with `ANTFLY_TEST_ALLOCATOR_TRACES=1`; allocator safety
and leak detection remain enabled. Temporary phase instrumentation and CPU
samples separate fixture setup from production work.

### HITS: fixture setup, not a metric-kernel change

The historical traced fixture took 23.73 s. With backtraces disabled and the
original individual writes, it takes 1.94 s: two graph setups total 1.085 s,
the local metric takes 0.038 s, and paired workers take 0.741 s. Using one
public `batchApply` per graph seeds the same 131 edges and 132 nodes in
0.015 s total; the instrumented whole test takes 0.724 s. Both fan-outs still
cross the 64-unit test page boundary, the self-edge remains, both workers
must complete pages, and local/planned authority and hub scores and cleanup
are still compared. Added count assertions guard the batched fixture shape.

### Wide vectors: a production relocation inefficiency

The 4,096-vector, 1,536-dimensional fixture inserts 16 batches, updates eight
batches, and verifies 64 queries across warm cache, cleared cache, checkpoint,
state overlays, and reopen. Before the production change, with allocation
backtraces off, Debug phase totals were:

| Phase | Seconds |
| --- | ---: |
| Generate fixture vectors | 0.236 |
| Initial insertions | 1.041 |
| Existing-vector updates | 15.251 |
| Batch persistence | 1.961 |
| Queries after each batch | 0.278 |
| Each 64-query verification pass | approximately 0.61–0.63 |
| Explicit checkpoint | 0.059 |
| Reopen and activate | 0.003 |
| Entire regression | 21.981 |

Sampling caught `removeFromLeaf` rebuilding the source leaf's quantized
payload on each relocated vector. It saved with default options, dropping
the enclosing batch's deferred-rebuild policy. Relocation now carries the
batch options through source-leaf saves, sibling merges, parent updates, and
single-child collapse. Direct removal retains its existing default behavior.
The deferred-node set and finalization machinery perform the eventual rebuild.
Single and grouped append paths also preserve an explicitly requested payload
publication suppression instead of overwriting it with the leaf-split flag;
the relocation contract caught this second option loss after the source fix.

A nine-vector regression checks that a real cross-leaf update with deferred
publication writes zero quantized payloads before batch finish, registers both
touched leaves, drains the deferred set, bounds final publications by node
count, and returns the updated vector. Against the original implementation,
it fails with `expected 0, found 2` before finalization. This is a work-count
contract, not a timing threshold. The original full-size cache/checkpoint/
overlay/reopen regression remains in the unit gate.

Three alternating ReleaseFast before/after runs, with the same phase
instrumentation and allocation backtraces disabled, produced:

| Median duration | Original code | Fixed code |
| --- | ---: | ---: |
| Update phase | 2.364 s | 1.718 s |
| Entire regression | 3.057 s | 2.422 s |

That is about 27% less update time and 21% less whole-regression time in this
optimized local workload. Instrumented Debug runs measured 21.98 s before
and 16.08 s after; the first fixed update phase measured 8.83 s versus 15.25 s
before. A final clean Debug run took 21.80 s while other builds were active,
so use the alternating optimized measurements rather than those individual
Debug wall times to quantify the production improvement. These are local
workload measurements, not CI wall-time or general throughput claims. Remaining relocation
work includes centroid reconstruction; changing that needs separate routing
and previous-vector correctness analysis, not simply skipping refreshes.

The broader `antfly-storage-test` target also runs storage-owner executables.
Its `opaque storage owner performs coarse batch and query on one live DB`
failure was traced separately: the fixture installed a replacement text index
with repair advancement disabled, then expected indexed query visibility.
Direct lookup found `doc:bulk`, while queries were empty even before bulk
ingestion. The fixture now explicitly completes bounded targeted repair and
checks that the replacement index finds an existing document before proceeding.
The original bulk query assertion is retained; no production query behavior
or synchronization guarantees were changed. The complete storage-owner suite
passed all 27 tests with no leaks (32/32 build steps).

Final code without phase instrumentation passed all 23 selected vector test
executions (including import-only roots), covering deferred rebuilds, narrow
and wide native reopen, coalesced centroids, external previous-vector handling,
delete, and merge ownership cleanup. The final HITS build passed 19/19 steps;
its clean test took 0.903 s. Formatting and whitespace checks also passed.
The final HITS case and narrow/wide native reopen plus the new relocation
contract also passed with allocator backtraces enabled, with no leaks.

## Follow-up: remaining centroid reconstruction work

A fresh Debug sample and a two-second ReleaseFast sample confirmed that source
leaf removal still spends substantial time reconstructing centroids. The
optimized sample exposed raw-vector staging copies as well as the required
centroid/radius arithmetic. The point-loader fallback first loaded all raw
vectors into a second leaf-sized matrix, then transformed them into the
caller's destination matrix.

The fallback now loads and transforms one vector at a time using the existing
single-vector scratch. It retains `getVectorInto`'s cache/invalidation behavior,
all centroid and covering-radius arithmetic, and the specialized batch-loader
paths. It retains no transformed-vector cache across mutations. For 168 members
and 1,536 dimensions, the removed raw staging allocation is 1,032,192 bytes per
reconstruction; generally it is `members × dimensions × sizeof(f32)`.

Three alternating ReleaseFast runs of matching instrumented before/after
binaries, with the original 4,096-vector fixture and backtraces disabled, gave:

| Median duration | Before | After |
| --- | ---: | ---: |
| Eight update batches | 1.571 s | 1.495 s |
| Complete test process | 2.283 s | 2.176 s |

This is a modest local improvement (about 5%), with one after-run slower than
its paired before-run. The deterministic benefit is removal of the extra raw
matrix. The ordinary DB index manager already installs a transformed batch
loader, so these measurements do not establish a speedup for that path.

The new regression covers L2, cosine, and inner-product transforms, nontrivial
ID order, a vector revision after cache invalidation, and no raw batch staging
in the apply-workspace high-water mark. Against the original implementation,
it fails with `expected 0, found 24` bytes for three two-dimensional vectors.
The initial focused validation passed 25 executions with no leaks, including
narrow/wide reopen, deferred publication, coalesced centroids, external previous
vectors, deletion/merge cleanup, and the existing bulk split workspace contract.

Further reducing full centroid passes is a separate algorithm change: a
normalized cosine centroid does not retain the magnitude of its vector sum,
so subtracting the removed vector from it cannot recover the exact mean.
Maintaining authoritative unnormalized sums would need explicit handling of
external current/previous revisions, coalesced updates, split/merge transitions,
and reopen. This change preserves the existing arithmetic and routing instead.

Expanded diagnostic selection (`external` plus the focused vector filters)
ran 139 executions: 134 passed, one skipped, four failed, with no leaks. All
27 selected `storage.hbc_adapter` tests passed. The four imported API failures
were then run against the pre-change implementation and reproduced identically:

- Hosted profiled dense query after an external write-sync batch: `StoredDocMissing`.
- Cold profiled dense query after external write-sync batches: `IndexNotFound`.
- Managed startup catch-up of external dense gaps: `result.had_debt` assertion.
- Public dense projection readiness: expected progress `0.999`, got `0`.

These are separate follow-ups from the centroid change. This manually broadened
diagnostic selection does not establish the status of their owning CI targets;
the latest full gate is not claimed green.


### API regression fixture follow-up

The four baseline failures exposed outdated fixtures, and repairing the startup
fixture also exposed a production ordering bug. Neither came from centroid
streaming. Embedding-only writes update artifacts and do not create
primary documents; the profiled-query fixtures now insert a document field too.
The cold-reader fixture provisions its metadata-defined empty index through a
writer before opening a query-only DB.

The 50,000-document fixture retains all 200 write-sync batches, 384 dimensions,
and the cached cold-reader/first-query boundary. Its former two-second polling
window observed only 12,250 indexed documents in one diagnostic run. The local
harness now drains the writer and publishes its status before asserting public
readiness, instead of depending on elapsed time and an absent status publisher.
Allocator backtraces are opt-in with the existing environment switch; leak
checking remains enabled.

The startup repair fixture uses the current catalog format and damages the
selected native generation after DB shutdown, preserving the original status
watermark. A read-only reopen asserts zero indexed members and three primary
documents before repair. The public readiness fixture now supplies publication
target count/readiness, retaining its assertion that pending native projection
keeps progress below completion.

With a real persisted membership gap, startup attempted native vector projection
publication before artifact repair. Publication correctly rejected the mismatch
between three exact vectors and zero HBC members with
`VectorBlockPublishedGenerationNotReady`. Startup now restores membership before
publishing that projection. The updated fixture reproduced this failure on the
original ordering and preserves the no-replay-debt boundary explicitly.

The complete 50,000-document regression passed in 291 seconds in local Debug
with allocator traces disabled and no leaks (other compilation was active, so
this is diagnostic timing, not a controlled benchmark). This remains an indexing
performance investigation, not evidence that a two-second completion deadline
is valid. Its original input size remains unchanged.

All four original failures now pass individually with no leaks. The hosted query
measured about 0.85 seconds, readiness below a millisecond, and startup repair
about 1.3 seconds. The updated repair regression fails on the old production
ordering and passes after reordering, so it guards the dependency directly.

Supplemental startup validation exposed a separate obsolete-file fixture
self-deadlock: it called `persistManifest` while already holding the backend
mutex. It now calls `persistManifestLocked`, waits only until the captured
retention deadline, and verifies persisted reclamation through a read-only
reopen. Retired startup owners do not guarantee optional live storage metrics
in their cached status.

Validation completed for nine distinct regressions: the original four, legacy
artifact repair, native publication contention, terminal restore debt,
counterless incomplete-generation repair, and obsolete-file reclamation. All
passed with no leaks; the fixed reclamation fixture took about 0.37 seconds.
Formatting and whitespace checks pass. This focused validation does not claim
the entire latest CI gate is green.
