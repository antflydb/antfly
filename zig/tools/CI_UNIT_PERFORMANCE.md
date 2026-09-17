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

### Next batch: training test diagnostics

Controlled experiments replaced the allocator in the following fixtures with
`DebugAllocator(.{ .stack_trace_frames = 0, .resize_stack_traces = false })`,
asserting leak-free deinitialization. Dimensions, iterations, ownership checks,
failure-injection loops, cancellation checks, and resume assertions stayed the
same. These experimental source edits were reverted after measuring; they are
recommendations for the next change, not optimizations already shipped here.

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

Start by applying the existing opt-in allocation-backtrace convention to these
fixtures, preserving their exhaustive failure coverage. Then profile remaining
training ownership and manifest tests: regional/staged-head ownership failure
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

The scheduler belongs in the diagnostic-overhead batch; its boundary need not
be reduced to recover most of the time. The vector case still has substantial
work after diagnostics are removed. Keep that representative workload in a
scale/performance suite, preserve a smaller regression that crosses the actual
tree/cache/update boundaries, and profile the remaining work before promising
a production optimization. The existing narrow-vector variant does not by
itself prove the wide-vector regression is covered.

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

## Ranked remaining work

1. **Allocator diagnostics, preserving test coverage.** The five largest
   measured training cases plus the relational scheduler sum to 311 seconds
   with normal test backtraces and about 4 seconds in the no-backtrace
   experiments. Apply the existing opt-in trace convention first; keep leak
   checks, exhaustive failure injection, cancellation, and durable resume.
   This is about five minutes of local test work, not a promised CI wall-time
   reduction. Continue the same comparison for the remaining training and
   storage failure-injection tests before changing their fixtures.
2. **Assign one aggregate owner per test.** The Antfly timing records contain
   11,094 executions of 9,893 distinct names: 1,201 repeated executions.
   Keeping the longest observation of each name leaves about 211 seconds of
   repeated measured work. That is an audit estimate, not permission to delete
   cases: inspect runner arguments/configurations and preserve the union of
   selected tests when assigning ownership. Examples include portable backup
   history (standalone and storage support), vector-payload publication and
   admission (standalone and storage support), Lite namespace deltas
   (standalone and storage engine), and graph reverse-rebuild recovery
   (release-blocker and DB-core). Keep focused targets available without
   scheduling identical cases twice in the same aggregate gate.
3. **Separate scale from correctness boundaries.** The wide-vector case still
   takes 20.8 seconds without backtraces. Remaining large Antfly observations
   include dense-filter pagination (41.5 s), physical-churn benchmark (38.3 s),
   canceled compaction staging (35.6 s), sequential relational selection
   (32.4 s), and graph artifact restore (30.7 s). Compare diagnostics first,
   then retain representative workloads in `release-scale-test` and exercise
   the same block/page/threshold transitions with bounded unit fixtures.
   The optional CI `>1M chunks` job is a different scale suite; removing its
   PR label does not remove these retained regression cases.
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
