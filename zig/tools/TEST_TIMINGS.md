# Per-test execution profiling

Set `ANTFLY_TEST_TIMINGS=1` to make the Antfly custom test runner emit a
`TIMING` record for every completed test, including skipped or failed tests.
This does not change test selection or suppress failures. A crashed or hung
test will have its normal leading name but no completed timing record.

For the storage portion of the unit gate, run from `zig/`:

```sh
ANTFLY_TEST_TIMINGS=1 python3 tools/run_bounded_zig_build.py \
  --max-rss-cap 23622320128 -- build unit-storage-test \
  -Dmetal=false -Dcuda=false -j4 --summary all > /tmp/storage-timings.log 2>&1
python3 tools/summarize_test_timings.py /tmp/storage-timings.log \
  --top 40 --json /tmp/storage-timings.json
```

The tab-separated record contains setup, body, I/O teardown, allocator
teardown (integer nanoseconds), and the full test name. A separate monotonic
clock remains valid while the per-test I/O is destroyed. Body time includes
test-local deferred cleanup, such as closing a database; the teardown columns
measure only the runner's `testing.io_instance` and `testing.allocator_instance`.
Printing the timing record itself is outside the measured intervals.

Zig may buffer entire suites' output. Use the recorded durations rather than
CI log timestamps. The summarizer accepts CI timestamps and partition labels
before records. Its totals sum execution durations across processes, so they
are not aggregate wall-clock time. Passing overlapping logs also counts
repeated executions; keep independent attempts separate when comparing runs.

Native macOS and Linux CI timings are not interchangeable. Record commit,
target, optimization mode, concurrency, exclusions, and failures with results.
These timings do not measure compiler RSS or establish safe memory reservations.

## Storage work contracts and scale coverage

`storage-work-contract-test` compiles a focused suite independently of the
three large unit artifacts. It includes native Lite integrity/recovery tests,
DocSet representation tests, the two portable restore round trips, traversal,
monotone insertion, bitmap promotion, and compaction phase handoff.

```sh
ANTFLY_TEST_TIMINGS=1 ANTFLY_TEST_WORK_PROFILE=1 \
  python3 tools/run_bounded_zig_build.py --max-rss-cap 12884901888 -- build \
  storage-work-contract-test -Dmetal=false -Dcuda=false --summary all
```

`ANTFLY_TEST_WORK_PROFILE=1` adds phase timings and cumulative text/dense-index manifest
and WAL-reset counters for Lite restore, split/save/range-visit counters for
insertion, and retained/discovery/validation memory for compaction. Source
close and export are separate intervals; import and index rebuild phases are
reported separately. These diagnostics are opt-in and do not set pass/fail
latency thresholds.

The ordinary correctness fixtures use:

- A balanced 128-vector tree for traversal, preserving the two-leaf initial
  wave and the requirement to continue without a valid pruning proof.
- Ordered insertion at normal fanout, with explicit split, save, and metadata
  range-visit budgets. Binary fanout is separately exercised at full scale.
- An explicit internal bitmap policy with threshold 16, asserting ordinary
  ordinals immediately below it and bitmap promotion at it. Production callers
  still use 4,096, and existing representation tests cover that default.
- 768 compaction directory entries, including a 513-input closure, an explicit
  32-input planning budget, and 128 KiB admission capacity. All three modes
  retain the overlap denial, scratch retirement, epoch validation, cancellation,
  and peak-memory assertions from the original fixture.
- Empty free-map validation with an allocator that fails its first allocation.
  It must do no reachability work. Nonempty maps retain previous-checkpoint
  protection and corruption checks.
- An idle maintenance probe that must report no work and publish
  no additional text/dense-index manifests. The existing aggregate counter
  does not include graph or sparse indexes; the zero-pass assertion covers
  the whole LSM maintenance step.

`release-scale-test` retains the original 4,096-document bitmap case, the
30,000-entry compaction case in all three modes, and the 128-vector binary
insertion case. The existing CI release-scale step runs these. Both focused
and scale targets accept filters that narrow their curated inventories:

```sh
python3 tools/run_bounded_zig_build.py --max-rss-cap 12884901888 -- build \
  release-scale-test -Dmetal=false -Dcuda=false -- --test-filter 'production scale'
```

## Repeated latency measurements

Compile before measuring. Use the emitted `storage-work-contract-tests`
binary directly (its path appears in `zig build --verbose`) to keep compile
and build-cache time out of measurements. Compare the same optimization mode,
allocator, hardware, filesystem, and synchronization configuration. Run trials
serially after other compiler/test workloads have stopped. For example:

```sh
# Set this to the executable emitted by the desired build.
work_test_binary=/absolute/cache/path/storage-work-contract-tests
work_filter='lite restore staging accepts aflite input for normal restore'
"$work_test_binary" --test-filter "$work_filter"  # warmup
for trial in 1 2 3 4 5; do
  ANTFLY_TEST_TIMINGS=1 "$work_test_binary" --test-filter "$work_filter" \
    > "/tmp/restore-trial-$trial.log" 2>&1 || break
done
python3 tools/summarize_test_timings.py /tmp/restore-trial-*.log \
  --json /tmp/restore-trials.json
```

Retain individual samples and compare their median and range. Work-count
assertions belong in the correctness gate; latency changes need repeated
measurements, not tight wall-clock assertions. These end-to-end test timings
include fixture construction and, outside the two Lite round trips, the
testing allocator's stack tracing even in ReleaseSafe, so they are not application throughput benchmarks. Use the
existing `dense-stack-bench`, `lsm-write-bench`, and `backend-bench` executables
with fixed workload parameters when qualifying production throughput.

The local investigation, measured changes, and remaining bottlenecks are in
[STORAGE_WORK_CONTRACTS.md](STORAGE_WORK_CONTRACTS.md).

## Separating allocation stack capture from storage work

Selected expensive Lite, graph, relational, PDF, and extractor fixtures default
to a local DebugAllocator with zero captured allocation stack frames, retaining
safety and leak checks. Set `ANTFLY_TEST_ALLOCATOR_TRACES=1` to use
`std.testing.allocator` and capture allocation/free traces when diagnosing a
failure. The PDF and extractor fixtures still inject every allocation failure;
only successful allocation/free backtraces are disabled. Other tests retain
their existing allocator settings.

See [CI_UNIT_PERFORMANCE.md](CI_UNIT_PERFORMANCE.md) for suite ownership,
finetuning build reuse, graph/relational boundaries, and measured results.

```sh
ANTFLY_TEST_TIMINGS=1 ANTFLY_TEST_ALLOCATOR_TRACES=1 \
  python3 tools/run_bounded_zig_build.py --max-rss-cap 12884901888 -- build \
  storage-work-contract-test -Dmetal=false -Dcuda=false -- \
  --test-filter 'lite restore staging accepts aflite' \
  --test-filter 'lite portable backup roundtrips'
```

Compare the same emitted binary with the flag set to `0` and `1`. Keep
optimization mode and fixtures fixed. Maintenance can also react to elapsed
time, so this is a comparison of instrumentation configurations rather than
an exact accounting of time spent only in stack capture. A leak in the default
mode still fails the test; rerun with tracing for allocation-site diagnostics.

## Ownership audit

`make unit-test` also runs `unit-test-inventory`. It queries the actual selected
inventories of all four unit gates, checks that aggregate exclusions preserve
the original named-test union, and fails on repeated names. Inspect
`zig-out/unit-test-inventory.json` for each test's owner. Anonymous `.test_0`
reachability probes are preserved but excluded from name-based comparisons.
The audit queries metadata/listing modes; it does not execute test bodies.

The wide-vector update/reopen, relational selection/reference scan, and reopened
PageRank scheduler fixtures also emit cumulative phase durations with
`ANTFLY_TEST_WORK_PROFILE=1`. The overwrite fixture emits flush, compaction, and
manifest work counts. See [CI_STORAGE_FOLLOWUP.md](CI_STORAGE_FOLLOWUP.md) for
interpretation and the post-merge suite reassessment.

### Backtraces and storage group measurements

Graph-runtime, HBC, vector-payload and algebraic-index fixtures use a shared
leak-checking DebugAllocator without allocation/free stack traces by default.
Set `ANTFLY_TEST_ALLOCATOR_TRACES=1` to restore traces when diagnosing ownership
failures. This setting does not disable leak detection or failure injection.

For a compiled storage-support binary, compare the same filters and repeat on
an otherwise idle machine. Preserve unit exclusions when using a diagnostic
binary compiled with broader filters (`--skip-test-filter 'production scale'`).
`ANTFLY_TEST_WORK_PROFILE=1` reports payload failure-sweep attempt counts and
HBC routing/centroid work in addition to the phase timings. Tight wall-clock
limits are intentionally absent from correctness assertions. See
[the storage follow-up](CI_STORAGE_FOLLOWUP.md) for measurements and work contracts.

### Transport allocation-failure campaigns

The four native TS/AVC allocation campaigns use a private `SafeAllocator`
without successful-allocation stack traces. Exhaustive allocation and resize
failure injection, leak checks, mismatched/double-free detection, and
write-after-free checking remain enabled. `FailingAllocator` still records the
injected failure's stack. `ANTFLY_TEST_ALLOCATOR_TRACES=1` restores successful
allocation traces through the ordinary test allocator for diagnosis.

A native aarch64 Darwin Debug comparison on main `730999bf2d` sampled the HTTP
reader campaign in repeated SafeAllocator/DWARF stack unwinding. Running the
same four test bodies from cached baseline and fixed binaries, through Zig
0.17.0's test protocol, measured:

| Campaign | Baseline | Without redundant traces |
| --- | ---: | ---: |
| AVC adapter | 0.197 s | 0.037 s |
| HLS factory | 1.962 s | 0.118 s |
| Incremental normalization | 85.396 s | 1.237 s |
| HTTP reader | 75.005 s | 1.396 s |

Both runs reported zero test errors or leaks. Fixtures and failure positions
are unchanged. These measurements exclude compilation and include concurrent
local builds; they demonstrate the removed diagnostic overhead rather than
establishing a CI latency budget or a production parser throughput claim.

### HTTP timeout policy without long sleeps

The personal-connections polling regression previously slept for 16 seconds
to exceed the ordinary HTTP request timeout. It now observes the deadline
through the existing transport-attempt admission hook: the default polling
budget is 60 seconds, and an explicit caller budget overrides that policy.
Before/after timestamps bracket the observed deadline without imposing a
tight elapsed-time assertion. Short real-network cases separately verify a
successful response beyond a configured 1 ms ordinary-request budget and a
50 ms explicit timeout. The native Debug test measured 16.066 s before and
0.182 s after; it still checks the admitted production deadline rather than
inferring it from a long sleep.

### Large native auth artifacts

The native Darwin Debug auth artifact on `730999bf2d` produced 152 MiB of
machine code, including direct ARM64 branch relocations beyond the 128 MiB
branch reach, and failed Zig's final Mach-O link with `Overflow`. The auth
suite now uses the existing `linked_tests` object/final-image split and native
Darwin linker, as the public API suite already does. Its discovery root,
filters, ownership checks, debug information, and measured memory reservation
remain intact. Treat this as a build/link failure when reporting suite times;
test-body timing cannot diagnose an artifact that never starts execution.

### Large workspace CLI fixtures

The grep black-box suites use `antfly-grep-cli-test`, which calls the same CLI
entry point with Debug safety checks and an explicitly checked SafeAllocator.
Successful allocation traces are disabled unless
`ANTFLY_TEST_ALLOCATOR_TRACES=1`; leak, mismatch, double-free and
write-after-free checks remain enabled. The shipped CLI keeps its ordinary
allocator. `grep-cli-test-build` installs the driver for focused reproduction.

On the same Mac, the unchanged 24,000-file admission/backoff/shrink fixture
failed the ordinary Debug CLI's 15-second startup deadline. A diagnostic run
with a longer observation window took 147.9 seconds, including a content-only
refresh of 81.2 seconds. Stack samples showed SafeAllocator/DWARF unwinding
dominating capture. The checked driver passed the original deadlines in
23.1 seconds total, including creation and removal of the fixture files.

The resolver-curation integration test checks `applied=true` with its original
10-second wait deadline. Expiry preserves the accepted `applied=false` response,
so the payload proves completion. An additional five-second wall-clock check
measured runner contention rather than a separate correctness property.

Grouped-source lease scheduling has a deterministic regression with simulated
31-second namespace rounds against 30-second leases. A successful claim must
be consumed in its bounded turn; waiting for the next namespace visit can
reclaim forever. The test also checks live foreign ownership and reacquisition
before finishing an existing publication, without sleeping.

The C API query/writer concurrency fixture also fences setup with the existing
maintenance-capture hook. `run_until_idle` drains debt but does not stop later
checkpoint publication: a refresh can otherwise replace the snapshot containing
an injected provider before query inference starts. The fixture resumes
maintenance after both queries borrow their provider, then exercises real
concurrent writes and configuration changes. Both Linux regressions were
reproduced before this barrier and pass with the same compiler backend after it.

### Explicit local-model qualification

The C API unit gate runs model-free contracts regardless of which models are
installed. Actual embedding, reranking, generation/streaming, and Lite embedding
drain tests belong to `capi-local-model-test`. Both targets share one compiled
artifact and its existing compilation memory reservation; they have disjoint runtime filters.
The model target requires all selected tests to run without skips, so an absent
model fails qualification rather than silently passing it. It never pulls models.

Run from `zig/`, using the shared model cache or an explicit models directory:

```sh
ANTFLY_TEST_TIMINGS=1 ANTFLY_TEST_FAIL_ON_ERROR_LOGS=1 \
  zig build capi-local-model-test -Dinference-optimize=ReleaseFast \
  -Dmetal=false -Dcuda=false --summary all
```

The original full native Debug run spent 109.7 seconds in its real-model
generation case; samples showed decoder work. That is an integration workload,
not a stalled unit test. Keep this qualification alongside ordinary unit results
when changing inference or the C API.

### Maintenance phases and snapshot lifetime

Detached C API fixtures advance bounded maintenance turns until their target
phase is observed, rather than assuming one turn always reaches it. Existing
foreground concurrency and publication-fencing assertions remain at that phase;
no production scheduler is drained or bypassed. A bounded turn count reports
failure if the phase is never reached, and background driver errors propagate to
the test instead of being discarded. Provider reference checks run while a
runtime-backed snapshot is captured; post-turn checks allow that runtime to have
been released already. The failed-open cleanup test injects malformed durable
catalog bytes below the logical writer, which now correctly rejects them before
opening a new handle.

Other retained native Debug outliers in this run were 67.1 seconds for the HA
hostname cleanup regression (2,000 actual resolution/connection/response cycles),
52.3 seconds for the PostgreSQL native mutation campaign (many independent
source/constraint cases), and 45.5 seconds for a staged-restore/reopen regression.
These durations alone do not justify shrinking the fixtures or raising their
deadlines. The hostname sample showed real connection work and its caller
waiting for I/O completion. Compiler samples must be recorded separately: one
12-minute compilation was in LLVM DWARF finalization. None of these observations
establishes a production memory-reservation change.

The 2,200-document full-result hybrid aggregation regression sampled repeated
SafeAllocator/DWARF stack capture during fixture ingestion. Giving that test a
checked SafeAllocator with opt-in traces reduced execution from 117.2 seconds
to 1.73 seconds. The dataset, two vector windows, text matches, ranked page,
aggregation expectations, leak checks, and write-after-free checks are unchanged.
This measures diagnostic overhead in a Debug fixture, not production query
throughput. A repeated fixed execution measured 1.71 seconds before a rerun
with localhost access passed the aggregation and four HTTP contracts together.

### Video checkpoint failure injection

A protocol-timed native Debug run of all 320 video cases took 268.5 seconds.
The dynamic-checkpoint construction/replacement allocation campaign took
48.7 seconds; a separate invocation under heavier contention took 82.2 seconds.
A sample attributed about 79% of active stacks to successful-allocation trace
capture. The shared checked campaign helper keeps every failure position,
checkpoint/geometry fixture, decoded-pixel oracle and memory check, with traces
opt-in. Its fixed suite execution measured 12.6 seconds. The remaining time
includes real decoder work; the test is not reduced to a mock.

The retained 10-bit H.264 parity case took 36.1 seconds, original Sintel reference
hashes took 20.4 seconds, and shared/copying progressive output took 18.2 seconds.
These exercise actual reference outputs in Debug and remain in the suite.
The displayed-RGB embedding fixture now uses the admitted 48×48 geometry,
checks nine patches/one soft token, and retains both pixel formats and rotations.
It does not relax production geometry validation.

The rebuilt full video artifact passes 318 cases with two platform skips and
zero error logs or leaks, including all six checked allocation campaigns. The
protocol-measured sum is 233.9 seconds. A coordinator campaign also passes with
`ANTFLY_TEST_ALLOCATOR_TRACES=1`; these comparisons exclude compilation and
include concurrent local workloads.

### Expired graph rows and exact cursor work

The public borrowed-wire cursor regression retains 8,200 expired rows followed
by one live row, an initial scan allowance of 8,199, and the exact total of
8,201 rows without replay. Its native Debug execution measured 126.6 seconds;
97% of sampled setup stacks were SafeAllocator trace capture during graph batch
construction. A local checked allocator with opt-in traces reduced it to
1.39 seconds with the same dataset, cursor boundaries, output, and memory
checks. No production graph scan limit or scheduler policy changed.

### SQL fault campaigns and large operator fixtures

SQL's shared test allocator retains leak, mismatch, double-free and
write-after-free checks. Fault campaigns still use the platform's stable
no-resize/no-remap enumeration. `ANTFLY_TEST_ALLOCATOR_TRACES=1` restores
successful-allocation traces. Four large operator fixtures use the same checked
allocator without changing row counts, spill quotas, compression/parallel modes,
reference outputs, capture counts or early-close checks.

Samples of sorted-cursor fault injection and high-cardinality set execution
attributed about 80–82% of active stacks to SafeAllocator trace capture.
Native Debug measurements, excluding compilation, were:

| Regression | Before | Checked allocation |
| --- | ---: | ---: |
| Shared spill fault campaign | 56.2–80.1 s | 2.8–3.1 s |
| Sorted delivery fault campaign | 41.7–69.6 s | 5.8–10.6 s |
| Compact delivery fault campaign | 51.1 s | 3.6–3.8 s |
| High-cardinality set promotion | 57.3–67.7 s | 6.5 s |
| Parallel partition reducers | 50.2 s | 2.1 s |
| Partial join spill | 49.7 s | 3.1 s |
| Shared-file sort cohorts | 44.8 s | 1.2 s |

Concurrent local work accounts for substantial run-to-run variation; these are
not production throughput claims or pass/fail latency thresholds. The complete
SQL target passes 1,272 distinct tests with three skips and no failures or leaks
across its two ownership partitions (22/22 build steps). Trace opt-in also passes.
