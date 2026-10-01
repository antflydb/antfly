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
