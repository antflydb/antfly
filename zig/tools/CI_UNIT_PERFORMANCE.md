# Unit gate performance investigation

Measured locally on macOS ARM64, Zig 0.16.0, Debug, Metal/CUDA disabled.
These are per-test/process observations, not a prediction of Linux CI wall time.
The broad baseline used commit `964c396146`; follow-up measurements use this
change. Concurrent compiler activity introduces noise.

## Finetuning: share compilation, retain every entrypoint

The registry contains 66 command entrypoints. Previously the finetuning gate
compiled each as a separate executable; the measured compile steps summed to
1,077 seconds, excluding the actual test executables and auxiliary tools.
Summed compile durations are not elapsed gate time.

The root `inference-finetune-test` and standalone `test-finetune` gate now use
34 compile/link groups. Groups must have identical named imports, asset owner,
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

Validation passed for all 34 command groups and the finetuning gate, 99 storage
work-contract tests, 20 sparse tests, 38 foundation tests, the retained graph and
relational scale cases, and 10 selected graph release-blocker tests. The PDF and
focused extractor build reported 486/486 tests passed; the Python checks passed
five tests. The small graph fixture also passed with allocation traces enabled
(17.1 s). The standalone inference build exposes the new command-check target.

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
