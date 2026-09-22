# Storage unit-test work investigation

Local measurements on native macOS ARM64, Zig 0.16.0, from the
`codex/unit-test-timings` worktree based on `70098c101d`. These are test-body
measurements, including fixture construction and deferred cleanup. They are
not Linux CI measurements or production throughput results.

## Results

| Correctness test | Earlier Debug seconds | After Debug seconds |
| --- | ---: | ---: |
| Flat traversal, 128 vectors | 253.274 | 0.221 |
| Lite input restored through normal backup APIs, three documents | 171.518 | 59.111 |
| Lite → normal → Lite portable backup round trip | 172.037 | 61.200 |
| Bitmap promotion planning statistics | 163.653 | 0.421 |
| Compaction phase handoff, three modes | 66.897 | 1.808 |

The new separate normal-fanout insertion regression takes about 2.4 seconds.
The optimized smoke measurements were 47.809 → 0.053 seconds for traversal,
41.575 → 14.274 seconds for Lite input restore, and 40.791 → 14.595 seconds for
the portable round trip. Repeatability measurements are recorded separately
below. Debug and ReleaseSafe both use Zig's tracing testing allocator.

The original large bitmap, compaction, and binary insertion fixtures still
run in `release-scale-test`, which CI already invokes after its unit phase.
Moving those fixtures shortens the unit phase; it does not remove their cost
from the complete CI job. The Lite change reduces real work in both phases.
This investigation does not claim that the entire hour-long CI job is fixed.

## Serial repeatability measurement

After the scale run and compiler processes finished, three alternating
fresh-process ReleaseSafe trials of the Lite input-restore test measured:

| Trial | Before seconds | After seconds |
| --- | ---: | ---: |
| 1 | 38.689 | 13.867 |
| 2 | 38.269 | 13.744 |
| 3 | 39.745 | 14.327 |

Median: **38.689 → 13.867 seconds**, a **2.79×** improvement
in this local test-process comparison. Phase logging was disabled in both
versions. The before executable was the complete engine-test artifact; the
after executable was the focused contract artifact. Consequently this is an
end-to-end test-latency comparison, not an isolated production microbenchmark
or a claim that every part of the ratio is attributable to the one-line fast
path. The zero-allocation work contract independently verifies elimination of
the unnecessary checkpoint walk.

## Vector insertion and traversal

The ordered two-dimensional input with branching factor 2 is pathological:
128 insertions cause 40 leaf splits, 780 internal splits, and 2,588 node saves.
A native stack sample caught 26 nested internal-split calls, followed by 24
nested metadata-range computations. Missing ranges fall back to descendant
walks, so the deep tree amplifies both rebuilding and metadata work.

The traversal assertion does not require constructing that tree by insertion.
It now bulk-builds the same 128 vectors and retains the original assertions:
a two-leaf initial wave, zero certified bound stops, and exploration beyond
the initial wave. Ordered insertion is covered separately, with split, save,
and metadata-range visit budgets. At normal fanout 16 the same input takes
40 leaf splits, 5 internal splits, 263 saves, and 1,013 range-node visits.
The binary fixture retains its original size and has split/save budgets in
the scale suite.

The binary-fanout tree algorithm remains a production performance concern;
this change isolates and measures it rather than claiming to repair it.
A subsequent algorithm change should prevent unary-node cascades and/or
cache proven-empty metadata ranges without confusing them with missing
legacy metadata. It needs compatibility, recall, and persistence coverage.

## Lite restore

`NativeFile.pageAllocatorFromFreeMap` called
`validateFreePagesSafeForCheckpointSlots` before every write. Even when there
were no free pages, validation traversed both valid checkpoint graphs and
built a protected-page set. Small catalog and WAL-control writes repeatedly
paid for the existing history, including allocation and stack-trace overhead.

The production fix returns immediately for an empty free-page list. There
is nothing to reclaim in that case. Nonempty lists still undergo the same
checkpoint-protection proof; free-map decoding and other integrity validation
remain in place. A regression provides a failing allocator and requires zero
allocations for empty-list validation. Existing nonempty/corruption/recovery
coverage remains active.

Optional phase diagnostics now separate initialization, the source batch,
maintenance draining, source close, export, import, index rebuilds, primary
sync, and index sync. A representative optimized restore after the fix:

| Phase | Seconds |
| --- | ---: |
| Source initialization/index setup | 0.219 |
| Three-document source batch | 4.660 |
| Source maintenance drain | 0.541 |
| Source close | 0.171 |
| Export/staging | 4.090 |
| Import | 0.387 |
| Dense rebuild | 2.636 |
| Sparse rebuild | 0.087 |
| Graph rebuild | 0.376 |
| Restored maintenance drain | 0.205 |
| Primary sync | 0.000011 |
| Index sync | 0.628 |

Available text/dense manifest counters stay at two publications from dense
rebuild through final sync. One additional maintenance probe must report no work and leave those
manifest counts and bytes unchanged. The existing
aggregate counters omit graph/sparse indexes, so these counters do not prove
that all index types avoid redundant publication. WAL reset, catalog history,
export, and dense rebuild remain useful follow-up profiling targets. We did
not suppress flushes or change durability semantics to make the tests pass.

## Follow-up: why Lite still took a minute

A second Debug profile after the empty-free-map fix found graph WAL checkpoint
operations repeatedly reading small control files through
`getCatalogRecordRangeFromRootAtCheckpointAlloc`. Each lookup follows catalog
history and allocates page copies. Samples repeatedly entered allocation/free
stack capture beneath these reads and writes.

Using a DebugAllocator with zero captured stack frames, while retaining
safety and leak checking, the two unchanged round-trip workloads took
**1.887 and 2.004 seconds**. A control run of the same newly compiled binary
with normal tracing took **58.085 seconds** for the first case. Both modes
passed without leaks. `ANTFLY_TEST_ALLOCATOR_NO_STACKS=1` is an
opt-in diagnostic for these fixtures; it is not enabled in normal CI.

Thus the minute is heavily amplified by test instrumentation and should not
be interpreted as normal application startup latency. The fixtures are also
Debug builds with no-sync Lite files, not production durability benchmarks.
Real work remains: repeated linear catalog lookups and page copies, plus WAL
control-file rewrites. A checkpoint-aware, bounded catalog lookup structure
and avoiding provably unchanged WAL-control publication are appropriate next
production investigations; snapshot isolation, tombstones, reopen, and
ambiguous-write recovery must remain correct.

## Catalog point-lookup fix

Follow-up production change: full, range, and size reads now probe the existing
bounded page-link cache, comparing keys under its mutex without copying them.
Unrelated historical pages need no payload allocation; size reads also use
cached value length and tombstone state. Cold entries decode and populate the
same cache. Existing page-reuse/vacuum invalidation and integrity-check bypass
remain in force, and checkpoint page bounds are checked before cache access.
Cache accounting includes the enlarged metadata entries.

With ordinary allocator stack traces enabled, the two Debug fixtures measured
**7.824 and 8.738 seconds**, versus the preceding roughly 58–61 seconds.
All 81 focused tests passed without leaks. The added deterministic regression
requires zero allocations for warm size lookups, missing keys, and tombstones
across 32 historical writes, and checks pinned checkpoints, updates, range
reads, cold-cache fallback, and disabled caching.

This removes history-proportional page-copy allocations. Chain traversal is
still linear. Append and rename lookups now use the same allocation-free
history probes.

Small, single-record index writes additionally compare current contents and
skip checkpoint publication when bytes are unchanged; deleting an absent key
also skips publication. Both paths still call the configured file sync.
Spilled values and batch writes keep their original publication path. An
uncertain checkpoint-publication flag prevents using an old in-memory header
as a no-op proof after a failed publication. Read-only checks and mutation
validation precede the optimization.

The WAL reset function still performs all its operations; no retention cache
is trusted to skip them. A new integration contract requires repeated empty
WAL resets to leave the complete native checkpoint unchanged, and verifies
that append followed by reset still clears retained data. Native regressions
cover equal-length changed bytes, delete/reinsert, cold cache, read-only
reopen, and the uncertain-publication fallback.

Final follow-up validation: **93 tests passed, no leaks**, including the native
index-storage suite and the WAL reset contract. With normal stack tracing the
round trips took **6.430 and 7.393 seconds**. The same Debug executable with
`ANTFLY_TEST_ALLOCATOR_NO_STACKS=1` took **0.200 and 0.272 seconds**, retaining
allocator safety and leak checks. Both use the original three-document
fixtures; the flag remains diagnostic-only. Maintenance is time-budgeted, so
scheduling and manifest counts can differ between allocator configurations;
these are end-to-end observations, not isolated CPU ratios or durable-storage
benchmarks. The remaining several seconds are largely allocator trace cost.

## Correctness boundaries

Bitmap promotion now accepts an explicit immutable internal policy. The fast
DB test resolves 15 and 16 documents using a threshold of 16 and checks both
representation and planning-statistic changes. Normal callers still default
to 4,096, and the full-size DB case remains in the scale suite. No global test
threshold or public configuration change was introduced.

Compaction uses an existing explicit planning budget with a smaller directory:
768 entries, a 513-input closure, a 32-input immediate planning budget, and a
128 KiB admission cap. Discovery retains 79,264 bytes and validation requires
77,384 bytes: they cannot overlap. After discovery scratch is released,
retained state is 22,344 bytes and validation fits. The unrelated-edit,
input-replacement, and partial-cleanup modes retain their original ownership,
epoch, cancellation, and peak-memory checks. The original 30,000-entry fixture
uses the production planning budget in the scale suite.

## Running and validation

See [TEST_TIMINGS.md](TEST_TIMINGS.md) for commands, repeated measurements,
and the focused `storage-work-contract-test` target. The ordinary storage gate
still owns all fast cases. Focused storage selection now excludes the fixed
unit-lane ownership filters, instead of accidentally excluding the requested
test names and succeeding with zero tests. Curated work-contract and scale
suites support narrowing their inventories with runtime filters.

Validated locally:

- 80 focused work-contract, DocSet, and native Lite tests: passed, no leaks.
- 131 additional Lite tests: passed, no leaks; two expected injected error logs.
- Latest insertion range-visit contract and compaction memory contract: passed.
- Three original-size scale cases: passed, no leaks.
- Final single-probe idle checks and empty-free-map guard: three tests passed.
- Final scale inventory narrowing: exactly the three retained cases selected.
- Two standalone vector metadata-range tests: passed.
- Seven timing-runner/parser tests: passed.
- Zig formatting and whitespace checks: passed.

The complete unit suite was not rerun in this change. The earlier local VOPR
crashes remain outside this fix. Raw timings, samples, and phase logs are kept
locally under `.benchmark-results/unit-test-timings/work-contracts/`.

## Default allocator policy

The two Lite round-trip integration fixtures now default to the no-stack
DebugAllocator, retaining safety checks and failing on leaks. Other tests
retain their existing allocator policy. The earlier `ANTFLY_TEST_ALLOCATOR_NO_STACKS`
diagnostic flag has been replaced by `ANTFLY_TEST_ALLOCATOR_TRACES=1`, which
restores full tracing for failure diagnosis. Historical timings above describe
the configurations used when those measurements were collected.
