# Compile work reduction, 2026-11-01

This follows the [operation-owner experiments](operation-owner-probes-2026-11-01.md).
Current architecture and commands remain in [COMPILATION.md](../../../../zig/COMPILATION.md).

## Implemented changes

BatchWrite and TransactionWrite previously had identical field layouts but
separate Zig type identities. They now alias one canonical request value in
`storage/db/mutation_values.zig`. The two slice adapters return their input
without pointer reinterpretation.

The complete request coalescer now lives in `storage/db/mutation_plan.zig`.
It accepts concrete write/delete/transform slices and an explicit read snapshot,
returns owned preparation output, and has no DB, backend, scheduler, schema,
or inference dependencies. It owns the existing graph delta helpers as well.
`prepare` is noinline so receiver-specific adapters cannot duplicate its body
through inlining. Allocation ownership, transform ordering, read-set validation,
and pinned-schema lifetimes remain explicit. Compatibility helpers no longer
used by the DB/local-mutation mixins were removed.

The existing recovery adapter rejects any transform needing a durable base when
no pinned snapshot is available. Foreground callers without a supplied snapshot
capture one through the existing concrete preparation owner. This fallback is
covered by the preparation/transaction tests; normal preparation still supplies
its snapshot and validates it at commit.

The common, configuration, preload parser, provider-default, and secret suites
reuse one compile artifact while retaining their named run steps, filters,
and configuration error-log policy. Native source partitioning still produces
one server and one local artifact, rather than five of each.

## Reachable build graph

`zig build test-build-graph -Dtest-graph-step=unit-test` writes a JSON inventory
without compiling the test gate. It visits each reachable test/test-object
compiler once and records source, mode, filters, and memory reservation.

On native Apple Silicon, Zig 0.17.0, CPU-only, the unit gate went from **225 to
217 compiler jobs**, with one `common-runtime-tests` and one
`common-runtime-tests-local`. The earlier 225-job inventory contained only 18
server-root jobs (16 tests plus two test objects), not 178 recompilations of the
entire server. The 59 physical source-catalog jobs cover different test filters
and have deliberately different storage workloads; their shared root alone is
insufficient evidence for merging them.

These are graph counts, not measured compile time or peak-memory improvements.

## Verification

- All 18 standalone mutation/read-plan tests pass in ReleaseFast, including transform
  parsing, repeated transforms over an immutable snapshot, and full allocation-failure
  cleanup for graph/document effects.
- Five Debug DB regressions pass: expanded snapshot budgeting, preparation OOM
  cleanup, durable-base fencing, projected graph push/pull across restart, and
  transaction transforms observing pending writes.
- The allocation sweep disables backing-allocator remaps so its allocation-site
  sequence is stable across optimized and Debug runs. In-place remapping otherwise
  makes Zig's checkAllAllocationFailures report NondeterministicMemoryUsage.

- The control-source catalog check, 32 compilation-tool tests (Python 3.13), four
  ownership-audit tool tests, Zig formatting, and license checks pass.

- All ten common-suite selections (five suites in two source partitions) pass
  using the built Debug binaries with local socket access (209 named test
  executions, zero failures and leaks). The initial build
  completed but three tests were denied socket access by the sandbox; the
  corresponding direct rerun executes all ten selections, not just the failed
  cases. The additional secret ABI and cluster status dependencies pass in the
  original gate. A redundant rebuild was stopped after direct verification.

## Why the query split remains a design task

The current local-query provider borrows a concrete DB pointer. Its physical
source catalog also declares query implementation files. Moving its exports to
another archive alone would neither remove that source dependency nor establish
an independent opaque owner. A complete split must first separate query value
contracts and DB capabilities, retain cancellation/read-generation leases and
result ownership, and prove implementation-edit cache isolation. This change
does not claim to have completed that redesign.

## Qualification limits

No matched cold full-release build, final executable-size comparison, or
mutation-throughput comparison was completed for this extraction. Existing
production storage/inference owners and Debug CI policy already implement major
parts of the original suggestions. The index and fused CPU probes, borrowed
batch API, and CLI ReleaseSmall option remain experiments with the limits in
the preceding record. Additional production owner splits or transport changes
need qualification of their complete operations and callback/lifetime semantics;
small probe success is not sufficient evidence to enable them by default.
