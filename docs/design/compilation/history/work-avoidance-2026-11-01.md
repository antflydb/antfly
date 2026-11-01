# Compile work avoidance, 2026-11-01

This follows the [query-owner split](query-owner-redesign-2026-11-01.md) and
[mutation/test consolidation](work-reduction-2026-11-01.md). The current guide is
[COMPILATION.md](../../../../zig/COMPILATION.md). The production archive count
remains eight runtime archives plus the shared physical owners. This change
removes repeated work inside that composition and gives small edits a smaller
build graph; it does not add more production archives.

## Foreign callers no longer emit the provider's dispatcher

`runtime_callback_abi.Boundary.call` compared the supplied dispatch pointer with
its own `local_dispatch`. Taking that address forced each consumer to emit the
entire dispatcher for the engine VTable, including contracts and trampolines for
methods that consumer never called. The preceding stripped query and storage
archives each emitted a roughly 93,960-byte storage-engine dispatcher, as well
as supporting adapters. Symbol address extents on Mach-O are estimates.

`Boundary.callRemote` contains only the checked caller transport. Storage engine
clients use it, and inference engine clients use it outside source-local tests.
The provider still owns its dispatcher; source-local callbacks keep `call` and
its local Zig error semantics. Method IDs, native layout checks, output and
argument contracts, stable error transport and public C APIs are unchanged.
Both compiled Debug query/storage consumer archives contain zero instances of
the storage-engine dispatcher. The profile workflow now rejects its return in
those production consumer archives.

## Complete query regressions use opaque owners

The complete-query request and response contracts live in `local_query_abi`,
including the existing storage-owner query entry and buffer destructor.
`query_client.executeOwner` accepts an owner-issued opaque handle and concrete
request. It checks the exact failure envelope and boundary, returns a
provider-owned response, and frees it with the issuing owner. The server owner
client uses this shared implementation. Only physical DB owners construct the
remaining checked native DB borrow; the heavier local request adapters no
longer specialize by DB receiver type.

The compiled query test caller no longer imports DB, backend, index, server
composition, inference implementation or the full storage dependency profile.
Its three regressions cover:

- Retained local text generations across later writes, profiled and unprofiled.
- Deadlines, cancellation during provider entry, and a successful later query.
- Repository-backed continuation after closing and deleting the donor DB and
  its local pins, then opening a different carrier owner. Both profile modes
  return the original retained generation and preserve its transport token.

`local-query-owner-test` and `opaque-query-owner-test` share one compiled caller
and one run. The former physical caller/root is removed. The earlier release
caller reported approximately six minutes and 4 GB MaxRSS. A fresh stripped
ReleaseFast/baseline-CPU compile of the replacement reports **3 seconds and
239 MB MaxRSS**, with a sampled peak of 237,551,616 bytes. This is roughly
120x less compiler time and 17x less reported compiler RSS for that caller,
while adding two regressions. It is not a whole-suite or whole-product ratio.
The separate build-graph configurer sampled 555,630,592 bytes and is excluded
from the caller comparison. Zig's summary uses decimal MB/GB; the sampled
byte counts are retained separately. In the complete product run the same
caller reported 6 seconds and 177 MB. This does not remove all private native
DB-layout contracts between the physical query and storage implementations.

## Concrete query plans and focused tests

`storage/db/query/paging_plan.zig` owns concrete paging/window inputs and
aggregation-domain decisions. It imports no DB, schema, wire parser, inference
runtime, backend or source catalog. Production request adapters translate the
existing SearchRequest into those values. Candidate-count precedence, saturated
offset arithmetic, fusion/pruning windows, legacy-knn and hierarchy exclusions,
and exact aggregation budgets retain their existing behavior.

Complete aggregation collection uses one concrete SearchCapability and
non-inlined collection loops. Only the small context-to-search callback adapter
specializes by caller. Calls remain one per complete search component, never
per hit or record. Tests verify text-first collection, complete domains,
deduplication, budgets and cleanup when a later callback fails.

Ten aggregation-domain tests moved out of table-read/provider roots into their
own consumer root (nine existing tests and one new callback-failure regression).
Five paging-plan tests exercise boundary cases and combinations of saturated
limits, offsets, fusion windows and reranker settings. They join the normal unit
gate; manual aggregation/table-read gates reuse the same planner test artifact.
The same native CPU-only unit graph now has 219 compiler jobs, versus the prior
217: two deliberately small compiler owners were added, while repeated domain
test compilation was removed from the API selections. Job count is not compiler
work or a whole-program recompile count.

`build.focused.zig` constructs only the authored dependencies needed by these
plans and the existing concrete mutation/transform plans. It avoids configuring
the server, physical providers, source catalogs, test partitioning and release
composition. All 33 focused tests pass in Debug and ReleaseFast on macOS, and
in Debug on Linux. The normal product graph uses the same aggregation-module
constructor, preventing a separate implementation or test-only type substitute.

## Local edit measurements

The watch measurement tool changes the real default aggregation budget from
100,000 to 100,001 in a private overlay. An injected qualification test prints
the loaded value; both that value and a successful build summary must be
observed before accepting a timing. A cached rerun of the old tests cannot
qualify an edit. Overlay sources and caches are removed after the process exits.

One observation per configuration, Zig 0.17.0. Linux runs use the same pinned
compiler image, a 4 GiB container and four CPUs on this Apple Silicon host:

| Workload/build entry | Initial build | Verified edit |
|---|---:|---:|
| Earlier aggregation adapter, full graph, ordinary watch | 31.550 s | 20.997 s |
| Earlier aggregation adapter, focused graph, incremental watch | 8.928 s | 3.459 s |
| Current paging value plan, full graph, ordinary watch | 34.033 s | 2.898 s |
| Current paging value plan, focused graph, incremental watch | 5.940 s | 1.449 s |
| Current paging value plan, focused graph, macOS ordinary watch | 5.973 s | 2.209 s |

The aggregation comparison is about 6x; the current paging edit comparison is
about 2x, with about 5.7x lower startup time. These workloads have different
coverage and must not be combined into a claimed multiplicative speedup.
They do not measure a completely cold product build or arbitrary storage edits.

`make -C zig watch-plans` selects the focused paging suite by default; set
`WATCH_STEP=aggregation-plan-test` for the richer collection adapter tests.
macOS retains ordinary watch because the pinned compiler's incremental Mach-O
linker still panics when loading objects/archives. A native-backend experiment
also crashed and is not enabled. `make -C zig watch-linux` uses the pinned,
checksum-verified Linux development compiler through Docker. Zig's fanotify
watcher requires relaxing Docker's default seccomp filter in this development
container; no additional Linux capabilities are granted.

## Production attribution and verification

`runtime-archives` installs the actual production runtime, storage and inference
archives without creating a second compilation profile. The profile workflow
can select API, physical storage, inference, opaque consumer and focused plan
steps independently. `report_zig_archive_work.py` reports text families,
JSON/generated-contract candidates and repeated names across owners. ELF sizes
are reported directly; Mach-O address gaps include padding and omit the last
unknown extent. Shared names are candidates for investigation, not proof of
identical code or compiler time.

Completed checks include 57 compiled storage-owner regressions and the three
opaque query regressions in Debug; 47 native callback/error tests; all 33
focused tests in the modes above; 32 compilation-audit tests; six symbol-report
tests; six watch-measurement tests; ten compiler-sampler/summarizer tests; and
the WASM profile fixture. All five real production query/storage cache phases
pass after the redesign: initial build, warm reuse, query-only edit,
storage-only edit, and shared-contract edit.

The complete native stripped ReleaseFast product and C library build succeeds
with 136/136 selected steps, including 57 owner regressions, all three opaque
query regressions and the public C API smoke suite. Ten HTTP end-to-end tests
pass against the installed executable (query pruning/projection, query/join/
graph deadlines and semantic embedding-cache behavior).

With the same native baseline CPU, SDK 27.0, Zig 0.17.0 and disabled Metal/CUDA/
system BLAS settings as the preceding query split:

| Artifact | Preceding query split | This change | Difference |
|---|---:|---:|---:|
| Stripped executable | 232,628,712 B | 232,058,632 B | -570,080 B (-0.245%) |
| Public C library | 80,489,232 B | 80,489,232 B | Same byte count; different digest |
| Query + storage-adapter archives | 48,225,488 B | 46,921,816 B | -2.703% |

The C library remains below the 88 MiB release gate. The executable is still
2.845% larger than the earlier pre-query-split 225,639,896-byte executable;
this follow-up does not erase that earlier size tradeoff.

Observed per-compiler RSS samples include API 10.249 GiB, storage engine
4.530 GiB, storage adapter 4.165 GiB and query 1.334 GiB. Inference core reused
its compiled artifact in this run. Other host builds and substantial host swap
make these resident-memory and elapsed-time observations unsuitable for
lowering reservations or asserting a cold full-build improvement. No memory
claims or production optimization modes are relaxed. Digests, exact commands,
per-process traces and the consolidated validation JSON are retained under
`.benchmark-results/compile-costs` (ignored build evidence).

The sampler now recognizes Zig 0.17 `test-obj` commands as well as `test` and
`build-*`. The product trace started with the earlier sampler; its
production-library samples remain valid, and the fresh caller trace separately
measures the previously omitted test-object compiler. Both the object kind and
portable argument attribution have regression coverage.

Request throughput was not benchmarked. The new callback occurs once per
complete search component; no per-hit or per-record callback was introduced.

No whole-product 10x claim follows from these changes. A much smaller query
caller, real implementation-cache reuse, and small plan edit loops are separate
results from clean release time, compiler peak memory and final product size.
