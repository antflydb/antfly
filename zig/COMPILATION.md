# Antfly Zig compilation architecture

Last updated: 2026-08-11

## Main goal

Build the complete Antfly release reliably and quickly in `ReleaseFast` on the
normal cost-efficient CI runner, without serializing the build or relying on
extra memory as the permanent solution.

The result must remain one statically linked `antfly` executable. Standalone
must include embedded inference, the C API must reuse the same compiled storage
implementation, and production behavior must not be weakened to make the
compiler succeed.

This work has two related objectives:

1. Keep any one Zig/LLVM compilation unit below the memory and complexity level
   that triggered the ARM64 musl `std::bad_alloc` failure.
2. Reduce repeated LLVM optimization and object emission across the units that
   make up the final executable and release archive.

Build cache, swap, a larger runner, or `-j1` may be useful diagnostics, but they
do not satisfy the main goal.

## Product and build constraints

- The product remains a modular monolith delivered as one executable.
- `ReleaseFast` is the target configuration. `ReleaseSmall` is not the desired
  long-term release mode.
- Standalone always has embedded inference.
- Normal build concurrency must remain enabled. We do not use `-j1` as the
  architecture.
- Data, metadata, serverless, standalone, Lite, API, and inference remain
  independently testable commands or modes even when some are co-generated.
- The shared C API library must not carry unrelated server/runtime code.
- LSM is the production backend. LMDB remains available for tests, fixtures,
  conversion, and legacy compatibility while that support is needed.
- Runtime boundaries must be coarse enough that they do not introduce a call
  per record, posting, LMDB operation, or vector candidate.
- Allocation ownership, cancellation, deadlines, and error translation must be
  explicit at every compiled ABI boundary. Declared errors keep their stable
  semantic identities; adapters must not collapse them into broad status
  classes. Every migrated operation carries a validated failure envelope with
  its originating boundary and operation stage, and nested wrappers forward a
  valid inner envelope unchanged.

## Goal loop

Repeat this loop until the main goal and exit criteria are satisfied:

1. Measure the current cold ARM64 Linux musl `ReleaseFast` build, including
   per-unit wall time, LLVM time, declarations, analyzed files, overlap, peak
   memory when available, and final artifact/symbol shape.
2. Select one coarse ownership boundary from the largest remaining duplicated
   implementation family. State what control remains outside the boundary and
   what complete local operation moves behind it.
3. Implement the boundary behind the storage-kernel experiment. Preserve one
   static executable, embedded standalone inference, normal build concurrency,
   the small C API surface, exact behavior/error/cancellation ownership, and
   provider-to-consumer error identity. Model call failure, item outcome, and
   callback failure as independent channels; do not let one overwrite or
   normalize another. A wrapper may originate a new failure identity only for
   work it performed itself or for a malformed inner envelope; it must never
   relabel a valid nested failure as its own.
4. Validate the affected behavior in both experiment-enabled and legacy
   configurations, then run graph gates, ABI tests, artifact/symbol audits, and
   a genuinely cold ARM64 `ReleaseFast` comparison.
5. Make an explicit decision:
   - **keep** only when the boundary is behaviorally sound and either removes
     material compiler work now or is a necessary, bounded prerequisite to a
     named immediately-following cut;
   - **revise** when the ownership direction is correct but legacy roots remain
     reachable or the measured change is only host variance; or
   - **revert** when the boundary duplicates work, grows the wrong artifact,
     weakens behavior, or has no credible path to the target architecture.
6. Record the evidence and decision here, commit and push only the accepted
   increment, then name and begin the next largest viable slice.

The loop ends only when repeated cold builds are reliable on the normal runner,
storage/local query is compiled once and reused by the executable and C API,
the distributed critical path is at most 380 seconds (preferably 350 seconds),
production artifacts contain no LMDB implementation, and the experiment can
become the production architecture without increasing runner cost. Enabling it
by default, merging it, or increasing CI cost still requires explicit approval.

## Why source-module cleanup alone is insufficient

Zig analyzes source imports lazily. A source module, facade, or directory is not
itself a separately compiled library, and merely turning each `zig/lib`
directory into a static library would not guarantee reuse of generated code.

The profiles collected during this investigation consistently attribute about
97–98% of compiler wall time to LLVM emission. Parsing a small contract file in
multiple units is therefore much less important than optimizing and emitting a
large implementation in multiple units.

Meaningful reuse requires a compiled artifact boundary. If two separately
generated units need the same implementation, that implementation must be
compiled once behind an ABI and linked once into the final executable.

References:

- [Zig compilation model](https://ziglang.org/documentation/0.16.0/#Compilation-Model)
- [Zig 0.16 release notes](https://ziglang.org/download/0.16.0/release-notes.html)

## Original failure and diagnostics

The triggering CI failure was an ARM64 Linux musl release build ending in
`std::bad_alloc` during the Zig release/archive path.

The evidence did not match an ordinary cgroup OOM:

- cgroup OOM counters remained zero;
- observed cgroup peak memory was about 18–20 GB;
- the extracted direct `zig build-exe` command succeeded at approximately
  15.2 GB RSS;
- a direct `strace` replay also succeeded;
- the failing path invoked the compiler through Zig's build-runner/compiler
  protocol with `--listen=-`, while the successful direct replay omitted that
  protocol; and
- cache state changed how far the build progressed, but did not establish that
  invalid Antfly code generation or basic runner exhaustion was the cause.

This leaves a likely Zig build-runner/compiler-server or LLVM pressure issue.
The architecture work does not depend on proving the precise upstream bug: a
smaller, explicit compilation graph improves reliability and build time either
way.

We briefly considered a 64 GB runner and swap. They remain useful controls, but
were rejected as the primary fix because the evidence did not show a cgroup OOM
and the direct compiler invocation already succeeded within the smaller memory
envelope.

## What we learned from the graph

An early five-report compiler comparison contained:

- 2,147 repository-file instances;
- 1,204 unique repository files;
- 943 duplicate instances; and
- 163 duplicated storage files accounting for 255 duplicate instances and
  roughly 382,000 duplicated source lines.

HTTPX and LMDB were much smaller targets. Their exact instance counts changed
as runtime units were consolidated—approximately 72–94 duplicate HTTPX
instances over 22,000 unique lines and 48–72 LMDB instances over 13,000 unique
lines. These counts are report-set dependent and must not be compared directly
with the current three-unit reports.

The important conclusion was stable: storage dominated the overlap. HTTPX was
too small to justify a dedicated ABI, and LMDB should disappear naturally from
production compilation rather than receive another production library.

Lexical graph measurements are deliberately conservative. They include imports
inside lazy declarations, tests, and disabled comptime branches. Zig time
reports are better for measuring real compiler units, but their `all_files`
list can still include cheaply parsed lazy/test-only files. File and line counts
are diagnostics; LLVM time and declarations are the stronger decision inputs.

## Experiment log

Measurements in different rows are not necessarily comparable unless they use
the same host, target, cache state, and report set.

| Experiment | Result | Decision |
|---|---:|---|
| Direct replay without build-runner protocol | Succeeded at about 15.2 GB RSS | Failure was not demonstrated to be ordinary OOM |
| Larger runner / swap | Diagnostic workaround | Do not make it the architecture |
| Serial compilation | Avoids concurrency pressure but lengthens the build | Rejected; keep normal concurrency |
| CLI-only source facade | Broad implementation leaked back through internal imports | Rejected as insufficient |
| Runtime-specific root for production runtimes | Removed direct imports of the public/test root | Kept as graph hygiene |
| Data alone, local ARM64 `ReleaseFast` | 371.6 s | Baseline for co-generation |
| Data + metadata in one unit | 376.9 s, only +5.3 s | Strongly favors co-generation |
| Data + serverless in one unit | 387.1 s, +15.5 s | Strongly favors co-generation |
| Serverless alone | 131.7 s | Separate emission would mostly duplicate work |
| Data + metadata artifact | About 5.3 GB RSS, 31 MB stripped static executable | Safe enough to continue |
| Restore-staging boundary prototype | Client 144.3 s → 38.8 s; removed 311 files and about 485,000 lexical lines | Coarse opaque bridges can pay off |
| API + distributed co-generation | About 420 s, roughly 70 s slower than the then-current split | Rejected |
| Reuse distributed archive for CAPI | `libantfly.so` temporarily grew 16.6 MB → 28.3 MB | Needed section-level GC |
| Function/data section GC | `libantfly.so` returned to 16.55 MB | Kept |
| Production LSM-only feature set | No `mdb_*` or LMDB backend symbols in the storage archive | Kept; LMDB remains in test/legacy profiles |
| Opt-in build-only storage kernel | Distributed 425.652 s → 415.352 s, but added a 230.093 s unit and raised duplicate instances from 393 to 756 | Keep only as Phase 2 scaffolding; do not enable in production yet |
| One-call `DB.search` probe | Distributed 415.352 s → 401.086 s, but declarations changed only 41,984 → 41,981 and storage overlap remained 98.9% | Rejected; a call boundary without storage ownership does not remove codegen |
| Provisioned read-vtable probe | Distributed 415.352 s → 397.153 s; only 83 declarations moved | Rejected as incomplete; hosted reads retained the physical-query graph |
| Combined provisioned + hosted read-vtable probe | Distributed 415.352 s → 367.185 s and 41,984 → 40,585 declarations | Go for a local-query island, but revert the raw prototype and redesign the ABI/ownership split |
| Post-serverless compile-once control | Repeated cold combined distributed/storage builds: 358.589 s and 352.900 s; aggregate duplicate instances 873 → 482; executable 72.121 MB → 60.804 MB; C API unchanged at 16.382 MB | Historical keep decision; superseded by repeated 9–11 m normal-runner results |
| Post-main normal-runner controls | Two clean ARM64 musl `ReleaseFast` archives succeeded on the normal 24 GiB publish runner; application/storage varied from 9 m to 11 m at 10 GB MaxRSS, with zero swap | Reliability and artifact gates pass, but the critical unit repeatedly misses the 380-second target; continue architecture work at the existing runner cost |
| Document/media compute island, normal runner | Storage 615.200 s, distributed 459.376 s, inference 498.361 s, enrichment 34 s; archive 17:52.22; observed cgroup peak 15.51 GiB with zero OOM events | Boundary and artifacts pass, but overlapping the large initial units causes CPU contention and misses the time gate; keep the boundary opt-in and revise scheduling/composition |
| Data-only PIC storage probe | 283.018 s, 277.375 s LLVM, 593 repository files, 36,065 declarations | Establishes that PIC/CAPI storage ownership is not the excess cost |
| Data + standalone/Lite + CAPI PIC probe | 288.171 s, only +5.153 s over data alone; 614 repository files, 37,166 declarations | Strong candidate ownership island; CLI/metadata roots account for the remaining 82.685 s |
| CLI + metadata control-only probe | 295.647 s, 289.733 s LLVM, 600 repository files | A separate control unit meets the time gate but duplicates too much physical storage by itself |
| Remote-only metadata control split | Storage 348.021 s and metadata 266.156 s, but duplicate instances 438 -> 965 and executable 59.271 MB -> 76.892 MB | Rejected; metadata still emits physical/catalog storage and cannot split before that ownership crosses the kernel ABI |
| API/serverless + CLI/metadata coalescing | API/control 409.246 s; storage runtime 313.292 s; duplicate instances 484 → 676; executable 61.224 MB → 72.464 MB | Rejected; moving roots without moving physical ownership misses time, overlap, and artifact gates |
| Application/storage without remote CLI | 340.130 s, 332.415 s LLVM, 40,037 declarations, 636 repository files | Keep; below the preferred 350-second local gate |
| Remote CLI after HA/restore ownership cut | 38.029 s, 35.904 s LLVM, 5,804 declarations, 53 repository files and no Antfly storage files | Keep as an independent final codegen unit |
| Four-unit production archive with deterministic 20 GB scheduling | 30/30 steps in 374.23 s locally and 15:17.77 on the normal runner; static executable 62.421 MB; C API 16.665 MB | Reliable, but the 11 m Linux application unit misses the performance gate |
| Serverless local runtime co-generated with application/storage | Application 361.270 s; API 125.604 s; duplicates 527 → 438; executable 62.421 MB → 59.271 MB | Keep pending normal-runner confirmation; under 380 s and removes material storage duplication |
| Canonical storage-contract/import cut | Removed 42k lexically reachable lines but declarations 17,332 → 17,334 with identical generic/inline counts | Rejected; lazy file removal is not emitted-code removal |
| Isolated local HA runtime | Application saved 5.344 s while a new HA unit cost 30.525 s and duplicated 75 files | Rejected; small role splitting increases aggregate LLVM work |
| Experimental control-only provisioned write vtable | Application 366.523 s, 41,591 declarations, and only 703 KB less allocatable object data while adding a 216.358 s storage unit | Rejected; the vtable shell is small and direct runtime storage ownership remains |
| Atomic control-only physical-source cut | Focused distributed control 236 s; full cold archive 509 s -> 458 s; storage 316 s / 4.03 GiB and distributed 233 s / 2.98 GiB; executable 71.79 MB -> 65.27 MB | Keep behind the experiment; production enablement remains pending repeated normal-runner proof |
| First normal-runner physical-source cut | Storage 11 m / 10 GB, distributed 8 m / 8 GB, inference 7 m / 6 GB; complete archive 16:25.14; executable 65.27 MB; C API 16.68 MB | Reliable and structurally valid, but both critical units miss 380 s; revise using authoritative Linux time reports |
| Atomic local-query cut, normal runner | 43/43 steps; storage 590.707 s, distributed 482.636 s, local query 127.258 s, inference 528.849 s; complete build 19:23.59; executable 63.312 MB; C API 20.495 MB | Keep the identity-safe boundary opt-in; source ownership and artifacts pass, but reject the current six-unit schedule as the production performance solution |
| Co-generated storage/local query, local cold ARM64 | Combined unit 313.334 s; duplicate instances 727 -> 523; emitted overlap 3.553 MB -> 1.948 MB; executable 61.400 MB; C API 18.461 MB | Keep as the five-unit runner candidate; same opaque/error ABI with one fewer LLVM unit, pending normal-runner proof |
| Co-generated storage/local query, normal runner | 39/39 steps; matched-control storage/query chain 664.411 s -> 599.665 s; duplicate instances 523; executable 61.399 MB; C API 18.461 MB; build 17:21.73 | Keep opt-in; all structural gates pass, but distributed -> inference now controls wall time and storage/query still misses 380 s |

The `/tmp` graph analysis was consolidated into
`tools/analyze_zig_import_graph.py`. It reports lexical reachability, consumes
Zig time-report JSON, reads allocatable sections from cross-compiled ELF
objects, compares loaded-file and emitted-module overlap, and checks
architectural boundaries. Object reporting distinguishes lazy import
reachability from machine code/data that LLVM actually emitted. Its regression tests live in
`tools/test_analyze_zig_import_graph.py`.

## Current compilation architecture

The default linked release currently generates four coarse libraries with
normal, memory-budgeted concurrency:

```text
antfly executable
├── antfly-runtime-api_kernel   # public API protocol/handler implementation
├── antfly-storage-kernel       # application/serverless roles plus storage/CAPI
├── antfly-runtime-inference
└── antfly-runtime-cli          # remote/client commands only
```

The executable links all four into one statically linked binary. The C API
shared libraries link the same PIC distributed/storage archive, with function
and data sections allowing the linker to retain only C API roots.

The name `antfly-storage-kernel` currently describes the archive's reuse role,
not a pure domain boundary. It contains data, metadata, serverless local
execution, standalone/Lite, local HA, restore staging, and storage
implementations. Remote CLI commands are a small independent unit. The public
API protocol remains separate; inference remains a safety valve and is linked
into standalone.

The first ABI preparation is complete:

- import-facing `TableReadSource` and `TableWriteSource` contracts are separate
  from their implementations;
- query responses, table creation, backup plans, transaction envelopes,
  preflight results, background statistics, and other focused payloads live in
  smaller data-contract modules;
- live backup locations cross the write callback boundary as opaque handles;
- production API imports the callback contracts instead of the table read/write
  implementations;
- linked production builds default to `-Dproduction-lsm-only=true`;
- LMDB-specific tests and builds can set
  `-Dproduction-lsm-only=false`; and
- the graph checker rejects direct storage/table implementation imports from
  the API ABI files; and
- the experimental owner ABI has one process-scoped context for shared
  resource-manager, LSM-cache, and HBC-cache state across opaque group owners.

This source-level separation protects the public API from accidental
implementation barrels. The process context was the foundation for the
physical-owner cut now selected by the opt-in composition; its existence alone
still is not evidence that the experiment should be production-enabled.

The opt-in experiment now selects a different four-unit composition:

```text
antfly executable
├── antfly-runtime-distributed  # API/distributed control plus product composition
├── antfly-storage-kernel       # physical storage/local query, restore, CAPI
├── antfly-runtime-inference    # inference plus standalone inference host
└── antfly-runtime-cli          # remote/client commands only
```

In that composition, the distributed unit is compiled with control-only
storage sources and can reach physical local operations only through opaque
owner, data-apply, wire, and callback contracts. The same sectioned PIC storage
archive supplies the executable and C API libraries. This remains opt-in until
the normal Linux runner confirms the local cold-build result repeatedly.

## Historical pre-serverless clean-cache profile

The following was a local macOS-to-ARM64-Linux-musl cross compilation from a
fresh cache on 2026-08-07. All three units compiled concurrently and all 23
build steps succeeded.

| Unit | Compiler time | LLVM emit | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|
| API kernel | 142.809 s | 138.703 s (97.1%) | 17,232 | 325 |
| Distributed/storage | 425.652 s | 416.721 s (97.9%) | 42,705 | 727 |
| Inference | 242.082 s | 234.470 s (96.9%) | 24,945 | 523 |

Because the units run concurrently, the distributed/storage unit determines
the approximately seven-minute critical path.

Across these three reports there were 1,575 repository-file instances, 1,182
unique files, and 393 duplicate instances. Storage accounted for 50 duplicated
files and 52 duplicate instances. This is not directly comparable to the early
943 count because the earlier measurement used five compiler graphs.

Artifacts from that profile were:

| Artifact | Size |
|---|---:|
| `antfly` ARM64 static executable | 57,640,592 bytes |
| `libantfly.so` | 16,554,944 bytes |
| distributed/storage archive | 41,591,102 bytes |
| API archive | 13,166,554 bytes |
| inference archive | 22,779,076 bytes |

The distributed/storage archive contained no `mdb_*`, `lmdb_backend`, or
`backend_lmdb` symbols. Remaining LMDB strings are metric names, compatibility
enums, or diagnostics rather than the production engine.

### Phase 1 build-only storage-kernel result

The first opt-in skeleton was measured from fresh local and global caches on
the same macOS host, cross-compiling ARM64 Linux musl `ReleaseFast` with normal
build concurrency. It compiled the existing coarse, opaque-handle C API DB
implementation into a fourth PIC static archive. The distributed runtime no
longer rooted those C API exports, while the executable and both shared C API
libraries linked the new archive.

| Unit | Compiler time | LLVM emit | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|
| API kernel | 147.667 s | 143.258 s | 17,232 | 325 |
| Distributed control plus direct storage ownership | 415.352 s | 406.186 s | 41,984 | 723 |
| Experimental storage kernel | 230.093 s | 224.432 s | 25,362 | 367 |
| Inference | 247.848 s | 239.826 s | 24,945 | 523 |

The clean build completed all 26 steps. The largest sampled compiler RSS was
about 4.34 GiB during distributed LLVM emission; this was observational
sampling rather than a per-unit peak-RSS trace.

The distributed unit improved by only 10.300 seconds (2.4%) and 721
declarations. Meanwhile, aggregate duplicate repository-file instances rose
from 393 to 756. The storage unit shared 363 of its 367 repository Zig files
with distributed—98.9% of the smaller graph. This is the expected result while
the runtimes continue to own and call storage implementations directly.

Artifact boundaries remained healthy:

| Artifact | Phase 1 size | Baseline size |
|---|---:|---:|
| `antfly` ARM64 static executable | 57,638,496 bytes | 57,640,592 bytes |
| `libantfly.so` | 16,355,776 bytes | 16,554,944 bytes |
| distributed archive | 33,939,148 bytes | 41,591,102 bytes |
| experimental storage archive | 22,115,732 bytes | not present |

Neither the executable nor shared C API library contained production LMDB
implementation symbols. The shared library exported the intended `antfly_db_*`
surface and did not retain distributed runtime entry points.

Decision: the skeleton validates the link topology, PIC reuse, opaque handle
convention, and section GC, so it remains available behind
`-Dstorage-kernel-experiment=true`. It is not enabled by default and is not a
go decision by itself.

### Phase 2a one-call local-query probe

The first query probe deliberately used a temporary, non-production raw-pointer
bridge to answer a narrow codegen question before designing the stable wire
ABI. It routed the final `DB.search` / profiled dense-search call through the
experimental archive while leaving DB ownership, leases, aggregation, result
merging, and all other storage operations in the distributed runtime.

The probe was built from fresh local and global caches on the same host and
with the same ARM64 Linux musl `ReleaseFast` flags and normal concurrency as
Phase 1. All 27 build steps succeeded.

| Unit | Compiler time | LLVM emit | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|
| API kernel | 137.866 s | 133.586 s | 17,232 | 325 |
| Distributed control plus direct storage ownership | 401.086 s | 392.515 s | 41,981 | 723 |
| Experimental storage kernel | 220.105 s | 214.689 s | 25,370 | 367 |
| Inference | 236.875 s | 229.213 s | 24,945 | 523 |

The distributed time was 14.266 seconds below the Phase 1 run, but only three
declarations left the unit. Storage still shared 363 of 367 repository files
with distributed (98.9%), and aggregate duplicate instances remained 756.
That combination identifies timing variance, not a material movement of LLVM
work. The probe also grew the stripped executable from 57,638,496 to
60,697,720 bytes (5.3%) because both ownership paths remained reachable.

The focused cross-archive local-query suite passed all 15 tests and the C API
suite passed all 11 selected tests. A broader table-read fixture failed with
the same `DocIdentityNamespaceMismatch` both with and without the probe, and a
standalone smoke failed during startup identically in both configurations;
neither was used as evidence for or against the boundary.

Decision: reject and fully revert the raw-pointer bridge. Moving a terminal
method call while the consumer still owns `DB` does not satisfy Phase 2's
complete-operation requirement and cannot remove the implementation graph.
Do not spend time designing a stable result wire ABI around this shape. The
next storage experiment must transfer ownership of a complete local table/shard
runtime behind an opaque kernel handle, so distributed code cannot directly
instantiate or call the storage implementation.

### Phase 2b/2c complete read-vtable probes

The next measurement routed the complete `ProvisionedTableReadSource` callback
vtable through the storage archive. Unlike the one-call probe, this covered
lookup, scan, query, preflight, text/algebraic planning, graph operations,
runtime status, and artifact reads. The source context remained layout-coupled
and results/errors remained raw Zig types solely to measure codegen before
designing the production ABI.

| Unit | Provisioned-only time | LLVM emit | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|
| API kernel | 138.431 s | 134.315 s | 17,232 | 325 |
| Distributed | 397.153 s | 388.728 s | 41,901 | 723 |
| Experimental storage kernel | 240.743 s | 235.091 s | 27,301 | 415 |
| Inference | 235.827 s | 228.249 s | 24,945 | 523 |

This moved 1,939 declarations into storage but removed only 83 declarations
from distributed. The reason was a second production owner:
`HostedProvisionedTableReadSource` still rooted the same physical-query
implementation for metadata/standalone composition.

A follow-up routed both provisioned and hosted read vtables through the same
storage artifact:

| Unit | Combined-vtable time | LLVM emit | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|
| API kernel | 135.831 s | 131.757 s | 17,232 | 325 |
| Distributed | 367.185 s | 358.941 s | 40,585 | 716 |
| Experimental storage kernel | 240.425 s | 234.618 s | 27,575 | 424 |
| Inference | 232.352 s | 224.778 s | 24,945 | 523 |

All 30 clean-cache build steps succeeded with normal concurrency. The result
reduced the Phase 1 distributed unit by 48.167 seconds (11.6%) and 1,399
declarations, crossing the 380-second go/no-go threshold. Adding the hosted
surface cost the storage unit only 274 declarations and no measurable time.
This is strong evidence that a compiled local-query island can pay off.

The raw prototype itself was fully reverted because it violated three required
properties:

- the hosted vtable also carried routing, remote HTTP fanout, and result merge
  into the storage archive instead of leaving distributed control outside;
- arbitrary Zig error-set numbers differed between compilation units (the
  expected `HAReadRequiresPrimary` arrived as an unrelated error), proving that
  explicit stable status translation is mandatory; and
- the executable grew from 57,638,496 to 71,458,096 bytes (24.0%) while both
  direct storage ownership and linked read paths remained reachable.

The focused suite passed 14 of 15 tests; the sole failure was the deliberate
error-identity check above, not a success-path or memory-layout failure. The
combined reports contained 806 duplicate repository-file instances, so the
probe improved the critical path without yet satisfying the compile-once goal.

Decision: proceed, but first separate distributed orchestration from local
execution in the source model. Top-level lookup/scan/query, routing, fanout,
remote calls, graph coordination, aggregation merge, and postprocessing stay
in distributed. A dedicated local-operation source crosses a versioned ABI and
owns one complete group operation at a time. Its opaque owner contains the DB,
read/write caches, resource manager, and storage runtime. Statuses use an
explicit enum, request/result envelopes have C-compatible versioned layouts or
owned wire buffers, and every returned allocation is destroyed by the kernel.

### Phase 2d stateless stable-query ABI probe

A subsequent probe implemented the intended stable mechanics before moving
ownership: versioned C-compatible request/result structures, explicit status
translation, borrowed table metadata and query wire, a readable-lease callback,
kernel-owned response/profile buffers, and explicit destruction. The
distributed side retained routing, aggregation, and postprocessing. A focused
test linked the consumer and provider as separate static archives and executed
the existing dense and profiled-dense query fixtures across the boundary.

Both cross-archive queries correctly failed with `GenerationTransitionActive`.
The fixture, like a production data node, already had its writer DB open in the
distributed compilation unit. The storage archive has a separate process-local
DB registry and cannot open a second query DB for that live generation. The
current in-process implementation avoids that conflict by leasing the resident
writer DB; a stable compiled boundary cannot safely borrow that Zig `DB`
pointer or its layout-coupled lease.

Decision: reject and fully revert the stateless query ABI without spending a
cold ReleaseFast measurement on a behaviorally invalid architecture. Do not
work around this by closing the writer, weakening generation locking, or
mapping the conflict to a retry. The next experiment must establish the opaque
storage owner first: the kernel must create and retain the live table/shard DB
used by both writes and reads. Query migration then targets that kernel-owned
handle. This tightens Phase 2's prerequisite from “query plus a new read cache”
to “shared read/write storage ownership,” while leaving routing, fanout, merge,
and policy in distributed control.

### Phase 2e opaque live-owner foundation

The storage archive now exposes an opt-in, hidden internal owner ABI backed by
the same `Handle` and `DB` implementation already compiled for the C API. The
open request is versioned and carries only explicit-width path, root-generation,
and document-identity fields. The consumer receives an opaque handle; coarse
batch and query operations accept borrowed JSON already used by the API
contracts; result buffers remain kernel-owned and have an explicit destroy
operation. A typed consumer imports only the ABI module and cannot name `DB`,
LSM, indexes, or any storage implementation type.

A focused test compiles the consumer and provider as separate static archives,
opens one writer owner, performs a two-document batch and match-all query on
that same live DB, validates the response, and destroys both returned buffers.
A concurrent second owner for the same root is rejected as `busy`, confirming
that the boundary preserves writer exclusivity rather than bypassing the
generation lock. ABI-version rejection and repeated empty-buffer destruction
are also covered.

Validation at this foundation checkpoint:

- opaque owner ABI tests: 2/2 passed with zero leaks;
- existing public C API tests: 10/10 passed with zero leaks;
- the full linked Debug executable built successfully with normal concurrency;
- runtime/codegen/API graph gates and all 13 analyzer tests passed; and
- the internal `antfly_storage_owner_*` symbols remain hidden from the shared
  C API library's global symbol table.

Decision: keep the owner foundation. It intentionally has no production
consumer yet, so a cold ReleaseFast graph measurement would only remeasure the
existing CAPI-backed storage unit and cannot demonstrate deduplication. The
next measured checkpoint must route a representative provisioned local batch
and local query through the same per-group owner. Once an owner is active for a
group, neither operation may fall back to a distributed-owned `DB`; lifecycle
and destruction tests must prove the owner is closed only after both read and
write users drain.

### Phase 2f provisioned batch/query owner slice

The first production-source consumer now exists behind the opt-in storage
kernel architecture. `ProvisionedTableWriteSource` retains top-level catalog
routing, grouping, write coalescing, admission, table activity, HA gating, and
change publication, then delegates one complete group batch through a new
group-local physical-write seam. The existing provisioned read source retains
query routing, consistency policy, fanout, merge, and postprocessing. Its
group-local query callback crosses the same owner ABI.

`ProvisionedKernelOwnerSource` is the shared consumer-side lifecycle object. It
caches one opaque owner per table/group/generation/identity descriptor, leases
that owner independently to reads and writes, and rejects an in-place catalog
identity or visible-generation change until an explicit transition closes the
old owner. First-open creation is serialized with a yielding lock; steady-state
operations hold the lock only while incrementing or decrementing the active
lease count. Destruction asserts that all users have drained before closing the
kernel DB.

The internal operation envelope now carries its ABI version and table name
explicitly. The open descriptor also carries owned catalog schema and index
JSON. The storage archive applies that contract to the writer DB before
publishing the handle. Batches use the canonical internal batch wire dialect,
which preserves sync level and split-replication metadata while rejecting
unsupported graph or predicate fields instead of silently dropping them.
Queries use the existing resolved internal group-query wire dialect rather
than reparsing it as a public request, and non-stale reads still pass through
the existing Raft readable-lease gate before crossing the ABI. Returned JSON
remains kernel-owned until the consumer duplicates or parses it and calls the
explicit destroy operation.

A cross-static-archive test exercises the normal top-level provisioned source
APIs rather than calling the ABI directly. It routes a two-document indexed
batch, a match-all query, and a dense query through one owner; verifies both
`read_index` gates; confirms the real table name survives the ABI; confirms a
second writer open remains `busy`; and proves the DB can reopen only after the
provisioned write source and shared owner have drained. The dense result order
also proves that catalog index installation and indexed execution occur in the
storage archive.

Validation at this checkpoint:

- provisioned cross-archive owner suite: 6/6 passed with zero leaks;
- opaque owner ABI suite: 2/2 passed with zero leaks;
- provisioned table-write regression suite: 73/73 passed with zero leaks,
  including the focused routing/write seam;
- existing public C API suite: 10/10 passed with zero leaks;
- linked native Debug with production LSM-only options succeeded with normal
  concurrency;
- runtime/codegen/API graph gates passed and all 13 analyzer tests passed; and
- the internal owner/runtime symbols remained hidden from the shared C API
  global symbol table.

Decision: keep this behaviorally valid representative slice, but do not attach
it to every data-runtime path or claim a ReleaseFast reduction yet. Once this
owner is resident, an unmigrated lookup, scan, runtime-status, transaction,
restore, or maintenance path must not open a second distributed-owned DB for
the same group. Global enablement would therefore be premature and a cold
application measurement would still contain both physical implementations.
The next slice must broaden the same owner—not add another handle—to the
remaining coarse group-local read operations and the minimum writer lifecycle
needed for safe runtime attachment. Only then should compile-time selection
make the distributed query/write implementations unreachable and trigger the
next ARM64 ReleaseFast go/no-go measurement.

### Phase 2g provisioned lookup/scan owner slice

The same provisioned owner now performs group-local point lookup and range
scan. The consumer retains table/range routing, multi-range scan aggregation,
HA admission, read preparation, and Raft consistency fallback. One complete
lookup or scan then crosses the compiled boundary; neither operation opens a
second DB or introduces a per-record callback.

The owner ABI is now version 2 and binds the table name at open time. Every
batch, lookup, scan, and query operation must name that same table, preventing
a caller from relabeling a response or accidentally using a table/group owner
for another catalog entry. Lookup requests preserve field projection and
return both owned JSON and the stored document version. Scan requests preserve
the complete physical contract: range endpoints, inclusive/exclusive bounds,
document inclusion, limit, field projection, and filter-query JSON. The
storage archive returns the existing NDJSON representation in one owned
buffer.

Validation also found that the internal batch encoder preserved sync and split
metadata but not an explicit `timestamp_ns`. The internal-only batch dialect
now round-trips `_timestamp_ns`; the public batch parser still rejects that
field. This prevents document versions from changing merely because a batch
crossed the compiled boundary.

The cross-static-archive test now drives the normal provisioned APIs through a
single owner in this order: an indexed batch at timestamp 4242, projected
lookup, missing lookup, filtered/projected bounded scan, match-all query, and
dense query. It verifies the exact lookup version, projection/filter behavior,
all five readable-lease requests, indexed result order, one live owner, writer
exclusivity, and reopen after drain. The suite remains 6/6 with zero leaks;
the low-level owner ABI suite remains 2/2 with zero leaks. The 73-case
provisioned write regression suite and 10-case public CAPI suite also pass with
zero leaks. The full linked native Debug build succeeds with normal
concurrency, graph gates and all 13 analyzer tests pass, and no internal owner
or runtime symbol appears in the shared CAPI global symbol table.

Decision: keep this slice. It closes the ordinary document-read surface but is
still not a production-enable or cold-build checkpoint. Provisioned preflight,
text/algebraic/graph helpers, document-artifact reads, runtime status, and
writer transition/maintenance operations still have physical callbacks that
must share this owner before the distributed runtime can attach it without
losing behavior or opening a competing DB. Continue migrating those coarse
families, then make the legacy physical path compile-time unreachable and
measure the clean ARM64 ReleaseFast application unit.

### Phase 2h provisioned preflight owner slice

Provisioned group-local query preflight now executes against the same opaque
owner as batch, lookup, scan, and query. Distributed control retains table and
range routing, fanout scheduling, Raft read admission, cross-shard summary
merging, and vector-worker policy. One coarse owner call performs live index
binding validation and local planning-stat collection, then returns the full
`RuntimePreflightSummary` as one kernel-owned response buffer.

The cross-archive test preflights the same named dense query later used for
execution. It verifies the installed dense index estimate, dense planning-cost
count, and the existing composed-query worker classification while retaining a
single owner for all six read/write leases. This also exercises the production
query-normalization shape: a named embedding query carries an unbound
`full_text match_all` sentinel even when the table has no text index.

That shape exposed an existing inconsistency between live binding validation
and planning-stat collection. Both previously treated every non-null
`full_text` query as requiring a primary text index. The shared requirement is
now explicit: scoring text and structured filters require an index; an
explicitly named sentinel still requires its named index; and an unbound
`match_all` or `match_none` sentinel uses a default text index when one exists
but remains valid when none exists. Binding validation and index-estimate
collection now use the same rule. The existing live-binding regression proves
that explicitly missing indexes still fail closed.

Validation at this checkpoint:

- provisioned cross-archive owner suite: 6/6 passed with zero leaks;
- opaque owner ABI suite: 2/2 passed with zero leaks;
- current provisioned write/lifecycle regression set: 67/67 passed with zero
  leaks;
- existing public C API suite: 10/10 passed with zero leaks;
- DB compatibility suite: 650 passed, 5 intentionally skipped, zero failures
  and zero leaks, including the live preflight binding regression;
- linked native Debug with production LSM-only options succeeded with normal
  concurrency;
- runtime/codegen/API graph gates passed and all 13 analyzer tests passed; and
- the internal owner/runtime symbols remained hidden from the shared C API
  global symbol table while the public `antfly_db_open` and
  `antfly_db_close` exports remained present.

Decision: keep this slice. It removes another live-DB user from the eventual
distributed physical graph, but the legacy implementation remains reachable
through text-stat, algebraic-partial, graph, document-artifact, runtime-status,
and writer lifecycle callbacks. Do not infer a clean-build improvement from a
warm Debug validation or enable the owner globally yet. Migrate the remaining
coarse auxiliary read families through this owner, close the minimum writer
lifecycle surface, then make the old callbacks compile-time unreachable before
the next cold ARM64 Linux musl `ReleaseFast` go/no-go measurement.

### Phase 2i text-stat and algebraic-partial owner slice

The two auxiliary planning/execution scans now use the same resident opaque
owner. A text-stat call carries one complete existing JSON request, performs
the explicit-query or background-field statistic scan inside the kernel, and
returns one complete response. An algebraic-partial call carries the complete
planned tensor/access-path request, performs the local materialized-expression
scan inside the kernel, and returns all local partials together. There is no
per-term, per-posting, per-row, or per-LSM-call ABI crossing.

Distributed control still owns group selection, read admission, fanout,
cross-group statistic/partial merging, algebraic proof and program planning,
and final response construction. The kernel owns parsing the already-natural
local wire payload, validating its live generation and index bindings, reading
the resident DB and indexes, and encoding the local result. Both callbacks
acquire the owner already used by batch, lookup, scan, query, and preflight;
they do not open another DB or introduce another storage handle.

The internal owner ABI is now version 3. It gives query failures stable status
values for invalid and unsupported requests, missing indexes, changed identity
read generations, and timeouts instead of collapsing them to a generic kernel
failure. The consumer maps those statuses back to the existing exact errors.
The cross-archive regression installs full-text and algebraic indexes beside
the existing dense index, verifies corpus and term frequencies, verifies a
materialized `sum_by_category` partial, and checks exact stale-generation and
invalid-request failures while retaining one live owner.

Validation at this checkpoint:

- provisioned cross-archive owner suite: 6/6 passed with zero leaks;
- opaque owner ABI suite: 2/2 passed with zero leaks, including every new
  stable status and both invalid-ABI entry points;
- current provisioned write/lifecycle regression set: 67/67 passed with zero
  leaks;
- existing public C API suite: 10/10 passed with zero leaks;
- linked native Debug with production LSM-only options succeeded with normal
  concurrency;
- runtime/codegen/API graph gates passed and all 13 analyzer tests passed;
- the linked native executable and shared C API contained no LMDB or
  `backend_lmdb` global symbols; and
- internal owner/runtime symbols remained hidden from the shared C API global
  symbol table while public `antfly_db_open` and `antfly_db_close` remained
  present.

Decision: keep this slice. The authoritative lexical graph remains unchanged
at this intermediate point because the legacy physical callbacks are still
reachable, so a warm native Debug build is not evidence of a compiler-time or
RSS improvement and no such claim is made. The next useful slices are the
remaining graph and document-artifact read families, followed by runtime
status and the minimum writer lifecycle needed to attach this owner globally.
Only after the legacy provisioned physical path becomes compile-time
unreachable should the experiment pay the cost of the next clean ARM64 Linux
musl `ReleaseFast` time/RSS/graph/artifact comparison.

### Phase 2j graph-operation owner slice

All three group-local graph operations now execute against the same resident
opaque owner. Expand carries one frontier batch and returns all local
expansions; hydrate carries one key batch and returns the complete hit and
incoming-edge vectors; and edge lookup carries one validated tensor access
path/program and returns the complete edge list. The boundary reuses the
canonical internal graph JSON contracts, so it does not cross per frontier
node, edge, posting, or document.

Distributed control still validates the topology epoch, performs HA and Raft
read admission, selects groups, schedules fanout, authorizes cross-table
hydration, and combines traversal state. Before invoking the owner it clears
the already-validated topology epoch. The kernel validates identity generation
and resolved-filter context, performs the local graph/index and document reads,
and encodes the local result. Expand, hydrate, and edge lookup all lease the
same owner as the existing batch/query/read operations; none opens a competing
DB.

The internal owner ABI is now version 4. Its controlled JSON request carries an
absolute monotonic deadline and a synchronous borrowed cancellation flag. The
deadline is established before admission and is not reset while crossing the
compiled boundary; the cancellation pointer is never retained. Exact
`Cancelled` and `Timeout` outcomes therefore survive the ABI rather than being
collapsed into a generic failure.

Seeding the cross-archive graph regression exposed two pre-existing wire gaps:

- the canonical internal batch encoder rejected graph mutations even though
  the owner batch operation is intended to represent the complete local batch;
  internal `_graph_writes` and `_graph_deletes` now round-trip owned edge
  fields while the public batch parser rejects those internal fields; and
- graph expansion JSON omitted null optional path fields, but the typed decoder
  treated them as required. Those fields now default to null, matching the
  established wire encoder and remote-worker behavior.

Validation at this checkpoint:

- provisioned cross-archive owner suite: 6/6 passed with zero leaks, exercising
  graph expand, hydrate, edge lookup, graph mutation seeding, and exact live
  cancellation and expired-deadline errors while retaining one owner;
- opaque owner ABI suite: 2/2 passed with zero leaks, including all three new
  invalid-ABI entry points and the stable cancelled status;
- focused table-read and distributed-graph suite: 25/25 passed with zero
  leaks, including cross-range graph expansion and remote deadline propagation;
- current DB compatibility filter: 634 passed, 5 intentionally skipped, 0
  failed, and 0 leaked, covering graph mutation, index, replay, split/merge,
  snapshot, restore, and durable-LSM behavior;
- current provisioned write/lifecycle regression set: 68/68 passed with zero
  leaks, including the new internal graph-mutation wire round trip;
- existing public C API suite: 10/10 passed with zero leaks;
- linked native Debug with production LSM-only options succeeded with normal
  concurrency;
- runtime/codegen/API graph gates passed and all 13 analyzer tests passed; and
- production-LSM native artifacts contained no LMDB globals, internal
  owner/runtime symbols remained hidden from the shared C API, and public
  `antfly_db_open` and `antfly_db_close` remained present.

Decision: keep this slice. It removes the last graph-specific live-DB callbacks
from the path that will attach the compiled owner, but legacy document-artifact
reads, runtime/status inspection, and writer lifecycle operations still keep
the old physical implementation reachable. As in the preceding migration
checkpoints, unchanged reachability means there is no defensible cold-build or
RSS claim yet. Move those remaining coarse families, make the old provisioned
physical path compile-time unreachable, and then perform the next clean ARM64
Linux musl `ReleaseFast` comparison.

### Phase 2k document-artifact manifest owner slice

The single-manifest and manifest-list group-local reads now execute against the
resident opaque owner. Each call carries one document key, plus the artifact
name for the single lookup, and returns one complete owned manifest or list.
Nested child-range descriptors, the raw manifest, and optional state travel in
the same response, so the ABI does not cross once per artifact or child range.

Distributed control still resolves the document key to a group, enforces the HA
gate, and acquires the Raft-readable lease before entering the kernel. The
kernel performs the named lookup or artifact-prefix scan against the already
open DB. A missing named manifest remains an optional miss rather than a
generic kernel failure; listing a document with no manifests remains a valid
empty list.

The internal owner ABI is now version 5. The new operations reuse the ordinary
synchronous JSON request envelope and the established internal HTTP manifest
response shape. The response parser is shared with remote group reads, while
the kernel encoder serializes the storage-owned manifest types directly. This
keeps one ownership/deallocation path for every nested allocation and avoids a
second compiled protocol model.

Validation at this checkpoint:

- provisioned cross-archive owner suite: 6/6 passed with zero leaks, generating
  a real document-extraction manifest and round-tripping its summary, nested
  child ranges, raw manifest, state, list form, and missing-manifest result
  while retaining one live owner;
- opaque owner ABI suite: 2/2 passed with zero leaks, including both new
  invalid-ABI entry points;
- focused table-read and distributed-graph suite: 25/25 passed with zero leaks;
- current provisioned write/lifecycle regression set: 68/68 passed with zero
  leaks;
- existing public C API suite: 10/10 passed with zero leaks;
- linked native Debug and both C API libraries built with production LSM-only
  options and normal concurrency;
- runtime/codegen/API graph gates passed and all 13 analyzer tests passed; and
- production-LSM native artifacts contained no LMDB globals, internal
  owner/runtime symbols remained hidden from the shared C API, and public
  `antfly_db_open` and `antfly_db_close` remained present.

Decision: keep this slice. It removes the final document-artifact read callbacks
from the future owner-attached local path. Runtime/status inspection and the
minimum writer lifecycle needed to publish, retire, and replace resident owners
remain before the legacy physical source can become compile-time unreachable.
Migrate those ownership families next; do not claim a cold-build or RSS change
until the old source is actually unreachable and the ARM64 Linux musl
`ReleaseFast` comparison is rerun.

### Phase 2l resident runtime-status and retirement foundation

Runtime inspection now crosses the same resident owner used by batch and local
queries. One owner call captures the complete `LocalTableRuntimeStatus`,
including the consistent DB statistics and LSM maintenance/write snapshot. The
consumer parses the response in a temporary arena, clones it into the caller's
allocator using the existing runtime-status ownership rules, and supplies the
outer group identity, monotonic observation timestamp, freshness, and visible
root generation. This preserves the detailed status contract without exposing
DB or LSM layouts in the C ABI.

The internal owner ABI is now version 6. `ProvisionedKernelOwnerSource` also has
an explicit table-retirement operation. Retirement marks every matching owner
before closing it: an inactive owner closes immediately, while an active owner
remains valid until its last coarse-operation lease drains and then closes on
release. A later operation opens the winning catalog generation and identity.
Descriptor changes observed during acquisition follow the same path, rather
than leaving a stale owner resident or replacing one beneath an active call.

Validation at this checkpoint:

- provisioned cross-archive owner suite: 6/6 passed with zero leaks, including
  a full resident status round trip, explicit retirement, physical close, and
  successful reopen through the same read source;
- opaque owner ABI suite: 2/2 passed with zero leaks, including status capture
  after a batch and the new invalid-ABI entry point;
- current provisioned write/lifecycle regression set: 68/68 passed; and
- existing public C API suite: 10/10 passed.

Decision: keep this foundation, but do not attach it globally yet. Raft apply
currently enters `applyPreparedReplicatedBatchGroupLocal` directly so it can
avoid remote catalog reads while applying a committed transition. Attaching
the query owner before that path has its own coarse kernel operation would
create two competing writer owners for one group. The next slice must add a
prepared replicated-apply entry point and a deterministic local open descriptor
to the Raft envelope, then attach one owner to both Raft apply and query/status
composition. Structural notifications can retire that owner only after the
outer generation/catalog transition has published. The legacy physical source
remains reachable, so this checkpoint still makes no cold-build or RSS claim.

### Phase 2m deterministic prepared Raft-apply owner slice

Committed document batches can now enter the compiled storage owner without a
catalog read from the Raft apply thread. The leader captures a deterministic
open descriptor after local-leader admission and before taking the Raft mutex.
That descriptor carries the physical root generation, table/shard/range
identity, schema, and index contract in the replicated envelope. Every replica
therefore opens the same owner incarnation while replaying the same command;
apply does not depend on remote metadata availability or on mutable catalog
state observed after commitment.

The descriptor contract lives in a small module with no DB, catalog, or runtime
imports. The internal owner ABI is now version 7 and exposes one coarse
replicated-batch operation. It deliberately calls `DB.batchReplicatedApply`,
not the ordinary client batch entry point, so replicated apply retains its
forced write durability, split-replication semantics, and absence of leader-side
HA/range admission. The Raft apply state machine owns the experimental owner
source and supplies the provisioned generation source when composition is
available. The legacy architecture remains byte-compatible: envelopes without
the descriptor still decode, and experiment-disabled data artifacts compile
without referencing any hidden owner symbol.

Validation at this checkpoint:

- deterministic descriptor envelope test: 1/1 passed with zero leaks;
- focused leaderless forwarding and default WAL-backed data-Raft tests: 2/2
  passed with zero leaks, including the experiment-disabled link boundary;
- provisioned cross-archive owner suite: 6/6 passed with zero leaks, including
  a replicated update through the descriptor-driven owner;
- opaque owner ABI suite: 2/2 passed with zero leaks, including the new
  invalid-ABI entry point;
- current provisioned write/lifecycle regression set: 68/68 passed with zero
  leaks;
- existing public C API suite: 10/10 passed with zero leaks;
- linked native Debug executable built with the experiment, production LSM-only
  options, and normal concurrency; and
- runtime/codegen/API graph gates and all 13 analyzer tests passed.

Decision: keep the prepared-apply slice, but do not attach the owner as the
universal production source yet. Three physical mutation paths can still open a
competing legacy writer: post-apply sync, Raft snapshot installation, and HA
record apply. Migrate those as coarse owner/lifecycle operations before making
the old source compile-time unreachable. Transaction-participant callbacks are
the subsequent write-family boundary.

The full schema and index JSON currently travels in every experimental Raft
batch. That is the simplest replay-independent proof of deterministic opening,
not the intended permanent wire format. Before enabling this architecture by
default, measure representative envelope/log amplification and replace repeated
contracts with a versioned descriptor reference only after provisioning and
Raft snapshots durably carry the referenced contract. Do not trade deterministic
replay for a leader-local or catalog-dependent cache.

### Phase 2n replicated sync and HA-apply owner slice

Post-commit durability waiting and standby HA replay now reuse the resident
compiled storage owner. A Raft proposer that requested `full_text`,
`enrichments`, or `full_index` waits on the DB that applied the committed batch
instead of reopening the same physical root through the legacy writer cache.
`propose` and `write` preserve their existing no-wait behavior. When the data
Raft apply composition owns the experimental source, routed HA records enter
that owner as well; minimal compositions without a data-Raft apply state retain
the legacy fallback until owner composition becomes unconditional.

The internal owner ABI is now version 8. Sync uses a fixed scalar enum and HA
uses a borrowed record envelope containing the stable kind/codec values,
identity, timeline, LSN fields, and payload. No per-document callback or JSON
translation was introduced. The kernel reconstructs the storage-owned record
view and calls `DB.applyHAReplicationRecord`, preserving idempotent applied-LSN
handling and the existing mutation/metadata/derived-effect semantics.

Validation at this checkpoint:

- provisioned cross-archive owner suite: 6/6 passed with zero leaks, exercising
  replicated apply, full-index sync, and an HA checkpoint through one resident
  owner and retaining exactly one owner entry;
- opaque owner ABI suite: 2/2 passed with zero leaks, including both new
  invalid-ABI entry points;
- current provisioned write/lifecycle regression set: 68/68 passed with zero
  leaks;
- existing public C API suite: 10/10 passed with zero leaks;
- experiment-disabled WAL-backed data-Raft link/runtime test: 1/1 passed with
  zero leaks;
- linked native Debug executable built with the experiment, production LSM-only
  options, and normal concurrency; and
- runtime/codegen/API graph gates and all 13 analyzer tests passed.

Decision: keep this slice. Prepared Raft apply, its requested derived-visibility
wait, and routed standby replay can now share one writer. Raft snapshot
installation is the remaining competing physical writer in the immediate
replication pipeline and must move behind an owner lifecycle operation before
query/read composition becomes unconditional. The legacy implementation graph
is still reachable, so this checkpoint makes no cold-build or RSS claim.

### Phase 2o two-phase Raft-snapshot publication owner slice

Raft snapshot materialization and physical generation exchange now run behind
the compiled storage owner. The boundary is deliberately two phase. Distributed
control first captures the table/range/catalog contract, reserves the visible
root generation, and asks the kernel to build and seal an isolated candidate.
After group activity drains, control retires the resident owner and asks the
kernel to atomically publish the prepared candidate. The candidate remains
unservable while control performs the linearizable catalog publication check;
the kernel then either commits the exchange or rolls the old generation back
into place.

This split keeps the ownership boundary honest:

- the storage kernel owns staged DB creation, bounded document chunks, schema
  validation, local schema/index installation, durable DB/index sync, sealing,
  namespace exchange, commit, rollback, and destruction;
- distributed control owns group admission, catalog identity and range checks,
  generation reservation, cache invalidation, structural notifications, and
  the linearizable publication fence; and
- one opaque prepared-snapshot handle spans the publication decision. No DB,
  staged-generation, or lifecycle type crosses the compiled boundary.

The internal owner ABI is now version 9. Its prepare request borrows the path,
table, deterministic identity/generation descriptor, schema/index contract,
and complete encoded Raft snapshot for one synchronous preparation call.
Publish returns only the durability-uncertain bit. Commit, rollback, and destroy
operate on the opaque handle. This is one call per snapshot phase, not one call
per document or index posting.

The cross-archive regression exercises both terminal paths. An accepted
snapshot retires the prior resident owner, publishes only the replacement
document set, advances the outer generation reservation once, and reopens the
new generation through the same read source. A deliberately rejected catalog
fence publishes and then rolls back a second candidate, leaves the generation
counter unchanged, and reopens the previously accepted snapshot with the
rejected document absent.

Validation at this checkpoint:

- provisioned cross-archive owner suite: 6/6 passed with zero leaks, including
  snapshot commit, catalog-fenced rollback, owner retirement/reopen, and outer
  generation reservation checks;
- opaque owner ABI suite: 2/2 passed with zero leaks, including invalid-version
  prepare and null publish/commit/rollback/destroy cases;
- experiment-disabled provisioned write/lifecycle suite: 68/68 passed with zero
  leaks, retaining both legacy snapshot publication and rollback regressions;
- existing public C API suite: 10/10 passed with zero leaks;
- linked native Debug built with normal concurrency for both production
  LSM-only and LMDB-compatibility configurations;
- experiment-disabled default WAL-backed data-Raft test: 1/1 passed with zero
  leaks;
- runtime/codegen/API graph gates and all 13 analyzer tests passed; and
- both native C API libraries linked against the experimental storage artifact
  with public `antfly_db_open`/`antfly_db_close` globals, while internal owner
  and snapshot symbols and LMDB/backend-LMDB globals remained hidden or absent.

Decision: keep this slice. The immediate Raft replication pipeline can now use
one physical writer for deterministic batch apply, derived-visibility waits,
HA replay, and snapshot replacement. This remains experimental rather than
default-enabled: the prepared staging DB still needs the production owner
environment for backend runtime and optional provider/secret/remote-content
hooks before every managed index configuration is equivalent to the legacy
open path. Query/read composition must also attach to the same owner
unconditionally and make the old provisioned physical path compile-time
unreachable before the next cold ARM64 Linux musl `ReleaseFast` comparison.
Until then, this checkpoint makes no compiler-time, RSS, or graph-reduction
claim.

### Phase 2p process-owner attachment and read fallback removal

`DataServer` now owns one heap-stable `ProvisionedKernelOwnerSource` and lends
it to API reads, direct writes, Raft apply, HA replay, snapshot publication, and
warmup. In the experiment, provisioned and hosted group-local reads fail closed
when that owner has not been attached; they no longer retain a physical DB
fallback. The legacy resident DB source is disabled in the same composition.

Two cold profiles showed that this was necessary ownership work but not yet a
compiler win. The first candidate measured 392.784 seconds for distributed.
The repeated owner-attached profile measured 404.195 seconds with 724
repository files and 41,964 declarations. Relative to the preceding graph it
removed only four files and 254 declarations, while the host was 6–12% slower
across independent units. No timing claim is therefore attributable to the
change.

Decision: keep the single-owner composition as a bounded prerequisite, but
classify the compiler result as **revise**. Local write and lifecycle functions
remain roots through the provisioned source vtable, so attaching the owner is
not equivalent to removing the old implementation.

### Phase 3a transaction-participant owner slice

The internal owner ABI is now version 10 and exposes begin, prepare, resolve,
and status as one coarse call per transaction phase. Recovery deliberately
calls back into distributed control using opaque participant identifiers;
routing and coordination do not move into the storage kernel. Metadata uses a
remote transaction source, while provisioned local participants use the
resident owner under the experiment.

Focused cross-archive owner tests passed 6/6 with zero leaks, the owner ABI
tests passed 2/2, the default public C API passed 10/10, and the production
write regression suite passed 68/68. The cold ARM64 profile was:

| Unit | Compiler time | LLVM emit | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|
| API kernel | 142.441 s | 138.184 s | 17,241 | 325 |
| Distributed | 405.103 s | 396.328 s | 41,967 | 724 |
| Storage kernel | 236.926 s | 231.085 s | 26,544 | 395 |
| Inference | 239.373 s | 231.613 s | 24,945 | 523 |

The distributed graph was exactly the same 724 files as the preceding profile
and added only three declarations. Transaction participation is therefore a
correct architectural prerequisite, not a standalone build-time win.

Decision: keep it in the Phase 3 candidate, but **revise and continue** rather
than enabling the experiment or claiming success.

### Rejected Lite command-dispatch probe

Moving the offline `antfly lite` command entry into the storage archive while
leaving `lite serve` in distributed produced this cold comparison:

| Unit | Before | Probe | Delta |
|---|---:|---:|---:|
| Distributed | 405.103 s | 399.933 s | -5.170 s |
| Storage kernel | 236.926 s | 244.894 s | +7.968 s |

Distributed lost only two net repository files and 405 declarations, while
storage gained 25 files and 592 declarations. Standalone still directly owns
the Lite backend, so moving command dispatch cannot remove that implementation
from the distributed graph.

Decision: **revert** the probe. The result does not meet the go/no-go criteria
and confirms that the next Lite cut must move physical ownership, not CLI
dispatch.

### Phase 3b metadata remote-only write-source probe

Metadata/API proxy processes never own a data-group DB. Their hosted write
source now reflects that fact in its vtable: local schema, batch, transaction,
artifact, and runtime-status callbacks are absent. Required top-level batch and
artifact operations retain routing and HTTP fanout, with compile-time-specialized
local/no-route cases returning unhandled. This is a correctness boundary as
well as a graph experiment; an impossible metadata-local branch can no longer
silently open storage.

The linked native experiment built successfully. The focused cross-archive
suite passed 6/6 with zero leaks, the normal production write suite passed
68/68 with zero leaks, and all 26 cold ARM64 build steps succeeded. The exact
cold comparison was:

| Unit | Phase 3a | Remote-only | Graph/declaration result |
|---|---:|---:|---|
| API kernel | 142.441 s | 136.014 s | identical graph and declarations |
| Distributed | 405.103 s | 385.637 s | identical 724-file graph; -32 declarations |
| Storage kernel | 236.926 s | 227.861 s | identical graph and declarations |
| Inference | 239.373 s | 230.751 s | identical graph and declarations |

Every independent unit improved by roughly 4–6%, identifying host variance
rather than a 19.466-second distributed win. Distributed retained all 724
repository files and 1,053,172 repository source lines.

Decision: **revise**. Retain this only within the uncommitted Phase 3 candidate
because it is the correct metadata contract and a prerequisite to the next
cut; it does not earn acceptance on performance. The next measured slice must
move complete provisioned physical write/lifecycle operations behind the owner
and make their legacy implementations compile-time unreachable. If the
combined slice still leaves the graph unchanged, revert the candidate rather
than attributing host variance to the architecture.

### Rejected Phase 3 transaction and artifact-family candidate

The final Phase 3 probe completed the document-artifact family behind the live
storage owner. Reprocess-one, reprocess-range, issue listing, child-range
placement, child-range batch application, and complete repair execution each
crossed the archive boundary as one coarse operation. Repair retained routing,
admission, scheduling policy, and result notifications in distributed control;
the owner received a versioned options record plus synchronous cancellation,
yield, activation, and capacity callbacks. No per-document or storage-engine
call crossed the ABI.

Behavioral validation was successful:

- the cross-archive owner suite passed 6/6 with zero leaks, including actual
  repair execution and cancellation propagation;
- the owner ABI suite passed 2/2 with future-version, malformed-request, and
  status-translation coverage;
- the production write/lifecycle suite passed 68/68 with the experiment both
  disabled and enabled;
- the public C API suite passed 10/10 with zero leaks;
- the linked native Debug executable, all graph gates, and all 13 analyzer
  tests passed; and
- the release artifacts remained one stripped static ARM64 executable plus
  16.394 MB C API libraries. Internal owner symbols remained hidden and no
  LMDB implementation symbol or path appeared in the production artifacts.

The cold ARM64 Linux musl `ReleaseFast` profile used fresh local and global
caches, production LSM-only mode, and normal concurrency:

| Unit | Compiler time | LLVM emit | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|
| API kernel | 137.084 s | 132.976 s | 17,241 | 325 |
| Distributed | 397.836 s | 389.286 s | 41,961 | 724 |
| Storage kernel | 233.884 s | 228.110 s | 26,779 | 395 |
| Inference | 233.189 s | 225.572 s | 24,945 | 523 |

The authoritative graph result was unchanged: 1,967 repository-file instances,
1,187 unique files, 780 duplicate instances, and 474 duplicated files across
the four units. Distributed remained at the same 724 files and gained 25
declarations versus the preceding artifact profile; storage remained at 395
files and gained 189 declarations. All independent units were 3--7% slower
than that profile, so the wall-time change is host variance rather than an
architectural result.

Decision: **revert** the complete uncommitted Phase 3 candidate, including the
Phase 2p owner attachment that was retained only as its prerequisite. The
experiment proved the ABI and ownership mechanics but not the compiler result.
Adding operation families one at a time cannot pay while the distributed unit
still roots the complete provisioned write/lifecycle implementation through
the same broad source and vtable composition. The next viable probe must remove
that implementation root as one atomic cut--for example by separating control
and legacy physical provisioned sources into different modules and making the
entire physical module unreachable in the experimental distributed artifact--
before adding more owner operations.

### Rejected standalone plus storage-kernel codegen island

A follow-up probe moved the complete standalone/Lite runtime entry point from
the distributed unit into the experimental storage-kernel unit. This retained
four concurrent units, one final static executable, and the same linked
embedded-inference host. It specifically avoided the previously measured
six-minute standalone-only unit by co-generating standalone with storage that
the C API already required.

The cold ARM64 Linux musl `ReleaseFast` result initially looked attractive:

| Unit | Compiler time | Declarations | Repository Zig files |
|---|---:|---:|---:|
| API kernel | 135.427 s | 17,241 | 325 |
| Distributed | 382.842 s | 41,258 | 716 |
| Storage + standalone | 314.078 s | 36,033 | 608 |
| Inference | 230.540 s | 24,945 | 523 |

Distributed lost 8 files, 703 declarations, and approximately 15 seconds while
storage remained off the critical path. The complete result moved in the wrong
direction, however. Aggregate analyzed file instances rose from 1,967 to 2,172
and duplicate instances rose from 780 to 985. The storage unit gained 213 files
while distributed lost only 8. The stripped C API grew only 0.139 MB, but the
final static executable grew from 72.290 MB to 79.008 MB because standalone's
newly rooted storage/control code duplicated implementations still required by
the data runtime.

Decision: **revert**. Reducing one unit's latency by increasing total LLVM work,
cross-unit duplication, and final executable size is not the target
architecture. Standalone can move out of distributed only after it consumes a
genuinely shared compiled storage/control kernel; co-location with either side
before that ownership cut merely moves and duplicates the graph.

### Serverless API-protocol placement

The complete serverless command/runtime now shares the API-kernel codegen
island instead of distributed control. This matches ownership: serverless owns
HTTP adaptation, request validation/translation, and local query planning; it
does not own cluster topology, Raft coordination, or distributed fanout. The
move adds no compiler unit and changes no runtime ABI--the final executable
still calls the same hidden `antfly_runtime_serverless` entry point.

The cold ARM64 Linux musl `ReleaseFast` build used fresh caches, production
LSM-only mode, and normal concurrency:

| Unit | Compiler time | LLVM emit | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|
| API + serverless | 212.663 s | 207.160 s | 23,003 | 501 |
| Distributed | 361.564 s | 353.401 s | 39,686 | 641 |
| Storage kernel | 233.495 s | 227.935 s | 26,489 | 395 |
| Inference | 235.522 s | 227.951 s | 24,945 | 523 |

Relative to the immediately preceding four-unit baseline, distributed lost 83
repository files and 2,275 declarations and improved from 397.836 seconds to
361.564 seconds. Storage was effectively unchanged and inference was slightly
slower, so the 36.272-second critical-path reduction is not host-wide variance.
The API unit remains far off the critical path.

The tradeoff is explicit: aggregate file instances increased from 1,967 to
2,060 and duplicate instances from 780 to 873 because serverless shares some
storage/search implementation with distributed. Unlike the rejected
standalone/storage placement, this did not duplicate final executable code in
a harmful way: the stripped static executable decreased from 72.290 MB to
72.121 MB and both stripped C API libraries decreased to 16.382 MB. The C API
exports remain limited to public symbols and production artifacts contain no
LMDB implementation symbol or path.

Validation included the linked native executable with the experiment both
enabled and disabled, `serverless --help`, 42/44 serverless tests passing with
the two opt-in cloud integrations skipped, 5/5 linked-main tests, 10/10 C API
tests, all graph gates, and all 13 analyzer tests.

Decision: **keep**. This is a material, correctly-owned critical-path win that
meets the 380-second target without increasing runner claims or artifact size.
At this checkpoint the opt-in four-unit profile still duplicated storage. The
next required experiment was to remeasure the default compile-once topology
before continuing the opaque storage ownership cut; the result follows.

### Repeated compile-once control after serverless placement

The serverless/API placement invalidated the 425.652-second baseline that had
motivated a separately compiled storage kernel. The next experiment therefore
disabled `-Dstorage-kernel-experiment` and let the existing PIC distributed
archive own the C API exports again. The executable and both C API shared
libraries link that exact archive; section GC retains only public C API roots in
the shared libraries. No source behavior or runtime call path changes at this
boundary.

Two builds used separate fresh local and global caches, targeted ARM64 Linux
musl `ReleaseFast`, and retained normal build concurrency:

| Unit | Cold run 1 | Cold run 2 | LLVM emit (run 2) | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|---:|
| API protocol + serverless | 205.932 s | 203.586 s | 198.835 s | 23,003 | 501 |
| Distributed + storage + C API exports | 358.589 s | 352.900 s | 345.385 s | 40,313 | 643 |
| Inference | 228.355 s | 224.106 s | 217.117 s | 24,945 | 523 |

Both clean builds completed all 23 steps. The combined unit had an identical
graph in both runs: 643 repository files, 40,313 declarations, 22,717 generic
instances, and 14,731 inline calls. Its 352.9--358.6 second range is below the
380-second gate and close to the preferred 350-second target. This is not a
cache-assisted result.

Compared with the immediately preceding four-unit serverless/API profile:

| Metric | Separate storage experiment | Compile-once control | Delta |
|---|---:|---:|---:|
| Critical compiler unit | 361.564 s | 352.900--358.589 s | within host variance, still below 380 s |
| Compiler units | 4 | 3 | -1 |
| Repository-file instances | 2,060 | 1,667 | -393 |
| Unique repository files | 1,187 | 1,185 | -2 |
| Duplicate instances | 873 | 482 | -391 (-44.8%) |
| Static executable | 72,121,032 bytes | 60,803,592 bytes | -11,317,440 bytes (-15.7%) |
| `libantfly.so` | 16,382,112 bytes | 16,382,112 bytes | unchanged |

The API and inference controls were 3.2% and 3.0% faster than the preceding
profile, while the combined unit was only 0.8% faster despite gaining the C API
roots. Some wall-time improvement is host variance, but the graph, unit-count,
duplicate-work, and artifact-size improvements are structural. Production
artifacts contain no `mdb_*`, `lmdb_backend`, or `backend_lmdb` implementation
symbols. The shared library exports the public `antfly_db_*`/`antfly_lite_*`
surface and no distributed runtime or experimental owner symbols.

Validation of the unchanged production path included the two cold linked
builds, 42/44 serverless tests with only the two opt-in cloud integrations
skipped, 5/5 linked-main tests, 10/10 C API tests, all graph gates, and all 13
analyzer tests. The final product remains one static executable with embedded
standalone inference and normal concurrency.

Decision at that checkpoint: **keep the default compile-once topology and stop
the separate opaque-storage-kernel migration**. The experiment answered its question: after
the inference and serverless graph cuts, a dedicated storage compiler unit no
longer improves the critical path and instead duplicates nearly the entire
storage graph. The retained owner ABI work remains useful design evidence and
an opt-in diagnostic, but it is not the production direction. Further work
should reduce roots inside the three accepted units without introducing a
fourth copy of storage or a process-wide internal RPC ABI. These local cross
builds establish repeatability and graph shape; the overall goal remains open
until the same cold production path is reliable within the normal Linux
runner's memory budget.

### Post-main normal-runner control

After merging current `main`, GitHub Actions runs `31281549097` and
`31283050205` exercised the default compile-once topology on the normal
`arc-antfly-publish` runner. Both used a clean cache, ARM64 Linux musl
`ReleaseFast`, the production LSM-only feature set, normal build concurrency,
and the existing 24 GiB pod request. Neither added swap, a larger runner, or
serialized compilation.

The archive build succeeded and all artifact gates passed:

| Unit or artifact | Run 1 | Run 2 |
|---|---:|---:|
| API protocol + serverless | 5 m, 6 GB MaxRSS | 6 m, 6 GB MaxRSS |
| Distributed + storage + C API exports | 9 m, 10 GB MaxRSS | 11 m, 10 GB MaxRSS |
| Inference | 6 m, 6 GB MaxRSS | 7 m, 6 GB MaxRSS |
| Thin final executable link | 10 s, 427 MB MaxRSS | completed |
| Complete archive build | 12:28.74, 10,259,540 KiB peak RSS | 14:45.44, 10,299,704 KiB peak RSS |
| Swap | zero | zero |
| Static executable | 61,223,736 bytes | 61,223,736 bytes |
| `libantfly.so` | 16,669,872 bytes | 16,669,872 bytes |

These are two independent authoritative proofs that the accepted three-island
architecture completes the original failing build on the normal runner after
the `main` merge. They establish reliability at the existing cost, but they do
not satisfy the performance goal: the application/storage unit varies from
nine to eleven minutes, well above the 380-second gate.

The run also found a measurement-infrastructure defect. The API unit peaked at
6.32 GB while its build-step scheduling claim was still 5 GiB. Zig therefore
reported the first 5m41s API attempt as exceeding its declared upper bound and
discarded it before scheduling a successful retry. The claim is now 7 GiB.
Together with the application/storage unit's 11 GiB claim, the initial group
remains within the existing 20 GB Zig scheduling budget and the runner's 4 GiB
reserve. This corrects scheduling metadata; it does not increase runner cost,
add memory, serialize compilation, or weaken a product boundary.

Decision: **keep the API scheduling correction, but reopen the compilation
architecture decision and continue the goal loop**. The repeated runner result
invalidates the assumption that the 352.9--358.6-second local compile-once
control was sufficient as the final architecture.

### Control/storage root-isolation probes

Fresh local ARM64 Linux musl `ReleaseFast` probes used separate local and
global caches, production LSM-only mode, normal optimization, and real emitted
PIC archives. A capture threshold ignored early build-script/sema-only reports.

| Probe | Compiler time | LLVM emit | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|
| Current combined application/storage | 370.856 s | 362.486 s | 40,580 | 645 |
| Data + CAPI only, PIC | 283.018 s | 277.375 s | 36,065 | 593 |
| Data + standalone/Lite + restore staging + CAPI, PIC | 288.171 s | 282.314 s | 37,166 | 614 |
| CLI + metadata control only | 295.647 s | 289.733 s | 35,745 | 600 |

Standalone/Lite, restore staging, and the public CAPI add only 5.153 seconds to
the data/storage unit. Removing CLI and metadata from the current combined
unit removes 31 repository files, 40,542 repository source lines, 3,414
declarations, and 82.685 seconds. This is a real root-emission effect, not a
PIC penalty.

A four-unit source-only split would cap each isolated local unit below five
minutes, but it raises aggregate duplicate repository-file instances from 484
to 1,053. Co-generating CLI/metadata with API/serverless reduces that overlap,
but fails the go/no-go gates:

| Three-unit coalescing candidate | Result |
|---|---:|
| API/serverless + CLI/metadata | 409.246 s, 400.956 s LLVM, 725 repository files |
| Data + standalone/Lite + CAPI | 313.292 s under concurrent load |
| Inference | 226.252 s |
| Aggregate duplicate instances | 676, up from 484 |
| Static executable | 72,464,376 bytes, up 18.4% from 61,223,736 bytes |

Decision: **revert the source-only coalescing topology and resume the opaque
storage-owner experiment at an atomic physical-source cut**. Repartitioning
roots proves that a sub-five-minute shape exists, but it recompiles the same
storage implementation and carries the duplicate code in the final executable.
The next viable change must make both legacy provisioned read and write/
lifecycle implementations compile-time unreachable from control consumers;
adding more individual ABI operations while either physical fallback remains
reachable is not a valid measurement checkpoint.

### Remote-CLI cut and application/storage unit

The next root audit found a smaller atomic cut than API coalescing. Remote CLI
commands need HTTP clients and public wire types, but local HA owns WAL/LSM
operator state and restore staging already belongs with standalone/Lite. Moving
local HA into the application/storage artifact left `cli_runtime.zig` with only
remote/client commands. Metadata remained with data because the earlier
data-plus-metadata measurement showed only a 5.3-second marginal cost.

The resulting production layout was measured from fresh local and global
caches on the same macOS cross-compilation host, targeting ARM64 Linux musl
`ReleaseFast`, production LSM-only features, PIC application storage, and
normal build concurrency:

| Unit | Compiler time | LLVM emit | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|
| Application/storage: data, metadata, standalone/Lite, HA, restore, C API | 340.130 s | 332.415 s | 40,037 | 636 |
| API protocol plus serverless | 206.191 s | 200.895 s | 23,094 | 502 |
| Inference | 226.166 s | 218.664 s | 24,971 | 524 |
| Remote CLI | 38.029 s | 35.904 s | 5,804 | 53 |

The application/storage unit is 30.726 seconds faster than the comparable
370.856-second combined-root probe and lands below the preferred 350-second
gate. The CLI graph contains no file under `pkg/antfly/src/storage`; its 53
repository files are predominantly HTTPX, generated public API types, and
command parsing. Across all four reports there are 1,715 repository-file
instances, 1,188 unique files, and 527 duplicate instances. API/serverless
still duplicates 73 storage files with the application unit, so this is a
material partition improvement rather than the final physical compile-once
state.

After adding deterministic memory-group dependencies, a second genuinely cold
build used the patched Zig 0.16 runner and the production 20,971,520,000-byte
`--maxrss` budget. All 30 release steps completed in 374.23 seconds real time
(703.98 seconds user, 29.44 seconds system). Zig's per-compiler accounting
reported 5 GB MaxRSS for application/storage, 4 GB for API, 4 GB for inference,
and 1 GB for CLI. The host `time -l` process-tree RSS field was unavailable in
the sandbox, so these are Zig's rounded child-process peaks rather than a claim
about aggregate host residency. The artifacts were:

| Artifact | Size | Change from post-main normal-runner control |
|---|---:|---:|
| Static ARM64 `antfly` | 62,421,144 bytes | +1,197,408 bytes (+2.0%) |
| `libantfly.so` | 16,664,880 bytes | -4,992 bytes |
| Application/storage archive | 37,225,628 bytes | not directly retained in the earlier runner artifact |
| API archive | 20,070,070 bytes | not directly retained in the earlier runner artifact |
| Inference archive | 22,805,418 bytes | not directly retained in the earlier runner artifact |
| CLI archive | 2,927,196 bytes | new |

The executable is an AArch64 ELF executable with no dynamic section. The
shared C API exports its public `antfly_db_*` and `antfly_lite_*` surface while
retaining no global runtime, API-kernel, standalone-inference, restore, or
internal storage-owner symbols. Neither artifact contains production
`mdb_env`, `lmdb_backend`, or `backend_lmdb` implementation markers.

The release scheduler keeps the existing 20 GB Zig budget and 4 GB runner
reserve. The launch graph is deliberate: API (7 GiB claim) and application/storage
(11 GiB) form the initial group; an explicit compile dependency admits
inference (8 GiB) after API completes, while another admits the short CLI
(3 GiB) after application/storage completes. Explicit edges are necessary
because Zig 0.16 randomizes dependency traversal; enum or link order is not a
scheduling contract. This preserves overlap of the two long paths instead of
allowing the CLI to delay application admission. The local profile validates
compiler shape and artifacts; the normal Linux runner must still confirm the
claimed schedule, peak RSS, repeatability, and end-to-end wall time.

This increment also adds ABI version 10's process-scoped opaque owner context.
It owns the shared physical resource manager and decoded-index caches, and
group owners borrow it for their entire lifetime. Context destruction returns
`busy` until all owners drain. The cross-archive owner suite exercises two
owners sharing the context and verifies destruction ordering. While validating
that path, the batch-wire audit found a stale encoder guard that rejected graph
mutations even though the encoder already preserves them; the guard now only
rejects a bare predicate without its required transaction.

Validation for the accepted source change includes 3/3 opaque-owner ABI tests,
6/6 cross-archive provisioned-owner tests, 69/69 broad provisioned write and
lifecycle tests in the default production topology, 5/5 linked-main tests,
native `ha`, `table`, and `lite` command smokes, all runtime/codegen/API graph
gates, and all 13 graph-analyzer tests. The broad suite is not an acceptance
claim for globally enabling the older separate-kernel experiment: with that
option enabled, its two pre-existing Raft-snapshot fixtures still omit the
experimental storage-snapshot source and fail closed with
`StorageKernelSnapshotUnavailable`.

Normal-runner run `31287672283` then exercised the exact pushed topology on the
existing 24 GiB `arc-antfly-publish` runner. The graph gates, cold archive,
artifact audit, memory report, and upload all passed:

| Unit or artifact | Normal-runner result |
|---|---:|
| Application/storage | 11 m, 10 GB MaxRSS |
| API protocol plus serverless | 6 m, 6 GB MaxRSS |
| Inference | 8 m, 6 GB MaxRSS |
| Remote CLI | 1 m, 1 GB MaxRSS |
| Thin executable link | 11 s, 420 MB MaxRSS |
| Complete cold archive build | 15:17.77, 9,880,700 KiB process-tree peak RSS |
| Static executable | 62,421,080 bytes |
| `libantfly.so` | 16,664,864 bytes |

This is a third clean normal-runner success without swap, a larger runner, or
`-j1`, and the explicit dependency edges prevent random launch-order changes.
It does not improve the authoritative Linux critical unit: application/storage
remains at the slower end of the preceding 9--11-minute range, and total wall
time is comparable to the preceding 14:45 control.

Decision: **keep the remote-CLI ownership cut and process-owner foundation, but
do not treat them as the performance solution**. They preserve reliability,
remove all Antfly storage files from the CLI graph, keep artifacts within their
budgets, and are bounded prerequisites for the remaining cut. The goal remains
open. The next experiment should move serverless local execution from API into
the application/storage owner: API currently duplicates 73 storage files,
while the earlier data-plus-serverless probe measured only a 15.5-second
marginal cost when serverless was co-generated with data. If that does not make
the physical storage implementation compile once, the follow-up is the coarse
opaque owner ABI—not separate HTTPX, LMDB, or primitive storage libraries.

### Serverless local-runtime ownership cut

The post-CLI compiler reports made the remaining serverless tradeoff different
from the earlier placement decision. Serverless has public HTTP adaptation, but
its command also owns complete local query, maintenance, artifact, WAL, and
segment runtimes. Keeping that complete local executor in the API artifact
made API emit 73 storage files already present in application/storage. The
experiment therefore left the public API kernel separate while moving the
hidden `antfly_runtime_serverless` entry point and its implementation into the
application/storage artifact.

A fresh ARM64 Linux musl `ReleaseFast` build used cold local/global caches, the
patched 20 GB scheduler, production LSM-only features, and normal concurrency.
All four compiler reports were captured from the authoritative Zig time-report
protocol:

| Unit | Before | After | Change |
|---|---:|---:|---:|
| Application/storage compiler | 340.130 s | 361.270 s | +21.140 s |
| Application LLVM emit | 332.415 s | 353.873 s | +21.458 s |
| Application declarations | 40,037 | 42,512 | +2,475 |
| Application repository files | 636 | 723 | +87 |
| API compiler | 206.191 s | 125.604 s | -80.587 s |
| API LLVM emit | 200.895 s | 122.103 s | -78.792 s |
| API declarations | 23,094 | 17,332 | -5,762 |
| API repository files | 502 | 326 | -176 |

The critical application unit remains below the 380-second gate. Aggregate
repository-file instances fell from 1,715 to 1,626 and duplicate instances
fell from 527 to 438. Duplicated storage instances fell from 75 to 52; API's
storage-file set fell from 73 to 50. The remaining files include real DB,
algebraic planner, query executor, backend, maintenance, resource-manager, and
transaction implementations—not merely small ABI contracts—so the final
physical-owner cut is still required.

Artifact shape also improved:

| Artifact | Before | After | Change |
|---|---:|---:|---:|
| Static ARM64 `antfly` | 62,421,144 bytes | 59,270,808 bytes | -3,150,336 bytes (-5.0%) |
| `libantfly.so` | 16,664,880 bytes | 16,675,344 bytes | +10,464 bytes (+0.06%) |

The local compiler summary reported application/storage at 6 GB MaxRSS, API
and inference at 3 GB, and CLI at 2 GB, all within their existing claims. The
executable remains static, the shared C API exports no private runtime/kernel
symbols, and neither artifact contains production LMDB implementation markers.

Validation included 42/44 serverless tests with only the two opt-in cloud
integrations skipped, 5/5 linked-main tests, 10/10 direct CAPI tests, the linked
C consumer smoke, command dispatch through `serverless --help`, all graph
gates, and all 13 analyzer tests. The direct CAPI configuration exposed a
facade omission introduced with the process owner: `capi_root.zig` did not
export `platform_sync`. Adding that focused import makes the owner context
compile in both the linked production artifact and the standalone CAPI test.

Normal-runner run `31288827248` then passed all 30 cold archive steps on the
existing 24 GiB `arc-antfly-publish` runner, again without swap, `-j1`, or a
larger machine:

| Unit or artifact | Normal-runner result |
|---|---:|
| Application/storage | 12 m, 10 GB MaxRSS |
| API protocol | 3 m, 4 GB MaxRSS |
| Inference | 8 m, 6 GB MaxRSS |
| Remote CLI | 1 m, 1 GB MaxRSS |
| Thin executable link | 10 s, 435 MB MaxRSS |
| Complete cold archive build | 13:40.16, 10,693,812 KiB process-tree peak RSS |
| Static executable | 59,270,696 bytes |
| `libantfly.so` | 16,675,328 bytes |

This reduces complete normal-runner wall time by 1:37.61 versus the preceding
15:17.77 four-unit baseline and cuts API from six to three reported minutes.
Application/storage grows from eleven to twelve reported minutes, however, so
placement alone does not satisfy the normal-runner critical-path target.

Decision: **keep the serverless physical-runtime placement**. It exchanges
21.140 seconds of local critical time for an 80.587-second API reduction, 89
fewer duplicate compiler-file instances, a 3.15 MB smaller executable, and a
repeatable end-to-end Linux win while staying within the existing memory
claims. The next experiment is no longer a placement shuffle: remove the 50
remaining physical storage files from API by routing its coarse local
operations through the existing process-scoped opaque owner context.

### Rejected exact-sort owner callback

A representative local-query callback moved the complete distributed
exact-sort merge behind the process-scoped storage owner. The ABI returned an
owned typed response, invoked the owner once per merge, and preserved the
existing result semantics. Focused validation passed 182 of 184 query/storage
tests with the two expected skips, all 25 table-read tests, the linked ARM64
production topology, and the graph-analyzer gates without leaks.

Three authoritative cold API captures nevertheless made this a no-go:

| Capture | API wall time | Repository files | Difference from 125.604 s baseline |
|---|---:|---:|---:|
| Exact-sort owner v1 | 128.865 s | 327 | +3.261 s |
| Exact-sort owner v2 | 128.169 s | 327 | +2.565 s |
| Exact-sort owner v3 | 128.414 s | 327 | +2.810 s |

The baseline had 326 repository files. The only added file was the 212-line
wire contract; `search_exec.zig`, `graph_exec.zig`, `db.zig`, and
`docstore.zig` all remained in the compiler graph. The implementation was
therefore reverted. This confirms that moving individual operations across an
opaque callback cannot pay while the API unit still imports the broad physical
provisioned-source implementation. The next probe must make that complete
implementation root unreachable as one atomic control/physical source split.

### Rejected API wire-root and durable-persistence boundary

The next slice removed production imports of the broad `table_reads.zig` and
`table_writes.zig` barrels from the internal read routes, HTTPX handler,
distributed transaction coordinator, and HTTP client. It also moved complete
artifact-reprocess, repair, and restore job KV operations behind one opaque
application/storage-owned LSM handle. Path-backed stores and Lite's in-file
runtime namespace used the same ABI; no LMDB implementation or per-record
storage primitive crossed the boundary.

The boundary was behaviorally sound. Linked production Debug completed, the
cross-archive persistence/reopen test passed 1/1 without leaks, focused
artifact/repair tests passed 25/25, public API parity passed 155/155, all graph
gates passed, and all 13 analyzer tests passed. The ABI test also caught and
fixed two prototype defects before measurement: Zig error-set integer values
are not stable across compilation units, and failed C entry points must
explicitly destroy partially initialized owners rather than rely on
`errdefer` in a `c_int`-returning function.

Authoritative cold ARM64 musl `ReleaseFast` reports rejected the combined
slice:

| Unit | Baseline | Candidate | Delta | Baseline → candidate declarations |
|---|---:|---:|---:|---:|
| API | 125.604 s | 129.435 s | +3.831 s | 17,332 → 17,343 |
| Application/storage | 361.270 s | 368.114 s | +6.844 s | 42,512 → 42,557 |
| API LLVM emission | 122.103 s | 125.833 s | +3.730 s | — |
| Application/storage LLVM emission | 353.873 s | 360.524 s | +6.652 s | — |

API removed the two barrel files and 62,329 lexical source lines, but its
authoritative repository graph changed only from 326 to 327 files: three ABI
files were added, and all 50 physical storage files representing 223,296
duplicated source lines remained. Application/storage removed no files and
grew from 723 to 726 repository files. The complete cold build still linked a
single stripped static ARM64 executable, 59,294,744 bytes versus the
59,270,696-byte baseline.

Decision: **reject and revert**. A coherent persistence abstraction is not a
compilation win while query, transaction, restore, and provisioned-source code
still make the same physical implementation root reachable. Further
micro-facades or job-store bridges should not be attempted independently. The
next representative slice must remove the complete physical provisioned
storage/local-query root from API in one atomic operation-level owner ABI; if
that cannot be made unreachable, the experiment should stop rather than add
another compiled boundary.

### Rejected canonical storage-contract extraction

A combined source-root probe moved transaction identity/status/recovery,
participant-list destruction, byte ranges, split phases, and capacity-source
contracts into one implementation-free canonical module. Transactions,
DocStore, shard, and ResourceManager re-exported those exact definitions so
type identity was preserved. In the same candidate, production
`httpx_handler.zig` imported the narrow write-source and index-normalization
modules instead of the 39,581-line `table_writes.zig` implementation barrel;
its full DB/table imports remained available to tests only.

Correctness was not the limiting factor. Linked native Debug with the
production LSM-only topology built successfully. The focused resource suite
passed 25/25, the transaction suite passed 37/37, DocStore and shard targets
completed, all graph gates passed, and all 13 analyzer tests passed.

The authoritative cold ARM64 musl `ReleaseFast` API capture rejected the
slice:

| Metric | Baseline | Candidate | Delta |
|---|---:|---:|---:|
| Compiler time | 125.604 s | 140.942 s | +15.338 s |
| LLVM emission | 122.103 s | 137.212 s | +15.109 s |
| Declarations | 17,332 | 17,334 | +2 |
| Generic instances | 11,586 | 11,586 | 0 |
| Inline calls | 5,722 | 5,722 | 0 |
| Repository files | 326 | 325 | -1 |

The candidate removed `table_writes.zig` and the 2,581-line transaction
implementation from the API report, added the 93-line contract module, and
still retained 50 files under `storage`. Exact generic and inline counts were
unchanged and declarations grew by two. The slower wall/LLVM result is host
variance, but the declaration identity is stronger evidence: removing more
than 42,000 lexically reachable lines removed no emitted work.

A symbol audit agrees with that conclusion. The baseline API object contains
only two named physical-storage globals, both sort-diagnostic state in
`search_exec.zig`; the application/storage object contains 3,285 named
DB/DocStore/LSM/transaction symbols. Compiler-file reachability is useful for
finding accidental barrels, but it is not evidence that every line in a lazy
Zig import graph is emitted or duplicated.

Decision: **reject and revert**. Do not pursue more canonical-type extraction
or import-only cleanup as a compilation optimization. The remaining useful
storage experiment must remove an owner that emits executable code--the
legacy provisioned read/write/lifecycle vtables and `ProvisionedGroupStorage`
composition--from a compiler unit. Measurements should prioritize declaration,
object, and LLVM changes over lexical source-line counts.

### Rejected isolated local-HA runtime

A follow-up tested whether local HA explained the remaining application
critical path. Two measurement-only PIC archives compiled the exact current
application/storage roots without `cmd/ha.zig`, and the HA command alone. Both
used fresh caches, ARM64 Linux musl `ReleaseFast`, production LSM-only mode,
and normal concurrent code generation.

| Unit | Compiler time | LLVM emission | Declarations | Repository files | Archive size |
|---|---:|---:|---:|---:|---:|
| Current application/storage | 361.270 s | 353.873 s | 42,512 | 723 | 40,159,728 bytes |
| Application/storage without HA | 355.926 s | 348.423 s | 42,336 | 721 | 39,858,466 bytes |
| Isolated HA | 30.525 s | 28.918 s | 7,971 | 75 | 2,623,550 bytes |

Removing HA saves only 5.344 seconds, 176 declarations, and two repository
files. The isolated archive duplicates 75 files--48 of them storage files--and
raises the two-archive total by 2,322,288 bytes before final-link section GC.
The earlier large difference between data/storage-only and combined control
roots was therefore a union effect, not an individually removable HA cost.

Decision: **reject and revert**. A 5.3-second critical-path reduction does not
justify another runtime unit, 30.5 seconds of aggregate LLVM work, or more
storage duplication. Do not separately split metadata or other small roles on
the strength of lexical graph size. The next experiment must remove the
emitting provisioned owner implementation as an atomic compiled boundary.

### Rejected control-only provisioned write-vtable probe

The next upper-bound probe enabled the existing separate storage-kernel
topology and made `ProvisionedTableWriteSource.source()` compile-time-select a
fail-closed vtable in the distributed unit. That vtable retained only the three
operations already implemented by the resident owner--top-level batch
orchestration, group-local batch, and local runtime status. Every other legacy
physical and lifecycle callback was absent. This deliberately did not claim
runtime completeness; it asked whether the broad vtable itself was the large
emitting root before implementing dozens of additional stable ABI operations.

The experiment-enabled linked native Debug build succeeded with normal
concurrency. A fresh-cache ARM64 Linux musl `ReleaseFast` build then completed
all release artifacts and produced these reports:

| Unit | Compiler time | LLVM emission | Declarations | Repository files |
|---|---:|---:|---:|---:|
| API protocol | 134.129 s | 130.653 s | 17,332 | 326 |
| Distributed/control probe | 366.523 s | 358.347 s | 41,591 | 720 |
| Separate storage kernel | 216.358 s | 211.319 s | 26,676 | 396 |
| Inference | 228.304 s | 220.871 s | 24,971 | 524 |
| Remote CLI | 36.379 s | 34.495 s | 5,804 | 53 |

The current compile-once application baseline is 361.270 seconds, 42,512
declarations, and 723 repository files. The composite probe therefore removed
only 921 declarations and three files while becoming 5.253 seconds slower.
Most of that declaration delta is the already-known movement of CAPI roots to
the separate storage artifact; the earlier Phase 1 topology alone removed 721
distributed declarations. The narrowed vtable accounts for at most the small
remainder, not a storage implementation family. API was also 6.8% slower than
its 125.604-second baseline, so the wall-time increase is partly host variance;
the declaration and object results are the decisive evidence.

The current application object contains 28,597,098 allocatable bytes and
24,395,704 text bytes. The probe distributed object contains 27,894,241 and
23,697,424 bytes respectively--only 702,857 allocatable bytes and 698,280 text
bytes less despite omitting almost the entire write vtable. That upper bound
is comparable to the 621,629 bytes attributed to `api.table_writes` in the
baseline object and leaves the multi-megabyte `storage.db.*` implementation
rooted by direct `data/runtime.zig`, `ProvisionedGroupStorage`, cache,
maintenance, split/merge, and transition calls.

The five probe reports contained 830 duplicate repository-file instances,
including 199 duplicate storage instances across 150 storage files. The
stripped executable grew from 59,270,696 to 70,439,360 bytes (+18.8%) while
the additional storage unit duplicated code still owned by distributed. The
shared C API remained small at 16,382,112 bytes.

Decision: **reject and revert**. Separating or narrowing the vtable alone is
not the atomic owner cut. The next viable experiment must change the runtime
state model so `data/runtime.zig` no longer owns or directly calls physical DB,
cache, maintenance, snapshot, split/merge, and generation-transition
implementation. A control-only source becomes useful only as the outer face of
that owner transfer, not as an independent optimization.

### Restored process-scoped owner composition

The first bounded prerequisite after the control-vtable result restores the
single-owner invariant described in Phase 2p. `DataServer`, rather than the
Raft apply adapter, now owns one heap-stable
`ProvisionedKernelOwnerSource`. API reads, direct local writes, Raft apply,
snapshot publication, HA replay, sync waits, descriptor capture, and startup
warmup all borrow that same source. The apply adapter cannot destroy the
owner, and `DataServer` destroys it only after HTTP, write-source, Raft, and
background work have drained.

Owner creation is also the attachment invariant. Any path that lazily creates
the process owner immediately installs its local read, write, and snapshot
sources and removes the legacy resident-DB fallback. This matters outside the
normal API startup sequence: the first cross-archive test exposed that warmup
could create the owner and then let a following lookup fall back to a second
physical DB, producing `GenerationTransitionActive`. Centralizing creation and
attachment removed that race. Warmup validates the owner root and retires it
before startup catch-up, preserving the existing no-pinned-writer contract;
the first actual read or write installs the resident owner.

A new `storage-kernel-data-runtime-test` step links the data-runtime test
artifact to the real experimental provider archive. It covers warmup followed
by lookup and batch, verifies that the distributed read/write caches stay
untouched, and exercises routed HA replay. Validation at this checkpoint:

- process-owner data-runtime composition: 12/12 passed, zero leaks;
- opaque owner ABI: 3/3 passed, zero leaks;
- provisioned cross-archive owner: 6/6 passed, zero leaks;
- legacy data-runtime suite: 97/97 passed, zero leaks;
- linked native Debug production-LSM experiment built with normal concurrency;
- all runtime/codegen/API graph gates and all 15 analyzer tests passed.

Decision: **keep as a bounded prerequisite, without a compiler-time claim**.
The previous Phase 2p profiles already established that attachment alone does
not remove the physical implementation. A new cold profile would only repeat
that result. The immediately following cut must move a complete lifecycle
owner--startup catch-up/index repair, maintenance/status, or split/merge--and
then make its legacy data-runtime functions unreachable before measuring.

### Rejected runtime-status and startup-catch-up owner slice

The next probe moved resident/transient runtime-status capture and the complete
startup open, WAL catch-up, dense rebuild, index repair, resolver drain, and
obsolete-generation reclamation sequence behind the compiled owner boundary.
The transient path deliberately opened the physical writer in no-replay mode so
the kernel, rather than an earlier normal open, observed and discharged startup
debt. Control retained leadership admission, activation checks, status
publication, and retry scheduling through coarse callbacks. No DB, writer,
index, or cache layout crossed the ABI.

The implementation was behaviorally viable. The cross-archive data-runtime
suite passed 17/17 with zero leaks, covering replay debt, the no-debt busy-writer
case, leadership retry/replay, and routed HA apply. Provisioned-source tests
passed 6/6, opaque-owner ABI tests passed 3/3, the existing terminal-degraded
startup regression passed, and the complete linked native Debug production-LSM
build succeeded with normal concurrency.

A cold Apple-silicon cross-build at commit `cab805020` plus the uncommitted
probe, using Zig 0.16.0 and targeting stripped ARM64 Linux musl ReleaseFast,
produced the following comparison against the exact detached-commit baseline:

| Unit | Baseline | Probe | Delta | Declarations | Repository Zig files |
|---|---:|---:|---:|---:|---:|
| Distributed | 381.840 s | 382.922 s | +1.083 s | 41,930 -> 41,900 | 720 -> 720 |
| Storage kernel | 233.023 s | 246.084 s | +13.061 s | 26,676 -> 27,605 | 395 -> 404 |
| API kernel | 136.866 s | 139.833 s | +2.967 s | 17,332 -> 17,334 | unchanged |
| Inference | 230.284 s | 228.708 s | -1.576 s | 24,971 -> 24,971 | unchanged |
| CLI | 35.918 s | 35.408 s | -0.510 s | 5,804 -> 5,804 | unchanged |

The distributed compiler graph was byte-for-byte identical at the file level:
all 720 repository Zig files and 1,066,925 source lines were shared. Its LLVM
emit time increased from 373.804 to 374.665 seconds. The distributed object
lost only 131,833 text bytes, from 27,920,901 to 27,789,068, while the storage
object gained 815,928 text bytes, from 17,120,855 to 17,936,783. The stripped
executable grew from 74,033,864 to 74,730,112 bytes (+0.94%); `libantfly.so`
remained 16,382,112 bytes. Peak compiler RSS observed during the normally
concurrent build was approximately 3.1 GB, so memory was not the limiting
signal in this local probe.

Decision: **reject and revert**. Status and startup lifecycle are coherent
coarse operations, but moving them does not make any storage implementation
file unreachable from distributed control. Retaining this slice would add ABI
surface and roughly 13 seconds of storage-kernel work without reducing the
critical unit. The next measurement-worthy cut must be a complete local-query
or physical runtime-owner transfer that removes implementation roots from the
distributed compiler graph; another narrow status, callback, or lifecycle
operation is not sufficient.

### Rejected provisioned and hosted local-read fallback cuts

The stable opaque owner already supplied the complete provisioned group-local
read surface, but the orchestration source still retained its physical fallback
at runtime. A first probe made that fallback compile-time unreachable under the
storage experiment. It passed the cross-archive data-runtime, provisioned-owner,
and opaque-owner suites, plus linked Debug in both production-LSM and
LMDB-compatibility configurations.

The cold result removed only 11 distributed declarations, no repository file,
and 12,612 text bytes. Distributed measured 387.531 seconds versus the exact
381.840-second baseline while independent units were also slightly slower. The
hosted metadata source still retained the same physical query callbacks.

A revision also removed the hosted local-read fallback. This is behaviorally
consistent with its production metadata-server router, which reports the local
metadata node as absent for data-bearing groups and remote-routes public data
requests. The revised cold profile was:

| Unit | Baseline | Combined read cut | Delta |
|---|---:|---:|---:|
| Distributed | 381.840 s | 400.370 s | +18.530 s |
| Storage kernel | 233.023 s | 243.910 s | +10.887 s |
| API kernel | 136.866 s | 142.245 s | +5.379 s |
| Inference | 230.284 s | 241.212 s | +10.928 s |
| CLI | 35.918 s | 36.693 s | +0.775 s |

All long independent units were roughly 4--5% slower, so the raw distributed
increase is host variance rather than a regression attributable to the cut.
The structural evidence is nevertheless decisive: distributed removed only
four repository files, 261 declarations, 96 generic instances, 65 inline
calls, and 221,991 text bytes. The storage object was byte-identical. Hosted
writes, data-runtime caches, and lifecycle operations continued to instantiate
the same DB and local-query implementation.

Decision: **reject and revert**. Even a complete stable read vtable is not an
atomic ownership boundary while another consumer owns the writer/runtime that
queries must share. Do not attempt more fallback pruning. The next viable
experiment must move the complete physical runtime state--hosted and
provisioned writers, reader/writer caches, resource manager, generation
lifecycle, maintenance, and local query--behind one compiled owner, then expose
only routing/orchestration callbacks and coarse operation envelopes.

### Rejected API contract-import cleanup

A follow-up audit tested whether the 50 physical-storage files still analyzed
by the separately compiled API protocol unit were rooted primarily by two
accidental implementation imports. In production builds, `httpx_handler.zig`
was changed to import the narrow write contract and
`http_internal_group_read_routes.zig` was changed to import the narrow read
contract. The few pure index-normalization, vector-envelope, and text-stats
wire helpers used by those routes were placed in storage-neutral contract
modules. `kernel_bridge.zig` also used the narrow contract types directly.

The first writer-only cold probe removed `table_writes.zig` but changed the API
graph from only 17,332 to 17,330 declarations and 326 to 325 repository files.
The combined read/write probe removed both broad implementation files and
produced this cold ARM64 Linux musl `ReleaseFast` API comparison:

| Metric | Exact baseline | Combined contract probe | Delta |
|---|---:|---:|---:|
| Compiler time | 136.866 s | 127.683 s | -9.183 s |
| LLVM emission | 132.900 s | 124.405 s | -8.494 s |
| Declarations | 17,332 | 17,322 | -10 |
| Imported files | 490 | 488 | -2 |
| Repository files | 326 | 324 | -2 |
| Repository source lines | 664,608 | 602,279 | -62,329 |
| Object allocatable bytes | 11,346,229 | 11,346,229 | identical |
| Object text bytes | 8,800,424 | 8,800,424 | identical |

The apparent time reduction is host variance: the optimized API objects are
byte-for-byte identical. The two removed source files were lazy implementation
barrels and contributed no emitted code to this unit. All 50 storage files and
223,296 storage source lines remained in the authoritative compiler graph
after the writer-only cut; 49 remained after the read cut, including DB,
DocStore, LSM, algebraic query, transactions, maintenance, and backend-erasure
modules rooted by real API behavior and shared contract types.

The experiment used a temporary API-only profiling target. The focused cold
archive build succeeded, the complete linked native Debug build with the
separate-storage option succeeded under normal concurrency, and all 15 graph
analyzer tests passed.

Decision: **reject and revert**, including the temporary profiling target.
Import hygiene can be done independently, but deleting lazy barrel edges does
not reduce LLVM emission. Do not count compiler `all_files` entries or lexical
source lines as a codegen win without an object delta. The next compilation
experiment remains the complete physical runtime-state transfer described
above.

### Rejected remote-only metadata control split

The next probe tested a smaller alternative to transferring the complete
`DataServer` state immediately. Metadata's data-bearing router always reports
its local data-group status as absent, so its public table source should only
fan out to remote data stores. The probe compiled those impossible local read,
write, repair, and maintenance fallbacks out of a separate CLI-plus-metadata
control artifact and failed closed if routing nevertheless selected a local
data group.

An exact current-tree cold control proved that this was real emitted work, not
another lazy-import result:

| Control-only metric | Current control | Remote-only probe | Delta |
|---|---:|---:|---:|
| Compiler time | 305.614 s | 270.585 s | -35.029 s |
| LLVM emission | 299.360 s | 265.071 s | -34.288 s |
| Declarations | 34,690 | 32,926 | -1,764 |
| Imported files | 760 | 741 | -19 |
| Object allocatable bytes | 23,338,385 | 21,798,177 | -1,540,208 |
| Object text bytes | 19,598,032 | 18,174,052 | -1,423,980 |

The candidate was then linked as one complete executable with five normally
scheduled units: API, application/storage without metadata, remote-only
metadata control, inference, and remote CLI. A fresh ARM64 Linux musl
`ReleaseFast` build completed all 30 steps:

| Unit | Compiler time | LLVM emit | Declarations | Imported files |
|---|---:|---:|---:|---:|
| Application/storage | 348.021 s | 340.862 s | 40,339 | 889 |
| Metadata control | 266.156 s | 260.443 s | 32,295 | 722 |
| Inference | 216.549 s | 209.655 s | 24,971 | 708 |
| API protocol | 130.187 s | 126.637 s | 17,332 | 490 |
| Remote CLI | 37.126 s | 35.304 s | 5,804 | 205 |

The maximum unit landed just below the preferred 350-second local gate, and
the linked Debug build, all five top-level command help paths, and 5/5 linked
main tests passed. The stripped ARM64 executable remained static with no
dynamic section or production LMDB symbols. The C API stayed small at
16,666,392 bytes.

The holistic result nevertheless fails the architecture gates. Compared with
the four-unit application/storage baseline, compiler file instances increased
from 1,626 to 2,152 and duplicate instances from 438 to 965. Storage alone
increased from 52 to 189 duplicate instances. The stripped executable grew
from 59,270,808 to 76,892,480 bytes: +17,621,672 bytes or 29.7%, far beyond the
approximately 5% limit. Metadata still loaded 137 storage files and emitted a
20,592,552-byte allocatable object because its catalog, Raft application, and
public coordination paths retain physical storage implementation.

Decision: **reject and revert**. Remote-only public routing is a valid semantic
observation and saved 35 seconds in isolation, but splitting metadata before
moving its physical/catalog storage ownership merely creates another large
optimized copy. Do not revive a separate metadata unit until its remaining
storage implementation is consumed through the same opaque compiled kernel as
data, standalone/Lite, serverless, and the C API. The next experiment must
therefore be an atomic physical-owner transfer inside the current co-generated
application/storage topology, not another role regrouping.

### Emitted-module attribution for the atomic owner cut

The earlier compiler reports proved that distributed and storage loaded nearly
the same physical graph, but the distributed object lacked named function/data
sections and could not attribute its emitted bytes. A diagnostic-only cold
build enabled section granularity for that object, preserving the same source
graph and declarations. The profiling edit was reverted after the build.

The distributed unit measured 368.184 seconds, including 360.321 seconds in
LLVM, with the same 720 repository files, 41,930 declarations, 22,159 generic
instances, and 14,483 inline calls as the exact 381.840-second baseline. The
time difference is host variance and is not credited to section granularity.
The emitted objects provide the useful result:

| Object result | Distributed | Storage kernel |
|---|---:|---:|
| Allocatable bytes | 28,202,700 | 17,523,398 |
| Text bytes | 23,992,432 | 14,186,004 |
| Attributed Antfly modules | 361 | 175 |
| Attributed Antfly bytes | 15,885,059 | 9,078,329 |

Across those two objects, 172 Antfly modules were emitted twice, accounting
for 8,650,546 duplicate allocatable bytes and 8,642,944 duplicate text bytes.
The largest duplicate implementation modules were:

| Module | Duplicate text bytes |
|---|---:|
| `storage.db.db` | 1,880,488 |
| `storage.db.algebraic.index` | 977,408 |
| `storage.db.catalog.index_manager` | 785,596 |
| `storage.db.query.search_exec` | 472,764 |
| `storage.db.enrichment.enrichment_runtime` | 434,832 |
| `storage.db.aggregations` | 397,984 |

Those six modules alone account for 4,949,072 duplicate text bytes. The
distributed object also emits 617,028 text bytes from `api.table_writes` and
588,892 from `api.table_reads`. This is authoritative machine-code evidence
that the next win is the broad physical-source cut, not another import, status,
or command-dispatch cleanup.

Decision: use the existing process-scoped opaque owner and complete its
operation surface before changing source selection. The next candidate is one
atomic compile-time switch: the experimental distributed unit must select a
control-only provisioned read/write composition whose local callbacks all enter
the owner, while the legacy physical vtable, caches, DB opens, maintenance,
snapshot, split/merge, and generation-transition implementations become
unreachable together. Do not measure or enable a partially switched vtable.

The remaining implementation checklist is intentionally grouped by ownership
family rather than by file:

1. schema, index, enrichment-configuration, table-create, and table-retirement
   operations;
2. explicit and automatic bulk-ingest lifecycle;
3. transaction participation and recovery callbacks;
4. artifact mutation, reprocessing, and repair;
5. backup, restore staging/publication, and rollback;
6. startup catch-up, compaction/flush, structural reconciliation, split/merge,
   runtime-status publication, and generation transitions; and
7. the final control-source selection that removes distributed-owned physical
   caches/resources and the complete legacy local vtables in one build.

Each family may land as tested ABI preparation, but the next cold compiler
go/no-go checkpoint is item 7. If that switch does not remove the dominant
modules above from distributed, the candidate must be revised or reverted
rather than justified by loaded-file counts.

### Phase 4a: resident catalog-contract reconfiguration

The first prerequisite for ownership family 1 is now implemented behind
storage-owner ABI version 11. A resident owner accepts one synchronous,
borrowed table/schema/index contract and applies it to the same live DB used by
reads, writes, Raft apply, HA replay, status, and snapshots. The distributed
`ProvisionedKernelOwnerSource` reloads the authoritative catalog descriptor and
reuses the existing owner lease, rather than opening a second physical DB or
replacing the process-scoped owner.

Focused Debug validation with linked runtime libraries, the production LSM
backend, and the storage experiment enabled passed:

- the opaque-owner suite: 3/3 tests, including invalid ABI/table rejection and
  live dense-index reconfiguration;
- the provisioned owner-source suite: 6/6 tests, including preservation of one
  resident owner across reconfiguration and subsequent read/write/snapshot
  operations;
- the public C API regression suite: 10/10 tests; and
- the graph-analyzer suite: 15/15 tests.

This is accepted ABI preparation, not a compiler-time result and not yet the
completion of ownership family 1. Reconfiguration intentionally schedules
index work without synchronously draining a corpus-sized backfill. The owner
still needs a bounded reconcile/repair operation, and the control source must
route table creation, index/schema/enrichment changes, and retirement through
that operation before the legacy structural vtable can become unreachable.
Consequently no cold ReleaseFast improvement is claimed for this increment;
the next cold checkpoint remains the atomic source switch in checklist item 7.

### Phase 4b: bounded owner-side structural reconciliation

Storage-owner ABI version 12 adds a coarse `reconcile` operation that advances
one complete desired-state quantum on the resident DB. It applies the projected
schema/index/enrichment contract, advances one generated-artifact cleanup page,
optionally advances one durable index-repair intent, and returns an explicit
`complete`, `repair_pending`, `busy`, or `degraded` state plus bounded repair
statistics. The call remains synchronous and borrows every input; corpus work
is restartable and bounded behind the existing durable repair state machine.

The generic group-local write contract now exposes that result without
importing storage implementation types. Under the storage experiment, the
existing structural reconciliation scheduler calls the opaque owner and its
legacy DB-open/index-repair body is behind a compile-time-unreachable branch.
Direct create-table uses the owner synchronously as its empty-table readiness
barrier. Schema, index, and enrichment mutations retain the existing queued
control workflow, but their physical work now enters the compiled owner.

Validation passed with normal concurrency:

- opaque owner: 3/3, including bounded reconstruction of a full-text index over
  documents written before the index existed and a successful query afterward;
- provisioned owner source: 6/6, including create, batch, reconcile, query, and
  snapshot work through one resident owner;
- public C API: 10/10;
- the complete linked Debug executable with the production LSM backend and
  storage experiment enabled; and
- archive inspection confirmed both hidden internal symbols,
  `antfly_storage_owner_configure` and `antfly_storage_owner_reconcile`, in the
  compiled storage kernel.

This completes the physical schema/index/enrichment reconciliation mechanism,
but not ownership family 1 as a whole: table retirement still uses the legacy
physical source, and the node repair scheduler must be routed through the same
owner before the final source switch. No cold ReleaseFast result is credited
until those remaining roots are removed atomically.

### Phase 4c: owner-fenced table retirement

The group-local write contract now includes table-owner retirement. In the
storage experiment, drop-table first enters the existing table-wide structural
admission fence, retires every resident opaque owner, and only then atomically
moves and deletes each physical group path. The compile-time experimental path
returns before the legacy local DB/cache drop implementation, so a catalog
deletion cannot leave a live compiled owner or require a second physical DB
owner to perform cleanup.

The provisioned owner-source suite remains 6/6 and now proves that drop-table
closes the sole resident owner, reduces the owner count to zero, removes the
group path, and permits a later clean reopen. The complete linked Debug
executable also rebuilds successfully with normal concurrency, the production
LSM backend, and the storage experiment enabled.

This completes ownership family 1's physical create, schema, index,
enrichment-configuration, bounded repair admission, and retirement mechanisms.
The durable node repair scheduler still needs to consume the bounded owner
operation as part of ownership family 6, but no schema/index control path needs
to open a DB in distributed code. The next family is bulk-ingest lifecycle.

### Phase 4d: owner-side bulk-ingest lifecycle

Storage-owner ABI version 13 moves the complete explicit bulk-ingest lifecycle
behind the resident owner. Distributed control retains only table routing,
nested-session depth, and group completion bookkeeping. One coarse begin and
one coarse finish/abort call enter each group owner; no record, LSM run, index,
or backend primitive crosses the ABI. Finish preserves every existing scalar
option plus synchronous progress and admission callbacks.

The table-scoped control ledger is retry safe. If a multi-group finish succeeds
for some groups and fails for others, it retains only the unfinished group IDs;
a retry cannot finish a successfully published owner twice. Begin failure or a
concurrent abort rolls back every owner that actually started, source quiesce
aborts remaining sessions before owner teardown, and an active explicit session
continues to fence split/generation transitions.

Automatic cache bulk windows require no replacement owner protocol. Production
ordinary uploads intentionally stopped opening those windows: DB/LSM owns
online batching and L0 maintenance, and the source's automatic-window counters
are normally zero. Under the experiment the legacy cache cleanup methods
therefore return the same empty result without touching a distributed physical
cache. They still reject a transition while an explicit owner session is
active. This preserves current behavior without reintroducing an automatic
cache policy solely for the compilation experiment.

Warm Debug validation with normal concurrency, linked runtime libraries, the
production LSM backend, and the storage experiment enabled passed:

- opaque owner ABI: 3/3, including begin/write/finish visibility, abort followed
  by an ordinary write, wrong-table validation, and future-version rejection;
- provisioned cross-archive owner source: 6/6, including nested begin/finish,
  abort, transition fencing, zero automatic-window state, and one resident
  owner throughout;
- public C API: 10/10 with zero leaks;
- the complete linked executable;
- all runtime/codegen/API graph gates plus the 15/15 analyzer suite; and
- archive/shared-library symbol inspection: all three internal bulk symbols
  are present in the storage archive and absent from the public C API exports.

Decision: keep this as ownership-family 2 preparation. It removes the remaining
physical explicit-bulk behavior required by the future control-only source and
does not change the intentionally disabled automatic-window policy. As with the
preceding lifecycle slices, no cold compiler improvement is claimed before the
atomic source-selection checkpoint.

### Phase 4e: transaction participation and recovery

Storage-owner ABI version 14 moves group-local transaction begin, prepare,
resolve, status, acknowledgement, and background recovery behind the resident
owner. Distributed control retains topology-epoch validation, transition
admission, HA write gating, Raft proposal, leadership ownership, and participant
routing. Transaction intent validation, durable records, local resolution,
participant acknowledgement, cleanup, and document/index publication remain
one coarse physical operation inside the compiled storage unit.

This slice also fixes a correctness hole in the earlier experimental Raft apply
path. The owner parsed the internal `_transaction` envelope but passed every
request to `DB.batchReplicatedApply`, whose ordinary batch path does not execute
transaction mutations. The new storage-kernel replicated-batch entry point
validates against the persisted local schema and dispatches transaction
envelopes to the same durable implementation used by the legacy apply path.
Committed Raft transaction commands therefore can no longer be accepted while
silently leaving their transaction phase unapplied.

The owner open request now carries the real logical group ID separately from
the document-identity shard and a versioned recovery callback table. A live
owner owns a copied callback configuration and owner ID for exactly as long as
its DB recovery worker can invoke them. Local recovery resolution stays inside
the DB; remote resolution, replicated acknowledgement/cleanup, and leadership
ownership synchronously call back into distributed control through borrowed
participant identifiers. Owner shutdown stops the DB worker before releasing
that callback state or the distributed write source.

Warm Debug validation with normal concurrency, linked runtime libraries, the
production LSM backend, and the storage experiment enabled passed:

- opaque owner ABI: 4/4, including replicated begin/prepare/commit visibility,
  status, validation, and a real background recovery callback crossing the
  archive boundary before durable cleanup;
- provisioned cross-archive owner source: 6/6, including a full transaction
  lifecycle through the routing-aware source and the same single resident
  owner used by reads and ordinary writes;
- DataServer process-owner composition: 12/12;
- public C API: 10/10;
- complete linked Debug artifacts with the experiment both enabled and
  disabled;
- all runtime/codegen/API graph gates plus the 15/15 analyzer suite; and
- symbol inspection: `antfly_storage_owner_transaction_status` and the
  replicated-batch entry point are present in the storage archive, all
  internal owner/runtime symbols remain absent from public C API exports, and
  no production LMDB implementation symbol is present.

The DataServer composition gate also exposed an existing LSM-only startup bug:
the linked production role compiled LMDB out but still tried to open the legacy
LMDB restore-job registry. LSM-only roles now retain the server's in-memory
registry instead of failing startup. Durable restore-job persistence belongs in
the later storage-owner restore/maintenance family; it must not reintroduce
LMDB into the production artifact.

Decision: keep this as ownership-family 3 preparation. It closes both the
direct and Raft-applied transaction paths and preserves recovery semantics
without fine-grained ABI crossings. No cold ReleaseFast result is credited
until the atomic control-source selection removes the legacy transaction and
physical DB implementations from distributed codegen.

### Phase 4f: artifact mutation, reprocessing, and repair

Storage-owner ABI version 15 moves the complete group-local artifact operation
surface behind one coarse, tagged operation call. The owner now performs
single-document and bounded-range reprocessing, repair listing and execution,
child-range placement mutation, child-range batch application, and the internal
embedding-corruption diagnostic. Requests and owned results use the same JSON
shapes already carried by the internal HTTP routes; no document, embedding,
posting, child record, or backend primitive crosses the ABI independently.

Distributed control continues to resolve tables and groups, enforce the HA
write gate, hold table/group lifecycle admission, invalidate read/status
caches, aggregate paginated results, and publish local-change and repair-debt
notifications. The experimental group-local branches are compile-time
exclusive: a missing compiled-owner callback now fails with
`StorageKernelOwnerUnavailable` instead of silently opening a second physical
DB. The diagnostic corruption endpoint gained an explicit group-local contract
for the same reason.

Repair cancellation crosses a borrowed synchronous callback and is never
retained. Index-repair admission still sets
`defer_durable_index_repair_execution` before entering the owner, preserving the
bounded operator-request behavior. Rich yield, activation, and capacity
callbacks are rejected explicitly at this boundary rather than silently
dropped. Those controls belong to the storage-owned durable repair scheduler
that will move with startup catch-up and maintenance in ownership family 6.

This slice also fixed a wire-result omission: the shared repair-result decoder
now preserves `controls_applied`, so operator pause/resume results cannot lose
that count when returned through either a remote group or the compiled owner.

Warm Debug validation with normal concurrency, linked runtime libraries, the
production LSM backend, and the storage experiment enabled passed:

- opaque owner ABI: 4/4, exercising all seven tagged artifact operations,
  cancellation across the ABI, and invalid-version rejection;
- provisioned cross-archive owner source: 6/6, including placement mutation,
  document and range reprocessing, repair listing, child-range batch handling,
  diagnostic corruption, cancellation, and preservation of one resident owner;
- DataServer process-owner composition: 12/12;
- public C API: 10/10;
- complete linked Debug artifacts with the experiment both enabled and
  disabled;
- all runtime/codegen/API graph gates plus the 15/15 analyzer suite; and
- symbol inspection: `antfly_storage_owner_artifact_operation_json` is present
  in the compiled storage archive, all internal storage-owner/context/runtime
  symbols remain absent from public C API exports, and no production LMDB
  implementation symbol is present.

Decision: keep this as ownership-family 4 preparation. Every current
control-originated artifact mutation/reprocessing/repair path can enter the
same resident physical owner without a fine-grained ABI. The durable node
repair scheduler remains ownership family 6, not an excuse to carry its
physical DB implementation in distributed control. No cold ReleaseFast result
is credited before the atomic source-selection checkpoint in checklist item 7.

### Phase 4g: owner-side local backup materialization

Storage-owner ABI version 16 moves one complete group-local backup operation
behind the resident owner. Both native snapshots and portable `.afb` exports
now read the same live DB used by queries, writes, transactions, and repair,
then return one owned shard descriptor with its byte range, relative artifact
path, size, and SHA-256 integrity value. No record, LSM run, index entry,
directory entry, or storage-backend primitive crosses the boundary.

The API layer deliberately retains backup reservation, filesystem/object-store
scope validation, remote transport, manifest construction and conditional
publication, ambiguous-outcome fencing, and rollback cleanup. Only local
artifact materialization moves. `std.Io` is not treated as an ABI type: the
compiled storage unit owns the filesystem scheduler for this coarse synchronous
operation, while distributed control's existing scheduler continues to own
reservation, transport, and publication. The experimental branch fails with
`StorageKernelOwnerUnavailable` when its group-local callback is absent and
cannot silently reopen the DB through the legacy source.

Warm Debug validation with normal concurrency, linked runtime libraries, the
production LSM backend, and the storage experiment enabled passed:

- opaque owner ABI: 4/4, including native and portable backup through the live
  owner plus wrong-table, invalid-version, and invalid-format rejection;
- provisioned cross-archive owner source: 6/6, verifying both artifact formats,
  integrity metadata, and preservation of one resident owner;
- DataServer process-owner composition: 12/12;
- public C API: 10/10;
- complete linked Debug artifacts with the experiment both enabled and
  disabled;
- all runtime/codegen/API graph gates plus the 15/15 analyzer suite; and
- artifact inspection: the 101,807,792-byte Debug storage archive defines the
  hidden `antfly_storage_owner_backup_json` entry point and no native `mdb_*`
  implementation symbols; the public C API libraries remain approximately
  48.7 MB in Debug and export no storage-owner/context/runtime symbols.

Decision: keep this as the backup half of ownership-family 5. It removes the
remaining production local backup DB open/export path required by the future
control-only source without moving publication policy or remote I/O into the
kernel. Backup does not complete the family: backup-restore materialization,
generation publication/rollback, idempotent reconciliation, and durable
restore-job persistence still need coarse owner contracts. No cold ReleaseFast
result is credited before the atomic source-selection checkpoint in checklist
item 7.

### Phase 4h: owner-side restore materialization and publication

Storage-owner ABI version 17 moves the physical half of a complete local
backup restore behind four coarse operations: prepare, promote, reconcile, and
repair an already published generation. Prepare accepts one borrowed manifest
and artifact descriptor, validates backup and identity integrity, materializes
an isolated candidate, opens it with the managed restore-repair mode, advances
durable runtime repair, synchronizes DB and indexes, and seals the candidate.
No record, index entry, LSM file, restore phase, or scheduler primitive crosses
the ABI.

Distributed control retains table routing, lifecycle admission, catalog
publication hooks, cache invalidation, and structural notifications. A new
restore follows this ordered state machine while holding the existing local
generation transition:

1. storage prepares and seals an isolated candidate;
2. storage promotes the preparation into an exclusive generation transition;
3. distributed control publishes the authoritative table definition; and
4. storage publishes and commits the physical generation.

Failure before physical publication leaves the live generation unchanged. A
failure after definition publication invokes the definition rollback hook and
the storage snapshot rollback path. Commit removes superseded snapshot state.
An exact retry validates the imported artifact identity and repairs through the
same resident owner; reconcile-only mode validates and completes a committed
restore under a process-exclusive transition. The previously shared snapshot
publication API now exposes promotion explicitly so Raft snapshot replacement
preserves the same `prepare -> promote -> publish -> commit` ordering.

The short-lived candidate owns its backend scheduler and uses the managed
restore-repair runtime; `std.Io` is not an ABI type. Completion also advances a
recoverable durable index-shadow repair synchronously when the process runtime
would otherwise outlive the ABI call. Provider, secret, and remote-content
capabilities required by production-generated enrichments are not smuggled
through this restore request: making those process-scoped capabilities
available to every resident and candidate owner remains part of ownership
family 6 and is required before the atomic source switch.

Warm Debug validation with normal concurrency, linked runtime libraries, the
production LSM backend, and the storage experiment enabled passed:

- opaque owner ABI: 4/4, including future-version rejection and null snapshot
  promotion rejection;
- provisioned cross-archive owner source: 6/6, restoring native and portable
  artifacts, discarding post-backup mutations, preserving original data,
  rejecting catalog publication without changing the live generation, exact
  retry, reconcile-only completion, and single-owner reuse;
- DataServer process-owner composition: 12/12;
- public C API: 10/10;
- complete linked Debug artifacts with the experiment both enabled and
  disabled;
- all runtime/codegen/API graph gates plus the 15/15 analyzer suite; and
- artifact inspection: the 105,956,216-byte enabled Debug storage archive
  defines the hidden `antfly_storage_restore_prepare`,
  `antfly_storage_restore_reconcile`,
  `antfly_storage_owner_restore_repair`, and
  `antfly_storage_snapshot_promote` entry points and no native `mdb_*`
  implementation symbols; the 48,686,640-byte Debug public C API dylib exports
  no internal storage-owner/context/restore/snapshot symbols.

Decision: keep this as the physical restore half of ownership-family 5. Local
backup materialization, restore materialization, integrity repair, generation
publication, rollback, idempotent retry, and committed reconciliation now have
coarse compiled-owner contracts. Durable user-facing restore-job persistence
remains a control-plane concern, while its local physical work must continue to
enter this owner. No cold ReleaseFast result is credited before the atomic
source-selection checkpoint in checklist item 7.

### Phase 4i: owner-side maintenance scheduling boundary

The post-restore cold baseline was captured at commit `032b292f5` on the
14-logical-CPU ARM64 macOS host with Zig 0.16.0. Both baseline and candidate
used fresh local and global caches, normal build concurrency, the
`aarch64-linux-musl` target, `ReleaseFast`, stripping, linked runtime libraries,
the production LSM-only configuration, and the storage-kernel experiment.

The baseline compiler reports were:

| Unit | Total | LLVM emit | Declarations | Repository files |
|---|---:|---:|---:|---:|
| distributed | 371.179 s | 363.555 s | 41,950 | 720 |
| storage | 242.400 s | 236.988 s | 28,083 | 418 |
| inference | 224.570 s | 217.700 s | 24,971 | 524 |
| API | 134.458 s | 130.704 s | 17,333 | 326 |
| CLI | 35.144 s | 33.465 s | 5,804 | 53 |

Across those reports, 2,041 repository-file instances represented 1,189 unique
files: 852 duplicate instances across 493 files. Storage accounted for 200
duplicate instances and approximately 595,000 duplicated source lines. A
separate normal-concurrency run completed in 368 seconds, with 6,341,472 KiB
maximum single-compiler RSS and 10,433,296 KiB peak aggregate compiler RSS.
The stripped executable was 75,286,416 bytes.

Storage-owner ABI version 18 moves the complete LSM and dense-posting
maintenance quantum behind one coarse call. Distributed control still owns
resource admission, HA job gating, wake scheduling, retry policy, metrics, and
runtime-status invalidation. It can inspect owner pressure, select a resident
group, request one exact or best-effort LSM quantum, or request one idle dense
posting round. DB, primary-store, index-manager, compaction, and posting details
never cross the ABI.

The process owner takes short-lived leases on all eligible resident owners,
selects immediate-wake work before the largest maintenance score, and executes
at most one LSM quantum per control round. Dense maintenance remains one call
per resident owner rather than one call per index or posting. An owner records
its physical bulk-ingest window atomically. The control-side session ledger and
owner-side flag both exclude opening, active, and partially failed bulk
sessions from maintenance; a failed finish keeps the owner excluded until an
abort or successful retry.

The comparable cold candidate produced:

| Unit | Total | LLVM emit | Declarations | Repository files | Baseline delta |
|---|---:|---:|---:|---:|---:|
| distributed | 379.403 s | 370.947 s | 41,946 | 721 | +8.224 s |
| storage | 252.812 s | 246.897 s | 28,098 | 418 | +10.412 s |
| inference | 222.350 s | 215.525 s | 24,971 | 524 | -2.220 s |
| API | 145.484 s | 141.295 s | 17,333 | 326 | +11.027 s |
| CLI | 35.208 s | 33.509 s | 5,804 | 53 | +0.064 s |

The independent cold normal-concurrency build completed in 364 seconds versus
the 368-second baseline. Its maximum individual compiler RSS was 7,383,088 KiB
for distributed, and peak aggregate compiler RSS was 11,472,944 KiB. These are
about 1 GiB above the earlier samples but remain within the existing claims and
far below the original 18--20 GiB failure profile. Because overall wall time
improved while several individual time reports regressed, no small timing win
or loss is credited from these single-host samples. The critical unit remains
inside the 380-second gate by less than one second; normal-runner confirmation
is still required.

The ownership signal is small but direct:

- the distributed archive shrank from 32,902,274 to 32,890,032 bytes, while
  its object text shrank by 8,752 bytes;
- the storage archive grew from 25,528,498 to 25,538,236 bytes, while its
  attributed text grew by 5,564 bytes;
- the storage archive defines hidden `antfly_storage_owner_maintenance`, and
  distributed retains only an undefined reference to that entry point;
- the final stripped executable shrank from 75,286,416 to 75,283,520 bytes;
  and
- aggregate compiler overlap remained effectively unchanged at 2,042 file
  instances, 1,190 unique files, and 852 duplicate instances. This slice moves
  emitted maintenance code, not the remaining physical source roots.

Warm validation passed the 4/4 owner ABI suite, 6/6 cross-archive provisioned
suite, 12/12 DataServer composition suite, 10/10 public C API suite, complete
linked Debug builds with the experiment enabled, disabled, and with LMDB
compatibility enabled, all runtime, codegen, and API graph gates, and all 15
analyzer tests. The provisioned suite
explicitly verifies that a nested bulk session removes its owner from the
maintenance snapshot until the final successful finish. The cold product is
one stripped, statically linked ARM64 musl executable, and the storage archive
contains no native `mdb_*` implementation symbol.

Decision: **keep** this as the first ownership-family 6 prerequisite. It moves
real optimized maintenance text in the correct direction without binary growth,
per-backend ABI calls, reduced concurrency, or a runner-cost change. It is not
the atomic source cut and earns no claim that the duplicate compiler graph has
shrunk. The next experiment must redirect the remaining local runtime/storage
metric snapshots and physical lifecycle probes, then use those coarse owner
contracts to make the legacy provisioned DB source unreachable in the
experimental distributed unit.

### Rejected local-role placement probes after the physical-cache cut

After the process owner became the only provisioned read/write cache under the
experiment, three cold ARM64 Linux musl `ReleaseFast` probes tested whether
moving complete runtime roles into the storage archive could serve as the
final composition. These were diagnostic placement probes, not ownership
boundaries. All used normal build concurrency and completed the full 29-step
linked build.

| Storage-archive additions | Distributed | Storage | Repository files in distributed | Duplicate file instances | Executable |
|---|---:|---:|---:|---:|---:|
| None; source/cache cut only | 381.185 s | 251.340 s | 717 | 848 | 75,038,432 B |
| Standalone + Lite + restore staging | 359.957 s | 324.636 s | 688 | 1,013 | 80,098,968 B |
| Above + serverless | 317.725 s | 354.506 s | 593 | 1,011 | 80,099,872 B |
| Above + metadata | 288.411 s | 376.890 s | 572 | 1,008 | 80,111,776 B |

The final probe placed data plus HA in distributed and every local-storage role
in the storage archive. It demonstrated the available control-root reduction:
distributed dropped by 92.774 seconds and 145 repository files relative to the
source/cache-only candidate. It also demonstrated why role placement is not
the architecture:

- the critical unit merely moved to the 376.890-second storage archive;
- aggregate duplicate instances increased from 848 to 1,008;
- the executable grew 5,073,344 bytes (6.8%), beyond the approximately 5%
  artifact gate;
- the storage archive grew to 40,146,808 bytes and would couple the small CAPI
  artifact to server runtimes; and
- data plus HA still emitted physical DB, LSM, catalog, query, and split-state
  code through the data Raft apply/projection owner.

The process-scoped physical-cache change itself removed no compiler files from
distributed: 717 files remained, while declarations changed only from 41,680
to 41,655. Its 381.185-second result is within host variance of the preceding
373.506-second source-only profile. It remains useful only as a prerequisite
to an atomic physical-owner cut and has no independent compiler-time claim.

Decision: **reject the role-placement topology and restore the prior runtime
composition**. Retain its measurements as an upper bound. The next experiment
must move the complete data Raft apply/projection and split/merge storage owner
behind the compiled kernel ABI. Distributed should retain Raft orchestration,
routing, retries, and transition policy, but it must not construct
`RaftApplyStore`, hold `DB` transition leases, or pass backend stores across
the boundary. The kernel operation granularity is one committed apply batch,
snapshot phase, split/merge phase, or bounded projection page--never one record
or one LSM call.

### Phase 4j: opaque data-Raft apply owner foundation

The process-scoped kernel now owns an optional opaque data-Raft apply store.
ABI version 20 adds only lifecycle-sized operations:

- open and close one apply/projection store while borrowing the process storage
  context and its resource manager;
- apply one encoded committed Raft batch;
- build or install one complete group snapshot;
- prepare one MVCC snapshot view on the Raft thread, materialize it to a
  bounded-memory spool file on the worker, cancel it, and destroy it;
- read one copied scalar apply watermark;
- replace the admitted active-group set; and
- begin, commit, abort, and destroy one atomic active-group transition.

The consumer in `storage/data_raft_apply_client.zig` imports only the ABI
contract and `std`. It deliberately does not import the Raft state-machine
implementation to manufacture its callback vtable. A later adapter in the
distributed Raft unit will own that callback composition while delegating the
coarse buffers and scalars to this client.

The owner suite now passes 5/5 with zero leaks. Its new case applies an encoded
committed entry, verifies the latest watermark, commits and aborts placement
transitions, captures and materializes a prepared snapshot, verifies
cancellation, builds a snapshot, installs it into a second store, verifies the
restored watermark, rejects malformed group slices and future ABI versions,
and proves that the shared process context remains busy until both stores
close. The physical data-storage suite passes 57/57 with LMDB compatibility
enabled, including all prepared-snapshot and split-projection tests. The 6/6
provisioned cross-archive suite and 10/10 CAPI suite also pass. The graph gate
treats the new client as a kernel contract so a direct DB, docstore, or backend
import fails validation.

Decision: **keep as an unattached ownership foundation with no compiler-time
claim**. It is not yet valid to redirect `ManagedHttpHost`: data transition
control still calls the store's split projection directly and sometimes passes
a live DB/backend store into it. The next atomic slice must add coarse
projection/reconciliation and split/merge operations, then select the client
for the experimental distributed build in one change. Until that selection
makes the physical `RaftApplyStore` unreachable, do not measure or claim a
distributed graph reduction.

### Phase 4k: opaque data-Raft projection and local-transition owner

ABI versions 21 and 22 complete the data-Raft ownership prerequisite. The
compiled kernel now owns the physical apply/projection store and every DB
operation used by local split and merge coordination. Distributed control
retains Raft orchestration, metadata policy, retry scheduling, transition
admission, and deterministic two-group ordering, but it no longer needs to
reopen a DB already resident in the opaque process owner.

The boundary remains coarse:

- one committed Raft batch, snapshot phase, or bounded projection page crosses
  the data-apply ABI;
- one bounded reconciliation call copies authoritative resident DB state into
  the apply projection without exposing either owner;
- one synchronous local split/merge phase borrows the source/donor and
  destination/receiver handles, executes the existing coordinator entirely in
  the storage unit, and returns only scalar transition facts; and
- immutable table, schema, index, and source/target identity contracts are
  borrowed for the call. No DB, backend store, record, or LMDB primitive
  crosses the boundary.

Pre-bootstrap destinations are opened from the transition's pinned contract,
not from a fresh catalog lookup. This is required because the destination
range is intentionally absent from the catalog before bootstrap. Owner
acquisition remains globally ordered by group ID, while ABI argument order
preserves source/destination and donor/receiver roles. Merge identity
reassignment is performed inside the kernel against the resident receiver.

Focused validation with normal build concurrency passed:

- the cross-archive DataServer suite: 16/16, including complete local split and
  merge fallbacks and zero leaks;
- the opaque owner suite: 8/8 with zero leaks;
- the provisioned owner-source suite: 6/6 with zero leaks;
- complete linked native Debug builds with the experiment enabled and
  disabled, production LSM-only mode, and LMDB compatibility enabled; and
- all three graph boundary gates plus 15/15 analyzer tests.

A fresh-cache native Debug compiler capture compared this complete slice with
the immediately preceding projection baseline:

| Unit | Baseline | Candidate | Repository Zig files | Declarations |
|---|---:|---:|---:|---:|
| Distributed | 77.330 s | 75.980 s | 719 -> 719 | 42,183 -> 41,980 |
| Storage kernel | 51.582 s | 50.882 s | 429 -> 434 | 28,904 -> 29,081 |
| API kernel | 29.152 s | 28.871 s | 326 -> 326 | unchanged |

The distributed file graph and its 1,069,802 repository source lines were
identical. The small time deltas are local noise, while the five files added to
the storage unit are the coordinator implementation now owned there. Therefore
this checkpoint does **not** justify a cold ARM64 Linux musl `ReleaseFast`
build or claim a compiler-time improvement.

Decision: **keep as the completion of the data-Raft/split-merge ownership
prerequisite, with no compiler claim**. It removes the resident-owner conflict
that made the atomic physical-source cut behaviorally impossible. The next
measurement-worthy experiment remains checklist item 7 above: select a
control-only distributed composition in one compile-time switch so the legacy
physical caches, resource owner, local vtables, DB opens, maintenance, and
generation-transition implementations become unreachable together. Do not
measure another individual lifecycle callback.

### Rejected standalone/serverless root relocation

With the data-transition prerequisite complete, a source-selection probe moved
the existing standalone/Lite, serverless, and restore-staging entry roots from
the distributed archive into the separate storage archive. The hidden runtime
ABI, command dispatch, one-executable composition, embedded inference bridge,
and build concurrency were unchanged. Linked native Debug completed and the
`data`, `metadata`, `serverless`, `standalone`, and `lite` help paths all
returned successfully.

The fresh-cache Debug graph moved work but did not reduce the maximum unit:

| Unit | Before | Relocated roots | Repository Zig files | Declarations |
|---|---:|---:|---:|---:|
| Distributed | 75.980 s | 68.658 s | 719 -> 595 | 41,980 -> 37,438 |
| Storage kernel | 50.882 s | 75.229 s | 434 -> 707 | 29,081 -> 40,083 |
| API kernel | 28.871 s | 28.915 s | unchanged | unchanged |

Distributed removed 124 files and 112,065 source lines, but storage added 273
files and 296,744 lines. The maximum compiler unit changed by only -0.751
seconds in Debug, and the two units still analyzed nearly all of the same
physical storage implementation. A cold ARM64 `ReleaseFast` build would not be
a justified use of the measurement loop for this graph.

Decision: **reject and revert**. Relocating source roots is not compiled
deduplication. Standalone currently constructs the complete data, metadata,
API, and Lite implementation in-process, so whichever unit owns its entry root
inherits almost the complete application graph. The next viable boundary must
make standalone a thin composition layer over opaque compiled runtime handles;
moving its existing source body between archives cannot meet the critical-path
or compile-once goals.

### Phase 4l: atomic control-only physical-source cut

The ownership prerequisites now permit the measurement-worthy cut that the
earlier vtable probes could not make. `storage_source_options.control_only` is
a property of each compiler unit rather than a global behavioral flag. The
storage-owning unit selects the physical DB root; the distributed unit selects
contract-only roots for DB types, query validation, document identity, runtime
callbacks, and HA contracts. Data and metadata retain routing, Raft and
transition policy, public/API control, retries, and lifecycle orchestration,
but their local operations enter the one process-scoped kernel owner.

This is an atomic selection. The experimental distributed unit cannot fall
back to its former provisioned DB caches, construct the physical data apply
store, inspect restore state directly, or reopen DBs for split, merge, status,
maintenance, and startup reconciliation. Provider and ordinary test units
continue to select physical storage, which preserves independent legacy
coverage and prevents the build-wide option from accidentally turning the
provider into its own client.

A focused cold ARM64 Linux musl `ReleaseFast` control/API probe established the
source-cut signal before paying for the full archive:

| Metric | Before source selection | Control-only root |
|---|---:|---:|
| Compiler time | 341.1 s | 236 s |
| Peak compiler RSS | 5.19 GiB | measured below 3 GiB in the full composition |
| Emitted text | 21,747,232 B | 16,534,856 B |
| Attributed emitting modules | 309 | 260 |

The focused object no longer emitted the physical DB implementation family.
The complete cold comparison then used fresh local and global caches, normal
build concurrency, the same ARM64 Linux musl `ReleaseFast` target, stripping,
production LSM-only mode, and the same local host:

| Metric | Pre-cut topology | Source-selected candidate |
|---|---:|---:|
| Complete build | 509 s | 458 s |
| Storage kernel | part of 509 s build | 316 s / 4.03 GiB |
| Distributed/API control | part of 509 s build | 233 s / 2.98 GiB |
| Inference | part of 509 s build | 202 s / 3.34 GiB |
| Remote CLI | part of 509 s build | 33 s / 0.74 GiB |
| Final link | part of 509 s build | 12 s / 0.37 GiB |
| Largest compiler RSS | 6.08 GiB | 4.03 GiB |
| Static stripped executable | 71,792,160 B | 65,273,152 B |
| Distributed/API archive | 31,987,980 B | 23,861,542 B |
| Storage archive | 36,675,058 B | 36,705,768 B |
| Inference archive | 22,805,380 B | 22,805,382 B |
| CLI archive | 2,927,158 B | 2,927,160 B |

Both application-critical compilation units are below the preferred
350-second local gate, while the complete build improves by 51 seconds and the
executable shrinks by about 9.1%. The full wall time is intentionally longer
than the largest unit: under the deterministic memory schedule, storage and
distributed start together, then inference starts after distributed and
overlaps the tail of storage. This preserves normal parallel code generation
without exceeding the existing claims. It is not evidence for lowering those
claims before Linux measurements.

The first cold normal-runner measurement, GitHub Actions run `31437306380` at
commit `c4bfc0742`, completed successfully on the unchanged
`arc-antfly-publish` runner. It used the same ARM64 Linux musl `ReleaseFast`
target, production LSM-only mode, normal compiler concurrency, 20 GB Zig
scheduler budget, and 4 GB runner reserve:

| Unit or artifact | Normal-runner result |
|---|---:|
| Storage kernel | 11 m, 10 GB MaxRSS |
| Distributed/API control | 8 m, 8 GB MaxRSS |
| Inference | 7 m, 6 GB MaxRSS |
| Remote CLI | 1 m, 1 GB MaxRSS |
| Thin final link | 12 s, 417 MB MaxRSS |
| Complete archive build | 16:25.14, 9,811,980 KiB process-tree peak RSS |
| Swap | zero |
| Static stripped executable | 65,273,088 B |
| `libantfly.so` | 16,682,104 B |

This proves that the source-selected architecture avoids the original
`bad_alloc` and preserves the artifact gates on the normal runner. It also
invalidates the local timing acceptance: storage and distributed both exceed
380 seconds, so the experiment is not ready to become the production default.
The storage compiler actually peaked at 10,047,467,520 bytes, exceeding its
provisional 8 GiB scheduling claim. Zig discarded and retried that completed
step. The claim is therefore corrected to 10.25 GiB, about 9.5% above the
measurement, while distributed is reduced from 11 GiB to 9 GiB using its
measured 8 GB peak. Their 19.25 GiB initial group fits the unchanged 20,000 MiB
scheduler budget; neither runner size nor CI cost is increased.

The next normal-runner pass captures the authoritative Zig time report for
all four compiler units and uploads their cross-unit overlap summary. That
evidence, rather than the much faster local-host ratios, determines the next
coarse ownership slice.

The first report-capture attempt, run `31448346954`, is not a compiler
measurement. The ARC image did not expose `node` on `PATH`, so all collectors
exited before receiving a report and the diagnostic wrapper interrupted the
still-running storage compiler after its completion-marker timeout. The
workflow now invokes the Node 24 binary bundled with the Actions runner and
fails collector setup immediately; this run neither accepts nor rejects the
architecture.

The corrected capture, run `31450058350` at commit `a018af4b2`, completed on
the normal runner and produced all four authoritative reports. The WebUI
instrumentation build took 18:17.56 wall time and 10,561,540 KiB process-tree
peak RSS with zero swap. Its compiler reports were:

| Unit | Total | LLVM emission | Repository files / lines | Declarations |
|---|---:|---:|---:|---:|
| Storage | 689.361 s | 675.627 s (98.0%) | 714 / 1,057,526 | 39,624 |
| Distributed/API | 501.152 s | 489.937 s (97.8%) | 557 / 848,638 | 31,464 |
| Inference | 487.309 s | 438.081 s (89.9%) | 524 / 579,553 | 24,966 |
| Remote CLI | 68.494 s | 65.435 s (95.5%) | 53 / 39,003 | 5,804 |

Across the four reports there were 1,848 repository-file instances but only
1,203 unique files, leaving 645 duplicate analysis/codegen instances. More
importantly, storage and distributed shared 503 files and 778,101 source lines,
or 90.3% of the smaller distributed graph. Storage alone still analyzed 51 API
files and 201,656 API lines; distributed still analyzed 121 storage files and
246,556 storage lines. The largest aggregate duplicated groups were storage
(119 duplicate instances / 245,781 duplicate lines), API (49 / 200,378), and
HTTPX (72 / 61,823).

This rejects the claim that the first source-selected artifact was already a
storage-only kernel. Its explicit ownership of standalone, Lite, and serverless
composition re-imported most data/API/control code, while the distributed
consumer retained the corresponding control-side storage contracts. The next
coarse experiment therefore moves those product orchestration roots to the
distributed control unit and leaves physical storage, local-query execution,
restore staging, and CAPI exports in the kernel.

That placement compiles without adding a new fine-grained ABI: the existing
control-only provisioned-storage selection already routes its physical
operations through the opaque owner interface. Linked native Debug, all graph
gates, 8 opaque-owner tests, 6 provisioned-source tests, 13 cross-archive data
runtime tests, and 10 CAPI tests pass with zero leaks.

Two clean local ARM64 Linux musl `ReleaseFast` builds measured the composition
move and then corrected its deterministic scheduling edge:

| Metric | Source-selected placement | Composition moved, old gate | Composition moved, storage gate |
|---|---:|---:|---:|
| Complete build | 458 s | 541.61 s | 409.34 s |
| Storage kernel | 316 s / 4.03 GiB | 3 m / 6 GiB | 3 m / 6 GiB |
| Distributed/API/composition | 233 s / 2.98 GiB | 5 m / 9 GiB | 5 m / 7 GiB |
| Inference | 202 s / 3.34 GiB | 3 m / 5 GiB | 3 m / 5 GiB |
| Remote CLI | 33 s / 0.74 GiB | 31 s / 1 GiB | 30 s / 1 GiB |
| Static executable | 65,273,152 B | not retained | 68,996,496 B |
| `libantfly.so` | 16,682,104 B | not retained | 16,553,032 B |

The first moved build waited for the newly longer distributed unit before
starting inference. The corrected graph instead admits inference when the now
shorter storage unit releases its claim, overlapping inference with the tail
of distributed under the same 20,000 MiB scheduler cap and normal compiler
concurrency. That removes 132.27 seconds of avoidable wall time without a
runner-cost change. The storage archive shrinks from 36,705,768 B to
26,022,134 B and distributed grows from 23,861,542 B to 37,985,820 B. The CAPI
remains small, while the executable grows 5.70%, slightly above the approximate
5% guardrail.

An emitted-object audit then supplied the decisive go/no-go evidence. Enabling
per-source sections on the experimental distributed archive changed the final
binary by only -28,768 B (to 68,967,728 B) while making its machine code
attributable. Storage and distributed still emitted 182 of the same Antfly
modules and 8,380,444 duplicate text bytes. The overlap includes the complete
physical family that the boundary is meant to compile once:

| Duplicated module | Duplicate text |
|---|---:|
| `storage.db.db` | 1,630,316 B |
| `storage.db.algebraic.index` | 877,944 B |
| `storage.db.catalog.index_manager` | 755,664 B |
| `storage.db.query.search_exec` | 442,288 B |
| `storage.db.enrichment.enrichment_runtime` | 435,420 B |
| `storage.db.aggregations` | 397,748 B |

Focused, sectioned ARM64 `ReleaseFast` root probes isolated the source of that
failure:

| Root | Compiler result | Duplicate text against storage | Important observation |
|---|---:|---:|---|
| Data + metadata + HA + API control | 209.21 s / 6 GiB | contract/merge overlap only | Does not emit DB core, index-manager, or enrichment-runtime implementations |
| Serverless alone | 107.36 s / 3 GiB | 1,663,456 B | Primarily aggregation/algebraic local-query code |
| Standalone + Lite alone | 229.06 s / 6 GiB | 8,191,824 B | Emits essentially the entire physical DB/index/LSM implementation |

These probes used fresh local caches and the same populated global dependency
cache; their role is root attribution, not a replacement for the cold timing
gate. They prove that moving standalone/Lite source roots is not an opaque
boundary: `standalone/runtime_root.zig` still imports physical DB, Lite, LSM,
erased-backend, and maintenance implementations directly. Serverless is itself
a local-query implementation and belongs with the kernel rather than in the
control unit.

Decision: **revise; do not accept the composition relocation as the ownership
boundary**. Keep it only as opt-in scaffolding for the next slice. Put
serverless local execution back in the kernel, retain data/metadata/HA/API as
the already-small control island, and migrate standalone/Lite physical state
to coarse opaque kernel handles before measuring the combined topology again.
The experimental per-source sections stay enabled so CI can reject source-only
relocations using emitted overlap rather than lexical reachability. The next
candidate must eliminate the DB core, index manager, local query, enrichment,
and LSM modules from the standalone/control object; a faster wall clock alone
is insufficient.

Artifact validation found one stripped static ARM64 executable, a 16,682,104 B
`libantfly.so`, and one 36,705,768 B storage archive reused by both product
links. The storage archive contains no native `mdb_*`, `lmdb_backend`, or
`backend_lmdb` symbol. The C API dynamic symbol table retains public
`antfly_db_open` and `antfly_db_close` while excluding internal storage-owner
and runtime entry points.

Current correctness validation with normal concurrency includes:

- public API parity: 158/158 passed with zero leaks;
- opaque owner ABI: 8/8 passed with zero leaks;
- provisioned owner source: 6/6 passed with zero leaks;
- cross-archive DataServer composition: 13/13 passed with zero leaks;
- four legacy physical split/merge/cache/warmup regressions: 4/4 passed with
  zero leaks;
- five focused metadata server, restore, replication, and apply-store tests;
- a complete linked native Debug build with the experiment enabled;
- all runtime, codegen, and API graph gates; and
- all 15 analyzer tests.

The warmup validation also exposed a stale expectation already present on
`origin/main`: synchronous test-mode startup reconciliation publishes a fresh
snapshot through a transient writer, rather than leaving the earlier synthetic
placeholder. The corrected regression now asserts that authoritative source
and also proves both physical write caches are empty afterward.

Decision: **keep this increment behind the opt-in experiment and revise its
unit composition**. It is the first candidate that makes the ownership
architecture materially true, avoids `bad_alloc`, and shrinks the executable,
but the authoritative Linux unit times fail the performance gate. The
production/default decision remains **revise/pending** while compiler reports
identify the next coarse slice and repeated cold builds prove the result. Do
not enable the option by default, merge it, lower memory claims, or increase
runner cost.

### Phase 4m: coarse aggregation execution and lossless failure ABI

The first accepted post-cut operation moves complete aggregation folding into
the storage kernel. Distributed control still parses and merges the query, but
passes the aggregation request, one borrowed descriptor array for all merged
hit bodies, and a small JSON context through a single synchronous call. The
result remains JSON because it is comparatively small and already matches the
public response model. Document bodies are not copied into an intermediate
JSON request and no per-hit or storage-engine callback crosses the boundary.

The ABI status enum is part of the operation contract, not merely an
ok/failure flag. Expected failures retain stable identities including
`InvalidAggregation`, `UnsupportedAggregation`, candidate-budget exhaustion,
invalid index configuration, algebraic planner/bucket limits, and malformed
algebraic tensor expressions/rows. `internal` is reserved for unexpected
defects. A linked owner test exercises a successful terms fold over borrowed
hits and proves that invalid and unsupported aggregation errors make a full
provider/client round trip without collapsing to a generic storage failure.

ABI version 28 generalizes that rule across every compiled storage boundary.
`runtime_failure_identity.zig` is the single bidirectional registry used
by providers, clients, callback adapters, the data-Raft apply client, and the
low-volume system-store adapter. A status is a stable semantic identity, not a
severity class: for example `LsmRootWriterAlreadyOpen`, `WouldBlock`, and
`StorageBusy`; `Canceled`, `Cancelled`, and `SnapshotBuildCancelled`; the four
backup-integrity failures; and every expected metadata-apply validation or
snapshot failure remain distinguishable. The registry has uniqueness,
round-trip, and status-enum exhaustiveness tests: adding an ABI status without
its inverse error mapping fails the owner suite. A provider error absent from
the registry is an unexpected defect and is the only case converted to
`internal` / `StorageKernelFailure`.

New coarse operations must add their expected error vocabulary to this
registry before exposing an ABI entry point, then test at least one real
provider/client failure round trip. Protocol directives may still use a small
intentional status vocabulary (for example an admission callback), but that
normalization belongs to the protocol definition and must not be reused as a
generic error mapper.

The candidate passed 78 opaque-owner tests, 6 provisioned-source tests, 13
cross-archive data-runtime tests, 43 standalone tests, and 10 CAPI tests with
zero leaks. A populated-dependency ARM64 Linux musl `ReleaseFast` build then
completed all 34 steps with normal concurrency:

| Unit or artifact | Result |
|---|---:|
| Storage kernel | 4 m / 4 GiB MaxRSS |
| Distributed/API control | 3 m / 3 GiB MaxRSS |
| Inference | 3 m / 5 GiB MaxRSS |
| Remote CLI | 32 s / 818 MiB MaxRSS |
| Static stripped executable | 60,434,856 B |
| `libantfly.so` | 16,612,744 B |

The emitted-object comparison is the acceptance signal:

| Metric | Before | Aggregation boundary | Change |
|---|---:|---:|---:|
| Storage/distributed duplicate text | 3,237,340 B | 2,281,040 B | -956,300 B (-29.5%) |
| Distributed object | 29,236,520 B | 27,513,016 B | -1,723,504 B |
| Storage object | 30,495,232 B | 30,570,064 B | +74,832 B |
| `storage.db.algebraic.index` duplicate text | 376,068 B | 1,252 B | -374,816 B |

`storage.db.aggregations` disappears from meaningful overlap. The tiny
remaining algebraic-index attribution consists of shared contract helpers,
not another physical index implementation. This is therefore a **keep**: it
removes real LLVM work, keeps both changed critical units under 380 seconds on
the local comparison, preserves artifact shape, and avoids a fine-grained ABI.
The next measured duplicate is `api.query_contract` at 258,480 text bytes,
followed by the LSM/runtime family.

A follow-up control-only native-storage-pool selector was **rejected**. It
replaced the descriptor pool embedded in the generic background executor while
retaining the same scheduler behavior. All native suites and the full ARM64
build passed, but duplicate text changed only from 2,281,040 B to 2,280,284 B
and distributed shrank just 1,464 B. The LSM runtime, compaction, and recovery
modules remained unchanged because other active control paths still instantiate
that family. The selector was reverted rather than retaining an abstraction
that did not remove LLVM work.

### Phase 4n: metadata apply ownership and replica-root reconciliation

The next ownership slice removes the metadata control runtime's physical
Raft-apply store. A storage-free client now opens one opaque metadata-store
handle and crosses the compiled boundary only for complete committed batches,
snapshots, and bounded projection envelopes. Transactions, cursors, backend
records, snapshot construction, and projection scans remain entirely in the
storage kernel. Projection and committed-key listeners cross through small
reverse callback records; they do not expose a storage handle.

The metadata runtime also needs to reconcile the metadata replica root during
startup. That path is one coarse operation rather than a table-by-table ABI:
control sends one JSON envelope containing the hosted group IDs and the
already-owned table/range snapshots, and the storage owner returns a fixed
summary. This restores normal production startup behavior while keeping the
provisioner and its physical DB graph out of distributed control. The request
is intentionally low-frequency control-plane JSON; it is not a precedent for
per-record storage calls.

Two contract files keep type identity without restoring the implementation
graph. `raft_apply_contract.zig` owns only applied-batch and listener records;
`provision_contract.zig` owns the reconciliation summary. Both the concrete
owner and opaque client alias those declarations. In particular, the client
must not import the 10,000-line concrete apply-store merely to reproduce a Zig
type.

Failure identity is symmetric across this boundary. Every forward provider
result and every failure-bearing reverse callback uses the shared ABI-28
registry. Distinct admission and lifecycle results such as `WouldBlock`,
`StorageBusy`, `ResourceBudgetExceeded`, `Canceled`, and `Cancelled` are not
normalized into one callback failure. Real provider/client tests verify exact
round trips for `InvalidMetadataSnapshot` and
`AppliedSnapshotIndexMismatch`; the earlier aggregation tests continue to
cover `InvalidAggregation` and `UnsupportedAggregation`. The audit also removed
an older lossy hop through the small public-CAPI error enum from the internal
query, batch, and replicated-batch entry points. Their full query/batch/native
format error vocabulary is now registered, and real malformed operations
round-trip as `InvalidQueryRequest` and `InvalidBatchRequest`. Notifications
whose callback signatures cannot fail remain deliberately status-free.

Native validation passed with zero leaks:

- 88 opaque-owner tests, including the status-registry exhaustiveness canary;
- 6 provisioned-source tests;
- 13 cross-archive data-runtime tests;
- 5 focused metadata-runtime restart/projection tests using the normal local
  replica-root hook;
- all 393 default metadata tests across the reconciler, apply store, planner,
  service, provisioner, replication, state, and runtime shards;
- 43 standalone runtime tests and 11 CAPI tests, including a reverse-callback
  identity test;
- all 15 analyzer tests; and
- the runtime, codegen, and API boundary gates.

A fresh Apple-Silicon cross-build to ARM64 Linux musl `ReleaseFast`, with new
local and global caches and normal concurrency, completed without `bad_alloc`
or OOM. It took 445.84 seconds locally; this Darwin cross-build is useful for
artifact and reliability evidence but is not comparable to the normal Linux
runner performance gate. The final lossless query/batch status adjustment was
then rebuilt from that populated cache; the artifact sizes below are final,
while a normal-runner cold build remains the authoritative timing check.

| Metric | Phase 4m | Metadata boundary | Change |
|---|---:|---:|---:|
| Distributed object | 27,513,016 B | 27,172,752 B | -340,264 B |
| Storage object | 30,570,064 B | 31,255,000 B | +684,936 B |
| Conservative storage/distributed duplicate text | 2,281,040 B | 2,309,244 B | +28,204 B (+1.2%) |
| Static stripped executable | 60,434,856 B | 60,663,400 B | +228,544 B (+0.4%) |
| `libantfly.so` | 16,612,744 B | 16,614,968 B | +2,224 B (+0.01%) |

The overlap increase is dominated by shared wire/contract attribution; the
distributed object still loses physical metadata work. Production objects
contain no `storage.lmdb` or `mdb_*` sections. The executable retains legacy
LMDB configuration/metric strings for compatibility, which is not evidence
that the LMDB engine is linked.

Decision: **keep this slice behind the opt-in experiment, pending normal-runner
measurement**. It makes metadata storage ownership real and reduces the
distributed unit, but it is not independently an accepted performance win:
the storage unit grows, and storage is currently the longer compiler unit. A
normal-runner cold build must show that the critical path remains within the
gate. If it regresses materially, revise the serialization/provider shape or
revert this slice rather than enabling it by architectural preference alone.

### Phase 4o: opaque physical-WAL ownership

The next coarse cut moves the production WAL and its physical LSM store out of
distributed control and into the compiled storage owner. HA replication,
fencing, standby apply, slot persistence, and the Raft WAL replica state now
import `storage/wal_runtime.zig`. That facade selects the native WAL only in an
owner compilation and otherwise exposes a storage-free client over one opaque
handle. A source-graph gate rejects any control consumer that imports
`storage/wal.zig` directly while deliberately allowing the facade's native
compile-time branch.

The ABI crosses once per durable log operation: open/close, append, sync,
prefix or suffix truncation, bounded iteration, point read, exact statistics,
and last-LSN inspection. Iteration returns coarse pages bounded at 512 entries
and 4 MiB in production. The provider encodes each page directly while walking
its native streaming cursor, so it does not materialize the full log or invoke
control once per record. Wire framing has explicit magic, version, length, and
trailing-data validation. Tests force one-entry pages to exercise continuation
and ownership even for small fixtures.

Control does not mirror physical WAL state. The provider remains authoritative
for last LSN and every statistics counter. Concurrent append results return
the assigned operation's `lsn + 1`, not a later read of the shared global
cursor; the four-thread ABI benchmark caught this distinction when another
append advanced the physical WAL before the first response was copied.
Empty-timeline repositioning is an explicit bootstrap operation and rejects a
nonempty or unexpectedly positioned log with `WalLsnMismatch`.

ABI version 29 retains the lossless error rule. The shared bidirectional
registry now includes the WAL's declared semantic failures and its expected
operating-system/durability failures: corruption and truncation variants,
unsupported physical formats, record/retention/pressure limits, access and
read-only failures, disk quota/full, I/O, resource exhaustion, and unsupported
durability primitives. These are stable explicit status discriminants and
round-trip to their original Zig errors. Only an error absent from the declared
registry becomes the explicit `internal` / `StorageKernelFailure` sentinel.
Operation state is not overloaded onto that error enum: transaction state,
scan continuation, exact WAL counters, and LSN position retain their own typed
fields and discriminants. Adding an ABI status without an inverse mapping
continues to fail the exhaustive identity canary.

The final populated-dependency ARM64 Linux musl `ReleaseFast` object
comparison shows the intended source removal:

| Metric | Phase 4n | WAL ownership | Change |
|---|---:|---:|---:|
| Distributed object | 27,172,752 B | 26,327,856 B | -844,896 B (-3.1%) |
| Storage object | 31,255,000 B | 31,269,608 B | +14,608 B |
| Conservative storage/distributed duplicate text | 2,309,244 B | 1,854,020 B | -455,224 B (-19.7%) |
| Static stripped executable | 60,663,400 B | 60,121,016 B | -542,384 B (-0.9%) |
| `libantfly.so` | 16,614,968 B | 16,614,968 B | unchanged |

Eleven duplicated native WAL/LSM modules disappear from the emitted-object
overlap report; duplicated modules fall from 122 to 111. The final production
objects contain no `storage.lmdb`, `lmdb_backend`, or `mdb_*` emission.

Behavioral validation passed with zero leaks: 91 opaque-owner tests, 20
experiment-enabled data/HA tests, 5 metadata-runtime tests, 6 provisioned-source
tests, 275 native HA tests, 196 Antfly Raft tests, 354 standalone Raft-library
tests, 43 standalone tests, and 11 CAPI tests. The experiment-disabled full
x86_64 Linux GNU Debug executable also compiles. Analyzer unit tests and all
three source-graph gates pass.

The existing four-thread WAL workload was run as four back-to-back native and
opaque pairs (2,048 256-byte appends, adaptive backend, no sync). Native versus
opaque averages were 815.752 versus 825.546 ms for the plain case (+1.2%) and
901.688 versus 910.411 ms for grouped commit (+1.0%). That is within local host
variance and is not a meaningful throughput regression. The exercise also
fixed the benchmark's pre-existing defer order so it now closes the WAL before
deleting its temporary directory.

Decision: **keep this slice behind the opt-in experiment**. It removes a real
physical implementation family, shrinks the executable, leaves the CAPI
unchanged, preserves exact results and failure identities, and introduces no
measurable WAL regression. A genuinely empty-cache Apple-Silicon cross-build
to ARM64 Linux musl `ReleaseFast` then completed with normal scheduling in
468.48 seconds without `bad_alloc`, OOM, swap, or a compiler-protocol failure.
The first large-unit wave ran concurrently at observed RSS of approximately
3.6 and 3.8 GiB; the remaining large unit was approximately 2.9 GiB. Its
objects and executable exactly match the sizes above. The normal Linux runner
cold build remains the authoritative per-unit timing gate before enabling the
experiment by default.

The first authoritative normal-runner build after this slice completed
successfully in GitHub Actions run `31518438580`, job `93869276925`, on the
ordinary `arc-antfly-publish` runner. All 34 build steps and artifact checks
passed. The build shell took 16 minutes 34 seconds, peak cgroup RSS was
9,337,216 KiB, swap remained zero, and the final static executable and C API
library were 60,120,968 B and 16,615,400 B respectively. The compiler reports
showed:

| Unit | Real time | LLVM emission | Repository graph | Declarations |
|---|---:|---:|---:|---:|
| Storage kernel | 530.153 s | 519.188 s | 588 files / 917,498 lines | 33,392 |
| Distributed/API control | 394.388 s | 384.861 s | 542 files / 762,997 lines | 28,499 |
| Inference | 427.659 s | 384.039 s | 524 files / 580,405 lines | 24,994 |
| Remote CLI | 60.469 s | — | — | — |

This is important positive reliability evidence: the ordinary runner completed
a cold ARM64 musl `ReleaseFast` archive without OOM, swap, cache priming, a
larger runner, or serialized compilation. It is also a performance rejection
for the current unit composition. Storage and distributed both exceed the
380-second gate, and storage is now the critical compiler unit. Across the
compiler reports there are 1,707 repository-file instances, 1,217 unique
files, and 490 duplicate instances; conservative storage/distributed emitted
text overlap is 1,854,020 B. The option therefore remains experimental.

### Phase 4p: physical-root isolation and rejected product splits

Three measurement-only ARM64 Linux musl `ReleaseFast` roots separated the
physical owner/CAPI core from the serverless and Lite product roots. They used
normal scheduling and the same production storage options:

| Probe | Local real time | Peak RSS | Object size |
|---|---:|---:|---:|
| Physical storage owner + CAPI core | 226.48 s | 5 GiB | 26,804,064 B |
| Serverless physical runtime | 122.91 s | 2 GiB | 13,459,256 B |
| Lite physical runtime | 174.36 s | 3 GiB | 20,826,728 B |
| Complete storage/product root | 261.36 s | 6 GiB | — |

Removing the product roots saves only 34.88 seconds locally, or 13.3%. Applied
to the normal-runner result, the physical core would still project far above
380 seconds. More importantly, compiling the three roots independently emits
507 repository-module instances but only 275 unique modules. The 167 repeated
modules contain 8,949,496 B of duplicate text, led by `storage.db.db`,
`storage.db.algebraic.index`, `storage.db.catalog.index_manager`, local search,
enrichment, and aggregations. A separate physical serverless or Lite library
would therefore trade a modest critical-unit reduction for another optimized
copy of the storage implementation and substantial artifact growth.

Decision: **reject the three physical product libraries**. Serverless and Lite
may become thin consumers of one owner, but neither may instantiate its own DB,
index, or query graph. The next substantial experiment must split within the
physical owner around an independently owned local index/query subsystem, or
otherwise remove a comparable operation family from the storage LLVM module.
It must use opaque handles and coarse batch/query/lifecycle calls; no record,
posting, candidate, or backend call may cross the ABI.

The failure contract applies unchanged to that internal physical split. Every
declared provider failure must receive a stable `Status` discriminant in
`runtime_failure_identity.zig` and must map back to the exact original Zig
error at the consumer. Distinct identities such as `ReadOnly`,
`HAReadOnlyStandby`, `WouldBlock`, `StorageBusy`, `Canceled`, and `Cancelled`
must not be normalized for convenience. Operation state—including admission,
continuation, lifecycle phase, cancellation observation, and partial progress—
uses separate typed fields or tagged results and is never smuggled through a
generic status. Only an unregistered provider defect or an impossible ABI
shape becomes `internal` / `StorageKernelFailure`. Each new operation family
requires an exhaustive-registry canary plus at least one real provider/client
failure round-trip test before it can be measured or kept.

### Phase 4q: enrichment isolation probe

A decoder-only root for document extraction, PDF text/rendering, and media
decoding compiled in 23.92 seconds locally and emitted a 1.4 MB object. That is
too small to justify a new compiled boundary by itself. A second root exercised
the complete production enrichment catch-up path. With function sections it
compiled in 48.40 seconds and emitted 2,916,916 B of text in a 6.3 MB object.
Its Antfly-attributed graph contained only 23 modules and 759,613 B, including
495,732 B from `enrichment_runtime`, 84,964 B from document extraction, and
only 60,028 B from `IndexManager`; the remaining roughly 2.16 MB of text is
media, archive, HTTP, template, and other external compute machinery. It does
not instantiate DB core.

This is a plausible parallel compute island, but the existing runtime cannot
be compiled across an ABI unchanged. It performs 105 direct store, scan,
catalog, checkpoint, replay-source, writer, and failure-ledger interactions.
Wrapping those calls one-for-one would create a backend-shaped ABI, lose
atomicity, and make the boundary chatty. That shape is **rejected**.

The only acceptable enrichment experiment keeps replay collection, leases,
catalog/index ownership, durable failure debt, coverage transitions,
checkpoints, and atomic derived writes in storage. A separate compute unit may
receive one bounded replay-window descriptor containing source documents,
resolved enrichment plans, and immutable configuration, then return one owned
derived batch with per-request outcomes. Embedding and asset-producer reverse
calls must preserve the existing batch sizes. It may not request one store key,
document, or index lookup at a time.

The response has two independent identity domains:

- the call-level `Status` identifies ABI, allocation, cancellation, and
  unexpected provider failure; and
- every item outcome is a tagged success, retryable failure, terminal failure,
  or skipped result carrying the exact registered semantic status.

Retryability and lifecycle state are explicit fields, not inferred by merging
errors into a generic `busy`, `retry`, or `internal` value. The storage
consumer maps every item status back to its original Zig error before applying
the existing retry/failure-ledger policy. Unknown statuses and impossible wire
shapes remain defects. This experiment is worth implementing only as a complete
bounded-window operation; the 24-second decoder-only adapter and direct-store
callback variants are not candidates.

Callbacks form a third identity domain, independent of both response domains.
When consumer code invoked by a provider callback returns an error, the
consumer adapter records that exact Zig error in call-scoped state and returns
only a callback-failed protocol sentinel to unwind the provider. After the
provider returns, the consumer rethrows the recorded error. The sentinel must
never escape as the apparent operation failure, and a provider failure must
never overwrite a callback error that already occurred.

An undeclared provider error is a contract defect rather than a new implicit
public status. It maps to `internal` for control flow, but the result also
carries bounded diagnostic identity (the original provider error name, a
stable hash of the complete name, and the operation/boundary version) for logs
and failure-ledger evidence. Consumers do not branch on this diagnostic
payload. If the error is expected and actionable, the next ABI revision must
assign it a stable registered status and add the bidirectional round-trip test.
This retains operational identity without pretending arbitrary
compiler-assigned Zig errors are a stable ABI.

### Phase 4r: boundary identity hardening

Before adding another physical split, the shared ABI now defines one reusable
`FailureIdentity`: a stable semantic `Status`, stable originating
`FailureBoundary`, boundary version, operation ID, bounded readable provider
error name, truncation bit, and stable hash of the complete name. Declared
errors still round trip through the exhaustive bidirectional registry. An
undeclared defect remains `internal` for control flow, but no longer becomes
operationally indistinguishable from every other provider defect in
diagnostics.

ABI 32 makes nested propagation explicit. Every provider returns a canonical
envelope on both success and failure, and every consumer validates the status,
version, origin, operation, name/hash pair, truncation marker, reserved bytes,
and zero padding before trusting it. A valid inner envelope is forwarded
unchanged through an outer provider: a local-query failure therefore remains
`local_query` after crossing the storage-owner ABI. An outer wrapper creates a
new identity only for an error it originated. If it detects malformed provider
data, it logs the raw untrusted envelope and emits a new registered
`invalid_boundary_failure_identity` failure at its own validation stage; it
never forwards malformed bytes or silently substitutes the inner status.

These rules preserve two different meanings without conflating them. Stable
registered statuses are the executable control-flow contract and map back to
the exact declared Zig error. Arbitrary undeclared Zig errors cannot safely be
reconstructed from compiler-assigned values across a C ABI, so `internal`
remains the control-flow status while the exact original name, full-name hash,
origin, and stage remain available for diagnostics and durable failure
evidence. The public C API may still deliberately translate this internal
contract into its documented coarser `ErrorCode`; that is an explicit product
boundary, not accidental loss inside the compiled architecture.

The audit also removed an existing context-dependent identity translation:
write-admission backpressure previously crossed as generic `busy` and the
client guessed `DenseRepairBackpressure` from the operation being called. ABI
30 assigns `dense_repair_backpressure` its own registered status, so
`StorageBusy` and `DenseRepairBackpressure` remain distinct in both directions.

The first synchronous reverse-callback adapter uses a consumer-owned
`CallbackErrorRelay`. Bulk-finish admission stores the exact `anyerror`,
returns `storage_kernel_callback_failed` only to unwind the storage provider,
receives the raw provider status, and rethrows the stored error before ordinary
status translation. The test models the complete callback-status -> provider
error -> exported-status round trip with an error that is intentionally absent
from the shared registry. A second registry canary proves that a different
provider status cannot overwrite the first callback failure. Process-level
runtime entry points also print the exact originating error name even though
their deliberate external contract remains a success/failure exit code.

This is a bounded prerequisite rather than a compiler-graph experiment. It
adds no implementation import to a consumer unit and changes no existing
exported function signature. Every new coarse operation must use this shape
for call failures and use the same semantic identity inside per-item tagged
outcomes; it must not introduce a boundary-specific broad error mapper.

### Phase 4s: document/media compute island

The first post-hardening physical split moves document extraction, archive
decoding, PDF text extraction, and PDF page rendering into one separately
compiled PIC unit. Storage still owns remote-download policy, replay windows,
catalog and index access, generated-text batching, resource accounting,
manifests, failure debt, checkpoints, and atomic derived writes. The provider
receives one already-downloaded bounded source plus immutable configuration
and returns extraction metadata followed by JSON batches capped at 64 units or
4 MiB. No store key, document lookup, index operation, or backend call crosses
the boundary.

Both production extraction roots use the compiled provider under the opt-in
experiment: the three-pass managed enrichment path and the direct DB/CAPI
precompute path. PDF OCR rendering crosses the same compute boundary. Native
implementations remain compile-time fallbacks for tests and for the disabled
experiment, rather than runtime fallbacks that could emit a second production
copy.

ABI 31 introduced the Phase 4q/4r identity contract for this boundary; ABI 32
adds the stable `enrichment_compute` origin and canonical consumer validation:

- expected extraction/config/archive failures have distinct registered
  statuses and map back to the original Zig error;
- every provider failure returns `FailureIdentity`, so an undeclared defect
  retains its exact bounded name, full-name hash, boundary version, and
  operation ID even though control flow remains `internal`;
- extraction manifests and per-unit PDF warnings consume that diagnostic
  identity instead of replacing it with `StorageKernelFailure`; and
- callback failures remain consumer-owned and rethrow the exact original
  `anyerror`; the provider unwind sentinel is cleared from the diagnostic
  channel and cannot overwrite that identity.

The first genuinely empty-cache Apple-Silicon cross-build to ARM64 Linux musl
`ReleaseFast` produced these compiler reports:

| Unit | Compiler time | LLVM emission | Repository graph | Declarations |
|---|---:|---:|---:|---:|
| Storage kernel | 320.571 s | 314.118 s | 589 files / 918,103 lines | 33,331 |
| Distributed/API control | 245.387 s | 239.775 s | 542 files / 763,217 lines | 28,509 |
| Inference | 230.676 s | 223.670 s | 524 files / 580,416 lines | 24,999 |
| Enrichment compute | 18.428 s | 17.364 s | 34 files / 55,694 lines | 3,275 |
| Remote CLI | 39.453 s | 37.416 s | 53 files / 39,130 lines | 5,807 |

The first profile still made inference wait for storage and took about 598
seconds end to end. The dependency gate now admits inference after distributed
finishes: storage's 10.25 GiB claim plus inference's 8 GiB claim remains below
the unchanged 20,000 MiB budget, while the scheduler holds the short 4 GiB
enrichment claim when necessary. A second empty-cache build of both `install`
and `capi` then completed all 40 Zig steps in 424.44 seconds. Its summary
reported storage at 4 minutes / 3 GiB, distributed at 3 minutes / 3 GiB,
inference at 3 minutes / 4 GiB, enrichment at 15 seconds / 783 MiB, and CLI at
33 seconds / 874 MiB, with no discarded or retried compiler step.

The stripped output remains one static executable. It is 61,367,008 B, a
2.1% increase from the 60,120,968 B normal-runner checkpoint and below the 5%
gate. `libantfly.so` is 17,877,824 B, up 1,262,424 B but still within the
documented 16--18 MB range. Both enrichment and storage archives are PIC and
shared by the executable and CAPI libraries. Dynamic-symbol inspection exposes
neither internal storage/enrichment entry points nor LMDB symbols.

Behavioral validation currently includes 91 storage-owner tests, 11 public
CAPI tests, five cross-archive enrichment tests, all three graph gates, and all
17 analyzer tests, with zero leaks. The enrichment suite covers owned text
results, PDF text/regions/rendered PNG bytes, a declared provider error, an
undeclared provider diagnostic identity, and an intentionally unregistered
consumer callback error.

Decision: **keep this slice behind the opt-in experiment and send it to the
normal runner**. It clears the local per-unit compiler gate and is the first
physical split to reduce both large units while adding only an 18-second
island. It does not yet enable the experiment by default: normal-runner
reliability/time/RSS, the complete archive checks, and representative runtime
throughput remain authoritative. The next graph experiment should use the
fresh emitted-object ranking rather than broaden this ABI into storage-shaped
callbacks.

The authoritative normal-runner build then passed in GitHub Actions run
`31530408426`, job `93908799932`, at commit `9bea66b75`. All 38 build steps,
the static ARM64 musl executable, the shared C API, symbol checks, and artifact
upload succeeded on the unchanged runner with normal concurrency. The live
cgroup peak was 16,655,601,664 B (15.51 GiB), every cgroup OOM counter remained
zero, and swap remained zero. The final executable was 61,366,960 B and
`libantfly.so` was 17,877,808 B.

The compiler reports were materially slower than the empty-cache local result:

| Unit | Compiler time | LLVM emission | Repository graph | Declarations |
|---|---:|---:|---:|---:|
| Storage kernel | 615.200 s | 602.403 s | 589 files / 918,103 lines | 33,324 |
| Distributed/API control | 459.376 s | 448.265 s | 542 files / 763,217 lines | 28,502 |
| Inference | 498.361 s | 446.889 s | 524 files / 580,416 lines | 24,994 |
| Remote CLI | 72.376 s | 68.375 s | 53 files / 39,130 lines | 5,804 |

The enrichment summary reported 34 seconds / 1 GiB; its first workflow did not
capture the new WebUI JSON report, which the follow-up workflow now corrects.
The complete archive took 17:52.22. Storage and distributed began together;
inference became eligible after distributed and overlapped the storage tail.
The resulting CPU contention inflated every large compiler unit relative to
the prior normal-runner control and did not improve the end-to-end critical
path.

Revised decision: **keep the physical compute boundary, reject this schedule
as a performance solution, and continue the goal loop**. The run is strong
reliability evidence and the boundary preserves exact failure identity, but it
does not satisfy the 380-second compiler-unit gate. Do not enable the experiment
by default or increase runner cost. Use the newly captured enrichment report
and emitted-object ranking to select the next coarse source cut; separately
measure a schedule that avoids contending two LLVM-heavy units before accepting
any concurrency-policy change.

### Phase 4t: compiled local-query island

The next representative cut gives one separately compiled PIC unit a borrowed
opaque `DB` for the duration of one complete local operation. Storage retains
DB/index lifecycle, admission, writes, snapshots, restore, and maintenance.
The provider owns request parsing, local planning/search, result shaping, and
wire encoding. No posting, candidate, stored-document, index, or backend handle
crosses the ABI. Distributed routing and consistency admission remain outside.

The first slice moved internal/public search used by the provisioned owner,
Lite, and C API. A genuinely cold Apple-Silicon cross-build to ARM64 Linux musl
`ReleaseFast` produced:

| Unit | Compiler time | LLVM emission | Repository graph | Declarations |
|---|---:|---:|---:|---:|
| Storage kernel | 307.033 s | 300.697 s | 590 files / 918,757 lines | 33,438 |
| Distributed/API control | 234.038 s | 228.535 s | 542 files / 763,635 lines | 28,523 |
| Local query | 53.399 s | 52.017 s | 204 files / 409,930 lines | 9,486 |

On the same host, the prior combined distributed/storage control was 358.589 s,
350.840 s LLVM, 643 files, 1,024,169 lines, and 40,313 declarations. The storage
candidate therefore removed 51.556 s (14.4%), 53 loaded files, 105,412 source
lines, and 6,875 declarations from that critical unit. The local-query island
itself remained a small 53.4-second unit and the build retained normal
concurrency.

The first linked artifacts exposed the next acceptance issue. After enabling
function/data sections on the new PIC unit, the stripped static executable was
65,001,944 B and `libantfly.so` was 19,932,656 B, versus the preceding
61,366,960 B and 17,877,808 B checkpoint. Section GC changed only a few
kilobytes, proving the C API was reaching the added implementation rather than
accidentally retaining one monolithic object. Emitted-object analysis found
4,014,762 duplicate allocatable bytes across storage, distributed, and local
query. The leading duplicate implementation families were local search,
query-contract lowering, algebraic planning, DB search support, and graph
execution.

The attempted broadening moved text statistics, algebraic partials, graph
expand/hydrate/edges, and the complete aggregation fold into the same provider.
A second cold build rejected it:

| Unit | Search-only | Broadened | Change |
|---|---:|---:|---:|
| Storage kernel | 307.033 s / 33,438 decls | 315.894 s / 33,141 decls | +8.861 s / -297 decls |
| Distributed/API control | 234.038 s | 240.534 s | +6.496 s |
| Local query | 53.399 s / 9,486 decls | 78.785 s / 10,633 decls | +25.386 s / +1,147 decls |

The broadened executable grew to 65,885,368 B and `libantfly.so` to
20,427,296 B. Only 8,122 loaded source lines left storage. Those operation
exports were not the roots retaining the large implementations, so the change
increased aggregate LLVM work and artifact size without improving the critical
unit.

A follow-up capability probe tested whether the generic `Engine.search`
function pointer was the hidden root. Moving it to a separate typed query
vtable produced an identical 590-file graph, increased declarations from
33,438 to 33,446, and changed cold storage time only from 307.033 to 302.983
seconds. Removing the pointer entirely produced a byte-for-byte identical
optimized storage object, proving that the unused vtable was already lazy and
not the retention edge. Both API variants were reverted.

A unit-scoped compile-error trace then identified the first real production
edge precisely: `storageOwnerGraphExpandJson` ->
`executeStorageKernelGraphExpand` -> `DB.search`. This explains why moving only
the ordinary query route could not remove `search_exec`, and why the partial
graph broadening above duplicated work: public CAPI graph/search operations
still retained the storage copy. A future retry must migrate the complete set
of storage-owner and public-CAPI query roots atomically, or it is not a valid
deduplication experiment.

Interim decision: **revert the broadening and continue revising the search-only cut**.
Do not add another library. Text statistics, algebraic partials, graph
operations, and aggregation remain in storage until a future ownership change
can remove their actual roots rather than duplicate them. Their newly added
failure envelopes are retained independently because they fix semantic ABI
behavior without moving implementation or adding a compiler unit. The
search-only boundary remains opt-in and unaccepted until the C API returns to
its established size envelope or a subsequent cut proves the growth is the
necessary cost of one reused implementation.

ABI 32 introduced one failure rule for the original search slice and the
audited storage operations. The envelope contains stable semantic status,
originating boundary, ABI version, append-only operation stage, exact bounded
Zig error name, truncation marker, and full-name hash. Storage wrappers forward
valid nested `local_query` failures unchanged; operations that still execute in
storage retain `storage_owner` as their true origin rather than claiming to
have crossed the local-query provider.
Malformed envelopes become a new `invalid_boundary_failure_identity` at the
consumer validation stage after the raw bytes are logged. Call-level failure,
per-item outcome, callback failure, cancellation state, and partial progress
remain independent channels. Focused tests exercise a nested search parse
failure plus algebraic, graph, and aggregation failures and assert each true
origin and stage at the boundary it actually crossed.

The atomic retry followed the compiler trace rather than merely broadening the
public surface. It moved graph expand/hydrate/edges, text statistics, algebraic
partials, and query preflight together because each was an actual storage root
of `DB.search` or `searchComposed`. Aggregation remains in storage because it is
not such a root. A second compile-error trace found the less obvious remaining
path: `storageOwnerPreflightJson` -> `DB.preflightSearchRequest` -> planning
statistics -> `searchLocked` -> `searchComposed`. After moving that complete
preflight operation, both sentinels stopped firing and the storage object
emitted zero bytes for `storage.db.query.search_exec`.

ABI 33 extends the local-query operation family without weakening the Phase
4r contract. Each new operation returns the same canonical `FailureIdentity`.
The storage wrapper validates it and forwards a valid nested `local_query`
identity byte-for-byte; only errors produced by the wrapper itself receive a
new `storage_owner` identity. Malformed envelopes become the registered
`InvalidBoundaryFailureIdentity` protocol failure rather than inheriting either
the inner or outer status. The 92-test owner suite verifies malformed search,
graph, text-statistics, algebraic, and preflight requests retain matching
status, original boundary/version, exact operation stage, bounded name, and
full-name hash. The 11 CAPI tests and seven enrichment-boundary tests pass as
well, with zero leaks. The experiment-disabled linked Debug executable also
builds successfully.

A genuinely cold Apple-Silicon cross-build to ARM64 Linux musl `ReleaseFast`
with normal concurrency produced:

| Unit | Search-only | Atomic query/preflight | Change |
|---|---:|---:|---:|
| Storage kernel | 307.033 s / 300.697 s LLVM | 278.304 s / 272.404 s LLVM | -28.729 s / -28.293 s |
| Distributed/API control | 234.038 s / 228.535 s LLVM | 232.290 s / 226.702 s LLVM | -1.748 s / -1.833 s |
| Local query | 53.399 s / 52.017 s LLVM | 61.716 s / 60.022 s LLVM | +8.317 s / +8.005 s |

Storage fell from 590 repository files / 918,757 lines / 33,438 declarations
to 580 files / 905,892 lines / 31,440 declarations. The local-query island grew
from 204 files / 409,930 lines / 9,486 declarations to 215 files / 460,659
lines / 10,297 declarations, but remains far below the 278-second critical
unit. Its scheduling edge follows storage to avoid admitting a third compiler
into the initial storage/distributed wave, so their measured compiler chain is
340.020 seconds while local query overlaps later work. The other isolated
units completed at 222.862 seconds for inference, 38.107 seconds for CLI, and
17.349 seconds for enrichment. The build finished all 43 steps without
`bad_alloc`; reported unit peaks were 4 GiB for storage, 3 GiB for distributed,
5 GiB for inference, and 2 GiB for local query.

The optimized storage object is 28,538,760 B and the local-query object is
6,324,152 B. Conservative emitted overlap across storage, distributed,
local-query, and enrichment is 3,552,673 B, below the search-only experiment's
4,014,762 B even though the latter did not include enrichment. The stripped
static executable shrank from 65,001,944 B to 63,312,104 B. `libantfly.so`
grew from 19,932,656 B to 20,495,368 B because its public search root reaches
the shared provider object; this is 19.55 MiB and remains below the explicit
20 MiB retained-code gate. Splitting search and controlled operations into two
exported functions did not permit section GC across the shared compiled object
and made the library 480 bytes larger, so that micro-experiment was reverted.
Dynamic-symbol inspection exposes `antfly_db_open` and none of the private
runtime, local-query, storage-owner, snapshot, restore, or data-apply ABI.

Decision: **keep the atomic query/preflight migration as the current
candidate**. It is the first local-query revision that removes the physical
search implementation from storage, lowers the local cold critical unit below
350 seconds, reduces aggregate emitted overlap, and shrinks the executable.
The provider consumes the same coarse JSON operation wires that these paths
already parsed and encoded; it adds no per-document, candidate, index, or
backend ABI crossing. It remains opt-in until the normal Linux runner confirms
ReleaseFast reliability, time/RSS, archive shape, and representative runtime
behavior. Do not raise runner cost or enable the experiment by default based
only on the local result.

The authoritative normal-runner build then passed in GitHub Actions run
`31547528877`, job `93963102627`, at commit `e931ad3af`. All 43 build steps,
artifact and private-symbol checks, and artifact upload succeeded with normal
concurrency on the unchanged 24,000 MiB ARC runner request. The release
remained one stripped, statically linked ARM64 musl executable. The compiler
reported zero swap and no `bad_alloc`; the largest individual GNU-time RSS was
8,251,228 KiB. This workflow did not sample cgroup-wide peak or event counters,
so the earlier cgroup measurements must not be attributed to this run.

| Unit | Compiler time | LLVM emission | Repository graph | Declarations | Zig MaxRSS |
|---|---:|---:|---:|---:|---:|
| Storage kernel | 590.707 s | 579.024 s | 580 files / 905,892 lines | 31,433 | 8 GiB |
| Distributed/API control | 482.636 s | 471.354 s | 542 files / 763,766 lines | 28,521 | 7 GiB |
| Local query | 127.258 s | 123.090 s | 215 files / 460,659 lines | 10,290 | 3 GiB |
| Inference | 528.849 s | 473.900 s | 524 files / 580,424 lines | 24,994 | 7 GiB |
| Remote CLI | 75.569 s | 71.049 s | 53 files / 39,138 lines | 5,804 | 2 GiB |
| Enrichment compute | 35.579 s | 34.298 s | 34 files / 55,942 lines | 3,276 | 1 GiB |

The complete build command took 19:23.59 and the Actions job took 20:24. The
storage-then-local-query compiler chain was 717.965 seconds, while distributed
and inference independently remained above 380 seconds. The runner result
therefore confirms reliability and the intended source cut, but rejects the
current six-unit schedule as a solution to the performance goal. Linux CPU
contention, not memory admission, remains the dominant discrepancy from the
340.020-second local chain.

The final executable was 63,312,024 B and `libantfly.so` was 20,495,352 B,
consistent with the local artifacts and below the 20 MiB CAPI gate. The graph
report contained 1,948 compiler file instances, 1,221 unique files, and 727
duplicate instances. Production storage still emits no
`storage.db.query.search_exec`, so this is not a regression to duplicate local
search implementation.

Revised decision: **keep the identity-safe local-query boundary opt-in, reject
the measured scheduling/composition as the final production build, and
continue the goal loop**. Do not enable the experiment by default or increase
runner cost. The next experiment must reduce or avoid competing LLVM-heavy
units on this two-vCPU runner while preserving normal concurrency; merely
rescheduling the same 590-second storage and 529-second inference work cannot
satisfy the compiler-time gate.

### Phase 4u: co-generated storage and local query

The next composition experiment keeps the complete local-query ABI but emits
its provider in the PIC storage kernel instead of a separate static library.
This is not an inlining or source-level fallback: distributed control and the
storage wrappers still call the same hidden `antfly_local_query_*` symbols with
the same opaque borrowed DB handle, request/result wire, and ABI 33
`FailureIdentity`. Valid nested failures therefore retain status, originating
boundary, operation, exact bounded Zig error name, truncation bit, and full-name
hash exactly as in Phase 4t. Only the archive that defines the provider symbols
changes.

The standalone local-query artifact remains test-only for the focused CAPI
root, which intentionally owns its DB directly. It is not linked or scheduled
by release outputs. A cold Apple-Silicon cross-build to ARM64 Linux musl
`ReleaseFast`, with normal concurrency and fresh local/global caches, produced:

| Unit | Separate provider | Co-generated provider | Change |
|---|---:|---:|---:|
| Storage / storage+query | 278.304 s + 61.716 s | 313.334 s | -26.686 s chain |
| Distributed/API control | 232.290 s | 238.758 s | +6.468 s |
| Inference | 222.862 s | 237.220 s | +14.358 s |
| Remote CLI | 38.107 s | 39.342 s | +1.235 s |
| Enrichment compute | 17.349 s | 17.503 s | +0.154 s |

The co-generated storage/query graph is 591 repository files / 919,247 lines /
33,475 declarations, with 306.555 seconds (97.8%) in LLVM emission. That is
larger than storage alone, as intended, but materially smaller than optimizing
the storage and query graphs separately. The complete five-report graph has
1,744 repository-file instances, 1,221 unique files, and 523 duplicate
instances, versus 1,948 / 1,221 / 727 on the six-unit normal-runner report.
Conservative emitted overlap across storage, distributed, and enrichment fell
from 3,552,673 B to 1,947,522 B. A remaining 47,234 B duplicate
`storage.db.query.search_exec` contribution is still attributable across
storage and distributed composition; the large physical implementation is no
longer duplicated as its own query compiler unit.

The stripped static executable is 61,399,552 B, down 1,912,552 B. The stripped
shared C API is 18,460,704 B, down 2,034,664 B and back well inside its
historical size range. The combined storage object is 31,373,344 B, versus
34,862,912 B for the prior storage and local-query objects together. The
artifacts remain ARM64 musl, the executable remains statically linked, and the
CAPI dynamic-symbol audit exposes none of the hidden runtime, local-query,
storage-owner, snapshot, restore, or data-apply symbols.

All 39 linked Debug build steps, 92 storage-owner tests, and 11 CAPI tests pass
with zero leaks. The three import/codegen gates, 19 analyzer/patch tests,
workflow lint, and experiment-disabled behavior also remain required before a
keep decision. The owner tests continue to exercise exact provider failure
identity and consumer attribution for malformed envelopes, demonstrating that
co-generation has not collapsed the semantic boundary.

Decision: **keep the co-generated provider as the next opt-in runner
candidate**. It removes an LLVM unit, improves the local critical chain, cuts
both analyzed and emitted duplication, and reverses the CAPI size growth while
preserving the designed ABI. It remains opt-in pending normal-runner timing,
RSS, artifact, and repeated reliability evidence; do not enable it by default
or raise runner cost based on the local result.

The first normal-runner candidate build then passed in GitHub Actions run
`31550210130`, job `93971084675`, at commit `8a21611c5`. All 39 build steps,
artifact/private-symbol checks, graph analysis, memory report, and upload
succeeded with normal concurrency on the unchanged 24,000 MiB ARC request.
The immediately preceding documentation-only run `31549077822` rebuilt the
six-unit commit on the same runner class, giving a closely matched cold control:

| Unit | Six-unit repeat | Five-unit co-generation | Change |
|---|---:|---:|---:|
| Storage / storage+query | 546.593 s + 117.818 s | 599.665 s | -64.746 s chain |
| Distributed/API control | 447.855 s | 445.754 s | -2.101 s |
| Inference | 486.614 s | 485.749 s | -0.865 s |
| Remote CLI | 69.741 s | 69.454 s | -0.287 s |
| Enrichment compute | 33.479 s | 33.347 s | -0.132 s |

The unchanged units are within 2.1 seconds of the repeat control, isolating the
64.746-second improvement to co-generation rather than runner variance. The
combined unit spent 587.510 of 599.665 seconds (98.0%) in LLVM emission and
reported 9 GiB MaxRSS, within its unchanged 10.25 GiB claim. Whole-build GNU
time reported 8,906,344 KiB maximum RSS and zero swap. As with Phase 4t, this
workflow did not sample cgroup-wide peak or event counters.

The build command took 17:21.73 versus 17:23.92 for the repeat control. The
improved storage/query path does not reduce end-to-end wall time yet because
the 445.754-second distributed unit followed by 485.749-second inference unit
is now the critical scheduled path. Co-generation nevertheless removes a real
compiler unit and 204 duplicate file instances rather than moving time between
unrelated units.

The runner reproduced the local graph exactly: 1,744 file instances, 1,221
unique files, 523 duplicate instances, and 1,947,522 B conservative emitted
overlap. The final executable was 61,399,488 B and `libantfly.so` was
18,460,688 B. Both retained the expected ARM64 artifact shape, the executable
was one stripped static musl binary, no private local-query/storage symbols or
LMDB implementation leaked, and the CAPI remained below its size budget.

Revised decision: **keep co-generation as the current opt-in architecture, but
continue the goal loop**. It passes the ownership, identity, graph, artifact,
memory-claim, and first-run reliability gates. It does not complete the main
goal because the 599.665-second storage/query unit still exceeds 380 seconds
and overall wall time is now governed by distributed followed by inference.
The next substantial experiment should remove inference implementation roots
that remain in distributed control while preserving inference as its own
compiled safety boundary; rescheduling the same units cannot remove the
measured LLVM work.

### Phase 4v: shared failure identity and inference lifecycle hardening

Failure identity is a runtime-wide ABI property, not storage-owned policy.
ABI 34 moves the canonical append-only `Status`, `FailureBoundary`, and
`FailureIdentity` declarations into the dependency-neutral
`runtime_failure_abi.zig` module. Storage re-exports those exact types for
source compatibility; inference and future compiled islands import the shared
contract directly. This prevents either provider from defining a subtly
different status vocabulary or envelope layout.

The older standalone-to-inference lifecycle bridge exposed only `c_int`, so a
linked create, configure, or route-registration failure became
`InferenceRuntimeStartupFailed` or `InferenceRouteRegistrationFailed` at the
first wrapper. It now returns the canonical status plus one caller-owned
failure envelope. ABI 34 adds the `inference_runtime` origin, append-only
create/configure/register-routes stages, and distinct registered identities
for invalid configuration, invalid model-cache configuration, resource-limit
exhaustion, temporary resource unavailability, and unsupported generator
providers. The consumer validates the complete envelope, rejects a mismatched
origin or stage as `InvalidBoundaryFailureIdentity`, maps every registered
status back to its original Zig error, and logs the exact provider name/hash
for an undeclared defect whose control-flow status must remain `internal`.

This establishes the rule for every remaining experiment: a new compiled
boundary is incomplete if it returns only a boolean, integer exit code, broad
error class, or status without provenance. Existing legacy storage functions
that return a stable `Status` retain their declared machine identity today,
but when one is moved, broadened, or otherwise materially changed it must also
gain the canonical envelope. Nested providers must forward a valid inner
identity byte-for-byte. Call-level failure, per-item outcomes, cancellation,
partial progress, and lifecycle state remain separate typed channels; none may
overwrite another merely because an ABI was crossed.

Validation passed in both composition modes: the focused standalone suite was
43/43 with zero leaks, the linked five-unit Debug build completed, the opaque
owner/status-registry suite was 92/92 with zero leaks, and the C API suite was
11/11 with zero leaks. The registry canary now proves an inference model-cache
failure round-trips through ABI 34 with its status, origin, operation, exact
name, and hash intact.

The same audit found one remaining transitional interface that prevents a
production keep decision. `AntflyProvider` is currently populated by the
inference archive as a Zig function table whose calls return raw `anyerror`
unions and carry Zig allocators and slices. Those values have no supported
stable ABI identity across independently code-generated units. The lifecycle
fix above must not be mistaken for validation of that provider table.

The next inference-root experiment must replace that crossing, not wrap it.
The control unit may retain the convenient source-level `AntflyProvider`, but
its callbacks must be consumer-local shims over a C-compatible coarse
inference ABI. One complete embed batch, sparse-embed batch, rerank, generation,
media read/transcription/extraction, or model-list request crosses at a time.
The provider returns a stable status, canonical `FailureIdentity`, and
provider-owned typed or versioned-wire result with an explicit destroy call.
Cancellation/deadline state has its own request fields. No `anyerror`, error
union, `std.mem.Allocator`, `std.Io`, generic Zig slice, or domain-owned
container crosses the compiled boundary. This is both an identity-correctness
requirement and the architectural cut needed to remove inference implementation
roots from distributed control.

### Phase 4w: total-order aggregation sort specialization

The first post-boundary compiler profile showed that the critical
storage/query unit was already below the preferred local gate but still spent
98% of its time in LLVM. `std.sort.block` was the second-largest reported
generic family: 143 stable-sort specializations, with 556 ms of aggregate
front-end/codegen attribution. This is not another archive boundary, but it is
a bounded way to reduce the LLVM input inside the existing physical owner.

An audit found 28 stable sorts in `storage/db/aggregations.zig`. Twenty-seven
sort bucket collections whose keys are unique or whose comparators include an
explicit deterministic tie-breaker. Stability therefore carries no semantic
information. Those calls now use `std.mem.sortUnstable`, Zig's PDQ sort. The
percentile value sort remains stable because IEEE NaN values compare
equivalent under its current comparator and changing their relative order was
not part of this experiment.

Matched cold Apple-Silicon cross-builds used Zig 0.16.0, fresh local/global
caches, ARM64 Linux musl `ReleaseFast`, production LSM-only mode, stripping,
the opt-in five-unit topology, and normal concurrency:

| Metric | ABI-34 baseline | Total-order PDQ candidate | Change |
|---|---:|---:|---:|
| Storage/query compiler | 327.342 s | 302.038 s | -25.304 s (-7.7%) |
| LLVM emission | 320.798 s | 295.722 s | -25.076 s |
| Generic instances | 18,211 | 18,136 | -75 |
| Stable block-sort instances | 143 | 119 | -24 |
| Storage object | 31,374,888 B | 31,337,648 B | -37,240 B |
| `libantfly.so` | 18,462,032 B | 18,438,256 B | -23,776 B |
| Reported storage MaxRSS | 5 GiB | 5 GiB | unchanged |

The authoritative graph is unchanged at 771 imported files, 592 repository
files, 919,313 repository lines, and 33,479 declarations. This is expected:
the gain removes generic specializations rather than dependency edges. The
aggregate emitted `aggregations.zig` attribution grows from 399,498 B to
462,734 B because the PDQ implementations are attributed to their callers,
while unassigned standard-library sort code falls enough for the complete
storage object and final CAPI to shrink. Object totals, rather than per-module
attribution alone, are the artifact gate.

All 56 focused aggregation tests pass with zero leaks, including deterministic
terms, composite, histogram, date, range, nested-cardinality, and distributed
merge ordering. The storage-owner, CAPI, and enrichment cross-archive suites
also pass; graph gates remain green and the analyzer is 17/17. A temporary
ReleaseFast benchmark over 20 rounds of 100,000 representative total-order
buckets verified identical sorted keys and measured PDQ at 0.767 times the
stable-sort duration, so the compile win does not trade away the relevant
runtime path.

The focused DB suite initially could not compile because its physical-storage
root lacked the canonical `storage_source_options` and `kernel_owner_abi`
modules introduced by the owner cut. The build now injects those exact shared
modules explicitly. This is a test-composition repair, not a duplicate ABI:
focused DB tests exercise the same source-selection and stable owner contract
as the linked storage artifact, including its failure/status identities.

Decision: **keep the total-order sort specialization and test-root repair**.
It is a material, behavior-preserving 25-second reduction inside the local
critical unit and leaves memory, graph, artifact, and boundary identity gates
green. It does not complete the main goal by itself and does not supersede the
required `AntflyProvider` replacement described in Phase 4v. Normal-runner
evidence is still required before attributing the same reduction to Linux.

### Phase 4x: identity-preserving dense-inference ABI slice

The first `AntflyProvider` replacement slice moves a complete dense-embedding
batch onto a dependency-neutral C-compatible ABI. The control unit retains the
source-level `AntflyProvider` expected by data and enrichment code, but replaces
its dense callbacks with consumer-local shims. The shim sends borrowed string
descriptors, an optional scalar deadline, and an opaque cancellation callback
and context to the inference archive. It does not send a Zig allocator,
`std.Io`, atomic type, slice-of-slices, error union, or provider function table
on this call. The inference provider owns all result vectors and descriptors;
the consumer validates and copies them into its requested allocator and invokes
the matching provider destroy function on every path.

ABI 35 adds the append-only `embed_dense_texts` operation and stable statuses
for `UnsupportedEmbeddingProvider`, `ModelNotFound`, `ModelNotSpecified`, and
`ModelArtifactsChanging`. Existing `Timeout`, `Cancelled`, `OutOfMemory`,
protocol, and resource statuses remain unchanged. Every non-successful call
returns the canonical `FailureIdentity`; the consumer validates the status,
ABI version, `inference_runtime` origin, operation, bounded exact Zig error
name, and full-name hash before converting a registered status back to the
identical Zig error. Unexpected provider defects remain `.internal` for control
flow but retain their exact diagnostic name and hash. An inconsistent status,
origin, operation, or success envelope is a protocol error rather than being
relabelled as the reported domain failure.

Request validation also preserves identity: only an actual version mismatch is
`InvalidAbiVersion`; malformed pointers, counts, flags, reserved bytes, or a
noncanonical cancellation pair are `InvalidArgument`. Results reject ambiguous
zero-length pointers, missing ownership, excessive counts, and nonzero reserved
fields as `InvalidBoundaryQueryResponse`. Empty successful batches still carry
an explicit provider owner, so destruction remains unambiguous and idempotent
at the consumer call site.

Behavioral validation passed with zero leaks:

- 49/49 focused standalone tests, including provider-owned result copying and
  destruction, request/result validation, exact registered-error round trips,
  and wrong-origin rejection;
- the complete linked five-unit native Debug build;
- 92/92 opaque storage-owner and status-registry tests;
- 11/11 CAPI tests; and
- all three graph gates plus 17/17 graph-analyzer tests.

The unavoidable copy introduced by provider ownership is not a meaningful
inference-path regression in a local `ReleaseFast` microbenchmark: copying a
32-by-768 f32 batch (98,304 B) took 3.04 microseconds, and copying a
256-by-1536 batch (1,572,864 B) took 58.14 microseconds. These are deliberately
large representative result shapes and remain negligible beside model
execution. If a future measured workload disproves that assumption, the next
design should use a typed caller-buffer protocol, not a cross-unit Zig
allocator.

The same template now covers the three simple result families: sparse embedding
returns provider-owned paired index/value vectors, reranking returns one owned
f32 vector whose cardinality must equal the document count, and model listing
returns one bounded owned byte buffer. Each operation has a distinct append-only
operation identity and therefore cannot be misattributed to dense embedding or
to another provider stage. Their consumers reject ambiguous pointer/count
shapes before copying. During the mixed migration, the adapters deliberately
preserve the raw table's existing opaque Node pointer; changing it to the
lifecycle-state handle would silently break every not-yet-migrated callback.

Decision: **keep this as the identity and ownership template for the remaining
coarse inference operations**. It is a prerequisite slice, not yet a compiler
graph win: multipart embedding, generation, media, and extraction still come
from the transitional raw Zig provider table. Because that table still exists,
a cold `ReleaseFast` compiler comparison would not measure the intended root
removal. Migrate the remaining callbacks using the same status/envelope and
provider-owned-result rules, then delete `ProviderContext`,
`linkedInferenceProvider`, and the raw provider export before taking the
authoritative cold graph, time, RSS, and artifact measurement.

## Holistic target architecture

The current structural candidate is the opt-in five-unit source-selected topology above:
storage plus local query, distributed/API control, enrichment compute,
inference, and remote CLI. Unlike the rejected source-only coalescing probes, it gives the
distributed/API unit only control sources, co-generates physical local-query
execution with its storage owner, and keeps compute-heavy
inference/enrichment isolated.
The local cold combined storage/query unit is now 313.334 seconds, below the
350-second preferred gate. On the normal Linux runner the five-unit candidate
measured 599.665 seconds for storage/query, versus 664.411 seconds for the
immediately preceding matched six-unit repeat. The topology therefore has
runner reliability evidence and a controlled 64.746-second improvement, but
is not yet the production baseline because it still misses the 380-second
critical-unit gate.

The reopened target is a modular monolith with one compiled physical-storage
owner and separately compiled control consumers:

```text
thin linked main
├── API protocol and distributed-control consumer islands
│   ├── HTTP/auth/public translation, routing, topology, fanout and merge
│   ├── serverless orchestration plus standalone/Lite product composition
│   └── opaque storage handles plus coarse request/result/callback ABIs
├── provisioned storage/local-query kernel (PIC, sectioned, compiled once)
│   ├── DB/LSM/index ownership and complete group-local query execution
│   ├── writes, transaction participants, snapshots, restore and maintenance
│   └── public C API exports reused by the shared libraries
└── inference island
    └── model lifecycle plus the linked standalone inference host
```

The current candidate grouping keeps remote CLI separate; co-generates API,
serverless orchestration, standalone/Lite composition, and the data, metadata,
and HA control consumers; and keeps restore staging, physical storage/local
query execution, and the C API in the storage unit. Standalone remains a
product composition mode and always links the separately compiled inference
host. Fresh Linux reports determine whether this placement is accepted or
revised again.

The kernel is not a per-backend wrapper and not an internal RPC service. It is
one static PIC artifact linked into the executable and the C API libraries.
Consumers cross it only for complete local operations. This preserves one
process, one static executable, direct in-process calls, explicit ownership,
and one optimized copy of storage/local-query code.

### Source ownership rules

These rules guide module structure, lazy-import cleanup, and the remaining
compiled ABI between control consumers and physical storage ownership.

| Layer | Owns | Must not own |
|---|---|---|
| API protocol | HTTP, auth, public validation, request translation | Provisioned DB ownership, raft application |
| Serverless | Published artifact lifecycle and serverless request orchestration | Provisioned table/shard ownership, physical query execution, or cluster Raft |
| Distributed control | Table routing, topology, leadership, fanout, distributed merge, transaction coordination | Local storage implementation details |
| Provisioned storage | Table and shard handles, local physical query execution, batches, transaction participant state, snapshots, restore publication, maintenance | HTTP, auth, cluster routing, remote topology |
| Inference | Model lifecycle and inference execution | Table/storage ownership |
| Main | Command dispatch and linked-unit invocation | Domain implementations |
| Standalone | Product-mode wiring, startup and shutdown | Another copy of inference or storage codegen |
| C API | Public ABI adaptation | A second storage implementation |

Raft leadership, routing, and distributed transaction coordination remain
control concerns. Raft apply, local transaction participation, and physical
snapshot publication operate through the storage owner.

### Criteria for the compiled storage ABI

The normal-runner and root-isolation results reopen the storage-kernel
experiment. It must follow these rules and beat the current three-unit baseline
before becoming the production architecture:

The failure and state rules below apply equally to inference, enrichment, and
any future compiled runtime island; storage is merely the largest current
consumer.

- Handles such as storage, table, shard, and snapshot handles are opaque.
- ABI declarations use C-compatible layouts and explicit-width types.
- Inputs are borrowed for the duration of a call unless explicitly documented
  otherwise.
- Results allocated by the kernel are destroyed by the kernel.
- Status values use explicit enums or tagged result structures, not exported
  arbitrary Zig error sets.
- Raw Zig `anyerror`, error unions, allocators, `std.Io`, generic slices, and
  domain-owned containers never cross independently code-generated units.
  Source-level callback tables are allowed only when provider and consumer are
  compiled in the same unit; linked units use C-compatible request/result
  contracts and explicit ownership.
- Each expected failure has one stable status identity in the shared
  bidirectional registry. Do not merge distinct failures into `busy`,
  `cancelled`, `invalid_argument`, or `internal` merely to shorten an adapter.
- Every migrated operation also returns one canonical `FailureIdentity` whose
  origin and append-only operation stage identify where the failure happened.
  Consumers validate the whole envelope. Nested wrappers forward a valid inner
  identity unchanged and originate a new one only for their own work or for a
  detected protocol defect.
- Operation state (`pending`, `partial`, continuation position, retryability,
  lifecycle phase, or cancellation observation) is carried by typed fields or
  tagged outcomes. It is not encoded by borrowing an error status.
- Batched operations have separate call-level and per-item result channels. A
  successful call can contain exact item failures; an item failure must not be
  promoted to a generic call failure or hidden by an overall `ok`.
- A callback-originated error is stored in consumer-owned call state and
  rethrown exactly after the provider unwinds. Callback protocol sentinels are
  not domain-error identities.
- `internal` is reserved for defects not declared by the operation contract;
  its diagnostic payload retains the provider error name, full-name hash, and
  originating boundary, operation stage, and boundary version, but consumers
  may not branch on that payload. Malformed diagnostic payloads are logged as
  untrusted evidence and replaced by a registered protocol-failure identity;
  they are never forwarded as if valid.
  Provider/client tests must prove representative declared errors and callback
  errors round trip with their original Zig error identity.
- Calls represent complete operations: one group query, one batch, one
  transaction phase, one restore publication, or one maintenance request.
- Do not cross the ABI per document, posting, edge, LMDB operation, or vector
  candidate.
- Prefer compact borrowed descriptors or existing wire payloads over universal
  JSON serialization. JSON is acceptable when it is already the natural
  external representation and profiling shows it is not material.
- Callbacks are limited to necessities such as cancellation, deadlines,
  logging, bounded I/O, and progress. Arbitrary callbacks into control logic
  would blur ownership and make reentrancy difficult.
- Logical/public validation remains outside the kernel. Local physical planning
  and execution live together inside it so storage internals do not leak.

## Storage-kernel experiment plan and historical checkpoints

This experiment originally asked whether a separately compiled storage kernel
could reduce the then-425.652-second critical path enough to justify the ABI and
ownership migration. The 352.9--358.6-second local compile-once control paused
the migration, but the repeated 9--11-minute normal-runner result and the
283--296-second root-isolation probes reopen it. The phases below remain the
design and experiment record. Work resumes at the atomic physical-source cut
identified after Phase 3, rather than repeating already validated ABI slices.

### Phase 0: reproducible baseline

1. Record commit, host, OS, target, cache state, Zig version, concurrency, and
   build flags.
2. Capture time reports for API, distributed/storage, inference, and any new
   experimental units.
3. Record wall time, peak RSS, LLVM time, declarations, file overlap, archive
   sizes, executable size, and `libantfly.so` size.
4. Run the same build at least twice when comparing small deltas. A cold-cache
   comparison must use cold caches on both sides.

### Phase 1: build-only kernel skeleton

1. Add an experimental runtime unit, guarded by a temporary build option, with
   a PIC static library and a narrow ABI module.
2. Define opaque storage/table handles, borrowed byte slices, owned output
   buffers, stable statuses, and destruction functions.
3. Link the kernel once into the final executable. Consumer archives reference
   its exported symbols instead of each embedding the kernel.
4. Link the same kernel into the C API shared libraries.
5. Add graph checks preventing experimental consumers from directly importing
   DB, DocStore, LSM, table implementation, or production LMDB modules.

This phase validates link topology, ownership conventions, and section GC
before behavior moves.

### Phase 2: representative local-query slice

Move one complete local group query into the kernel:

```text
distributed route and fanout
    → kernel query(table handle, request envelope)
    → complete local index/search execution
    → owned result envelope
    → distributed merge
```

The kernel call must include all local posting, vector, graph, filtering,
stored-field, and scoring work. Moving only a parser or one index primitive will
not remove the implementation graph and is not a useful experiment.

After the move, capture fresh reports for the storage kernel, distributed
control, standalone composition, API, and inference. This is the first
go/no-go checkpoint.

### Phase 3: complete write and transaction-participant slice

If the query slice pays off, move coarse local operations:

- batch writes, deletes, and transforms;
- local transaction begin, prepare, resolve, and status;
- local schema and index installation;
- bulk-ingest lifecycle; and
- artifact reprocessing and repair.

A batch containing thousands of documents must remain one ABI call.
Distributed transaction coordination and table routing stay in control.

### Phase 4: restore and maintenance

Move storage-owned lifecycle operations:

- snapshot and portable backup production;
- restore staging, integrity validation, generation publication, and rollback;
- table/shard open, close, replace, and destroy;
- compaction, flush, and structural reconciliation; and
- local runtime/storage statistics.

API continues to own authentication, public HTTP handling, remote repository
coordination, and user-facing job state. The kernel owns atomic local storage
changes.

### Phase 5: split orchestration from composition

Once distributed control and standalone no longer import storage
implementations:

1. Compile the storage/local-query kernel once.
2. Compile data + metadata + serverless control together.
3. Keep API and inference separate.
4. Make standalone/Lite a thin composition layer over the compiled units.
5. Compile all independent units concurrently with normal job scheduling.

The earlier data + metadata result of approximately 376.9 seconds suggests a
possible critical path below the current 425.652 seconds once standalone and
storage emission are no longer fused into that unit. This is a hypothesis, not
a promised result.

## Storage-kernel go/no-go criteria

The separate-kernel migration proceeds beyond the atomic physical-source cut
only if it demonstrates a material improvement. Its thresholds are:

- reduce the 425.652-second distributed critical path to at most about 380
  seconds, with a preferred result below 350 seconds;
- or show at least a repeatable 30–45 second reduction plus a clear movement of
  LLVM work into a concurrently compiled kernel;
- keep every unit within its current CI memory claim;
- keep `libantfly.so` near 16–18 MB;
- limit executable growth to approximately 5%;
- introduce no meaningful query or write throughput regression;
- retain coarse calls with no per-record/backend ABI crossing;
- keep storage implementation imports out of API, distributed control, and
  standalone composition roots; and
- pass repeated clean-cache ARM64 musl `ReleaseFast` builds without depending
  on cache luck.

Stop the migration if a representative complete query still leaves the maximum
compiler unit close to 425 seconds, if serialization/callback overhead is
material, or if the boundary requires exposing most DB internals. A clean
domain boundary is desirable, but not at unlimited implementation and runtime
cost.

That conclusion is now historical. The compile-once control was locally fast,
but the same unit repeatedly takes 9--11 minutes on the normal runner. The new
root-isolation probes also show an 82.685-second removable control cost. The
migration resumes only at an atomic cut that removes legacy physical sources;
the previously rejected configuration that merely added a kernel alongside
those sources remains invalid.

## Required validation

Each experimental phase should run, in proportion to the code moved:

- linked native Debug with production LSM-only options;
- linked native Debug with LMDB compatibility enabled;
- clean-cache ARM64 Linux musl `ReleaseFast`;
- data, metadata, serverless, standalone, Lite, API, and CAPI smoke tests;
- local and distributed query tests, including vector and graph paths;
- batch and transaction tests;
- backup/restore integrity, rollback, and idempotency tests;
- maintenance and structural reconciliation tests;
- LMDB wrapper, fixtures, and conversion tests; and
- ABI ownership tests for success, errors, cancellation, partial initialization,
  and destruction.

Run symbol audits on production artifacts to ensure the LMDB implementation is
not linked and section GC is still keeping server-only roots out of the shared
C API.

## Reproduction commands

The current linked production build is:

```sh
zig build \
  -Dtarget=aarch64-linux-musl \
  -Doptimize=ReleaseFast \
  -Dstrip=true \
  -Dlinked-runtime-libraries=true \
  -Dproduction-lsm-only=true
```

The opt-in separate-storage experiment adds:

```sh
zig build \
  -Dtarget=aarch64-linux-musl \
  -Doptimize=ReleaseFast \
  -Dstrip=true \
  -Dlinked-runtime-libraries=true \
  -Dproduction-lsm-only=true \
  -Dstorage-kernel-experiment=true
```

The atomic physical-source cut is complete in the opt-in data, metadata, HA,
and API consumer unit. One normal-runner archive has succeeded, but its storage
and distributed units remain above the performance gate and repeated proof is
still missing. Production must not enable it until the behavioral, compiler,
memory, graph, artifact, and normal-runner gates above pass.

Compatibility validation keeps the linked architecture but enables LMDB:

```sh
zig build \
  -Doptimize=Debug \
  -Dlinked-runtime-libraries=true \
  -Dproduction-lsm-only=false

zig build lmdb-test storage-lmdb-test
```

Run the graph gates and analyzer tests from the repository root:

```sh
python3 zig/tools/analyze_zig_import_graph.py \
  --check-runtime-boundary \
  --check-codegen-boundary \
  --check-api-kernel-boundary \
  --json

python3 -m unittest zig/tools/test_analyze_zig_import_graph.py
```

Capture compiler reports by starting a Zig build with `--time-report` and a
localhost web UI, then connect one capture process per unit:

```sh
zig build \
  -Dtarget=aarch64-linux-musl \
  -Doptimize=ReleaseFast \
  -Dstrip=true \
  -Dlinked-runtime-libraries=true \
  -Dproduction-lsm-only=true \
  --time-report \
  --webui=127.0.0.1:19125

node zig/tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-distributed reports/distributed.json 30
node zig/tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-storage-kernel reports/storage.json 30
node zig/tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-api_kernel reports/api.json 30
node zig/tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-local_query reports/local-query.json 30
node zig/tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-inference reports/inference.json 30
node zig/tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-cli reports/cli.json 5
```

The first optional argument is the minimum LLVM-emission duration in seconds.
It prevents an early build-script or sema-only report from being mistaken for
the final optimized unit. A second optional argument bounds the wait in seconds
and defaults to 1,200, so a missing step cannot hang CI. The collector creates
the output parent directory. Zig 0.16 intentionally keeps a `--time-report`
WebUI build runner alive after the final summary; after all reports and the
successful `Build Summary` are present, interrupt that idle build-runner
process rather than waiting for it to exit on its own.

Use a new `--cache-dir` and `--global-cache-dir` for genuine cold-cache
comparisons. Do not compare a cold candidate with a warm baseline.

Pass the resulting Zig compilation objects to the analyzer to rank actual
emitted modules and cross-object duplication:

```sh
python3 zig/tools/analyze_zig_import_graph.py \
  --object application=/path/to/libantfly-storage-kernel_zcu.o \
  --object api=/path/to/libantfly-runtime-api_kernel_zcu.o \
  --top-groups 30
```

Attribution uses Zig's named function/data sections. An object built without
section granularity is still measured, but its monolithic bytes are reported
as unassigned rather than falsely attributed from a lazy compiler file list.

## Experiment record template

Append future results here or in a linked PR note using this schema:

| Field | Value |
|---|---|
| Date and commit | |
| Host / runner | |
| Zig version | |
| Target and optimization | |
| Cache state | cold / warm |
| Runtime-unit layout | |
| Unit compiler times | |
| Unit peak RSS | |
| LLVM emit times | |
| Declarations | |
| Repository file overlap | |
| Executable / archive / CAPI sizes | |
| Functional tests | |
| Throughput or latency delta | |
| Result | keep / revise / reject |

## Definition of done

This compilation architecture work is complete when:

- clean-cache ARM64 musl `ReleaseFast` is reliable on the normal runner;
- the critical compiler unit meets the accepted build-time and memory budget;
- the release still produces one executable with embedded inference;
- storage and local query implementations are emitted once and reused by the
  executable and C API;
- standalone is composition rather than another implementation graph;
- production artifacts contain no LMDB engine;
- public ABI ownership and failure behavior are covered by tests; and
- the graph gates prevent the broad implementation dependencies from silently
  returning.
