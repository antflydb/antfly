# Antfly Zig compilation architecture

Last updated: 2026-08-08

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
  explicit at every compiled ABI boundary.

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
   the small C API surface, and exact behavior/error/cancellation ownership.
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
| Data-only PIC storage probe | 283.018 s, 277.375 s LLVM, 593 repository files, 36,065 declarations | Establishes that PIC/CAPI storage ownership is not the excess cost |
| Data + standalone/Lite + CAPI PIC probe | 288.171 s, only +5.153 s over data alone; 614 repository files, 37,166 declarations | Strong candidate ownership island; CLI/metadata roots account for the remaining 82.685 s |
| CLI + metadata control-only probe | 295.647 s, 289.733 s LLVM, 600 repository files | A separate control unit meets the time gate but duplicates too much physical storage by itself |
| API/serverless + CLI/metadata coalescing | API/control 409.246 s; storage runtime 313.292 s; duplicate instances 484 → 676; executable 61.224 MB → 72.464 MB | Rejected; moving roots without moving physical ownership misses time, overlap, and artifact gates |
| Application/storage without remote CLI | 340.130 s, 332.415 s LLVM, 40,037 declarations, 636 repository files | Keep; below the preferred 350-second local gate |
| Remote CLI after HA/restore ownership cut | 38.029 s, 35.904 s LLVM, 5,804 declarations, 53 repository files and no Antfly storage files | Keep as an independent final codegen unit |
| Four-unit production archive with deterministic 20 GB scheduling | 30/30 steps in 374.23 s locally and 15:17.77 on the normal runner; static executable 62.421 MB; C API 16.665 MB | Reliable, but the 11 m Linux application unit misses the performance gate |
| Serverless local runtime co-generated with application/storage | Application 361.270 s; API 125.604 s; duplicates 527 → 438; executable 62.421 MB → 59.271 MB | Keep pending normal-runner confirmation; under 380 s and removes material storage duplication |
| Canonical storage-contract/import cut | Removed 42k lexically reachable lines but declarations 17,332 → 17,334 with identical generic/inline counts | Rejected; lazy file removal is not emitted-code removal |
| Isolated local HA runtime | Application saved 5.344 s while a new HA unit cost 30.525 s and duplicated 75 files | Rejected; small role splitting increases aggregate LLVM work |

The `/tmp` graph analysis was consolidated into
`tools/analyze_zig_import_graph.py`. It reports lexical reachability, consumes
Zig time-report JSON, reads allocatable sections from cross-compiled ELF
objects, compares loaded-file and emitted-module overlap, and checks
architectural boundaries. Object reporting distinguishes lazy import
reachability from machine code/data that LLVM actually emitted. Its regression tests live in
`tools/test_analyze_zig_import_graph.py`.

## Current compilation architecture

The linked release currently generates four coarse libraries with normal,
memory-budgeted concurrency:

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
implementation barrels. The process context is retained foundation for the
remaining physical-owner cut; it is not evidence by itself that the separate
kernel experiment should be production-enabled.

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

## Holistic target architecture

The current candidate baseline is the four-unit topology above with serverless
local execution in application/storage. It is smaller than the rejected
source-only coalescing, its isolated application/storage unit remains under the
accepted local time gate, and its predecessor was reliable on the normal
runner. It is not yet the final target: the Linux application/storage critical
unit is still too slow and API still emits 50 physical storage/local-query
files.

The reopened target is a modular monolith with one compiled physical-storage
owner and separately compiled control consumers:

```text
thin linked main
├── API protocol and distributed-control consumer islands
│   ├── HTTP/auth/public translation, routing, topology, fanout and merge
│   └── opaque storage handles plus coarse request/result/callback ABIs
├── provisioned storage/local-query kernel (PIC, sectioned, compiled once)
│   ├── DB/LSM/index ownership, serverless, and complete group-local queries
│   ├── writes, transaction participants, snapshots, restore and maintenance
│   └── public C API exports reused by the shared libraries
└── inference island
    └── model lifecycle plus the linked standalone inference host
```

The accepted grouping keeps remote CLI separate, and keeps data, metadata,
serverless, HA, standalone/Lite, restore, and the C API in the
application/storage unit.
Further regrouping is a measured decision after the remaining API physical
imports disappear. Standalone remains a product composition mode and always
links the separately compiled inference host.

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
| Serverless | Published artifact lifecycle and serverless-local query execution | Provisioned table/shard ownership or cluster Raft |
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

- Handles such as storage, table, shard, and snapshot handles are opaque.
- ABI declarations use C-compatible layouts and explicit-width types.
- Inputs are borrowed for the duration of a call unless explicitly documented
  otherwise.
- Results allocated by the kernel are destroyed by the kernel.
- Status values use explicit enums or tagged result structures, not exported
  arbitrary Zig error sets.
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

This option remains off by default while the physical-source cut is incomplete.
Production must not enable it until the behavioral, compiler, memory, graph,
artifact, and normal-runner gates above pass.

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
  ws://127.0.0.1:19125/ antfly-storage-kernel reports/distributed.json 30
node zig/tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-api_kernel reports/api.json 30
node zig/tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-inference reports/inference.json 30
node zig/tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-cli reports/cli.json 5
```

The optional final argument is the minimum LLVM-emission duration in seconds.
It prevents an early build-script or sema-only report from being mistaken for
the final optimized unit. The collector creates the output parent directory.

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
