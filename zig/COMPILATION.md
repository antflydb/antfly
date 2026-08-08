# Antfly Zig compilation architecture

Last updated: 2026-08-07

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

The `/tmp` graph analysis was consolidated into
`tools/analyze_zig_import_graph.py`. It reports lexical reachability, consumes
Zig time-report JSON, compares compiler units, and checks architectural
boundaries. Its regression tests live in
`tools/test_analyze_zig_import_graph.py`.

## Current compilation architecture

The linked release currently generates three large libraries concurrently:

```text
antfly executable
├── antfly-runtime-api_kernel
├── antfly-storage-kernel       # currently the broader distributed unit
└── antfly-runtime-inference
```

The executable links all three into one statically linked binary. The C API
shared libraries link the same PIC distributed/storage archive, with function
and data sections allowing the linker to retain only C API roots.

The name `antfly-storage-kernel` currently describes the archive's reuse role,
not yet a pure architectural boundary. It still contains data, metadata,
serverless, standalone/Lite, CLI, and storage implementations.

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
  the API ABI files.

This is prerequisite work for a true storage kernel. It is not the completed
storage ownership migration.

## Current clean-cache profile

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

## Holistic target architecture

The preferred end state is a modular monolith with a small number of compiled
domain islands. It is not a generic internal RPC system and not a microservice
split.

```text
thin main / standalone composition
├── API protocol runtime
│   └── HTTP, authentication, validation, public request translation
├── distributed control runtime
│   └── routing, topology, raft coordination, fanout and result merging
├── storage + local-query kernel
│   └── DB ownership, LSM, indexes, local physical queries,
│       transaction participation, restore publication and maintenance
├── inference runtime
└── C API
    └── calls the same compiled storage kernel
```

Standalone should become primarily a composition mode that starts and wires
the already compiled API, control, storage, and inference units. It should not
own another copy of those implementations.

### Ownership rules

| Layer | Owns | Must not own |
|---|---|---|
| API protocol | HTTP, auth, public validation, request translation | DBs, LSM, local indexes, raft application |
| Distributed control | Table routing, topology, leadership, fanout, distributed merge, transaction coordination | Local storage implementation details |
| Storage/local-query kernel | Table and shard handles, local physical query execution, batches, transaction participant state, snapshots, restore publication, maintenance | HTTP, auth, cluster routing, remote topology |
| Inference | Model lifecycle and inference execution | Table/storage ownership |
| Main/standalone | Wiring, startup, shutdown and product mode selection | Domain implementations |
| C API | Public ABI adaptation | A second storage implementation |

Raft and distributed transaction coordination remain outside the kernel. A
raft apply or transaction participant operation enters the kernel as one coarse
local operation.

### ABI principles

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

## Storage-kernel experiment plan

The experiment should answer one question early: can a separately compiled
storage kernel reduce the 425.652-second critical path enough to justify the ABI
and ownership migration?

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

## Go/no-go criteria

Proceed beyond the query slice only if it demonstrates a material improvement.
The initial thresholds are:

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

The opt-in Phase 1/2 storage-kernel scaffold adds:

```sh
zig build \
  -Dtarget=aarch64-linux-musl \
  -Doptimize=ReleaseFast \
  -Dstrip=true \
  -Dlinked-runtime-libraries=true \
  -Dproduction-lsm-only=true \
  -Dstorage-kernel-experiment=true
```

This option is intentionally off by default until the representative query
slice meets the go/no-go criteria.

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

node tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-storage-kernel distributed.json
node tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-api_kernel api.json
node tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-inference inference.json
```

Use a new `--cache-dir` and `--global-cache-dir` for genuine cold-cache
comparisons. Do not compare a cold candidate with a warm baseline.

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
- ABI ownership and failure behavior are covered by tests; and
- the graph gates prevent the broad implementation dependencies from silently
  returning.
