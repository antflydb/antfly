# Antfly Zig compilation architecture

Last updated: 2026-08-23

This is the living design and operating guide for Antfly's Zig compilation
architecture. The complete chronological investigation, including rejected
probes and superseded measurements, is preserved in
[COMPILATION_EXPERIMENTS.md](COMPILATION_EXPERIMENTS.md).

## Status at a glance

The production build uses a six-unit architecture. It builds the
complete product as one statically linked `antfly` executable, always includes
embedded standalone inference, compiles production without LMDB, and reuses the
same compiled storage implementation in the executable and `libantfly` C API.

The architecture and reliability gates pass on the normal runner. The build-time
goal is not complete: storage/local query and inference remain above the
380-second per-unit runner target.

The authoritative per-unit baseline is GitHub Actions run `31643584514`, job
`94271808409`, at commit `42b494546`:

| Compilation unit | Normal-runner time | LLVM | Zig MaxRSS | 380 s gate |
|---|---:|---:|---:|---|
| Storage + local query | 482.880 s | 473.049 s | 8 GiB | Over by 102.880 s |
| Inference + standalone inference host | 444.092 s | 393.655 s | 7 GiB | Over by 64.092 s |
| Data/metadata/HA + standalone | 297.394 s | 289.441 s | 5 GiB | Pass |
| API kernel | 244.981 s | 238.614 s | 5 GiB | Pass |
| Serverless + remote CLI | 211.154 s | 204.531 s | 4 GiB | Pass |
| Enrichment compute | 32.552 s | 30.050 s | 1 GiB | Pass |

That build completed in 16:14.57 with 7,968,692 KiB process-tree peak RSS and
zero swap. It produced a 72,995,616-byte static executable and an
18,910,408-byte `libantfly.so`.

The exact current head was confirmed again by GitHub Actions run `31645335108`,
job `94277464595`. It completed all 43 steps in 19:42.13 with 8,156,880 KiB
process-tree peak RSS, zero swap, the same 72,995,616-byte executable, and the
same 18,910,408-byte `libantfly.so`. Its rounded unit summary reproduced the
same shape: storage and inference at about nine minutes, distributed and API at
about five minutes, serverless at about four minutes, and enrichment at about
40 seconds.

Current decision:

- Ship the six-unit composition as the production architecture.
- Work on storage first and inference second. API, distributed/standalone,
  serverless/CLI, and enrichment no longer need top-level composition changes.
- Do not substitute a larger runner, swap, cache priming, `-j1`, or merging
  inference back into application code for the remaining architecture work.

## Main goal

Build the complete Antfly release reliably and quickly in `ReleaseFast` on the
normal cost-efficient CI runner, without serializing compilation or treating
extra memory as the permanent solution.

The result must remain one statically linked `antfly` executable. Standalone
must include embedded inference, the C API must reuse the compiled storage
implementation, and production behavior must not be weakened to make the
compiler succeed.

The work has two related objectives:

1. Keep every Zig/LLVM compilation unit below the complexity and memory level
   associated with the original ARM64 musl `std::bad_alloc` failure.
2. Reduce repeated LLVM optimization and object emission across units that are
   linked into the executable and release archive.

## Non-negotiable constraints

- The product remains a modular monolith delivered as one executable.
- `ReleaseFast` is the target configuration. `ReleaseSmall` is not the desired
  release mode.
- Standalone always includes embedded inference.
- Normal compiler concurrency remains enabled. `-j1` is not the architecture.
- Data, metadata, serverless, standalone, Lite, API, CLI, and inference remain
  independently testable commands or modes even when some are co-generated.
- The public C API is the `capi` build target and `libantfly` shared library.
  It must not retain unrelated server or runtime roots.
- LSM is the production backend. LMDB remains available only for tests,
  fixtures, conversion, and legacy compatibility while needed.
- Runtime boundaries are coarse. They never cross per record, posting, edge,
  LMDB operation, or vector candidate.
- Allocation ownership, cancellation, deadlines, operation state, callbacks,
  and error translation are explicit at every compiled boundary.
- Declared failures retain stable semantic identities across every nested
  provider, callback, wrapper, and consumer boundary.

## Current compilation architecture

The production topology produces six independently code-generated static
libraries and links them into one executable:

```text
thin linked antfly executable
├── antfly-storage-kernel
│   ├── physical DB, LSM, indexes, DocStore and local query
│   ├── writes, transaction participation, WAL and Raft apply
│   ├── snapshots, restore, maintenance and Lite
│   └── public C API implementation reused by libantfly
├── antfly-runtime-distributed
│   ├── data, metadata and HA control
│   └── standalone lifecycle and product composition
├── antfly-runtime-api_kernel
│   └── HTTP, auth, public validation and API protocol handlers
├── antfly-runtime-serverless
│   ├── serverless orchestration over published artifacts
│   └── remote/client CLI commands
├── antfly-runtime-inference
│   ├── model lifecycle and inference execution
│   └── linked standalone inference host
└── antfly-runtime-enrichment_compute
    └── bounded document and media extraction compute
```

These are compiled libraries, not processes or internal services. Calls remain
direct in-process ABI calls. The executable retains one command dispatcher and
one implementation of each owned subsystem.

The former combined storage/application layout is retained in repository
history for comparison; it is no longer a supported build topology.

### Unit ownership

| Unit | Owns | Must not own |
|---|---|---|
| Storage/local query | Physical table and shard handles, DB/LSM/index execution, local planning, batches, transaction participants, snapshots, restore publication, maintenance, Lite and CAPI exports | HTTP, auth, cluster routing, remote topology, model execution |
| Distributed/standalone | Table routing, topology, leadership, fanout, merge, distributed transactions, HA control, standalone startup/shutdown | Physical DB, index-manager, LSM, enrichment implementation or a second inference implementation |
| API kernel | HTTP, auth, public validation, request translation and protocol handlers | Provisioned DB ownership, physical query execution or Raft apply |
| Serverless/CLI | Published-artifact orchestration, serverless requests and remote administration | Provisioned storage ownership, physical index execution or cluster Raft apply |
| Inference | Model lifecycle, tokenizer/model/graph execution and standalone inference host | Table or storage ownership |
| Enrichment compute | Bounded extraction, media decode and PDF/image compute | Durable storage state, replay, manifests or index ownership |
| Main | Command dispatch and hidden linked-unit invocation | Domain implementations |
| C API | Public ABI adaptation over the compiled storage owner | A second storage implementation or private runtime exports |

Raft leadership, routing, and distributed transaction coordination are control
concerns. Raft apply, local transaction participation, physical WAL state, and
snapshot publication execute through the storage owner.

### Scheduling

Zig 0.16 randomizes dependency traversal, so source or enum order cannot define
a reliable memory-safe launch group. Explicit build dependencies preserve
useful overlap while `max_rss` claims let the build runner admit all work that
fits:

1. Storage and distributed/standalone form the initial group.
2. Inference starts when distributed completes and may overlap the storage
   tail.
3. API, serverless/CLI, and enrichment wait for storage, then overlap inference
   as memory claims permit.
4. Final executable and `libantfly` links reuse completed PIC objects.

This is normally concurrent compilation, not serialized compilation.

### C API composition

`libantfly` links the sectioned PIC storage and enrichment artifacts. Function
and data section GC retains public `antfly_db_*` and `antfly_lite_*` roots while
discarding private executable entry points. The symbol audit rejects exported
runtime, API-kernel, inference, storage-owner, snapshot, restore, and data-apply
symbols.

There is one canonical Zig C API identity:

- build target: `capi`;
- shared library: `libantfly`;
- public header: `antfly.h`.

Historical references to two C API libraries in the experiment ledger predate
this consolidation.

## Why compiled boundaries are required

Zig analyzes source imports lazily. A source module, facade, or directory is not
itself a separately compiled library. Turning every `zig/lib` directory into a
static library would not guarantee reuse of generated code.

Profiles consistently attribute approximately 97–98% of compiler wall time to
LLVM emission. Parsing a small contract file in multiple units matters far less
than optimizing and emitting a large implementation in multiple units.

Meaningful reuse therefore requires a compiled artifact boundary. When two
separately generated consumers need the same implementation, that implementation
must be compiled once behind a stable internal ABI and linked once into the
final product.

References:

- [Zig compilation model](https://ziglang.org/documentation/0.16.0/#Compilation-Model)
- [Zig 0.16 release notes](https://ziglang.org/download/0.16.0/release-notes.html)

## Original failure conclusion

The triggering ARM64 Linux musl archive build ended in `std::bad_alloc`. The
evidence did not establish ordinary cgroup OOM:

- cgroup OOM counters remained zero;
- observed cgroup peak memory was approximately 18–20 GB;
- the extracted direct `zig build-exe` command succeeded at approximately
  15.2 GB RSS;
- a direct `strace` replay succeeded; and
- the failing path used Zig's build-runner/compiler protocol with `--listen=-`,
  while the successful direct replay omitted that protocol.

Cache state affected progress but did not prove invalid Antfly code generation
or basic memory exhaustion. The likely cause remains Zig build-runner/compiler-
server or LLVM pressure. The architecture work is useful independently because
it reduces critical compiler units and repeated LLVM emission.

## Graph and emitted-code evidence

The early graph comparison contained 2,147 repository-file instances, 1,204
unique files, and 943 duplicate instances. Storage dominated: 163 storage files
accounted for 255 duplicate instances and roughly 382,000 duplicated source
lines. HTTPX and LMDB were much smaller and did not justify separate ABIs.

The accepted six-unit runner report contains:

- 2,305 repository-file instances;
- 1,257 unique repository files;
- 1,048 duplicate instances; and
- 3,607,360 bytes of repeated named text.

The higher source-instance count is an explicit composition tradeoff: the API
and distributed split shortened the scheduled critical chain while adding
bounded contract/runtime duplication to the static executable. The C API did
not grow because it does not link either control archive.

Lexical graph measurements are conservative. They include lazy declarations,
tests, and disabled comptime branches. Zig time reports are better for actual
analyzed units, but their `all_files` list can still include cheaply parsed
files. Decision weight is therefore:

1. normal-runner compiler time and peak RSS;
2. LLVM emission time and declarations;
3. emitted named-section overlap and artifact size;
4. analyzed-file overlap; and
5. lexical reachability as a preventive architecture gate.

## Internal ABI rules

These rules apply to storage, inference, enrichment, API, and any future
compiled runtime island.

### Representation and ownership

- Handles are opaque and are created, retired, and destroyed by their owning
  provider.
- ABI declarations use C-compatible layouts and explicit-width types.
- Inputs are borrowed only for the duration of the call unless explicitly
  documented otherwise.
- Provider-allocated results are destroyed by the provider. Consumers copy or
  parse results into consumer-owned memory before destruction.
- Raw Zig `anyerror`, error unions, allocators, `std.Io`, generic containers,
  and domain-owned slices do not cross independently generated units.
- Existing compact wire payloads or borrowed descriptors are preferred over
  universal JSON. JSON is acceptable when it is already the natural external
  form and profiling shows it is immaterial.

### Operation granularity

- One ABI call performs one complete local operation: group query, batch,
  transaction phase, restore publication, maintenance quantum, inference batch,
  or bounded extraction operation.
- No ABI crosses per document, posting, edge, backend call, LMDB operation,
  token, or vector candidate.
- Logical/public validation remains with control. Local physical planning and
  execution stay together in the owning provider.
- Callbacks are limited to necessities such as cancellation, deadlines,
  bounded I/O, logging, resource accounting, and progress.

### Failure identity and operation state

- Every expected failure has one stable status in the shared append-only
  registry. Distinct errors are not collapsed into `busy`, `cancelled`,
  `invalid_argument`, or `internal` for adapter convenience.
- Every failed migrated operation carries one canonical `FailureIdentity` with
  its originating boundary, boundary version, append-only operation stage,
  exact bounded Zig error name, and stable full-name hash.
- Consumers validate the whole failure envelope. A nested wrapper forwards a
  valid inner identity unchanged. It may originate a new identity only for
  work it performed or for a malformed inner envelope.
- Call failure, per-item outcome, callback failure, retryability, cancellation,
  lifecycle phase, and continuation position are independent channels. One
  channel must not overwrite or normalize another.
- A successful batch call may contain exact item failures. Item failures are
  neither promoted to generic call failures nor hidden by overall success.
- A callback-originated error is stored in consumer-owned call state and
  rethrown exactly after the provider unwinds. Callback protocol sentinels are
  not domain-error identities.
- `internal` is reserved for undeclared defects. Its diagnostic payload retains
  the provider error name/hash and origin metadata, but consumers do not branch
  on that untrusted payload.
- Provider/client tests prove representative declared errors, nested errors,
  malformed envelopes, callback failures, and operation states round-trip
  without losing identity.

## Active performance work

Only storage/local query and inference remain above the normal-runner target.
Composition changes to already-passing units require new evidence that they
shorten one of those two critical paths without unacceptable aggregate work.

### Priority 1: storage/local query

Storage is the largest current compiler at 482.880 seconds. The next credible
experiment is inside the physical owner, not another source facade or top-level
role split.

The leading candidate is a coarse local-index subsystem built from exact
production entry points. It should own a complete, coherent family such as:

- index-manager and algebraic-index lifecycle;
- local query planning and execution;
- index mutation and generated-artifact maintenance; and
- aggregation work that is inseparable from local physical execution.

The experiment must:

1. Root an existing production operation first, demonstrating that the exact
   shape compiles safely.
2. Introduce opaque handles and complete batch/query/lifecycle calls.
3. Avoid duplicating DB/index ownership between storage and the new unit.
4. Avoid per-record, per-posting, or backend callbacks.
5. Compare emitted sections and aggregate LLVM work, not only source imports.
6. Preserve the shared failure envelope and provider-owned result rules.

Synthetic provider probes previously triggered Zig compiler failures that the
exact production mutation root did not. New experiments must not extrapolate
from a hand-written shape without the production-root control.

### Priority 2: inference

Inference is the second remaining compiler at 444.092 seconds. Splitting the
inference command, dedicated server, standalone host, or offline command names
was rejected because those roots repeat the model, graph, tokenizer, and server
implementations.

The next inference experiment must first establish a compiled engine boundary
around one complete heavyweight operation family. A viable candidate owns the
model/session/graph execution needed for a full request and exposes coarse
request/result operations to the command and embedded-host consumers. It must
not split dispatch names while both sides instantiate the same engine.

Storage and inference experiments should be measured independently before
combining them. Otherwise a regression in one unit can be hidden by scheduling
variance in the other.

### Artifact and duplication budget

Phase 4ab's accepted API split intentionally grew the executable to
72,995,616 bytes, 5.20% above Phase 4aa and 11.40% above Phase 4y. That artifact
is now the comparison baseline for subsequent experiments, while the cumulative
Phase 4y delta remains visible as architectural debt.

For subsequent increments:

- `libantfly` has a hard 20 MiB release gate and a working target at or below
  approximately 19 MiB.
- No single experiment should grow the executable more than approximately 5%
  without an explicit, measured critical-path benefit and approval of the
  cumulative tradeoff.
- Repeated named text and aggregate object bytes must be reported with unit
  time; moving time by blindly duplicating implementation code is not a win.
- Before production enablement, the 11.40% cumulative executable increase from
  Phase 4y requires an explicit product-size decision or a demonstrated
  reduction.

## Goal loop

Repeat this loop until the exit criteria are satisfied:

1. Measure the exact current cold ARM64 Linux musl `ReleaseFast` tree with
   per-unit wall time, LLVM time, declarations, analyzed files, emitted overlap,
   peak memory, artifacts, and symbols.
2. Select one coarse ownership boundary from the largest remaining emitted
   implementation family. State what control remains outside and what complete
   operation moves inside.
3. Implement it within the split runtime graph while preserving the product
   and ABI constraints above.
4. Validate behavior in production-LSM and LMDB compatibility configurations.
5. Run graph gates, cross-archive ABI tests, symbol/artifact audits, and a
   genuinely cold local ARM64 comparison.
6. Send only a locally credible candidate to the unchanged normal runner.
7. Decide explicitly:
   - **keep** when behavior is sound and the runner shows material improvement,
     or when the increment is a bounded prerequisite to one named immediate
     cut;
   - **revise** when ownership is correct but old implementation roots remain,
     the change is host variance, or aggregate work offsets the critical-path
     benefit; or
   - **revert** when the boundary duplicates the implementation, grows the
     wrong artifact, weakens behavior, or lacks a credible path to the target.
8. Record full evidence in [COMPILATION_EXPERIMENTS.md](COMPILATION_EXPERIMENTS.md)
   and update only the current baseline and decision here.

## Acceptance gates

Normal-runner evidence is authoritative. Local cross-builds are pre-screening,
not production acceptance, because Linux runner times have scaled differently
from local Apple-Silicon cross-builds.

### Performance and reliability

- Every critical compiler unit is at most 380 seconds on repeated cold normal-
  runner builds; below 350 seconds is preferred.
- A candidate that does not yet cross 380 seconds shows at least a repeatable
  30–45 second runner reduction and a credible next ownership cut.
- The complete archive is reliable with normal concurrency and unchanged runner
  cost.
- Every unit remains within its scheduler memory claim, with no discarded-
  compiler retry, cgroup OOM, or swap dependency.
- Cold candidate and baseline use separate fresh local and global cache paths.

### Product and artifact shape

- One static `antfly` executable contains every required command and embedded
  standalone inference.
- `libantfly` remains below 20 MiB and exposes only the public C API.
- Production artifacts contain no LMDB implementation symbols or entry-point
  strings.
- Executable size and emitted duplication remain within the budget above.
- Query, write, restore, and inference throughput show no meaningful regression.

### Architecture and behavior

- Storage/local query implementation is emitted once and reused by the
  executable and C API.
- API, distributed control, standalone composition, and serverless do not
  analyze physical storage implementation roots.
- Calls remain coarse and ownership-safe.
- Failure identity, item outcomes, callback failures, cancellation, deadlines,
  and operation state retain their exact contracts.
- Graph gates prevent broad implementation imports from returning.

Enabling the experiment by default, merging the architecture, increasing
runner cost, or accepting a larger artifact remains an explicit approval
decision even after technical gates pass.

## Required validation

Run validation in proportion to the moved code. A production-boundary change
normally needs:

- linked native Debug with production LSM-only sources;
- linked native Debug with LMDB compatibility enabled;
- affected data, metadata, serverless, standalone, Lite, API, inference, and
  CAPI tests;
- cross-archive provider/consumer ownership and failure-identity tests;
- local and distributed query tests, including vector and graph paths;
- batch and transaction tests;
- backup/restore integrity, rollback, and idempotency tests;
- maintenance and structural reconciliation tests;
- graph-analyzer tests and source-selection gates;
- symbol audits for LMDB and private CAPI exports;
- a cold local ARM64 Linux musl `ReleaseFast` comparison; and
- a cold normal-runner build for any candidate considered for acceptance.

Historical numeric test counts belong in the experiment ledger. This living
document names required suites so ordinary test growth does not make it stale.

## Canonical commands

Run Zig commands from `zig/` unless noted otherwise.

### Candidate release artifacts

Build both the executable and canonical C API:

```sh
zig build antfly capi \
  -Dtarget=aarch64-linux-musl \
  -Doptimize=ReleaseFast \
  -Dstrip=true \
  -Dcpu=baseline \
  -Donnx=false \
  -Dmetal=false \
  -Dsystem-blas=false \
  -Dproduction-lsm-only=true
```

The production packaging script at
`../scripts/packaging/build_zig_release_archive.sh` uses the same unconditional
split-storage `antfly` and `capi` topology.

For a genuine cold comparison, add different empty paths on both sides:

```sh
zig build antfly capi \
  --cache-dir /tmp/antfly-candidate-local-cache \
  --global-cache-dir /tmp/antfly-candidate-global-cache \
  -Dtarget=aarch64-linux-musl \
  -Doptimize=ReleaseFast \
  -Dstrip=true \
  -Dcpu=baseline \
  -Donnx=false \
  -Dmetal=false \
  -Dsystem-blas=false \
  -Dproduction-lsm-only=true
```

Do not compare a cold candidate with a warm baseline.

### Native Debug and compatibility

```sh
zig build antfly capi \
  -Doptimize=Debug \
  -Donnx=false \
  -Dmetal=false \
  -Dsystem-blas=false \
  -Dproduction-lsm-only=true

zig build capi-test capi-smoke lib-standalone-runtime-test \
  -Doptimize=Debug \
  -Donnx=false \
  -Dmetal=false \
  -Dsystem-blas=false

zig build \
  -Doptimize=Debug \
  -Dproduction-lsm-only=false

zig build lmdb-test storage-lmdb-test
```

### Graph gates

Run from the repository root:

```sh
python3 zig/tools/analyze_zig_import_graph.py \
  --check-runtime-boundary \
  --check-codegen-boundary \
  --check-api-kernel-boundary \
  --json

python3 zig/tools/analyze_zig_import_graph.py \
  --time-report distributed=reports/distributed.json \
  --check-compiled-storage-boundary \
  --check-ha-seed-failure-registry \
  --json

python3 -m unittest zig.tools.test_analyze_zig_import_graph
```

### Compiler report capture

Start the exact candidate build with fresh caches, `--time-report`, and a local
WebUI:

```sh
zig build antfly capi \
  --cache-dir /tmp/antfly-report-local-cache \
  --global-cache-dir /tmp/antfly-report-global-cache \
  -Dtarget=aarch64-linux-musl \
  -Doptimize=ReleaseFast \
  -Dstrip=true \
  -Dcpu=baseline \
  -Donnx=false \
  -Dmetal=false \
  -Dsystem-blas=false \
  -Dproduction-lsm-only=true \
  --time-report \
  --webui=127.0.0.1:19125
```

Connect one collector per current unit:

```sh
node tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-storage-kernel reports/storage.json 30
node tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-distributed reports/distributed.json 30
node tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-api_kernel reports/api.json 30
node tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-serverless reports/serverless.json 5
node tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-enrichment_compute reports/enrichment.json 5
node tools/capture_zig_time_report.mjs \
  ws://127.0.0.1:19125/ antfly-runtime-inference reports/inference.json 30
```

The first optional number is the minimum LLVM-emission duration. The next
optional argument, when supplied, bounds the wait and defaults to 1,200
seconds. Zig 0.16 intentionally keeps a WebUI build runner alive after the
successful summary; interrupt the idle runner after every report and the
successful `Build Summary` are present.

Analyze all current reports and objects together:

```sh
python3 tools/analyze_zig_import_graph.py \
  --time-report storage=reports/storage.json \
  --time-report distributed=reports/distributed.json \
  --time-report api=reports/api.json \
  --time-report serverless=reports/serverless.json \
  --time-report enrichment=reports/enrichment.json \
  --time-report inference=reports/inference.json \
  --object storage=/path/to/libantfly-storage-kernel_zcu.o \
  --object distributed=/path/to/libantfly-runtime-distributed_zcu.o \
  --object api=/path/to/libantfly-runtime-api_kernel_zcu.o \
  --object serverless=/path/to/libantfly-runtime-serverless_zcu.o \
  --object enrichment=/path/to/libantfly-runtime-enrichment_compute_zcu.o \
  --object inference=/path/to/libantfly-runtime-inference_zcu.o \
  --check-compiled-storage-boundary \
  --check-ha-seed-failure-registry \
  --top-groups 30
```

Attribution uses Zig's named function and data sections. A monolithic object is
reported as unassigned rather than being falsely attributed from the lazy
compiler file list.

## Experiment-record policy

Append full results to
[COMPILATION_EXPERIMENTS.md](COMPILATION_EXPERIMENTS.md), not to the middle of
this living design. Use this schema:

| Field | Required content |
|---|---|
| Date and commit | Exact tree measured |
| Host / runner | Hardware, OS and runner request |
| Zig version | Including patches to the build runner |
| Target and optimization | Target, CPU, backend flags and strip setting |
| Cache state | Cold or warm; exact separate cache paths for comparisons |
| Hypothesis | The implementation family expected to move |
| Ownership change | What complete operation moved and what remained control |
| Runtime-unit layout | Every generated unit and relevant scheduler edge |
| Unit metrics | Wall time, LLVM, declarations, files and MaxRSS |
| Overlap | Duplicate instances and emitted named sections |
| Artifacts | Executable, archives and CAPI sizes/symbol shape |
| Behavior | Focused tests, cross-archive identities and throughput |
| Decision | Keep, revise or revert, with the next named cut |

Update the status table in this file only when an accepted candidate becomes
the new comparison baseline. Rejected and intermediate measurements remain in
the ledger and must not silently redefine “current.”

## Definition of done

This work is complete when:

- repeated clean-cache ARM64 musl `ReleaseFast` builds are reliable on the
  unchanged normal runner;
- every critical compiler unit meets the accepted time and memory budget;
- the release remains one static executable with embedded inference;
- storage and local query are emitted once and reused by the executable and
  C API;
- standalone is composition rather than another storage or inference graph;
- production artifacts contain no LMDB engine;
- CAPI size, executable growth and repeated emitted text have accepted budgets;
- public and internal ABI ownership, cancellation, operation state, callback
  failure and exact semantic error identity are covered by tests;
- graph gates prevent broad implementation dependencies from returning; and
- enabling the candidate as the production default receives explicit approval.
