# Antfly Zig compilation architecture

Last updated: 2026-09-12

This is the living design and operating guide for Antfly's Zig compilation
architecture. The complete chronological investigation, including rejected
probes and superseded measurements, is preserved in
[COMPILATION_EXPERIMENTS.md](COMPILATION_EXPERIMENTS.md).

## Status at a glance

Production builds seven static runtime archives and links them into one Antfly
executable. The executable and `libantfly` reuse the physical storage archive.
The source roots are explicit: selecting one archive does not declare the other
archives' entry files through inactive imports.

The current verification separates three questions:

- Does a Debug product preserve storage, lifecycle, and API behavior?
- Does changing one owner's implementation reuse unrelated compiled archives?
- Does a clean release build improve wall time, memory, and artifact size on the
  same runner and settings as its baseline?

`tools/check_storage_compilation.py` answers the second question with real
production artifacts. The smaller `tools/test_runtime_cache.py` fixtures check
build options and generators. Neither establishes release performance.

The earlier six-unit release baseline is recorded in
[COMPILATION_EXPERIMENTS.md](COMPILATION_EXPERIMENTS.md), including Actions runs
`31643584514` and `31645335108`. Those runs predate this integration; their
compiler timings and memory reservations are historical, not measurements of
the current source roots. The matched x86_64 GNU release measurement below
updates admission claims only for that measured profile; ARM64/musl and GPU
profiles still need their own release measurements.

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

The production topology produces seven independently code-generated static
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
│   └── serverless orchestration over published artifacts
├── antfly-runtime-cli
│   └── remote/client CLI commands
├── antfly-runtime-inference
│   ├── model lifecycle and inference execution
│   └── linked standalone inference host
└── antfly-runtime-enrichment_compute
    └── bounded document and media extraction compute
```

These are compiled libraries within one process. Calls remain
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
| Serverless | Published-artifact orchestration and serverless requests | Provisioned storage ownership, physical index execution or cluster Raft apply |
| CLI | Remote administration and restore staging through the storage ABI | Local DB implementation or server startup |
| Inference | Model lifecycle, tokenizer/model/graph execution and standalone inference host | Table or storage ownership |
| Enrichment compute | Bounded extraction, media decode and PDF/image compute | Durable storage state, replay, manifests or index ownership |
| Main | Command dispatch and hidden linked-unit invocation | Domain implementations |
| C API | Public ABI adaptation over the compiled storage owner | A second storage implementation or private runtime exports |

Raft leadership, routing, and distributed transaction coordination are control
concerns. Raft apply, local transaction participation, physical WAL state, and
snapshot publication execute through the storage owner.

### Compilation and source ownership

The storage archive owns `storage/db/db.zig`, `storage/local_query.zig`, and
`storage/local_write.zig`. Serving coordination in `api/table_reads.zig` and
`api/table_writes.zig` uses opaque owners. Shared request and result helpers
live in `api/local_query_contract.zig` and `api/local_write_contract.zig`;
physical resource setup lives under `storage/`.

The executable dispatches Lite administration to storage and `lite serve` to
the distributed runtime. Storage never links back to a server entry point.
Storage owner tests link the same production archives and are included in the
storage and integration aggregates, using module-name test filters.

Consumer unit tests use interfaces and fakes. Consumer integration tests use
real opaque storage owners; they do not import the physical DB to set up a
fixture. Tests of physical DB, index, WAL, cache, recovery, and lease internals
remain in implementation suites. API read/write/lifecycle and Data selections
can span both kinds of tests without moving physical assertions into the ABI.

Consumer tests compile to Zig test objects. A separate source-less executable
links each object with the provider archives, native C objects, and stable test
metadata. A DB edit rebuilds storage and relinks these executables while reusing
their test objects. C sources remain separate from Zig test objects so native
stack traces survive the final link. Implementation suites continue to compile
against their physical owners.

Ownership factories expose consumer and implementation namespaces from mixed
source files. Public selections remain patterns. Before running a partitioned
selection, Zig inventories both binaries and the audit checks their union for
unmatched patterns and duplicate ownership. One partition may be empty; the
whole selection may not silently be empty. Zig runs the binaries, preserving
its target and foreign-execution policy. The test-only owner fixture supplies
allocator callbacks and borrowed runtime I/O so allocation-failure and VOPR
checks continue to exercise the real provider.

Simple test runners use piped diagnostics and captured stdout rather than
inheriting the terminal. In Zig 0.16, inherited output holds a global terminal
lock for the child's lifetime, serializing otherwise independent tests.
Explicit side effects force test execution on every invocation; inventories
remain cacheable and server-protocol runners retain Zig's execution policy.
Existing memory reservations still bound concurrent tests. A two-process
barrier regression verifies overlap, repeated execution, and retained output.
CI requests `--summary all` for unit and E2E builds to report each compilation
and run step's duration and available peak-memory measurements.

`max_rss` claims govern compilation admission. There are no artificial archive
ordering dependencies; independent compilations can run concurrently when the
runner has sufficient memory. `pkg/antfly/build/runtime_memory.zig` owns the
admission policy separately from runtime construction. Its lightweight tests
check that measured storage/inference compilations fit together and that
unmeasured hosts, CPU features, targets, modes, backend settings, and sanitizer
settings retain conservative reservations.

`python3 tools/check_storage_compilation.py --report storage-compilation.json`
checks the real compiler in an isolated source overlay. It measures cold and
warm builds, then verifies these source changes:

| Changed source | Required rebuild | Required reuse |
|---|---|---|
| Read/write coordination | Distributed runtime and affected consumer objects/links | Every other runtime archive and unrelated test objects |
| Physical DB/local query | Storage archive and final test links | Other runtime archives and consumer/owner test objects |
| Owner or consumer test root | Its test object and final link | Every runtime archive and unrelated test objects |
| Storage ABI contract | Storage, distributed runtime, owner tests | CLI, inference, enrichment |

This expensive check runs with `zig-full / build-cache`. The lightweight
configuration fixtures remain useful for option and generator dependencies.

A matched native Darwin Debug experiment compared the test ownership split
(`c50cc4b0c`) with its parent (`60ff56514`), before the subsequent main merge.
It compiled four consumer families, their physical partitions, three owner
suites, and all seven production archives, with `-j2`, separate local caches,
and a shared dependency cache. Whole-build RSS is the sampled sum of descendant
process RSS, which can double-count shared pages; CPU time includes children.

| Case | Wall seconds before / after | CPU seconds before / after | Peak build RSS GiB before / after |
|---|---:|---:|---:|
| Cold local cache | 431 / 399 | 625 / 569 | 7.58 / 8.13 |
| Physical DB edit | 332 / 218 | 427 / 225 | 4.35 / 6.79 |
| Consumer test root edit | 64 / 17 | 63 / 19 | 3.75 / 1.75 |

This single local trial demonstrates cheaper incremental consumer compilation.
It does not establish lower whole-suite peak memory: the physical Data test
executable remains large and raised the peak in this trial. These are Debug
measurements, not release-runner results. Cache regressions assert artifact
reuse rather than timing thresholds. Mutations stay in private source overlays.

Explicit entry roots and source profiles prevent inactive literal imports of
the principal implementations. The self-module `antfly_source_root` alias
selects those sources without giving overlapping files a second Zig module
identity. Some shared storage leaf files remain lexically reachable by control;
the source-mutation matrix does not prove independence for every leaf file.

### Cold x86_64 Linux release scheduling (2026-09-12)

Two fresh-cache builds used source `83cd076bf` on the same isolated build pod,
with only runtime compilation reservations changed. The pod used the Actions
runner image, an AMD EPYC 7B13 host, a 7.5 CPU request, a 24 GiB memory limit,
and the normal 22 GiB Zig admission budget. Both builds used Zig 0.16.0,
`-Dcpu=baseline -Doptimize=ReleaseFast -Dstrip=true -Dcuda=false
-Dcuda-artifacts=fatbin -Dantfly-version=release-measure -j8`, and built
`antfly capi capi-smoke`. Local and global Zig caches were empty and separate
for each run. This measures scheduling on one fixed source revision, not the
cumulative effect of the PR's other changes.

| Measurement | Conservative reservations | Measured reservations |
| --- | ---: | ---: |
| Elapsed build time | 72m44s | 27m01s |
| Process-tree CPU time | 87m52s | 78m15s |
| Sampled peak process-tree RSS | 5.93 GiB | 13.08 GiB |
| Storage compilation begins | 29m26s | 45s |
| Inference compilation begins | 56m28s | 45s |
| Antfly executable size | 115,997,560 bytes | 115,997,560 bytes |

Both builds passed all 43 steps, including C API smoke execution, and produced
the same executable SHA-256:
`2d5b4d04edc973a7356863a657b46be1068816511d5c515a1572c1f882eee9fe`.
The elapsed reduction was 62.9%. This is one paired measurement on a shared
node; CPU frequency and other tenants were not controlled, and total CPU time
also varied. The directly observed scheduling improvement is that storage and
inference overlap instead of being serialized by a combined 36 GiB claim.
RSS was sampled every 0.5 seconds by summing build descendants; shared pages
can be counted more than once and this is not the cgroup's total memory use.

| Runtime archive | Largest sampled compiler RSS across the pair | New claim |
| --- | ---: | ---: |
| Storage | 5.73 GiB | 8 GiB |
| Inference | 4.09 GiB | 8 GiB |
| Distributed | 3.48 GiB | 5 GiB |
| API | 3.05 GiB | 5 GiB |
| Serverless | 2.65 GiB | 4 GiB |
| CLI | 1.25 GiB | 2 GiB |
| Enrichment | 0.80 GiB | 2 GiB |

These claims apply only to native x86_64 Linux hosts building baseline x86_64
GNU, stripped ReleaseFast, with CPU inference and no thread sanitizer. Other
profiles keep their previous claims. Reservations remain admission estimates,
not hard per-process memory limits. After installing the final policy helper,
a warm build reused every compiler output and reran the C API smoke test; the
policy itself also passed its native Linux unit test.

The Actions unit job at `8e67d155f` completed its build/test step in 34m38s
([job 103640717903](https://github.com/antflydb/antfly/actions/runs/34726182022/job/103640717903)),
after the previous run hit its 60-minute watchdog. That run failed in two
obsolete restore fixture setups, so it is not a passing full-suite benchmark.
Its build-tool tests took six seconds; the expensive cache matrix remains in
`zig-full`. Test execution still contributes materially to the base job:
`storage-support-tests` alone ran for approximately ten minutes.

### Boundary runtime cost

The existing `antfly-storage-bench` installs `storage_boundary_bench`. Run it
with a new directory, which it creates and removes itself:

```sh
zig build antfly-storage-bench -Doptimize=Debug
./zig-out/bin/storage_boundary_bench /tmp/antfly-boundary-benchmark-new
```

It compares the actual internal batch encoder/parser with the production
owner's batch operation, and the typed query-result parser with the production
owner's query operation. Document bodies are about 500 bytes. Each JSON line
reports ten iterations, payload size, and separate timings.

In the local Debug sample, a 1,000-document batch spent 8.9 ms per iteration
encoding and parsing; the separately measured owner batch took 46.7 ms. A
1,000-hit query took 20.9 ms in the owner, with another 11.2 ms for consumer
decoding. These are separate measurements, not additive phases captured from
one request or production throughput claims. JSON transport is a material
remaining cost. This change removes a redundant owned query-response copy;
it does not replace the internal JSON transport. A future compact or borrowed
transport must preserve complete-operation calls, provider-owned result
lifetimes, request validation, and exact error/cancellation semantics.

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

The historical six-unit runner report contains:

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

Storage was the largest compiler in the historical six-unit baseline at 482.880 seconds. The next credible
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

Merging the architecture, increasing
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

zig build capi-test capi-smoke antfly-standalone-runtime-test \
  -Doptimize=Debug \
  -Donnx=false \
  -Dmetal=false \
  -Dsystem-blas=false

zig build \
  -Doptimize=Debug \
  -Dproduction-lsm-only=false

zig build lmdb-test antfly-storage-lmdb-test
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
