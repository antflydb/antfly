# Antfly Zig compilation architecture

Last updated: 2026-10-28

This is the living design and operating guide for Antfly's Zig compilation
architecture. The complete chronological investigation, including rejected
probes and superseded measurements, is preserved in the
[experiment ledger](../docs/design/compilation/history/experiment-ledger-2026-08-to-10.md).

## Status at a glance

Production builds seven runtime archives plus shared Apache storage and inference
owners and links them into one Antfly executable. The executable and `libantfly`
use the same compiled physical engine and inference implementations. The C API
links the Apache owners without server coordination. Native owner sharing is the
standard topology; there are no shared-core build switches.
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

The earlier six-unit release baseline is recorded in the
[experiment ledger](../docs/design/compilation/history/experiment-ledger-2026-08-to-10.md), including Actions runs
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
must include embedded inference, the C API must use the same authored local storage
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
- LSM is the production backend. The public Zig package, embedded library,
  WASM bundle, and linked runtime disable LMDB. The standalone Zig LMDB port
  and vendored C oracle remain available to explicit test/benchmark fixtures.
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
├── antfly-storage-engine (Apache; also linked into libantfly)
│   ├── physical DB, LSM, indexes, DocStore and local query
│   ├── writes, transaction participation, WAL and local Raft apply
│   ├── snapshots, restore, maintenance and Lite
│   └── public C API and handle registry
├── antfly-storage-kernel (Apache-2.0)
│   └── server/Raft adapters over the compiled physical engine
├── antfly-runtime-distributed
│   ├── data, metadata and hot-standby control
│   └── standalone lifecycle and product composition
├── antfly-runtime-api_kernel
│   └── HTTP, auth, public validation and API protocol handlers
├── antfly-runtime-serverless
│   └── serverless orchestration over published artifacts
├── antfly-runtime-cli
│   └── remote/client CLI commands
├── antfly-inference-core (Apache; also linked into libantfly)
│   └── model lifecycle, inference execution and provider implementations
├── antfly-runtime-inference
│   └── server inference adaptation and standalone inference host
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

The storage archive owns `storage/db/db.zig`, `storage/query.zig`, and
`storage/write.zig`. Serving coordination in `api/table_reads.zig` and
`api/table_writes.zig` uses opaque owners. Shared request and result helpers
live in `api/query_execution_contract.zig` and `api/write_contract.zig`;
physical resource setup lives under `storage/`.

The Apache Lite executable links its local engine owner, enrichment, and
inference without database server owners. The Apache-2.0 standalone executable
serves Lite files through `standalone --storage-engine lite --storage-path`.
Storage never links back to a server entry point.
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

The unstripped native Apple Silicon Zig 0.17.0 ReleaseFast API owner reached
27,085,701,120 bytes during the logical-input commitment qualification
(`5eaa13cb68`). Its default macOS reservation is 28 GiB, providing over 10%
headroom while fitting a 36 GiB developer host's default 28.8 GiB compiler
budget. The admission regression also prevents overlap with the distributed
owner under that budget. Qualified stripped profiles and Linux claims retain
their separate measurements.

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

The native Linux audit has a separate scheduling envelope from production
builds. [Run 40380791678](https://github.com/antflydb/antfly/actions/runs/40380791678)
completed its cold build in 1,364.871 seconds (1,637.813 CPU seconds), with a
7,656,402,944-byte peak summed process RSS. The preceding run timed out at
1,800 seconds with a similar 7.47 GB peak. Both used the immutable Nix
`3swwykg6la02jblm03z3s4fxhiqdmpsm-zig-0.17.0` compiler, linked to glibc.

For that exact compiler on native x86_64 Linux, the audit tool explicitly
selects baseline GNU Debug, stripped symbols, CPU inference, and Debug inference
dependencies. Its overlay caps larger reservations at **9 GiB per compile**,
retains smaller authored claims, and keeps the **20 GiB** aggregate admission
budget and **two-job** limit. The cap is over 25% above the largest sampled
whole-build process RSS. Two large claims leave **2 GiB** for ready auxiliary
work, whose reservation can precede its actual execution. They also fit the
host-aware fallback budget (80% of a 24 GiB pod).

[Run 40395513767](https://github.com/antflydb/antfly/actions/runs/40395513767)
qualified the compiler/profile selection and diagnostics but timed out at
1,800 seconds. Its previous 10 GiB caps still spent most samples on one
compiler; the final API archive completed just before the timeout, and the
source-owner fixture had only started. Peak summed process RSS was
7,477,886,976 bytes; sampled anonymous memory peaked at 6,827,483,136 bytes,
file cache at 8,177,479,680 bytes, and total cgroup memory at 14.01 GiB.
This was active compilation, not an OOM. The tighter 9 GiB envelope retains
measured headroom while reserving space for auxiliary work; its paired timing
and memory qualification is pending CI. It affects only the audit's transitive
compilation graph, not production reservations. Other compiler derivations
retain existing claims until measured. No runner capacity or timeout was
increased.

Audit reports retain periodic active-process CPU time, state, RSS, artifact
name and source root (including Zig response files). A timeout captures this
snapshot before killing the tree, because `wait4` cannot recover its child CPU
accounting afterward. Full compiler command lines are not retained in these
process snapshots. The successful job's cgroup peak was 21.07 GiB; cgroup
usage also includes cached output pages and is not interchangeable with summed
process RSS. Reports now sample anonymous, file, dirty-file, inactive-file,
and kernel memory separately every 30 seconds so paired qualification can
verify actual pressure within the unchanged 24 GiB pod limit.

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

### Runtime compilation memory reservations

> **Relocated:** The 2026-09-12 cold x86_64 Linux release-scheduling comparison
> and the boundary-runtime-cost sample that previously lived here are
> preserved verbatim in the
> [experiment ledger](../docs/design/compilation/history/experiment-ledger-2026-08-to-10.md#2026-09-12-cold-x86_64-linux-release-scheduling-and-boundary-runtime-cost).

These per-archive reservations, adopted from that measurement, apply only to
native x86_64 Linux hosts building baseline x86_64 GNU, stripped ReleaseFast,
with CPU inference and no thread sanitizer. The same target/backend fences
also admit the ReleaseSafe profile measured below; other profiles keep their
previous claims.

| Runtime archive | Largest sampled compiler RSS across the pair | New claim |
| --- | ---: | ---: |
| Storage | 5.73 GiB | 8 GiB |
| Inference | 4.09 GiB | 8 GiB |
| Distributed | 3.48 GiB | 5 GiB |
| API | 3.05 GiB | 5 GiB |
| Serverless | 2.65 GiB | 4 GiB |
| CLI | 1.25 GiB | 2 GiB |
| Enrichment | 0.80 GiB | 2 GiB |

The [2026-10-01 full E2E build](https://github.com/antflydb/antfly/actions/runs/36903575966/job/110509376054)
completed all 35 build steps in stripped ReleaseSafe just as the 90-minute
job deadline expired, preventing artifact upload. Its rounded Zig MaxRSS
summaries support these separate admission reservations:

| Runtime archive | Reported MaxRSS | ReleaseSafe claim |
| --- | ---: | ---: |
| Storage | 8G | 12 GiB |
| Inference | 5G | 8 GiB |
| Distributed | 5G | 8 GiB |
| API | 4G | 7 GiB |
| Serverless | 3G | 5 GiB |
| CLI | 1G | 3 GiB |
| Enrichment | 1G | 3 GiB |

These are scheduler reservations, not process memory limits. Each leaves
headroom above the rounded observation. Storage and inference now fit together
inside the existing 22 GiB aggregate budget, whereas the previous 20/16 GiB
claims serialized them. This removes that admission bottleneck without
increasing runner size or the deadline; a matched CI rerun must establish the
resulting wall time.

### Optimized HTTP inference for model E2E

Full CPU model E2E builds `inference-http-server -Doptimize=ReleaseFast
-Dcpu=baseline -Dstrip=true -Dcuda=false` and selects
`zig-out/bin/antfly-inference-http-server` through
`ANTFLY_INFERENCE_SERVER_BIN`. This existing production entry calls the same
`runServer` as the CLI, without compiling offline training, benchmark and
other CLI commands. `ANTFLY_BIN` continues selecting the Debug product for
model pulls, CLI checks, storage/API and standalone process composition.
Optimizing the serving graph covers attention and quantization as well as
matrix multiplication; model assertions and request limits stay unchanged.

The standalone inference development CLI (`antfly-inference-dev`) is a
separate, heavier build profile. At PR #1264's `7c4df469bb`,
its native x86_64 GNU, baseline CPU, unstripped ReleaseFast build measured
10,717,630,464 bytes of compiler RSS and 49 minutes cold wall time. It used the
exact Nix/glibc Zig derivation pinned in `scripts/ci/toolchain-policy.json`.
The corresponding aarch64 Darwin-hosted Linux cross-build measured
12,537,217,024 bytes. Reserve the measured peak plus 25%, rounded up to 64 MiB: 12,800 MiB
native and 14,976 MiB cross-built. Scope these claims to the measured host,
target, baseline CPU, accelerator-free, optimization and unstripped profiles;
other profiles keep their existing claims. `-Dinference-compile-memory-mb`
remains an explicit override.

These claims do not change runner resources or model-process budgets. The
native full CLI leaves 9.5 GiB for concurrent steps inside the existing
22 GiB aggregate build budget. Model CI uses the HTTP target's independent
reservation rather than inheriting this development-CLI claim.

The root build forwards `strip` into its inference workflow context. The
field is required so memory-profile selection stays consistent with the
final graph's stripping policy. Regression probes audit both root and
standalone declarations and reject a deliberately broken handoff.

At `62475010b7`, the native x86_64 GNU HTTP-only compiler step succeeded in
30 minutes (30:51.51 for the build, including helper steps) with a peak of
3,100,064 KiB / 3,174,465,536 bytes / 2.96 GiB. It used the same pinned
Nix/glibc Zig, baseline CPU, CPU-only ReleaseFast, and explicit stripping.
The product compiler ran cold; generated assets were reused. GNU time
reported 100% CPU and no swaps. Its 3,840 MiB / 3.75 GiB reservation is the
peak plus 25%, rounded up to 64 MiB. Two 8 GiB steps plus this claim consume
19.75 GiB of the existing 22 GiB budget, leaving 2.25 GiB for smaller steps.
A subsequent real build admits under a 4 GiB budget and reuses the measured
artifact after the context fix. The reduction applies only to the measured
native Linux, baseline CPU, stripped ReleaseFast profile; other HTTP
profiles keep their existing claims.

The stripped HTTP executable also passes all 25 speech E2E cases locally;
public CLI configuration contracts continue testing the Debug product.

### October macOS compile-memory follow-up

The baseline Linux GNU reservations above remain scoped to their measured
profiles. macOS reservations now provision 28 GiB for storage, 14 GiB for API,
20 GiB for inference, and 4 GiB for CLI. The storage claim covers the reported
22–23 GB peak with at least 25% headroom. The other increases are provisional
headroom over claims reported as exceeded, **not new measured peaks**. No CPU
kernel or Accelerate behavior changes with these scheduling reservations.

Re-measure each affected unit with empty caches and `-j1` on the affected host
and product profile. Then validate concurrent admission on a 48 GiB host with
at least 8 GiB left outside the build budget. Do not extrapolate the historical
Linux measurements to a different target, optimization mode, or backend set.

`diagnose-zig-build-memory.sh` now assigns each polling pass a sample ID.
Summarize its trace with:

```sh
python3 scripts/summarize_zig_compile_memory.py /tmp/build.rss.tsv \
  --output /tmp/build.units.json
```

The report retains per-unit peak commands, recommends whole-GiB reservations
with 25% headroom, and reports the sampled aggregate compiler RSS. Legacy
six-column traces produce an explicitly approximate aggregate upper envelope.
Sampling can miss short-lived peaks; retain raw traces and build provenance.
Final reservation qualification still requires fresh cold-build evidence.

### C API composition

`libantfly` compiles its public C API and local DB from
`pkg/antfly-embedded/src`. It links embedded-owned native inference and
enrichment compute archives. It does not link the server storage archive,
standalone runtime, Raft coordination, HTTP handlers, or private server C API.
The server storage archive consumes the same local source through the
`antfly_local_sources` module and retains its server adapters.

The symbol audit rejects exported runtime, API-kernel, storage-owner, snapshot,
restore, and data-apply symbols. Native provider exports remain hidden; only
the public C ABI is exported. See
[embedded source ownership](../docs/design/embedded-source-ownership.md) for
physical ownership, independent builds, and test collection.

There is one canonical Zig C API identity:

- build target: `capi`;
- shared library: `libantfly`;
- public header: `antfly.h`.

Historical references to two C API libraries in the
[experiment ledger](../docs/design/compilation/history/experiment-ledger-2026-08-to-10.md) predate this consolidation.

The storage archive compiles with function and data sections, and linked
executables and `libantfly` link with section garbage collection
(`pkg/antfly/build/runtime.zig`, `build_support/embedded/shared_library.zig`).
When the C API first reused a shared runtime archive, `libantfly.so` grew from
16.6 MB to 28.3 MB until section GC returned it to 16.55 MB. Keep section GC on
for every linked product.

#### Embedded inference

`libantfly` embeds native inference in-process through the inference package's
host exports. The server runtime uses the same implementation through its own
entry point. Local inference remains available to Lite hosts and the public C
ABI without a server binary. Opening a Lite handle with the local-runtime
configured flag reports `inference_mode: "local_embedded"` and
`local_inference_runtime: true` (see LITE.md's "Local Embedded Inference"
section). There is no inference-free `libantfly` product.

ELF builds select LLVM for the C API, both Lite CLI build definitions, and their
embedded inference and enrichment archives. The final export boundary requires LLD, which cannot link Zig's
self-hosted Debug output. The provider archives must use the same backend:
borrowed `std.Io` executors exchange internal Zig callback pointers, whose
calling convention differs between LLVM and the self-hosted backend. Mixing
them crashes the first provider background job even though the public C ABI
links successfully. Keep the native `capi-smoke` runtime check alongside the
export and executable/shared-consumer linkage checks.

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

## Rejected approaches

Each of these was measured and rejected; the evidence is in the
[experiment ledger](../docs/design/compilation/history/experiment-ledger-2026-08-to-10.md#experiment-log). Do not retry one without new evidence
that addresses the recorded reason.

- Larger runners, swap, or serial (`-j1`) compilation as the fix: diagnostic
  workarounds that lengthen the build or raise cost.
- Source facades, import cleanups, or narrow call wrappers without moving
  physical ownership: lazy file removal does not remove emitted code.
- Small role splits (isolated local HA, remote-only metadata, API/control
  coalescing, top-level inference command splits): they add aggregate LLVM
  work and duplicate storage or engine code faster than they shorten the
  critical unit.
- Moving individual operations behind owner callbacks or vtables (exact-sort
  merge, control-only write vtable) while the consumer still imports the
  physical implementation: the shell is small and the broad root remains.

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
- The storage batch and query owner calls still use internal JSON transport,
  and it is a material remaining cost: a local Debug sample spent 8.9 ms
  encoding/parsing a 1,000-document batch (owner batch 46.7 ms) and 11.2 ms
  decoding a 1,000-hit query result (owner query 20.9 ms). A replacement compact
  or borrowed transport must preserve complete-operation calls, provider-owned
  result lifetimes, request validation, and exact error/cancellation semantics.
  Measure with `storage_boundary_bench` (see
  [Boundary runtime cost](#boundary-runtime-cost)).
- Replicated batches carry the deterministic owner-open descriptor, including
  the full schema and index JSON, in every Raft entry
  (`encodeWithStorageOwnerDescriptor` in `data/raft_batch.zig`). That was the
  simplest replay-independent proof of deterministic opening, not the intended
  wire format. Before default enablement, measure envelope/log amplification and
  replace repeated contracts with a versioned descriptor reference, but only
  once provisioning and Raft snapshots durably carry the referenced contract.
  Do not trade deterministic replay for a leader-local or catalog-dependent
  cache ([history](../docs/design/compilation/history/experiment-ledger-2026-08-to-10.md#open-items-recorded-in-the-log-later-resolution-not-recorded-here)).
- Provider, secret, and remote-content capabilities needed by
  production-generated enrichments must reach every resident and restore
  candidate owner; the Phase 4h log entry recorded this as required
  before the atomic source switch. Current code configures the inference
  provider, secrets, and remote-content security on the process storage context
  before any table owner opens, but `RestorePrepareRequest` carries no
  capability handles, so candidate-owner coverage remains unverified
  ([history](../docs/design/compilation/history/experiment-ledger-2026-08-to-10.md#open-items-recorded-in-the-log-later-resolution-not-recorded-here)).

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
72,995,616 bytes, 5.20% above Phase 4aa and 11.40% above Phase 4y (phases are
described in the [experiment ledger](../docs/design/compilation/history/experiment-ledger-2026-08-to-10.md#phase-4y-post-main-ha-seed-source-selection-repair)). That artifact
is now the comparison baseline for subsequent experiments, while the cumulative
Phase 4y delta remains visible as architectural debt.

For subsequent increments:

- `libantfly` has a hard 80 MiB release gate (raised from 60 MiB on
  2026-10-11 with explicit approval after a native stripped ReleaseFast
  profile measured 65.97 MiB). Retained linker maps attribute storage and
  inference code and support investigation of subsequent size growth.
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
8. Record full evidence in a new dated file under
   [`docs/design/compilation/history/`](../docs/design/compilation/history/)
   (see the [experiment-record policy](#experiment-record-policy)) and update
   only the current baseline and decision here.

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
- `libantfly` remains below 80 MiB (see the raised gate above) and exposes
  only the public C API.
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

Historical numeric test counts belong in compilation history files. This living
document names required suites so ordinary test growth does not make it stale.

## Canonical commands

Run Zig commands from `zig/` unless noted otherwise.

### Candidate release artifacts

Build both the executable and canonical C API:

```sh
zig build antfly capi \
  -Dtarget=aarch64-linux-musl \
  -Doptimize=fast \
  -Dstrip=true \
  -Dcpu=baseline \
  -Dmetal=false \
  -Dsystem-blas=false \
  -Dproduction-lsm-only=true
```

The production packaging script at
`../scripts/packaging/build_zig_release_archive.sh` uses the same unconditional
split-storage `antfly` and `capi` topology.

For a genuine cold comparison, add different empty paths on both sides. Keep
them inside the worktree (under the ignored `.zig-cache/`), never in `/tmp` or
`~/.cache` (see the [build-cache rules](../AGENTS.md#build-caches)), and delete them when the
comparison is recorded:

```sh
zig build antfly capi \
  --cache-dir .zig-cache/cold/candidate-local \
  --global-cache-dir .zig-cache/cold/candidate-global \
  -Dtarget=aarch64-linux-musl \
  -Doptimize=fast \
  -Dstrip=true \
  -Dcpu=baseline \
  -Dmetal=false \
  -Dsystem-blas=false \
  -Dproduction-lsm-only=true
```

Use `.zig-cache/cold/baseline-local` and `.zig-cache/cold/baseline-global` in
the baseline worktree. Do not compare a cold candidate with a warm baseline.
Remove the cold directories afterward (`rm -rf .zig-cache/cold`).

### Native Debug and compatibility

```sh
zig build antfly capi \
  -Doptimize=debug \
  -Dmetal=false \
  -Dsystem-blas=false \
  -Dproduction-lsm-only=true

zig build capi-test capi-smoke antfly-standalone-runtime-test \
  -Doptimize=debug \
  -Dmetal=false \
  -Dsystem-blas=false

zig build \
  -Doptimize=debug \
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

To measure the actual Darwin release archive pipeline, run this from the
repository root with Zig 0.17 on `PATH`. Use a new report directory for each
cold run:

```sh
report="$PWD/.local-test-results/darwin-release-cold"
RUNNER_TEMP="$report/work" ANTFLY_ZIG_CACHE_ROOT="$report/cache" \
  scripts/diagnose-zig-build-memory.sh --command \
  --out-dir "$report/trace" --label darwin-release \
  --target aarch64-macos --optimize fast --strip true --interval 5 -- \
  "$PWD/scripts/packaging/build_zig_release_archive.sh" \
  --version 0.0.0-diagnostic --target aarch64-macos --optimize fast \
  --archive-name antfly_diagnostic_Darwin_arm64.tar.gz \
  --out-dir "$report/dist" --metal true --system-blas true
```

This retains production job admission, captures per-compiler RSS and CPU clocks,
and includes the full Zig step summary and packaging output. Darwin packaging
uses the bounded runner's 80% host/cgroup budget so the 28 GiB storage reservation
fits on adequately provisioned hosts and larger hosts can admit safe overlap.
Compiler response files are retained beside the trace for portable attribution.
Run on an
otherwise idle host. Record host, SDK, commit and cache state alongside results;
native Darwin timings do not establish Linux cross-build performance.

Start the exact candidate build with fresh worktree-local caches (removed
afterward, as above), `--time-report`, and a local WebUI:

```sh
zig build antfly capi \
  --cache-dir .zig-cache/cold/report-local \
  --global-cache-dir .zig-cache/cold/report-global \
  -Dtarget=aarch64-linux-musl \
  -Doptimize=fast \
  -Dstrip=true \
  -Dcpu=baseline \
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

### Boundary runtime cost

`zig build antfly-storage-bench` installs `storage_boundary_bench`, which
compares the internal batch encoder/parser with the production owner's batch
operation, and the typed query-result parser with the owner's query operation
(about 500-byte documents, ten iterations per JSON output line). Pass a new
directory; the benchmark creates and removes it:

```sh
zig build antfly-storage-bench -Doptimize=Debug
./zig-out/bin/storage_boundary_bench zig-out/boundary-benchmark-run
```

Each timing is a separate measurement, not additive phases of one request.

## Experiment-record policy

Record full results in a new dated file under
[`docs/design/compilation/history/`](../docs/design/compilation/history/), not in
the middle of this living design, and add a row for it to the
[history index](../docs/design/history.md). Name the file after what it records
and its date (for example `darwin-release-cold-2026-10.md`). The
[experiment ledger](../docs/design/compilation/history/experiment-ledger-2026-08-to-10.md) through 2026-10-11 is closed; do not append to it.
Use this schema:

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
history and must not silently redefine “current.”

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

### Explicit macOS SDK selection

For cacheable native configuration, pass `-Dmacos-sdk=/absolute/path/to/MacOSX.sdk`
(or set `SDK_PATH`). The shared SDK helper tracks the selected directory metadata,
so replacing that SDK invalidates configuration. The CLI option takes precedence
over `SDK_PATH`. A shared libc configuration makes this SDK authoritative for
C compilation, linking, and C translation, including generated host tools. All
storage, inference, and finetuning owners use this helper.
Automatic `xcrun` discovery remains available and deliberately disables configure
caching because Xcode selection is an external input. Pin an SDK for repeated
local or CI builds; keep automatic discovery when following `xcode-select`.

C bindings use the official `translate-c` package pinned to its Zig 0.17 branch.
Zig extracts package sources into a local `zig-pkg/` cache, which is ignored by Git.
Debug tools and ownership fixtures use `std.heap.SafeAllocator`; its `deinit()`
returns a leak count. Memory-bounded fixtures retain a requested-byte cap outside
the allocator metadata.

### October 13 wolfspider experiment (opt-in)

A paired cold-cache `antfly capi` build took **44m28s → 34m42s**, saving
9m46s (21.97%). All baseline 73 and candidate 70 steps succeeded. The candidate
executable finished at about 25m47s; the C library determined combined completion.

| Artifact | Baseline bytes | Candidate bytes | Change |
| --- | ---: | ---: | ---: |
| antfly | 171,093,176 | 171,816,520 | +0.42% |
| libantfly.dylib | 67,920,704 | 68,036,592 | +0.17% |

This is a build-time improvement, with no binary-size win. The two new options
remain disabled by default. Both builds used the same frozen source at
`70fb8dd0ba` plus pre-existing workspace changes, the same control-catalog repair,
and the same linker-map instrumentation. The PR is rebased onto newer main;
these figures do not measure that rebase. Main has since removed ONNX Runtime
and changed storage/linker composition. Do not enable either option by default
from this single experiment.

The shared host had 14 CPU cores, 36 GiB RAM, Darwin ARM64, Zig 0.17.0 and SDK
26.4. Both builds used baseline CPU, stripped ReleaseFast, Metal/system BLAS,
and disabled ONNX Runtime/CUDA/PJRT. An unrelated inference service and other
work remained active. Existing swap usage was about 18.1 GiB and did not grow
materially. Sampled aggregate build RSS fell from 12.61 to 12.01 GiB; this is
summed process residency, not unique physical memory or a host memory limit.
This is one paired run, not repeated or throughput qualification.

The candidate compiled API alongside small units, then one PIC inference core,
then storage alongside distributed (826–1522 s and 826–1121 s respectively).
The shared core took about 340 s, Zig MaxRSS 8G; its server adapter took about
20 s/901M. Baseline server/embedded inference took about 346 s/8G and 233 s/6G.
The shared core retains a conservative 20 GiB Darwin admission reservation.
That snapshot required an explicit reservation option. Current builds select
qualified claims automatically only for the matching compiler, SDK, host/target,
baseline CPU, optimization, strip, sanitizer and backend settings. Other profiles
retain conservative claims.

The Apache inference owner contains HTTP serving, model ownership and CLI work.
Its checked private boundary borrows IO/allocator callbacks and transports error
names, reconstructed in the caller's error domain. Memory/callback ABI code has
a common owner with compatibility namespaces. Enrichment reuses an archive
outside sanitized server configurations. Unique-key/ordinal sorts use unstable
sorting where ties are irrelevant; resident/recovery row preparation shares a
concrete worker. Broader storage reuse and size reduction remain open.

The measured release passed twelve inference CLI help paths, model-free HTTP
health/readiness/model inventory, and graceful worker shutdown. No downloaded
model or throughput comparison was performed. Raw measurement files are retained
outside the repository. For current builds, use the worktree's caches and one
install prefix. Isolated qualification caches must also live in the worktree
and be removed when qualification finishes:

```sh
export ZIG_LOCAL_CACHE_DIR="$PWD/.zig-cache"
export ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-global-cache"
python3 tools/run_bounded_zig_build.py --zig /path/to/zig-0.17.0 -- \
  build antfly capi -Dtarget=aarch64-macos -Dcpu=baseline \
  -Doptimize=fast -Dstrip=true -Dmetal=true -Dsystem-blas=true \
  -Dcuda=false -Dpjrt=false -Dantfly-version=build-study \
  --cache-dir .zig-cache --prefix zig-out --summary all
```

On the measured older source also pass `-Donnx=false`; main no longer has that
backend option. Current release packaging selects native shared owners and
qualified admission automatically; the former shared-core switches are retired.
Regenerate error names with
`python3 zig/tools/generate_engine_errors.py --zig /path/to/pinned/zig`;
`--check` verifies freshness. Run the model-free release check with
`python3 zig/tools/check_shared_inference_core.py --server-binary --binary /path/to/antfly`.

### October 13 merged-main follow-up (draft qualification)

A second paired cold-cache macOS ARM64 `antfly capi` build, based on merged
main `7aa031f49d`, took **35m39s → 29m24s** (17.55% faster). Both runs completed
successfully. This comparison measures the follow-up against the already shared
inference/admission implementation; it does not use the older 44m28s baseline.

| Artifact | Baseline bytes | Candidate bytes | Change |
| --- | ---: | ---: | ---: |
| antfly | 178,399,544 | 176,102,600 | -1.29% |
| libantfly.dylib | 71,561,920 | 69,398,944 | -3.02% |

Both builds used the same 14-core, 36 GiB wolfspider host, Zig 0.17.0, SDK 26.4,
ARM64 baseline CPU, ReleaseFast, `-Dstrip=true`, Metal/system BLAS and disabled
CUDA/PJRT. Both enabled shared inference and measured Darwin reservations. The
candidate additionally enabled `-Dshared-storage-core=true`. A missing serverless
resolver import on this main revision was repaired identically in both snapshots.
Unrelated host work remained running. Sampled aggregate build RSS increased from
12.39 to 14.58 GiB; summed process residency is not unique physical memory.
This is one cold-cache pair, not repeated build-time or cross-platform qualification.

The follow-up changes are:

- Compile Mach-O C API objects without waiting for inference/enrichment providers;
  attach those archives at the final shared-library link instead. The measured
  scheduler still started C API code generation only after storage completed,
  so removing this dependency alone did not establish C API overlap.
- Reserve 12 GiB for the shared inference core only on the already qualified
  measured Darwin profile. Other profiles retain their existing reservations.
- Use a compact sorted error-name catalog with binary search, retaining lossless
  compiler-local error identity. Its generated test checks every catalog entry.
- Share one concrete prepared mutation worker between resident and recovery
  receivers. Its borrowed view points into live owner fields; it does not copy
  caches or synchronization state. The C API linker map now contains one large
  worker (1,924,960 bytes), replacing two workers (1,954,196 and 1,842,160 bytes).
- Add an opt-in storage ordinal archive owned by `antfly-embedded/src/storage/ordinals`.
  Calls operate on caller-owned scalar buffers. Small sorts and small set merges
  remain inline; union remains inline because the native union probe regressed.
  Unsupported targets and thread-sanitized builds retain the inline implementation.

That historical snapshot disabled the storage option by default and forwarded
it through release packaging. Current native builds use the full shared storage
owner automatically. Qualification of that ordinal snapshot includes 48 ordinal/document-set tests
in each inline/shared mode, runtime contracts, mutation preparation/replay/SQL
transform tests, native foreign-key and relational-index suites, the Apache source
boundary audit, release C consumer linkage, and model-free inference CLI/HTTP
lifecycle checks. Broader throughput, Linux, sanitizer and full integration
qualification remain pending. Raw JSON, logs and linker maps stay outside the repo.

The cold measurement snapshot predates relocating the ordinal sources into the
embedded package; both implementations passed their 48 tests after relocation.
Later rebases must be validated separately and do not change these historical figures.

An initial alternating release-library batch probe (three runs per variant,
40 batches of 64 rows) had median document-write time 23.99 → 26.02 ms and SQL
insert time 179.55 → 181.95 ms. These short, model-free runs do not establish
throughput parity; the apparent regressions (8.47% and 1.34%) require longer
workloads and attribution before promotion.

### Shared physical engine follow-up (qualification in progress)

Native macOS/Linux aarch64/x86_64 products use the complete Apache compiled
storage owner, including ordinal workers. Shared inference and storage are
mandatory on these targets; source-only targets retain their target implementation. `antfly-storage-engine` compiles the public C API once
and owns coarse DB lifecycle, batch, query (including leased reads), snapshot,
schema/index and maintenance operations. The executable's Apache-2.0 storage archive
retains server/Raft adapters and calls this same compiled owner. The installed
C library links the owner archive; it does not compile another public C API object.
Per-record loops remain within operations.

The private native descriptor checks compiler identity, method signatures, and
DB/Core/handle/registry sizes, alignment and field layouts before interpreting
borrowed native state. Conditional runtime implementations have authored names
so independent compilation units calculate identical type identities. The
existing error-domain dispatcher transports named failures without copying
compiler-local numeric error codes. Both products access one handle registry
and one C API TLS error state. The private native descriptor additionally checks
backend-runtime and resource-manager layouts. Named callback dispatch preserves
project error identities for server-provided mutation, query, replication, TTL,
repair/capacity and background-job hooks. Query proxies are synchronous and
captured requests restore the caller-owned hook. Borrowed executors and
filesystem storage retain stable I/O receivers, so their errors are interpreted
in the originating compilation unit. Public lookup/raw/schema output buffers use the
C allocator even when an internal handle has a custom allocator.

Each operation has a hidden accessor, rather than one global pointer table, so
final links can discard unused operations. Lookup, scan, statistics and relational
constraint/index maintenance also route to the owner. Enrichment reconfiguration
and recovery/resolver runtime construction belong to this same owner, preventing
server-side construction from retaining a second prepared-mutation engine.
Stateful relational readers and document/relational sessions cross complete
open, paging, normalization and destruction operations. Their native layouts
are checked, and retained row filters carry their originating named-error
dispatch for the entire reader lifetime. Statement snapshots, capture fences
and query leases release through the same owner. The caller keeps borrowed
filter context alive until reader destruction; owned request bytes and storage
pins remain under the original reader lifetime rules.

Static server adapters compile without waiting for the shared engine. Provider
archives are attached to final consumers; `runtime-unit-storage-adapter` compiles
the adapter alone for memory/time qualification. Reservations remain conservative
until the new unit is measured. Unsupported and thread-sanitized profiles keep
the independent implementations in that historical snapshot. Current native
products use the shared topology without a build option.

Both embedded and production-profile executable probes pass real cross-archive
lifecycle, batch/query, named-error, incompatible-layout, registry and TLS checks.
A handle registered by the client is looked up and closed through the public
C API. Combined conformance, smoke, probe, row-preparation, replay and SQL
transform validation passed all 109 build steps. Expanded runtime/LSM/relational
index qualification passed 104/104 steps, including 606 LSM tests (23 skips),
196 relational index tests and 44 runtime contracts. The final native storage
and production-linked inference ABI probes pass. The Linux x86_64 GNU Debug
ELF facade cross-link passed 52/52 steps and its exact 118-symbol public export
audit; Linux execution is still a separate qualification gate. The PDF suite passed 489 tests
after consolidating the two Roboto fonts into one PIC asset archive. This removes
repeated data emission without changing font bytes or rendering behavior.

A longer alternating probe of the earlier draft (four runs per variant, 1,200
batches of 64 rows, read-back verified) measured document-write medians
3.3390 → 3.2963 s (-1.28%) and SQL insert medians 13.7517 → 13.7660 s (+0.10%).
It does not reproduce the short probe's slowdown, and does not qualify this
expanded engine revision. The new reproducible benchmark warms each database,
checks insert counts and reads, and loads each library in an isolated process:

```sh
python3 zig/tools/benchmark_storage_owner.py \
  --baseline /path/to/baseline/libantfly.dylib \
  --candidate /path/to/candidate/libantfly.dylib \
  --rounds 4 --batches 1200 --batch-size 64 --warmup-batches 64 \
  --output /path/outside/repository/storage-throughput.json
```

Initial cold SDK 27.0 measurements of `59a0711ade` against current main
`2c8240e703` used Zig 0.17.0, native aarch64 baseline CPU, stripped ReleaseFast,
Metal/Accelerate enabled, CUDA/PJRT disabled, and fresh local/global caches.
Both enabled shared inference and the measured-memory option; only the candidate
enabled shared storage. The baseline also received the identical missing
serverless resolver-import repair. Both builds succeeded on a 14-core, 36 GiB Mac.
These initial measurements precede selective operation linking and the expanded
read adapters; they are not final results for the PR.

| Initial SDK 27.0 measurement | Current main | Shared engine `59a0711ade` |
| --- | ---: | ---: |
| Cold combined executable/C API build | 2,633.97 s | 2,475.22 s |
| Stripped executable | 178,481,624 B | 198,763,416 B |
| Stripped C library | 71,561,936 B | 68,358,688 B |

The initial executable size regression prompted selective linking and additional
shared read operations. Exact font bytes appear five times per font in the
baseline executable and twice in its C library, versus once per artifact in the
candidate. The native asset archive removes four executable copies and one
library copy of each font.

Rounded compiler MaxRSS on that candidate was 9G API, 9G storage adapter, 6G
storage engine, 5G inference, 4G distributed and 3G serverless. The opt-in SDK 27.0
admission profile requires **both** shared owners, the exact compiler/SDK/native
CPU/optimization/strip/backend settings above, and no thread sanitizer. It claims
14 GiB for the adapter, 10 GiB for the engine and 4 GiB for the CLI; these exceed the rounded bounds
plus a whole GiB and 25% margin. Remaining runtime claims use the conservative
SDK 26.4 envelopes, also above this run's peaks. API and adapter can fit together
within 28 GiB; a subsequent cold overlap qualification at `044fc332fd` succeeded in 1,402.07 s
(23m22s, 46.77% shorter than the 43m54s baseline). Its observed aggregate compiler
RSS peak was 10.32 GiB. Rounded MaxRSS was 9G API, 5G storage adapter, 4G engine,
7G inference, 7G distributed, and 4G serverless; all claims retained at least 25%
above a rounded upper bound. The executable was still 198,994,408 B and library
68,375,216 B. These figures precede moving runtime construction and completing
callback/I/O ownership, so they do not establish final size or performance.

Four alternating isolated-process runs (1,200 x 64 rows, 64 warmup batches) of
that initial revision measured medians 3.5341 -> 3.6658 s for document batches
(+3.73%) and 13.3586 -> 13.5646 s for SQL inserts (+1.54%). A single-row follow-up
had large timing swings in both variants, and another workspace compiler was
observed on the host. These results **do not establish throughput equivalence**.
Repeat on the final revision with a quiet host. Counts and read-backs passed.

Final release and overlap measurements remain pending. SDK 27.0 results are
separate from wolfspider's SDK 26.4 pair; earlier figures describe their frozen
snapshots. At that point shared storage and measured memory were opt-in;
the October 14 default-topology work below removes those switches.


Native Debug probes now emit entry objects and use the same system Mach-O linker
as the C API. Consumer import graphs omit C/ObjC inputs already bundled by the
providers, without modifying shared provider modules. This avoids Zig's large
Debug archive relocation overflow and duplicate native inputs. Both source-owner
profiles pass actual caller-created runtime/I/O, runtime reconfiguration, and
foreign observer/dense-predicate failure probes; `TestWindowFailed` retains its
exact identity across both directions. Resource accounting passed 64/64 steps
(92 accounting/cache tests plus selection audit), sparse passed 71 tests, and
background/storage foundation passed 37 tests. The Linux ELF facade still exports
exactly the 118 header functions. Linux thread-sanitized CLI fallback compiles.
Final v3 release and throughput measurements remain pending.


The concrete prepared-mutation entry also crosses this owner boundary. Resident
and recovery contexts borrow the same pointer-based prepared-owner view, memo
and allocator; their layouts are checked before the owner runs the one compiled
worker. The view's read/visibility callbacks use named-error dispatch, and the
allocator retains its originating I/O receiver without copying its mutex. A real
client-created prepared-worker invocation passes through both native source-owner
profiles, with read-back verified. Combined replay/SQL/native/C API validation
passed 103/103 steps, and the final direct-worker/C API run passed 80/80 steps.
Expanded Debug probe compilation measured 4.47 decimal GB; its 6 GiB reservation
retains more than 25% margin.


The profiled dense/text kernels, preflight, planning/text statistics and table
storage configuration also forward to the owner. The native query adapter keeps
borrowed predicate proxies inside the call and restores caller hooks in captured
results. The first validation of these entries passed 80/80 build steps. The
expanded owner rounded compiler MaxRSS to 9G; its qualified SDK 27.0 reservation
is 14 GiB, preserving a whole-GiB rounding allowance plus at least 25% margin.

Six alternating isolated-process runs of the prepared-owner release (1,200 x 64
rows, 64 warmup batches) measured document medians 3.3520 -> 3.3832 s (+0.93%) and
SQL insert medians 12.3181 -> 12.2978 s (-0.16%). All counts and read-backs passed.
These qualify those model-free C API workloads on that snapshot; they do not
establish equivalence for every server or inference workload.


### Final shared-owner qualification (SDK 27.0)

Frozen source `a7fb47aa4f` (production source from `162b3febe4`) was rebuilt with
empty local/global caches and the same stripped native baseline-CPU ReleaseFast,
Metal/Accelerate, no CUDA/PJRT settings as the baseline `2c8240e703` pair. The
baseline received the identical serverless resolver-import repair. All 78 final
build steps succeeded. This is one observed cold pair on a 14-core, 36 GiB Mac;
background host load and accumulated swap prevent treating it as a universal
speedup or a controlled multi-run result.

| Final SDK 27.0 measurement | Current main | Shared owner |
| --- | ---: | ---: |
| Cold executable + C API build | 2,633.97 s (43m54s) | 1,316.24 s (21m56s) |
| Stripped executable | 178,481,624 B | 195,011,208 B (+9.26%) |
| Stripped C library | 71,561,936 B | 68,672,528 B (-4.04%) |

The observed cold build is 50.03% shorter. Sampled aggregate compiler RSS peaked
at 9.50 GiB; rounded per-unit MaxRSS was engine 7G, inference 7G, API 5G,
server adapter 5G, distributed 3G, serverless 4G, CLI 1G and enrichment 1G.
Reservations retain the larger peaks from the preceding repeated qualification,
including 9G engine and API observations. API, engine and adapter each reserve
14 GiB in the strictly qualified SDK 27.0 profile; CLI reserves 4 GiB. The final
run demonstrates actual overlapping compilation within the normal host budget.

A diagnostic link map confirms one full 1,927,048-byte prepared-mutation worker
in the engine and a 968-byte forwarding stub in the server archive. The six
remaining shared query/planning entries removed another 1.90 MB relative to the
preceding shared snapshot. Exact bundled font bytes occur once per font in both
final artifacts, versus five copies per font in the baseline executable and two
in its library. Nevertheless, additional physical/query helper instantiations in
the server adapter keep the executable 16.53 MB larger than current main. **The
executable-size goal is not achieved.** This historical snapshot kept shared storage and measured admission opt-in.
The subsequent default-topology work removes these switches at the user's
request; executable-size qualification remains an independent acceptance gate.

Final library throughput used isolated processes, alternating order, 64 warmup
batches and verified insert/count/read-back checks. Six pairs of 1,200 batches of
64 rows yielded independent medians 3.4019 -> 3.5601 s (+4.65%) for documents and
12.7888 -> 13.3136 s (+4.10%) for SQL inserts. Timing drift was substantial:
within-round paired median changes were +0.46% and +0.12%, with individual pairs
ranging -9.71% to +14.65% for documents and -5.86% to +8.10% for SQL. Retain both
summaries; neither proves broad throughput equivalence.

Eight pairs of 1,200 single-row batches yielded independent median changes
-4.95% for documents and -0.48% for SQL; paired medians were -3.56% and +0.27%.
These model-free workloads do not demonstrate a systematic large boundary cost,
but variability and limited coverage remain. Server query/ingest concurrency and
model-backed inference performance require separate qualification.

Final native query/direct-worker/C API validation passed 80/80 steps, and final
Linux ELF cross-link/export validation passed 52/52 (exactly 118 public functions,
no internal exports). Default runtime/ordinal contracts passed 8/8, including 44
runtime tests; Apache ownership verified 2,044 sources. Earlier replay/SQL, LSM,
relational, resource, sparse, background and PDF gates are recorded above. Raw
JSON, logs and maps remain outside the repository. Wolfspider SDK 26.4 requires
renewed Tailscale authentication and is not qualified by these SDK 27.0 results.

The exact final stripped CLI help smoke and ordinary executable/shared-library C
consumer linkage both passed after cold qualification. Isolated measurement caches
were removed after preserving raw evidence; the installed release artifacts remain.

### Default shared-owner boundary completion (2026-10-14)

The build and release packaging no longer expose shared-inference-core,
shared-storage-core or measured-darwin-compile-memory switches. Native owner
sharing is automatic, including instrumented native storage/inference owners.
Compiler admission uses measured claims only for the exact pinned Zig, SDK,
CPU, optimization, strip, sanitizer and provider profile; other configurations
retain conservative reservations. SDK 26.4 does not inherit SDK 27.0 qualification.

The server call-site audit now routes physical transaction, restore/staging,
replication, index/graph/vector maintenance, statistics and read-session work
through the engine. Thin batch profiling wrappers call the already shared batch
entry. Progress callbacks use synchronous named-error receivers, and incoming
graph-build failures cross as names rather than compiler-local error integers.
The private v20 descriptor keeps all method and borrowed-state layout guards,
including reader/session, index-manager, algebraic-index, DocStore, rewrite
program, enrichment-set and backup-snapshot layouts. Shared contract helpers
invoke the engine through their selected source-owner capability, keeping
literal physical DB imports out of control catalogs and consumer cache inputs.
Collision checking hashes
each method once before comparing IDs, preserving detection while avoiding
repeated compile-time hashing. Anonymous restore-finish and graph-notification
payloads now have authored names shared by independently compiled profiles.
Per-record and per-candidate loops stay inside complete engine operations.
API coordinators use an aggregation-only native contract that does not import
physical DB sources. It shares the engine's compiled aggregation implementation
and validates borrowed physical state when an aggregation context carries it.
Physical query/aggregation helpers, managed DB/provider factories, schema
reconciliation and backup execution also use that owner. Disposable portable
restore decoders execute a bounded burst of at most 16 pages or 100 ms inside
one open/import/close operation. Explicit I/O arguments use named-error borrows;
prepared backups own their receiving I/O until snapshot destruction, including
copy calls made after preparation returns. Source-pin publication/reclamation,
resolution and promotion runtimes, and pinned snapshot materialization and
destruction also execute in the engine. Offline Lite portable exports and
static native-restore materialization/publication entry points use the same
owner. Staged generations, their manager and native manifests have explicit
layout guards; a native fixture materializes, restores and reads back a snapshot.
Artifact receipt callbacks preserve caller error names across independent
compilation units. Writer callbacks execute through their originating unit,
with named error transport and a local receiving writer; optional spool I/O
uses a receiving executor. Receiver completion delivers pending bytes without
prematurely flushing the caller’s writer. Native tests compare complete bytes
and exercise caller-side WriteFailed and spool-backed export. Sanitizer settings
propagate to the shared ordinal, storage, inference and enrichment owners; the
server reuses the same enrichment archive for instrumented configurations.
Release metadata attaches only to
final executable/library consumers; the provider archives stay independent of
release version objects and unit-test metadata.

Implementation `a9e704da59` passes 84/84 default Debug native/restore/runtime
steps, including both actual shared-owner profiles, portable writer/spool
transport, snapshot restore/readback, 44 runtime tests and 13 restore tests.
The earlier expanded implementation `9b6b19d94e` also passed the complete
100/100 Debug composition, including 73 C API implementation tests (2 skips)
and 48 production C API tests, with no failures, leaks or unexpected error logs.
The current source audit verifies 880 embedded production files without server
coordination imports; its 20 checker regression tests and all six memory-profile
policy tests pass. The generated control catalog and default build help are
consistent; no retired build switches remain. The real ordinal kernel compiles
with TSAN read/write/function hooks, but full native TSAN execution remains
unqualified. The focused CPU cache fixture passed; the full 29-fixture suite is
not qualified.

The clean-cache SDK 27.0 release at `9b6b19d94e` completed 88/88 steps in
1,024.735 seconds (17m05s), with sampled aggregate descendant RSS of 11.49 GiB.
This predates the final offline export, static restore and writer/sanitizer
review corrections. The earlier 43m54s to 21m56s cold pair above belongs to its
own frozen source. Neither timing establishes a matched speedup for the final
reviewed source. SDK 26.4, current Linux execution and full TSAN execution need
separate qualification.

The pre-review final boundary `087425d46d` passed 88/88 stripped release steps,
including ordinary C consumers and exactly 118 public C functions with no
internal exports. Its executable was 184,158,664 bytes (5.57% smaller than the
initial PR artifact, 3.18% larger than the recorded main baseline); its library
was 68,606,944 bytes (4.13% smaller than that baseline). The exact executable
link map placed the large prepared mutation worker, portable exporter, snapshot
materializer and resolution/promotion execution only in the Apache engine.
The adapter contribution fell from 24.11 MiB at the prior boundary to 22.52 MiB.
The executable size goal remains incomplete relative to main.

Four alternating model-free throughput pairs at `087425d46d`, each using
1,200 batches of 64 rows after 64 warmup batches, preserved counts and readback.
Document elapsed time increased 8.46% by independent medians and 3.49% by the
median paired change. Relational elapsed time decreased 23.86% and 24.55%,
respectively. Per-round variability and the shared host limit interpretation;
this does not establish broad production throughput parity.

Reviewed release artifact and link-map qualification is in progress. Raw logs,
JSON, command provenance and maps remain outside the repository.

Main `0cba59bceb` was merged into `f2882c6a6e`. Conflict resolution preserves
main's DB-backed extraction budget, repeated identical index admissions and
catalog checks, while retaining pointer-based prepared-owner borrows and the
compact generated error-name catalog. The merged default Debug composition
passes 103/103 steps, including API compilation, both native owner profiles,
73 implementation C API tests (2 skips), 48 production C API tests, 44 runtime
tests and 13 restore tests, with no failures, leaks or unexpected errors.
The merged source audit covers 882 production files and all 20 checker tests
pass. Main's compute-owner cache regression passes: storage-only ABI edits or
removal do not rebuild or block the shared enrichment archive. A standard cache
was removed during an initial run; qualification now uses an isolated cache
inside the worktree. The duplicate local release was stopped after Wolfspider
became available; its isolated cache was removed and raw logs were preserved.
The merge hook could not run the pinned Node formatter for unchanged generated
Model Explorer files inherited from main; only the merge commit bypassed it.
The generated JSON is identical to origin/main.


Concurrent review after the main merge found and fixed three regressions at
`4fc2d298f5`: returned identity/captured search requests now restore the original
caller-owned predicate instead of retaining a stack receiver; resolution and
promotion callback carriers transport errors by name, including reverse
candidate-consumer callbacks and retained batch sources; independent embedded
packaging applies the same graph-wide strip/error-tracing profile to producers
and consumers. The private storage descriptor/getters are now v21; the public
C ABI is unchanged. Regression coverage checks post-return predicate identity,
source callback errors, reverse consumer errors and batch release ownership.
Formatting, AST and diff checks pass; compilation/execution of these final
review corrections is pending on Wolfspider (SDK 26.4). Earlier qualification
above remains attributed to its original source revision.

Wolfspider's earlier pre-review release and the contended local release were
stopped before completion to qualify the reviewed revision instead. Their
elapsed times are not completed build measurements. GitHub's Apache boundary
check passed for the merged revision; the draft PR CI gate deliberately keeps
test runners idle, so its action-required status is not a test failure.

The final review build scripts pass both pinned Zig 17 graph-only help checks
with `-Dstrip=true`, including independent embedded composition; retired flags
are absent from both. The current complete Apache audit verifies 2,050 source
files, and all 35 boundary-checker regressions pass. Temporary local graph-check
caches were removed. Wolfspider release execution remains in progress.

### Linux owner and final-link qualification (2026-10-15)

ELF Zig runtime archives, their final executable callers, and linked test
objects use the same LLVM backend policy as the shared native providers.
The default x86 Debug final link failed to resolve 232 private symbols in the
real server composition; the matching LLVM composition passes. Static
consumers precede the physical engine in final archive order. The final image
receives the engine's inference and enrichment dependencies without making
their emission prerequisites of adapter codegen.

Provider discovery stops at dynamic-library boundaries and excludes foreign
targets. A browser artifact cannot acquire a native engine or libc through
the native finalizer. Direct source-test objects keep their test compilation
context; linked production archives use their own context. Visits are keyed
by both module and context because one module may serve both compilations.
The small ELF/WASM fixture covers these cases, repeated finalization, exact
provider counts, and independent archive emission.

Module graph clones own their mutable configuration containers. Zig's
`Module.init(.existing)` makes a shallow copy; late SDK/include-path changes
could free a buffer still referenced by another clone, crashing configuration
serialization. The shared clone helper copies those containers and resets the
cached import graph. A regression verifies independent buffers and late path
mutation; the real GNU server/C API configuration also passes after this fix.

Both stripped x86_64-linux-musl Debug and unstripped x86_64-linux-gnu Debug
server/C API builds pass 78/78 steps with Zig 0.17.0. The export audit retains
exactly 118 public C functions. The GNU server version command and the real
public C API smoke client execute successfully in Linux. Unstripped GNU
provider codegen peaks at about 5 GiB per large owner in this local cross
qualification; existing large-owner claims remain unchanged.

The shared PDF asset owner exceeded its 256 MiB claim in Linux CI (about
303 MiB). Its 512 MiB reservation retains compiler headroom and the single
shared copy of immutable fonts and glyph lookup data. Local unstripped GNU
codegen reported 264 MiB.

Pgwire passes all 110 tests in each stripped/unstripped Linux executable:
standalone peak RSS is approximately 17/21 MiB, respectively. Linux child
rusage can also retain the launcher's earlier high-water RSS: a trivial
`/bin/true` child of a Python process retaining 72 MiB reported 83 MiB in
the same qualification environment. This accounts for a process envelope
larger than the leaf test's working set. CI reported approximately 93 MiB,
so the Linux test-step reservation is 128 MiB (other hosts retain 64 MiB).
The native array/spool allocator measurements and limits are unchanged.

### Native Linux LLVM Debug admission and storage callback errors (2026-10-15)

PR CI run 38693574492 compiled the baseline x86_64 GNU/Linux CPU Debug
profile without stripping. The API archive peaked at 12,089,720,832 bytes,
exceeding its former 10 GiB reservation. Its native Linux profile now reserves
15 GiB, exceeding that sample by more than 25%. Distributed/CLI summaries
rounded to 8G/2G; their 12/4 GiB claims allow another whole GiB for rounding
before adding 25% headroom. Other runtime claims retain their conservative
values. Cross-host, stripped, sanitized and non-baseline profiles do not
inherit these measurements. Admission tests prevent the API from overlapping
with distributed/storage work within CI's 20 GiB compiler budget while still
allowing the smaller CLI alongside it.

The same run measured public-api-parity and data-runtime test objects at
16,964,825,088 and 11,133,898,752 bytes. Their native Linux LLVM Debug floors
are 20/14 GiB, each including at least 25% headroom and fitting that budget
alone. The policy never lowers an authored claim and leaves other test roots
and build modes unchanged.

Main run 40624165216 measured the C API consumer test object at
17,141,358,592 bytes with the pinned Nix/glibc compiler. Its native Linux
x86_64 GNU Debug reservation is 18 GiB: at least 10% above that peak while
leaving 2 GiB in the existing 20 GiB compiler budget. The separately compiled
physical implementation retains its authored 7 GiB claim instead of inheriting
the consumer's reservation. The Lite model workflow uses the same pinned
Nix/glibc compiler; portable compiler measurements are a different profile.

Main run 39957596246 subsequently exhausted the heavy runner's 24 GiB
cgroup limit while three default 7 GiB compiler claims overlapped under the
workflow's 22 GiB override. Kernel OOM records showed approximately 23.5 GiB
of compiler anonymous RSS and killed the entire runner cgroup; file-backed
pages were negligible. The workflow now matches ARC's 20 GiB aggregate
compiler budget, retaining 4 GiB for the driver, runner and test processes.
The largest current Linux claim (storage/public API, 20 GiB) still fits;
two 8 GiB compiles, or 8 + 8 + 3.75 GiB HTTP work, can still overlap.
The policy tests evaluate the real storage claim and check both this
parallel capacity and exclusion of the observed three-compiler overlap.
This changes scheduler admission without increasing runner resources or
individual compile reservations.

Native Apple Silicon CPU Debug qualification on 2026-10-16 (Zig 0.17.0,
macOS SDK 26.3.1) measured public-api-parity at 18,029,035,520 bytes,
exceeding its 15 GiB authored reservation. Its native Darwin Debug floor
is now 22 GiB, retaining more than 25% headroom. The floor never lowers an
authored claim and does not apply to cross-host builds, release modes or
other test objects. A 30 GiB local compiler budget can still overlap it
with smaller work, but cannot admit another 15 GiB API compile alongside it.
The same qualification measured the API document identity/restore root at
14,373,978,112 bytes against its former 12 GiB claim. Its native Darwin Debug
floor is 18 GiB with the same profile guards and headroom. The object now has
the explicit name `api-docid-tests` for attribution and can be qualified via
`zig build antfly-api-docid-test` without rebuilding unrelated unit roots.
The API auth and derived-coverage roots measured 13,895,155,712 and
14,097,039,360 bytes. Both receive a 17 GiB native Darwin Debug floor under
the same guards; they retain their existing focused targets and now use
explicit artifact names. Aggregate ownership exclusions follow the renamed
artifacts, preserving test selection and coverage.
The broad Raft fixture root also measured 14,420,606,976 bytes against its
former 12 GiB claim. Its `raft-unit-tests` artifact uses a 17 GiB native Darwin
Debug floor, retaining the existing `antfly-raft-test` target and ownership selection.
The shared API table-write implementation test object measured 13,252,476,928
bytes against its former 12 GiB claim. Its native Darwin Debug floor is 16 GiB;
its existing name and all other profiles retain their scheduling behavior.
The encrypted secret-backend fixture measured 15,418,605,568 bytes against its
former 13 GiB claim. Its explicitly named `secret-backends-tests` artifact uses
a 19 GiB native Darwin Debug floor and retains the existing focused target.

The production smoke reproduces Lite table creation returning HTTP 202 with
committed repair debt. Erased storage read/probe/batch and namespace handles
had not all adopted the existing callback error-domain bridge. A raw Zig
error number returned by a caller-owned backend can identify a different
error in the compiled physical owner. Every fallible method on those handles
now uses the originating dispatcher, with direct calls retained inside the
same compilation unit. The private engine ABI advances to v22 for the new
handle layouts; aggregation callers use the matching accessor version.

Storage test discovery now includes the native query reuse and runtime
callback sources. Ordinal tests retain their separately configured module
owner, are declared as dedicated local sources in the audit, and run in the
storage aggregate. Importing ordinal source files into the server catalog
would create duplicate module ownership. The audit validates dedicated local
paths against the local storage root, independently of server-owned suites.
Production smoke failures print server logs before temporary cleanup.

Lite's index-file storage, atomic writers, cold readers, range futures and
immutable range sources also retain their originating callback dispatcher.
Source visitors carry a return dispatcher so visitor failures cross back in
the correct error domain. The caller retains its exact visitor error while a
stable stop marker travels through the provider. Capability/integrity/resource
outcomes have append-only runtime error details, preserving optional mapping
fallbacks and corruption checks. Persistent publication uses the writer methods
rather than bypassing the dispatcher through raw vtable fields. Borrowed spans,
range coalescing, and local direct-call paths remain available.


Storage/source qualification passes 61 selected tests with unexpected error
logs treated as failures, including borrowed spans, fused authentication,
file I/O and returned writer/reader dispatch. The ordinal suite passes 2/2,
the shard audit passes, and all 28 runtime error-identity tests pass.
The restored native-query-reuse and runtime-callback tests also execute and
pass (2/2), rather than merely being accepted by the discovery audit.

The same native Darwin Debug full qualification measured hosted truncate/FK
recovery at 12,111,249,408 bytes and the data runtime implementation test object
at 13,980,401,664 bytes. Their native Debug reservations are 15 and 17 GiB,
respectively, retaining at least 25% headroom; other build profiles keep their
authored reservations.
The sibling self-FK and initial-FK recovery roots measured 11,942,445,056
and 11,940,200,448 bytes, respectively; each native Darwin Debug claim is
14 GiB with the same headroom policy.


### Native Linux Debug test admission (2026-10-24)

Main run `40093707715` used Nix's **glibc-linked Zig 0.17.0**, baseline x86_64
GNU Debug, and an aggregate admission budget of **20 GiB**. The repeated run
reported the following per-compiler high-water marks. No OOM kill was reported;
Zig rejected successful code generation because the declared bound was false.
Changing to the glibc toolchain would not address these measurements.

| Compilation owner | Measured peak bytes | Old claim | New claim |
|---|---:|---:|---:|
| Standalone inline inference owner tests | 15,286,923,264 | 7 GiB | 16 GiB |
| Data physical implementation tests | 17,153,777,664 | 14 GiB | 18 GiB |
| Hosted initial-FK real-service recovery | 13,219,102,720 | 7 GiB | 14 GiB |
| SQL physical test partition | 8,168,538,112 | 7 GiB | 9 GiB |

These roots deliberately exercise physical implementations or the inline host;
substituting consumer facades would weaken their coverage. The data implementation
previously inherited its *consumer's* reservation; a local partition could also
inherit a default unrelated to its selected source closure. Their measured
reservations now belong to distinct stable compile names. This does not change
runtime archive claims or unmeasured platforms/modes, and does not add ordering
edges. The previous fallback floors for other physical partitions are retained.

Claims round each observed peak plus 10% up to whole GiB (about 10–18% actual
headroom). All four can overlap a 2 GiB compile within 20 GiB. SQL can overlap a
normal 7 GiB compile, and hosted recovery can overlap a 6 GiB compile. Policy
regressions assert these admission combinations and preserve larger authored
claims. Future source growth still requires measurement, rather than raising the
aggregate runner size or applying a blanket margin to every test.

Model E2E binaries may use `-Dinference-optimize=ReleaseFast` with a Debug product.
Only the model execution module graph changes mode; API/storage retain Debug
checks. This lets the embedded standalone composition check exercise the same
optimized kernels as the separate inference HTTP helper. BLAS accelerates only
part of model execution and cannot make the remaining Debug kernels satisfy
serving-latency deadlines.

### Native Darwin Debug unit admission (2026-10-25)

The expanded root, data consumer, CPU inference and data implementation tests
exceeded their native Apple Silicon Debug reservations. Zig 0.17 checks a
compiler's reported child high-water RSS after compilation completes; these
were complete compilations whose peaks exceeded their declarations. Zig 0.17
prints failure-looking diagnostics for these overruns, but leaves otherwise
successful compile steps successful. The declarations still govern aggregate
admission, so an understated claim can permit excessive overlap.

Qualification used Zig 0.17.0, SDK 27.0 on macOS 27.0.1, baseline aarch64,
unstripped Debug tests, the default Zig LMDB fixture selection, CPU inference
with Accelerate, and model execution dependencies in ReleaseFast. Production/test source was
`a975e59463`; the previously anonymous root artifact now has the stable
`root-unit-tests` label. Each isolated trial started with empty compiler
local and global caches, reusing only prebuilt provider inputs and immutable
dependency sources. Per-process measurements came from `/usr/bin/time -lp`.

| Compilation owner | Largest complete observed RSS (bytes) | Claim |
|---|---:|---:|
| Root unit contracts | 17,932,107,776 | 18,816 MiB / 18.375 GiB |
| Data runtime consumer | 14,813,134,848 | 15,616 MiB / 15.25 GiB |
| Data runtime implementation | 18,559,696,896 | 19,584 MiB / 19.125 GiB |
| CPU inference tests | 12,070,764,544 | 12,672 MiB / 12.375 GiB |
| Root physical partition | 10,826,579,968 | 11,392 MiB / 11.125 GiB |
| Data control partition | 2,270,003,200 | 2,432 MiB / 2.375 GiB |

Paired cold trial peaks in bytes were:

- Root: 17,932,107,776 / 17,853,480,960.
- Data consumer: 14,807,924,736 / 14,813,134,848.
- CPU inference: 12,070,764,544 / 10,844,323,840.
- Data implementation: 14,280,327,168 / 16,664,952,832; compiler-protocol
  repetitions reported 13,969,735,680 / 13,442,514,944. The broader bounded
  invocation reported 18,559,696,896, which determines its claim.
- Root physical partition: 10,783,260,672 / 10,826,579,968.
- Data control partition: 2,270,003,200 / 2,264,383,488.

The CLI trials preserved compilation inputs and used no-execution mode.
Physical-partition trials retained the actual `--listen=-` compiler protocol,
sending the same update/exit exchange as the non-watch build runner and
validating successful emitted artifacts. Use the largest complete observation
across modes and invocations; neither a sampled resident snapshot nor a
smaller isolated repeat justifies lowering a consumer's claim. Runtime tests
were not included in these compiler timings.

Each claim adds 10% to its observed maximum and rounds up to 128 MiB.
Finer rounding matters: a 15.25 GiB data consumer, 11.125 GiB root physical
partition and 2.375 GiB data control partition total 28.75 GiB, fitting a
36 GiB machine's existing 28.8 GiB compiler budget. Whole-GiB rounding would
serialize that combination. The root can also overlap a 10 GiB compile;
the data implementation can overlap a 9 GiB compile. No ordering edges or
larger aggregate budget were added.

Physical partitions previously inherited their consumers' reservations.
They now use independent claims only after the build verifies the native
Zig 0.17.0 / SDK 27.0 baseline CPU profile, default threaded Zig LMDB,
Accelerate, ReleaseFast inference dependencies, and the absence of stripping, thread sanitization,
TLA tracing, Metal, CUDA and PJRT inference. Other partition configurations
retain their inherited fallback. Consumer floors preserve larger authored claims; Linux,
cross-host and release profiles retain their existing values.

Policy regressions bound headroom, check the admission combinations and reject
unqualified reductions. A real build fixture raises a consumer reservation
without increasing its qualified local claim, then verifies that the
unqualified fallback still inherits it. Coverage and ownership inventories
remain unchanged.

The memory-dimension admission follow-up also qualified the expanded CAPI
physical partition with a cold local and global compiler cache. Compilation
completed in 384.46 s with 4,302,651,392 bytes of peak RSS, but the kernel's
`ri_lifetime_max_phys_footprint` reported 16,587,454,216 bytes. Compression
can therefore make RSS substantially understate the working allocation. The
qualified CAPI-local claim is 17 GiB: the larger complete measurement plus
10%, rounded up to 128 MiB. This is one completed cold observation, retained
as a lower bound for subsequent qualification. Consumer reservations remain
independent; neither compressed RSS nor a smaller repeat lowers this floor.

The expanded CAPI consumer also exceeded its 14 GiB reservation in observed
physical footprint: 15,545,119,176 bytes during completed model qualification.
Its native Darwin Debug floor is 16 GiB, applying the same 10% / 128 MiB rule
to that kernel high-water observation. Other profiles retain their authored
reservations. The consumer and API storage-authority claims sum to 28.375 GiB
and still fit the unchanged 28.8 GiB aggregate budget. The independently
qualified CAPI physical partition retains its larger 17 GiB floor.

### ARM64 Linux release API admission after document jobs

Release run `40426894698` (source `9ace47d363`, the pinned glibc-linked Nix
Zig 0.17.0, Linux x86-64 host, baseline ARM64 targets, stripped ReleaseFast)
exceeded the API archive's 12 GiB scheduling reservation on both targets:

| Target | Reported peak bytes | Reservation |
| --- | ---: | ---: |
| aarch64-linux-gnu.2.28 | 14,346,383,360 | 15 GiB |
| aarch64-linux-musl | 13,929,717,760 | 15 GiB |

The 15 GiB claim retains at least 10% headroom over both exact measurements.
Together with the existing 4 GiB enrichment claim it still fits a 20 GiB
compiler budget. Release builds retain their host/cgroup-aware aggregate budget. It applies only to the measured API configuration;
native x86, Debug, unstripped, sanitized, Darwin and other runtime claims are
unchanged. This avoids a full expensive API retry without serializing the
entire build or increasing the runner's memory budget. The run also exposed
missing compiled metadata job-state projections; those compilation errors are
separate from memory admission.

### Native Linux Debug auth admission (2026-10-27)

Main run [40586926751](https://github.com/antflydb/antfly/actions/runs/40586926751)
used Nix's glibc-linked Zig 0.17.0 on native x86_64 GNU Debug. Read-only
2-second `/proc` sampling retained the compiler's `VmHWM` through completion:

| Compilation owner | Observed high-water bytes | Old claim | New claim |
|---|---:|---:|---:|
| `api-auth-tests` | 15,804,899,328 | 7 GiB | 17 GiB |
| `antfly-inference-core` | 9,540,190,208 | 12 GiB | unchanged |

The auth object and inference compiler overlapped because their declared claims
summed to 19 GiB under the 20 GiB admission budget. The 24 GiB pod's cgroup peak
reached 25,769,779,200 bytes. Auth's new floor rounds its observed high-water
mark plus 10% up to a whole GiB. The runner and aggregate budget are unchanged:
auth can still overlap 3 GiB of smaller work, while its unsafe overlap with the
12 GiB inference compiler is excluded. Other owners and optimization/target
profiles retain their claims. This is an admission correction, not a reduction
in the compiler's own memory use.

The preceding main unit runner in run
[40566906276](https://github.com/antflydb/antfly/actions/runs/40566906276)
reported lost communication. GKE's retained pod deletion status identifies an
`OOMKilled` termination (exit 137) at 19:41:11 UTC against the same 24 GiB
container limit; this was not merely a GitHub runner connectivity failure.
Sampling the next run identifies the auth underclaim directly, rather than
attributing a vanished runner to a test assertion or increasing runner size.


### Linux-to-Darwin release admission (2026-10-28)

[Release run 40791025212](https://github.com/antflydb/antfly/actions/runs/40791025212/job/123177113959)
compiled source `d35a1ef33e` on the Linux x86-64 ARC publish runner with the
pinned Nix glibc-linked Zig 0.17.0, macOS SDK 26.1, baseline
`aarch64-macos.15.0`, stripped ReleaseFast, Metal and Accelerate. Its completed
server build reported these per-archive measurements:

| Archive | Compile time | Rounded MaxRSS | Reservation |
| --- | ---: | ---: | ---: |
| Apache storage owner | 26 min | 13G | 16 GiB |
| Apache inference owner | 17 min | 11G | 14 GiB |
| API kernel | 23 min | 14G | 17 GiB |
| Storage adapter | 14 min | 9G | 11 GiB |
| Distributed | 12 min | 8G | 10 GiB |
| Serverless | 6 min | 4G | 6 GiB |
| CLI | 2 min | 2G | 4 GiB |
| Enrichment | 45 s | 1G | 3 GiB |
| Inference bridge | 48 s | 1G | Existing shared-owner claim: 4 GiB |

Read-only runner inspection confirmed a 40 GiB cgroup and the unchanged
32 GiB compiler budget. The broad fallback reserved 28 GiB for both API and
storage adapter, 24 GiB for the Apache storage owner and 20 GiB for its
inference owner. Each pair therefore compiled serially despite its measured
working sets fitting together. The reservations above add a whole GiB to each
rounded measurement, then 10% headroom, rounding up again to GiB. The shared
inference bridge retains its existing 4 GiB claim.

The new profile requires this compiler version, GNU Linux x86-64 host,
baseline macOS ARM64 target, SDK 26.1, stripped ReleaseFast, shared owners,
Metal/Accelerate and no CUDA, PJRT or thread sanitizer. Native macOS, other
SDKs, CPUs, optimization modes and sanitizers retain their prior policies.
Regression tests check both margins and rejection of unqualified profiles.
Storage plus inference now reserves 30 GiB; API plus adapter reserves 28 GiB.
No runner size, aggregate budget, optimization mode or coverage changes.
A cold rerun is required to measure the resulting wall-time improvement;
these observations establish admission, not a claimed speedup.


### Base benchmark gate OOM (2026-10-28)

PR run `40799072264`, job `123203194445`, lost its ARC runner during the
Debug storage/graph benchmark compile. GKE recorded `TaskOOM` at 01:41:40 UTC
and container exit 137. This was an unbounded `zig build`, using the static
compiler, while the graph consumer also declared zero scheduler memory.

The gate now uses pinned Nix/glibc Zig and the bounded runner under its existing
20 GiB compiler / 24 GiB pod budget. The graph consumer inherits the conservative
storage compilation envelope (20 GiB on Linux); the independent storage-boundary
shim reserves 4 GiB in addition to its linked archives' own reservations. This
accounts for source codegen instead of allowing it to overlap large owners for
free. No forced single-job mode, larger runner or dropped benchmark contract
is introduced. These consumer claims are conservative pending isolated peak
measurements; they are not new measured reductions.

### Native Linux ReleaseFast auth qualification (2026-10-29)

Run `40820082669` (glibc-linked Zig 0.17.0, baseline native x86-64 GNU,
stripped ReleaseFast, CPU inference) reported API archive peak RSS of
6,698,409,984 bytes against a 5 GiB claim. Its reservation is now 8 GiB,
over 25% above the observed peak, while retaining overlap with the 8 GiB
storage owner within a 20 GiB budget. Other compiler versions, units and
profiles retain their existing claims. The observed peak came from a failed
compile and remains a lower bound pending successful qualification.

The same job completed the C API after 82 minutes and reported the API
archive failure after 111 minutes, then reached its two-hour job limit.
The auth workflow now allows three hours for the combined cold library and
server build, without changing runner size or aggregate memory admission.
