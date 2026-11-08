# Production compilation costs and ownership

Status: production ownership changes are implemented; final optimized runner
qualification remains open. The living build guide is
[COMPILATION.md](../../../zig/COMPILATION.md). Dated results, rejected approaches
and pinned detailed records are in the [qualification summary](history/production-owners-2026-11-02.md).

## Implemented boundaries

| Area | Current behavior |
| --- | --- |
| Test compilation | Common/provider/parser suites share compiled artifacts. Focused mutation, aggregation and paging plans compile without the full server. Watch mode retains incremental compiler state where supported. |
| Mutation values | Concrete request/coalescing plans avoid owner-specific generic instantiations. Public decoding imports public values; privileged commands use a separate internal codec. |
| Template PDF | Complete template extraction/render operations use the existing media owner by default. `-Dshared-pdf-templates=false` permits comparison. |
| Retained PDF | `-Dshared-pdf-sessions=true` routes retained readers, analysis, fork caches and bounded render batches through the media owner. |
| Image operations | `-Dshared-image-operations=true` selects whole encoded-image operations and retained CPU VP8 sessions on supported native 64-bit macOS/Linux profiles; it also enables retained PDF sessions. Default remains off. |
| Lake execution | `-Dshared-lake-execution=true` moves complete index attempts to the serverless owner. Coordinator publication/lease authority stays in the caller. Default remains off. |
| Query execution | Parsing, execution and encoding live beside private DB state. The dispatcher consumes a checked operation capability. |
| Read/restore ownership | Owner-issued relational/document readers, statement handles, statement capture fences and boxed query leases preserve typed values and explicit page release. Cypher uses bounded session operations and immutable metadata; pinned session construction stays in the owner. Retained local query-cut locks and publication readers expose release capabilities rather than file/executor/manager state. Disposable restore materialization stays inside its owner. |
| Embedded C library composition | `-Dcapi-profile=storage` omits local model execution while preserving the C ABI and storage operations. The default remains `full`; media remains included. |
| Commit disposition | Retry-shaped errors can retry preparation only before primary commit, preventing duplicate non-idempotent application. |
| Disabled CUDA | Compile-time capability checks prevent CPU-only owners from instantiating concrete CUDA cleanup/execution paths. |

These changes preserve eight runtime archives plus shared physical storage and
inference owners. Further splits require measured benefits to both installed
products. The standalone index/tensor probes and Q8 loop screening harness were
exploratory and are removed; they introduced no production topology or kernel
optimization.

## Ownership rules

Each boundary transfers a complete operation or bounded page/batch. Keep private
parsers, codec state, DB state and destruction in the creator's owner. Avoid
per-pixel, per-posting or per-row foreign data callbacks. Shared contract modules
must import values rather than private implementations.

Preserve allocator identity, alignment, resize/remap semantics and partial-result
cleanup. Forward actual allocations through the caller's original hard quotas.
Treat foreign allocator vtable identity as insufficient evidence of available
capacity: only a checked, creator-issued capacity query can expose that fact.
Lake admission takes the minimum of provider and backing capacity; unknown/low
capacity retains serial fallback.

Retained handles validate version, kind, generation and layout before reading
state. Keep cancellation, deadlines, IO and donor-drain lifetimes bounded to the
operation. Page and reader release remain explicit. Exact NUMERIC/SQLARRAY and
integer values must survive page release and session close without conversion
through lossy JSON numbers. Private spill/pattern/page metadata cannot escape.

The engine still has concrete lifecycle/layout checks outside the migrated
operation families. Do not delete those checks while their private borrows
remain. Retained cover authority, remote query-cut lifetimes and general DB
lifecycle coupling are follow-up work, not completed redesign claims.

For mutation cleanup, preserve acquire/release order, lock fences, durable
outboxes and publication authority. The [resource-frame design](mutation-resource-frame.md)
records the lifetime constraints. A larger cleanup migration needs compiler
attribution and crash/recovery qualification; the disposition fix alone does
not justify it.

## Measurement and acceptance

Track these outcomes separately:

| Outcome | Required measurement |
| --- | --- |
| Binary size | Installed stripped `antfly` and `libantfly`, with final-link attribution |
| Cold build speed | Equally cold compiler caches, both products, critical path and per-unit time |
| Compiler memory | Individual and simultaneous aggregate/cgroup peak, plus swap |
| Runtime | Request allocation/live bytes and representative latency/throughput |
| Local iteration | Real implementation edit with actual unrelated consumer cache reuse |

An archive's size or repeated symbol name is a lead, not a recoverable final
size estimate. Do not add overlapping estimates, count zero-filled BSS as file
bytes, or substitute a small caller benchmark for product qualification.

Freeze source, compiler/stdlib hashes, target/CPU, SDK/libc, backend flags,
sanitizers, PIC, strip, generators, version metadata, CPU allocation and memory
limits. Prime dependencies equivalently. Separate compiler-cold timing from
fully cold builds; exclude attribution overhead from primary acceptance timing.
A post-link stripped diagnostic image is not necessarily a `-Dstrip=true` build.

For focused ownership checks that do not need a debugger, combine
`-Doptimize=debug -Dstrip=true`: runtime safety remains enabled, while compiler
debug-information emission and symbolized traces are omitted.
`ANTFLY_TEST_DEEP_ALLOC_TRACES=1` expands allocation traces to 32 frames in an
unstripped diagnostic test run; normal tests retain the standard trace depth. Keep ordinary
Debug builds for debugger and trace qualification. A live compiler sample during
the November 6 main integration was in LLVM DWARF finalization, with a sampled
9.6 GiB peak footprint. That build was later invalidated by cache eviction;
the sample motivates a controlled comparison, not a build-speed claim.

Screen with one paired run; qualify with at least three alternating frozen
baseline/candidate pairs on the normal runner. CPU-only results do not qualify
GPU/default-Metal profiles. A successful collector run does not automatically
accept a migration or enable a default.

Retain exact source snapshots, source/toolchain provenance, installed artifacts,
raw process samples, maps and failed-run diagnostics outside tracked source.
Build caches remain within the worktree. Cache leases must detect deletion or
replacement; eviction invalidates the run. Compiler `--time-report` forces a
rebuild and must never be used in a warm-cache isolation check.

## Durable checks

- `check_owner_archive_symbols.py`: all ten production archive ownership guards.
- `check_image_owner_cache.py`, `check_media_owner_cache.py`: implementation,
  consumer and contract invalidation for the media owner.
- `check_lake_execution_cache.py`, `check_query_owner_cache.py`,
  `check_public_mutation_cache.py`, `check_read_session_owner_cache.py`:
  production cache isolation, including actual private-layout edits and shared
  request invalidation. The retained-read tool supports `--profile capture`, `--profile reader`,
  `--profile publication` and optional `--strip` for serial Debug checks without debug information.
- Image/PDF native contracts, opaque session/query consumers and retained-read
  fixtures: source/native equivalence, allocation failure and cancellation.
- Real S3 replay and same-executable lake runtime comparison: retained metadata,
  reopen/retirement fences, capacity admission and full-attempt timing.
- `profile-zig-build.yml`: optional compiler, archive and final-link attribution.
- `profile-zig-products-paired.yml`: frozen paired product costs and artifacts.
- `zig-owner-qualification.yml`: hosted policy checks, heavy native ownership
  tests and full/scheduled cache matrices. Focused behavior checks use stripped
  Debug; production archive attribution retains unstripped Debug symbols. Both
  C library profiles run the C consumer smoke suite.

## November 8 local screen

The optional C library profile was compared on frozen `ba8493237e` sources with
Zig 0.17.0, macOS arm64, baseline CPU, stripped Debug, Metal/CUDA/system BLAS off.
The installed full library was 199,828,784 bytes and the storage profile was
124,558,272 bytes (37.7% smaller). Both export/linkage and C smoke checks passed.
This measures the cost of omitting a capability; it does not claim equivalent
features or release build-speed/memory improvement. The available build times
mix warm and cold caches and are unsuitable for that comparison.

After integrating main, both library profiles were rebuilt from identical
`9c64a7384ffaf2615b2d69f647c3c03b8f5c6baa092e051240a73ac74f2a2ab0`
authored inputs with native image and lake options enabled. The full library was
200,065,984 bytes and storage was 129,707,344 bytes (35.2% smaller). The full
product/native-client build passed 156/156 steps; the storage C library and smoke
suite passed 54/54. Both retained the 141-function public export boundary.
The cold compiled-owner restore regression and all 32 generation-lifecycle tests
passed. This remains a CPU-only macOS Debug screen, not Linux release acceptance.

Optimized CPU image transport passed 112 source and 112 native fixtures, including
four VP8 cases per mode. This alone does not qualify runtime performance, GPU
profiles or a default rollout. The optimized lake screen passed correctness,
but native/source total-attempt medians were 1.081 cold, 1.183 on checkpoint
resume and 1.065 on incremental append. Native working sets were also larger.
Keep lake opt-in while investigating decoded-source reuse, provider cache cost
and I/O dispatch; faster compilation cannot justify that runtime regression.

The publication-reader migration retains file locks and manager registrations in
its creator. Shared handles carry version, context and a C release function.
Lock installation is owner-only and validates the creator before taking a file.
Pre-open restore admission acquires and queries publication readers through the
physical engine. A foreign I/O receiver stays at a stable owner-allocated address
until the publication lock closes.
Allocation failure must unwind reader registration and reconciliation promotion;
closing clears the handle before invoking the owner. Broader DB lifecycle
coupling remains open. The four-phase publication cache matrix passed: private
holder fields preserve API, kernel and native-client compiler caches while the
physical owner/source fixture rebuild; a shared-capability field rebuilds all
five participants.

The main integration also exposed a Cypher reader arena transfer bug. Projection
allocations now finish before the arena is copied into the reader and before
schema/index pins transfer. Twenty-five unstripped Cypher tests passed without
leaks, including a walk through every projection-initialization allocation
failure. Independent native session fixtures also exercise metadata, read-time
selection, bounded pages and scoped visitors.

## Open work

1. Qualify the final head on normal Linux release and runtime runners. Compare
   default production flags with the current main baseline; measure opt-in image
   and lake variants separately before considering rollout.
2. Repeat optimized product captures with at least three alternating pairs.
   Complete server final-link attribution where the pinned linker supports it.
3. Attribute optimized LLVM and final-link work before choosing another owner
   cut. The merged stripped Debug report no longer singles out private batch
   parsing: generic formatting, sorting and JSON lead frontend costs, while
   reported LLVM emission dominates wall time. Measure complete-operation
   duplication and backend cost before broad codec or mutation-phase rewrites.
4. Remove remaining private lifecycle borrows one complete operation family at
   a time, retaining layout checks until each replacement is qualified.
5. Evaluate per-module optimize modes, embedded assets and additional cold
   libraries only after linked attribution establishes a worthwhile target.

Do not promote ThinLTO, safe identical-code folding, Q8 loop changes or broad
serialization rewrites from isolated probes. The pinned Mach-O linker has
capability limitations, Q8 small-row timings were mixed, and generic JSON totals
do not establish equivalent code or an allocation-safe replacement.
