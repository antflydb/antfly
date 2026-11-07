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
| Read/restore ownership | Owner-issued relational/document readers, statement handles, statement capture fences and boxed query leases preserve typed values and explicit page release. Retained local query-cut locks expose a release capability rather than file/executor state. Disposable restore materialization stays inside its owner. |
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
debug-information emission and symbolized traces are omitted. Keep ordinary
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
  request invalidation. The retained-read tool supports `--profile capture`
  and optional `--strip` for serial Debug checks without debug information.
- Image/PDF native contracts, opaque session/query consumers and retained-read
  fixtures: source/native equivalence, allocation failure and cancellation.
- Real S3 replay and same-executable lake runtime comparison: retained metadata,
  reopen/retirement fences, capacity admission and full-attempt timing.
- `profile-zig-build.yml`: optional compiler, archive and final-link attribution.
- `profile-zig-products-paired.yml`: frozen paired product costs and artifacts.
- `zig-owner-qualification.yml`: hosted policy checks, heavy native ownership
  tests and full/scheduled cache matrices.

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
