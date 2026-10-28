# AQ: large-owner consolidation campaign

Baseline: `b125030454`. All four large-win audit areas were prototyped.
Accepted changes and rejected approaches are recorded separately below.

## Metal pipeline lifecycle: implemented

462 ordinary scalar pipeline entries now have one lifecycle identity catalog.
Its 29 declaration groups preserve the complete runtime structure layout.
Separate creation and retirement ordinals preserve their independent original
orders. Constructor and destructor ranges stay at their original positions,
including all intervening conditional/custom lifecycle work.

Hot encoders retain their original named fields. There is no hot dictionary
lookup. The lifecycle tables use typed slot selection so Objective-C ARC sees
the actual strong field; casting byte offsets to strong Objective-C slots is
invalid and was rejected by the first syntax check. The new header participates
in Metal JIT qualification identity.

Including the catalog, typed accessors and JIT identity entry, this removes
**59,693 maintained production bytes**. Generated sources and shader arithmetic
are unchanged. No demand-loading or pipeline-sharing performance claim is made
for this first migration.

Acceptance:

- Expanding the catalog's declarations and ordered lifecycle ranges reconstructs
  all **550,526 original C tokens** exactly, excluding the new machinery itself.
- Clang's complete runtime record layout, including every field offset and total
  size/alignment, is identical to baseline.
- Clang ARC syntax checking passes.
- The production OCR Metal checker passes all numerical projection, producer/
  consumer, copy, canary, fallback and cancellation checks.
- Generated-source freshness passes: **70 files + 129 native owned regions**.

Evidence: `.benchmark-assets/1166/aq-campaign/`, including the original source,
462-entry mapping, semantic expansion/layout proof, compiler logs and production
Metal check results. The rejected byte-offset ARC prototype is retained in the
initial syntax log.

## CPU fixed-cardinality projection family: implemented

Q1 and Q8 projection pairs/triples share validation and traversal owners. Q1,
Q8 and Q8_1 single/pair/triple activation preparation shares one stack/heap
ownership scope with compile-time codec/cardinality policies. Q1 keeps its
serial quantizer; Q8 keeps its dispatching quantizer. Single-Q8 profiling and
all existing worker/dot-kernel selections remain explicit.

The final source removes **19041 maintained production bytes**.
Generated activation/worker regions remain byte-identical. The reduction is
slightly below the audit's 20 KB prototype goal; Q4/Q6 packed-layout/bias routes
are retained rather than forcing distinct schedules through this owner.

Acceptance and limits:

- Existing format/routing regressions: **31 passed / one skipped**.
- The final Q1/Q8/Q8_1 test binary also compiles for baseline x86 Linux.
  This is a portability compile check; x86 runtime performance is unmeasured.
- The initial 48-profile Q1/Q8 kernel-owner matrix passes AA/AB/BA on both
  native and portable non-SDOT builds, with exact original-function outputs.
- The expanded 108-profile matrix includes original Q1 dispatch and scan
  bodies, raw Q8 and prepared Q8_1, one/two/three planes, tails, panel and
  row-major storage, and stack/heap-sized inputs. Both binaries pass all exact
  output comparisons and their 38 imported smoke tests.
- All 324 native profile/order checks pass. Maximum candidate AB/BA median
  ratio is 1.032574.
- Every portable candidate AB/BA comparison meets the same 1.05 median / 1.10
  p95 bounds (maximum median 1.013484). One **baseline-versus-itself AA p95
  control fails at 1.128196**, while its median is 1.002605. The full portable
  matrix remains recorded as failed; no failed measurement is retried or
  relabeled. Portable timing is therefore not fully qualified.

Evidence and exact source/binary pins: `.benchmark-assets/1166/aq-campaign/cpu/`.
Temporary original-function references live inside the retained qualification
fixture; they are removed from production before committing. The numerical
implementation remains in the production source.

## Metal immutable pipeline sharing: implemented

The catalog now also owns preparation identity: the same catalog slot and
immutable library identity share a pipeline across runtimes. Distinct fields
remain distinct even when their shader function names match. Ordinary and
precise libraries cannot alias. Hot encoders continue to read their named
runtime fields directly.

Each retained library has a fixed 462-slot strong array; at most four library
namespaces are retained. Eviction releases cache references without invalidating
live runtimes. Preparation is synchronized, successful results publish once,
and failures remain retryable. Custom and conditional pipeline loaders retain
their original preparation behavior. Eager validation remains intact; demand
loading requires an explicit model preparation contract and is deferred.

The sharing owner adds **2,387 bytes** to the catalog migration, leaving
**57,306 bytes removed** across the Metal lifecycle family including its header
and JIT identity entry.

Acceptance:

- Contract checks cover identity, distinct slots, library provenance, concurrent
  publication, failed compilation retry, device mismatch, bounded eviction and
  caller-held pipeline lifetime, plus every valid catalog slot.
- The final production Metal numerical/projection checker passes again with
  sharing enabled, including copy, canary, fallback and cancellation checks.
- A fixed AA/AB/BA startup fixture measures nine constructors per process,
  three process pairs per order, after preparing shared shader libraries.
  Warm constructor medians improve **3.76–3.94×**, from 0.628–0.664 ms to
  0.167–0.169 ms. Warm p95 ratios are 0.274–0.288.
- First constructor ratios are 1.038–1.042, within the preset 1.10 bound.
  All controls pass. These measurements exclude source-library compilation
  and are neither model throughput nor fully cold startup claims.

Evidence: `.benchmark-assets/1166/aq-campaign/pipeline-cache/`, including
contract fixtures, immutable before/after binaries, protocol, raw observations
and source/binary hashes. Advisory NSCache and name-keyed alternatives were
rejected before timing in favor of the bounded typed-slot owner.

## Metal whole-decoder phase ownership: prototype rejected

The direct/planned decoder inventory does not represent 164 KB of identical
phase bodies. The selected four native functions share at most 77 exact lines
per pair. A concrete Zig prototype replaces the gathered fallback's finish
sequence with the existing common finisher and removes **6,251 bytes**, below
the audit's 30 KB acceptance goal.

More materially, that finisher attempts BF16 gate/up execution and additional
planned scopes/output reuse, while the original fallback explicitly takes F32
projection and activation routes. Calling it directly changes precision,
scheduling and encoder ownership. The prototype was rejected before runtime;
none of its apparent savings count toward the accepted reduction.

A further migration needs explicit precision, schedule and ownership policies
in the existing prepared decoder plan before routes can share an execution
owner. It is not safe to replace this with call forwarding alone.

Evidence: `.benchmark-assets/1166/aq-campaign/decoder/`, including the original
source, formatted standalone prototype, exact-phase overlap and rejection.

## CUDA attention launch preparation: implemented; device validation queued

This family removes **8,528 maintained production bytes**, including migrated
call sites. Six dense, device-scalar, generated and physical-paged attention launchers now
accept a shared GQA request. Buffer preflight and checked ABI packing have one
owner. Existing eligibility descriptors are projected at compile time; native
ABI declarations remain authoritative for wire types. Each launch still owns
mutable argument storage at its existing capture/driver boundary.

Kernel selection, explicit argument tuple order, physical KV policies,
workspaces, launch schedules and device-scalar replay semantics remain in their
existing launchers. The prior unmasked generated-paged route still explicitly
zeros mask/bias wire fields. Device source, generated regions and compiled
images are unchanged.

Static comparison confirms all **191 argument slots across nine launch tuples**
retain their original order. Host checks pass for dense/scalar/paged extents, overflow, short buffers,
mutable wire addresses and the non-device-scalar generated-route guard.
Both-backend compilation, Linux ARM64 CUDA test compilation and generated-source
freshness pass. CUDA numerical, compressed-format, paged-prefill and graph-replay
checks are queued on Glowworm behind unrelated GPU work; their status will be
recorded after execution, without treating skips as passes.

Evidence: `.benchmark-assets/1166/aq-campaign/cuda/`. Final whole-campaign
source accounting includes all replacement metadata and excludes tests,
generated source and binaries. CPU portable timing qualification remains
limited by the failed AA control recorded above.

## Net accepted source reduction

Against the audit baseline, including every replacement owner and authored
catalog, the campaign removes **84,875 maintained production bytes / 1,353
lines**:

| Family | Net bytes removed |
| --- | ---: |
| Metal lifecycle, sharing, catalog and identity | 57,306 |
| CPU projections | 19,041 |
| CUDA launch preparation and callers | 8,528 |
| Total | 84,875 |

Authored Metal shader arithmetic, generated regions and compiled device images
have zero source/image delta in this campaign. The new numerical/contract tests
are accounted separately, not used to inflate production savings. This is a
reduction in maintained runtime and launch code, not a 20% reduction in GPU
arithmetic bodies. The rejected decoder prototype contributes zero savings.

Machine-readable accounting and source pins:
`.benchmark-assets/1166/aq-campaign/source-final.json` and `accepted-snapshot.json`.
