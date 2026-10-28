# AQ: large-owner consolidation campaign

Baseline: `b125030454`. This implements the large-win audit's four work areas;
the campaign remains in progress until each area's prototype and acceptance
result is recorded.

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

## Remaining campaign work

- Prototype pipeline sharing/demand preparation independently of the catalog;
  preserve precise-library provenance and required failure behavior.
- Prototype Metal whole-decoder phase ownership across the audited direct and
  planned routes, without changing kernel schedules or frame resource rules.
- Preserve the portable timing-control limitation above; any further CPU
  timing work must retain the failed matrix and use a justified protocol.
- Prototype CUDA attention launch preparation using existing descriptors and
  native ABI owners; preserve capture slots and all physical KV formats.

Both-backend compilation and final whole-campaign source accounting will be
recorded against the final accepted sources. An unqualified prototype does not
contribute to reported savings.
