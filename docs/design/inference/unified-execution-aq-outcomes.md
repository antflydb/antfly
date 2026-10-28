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

## Remaining campaign work

- Prototype pipeline sharing/demand preparation independently of the catalog;
  preserve precise-library provenance and required failure behavior.
- Prototype Metal whole-decoder phase ownership across the audited direct and
  planned routes, without changing kernel schedules or frame resource rules.
- Prototype the fixed-cardinality CPU projection family, preserving specialized
  microkernels and measuring independent arithmetic/performance references.
- Prototype CUDA attention launch preparation using existing descriptors and
  native ABI owners; preserve capture slots and all physical KV formats.

Both-backend compilation and final whole-campaign source accounting will be
recorded against the final accepted sources. An unqualified prototype does not
contribute to reported savings.
