# Retained PDF operation ownership

Status: implemented and focused qualification passed; complete native product
screening is next. `-Dshared-pdf-sessions=true` selects this migration separately
from template routing. The user accepted the earlier template size tradeoff;
`shared-pdf-templates` now defaults to true, with an explicit false comparison
still available. The retained migration remains separately selectable during
whole-product qualification.

The existing media archive now owns mutable readers, immutable fork templates,
PDF parsing, page analysis, render preparation and PNG/raster batches. Storage
and inference retain their admission, scheduling, document orchestration and
model/device selection. They use opaque reader and fork handles through a PDF
facade. Source-only PDF and document suites retain the original source APIs.

## Ownership and contracts

Native operations transport compiler/layout-checked arguments and results, with
one hidden accessor per complete operation. Unused accessors can be discarded
at final link. A global function table would retain every renderer operation in
each product and is intentionally avoided.

Error integers never cross archives. Scalar errors, prepared-plan failures,
per-page failures and executor failures travel as static error-name borrows.
Caller-only errors survive in the caller's operation-local executor slot or
original failed-plan slot, including errors absent from the shared registry.
Already-failed plans remain ordered slots and never launch rendering work.
The provider does not root a global error-name decoder just to mark those slots.
The existing versioned `IoBorrow` translates the compatibility IO executor.

Reader/fork state and all scratch/results use the original allocator value and
vtable, including resize/remap. No stack-backed allocator adapter is retained.
Source bytes and allocator contexts remain borrowed until close, and a prepared
source outlives its fork templates and instantiated readers. All batch/executor
calls are synchronous through worker completion. Raster and PNG payloads are
transferred directly; only bounded plan/result metadata arrays are converted.
Concurrent-output allocator identity is preserved, allowing existing direct
worker-to-result allocation to remain active.

The original render requests, decode limits, admission limits, profiles,
geometry, diagnostics, cancellation, scratch-growth callbacks, fixed executor
and compatibility executor are forwarded. GPU benchmark preparation can obtain
complete owned render runs while device execution remains with its caller.
There are no per-pixel, glyph or quantized-block foreign callbacks.

`lib/pdf/src/values.zig` now owns shared geometry, diagnostic, text, image/run,
plan and result values plus their ownership/geometry math. The original PDF
reader and renderer re-export these definitions instead of maintaining parallel
DTOs. This module imports only `std`; neither the contract nor facade imports
the reader/renderer implementation. Transient render-font references are opaque
and cleared before render runs escape the provider. Shape/pattern/shading enums
have stable names so native layout fingerprints do not depend on compiler-local
anonymous enum IDs. The private storage descriptor version is 31; public CAPI
signatures and durable JSON/record formats are unchanged.

## Focused qualification

- Seven compiled owner/client tests passed in Debug and Fast, without skips or
  leaks. They cover retained analysis, fork instantiation, prepared PNG/raster
  batches, full backend factories, adaptive outputs and GPU-preparation runs.
- Exhaustive allocator failure injection exercises open, analysis, fork/child
  creation, plan preparation, rendering and metadata/payload cleanup.
- Fixed-executor tests preserve direct concurrent output and caller-only error
  names; two-page compatibility IO execution, cancellation, invalid limits,
  failed-plan ordering and incompatible endpoint rejection are covered.
- The original PDF/document source suites passed all 548 tests after the value
  move. Separate source constructors under the retained-owner option passed
  79 additional tests, including focused inference kernel coverage.
- Existing seven opaque PDF/session/backend tests and two compiled-template
  tests passed before the value move; the complete product run will repeat them.

## Actual cache matrix

`python3 zig/tools/check_media_owner_cache.py --retained --report ...` uses a
private overlay inside the worktree cache and removes it afterward. All phases
passed without pulling storage or inference compilers into the media-only test:

| Edit | Media compiler | Client compiler | Observed seconds |
| --- | --- | --- | ---: |
| Initial | built | built | 103.2 |
| Warm | cached | cached | 11.4 |
| Provider operation | built | cached | 47.8 |
| Reader implementation | built | cached | 44.2 |
| Renderer implementation | built | cached | 45.3 |
| Client facade | cached | built | 17.1 |
| Shared values | built | built | 43.5 |
| Native protocol | built | built | 43.3 |

These establish cache reuse for this real compiled provider/client pair, not a
whole-product cold-build speedup or an accepted memory reservation. Production
archive guards and final linked attribution still need to show the old PDF
implementations disappear from storage and inference.

Ignored receipts live in `.benchmark-results/compile-costs/`: the
`pdf-native-*`, `pdf-values-*`, and `pdf-retained-cache*` logs/reports. Earlier
failed logs retain incorrect expected error spellings and the anonymous-enum
layout mismatch, separately from the passing results. No PDF error semantics
were changed to satisfy those expectations.
