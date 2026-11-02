# Retained PDF operation ownership

Status: implemented; focused regressions and repeated cancellation checks pass.
Debug product screening passed; optimized size/cost screening is still pending. `-Dshared-pdf-sessions=true` selects this migration separately
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

Foreign error integers are never interpreted. Scalar errors, prepared-plan failures,
per-page failures and executor failures travel as static error-name borrows.
Caller-only errors survive in the caller's operation-local executor slot or
original failed-plan slot, including errors absent from the shared registry.
Already-failed plans remain ordered slots and never launch rendering work.
The provider does not root a global error-name decoder just to mark those slots.
The existing versioned `IoBorrow` translates the compatibility IO executor.

Decoder controls cross the native endpoint as an explicit synchronous borrow.
The provider uses a terminal cancellation marker internally so parser fallbacks
cannot swallow a caller's control error. Its static error name and opaque
origin-scoped token accompany the failure. Only the original module recognizes
the domain address and restores that token to its own error value; foreign
modules decode names. Workers retain the identity before their TLS scope ends. The joined
coordinator returns terminal identity through an explicit operation-local output
borrow before unwinding the scalar cancellation result. CPU workers install the borrowed control themselves, so the coordinator
keeps no thread-local scope installed across a compatibility I/O suspension.
The image control is one named module per compiler owner, shared with codecs;
its interface does not import any decoder implementation.

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
anonymous enum IDs. The initial rollout used private storage descriptor 32 and
native PDF endpoint v2. The subsequent shared image-value migration uses
descriptor 33; the counted-table guard below advances the PDF endpoint to v3.
Public CAPI signatures and durable JSON/record formats are unchanged.

## Focused qualification

- Nine compiled owner/client tests passed in Debug and Fast, without skips or
  leaks. They cover retained analysis, fork instantiation, prepared PNG/raster
  batches, full backend factories, adaptive outputs and GPU-preparation runs.
- Decoder controls retain caller-only error identity through direct calls,
  thread workers, compatibility IO and fixed executors. A restart JPEG fixture
  proves cancellation after decoding has started; a subsequent call recovers.
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

## Debug iteration and cancellation qualification

Routine ownership, cancellation, source-library, scene-transport and cache
checks use Debug. `check_media_owner_cache.py` and
`check_pdf_gpu_scene_transport.py` now default to Debug and accept an explicit
`--optimize fast` for final qualification. The profile workflow forwards its
selected mode. Binary-size and LLVM compile-time/memory comparisons require a
separate optimized build after correctness is stable.

Repeated execution exposed a secondary wave-cancellation race after the first
passing focused runs. An earlier worker could report its own `Canceled` marker
before the lane that carried the caller's original control error. Worker
identity capture now retains forwarded identities instead of secondary local
markers. The regression also runs a fixed executor in reverse order. The fix
passed 200 repeated executions in each of Debug and Fast in isolated overlays
before promotion. These runs include actual compatibility-I/O scheduling.
The earlier passing runs alone were insufficient qualification.

The seven PDF/OCR HTTP cases pass against the preserved accepted template
baseline: inline/URL/paged OCR and JPEG inputs, automatic OCR selection,
malformed-document terminal manifests, and local/Lite cursor reopening. The
migrated Debug products also passed those seven cases, plus all six query
deadline cases (13 total).

Known-failing or superseded optimized runs are retained as failed receipts,
not performance samples. After switching the iteration loop to Debug, the
optimized product build was stopped. No new release-size or memory improvement
has been established for retained readers yet.

## Actual cache matrix

`python3 zig/tools/check_media_owner_cache.py --retained --report ...` uses a
private overlay inside the worktree cache and removes it afterward. The earlier
Fast matrix, before decoder-control forwarding, passed all phases without pulling storage or inference compilers into the media-only test:

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

The current cache tool additionally checks JPEG implementation and control
interface edits, plus a reachable provider-code edit. That edit introduces a
non-inlined volatile-read identity function into page count, preserving returned
values while forcing a code change. It checks that the provider archive hash
changes and the client compiler remains cached. Private overlays and their
build output are removed after each run.

## Complete Debug products

The corrected graph passed all 159 build/check steps, including both products,
CAPI smoke/export checks, CLI and inference ABI smokes, 57 storage-owner tests,
three query-owner tests, and the nine native PDF regressions. The standalone
inference integration executable now composes the media archive when retained
PDF is enabled. This dependency belongs to final composition; adding it to the
inference archive itself would damage independent compiler-cache reuse.

The corrected cancellation code and all eleven Debug cache phases passed.
Reader, renderer, JPEG and reachable provider-code edits preserve the client
compiler; client edits preserve the provider; values, protocol and decoder
control edits rebuild both. GPU scene preparation matches the source path in
Debug too. These are correctness and cache results, not new release-size,
optimized compile-time or peak-memory claims.

Debug archive guards also pass: neither storage nor inference emits the raw
PDF image decoder. The image-decoder inventory remains a separate P1c task,
including fused JPEG preprocessing and retained VP8 state, not just RGBA decode.

## Counted endpoint layout tables

The image/lake integration review also found that a differently sized PDF
supplementary layout table could be read using the consumer's length. PDF
endpoint v3 now carries a checked count and borrowed table pointer. An old
version or either shorter/longer count is rejected before the pointer is read.
The regression uses an unreadable table pointer to enforce that order. The
focused combined-image/lake Debug run passes all 37 steps, including nine native
PDF tests and the image/source equivalence suites
(`pdf-counted-table-debug.log`). Earlier cache receipts remain evidence for their
recorded source versions; this guard change receives fresh functional checks.

The subsequent combined Debug replay passes 225/225 steps, including both
products, and all 13 HTTP regressions against the final installed server
(`media-lake-layout-products-debug-v4.log`, `media-lake-layout-debug-v4-http.log`).
