# BB: operation-based native program owners

This wave merges `origin/main` through `e5aad86397` (merge baseline
`3d9aa31e6a42ec310bd836ff808465e91e900fa2`) and implements the six opportunities
in the next-family audit. It improves reuse and authoring boundaries; the
additional net source reduction is small. Renames are not counted as deletions.

## Implemented families

- Metal MPP split projections share a static plane/row program. Float and finite
  BF16 MPP gate sources share a source fragment while retaining their original
  MMA geometry, storage, split reduction and epilogues.
- Segment Flash backward shares the score/adjoint MMA phase and cell VJP.
  Barrier sites, dQ versus dK/dV direction, masks, replay and dropout remain
  schedule-specific. The deterministic CPU reference is now
  `range_attention_reference.zig`; the existing dropout-aware segment module
  remains a separate contract. `ModernBertTrainingAttentionAttrs` remains a
  compatibility alias for `RangeTrainingAttentionAttrs`.
- CUDA F16 normalization joins the shared row family with an explicit fixed-256
  warp-finish trait. BF16 RMS retains its original tree association.
- Rotary families share pair/axis indexing and component publication. CUDA
  training retains independently BF16-rounded products, frequency evaluation
  remains native, and Metal retains its two upper-component operand orders.
- CUDA and Metal causal depthwise schedules share one compile-time stencil
  owner, including distinct weight order, history, segment start, activation and
  separate-product policies. The spatial 3x3 residual keeps its own traversal.
  Fused/vector recurrence schedules are retained.
- CPU dense promotion consumes typed workload, shape and quant-layout metadata.
  Checkpoint-name classification lives in a model adapter and is registered on
  storage. Explicit metadata overrides legacy names; existing cache budgets,
  ownership, fallback and prepared-panel admission boundaries remain intact.

Eleven CUDA source owners, shared Metal source factories, reusable entrypoints
and private helpers now use operation-based names. Legacy image paths and
remaining historical exports are compatibility adapters. The
[kernel authoring guide](kernel-authoring.md) and `src/ops/AGENTS.md` tell future
implementers where to extend each family and require new reusable names to
express operation, layout, precision and schedule rather than a model.

## Qualification

Raw receipts, frozen originals, binaries and disassembly are retained locally
under `.benchmark-assets/1166/bb-campaign/`. The baseline Metal header freezes
all original global source macros as well as its factories, preventing candidate
macros from silently changing the reference.

On an Apple M4 Max:

- The final new-program suite passes 90 original-bitwise/independent-reference
  cases, including MPP row tails and split shapes, Flash forward/backward,
  local/disjoint masks, empty rows, seeded dropout and causal segment histories.
  Two near-cancellation tests pass under fast and safe math, checking separate
  product rounding in the portable stencil.
- All 246 AA/AB/BA gates in the projection/attention assay pass. Final emitted
  source is identical to the measured version for these programs after the
  private helper rename; the main shader suffix is also identical. The final
  stencil prefix is separately qualified.
- The final stencil assay passes 23/24 gates. The miss is fast-math shape 1,
  BA: median 1.01157, p95 1.11112 against a 1.10 p95 limit. Retain this miss;
  it does not support a universal tail-latency parity claim.
- The full production OCR projection/recurrence/dispatch oracle passes, as do
  all 108 encoder-family cases.
- The final CPU/compiler suite passes 167/169 selected tests, with two expected
  backend-dependent skips. A Metal-enabled integration suite separately checks
  the production range/segment attention linkage (40/40 pass).

On Glowworm's GB10, with another checkout using the GPU:

- Native and actual shipped-PTX prototype suites each pass 103 original-bitwise
  and independent-reference cases. Core audio passes 48, packed heads 14, and
  cached/uniform/ragged GDN convolution schedules 36 cases with history and
  guard checks across both production head geometries.
- All measured medians pass the 1.05 gate. Tail gates pass 89/90 native,
  87/90 shipped, 46/48 audio, 24/24 packed-head and 61/72 GDN comparisons.
  GDN includes four AA tail misses. These shared-GPU tails are inconclusive;
  they are retained rather than repeatedly sampled until passing. This is not
  an isolated-GPU or full-model performance claim.
- Native SM121 Florence, activation and vision programs have identical
  instructions and resources after exact symbol normalization. Recurrent
  programs retain 16/23 identical instruction streams; seven convolution
  streams change. Batch convolution uses 26 registers instead of 24; prefix
  convolution uses 28 instead of 30. Stack, local and shared storage are
  unchanged. Core changes are limited to packed rotary heads and audio causal
  convolution, with unchanged native resource counts.
- Final compact portable-source edits preserve the measured candidate's full
  native instruction/resource dumps and prototype PTX. Canonical artifacts are
  regenerated using the pinned CUDA 13.2 contract; driver-compatible prototype
  PTX uses CUDA 13.0 with its existing arithmetic flags.

The relocated CUDA oracle generators now preserve their local include closure.
The activation oracle additionally carries its real RMS template and ABI
instead of relying on a previously standalone wrapper. Include-closure and
source-accounting regression suites pass (two and nine tests respectively).
The backend identity cache regression passes for both Metal and CUDA; its
independent dependency lists now include current owners and shared headers.

## Source accounting and limits

The receipt counts all new production owners, adapters, contracts and build
identity, with rename detection disabled so both sides of every move cancel.
The portable GPU owner is counted once. Tests, documentation, generated source
and image bytes are reported separately.

The authored Metal shader removes 3,721 bytes; authored CUDA adds 492 bytes and
the portable device owner adds 2,488 bytes: **741 fewer maintained device
bytes** across these owners. CPU/ownership removes another 4,112 bytes, while
new contracts, metadata and source identity consume most of that saving. The
wave's total authored production reduction is **603 bytes / 22 lines**.
Compiled images add 2,394 bytes in aggregate. It is
not another 20% reduction. Qualification tooling grows to cover the newly
shared numerical contracts.

The value here is removing model ownership from reusable programs and making
future models extend established static families. Substantial further deletion
requires retiring additional complete schedules or moving larger truly
identical phases into existing families; renaming the remaining compatibility
exports alone would not accomplish that. Tuned MMA, vector and fused-state
paths remain available, and no end-to-end inference speedup is claimed.
