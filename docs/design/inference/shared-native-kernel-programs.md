# Shared native kernel programs

## Objective

Reduce independently maintained CPU, CUDA and Metal algorithms without losing
the precision contracts or schedules that make the existing kernels fast.
Generated file coverage is not the completion metric: emitting a handwritten
body from a registry changes ownership, but does not share its algorithm.

The current branch already supplies graph capture, backend partitioning, tile
descriptions, expression trees, native schedule families, buffer planning and
command planning. Extend those owners; do not introduce a second graph IR,
runtime interpreter, scheduler or kernel catalog.

## Architecture

```text
model adapter: checkpoint/configuration -> operation and physical layout
shared operation: indexing, masks, arithmetic and publication contracts
shared program: loads, ordered phases, reductions, stores
static schedule: tile/vector sizes, staging, reduction tree, fusion
backend lowering: CPU SIMD / CUDA warp+MMA / Metal SIMD-group matrix
artifact: kernel + ABI + scratch + launch requirements + provenance
```

Operation semantics and scheduling are distinct. Common semantics do not imply
that a CUDA warp reduction should become a Metal shared-memory reduction.
Keep independently qualified schedules as explicit alternatives. Backend
intrinsics, vendor libraries and genuinely different hardware algorithms remain
backend-specific. Do not force them through a slower portable implementation.

Use existing `fexpr`/`native_math` for arithmetic, `layout` for addressing,
`norm`/`rows` for statistics, and `attention`/`gemm` for composed operations.
Extend the smallest existing vocabulary that can express a production family.
Source spelling and historical device names belong to compatibility adapters.

Every program must specify storage, multiplication, accumulation and output
types; explicit rounding points; arithmetic association and FMA; math function
profile; reduction tree; mask, empty and nonfinite behavior; index width; alias
rules; and synchronization/scratch ownership. A BF16 store is a numerical
boundary even when the next consumer widens it to F32.

Generate buffer bindings, parameter layout, launch geometry and scratch needs
from a specialization where practical. Preserve existing ABIs during migration;
do not combine an algorithm migration with a serving ABI redesign. Existing
launchers may remain compatibility adapters until their consumers are migrated.

PJRT is a separate boundary: shared Metal generation does not implement the
PJRT provider ABI or import HLO/StableHLO. A future provider should reuse these
programs and execution plans, rather than owning new kernels.

## Migration batches

Each batch includes production dispatch, deletion of replaced bodies and
qualification. A new generator with only an offline caller is incomplete.

1. **Row statistics and epilogues.** Share serial/tree statistics, softmax,
   normalization, rotary and fused arithmetic using explicit numerical policies.
   Preserve centered two-pass, raw moments, Welford, RMS, warp-finish and SIMD
   schedules separately. Begin with matching shared-tree stages and existing
   row/layout descriptions; do not rewrite all normalization as one kernel.
2. **Layout and storage.** Share gather, slice, repeat, concatenation, transpose,
   sequence transforms, padding and KV address calculations. Keep invalid-index,
   signed-index, alias and storage-conversion policies explicit. Use existing
   validated views instead of adding a parallel indexing language.
3. **Projection.** Share tile traversal and quantization/epilogue composition
   across dense, quantized, paired and routed projections. Keep MMA fragment
   layouts, split-K, prefetch, register ownership and vendor GEMM admission in
   their schedules. Preserve operand strides and BF16 publication boundaries.
4. **Attention.** Share visibility, score modifications, online-softmax state
   updates, rescaling and output publication across dense/causal/local/relative/
   cross/paged attention. Retain native decode/prefill and head-width schedules,
   probability storage types, empty-tile behavior and KV formats. Migrate phases
   before whole bodies; never substitute the portable attention prototype for a
   qualified tensor-core schedule merely to improve generated coverage.
5. **Audio, convolution and spatial transforms.** Share FFT/log-mel stages,
   windowing, convolution indexing and spatial pooling. Preserve traversal,
   separate-product/FMA policies, nonfinite behavior and native tiled/MMA paths.
6. **Finish integration.** Generate remaining launch contracts, remove obsolete
   compatibility code, update the existing ownership inventory to distinguish
   shared algorithms from native source packaging, and prevent new model-named
   numerical owners. Keep independent references and necessary hardware tools.

## Qualification and acceptance

- Freeze complete native compiler inputs before migration. Byte-identical
  compiler inputs establish that a source-only consolidation preserves the
  existing schedule; they do not establish a new hardware performance win.
- Where emitted code changes, compare against frozen originals and independent
  references. Exercise tails, empty/all-masked rows, NaNs/infinities, duplicate
  and invalid indices, cancellation and BF16 rounding boundaries as applicable.
- Batch related implementation changes before compiling. Run focused shared
  tests, source freshness and ownership checks, then hardware correctness and
  warmed performance comparisons. Rebuild again only for a new change or failure.
- Qualify CPU and local Metal; use Glowworm for CUDA, labeling shared-GPU runs.
  Do not claim L4/A100 qualification without those devices.
- Cover top-tier Whisper, EmbeddingGemma 2, LightOnOCR, Gemma 4, GLiNER 2.5 and
  decide paths as affected, plus Qwen3 architecture regression cases. Reuse
  existing shared model directories. Task quality is separate from numerical
  smoke tests; vendor reduction differences do not justify arbitrary tolerance
  widening.
- Record source provenance, arithmetic profile, shape, warmup/sample counts,
  latency distribution, memory and native resource changes. Investigate
  regressions rather than hiding them in an average across unrelated shapes.
- Count all maintained program, renderer, specialization and adapter code.
  Generated copies and relocated strings earn no algorithm-reduction credit.
  Delete a former owner only after tracing its real production callers.
- Use one canonical output prefix and worktree-local caches. Retain compact
  receipts, avoid duplicate models/binaries, and garbage-collect safely.

## Implementation ledger

Update this section only with implemented and checked facts. A batch is not
complete merely because a shared primitive exists.

- [x] Introduce shared dense-row slice/repeat/take/concat/first-match programs;
  wire eight existing CUDA/Metal exports and remove their independent bodies.
- [x] Move CPU first-word extraction to the same operation owner, preserving its
  token-major scan and reducing seen-state storage from F32 to U8.
- [x] Generate CUDA interleaved shared-tree statistics and Metal RMS/raw-moment
  tree stages from one ordered tree program.
- [x] Check CPU semantics and malformed geometry; compare the initial migration
  against frozen originals on local Metal and Glowworm CUDA. Hardware results
  apply to the migrated families, not all models or all remaining schedules.
- [x] Share ordered dense projection and scalar attention dots, Metal
  F32/F16/BF16 projection helpers, and CUDA nested matrix-reduction planes.
- [x] Share CPU/CUDA/Metal GRU gate/candidate/blend arithmetic and remove the
  former CPU sigmoid helper and separate native cell bodies.
- [x] Share causal-window rules and online-attention expressions while retaining
  each schedule's maximum, empty-row, probability-store and rescale policies.
- [x] Share page-table reconstruction with explicit block-number versus
  token-base entries, checked U64 versus wrapping U32 addressing, and legacy
  contiguous-page behavior.
- [x] Share tiled transpose and padded window pack/unpack. Generate transpose
  launch constants consumed by the actual CUDA and Objective-C launchers.
- [x] Share direct 1D/2D convolution as one spatial program, specializing the
  unit-width axis, grouping, optional bias and signed/unsigned bounds policies.
- [x] Share CPU SIMD FFT complex products and Metal plain/separate-product FFT
  butterfly stages. Native CUDA fixed-size FFT/log-mel stages also use these
  programs; CUDA vendor FFT remains a separate schedule where selected.
- [x] Extend the existing family inventory with per-export shared-program
  provenance, numerical/schedule contracts and verified source fingerprints.
  Unannotated native regions remain explicitly unclassified.

- [x] Replace four nucleus reduction trees and row softmax/reduction trees with
  shared ordered statistics, including conditional scratch-plane admission.
- [x] Share bounded stable top-k insertion and the exact per-token Gumbel
  random stream without changing candidate, tie or penalty rules.
- [x] Share relative-score products with explicit combined-expression and
  per-product accumulation profiles; retain native shuffle/MMA schedules.
- [x] Share Q4-K packed-subblock staging and output-row traversal across paired,
  activated, F16-publication and single-weight variants. Emitted native tokens
  for the complete Q4-K paired region remain unchanged against the frozen input.
- [x] Remove the obsolete inference template compatibility file; production
  rendering and its tests now import the existing shared tile owner directly.

- [x] Generate relative-row and FFT512 geometry from the shared operation
  descriptions; consume them in production CUDA/Objective-C launchers and the
  CPU FFT implementation. Keep the fixed qualified geometries intact.
- [x] Route production CPU Q4-K/Q5-K scale/min unpacking through the same packed
  recipes as CUDA and Metal. Header views are twelve bytes/eight subblocks;
  integer evaluation is statically unrolled and avoids a float round trip.
  Delete the former two CPU bit-layout implementations.

- [x] Consolidate the remaining shared-tree consumers in masked pooling,
  embedding normalization, per-head quantization, embedding moments and paired
  projections. Keep warp-shuffle and candidate/index trees separate schedules.
- [x] Make shared-scratch broadcast own its post-read barrier. A physical CUDA
  race audit exposed the former selected-log-probability race; migrate both it
  and the matching relative-score softmax to the repaired publication phase.
- [x] Remove the final model-named native family owner by folding the historical
  local-attention export into the existing full/local block schedule. Extend
  the naming guard to local-action, relative-encoder and embedding model names.

- [x] Replace four dense Metal row/column kernel bodies with one static
  register-tile program. Preserve 1x8 and 4x4/8/16 geometry and scratch, and
  compose its products and tree stages from the existing shared owners.
- [x] Audit the canonical CUDA include leaves as authored kernel code too.
  Native FFT512/log-mel consumers in the multimodal header are production
  paths, distinct from vendor FFT; generate them from `native_audio`.
- [x] Share sparse mel projection and the scalar 16x16 3x3/stride-two convolution
  schedule across CUDA and Metal. Keep CUDA explicit FMA/full padding and Metal
  native contraction/valid-tail policies, index widths and bias order explicit.
  Matrix convolution reuses stencil coordinates while retaining MMA staging.

### Qualification and retained findings

The expanded batch passed 67 selected inference tests with real EG2 image,
GLiNER and Whisper fixtures and no skips. The standalone architecture FFT
module has a separate qualification target; a main-suite filter does not include
that module. Separate CPU GRU tests preserve exact
results; paired timing controls cover widths 1, 33, 768 and 2560. Local Metal
qualification covers both fast and safe math, F32/F16/BF16 projections, direct
convolution, FFT/log-mel, GRU, transpose and windows. Glowworm qualification uses
frozen-versus-generated CUDA kernels and AA/AB/BA shared-GPU controls; a full
CUDA-enabled host test binary also compiles successfully.

An initial generic Metal window decomposition regressed latency about 11%.
Restore the qualified early exit and batched quotient/remainder decomposition
as a static schedule within the common program. This repaired schedule passes
the paired median/p95 gates. Keep the failed prototype results in qualification
receipts; do not replace its schedule with the CUDA decomposition. The CUDA
pack schedule retains parity and reduces register use from 24 to 22.

These are operation and affected-path qualifications, not a claim that every
top-tier model has a new full-model performance result or that all remaining
native algorithms have been migrated. The inventory currently annotates 37 of
599 tile regions; unclassified does not mean dead code or prove duplication.
Large retained owners still requiring audit include split attention, nucleus
sampling and K-quant programs. Some already share native specialization
templates, which must not be counted as new cross-backend algorithm sharing.

- [ ] Row/statistics batch fully migrated and qualified.
- [ ] Layout/storage batch fully migrated and qualified.
- [ ] Projection batch fully migrated and qualified.
- [ ] Attention batch fully migrated and qualified.
- [ ] Audio/convolution batch fully migrated and qualified.
- [ ] Launch contracts, ownership inventory and dead-code cleanup complete.

The existing native source families remain valid production schedules throughout
the migration. Partial progress must identify the exact migrated operations and
remaining bodies rather than describe all kernels as generated from shared IR.
