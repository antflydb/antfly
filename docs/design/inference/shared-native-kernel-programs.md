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

- [x] Route the remaining matching CUDA projection/include trees and Metal
  attention/training trees through the same statistics program. Preserve
  caller-owned publication barriers, bounded planes and explicit RN addition.
- [x] Generate segmented, split, SIMD/MMA and cooperative-matrix online math
  from the common attention expressions: 34 more Metal expressions and three
  CUDA expressions. Reuse the existing CUDA visible-window phase and shared
  weighted-output equation. Native schedule shells are relocated without
  counting that relocation as algorithm reduction.
- [x] Remove the duplicated batched argmax reduction by calling its existing
  shared helper; preserve its tie, empty-lane, suppression and nonfinite rules.
- [x] Split BF16 shared-tree and warp-finish kernels into static entrypoints,
  rather than combining their register allocations in a runtime branch. The
  loader and production launchers consume the new operation-named exports.
  Admit the optimized schedule only on physically qualified GB10; other
  devices retain the original entrypoints and register contract.

### Qualification and retained findings

The common-program tests pass, including the admission contract that preserves
A100/L4 baselines. Ownership verification checks emitted regions/files, actual
shared-source fingerprints and include closure. The direct annotations cover
128 of 651 Tile regions; this is provenance at the annotated region, not a
percentage of serving kernels or a transitive call-graph coverage metric.
Unclassified regions are not evidence of dead code or duplicated algorithms.

CPU qualification includes 67 selected inference tests with real EG2 image,
GLiNER and Whisper fixtures, with no skips. The standalone FFT module separately
passes its independent DFT/tail/silence test; a main-suite filter does not import
that module. FFT paired timing passes all 18 controls/comparisons. Q4-K/Q5-K
header recipes pass independent GGUF decoding tests and all 18 paired timing
comparisons; the isolated header microbenchmark improves approximately 5–14%.
These measurements are not full-model speedup claims.

The follow-up CPU block-dot migration removes seven independent Q4_0/Q4_1,
Q5_0/Q5_1 and Q8_0/Q8_1/Q8_K arithmetic bodies. Their serving adapters now
lower the existing quantization recipe, also used by CUDA/Metal decoding,
through two CPU reduction families. Centered formats retain deferred scaling;
affine formats retain scale/minimum on each product; Q8 retains its vector
accumulator and runtime 256-value loop. Contiguous byte components lower to
vector loads. Explicit Q4_0 unrolling preserves its qualified compiler schedule;
other low-bit formats retain their native loop schedule. The frozen comparison
retains failed scalar-gather and unrolling prototypes. This is common packed
semantics and fewer independently authored arithmetic bodies, not a new
whole-model acceleration or a large total source-size reduction.
Qualification covers 72 selected serving-path tests without skips, independent
GGUF/F64-reference dots at widths 4/8/16, and 63 AA/AB/BA codelet timing gates
on local Apple Silicon. All median gates pass against frozen routines; Q5
codelets improve approximately 7–19%, while Q4/Q8 retain near-baseline latency.
These timings do not qualify physical x86 CPU performance.

The next CPU batch shares five raw K-quant dot bodies and nine prepared integer
staging paths through those same recipes, including removal of the private Q4
value-unpack helper. The Q6×Q8 raw integer-dot routine retains its native
schedule and stored Q8 subblock-sum correction: the shared extraction prototype
was bitwise correct but consistently 2–4% slower, so it was not admitted.
SIMD lowering
uses conservative unsigned bounds to retain byte shifts/masks, and keeps the
native subblock and offset induction variables explicit. Uniform scale/minimum
expressions stay scalar. Q3 retains integer scale-header staging and Q4 retains
paired low/high-half traversal; flattening either schedule regressed the first
prototype. The shared decoder uses aligned byte lowering while Q5_1 retains
its qualified wide-integer decode schedule. These static choices belong to
operation/format schedules, not model adapters.
Qualification passes independent aligned/unaligned recipe and F64-reference
checks, 102 selected inference tests with no skips, and 234 retained AA/AB/BA
timing gates. Nine prepared-block transforms improve 2.5–7.8× locally; several
format decoders improve approximately 5–18%. The five raw K-quant float dots
remain near baseline. Frozen bytes and guard regions match. The Q6×Q8 rejected
prototype is recorded separately and excluded from retained-path totals. These
are local CPU codelet results, not physical x86 or full-model speedup claims.

K-quant prepared headers also share one staged serializer. It derives factors,
integer codes and centering from the existing recipes, preserving Q3 integer
staging, contiguous little-endian F32 planes and Q6's scales-only header. This
removes the five private format writers and the unused Q4/Q5 scratch arguments.
The reference test reconstructs all 256 weights against independent GGUF
outputs, including signed, zero and subnormal scales. Frozen full headers and
prepared values match byte-for-byte. Against the already optimized preceding
batch, local preparation improves approximately 23–26% for Q2_K and 11–16% for
Q6_K; Q3_K/Q4_K/Q5_K retain parity. Q4_K/Q5_K native instructions and operands
are identical after normalizing only compiler basic-block namespaces.

Header timing uses AA, ABBA and BAAB controls with dispatch selected outside
the timer and the same output buffer for both arms. Earlier separate-buffer
runs exposed destination-alignment bias even in AA and unchanged-code cases;
those receipts remain recorded. All 45 final timing gates pass. The baseline
pins the complete preceding recipe module so later lowering changes cannot
silently alter the frozen writer dependency closure. These gains concern
prepared-block construction, not model inference latency.

Local Metal qualification includes both fast and safe math. The matrix and
statistics/sampling matrix passes 338 correctness cases and 846 timing gates.
The expanded audio/frontend/convolution matrix passes 138 cases and 414 gates;
attention/training reductions pass 88 cases and 264 gates; batched argmax passes
40 cases and 120 gates, including tails, ties, all suppression and nonfinite
inputs. Earlier failed prototypes and timing controls remain in the receipts.

Glowworm qualification is explicitly shared-GPU qualification. The combined
production launcher suite passes 66 selected tests with no skips, including
156 projection profiles and mutable-operand graph replays. Physical Racecheck
reports zero hazards for the repaired selected-log-probability, relative-row,
cross-attention, channel-attention and BF16 softmax cases. The CUDA 13.2
production PTX/fatbin/sm89 artifacts are regenerated and preserve all 492
historical exports; two new generic warp-finish exports serve the optimized
schedule. At compute_120, 485 of the historical exports have identical PTX
instructions/register declarations after normalizing comments, whitespace,
private operation-symbol renames and compiler namespaces only.

Two failed prototypes are material to the retained schedules:

- A generic Metal window decomposition regressed latency about 11%. The shared
  program retains Metal's qualified early exit and batched quotient/remainder
  decomposition. Repaired median/p95 gates pass. CUDA pack retains parity and
  reduces register use from 24 to 22.
- Combining narrow and wide BF16 softmax schedules regressed some wide rows
  about 8–18%, despite passing numerical checks. Separate static entrypoints
  restore the original 23/20-register wide-row kernels without spills. All 66
  BF16 frozen/reference cases and 198 median gates pass. Larger narrow grids
  are 17–33% faster on GB10. Physical A100/L4 performance admission is deferred;
  their serving path retains the original shared-tree kernel.

Shared-GPU p95 outliers also occur in AA controls. Those runs establish the
reported median results and numerical/resource checks, not unconditional tail
latency parity. A high-magnitude Metal attention fixture produces the same
F32-versus-F64 error in frozen and migrated kernels; its failed strict-reference
receipt is retained separately from normalized-fixture qualification. No
precision tolerance was widened to make that case pass.

The final attention-expression migration preserves compiler-input bytes after
removing ownership marker comments. The sole batched argmax change replaces a
loop with an existing helper whose loop body is byte-identical; its separate
hardware/reference/timing matrix qualifies that call.

The paired value/index maximum now extends the existing shared-tree owner.
CUDA scalar/suppressed argmax, the MTP token-score reducer and the Metal argmax
helper select the same operation with static load/spelling bindings. The scan
seed, suppression, invalid-index guards and final publication remain caller
policies. In particular, a NaN is not assigned a new total ordering and exact
ties retain the smaller U32 index. Caller scratch publication and every stage
barrier retain their native placement; explicit MTP lhs snapshots remain.

The 51 common-program tests include independent finite selection and explicit
NaN/infinity/signed-zero cases. The complete Metal source and CUDA scalar fold
are byte-identical after removing ownership markers. The generated CUDA module
differs only by a blank line; its C/C++ token stream, including string literals,
is identical. Local Metal passes 40 correctness cases and 120 timing gates in
fast/safe math. This phase establishes common algorithm ownership; generated
copies and source relocation earn no source-size reduction or speedup credit.
An isolated Glowworm checkout passes CUDA 13.2 `--check --all`: PTX, compressed
fatbin and SM89 cubin hashes match the committed artifacts exactly. The temporary
checkout and its build cache are removed after saving the receipts.

### Retained schedules and migration boundary

The common algorithms listed in the six batches now have production consumers,
shared source owners, admission/precision contracts and affected-path evidence:

- [x] Row/statistics phases, normalization adapters, scratch publication and
  sampling insertion/random-stream phases.
- [x] Dense row/layout transforms, views, first match, transpose, windows and
  page reconstruction.
- [x] Ordered dots, shared projection trees, Metal register tiles, compatible
  quantized staging/traversal and CPU packed-header recipes.
- [x] Visibility, relative products, online state/rescaling and compatible
  attention reduction phases, including split/cooperative-matrix callers.
- [x] FFT, sparse mel/frontend phases and compatible direct/tiled convolution
  and matrix-stencil addressing.
- [x] Generated launch constants, ownership/freshness checks, operation naming
  guards and removal of the replaced template/CPU helpers and duplicate bodies.

This closes the listed common-program migration, not the existence of native
code. Warp/SIMD/Welford schedules, warp-index/top-k and radix reductions,
format-specific SIMD/DP4A/MMA traversal, tensor-core fragments, vendor GEMM/FFT,
and distinct training replay/rounding algorithms remain native specializations.
Some already shared one implementation before this work. Their retained source
must be counted, and renames/generated copies earn no reduction credit.

There is no claim here that every native instruction is generated from the
portable conformance kernels, that total maintained code fell 20%, that each
top-tier model has a new end-to-end benchmark, or that every model is faster
than an external runtime. Those require separate measurements and, where
appropriate, further compiler/schedule work rather than relabeling coverage.

### Retained-family consolidation batch

This audit covers the retained families together. Compatible phases share their
existing owners, with implementation batched before qualification:

- Scalar shuffle folds use `native_statistics.CudaShuffle`. XOR versus down,
  native typed maximum versus `fmaxf`, and explicit RN addition remain policies.
  Partial fragment folds with ascending or interleaved schedules stay distinct.
- Ranked top-k insertion is shared by the Metal per-thread scan, tree merge and
  final merge. Its positive ranked predicate rejects NaNs and breaks ties by ID;
  legacy sampling retains its different NaN admission and encounter-order ties.
  Serial/SIMD radix histograms share four-bit prefix geometry while retaining
  their original accumulation orders.
- Q4×Q8 contributions use one static DP4A traversal. Signed centering and GGML
  unsigned nibble correction remain separate numerical profiles. The unused
  duplicate aligned-word source fragment is removed.
- WMMA score products share a live accumulator traversal with the complete
  dot/store path. Preloaded A fragments and persistent P/V accumulators retain
  their distinct acquisition, rescaling and synchronization schedules.
- CUDA and Metal AdamW moments/update use existing FExpr expressions. Callers
  retain their sqrt dialect, bias correction, stores, gradient clearing, alias
  handling and optional nonfinite flag. CPU optimizer variants with a variance
  clamp or different decay association are deliberately separate.

The PyTorch-derived Welford training normalization is a single licensed,
ordered implementation; there is no duplicate Welford algorithm to remove.
The quantized HMMA family already routes wrappers into one static tile. Keep
both rather than add forwarding owners or substitute a different algorithm.
This batch adds no source file or parallel compiler. Generated copies and new
provenance annotations do not count as maintained-source reduction.

Qualification: 52 core tests and seven ownership tests pass; both generators
pass freshness checks, and the ownership verifier accepts 857 emitted items
across 64 canonical native files. Sixty local Metal cases pass against frozen
kernels and independent sorted/optimizer references in default and safe math.
The full Metal shader has identical compiler tokens to the frozen baseline.
CUDA 13.2 regeneration on Glowworm produces byte-identical portable PTX,
seven-architecture fatbin and SM89 cubin. The final marker-newline repair has
identical compiler tokens across the complete 49-file CUDA input closure.
These identity checks preserve instruction/resource/performance parity; this
consolidation does not claim a binary-size reduction or a new speedup. The
isolated CUDA qualification checkout/cache and local probe binary are removed.

### Quantized acquisition and prefetch follow-up

The Q6×Q8 five-warp, eight-column partial tile now serves ordinary and L2
prefetch callers. Its optional compile-time prefetch phase preserves the same
addresses and issue points; dot order, registers, partials and barriers retain
their native schedule. The paired/down prefetch paths reuse the existing Q8
half-block scale/word acquisition. Their local bindings are read-only.

The existing Q4×Q8 source factory also owns the byte-pair variant's four DP4A
contributions, alongside centered aligned words and GGML unsigned correction.
Its original helper declaration point, packing dependencies, arithmetic order
and compatibility symbol are retained. This adds one generated region under
the existing quantization owner, with no new source file or compiler. Helper
relocation and generated copies earn no source-size reduction credit.

The maintained prefetch program plus both shared acquisition/tile headers fall
from 12,673 to 9,839 bytes (22.4%), and from 288 to 241 lines. These counts include
all three owners and exclude their generated copies. This is a family-specific
reduction, not a whole-codebase percentage. Reused headers are named for Q4/Q8
half-block loads and Q6/Q8 tile partials; the JIT dependency manifest and cache
identity tests use those paths. Historical model-prefixed device exports remain
ABI adapters for their existing production launchers.

Qualification: both generators pass freshness checks; ownership verification
accepts 858 emitted items across 64 canonical native files. The 52 unchanged
core tests remain cached/passing, seven ownership tests pass, and the production
cache-construction fixture passes its enabled Metal/CUDA identity checks
with the renamed headers. CUDA 13.2 `--check --all` on Glowworm produces the
same portable PTX, seven-architecture fatbin and SM89 cubin bytes as the frozen
baseline. Consequently kernel instructions/resources remain unchanged; no new
speedup or whole-codebase reduction is claimed. CPU/Metal numerical programs
are unchanged in this follow-up. The private CUDA checkout/cache is removed,
and the cache fixture removes its temporary build tree.
