# BG: prefix, pooling and training-row programs

Baseline: `ea3c003426`. This batch implements the six remaining audited family
opportunities. It generalizes the numerical owners and launch geometry while
retaining the admitted schedules. It is **not a net source-reduction win**:
including the complete new API and portable owners adds 6,987 production bytes
and 144 lines. The complete Metal translation unit shrinks by 49 bytes after
charging its new source-phase definitions. Moving the prefix body between files
and the CPU rename receive no reduction credit.
Tests, documentation and generated images are accounted separately.

## Owners and retained contracts

- `cuda/prefix_normalization.zig` and `kernels/prefix_normalization_program.cuh`
  replace implicit 768-wide input geometry with explicit rows, prefix width and
  input stride. New generic forward and accumulated-VJP exports accept arbitrary
  admitted strides. The 768-column specialization remains instruction-identical
  and keeps its 256-thread launch and original dynamic-shared reservations.
  The model adapter supplies stride 768 and retains its model admission bounds;
  compatibility launch methods remain available. The reusable program borrows
  its caller's module and uses the existing stack launch-ABI helper.
- Prefix normalization retains four independent accumulators, the row/width
  dependent lane plan, explicit FMA versus multiply/add boundaries, the 1e-12
  clamp, distinct forward/backward divisions and objective scaling. The VJP
  accumulates into existing gradients and leaves the input-stride suffix alone.
  Geometry validates nonempty extents, width <= stride, safe iteration bounds
  and checked byte multiplication. Weight must be finite.
- `ANTFLY_CUDA_TRAINING_FOLD` in `row_program.cuh` owns the block-x tree and warp
  fold shared by training RMSNorm forward and input VJP. Caller-owned block-y
  slices, scalar liveness, plane interleaving, barriers and BF16 cast nodes stay
  intact. An array-based prototype changed VJP lowering and was replaced by the
  native source phase; it was not substituted for inference RMSNorm arithmetic.
- `ANTFLY_MSL_Q8_*` owns staged K/V loop control, SIMD score products and weighted
  accumulation for fixed-256 and token-major tail attention. Layout addressing,
  score capacity, masks, tile tails, softmax and zero-denominator publication
  stay as distinct policies. Both emitted kernel bodies are byte-identical to
  the frozen original despite their shared maintained phases.
- `gpu/masked_pool_program.h` owns masked column traversal on CUDA and Metal.
  Mask optionality, counting, coordinate/address width and per-term division
  are static policies. CUDA BF16 mean/L2, Metal F32 mean/L2 and the legacy Metal
  per-term-divided row mean share it. Inference reciprocal/count placement,
  normalization and empty-row behavior stay with their original row programs.
  Training mean/L2 retains its different four-accumulator and BF16 graph rules.
- Segmented audio convolution now instantiates `gpu/depthwise_program.h` with
  token-major weights, no history/activation, fused accumulation, unsigned
  coordinates and wide addresses. No-history acquisition eliminates the history
  branch statically. Other stencil specializations retain their contracts.
- CPU `WideSegmentAttentionGemm` names the physical 512-query BLAS schedule.
  Model admission and key-tile selection remain in the adapter. This is naming
  cleanup; it does not claim new CPU arithmetic or a speedup.

The native include closure is registered in both backend cache identities and
the independent cache regression fixture. The production artifact export list
now requires both generic prefix symbols. Historical exported model symbols
remain compatibility entrypoints rather than naming examples.

## Native program preservation

The corrected complete-function comparator verifies all **451 existing SM121
functions** and **3,157 existing shipping functions** across SM75/80/89/90/100/
110/120. Every existing function preserves register, stack, local and shared
storage. Only mean/L2 pooling changes instructions: the other 450 native and
3,150 shipping programs are instruction-identical. The two generic prefix
exports are additional programs, not replacements counted as old identities.
Other architectures have compilation/resource evidence; only GB10 has CUDA
hardware qualification. The complete PTX, fatbin and SM89 images are regenerated.

## Qualification and rejected prototypes

The original CUDA include closure and full Metal shader were frozen before
editing. CUDA uses pinned NVCC 13.2.86 on Glowworm's GB10. Metal uses the local
M4 Max with default and safe math modes. References run independently in F64;
mutable buffers are reset before each side, with nonzero output state, suffix
checks and trailing canaries.

- CUDA passes 90 original/reference cases: 42 prefix cases, 36 training RMSNorm
  cases, eight pooling cases and four segmented convolution cases. Prefixes
  cover strides 17/257/768/1152, width tails through 1025, 513 rows, zero and
  below-clamp norms, forward/VJP and nonzero initial gradients. Legacy-stride
  cases also compare the generic export with the frozen 768 specialization;
  arbitrary strides have independent F64 references. RMSNorm covers
  optional weights, widths 64/768/1152 and block-y heights 1/2/4. Pooling covers
  partial/empty masks and dimensions through 1025. Segmented convolution covers
  segment boundaries, row/column tails and kernel widths 1/3/5/7.
- Metal passes 40 Q8/pooling cases plus 92 existing program-family checks,
  including segmented stencils and explicit separate-product cancellation.
  Q8 includes both layouts at sequence 256 and masked/tail sequences 8 through
  128. Pooling includes empty documents, width tails, optional normalization
  and per-term-divided rows. Q8 source identity and native pipeline resources
  are checked against the frozen shader.
- Metal's final fixed AA/AB/BA assay passes all 12 gates (31 pairs of 32
  dispatches, median <=1.05 and p95 <=1.10). Candidate Q8 medians are
  0.99881–1.00130; pooling medians are 0.97463–0.97708.
- CUDA's final fixed stream/graph assay passes pooling and convolution gates.
  Overall it passes 22/24 gates; an unchanged RMSNorm stream tail and an RMSNorm
  graph AA control miss the p95 bound. Native RMSNorm code is identical, and its
  earlier unchanged-program assay passed. This shared-GPU run does not establish
  isolated tail-latency parity, and misses are retained without resampling.
- The actual shipping fatbin passes another 90 correctness/reference cases on
  Glowworm. Its sole changed instruction stream, pooling, passes all six fixed
  stream/graph AA/AB/BA gates. Candidate median ratios are 0.99923–1.00068.
  This separately qualifies the shipped SM120 image rather than only a native
  SM121 development cubin; no other family was resampled to clear a miss.
- The first no-history convolution prototype retained a redundant conditional:
  graph median ratios 1.05860/1.06385 missed. Static no-history acquisition
  restores the original convolution instructions and passes its final gates.
- A flattened wide pooling iterator passed GB10 timings but increased registers
  on several shipping architectures. Separating unsigned token coordinates
  from wide addresses replaced that prototype. Its original images, resource
  reports and samples remain in the campaign evidence.

The native-report comparator verifies matching nonempty instruction/resource
key sets and architecture-qualified function records. Four regression tests
cover fatbin and native-cubin formats, including rejection of empty/malformed
reports. An initial ad hoc resource parser missed the actual function-label
format; only the corrected complete-function reports are qualification evidence.

CPU adaptive segment attention and prefix geometry tests pass in the CUDA-enabled
Zig suite. The backend cache-identity regression passes for both backends; nine
source-accounting tests and the generated-source check (70 files, 129 owned
regions) pass. New Python tools pass Ruff. An initial broad-suite invocation used
an ineffective filter and hit sandbox network/GPU restrictions; it was stopped
and is not counted as validation. The existing runtime-cache test file also has
five pre-existing lint findings outside the changed dependency lists.

Evidence lives under `.benchmark-assets/1166/bg-campaign/`: frozen sources,
prototype/final images and native reports, raw paired samples, independent
reference results, cache/build logs and full source accounting. Artifact builds
use the three exact canonical production profiles. Glowworm's old checkout has
an unrelated stale generator; the current local generator check passes, and
recorded direct NVCC commands reproduce the production profiles remotely.
