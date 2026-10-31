# Shared recurrent, audio, projection and training row programs

This wave implements all four remaining model-family opportunities listed after
AZ (`bdd41672f4`): Metal recurrent phases, audio relative-window attention,
BF16 projections, and model-neutral CUDA training row APIs. It keeps the tuned
state-update and dot-product schedules. It does not claim a 20% reduction across
the kernel tree or a full-model speedup.

## Production shape

- `metal_gdn_source.h` owns convolution/history advancement, stable gate
  preparation, Q/K normalization, gated output normalization, and fused
  preparation for both recurrent libraries. Scalar/SIMD step, single-token versus
  batched indexing, and register versus shared V storage are static choices.
  Six native ABI declarations share two emitters. The scalar decode, column
  recurrence and 512-thread batched recurrence retain their state-update loops,
  scratch geometry, ordered reductions and barriers.
- Audio attention shares window construction, key admission, bounded logit cap,
  threadgroup softmax and V gathering. Scalar accumulation, threadgroup-per-key
  reduction and eight-SIMD-group key scheduling remain separate. Scalar softmax
  still counts admitted keys; parallel softmax still recognizes the invalid
  sentinel. Gather keeps its original masking contract. Packed QKV strides and
  per-plane clamps are preserved.
- `antfly_bf16_gemv_vec4<Columns, Planes>` owns component-wise vec4 accumulation
  for one/two output columns and one/two projection planes. Bias and SiLU-product
  epilogues remain at their original reduction sites. This is distinct from the
  existing input-batch-row projection template.
- CUDA training source owners are `training_welford_layer_norm.cuh` and
  `training_row_softmax.cuh`. Training launch APIs identify Welford and persistent
  row contracts; LayerNorm phases use an enum. Softmax/log-softmax share their
  static dispatcher. Legacy exported device names remain stable. Welford, pinned
  CUDA 12.8 arithmetic, separately rounded backward inputs and inference
  statistics remain distinct contracts. Original license notices are retained.
- Build/JIT identity includes the new Metal owner and renamed CUDA owners. CPU
  projection families and CUDA cross-model recurrence were already shared; they
  receive no new reduction credit here.

## Validation and performance

On the Apple M4 Max, 108 frozen-original/candidate cases pass bitwise parity in
fast and safe math modes. BF16 and audio have independent CPU references
(`2e-4 * (1 + abs(reference))`) and output guards. The existing complete OCR Metal
oracle suite passes, including fused and batched recurrence, long prefill,
continuation/reset, producer/consumer behavior and canaries.

The final Metal campaign passes **324/324** one-sided timing gates: 31 paired
samples of 32 dispatches, AA/AB/BA, median ratio <=1.05 and p95 <=1.10. Immutable
inputs use the same allocations for both pipelines; mutable outputs/state have
separate allocations and reset outside GPU timing. This is kernel GPU timing,
not model throughput or a symmetric AA stability bound. After consolidating the
ABI declarations, both emitted MSL programs match the timed programs byte for
byte.

On Glowworm (GB10), **243** CUDA cases pass original-bitwise parity and independent
references, including forward/backward width boundaries, log-softmax width 2048,
parameter-gradient partition boundaries, canaries and mutable softmax graph
replay. The reference bound is `2e-5 * (1 + abs(reference))`. All four row kernels
have identical native instructions and resource reports on SM75, 80, 89, 90,
100, 110, 120 and native SM121. All canonical CUDA 13.2 images and the pinned
NVRTC 12.8.93 training PTX regenerate byte-identically; the training manifest's
source hash changes.

The final shared-GPU campaign passes **88/90** stream/graph timing gates. The two
misses remain in the ledger: softmax/129/1024 forward graph AA p95 1.1960, and
Welford/129/1024 input-gradient stream BA p95 1.5631 (median 1.0021). Another
checkout's training job was using the GPU. Identical instructions/resources and
unchanged shipped images establish code preservation; this run does not establish
an isolated CUDA tail-latency improvement.

Compiler/renderer tests: 160 selected tests pass. Codegen checks 70 generated
files and 129 native owned regions. Source-accounting and training-artifact
manifest tests pass. Production resident normalization and softmax tests pass their pinned
forward/backward hashes through the generic APIs: 40 selected tests pass,
including 38 module-registration tests. The first remote build needed the current
generated Cloudflare API dependency overlaid onto its old imported snapshot.

## Prototypes and assay corrections retained

The first generic prepare added division/remainder to single-token decode and
failed timing consistently. A static batched-indexing trait fixes that; its
72 prepare gates pass. An initial BF16 helper returned all reduced planes before
publication; the final helper publishes through its original static epilogue.

The early Metal assay duplicated large immutable weight allocations and produced
large opposite AB/BA ratios. Those measurements remain as inconclusive evidence;
the final assay shares read-only allocations. The early fused-decode p95 miss
also remains in its original log. The larger shared fused-preparation program
subsequently passes both recurrent schedule matrices.

The first shared CUDA dispatcher eliminated the unreachable backward/width-2048
log-softmax body. This changed whole-kernel allocation/scheduling and slowed
width-128 graph execution by about 11%. Restoring the original static case and
admission spelling makes all four programs instruction/resource-identical across
targets. The failed prototype and its timings remain available.

## Source ledger and reproducibility

Against `bdd41672f4`, charge every new owner, ABI emitter, launch contract and
identity declaration. `scripts/kernel_source_accounting.py` counts the entire
new Metal header as maintained device source, once, with a regression check.
The ledger separates tests/docs, generated source, host code and compiled images.
Header renames receive no deletion credit.

The final source receipt records approximately **3.95 KB / 56 lines less maintained
Metal shader source** and **3.18 KB / 64 lines less net authored production code**.
CUDA API/owner generalization adds a small amount of source while preserving
images. These are maintenance reductions, not compiled-instruction reductions.
The recurrent-plus-audio cohort shrinks by about 10%, including the complete new
shared owner; this wave does not reach its proposed 20% cohort target.

Ignored evidence is retained in `.benchmark-assets/1166/ba-campaign/`: frozen
factories, exact pre/post-ABI shader programs, native binaries, original and final
CUDA SASS/resource reports on every target, all raw timing samples and misses,
reference/oracle logs, source receipts, and artifact regeneration logs. Run
`check_metal_model_families.m` with the frozen header specified by
`ANTFLY_METAL_FAMILY_BASELINE_HEADER`; absent that header it performs AA.
`check_cuda_training_rows.py BASELINE.cubin CANDIDATE.cubin --bench` requires only
Python's standard library and the CUDA driver. No unrelated models or GPU jobs
were modified.
