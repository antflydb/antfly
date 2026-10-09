# CUDA Backend

## Overview

The CUDA backend supports inference on NVIDIA GPUs while preserving Antfly
inference's portability model:

- A normal Antfly inference build does not require the CUDA toolkit.
- A normal Antfly inference container does not ship CUDA runtime libraries, cuBLAS,
  cuDNN, TensorRT, ONNX Runtime, or XLA.
- The native CUDA path uses only the NVIDIA driver ABI at runtime:
  `libcuda.so.1`, loaded dynamically.
- XLA/PJRT remains an optional compiled-graph path for dense/static graph
  execution, not the primary GGUF quantized runtime.
- CPU/native fallback remains available for unsupported devices, tensor
  formats, and operators.

The production target is GGUF decoder inference on GKE-class NVIDIA nodes,
starting with L4 and T4. A100 and H100 should be supported by the same portable
kernel artifacts, then optimized when profiling justifies architecture-specific
paths.

## Research Basis

The current design is based on:

- NVIDIA CUDA Driver API documentation: the driver API lives in the driver
  `cuda` dynamic library and exposes `cu*` entry points; this matches a
  `dlopen("libcuda.so.1")` runtime contract.
- NVIDIA CUDA compatibility documentation: PTX embedded for a lower virtual
  compute capability can be JIT-compiled by the driver for later GPUs, but
  older PTX will not automatically exploit newer architecture features.
- OpenXLA PJRT documentation: PJRT is a uniform device API with device-specific
  plugin implementations. Antfly inference already exposes an `xla` backend choice that
  maps to PJRT when `enable_pjrt` is compiled.
- OpenXLA XLA:GPU documentation: XLA lowers StableHLO graphs through GPU
  compilation pipelines that can emit GPU kernels, including PTX-oriented code.
- OpenXLA StableHLO quantization documentation: StableHLO quantization is
  uniform per-tensor/per-axis quantization; GGUF's block-packed formats such as
  `Q4_0`, `Q8_0`, and K-quants are not naturally represented as one StableHLO
  quantized `dot_general`.
- XLA custom-call documentation: XLA FFI can call host functions that receive a
  CUDA stream and launch CUDA kernels, but that API is still experimental. It is
  useful as an escape hatch, not the core dependency-free CUDA backend.
- GGUF/ggml practice: common GGUF files carry mixed tensor formats including
  F16/BF16/F32 plus block quantized legacy, K-quant, I-quant, and newer formats.

Reference links:

- https://docs.nvidia.com/cuda/cuda-programming-guide/03-advanced/driver-api.html
- https://docs.nvidia.com/cuda/cuda-c-programming-guide/
- https://docs.nvidia.com/cuda/cuda-driver-api/
- https://openxla.org/xla/pjrt
- https://openxla.org/xla/gpu_architecture
- https://openxla.org/stablehlo/quantization
- https://openxla.org/stablehlo/spec
- https://openxla.org/xla/custom_call
- https://huggingface.co/docs/hub/en/gguf

## Strategic Decision

Use two lanes, with different jobs:

| Lane | Role | Dependencies | Best Fit | Not Best Fit |
|---|---|---|---|---|
| Native CUDA driver backend | Primary GGUF inference path | `libcuda.so.1` only at runtime | Quantized GGUF linears, resident weights, decoder fast paths | Full compiler optimization, arbitrary model graphs |
| XLA/PJRT | Optional compiled-graph path | PJRT plugin and XLA artifacts | Dense/static graph inference, safetensors/ONNX-style graph exports, correctness comparison | Direct block-packed GGUF quant matmul without custom calls |

Do not make XLA a prerequisite for CUDA. The native CUDA path should be useful
when the only NVIDIA component visible in the container is the driver mounted by
the node.

Do not require cuBLAS for the first production path. Optional cuBLASLt dispatch
is allowed for dense F16/BF16 matmul when the libraries are present, but the
driver-only kernels still need to preserve correctness and coverage for
dependency-light deployments and GGUF packed-weight formats.

## Existing Codebase Fit

Relevant local state:

- `pkg/inference/src/native_backend_choice.zig` already has `xla`, mapped to
  `BackendKind.pjrt` for compiled partitions.
- `pkg/inference/src/graph/compiled_pjrt.zig`,
  `pkg/inference/src/graph/pjrt_compiler.zig`, and
  `pkg/inference/src/graph/pjrt_executor.zig` already define the PJRT lane.
- `pkg/inference/src/graph/quant_matmul.zig` already provides the shared
  quantized matmul vocabulary: dispatch buckets, row buckets, packed format
  descriptors, and operator support.
- `pkg/inference/QUANT_KERNEL_COMPILER.md` documents the shared build-time
  quant kernel spec/codegen flow and promotion evidence policy used by Metal
  today and CUDA artifacts later.
- `pkg/inference/src/gguf/tensor_types.zig` and
  `pkg/inference/src/gguf/quant_codec.zig` are the canonical GGUF type and
  dequantization references.
- `pkg/inference/src/ops/native_compute.zig`,
  `pkg/inference/src/ops/metal_compute.zig`, and
  `pkg/inference/src/ops/wasm_compute.zig` already contain quantized matmul
  behavior and fallback patterns.
- `build.zig` exposes `-Dcuda` and `-Dcuda-artifacts` for embedding checked-in
  CUDA artifacts without invoking CUDA tooling during normal builds.

The CUDA work should integrate through these existing contracts instead of
creating another quant selector or model-specific backend path.

## CUDA Runtime Contract

CUDA support is optional and probe-based:

- On startup, try `dlopen("libcuda.so.1")`.
- Resolve only the CUDA Driver API symbols Antfly inference uses.
- Call `cuInit`, enumerate devices, and select one device.
- Prefer retaining the device primary context so Antfly inference composes with other
  driver users in the same process. Backend-owned contexts are acceptable for
  isolated smoke tests, but not the production default.
- Use one default stream per CUDA backend instance at first. Add extra streams
  only after there is measured overlap to exploit.
- If any probe step fails, mark CUDA unavailable and keep the existing fallback
  chain.
- Keep all driver handles behind Zig-owned `CudaDriver` and `CudaContext`
  tables.
- All device allocations, streams, modules, functions, and events are owned by
  the CUDA backend and released by backend teardown.

Initial symbol set:

- `cuInit`
- `cuDriverGetVersion`
- `cuDeviceGetCount`
- `cuDeviceGet`
- `cuDeviceGetName`
- `cuDeviceComputeCapability`
- `cuDevicePrimaryCtxRetain`
- `cuDevicePrimaryCtxRelease`
- `cuCtxSetCurrent`
- `cuStreamCreate`
- `cuStreamSynchronize`
- `cuStreamDestroy`
- `cuMemAlloc`
- `cuMemFree`
- `cuMemcpyHtoDAsync`
- `cuMemcpyDtoHAsync`
- `cuMemcpyDtoDAsync`
- `cuModuleLoadDataEx`
- `cuModuleUnload`
- `cuModuleGetFunction`
- `cuLaunchKernel`
- `cuGetErrorName`
- `cuGetErrorString`

Add events, graph launch, stream-ordered allocation, virtual memory, and
multi-GPU APIs only after the single-device inference path is correct.

## GPU Compatibility

Compatibility floor:

| GPU | Compute capability | Role |
|---|---:|---|
| T4 | `sm_75` | Cheapest compatibility floor |
| A100 | `sm_80` | Existing high-throughput accelerator |
| L4 | `sm_89` | Preferred GKE cost/performance target |
| H100 | `sm_90` | High-end validation target |

Checked-in CUDA artifacts are generated with the pinned CUDA `13.2` toolkit.
The portable artifact is PTX ISA `9.2` for `compute_75` / `.target sm_75`.
CUDA 13.2 PTX does not load on the tested R580 driver API 13.0
(`CUDA_ERROR_UNSUPPORTED_PTX_VERSION`), so that configuration must use the
default fatbin until portable PTX is generated with a genuinely compatible
toolkit. Rewriting only the `.version` header is not safe: it loaded but failed
the q4_0 smoke tolerance. The default artifact is a fatbin
with cubins for the current validation targets and a `compute_75` PTX fallback:

- `sm_75` baseline cubin for T4 startup latency.
- `sm_80` cubin for A100.
- `sm_89` cubin for L4.
- `sm_90` cubin for H100.
- `sm_100`, `sm_110`, and `sm_120` cubins for Blackwell-generation targets.

Keep the `compute_75` PTX path even when fatbins are used by default.
Architecture-specific cubins must never be the only checked-in artifact.

## Kernel Artifact Policy

Normal builds must not invoke `nvcc`, `ptxas`, `clang --cuda`, or network
downloads.

Use this layout:

| Path | Purpose |
|---|---|
| `pkg/inference/src/ops/cuda/driver.zig` | Driver API dynamic loader |
| `pkg/inference/src/ops/cuda/context.zig` | Device/context/stream lifecycle |
| `pkg/inference/src/ops/cuda/buffer.zig` | Device memory and host copies |
| `pkg/inference/src/ops/cuda/kernels.zig` | Embedded PTX/fatbin module loading and diagnostics |
| `pkg/inference/src/ops/cuda/quant.zig` | GGUF format descriptors for CUDA |
| `pkg/inference/src/ops/cuda/cuda_compute.zig` | `ComputeBackend` implementation |
| `pkg/inference/src/ops/cuda/kernels/*.cu` | Developer kernel sources |
| `pkg/inference/src/ops/cuda/artifacts/*.ptx` | Checked-in portable PTX |
| `pkg/inference/src/ops/cuda/artifacts/*.fatbin` | Checked-in multi-arch fatbins |

Build flags:

- `-Dcuda=true`: compile CUDA backend Zig code and embed checked-in artifacts.
- `-Dcuda=false`: current default; the backend is not yet enabled by default (see Open work).
- `-Dcuda-artifacts=portable`: embed the checked-in portable PTX only.
- `-Dcuda-artifacts=fatbin`: embed the checked-in multi-arch fatbin. This is
  the default.
- `-Dcuda-artifacts=sm89`: embed the checked-in SM89 cubin for an exact L4-class
  deployment or candidate gate; it is rejected on other compute capabilities.
- `-Dcuda-libs=auto`: use optional CUDA library acceleration when available.
- `-Dcuda-libs=required`: require CUDA libraries such as cuBLASLt to load.
- `-Dcuda-libs=off`: do not load optional CUDA libraries.

Use `scripts/regen-cuda-artifacts.sh --check` to verify CUDA artifacts and
`scripts/regen-cuda-artifacts.sh --write` to update them. The script requires
CUDA `13.2`, verifies portable PTX ISA `9.2` and `.target sm_75`, verifies
fatbin cubins for the supported SM targets when `cuobjdump` is available, and
checks that required CUDA symbols are present before updating checked-in
artifacts. CUDA-enabled CI may verify checked-in artifact freshness, but normal
CI should not need CUDA.
The equivalent Linux CUDA 13.2 build target is
`zig build cuda-artifacts-check`.

## Quant Kernel Compiler Lane

Quant matmul codegen is a build/dev-time lane, not runtime JIT. The compiler
spec lives in `pkg/inference/src/graph/quant_kernel_compiler.zig`; generated
dev candidates and manifests live under `pkg/inference/src/ops/cuda/generated`
and `pkg/inference/src/ops/metal/generated`. The unified generated-artifact
registry currently has 7 production-qualified, runtime-default-off Metal routes
and 21 non-promoted CUDA
entries. The Metal lane covers 25 small-batch quant routes across
Q2/Q3/Q4/Q5/Q6/Q8 families, plus opt-in generated RMSNorm, decode-1x paged
attention, and flash-prefill attention. `QUANT_KERNEL_COMPILER.md` is the
authoritative route and evidence inventory.

Use `zig build quant-kernel-codegen -- --check` to verify generated sources and
manifests, or `zig build quant-kernel-codegen -- --write` after intentionally
changing the spec. Standalone generated CUDA source files are never direct
artifact inputs. The canonical `artifacts/inference_cuda_kernels.cu` contains
both benchmark-qualified generated kernels and a compiler-managed region of
default-off, runtime-wired dev candidates; the manifest records the distinction.
Promotion requires correctness and sequential benchmark evidence, then CUDA
13.2 artifact regeneration.

Use `zig build quant-kernel-local-check -Dmetal=false -Dcuda=false` for the
cross-platform compiler gate: generated-source freshness, compiler/renderer and
CUDA evidence unit tests, and CUDA artifact source-policy checks. Pull-request
Zig CI runs this host-only gate explicitly, and the ordinary package `test` step
also enforces source freshness and source policy. On macOS, use
`quant-kernel-metal-local-check -Dmetal=true -Dcuda=false` for generated Metal
compile and on-device evidence. Neither replaces the Linux CUDA 13.2
`zig build cuda-artifacts-check` gate.

Use `zig build quant-kernel-metal-runtime-check -Dmetal=true -Dcuda=false` for
the dev-only generated MSL runtime correctness check; add
`-- --evidence-out /private/tmp/antfly-quant-metal-evidence.json` to persist the
same sequential correctness and handwritten-baseline timing evidence as JSON.
Add `--repeat-runs N` to aggregate sequential timings by median. Metal promotion
evidence requires at least 5 repeats, records `minimum_speedup`, and currently
requires both median and repeat-stability speedup of at least `1.02` over the
handwritten baseline for every promoted-kernel case.
Use `-- --check-evidence PATH --require-promotion-ready --require-kernel KERNEL`
to fail a promotion attempt for one candidate when the evidence is still
dev-only, lacks a baseline, misses the speed gate, or loses to the handwritten
route. Promotion evidence paths are kernel-specific and must include `KERNEL`;
the generated artifact manifest pins the exact evidence and check commands for
each Metal candidate.

From `pkg/inference`, the first lazy target evidence uses the manifest-pinned
portable fatbin and benchmark commands:

```sh
nvcc -fatbin \
  -gencode=arch=compute_75,code=sm_75 \
  -gencode=arch=compute_80,code=sm_80 \
  -gencode=arch=compute_89,code=sm_89 \
  -gencode=arch=compute_90,code=sm_90 \
  -gencode=arch=compute_75,code=compute_75 \
  src/ops/cuda/generated/quant_kernel_q4_k_small_batch_bias_gelu.cu \
  -o /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin

zig-out/bin/antfly-inference bench-cuda \
  --warmup-iters 5 \
  --measure-iters 50 \
  --quant-compiler-lazy-target \
  --quant-compiler-generated-ptx /tmp/antfly_q4_k_small_batch_bias_gelu_f32_v1.fatbin \
  --quant-compiler-repeat-runs 3 \
  --quant-compiler-evidence-out src/ops/cuda/generated/evidence/q4_k_small_batch_bias_gelu_benchmark.json

zig-out/bin/antfly-inference bench-cuda \
  --quant-compiler-check-evidence src/ops/cuda/generated/evidence/q4_k_small_batch_bias_gelu_benchmark.json \
  --quant-compiler-require-promotion-ready
```

The `--quant-compiler-*-ptx` option names are retained for CLI compatibility,
but the loader accepts any CUDA module image. Generated benchmark fatbins carry
`sm_75`, `sm_80`, `sm_89`, and `sm_90` SASS plus a `compute_75` PTX fallback, so
the evidence command runs on those architectures without asking an older driver
to JIT CUDA 13.2 PTX ISA.

That final check is intentionally a promotion gate: it fails while the CUDA
candidate is still dev-only.

## GLiNER2 CUDA Q4 Span Kernels

The complete FP16 encoder, generated tensor-core attention, Fastino comparison,
correctness evidence, production dispatch policy, and remaining work are
documented in [`models/gliner2/CUDA.md`](models/gliner2/CUDA.md).

CUDA GLiNER2 span-head weights use resident `Q4_K` kernels by default when the
checked-in CUDA module exposes the required GLiNER span primitives. This avoids
upload-time dequantization for `span_rep.span_rep_layer.*.weight` tensors and
keeps the span head on packed weights.

Runtime overrides:

- `TERMITE_CUDA_DISABLE_GLINER_SPAN_Q4_KERNELS=1`: use the fp32-upload span
  path instead of the resident GLiNER span `Q4_K` kernels.
- `TERMITE_CUDA_ENABLE_GLINER_SPAN_Q4_KERNELS=0`: legacy opt-out alias for the
  same behavior.
- `TERMITE_CUDA_DEQUANTIZE_QUANT_WEIGHTS=1`: force upload-time dequantization
  for quantized weights.

## GLiNER2.5 Multi and Decide-1B

The inference runtime has separate CUDA profiles for the multilingual boundary
models and the Ettin/ModernBERT decision encoder. The nested encoder config
selects the architecture; a span wrapper no longer implies DeBERTa. The new
profiles do not change GLiNER training or the existing Metal dispatch policy.

| Checkpoint | Implemented CUDA path | Context policy |
|---|---|---|
| `fastino/gliner2.5-multi-v1` | Boundary extraction and classification heads | Explicit boundary windows |
| `fastino/GLiNER2.5-multi-Decide` | Boundary classification and typed decisions | Explicit classification windows |
| `fastino/GLiNER2.5-Decide-1B` | Label-marker MLP over Ettin/ModernBERT | Native 7,999 encoder tokens, including schema |

Distinct decision rows are grouped by encoded length, padded with independent
attention/marker masks, and scattered back to request/task order. Default
physical batches are bounded to 64 rows and 16,384 padded tokens. CUDA boundary
requests and classification windows also batch through the managed executor.
The Ettin attention primitive handles both sliding and full attention without
materializing a quadratic score tensor, including fully masked rows. Boundary
inference has a separate tiled FP32 attention kernel; training keeps its
versioned replay implementation.

These model families remain **pending release qualification**. No new release
capability rows or registry model pins are granted by the diagnostic tools.
Exact upstream revisions, file hashes, source hashes, FP32 oracle captures and
capacity fixtures live in `testdata/gliner25/family/manifest.json`. The Decide
capacity matrices cover 128/512/2048 tokens at B1/B8 and 128 tokens at B32/B64.
The 1B matrix also covers 4096/7999 tokens at B1; these synthetic cases are not
a natural-language holdout.

Retained L4 measurements use 30 pairs and 200 tail samples per fixture or
capacity cell. The three reports and compressed samples are under
`testdata/gliner25/family/evidence/`:

| Fixture campaign | Fastino reference | Native geometric speedup (95% interval) |
|---|---|---|
| Multi, ten extraction cases | FP32 eager | 5.44× (5.41–5.47×) |
| Multi-Decide FP16, eight capacity cells | FlashDeBERTa FP16, FP16 resident weights | 1.53× (1.52–1.54×) |
| Decide-1B FP16, ten capacity cells | SDPA FP16, FP16 resident weights | 1.123× (1.120–1.126×) |

Reports distinguish `reference_dtype` from `reference_weight_dtype`. Fastino
FP16 autocast with FP32 resident weights is a different profile from FP16
resident weights; every profile must pass quality checks before comparison.

All Multi extraction cells passed the 10% regression guard. These are fixture-level
comparisons against the named profiles, not claims against the fastest valid
profile across the full release matrix. Multi's FlashDeBERTa FP16 candidate
changed extraction output structure and failed the quality gate.
The latest full Multi-Decide capacity campaign with reduced encoder projections
and wide/compact relative attention passes five of eight cells against
FlashDeBERTa with FP16 resident weights. Its geometric speedup is 1.53×
(1.52–1.54×), but 512 tokens/B8 and 2048 tokens/B1/B8 fail the per-cell guard
at 1.22×, 1.22× and 1.49× Python latency.
Passing the aggregate speedup does not override those failures. The same
candidate passes all 18 classification fixtures against the FP32 oracle;
the earlier Multi extraction candidate passes all ten fixtures after restoring
exact exponentiation in boundary attention. Passing and rejected quality
results are summarized in `evidence/summary.json`. The final Decide-1B FP16 campaign, after
QKV/RoPE fusion and the NFC tokenizer fix, passes all ten capacity cells against
the quality-checked Fastino SDPA profile with FP16 resident weights. Its
geometric speedup is 1.123× (1.120–1.126×). At 7,999 tokens/B1, native median
latency is 1,073 ms versus Fastino's 1,142 ms: 0.940× Python latency
(95% interval 0.938–0.944×). The previously failing 2,048-token/B8 cell now
passes at 1.066× Python latency. Raw paired samples, 200 tail measurements per
cell, both worker identities and the full report are archived under
`evidence/l4_decide_1b_capacity_fp16_nfc_vs_fastino_fp16_weights/`.
Intermediate campaign histories are external run artifacts. These
loaded-model measurements do not qualify production mixed precision, serving
latency or every possible Fastino dtype profile. Multi-Decide's three capacity
regressions and the remaining release checks are follow-up work.
Use `--stop-on-regression` to stop a
diagnostic campaign after the first measured performance failure without
discarding its samples. A completed campaign also exits unsuccessfully when
the aggregate or any per-cell performance guard fails; its complete report
and raw samples remain available.

`/decisions` accepts optional `long_document` with `mode`, `window_words`,
`overlap_words`, and `max_windows`. Omission keeps over-limit rejection.
Window mode is limited to qualified boundary decision models; Decide-1B keeps
its native context policy. Preload configuration accepts `cuda_precision`:

```json
{"kind":"extractor","name":"fastino/GLiNER2.5-Decide-1B","backend":"cuda","cuda_precision":"fp32"}
```

`auto` currently resolves to FP32. Managed loads reject explicit `fp16`/`bf16`
with `UnqualifiedGlinerCudaPrecision` until release evidence is available.
Diagnostic 1B constructors may measure those candidates: only encoder matrix
weights are reduced, while embeddings, norms, classifier weights and logits
remain FP32. Mixed candidates use masked FP16 tensor-core attention with FP32
online softmax/output accumulation. The saved initial measurements used FP32
local attention; later evidence records the local tensor-core candidate.
The boundary diagnostic worker also accepts `--precision fp16`; this candidate
reduces encoder projection matrices and attention while retaining authenticated
FP32 source weight storage, embeddings, norms and task heads. Projection mirrors
are model-owned and reject non-finite or overflowing values. Use
`--attention-only 1` on the worker (or `--attention-only` on the Python harness)
to retain FP32 projections. Reports distinguish the two compute policies. The
relative-position tiles use the exact processor bucket map and retain the
original exponential calculation. For at least 512 tokens, wider direct
attention tiles reuse query/key fragments and scatter compact relative
products without an extra global workspace. At 2048 tokens and above,
adjacent equal buckets share one relative product; arbitrary bucket order and
original score arithmetic are preserved. Earlier cached-product candidates
remain documented in the evidence reports. FP32 bias addition remains separate
from FP16 matrix multiplication: a cuBLASLt bias-epilogue candidate changed two
Arabic selections and failed the holdout gate, so it was removed from dispatch.
This diagnostic option does not enable managed mixed precision serving. The
ModernBERT profile fuses QKV splitting and full-head split-half RoPE; its rebuilt
worker passes all 20 short and capacity accuracy fixtures. Packed and branched
position layouts retain their existing operations.
The 1B public holdout subsequently found a Hindi token mismatch: its BPE
tokenizer declares NFC, including canonical composition exclusions. BPE now
applies that normalization before pre-tokenization, protecting added tokens.
The tokenizer suite passes 101 tests (one skipped), and the rebuilt 1B FP16
worker matches all 1,600 documents across eight languages, including every
token ID. The current replay oracle retains the corrected token IDs.
Precision is part of both resident and in-flight model identity.

The split Q8_0 Decide-1B bundle from
[Metal PR #1033](https://github.com/antflydb/antfly/pull/1033) also has an
offline CUDA route. Use the immutable
[Hugging Face revision](https://huggingface.co/antflydb/gliner2.5-decide-1B-gguf/tree/5c61a1a39ad4c6865d0e0ff98a7ac507e8c35cef).
Both GGUF files, all sidecars, the 199-tensor inventory and exact encoder
geometry are authenticated before CUDA upload. Shadowed tensor names and
other packed formats are rejected. The 113 encoder matrices (including token
embeddings) retain Q8 storage; norms, classifier and auxiliary heads stay F32.
Q8 tensor-core projections use FP16 operands and F32 accumulation/output.
`ANTFLY_CUDA_QMATMUL_VARIANT=legacy` selects direct Q8/F32 arithmetic without
the additional tensor-core weight packs. Missing optional tensor-core symbols
retain the existing direct-Q8 compatibility path. Explicit diagnostic
`fp16`/`bf16` upload precision is rejected for packed-only source weights.

Reproduce the offline comparison without adding model files or large fixtures
to Git (run from `zig/pkg/inference`):

```sh
MODEL=/tmp/gliner-decide-q8
hf download antflydb/gliner2.5-decide-1B-gguf \
  antfly_inference_bundle.json config.json encoder_config/config.json \
  tokenizer.json tokenizer_config.json gliner2-encoder.Q8_0.gguf gliner_head.gguf \
  benchmark-cases.json validation.json \
  --revision 5c61a1a39ad4c6865d0e0ff98a7ac507e8c35cef --local-dir "$MODEL"
python scripts/gliner25/prepare_decide_quant_capture.py \
  --cases "$MODEL/benchmark-cases.json" --validation "$MODEL/validation.json" \
  --output /tmp/gliner-decide-q8-cases.json
zig build bench-gliner-decide-quant-build test-gliner-decide-quant \
  -Dcuda=true -Dmetal=false -Donnx=false -Doptimize=fast -j1
zig build test -Dtest-filter='GLiNER Decide CUDA' \
  -Dcuda=true -Dmetal=false -Donnx=false -Doptimize=fast -j1
ANTFLY_CUDA_DISPATCH_STATS=1 \
  zig-out/bin/antfly-inference-gliner-decide-quant-bench \
  --model-dir "$MODEL" --capture /tmp/gliner-decide-q8-cases.json \
  --backend cuda --warmups 3 --reps 10 --tolerance 0.002
```

The capture builder preserves label order and binds published input IDs to
the independently decoded Q8 oracle, rather than the original FP32 checkpoint.
The benchmark verifies exact token IDs, finite logits and absolute errors on
every run. It reports both the full loaded pipeline and the prepared
encoder/classifier/completed-readback clock; HTTP and model load are separate.

`ANTFLY_CUDA_GLINER_1B_Q8_F16_MIRRORS=1` enables an opt-in cached FP16
projection candidate analogous to the Metal optimization. Only the 112 typed
encoder projections receive mirrors. Raw Q8 tensors remain resident; token
embedding lookup and F32 normalization, attention and task heads retain their
existing routes. Host decoding uses a 16 KiB F32 tile and at most 26.25 MiB of
FP16 staging per projection, checked for malformed blocks and finite FP16
range. The mirror replaces the Q8 tensor-core pack rather than adding a third
weight representation. Model teardown releases all mirrors. Disabling the
candidate, or lacking its complete FP16 kernel/library capabilities, retains
Q8 execution. Device residency includes all execution packs and mirrors.

On an NVIDIA L4 with driver 580.159.03, all 14 published cases retain their
Q8-reference selections. Maximum logit error is 0.00177 for packed tensor-core
execution, 0.00140 with FP16 projection mirrors, and 0.00000477 for direct Q8.
The original FP32 checkpoint also passes all 14 cases (maximum error 0.00000406).
Projection mirrors use 3.61 GB of resident weights, versus 2.73 GB for Q8 plus
tensor-core packs and 1.74 GB for direct Q8. Prepared-core diagnostic medians
for the mirror profile and a decoded-Q8 PyTorch FP16 SDPA reference are:

| Tokens, batch 1 | Native mirrors | PyTorch FP16 |
|---|---:|---:|
| 25 | 9.86 ms | 22.80 ms |
| 83 | 13.79 ms | 24.74 ms |
| 198 | 24.84 ms | 25.05 ms |
| 512 | 70.58 ms | 30.53 ms |
| 2,048 | 664.25 ms | 132.94 ms |

These are sequential, unpaired diagnostic replays with three warmups and ten
timed repetitions, not a release performance campaign. Both clocks include
prepared CPU input upload and completed CPU logit readback. Python uses
Torch 2.14.0/Transformers 5.17.0, SDPA, disabled TF32 and an FP16 encoder with
an F32 classifier; native retains F32 embedding outputs, norms and attention. The
Python resident model contains only the encoder and classifier, while native
retains the complete bundle. The two synthetic capacity cases pass a 0.002
logit tolerance against an independent decoded-Q8 F32 reference. Short-case
competitiveness does not establish long-context parity: the 512/2,048-token
cells remain slower, and qualifying tensor-core attention for this Q8 profile
is follow-up work.

CUDA validation is independent of public serving qualification. The exact
Q8 bundle does not acquire CUDA `/decisions` qualification from the Metal PR
or this offline benchmark. HTTP admission, cancellation/concurrency, broad
holdouts and the complete capacity/performance matrix remain release checks.

Build optimized workers from `zig/pkg/inference`:

```sh
zig build bench-gliner25-decide-build bench-gliner25-cuda-build \
  -Dcuda=true -Dmetal=false -Doptimize=fast
ANTFLY_LAYA_BACKEND=cuda zig build test -Dcuda=true -Dmetal=false \
  -Dtest-filter='GLiNER CUDA long attention'
python -m unittest discover -s scripts/gliner25 -p test_family_contract.py
```

Use the pinned Fastino environment specified by the manifest, with `psutil`
installed for process monitoring. Download the exact model revisions/files and
provide explicit classification-head metadata when testing the raw 1B artifact
(`model_manifest.json` with `type: extractor`, `tasks: [extract, decide]`,
`capabilities: [classification, typed_decisions]`, `inputs: [text]`, and
`gliner_classification_head: label_marker_mlp`). Raw Multi-Decide directories
used by the typed-decision HTTP test need the same task/capability metadata,
with the `gliner_classification_head` field omitted. These local declarations
select routing; they do not grant boundary release qualification. The oracle
verifies downloaded files and imported upstream source before running:

```sh
python scripts/gliner25/check_family_decisions.py \
  --native zig-out/bin/antfly-inference-gliner25-decide-bench \
  --model-dir /models/decide-1b \
  --cases testdata/gliner25/family/decide_1b/capacity_cases.json \
  --oracle testdata/gliner25/family/decide_1b/capacity_oracle_fp32.jsonl \
  --output /tmp/decide-1b-parity.json
python scripts/gliner25/benchmark_family.py \
  --native zig-out/bin/antfly-inference-gliner25-decide-bench \
  --python /path/to/reference/bin/python --model-dir /models/decide-1b \
  --model decide_1b --attention sdpa --output /tmp/decide-1b-paired
python scripts/gliner25/family_oracle.py \
  --model multi_decide --model-dir /models/multi-decide --dtype fp32 \
  --cases testdata/gliner25/family/multi_decide/capacity_requests.json \
  > /tmp/multi-decide-capacity-oracle.jsonl
```

For Multi-Decide select `--model multi_decide` and
`zig-out/bin/antfly-inference-gliner25-cuda-bench` as the native worker.
Select `--model multi` to check the full boundary fixture suite, including
entity coordinates, attributes, relations, records and classification.
For Multi-Decide capacity campaigns, pass `--cases`, `--native-cases`, and
`--oracle` using its `capacity_requests.json`, `capacity_cases.json`, and
`capacity_oracle_fp32.jsonl` fixtures respectively.
Run separate campaigns for every quality-valid Fastino attention/dtype profile
and compare against the fastest one. Use `--reference-weight-dtype fp16` with
`--reference-dtype fp16` to measure reduced resident weights; the default keeps
FP32 resident weights. The harness checks all token IDs, selected
labels and confidences before alternating paired measurements; reports include
raw samples, tail distributions and bootstrap intervals. It requires at least
30 pairs and 200 tail samples per case and always emits `qualification: false`.
Serving concurrency, long-window performance, cancellation/OOM recovery and the
99.5% holdout gate must also pass before publishing release capabilities.

The public classification parity suite uses 200 deterministically selected
[MASSIVE test examples](https://huggingface.co/datasets/AmazonScience/massive)
per language: English, French, German, Spanish, Arabic, Hindi, Japanese and
Chinese. Dataset revision, file hashes, attribution and selection instructions
are under `testdata/gliner25/family/holdout/`. It measures agreement with Fastino
FP32, not gold accuracy or independence from checkpoint training data. Each
language must independently reach 99.5%; token drift or host fallback fails the
campaign. `--task entities --model multi` runs an additional entity-output and
coordinate comparison on the same texts. `--task structured --model multi`
uses the pinned `holdout/structured_schema.json` to compare entities,
attributes, relations and natural-mode records. It reports prediction counts
per head and refuses to pass when a head has no reference predictions across
the campaign. JointIE and other record modes still need their own holdouts.

`score_family_classification.py` separately scores these same pinned captures
against MASSIVE's gold intent labels, without rerunning inference or selecting
examples by outcome. Decide-1B native FP16 and Fastino FP32 both score 643/1,600
(40.1875%) on the 60-label public test subset. That measures task accuracy;
the 1,600/1,600 implementation agreement is a separate result. The gold score
and per-language results are retained in `evidence/summary.json`. The pinned
classification replay capture and scoring tool can reproduce the gold score.

The revised FP16 Multi-Decide candidate matched 1,599/1,600 documents: Spanish matched
199/200 and the other seven languages matched 200/200. Multi's FP16 candidate
failed the per-language gate (Arabic 198/200, Hindi 196/200, Chinese 198/200),
so its ten passing extraction fixtures do not justify enabling that precision.
Multi's FP32 candidate matched all 1,600 classification documents and 1,599/1,600
entity documents (Spanish 199/200, all other languages 200/200). The rejected
Multi-Decide bias-epilogue candidate matched 1,597/1,600 documents, with Arabic
at 198/200. Revised candidates need fresh measurements before qualification.
Current compressed holdout oracles and their report summaries are retained in
the evidence directory; rejected candidate results are in `evidence/summary.json`.
Multi FP32's structured-output campaign exercises 1,673 entity/attribute
predictions, 576 relations and 867 records. Strict parity fails French at
198/200 (Spanish 199/200, others 200/200): all three differing documents have
Fastino spans extending past the original input into its appended period.
Native preserves its existing source-valid span policy. The retained strict
structured oracle preserves that comparison. The approved compatibility policy excludes
Fastino entity predictions or relations whose spans include its single
synthetic terminal period beyond the original text. It does not clip spans,
ignore valid-coordinate differences, or change native decoding. Re-evaluating
the pinned capture under that policy matches all 1,600 documents, excluding
three entity predictions and one relation; 1,670 entities/attribute groups,
575 relations and 867 records remain checked. Every excluded prediction and
the original strict result are recorded. Other invalid coordinates, record
differences, token drift and confidence errors still fail. Use
`--source-span-policy original_text` for this explicit comparison, or
`recheck_family_holdout.py` to re-evaluate a pinned strict capture without
rerunning inference. This compatibility exception does not grant release
qualification.
The entity-only capture also matches all 1,600 documents under this policy,
with one synthetic-terminal entity prediction excluded; its strict comparison
already passed the per-language threshold.
Fastino's FlashDeBERTa FP16 reference matched 1,598/1,600 documents with either
FP32 or FP16 resident weights: Arabic and Hindi matched 199/200, the remaining
languages 200/200. This establishes classification quality for those profiles,
not their relative speed or suitability for Multi's extraction tasks.
The resident-FP16 Fastino profiles also pass all 18 Multi-Decide and all 20
Decide-1B short/capacity fixtures with FlashDeBERTa and SDPA respectively.
Decide-1B's resident-FP16 SDPA reference also passes its public comparison:
English, Arabic and Hindi match 199/200, and the other languages match 200/200.

```sh
python scripts/gliner25/check_family_holdout.py \
  --native zig-out/bin/antfly-inference-gliner25-cuda-bench \
  --python /path/to/reference/bin/python --model multi_decide \
  --model-dir /models/multi-decide --precision fp16 \
  --output /tmp/multi-decide-public-parity
python scripts/gliner25/check_family_reference_holdout.py \
  --python /path/to/reference/bin/python --model multi_decide \
  --model-dir /models/multi-decide --dtype fp16 --weight-dtype fp16 \
  --attention flashdeberta \
  --oracle-report testdata/gliner25/family/evidence/l4_multi_decide_fp16_compact_public_classification/report.json \
  --output /tmp/fastino-multi-decide-public-parity
ANTFLY_GLINER25_FAMILY_MODEL_DIR=/models/multi-decide \
  zig build test -Dcuda=true -Dmetal=false \
  -Dtest-filter='GLiNER family CUDA typed decisions'
```

The opt-in HTTP test uses actual loopback requests at concurrency 1/4/16,
compares distinct requests with their serial results and checks admission
cleanup. Multi-Decide FP32 passes all three levels with every lease released
and no leaks; recorded results and the source log hash are retained in
`evidence/summary.json`.
CUDA boundary requests queue before reserving device scratch, so waiting
requests do not each reserve the same session workspace. A queued client
disconnect also drains while that lane remains locked, with no device
reservation or kernel launches; retry returns the identical response.
It uses the existing test-only boundary qualification override;
passing it does not publish a production capability or establish serving
performance.
`ANTFLY_GLINER25_FAMILY_CUDA_PRECISION=fp16` selects the candidate precision in
these opt-in tests. Its per-manager override and factory entry point exist
only in test builds; production loads retain the release gate. The same
variable selects precision for the managed 1B full-context and boundary-window
tests.
The model-backed managed window test also passes 2/8/32 windows per document,
comparing window batch sizes one and four, exact window counts, pre-cancelled
and interrupted calls, and identical retry responses. Its retained summary is
correctness evidence; paired window performance is still required.

## Gemma4 And TurboQuant KV Status

Gemma4 CUDA defaults remain `f32` KV for production correctness. The optional
TurboQuant cache formats (`polar4`, `turbo3`) are available through
`--cache-dtype`, and CUDA now has an opt-in fully paged device path: compressed
keys are scored directly on device, values are stored as int8-per-head rows, and
attention resolves logical tokens through the CUDA block table instead of a
contiguous span assumption.

Current CUDA behavior:

- Default Gemma4 CUDA runs through the existing f32 device KV read/write and GQA
  attention path.
- `--cache-dtype polar4` stores device K rows in packed 4-bit Polar4 format and
  scores Q against compressed K directly.
- `--cache-dtype turbo3` stores device K rows as packed 3-bit keys plus the
  deterministic residual sketch and scores Q against both pieces directly.
- Both TurboQuant dtypes store CUDA V rows with the same int8-per-head value
  codec used by host KV storage, then dequantize V inside the decode attention
  kernel.
- `ANTFLY_CUDA_DISABLE_TURBOQUANT_COMPRESSED_V=1` keeps compressed K but forces
  f32 V storage for A/B testing.
- Unsupported shapes, missing CUDA symbols, stale artifacts, or
  `ANTFLY_CUDA_DISABLE_TURBOQUANT_KV=1` fall back to the existing non-compressed
  path.

`polar4` is a production-candidate opt-in compressed-K/compressed-V path with
zero host attention fallback on the workloads checked so far. It is not the
CUDA default yet: 12B Q4 f32/polar4 output matched deterministically in a
32-token raw check, but E2B f32/polar4 output diverged, so promotion needs an
explicit quality/parity acceptance gate, not just the runtime gate (see
History and Evidence for the dated measurement run). `turbo3` is functional
and resident but remains experimental — it is slower than `polar4` on L4
decode workloads and can change output quality more aggressively.

The measured win from the CUDA block-table upload cache is memory residency
and lower KV metadata overhead, not higher decode throughput: it cuts the
volume of block-table uploads needed to keep the paged device KV path
resident, which is why `polar4`/`turbo3` promote on residency and fallback
behavior rather than on raw tok/s.

User-facing E2B CUDA smoke from the repository root:

```sh
zig/pkg/inference/zig-out/bin/antfly-inference generate \
  .models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf \
  "Give a one sentence summary of Korean history." \
  --backend cuda \
  --max-tokens 128 \
  --print-timing \
  --print-token-count
```

If running from `zig/pkg/inference/zig-out/bin`, pass an absolute model path.
The model loader treats the first argument as a model path relative to the
current working directory, so `./antfly-inference generate .models/...` from the
binary directory will fail with `NoTokenizerFound`.

```sh
./antfly-inference generate \
  /home/timkaye/tim/antfly/.models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf \
  "Give a one sentence summary of Korean history." \
  --backend cuda \
  --max-tokens 128 \
  --print-timing \
  --print-token-count
```

Optional `polar4` E2B smoke:

```sh
zig/pkg/inference/zig-out/bin/antfly-inference generate \
  .models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf \
  "Give a one sentence summary of Korean history." \
  --backend cuda \
  --cache-dtype polar4 \
  --max-tokens 128 \
  --print-timing \
  --print-token-count
```

Validation ladder for compressed KV:

```sh
zig build -Dcuda=true

zig/pkg/inference/scripts/regen-cuda-artifacts.sh --check --all

zig/pkg/inference/zig-out/bin/antfly-inference cuda-info --smoke

zig/pkg/inference/scripts/gemma4/validate_cuda_turboquant_gemma4.sh --quick

zig/pkg/inference/zig-out/bin/antfly-inference generate \
  /path/to/gemma4-12b-target \
  "Write one sentence about ants." \
  --backend cuda \
  --cache-dtype polar4 \
  --max-tokens 16 \
  --temperature 0 \
  --print-token-ids \
  --print-timing

zig/pkg/inference/zig-out/bin/antfly-inference generate \
  /path/to/gemma4-12b-target \
  "Write one sentence about ants." \
  --backend cuda \
  --cache-dtype turbo3 \
  --max-tokens 16 \
  --temperature 0 \
  --print-token-ids \
  --print-timing
```

### Gemma 4 A4B defaults and rollback

CUDA qualifies one fail-closed Gemma 4 26B-A4B configuration: NVIDIA SM89,
30 MoE layers, 128 experts, top-8 routing, hidden size 2816, intermediate
size 704, and Q4_0 expert projections, with all packed experts resident.
Only full residency is supported: a streamed request, mismatched device,
geometry or quantization, missing required kernel, or insufficient memory
envelope fails model load instead of selecting host MoE execution.

The default CUDA A4B load policy is full residency loaded through a bounded
pinned-host pipeline; an explicit A4B budget flag is unnecessary for the
qualified model, although the global backend/combined budgets must still
admit the allocation. `--a4b-load-strategy legacy` is the rollback to the
prior loader. `pipeline` fails closed on pinned-staging/worker/host-allocation
problems, while `auto` falls back to `legacy` only in those same cases.

`--a4b-prepared-pack` controls whether an offline-built balanced expert pack
(`antfly-inference a4b-pack`) is used instead of the canonical GGUF. The
default `auto` policy uses `$MODEL/a4b-cuda-pack-v2` when present; an absent
pack uses the canonical GGUF normally, and a stale, malformed, or
geometry-mismatched optional pack emits a warning before falling back to the
canonical GGUF. `required` rejects any of those fallback conditions instead
of silently using the GGUF, and `off` always forces the canonical GGUF.
Pack manifests bind to a relocatable source fingerprint and preserve
per-shard SHA-256 digests for offline verification.

For rolling workers, clean checkpoint pages remain in the reclaimable kernel
page cache after a successful full-residency upload. Use
`--a4b-drop-host-cache-after-load` when host-memory pressure matters more
than replacement-worker admission. Server `startup_strategy: "prefetch"` can
warm the canonical GGUF or an installed prepared pack without creating a CUDA
session; this is an operator-controlled cache-warming mechanism, not a
promised restart-speedup, since its benefit depends on the deployment's
host-memory and storage topology.

For field isolation of the CUDA post-FFN normalization fusion, set
`ANTFLY_INFERENCE_CUDA_DISABLE_A4B_PARALLEL_FFN_POST_RESIDUAL=1`. The request
then uses the shared unfused graph path; model admission and every other
qualified CUDA A4B kernel remain unchanged.

## Inference Surface

The first CUDA execution surface should be:

```text
C[M, N] = A[M, K] @ B_quant[N, K]^T
```

where:

- `A` is dense f32 initially; add f16 input once f32 correctness is locked.
- `B_quant` is raw GGUF-packed weight storage.
- `C` is f32 initially; add f16 output only after tolerances and downstream ops
  are explicit.
- `M = 1` decode is the first performance target.
- `M = 2..8` small-batch decode/prompt is the second target.
- `M >= 9` prefill is the third target.

Route every CUDA quantized linear through `graph/quant_matmul.zig`:

- Use `quant_matmul.plan(...)` for row bucket and preferred operator.
- Add CUDA-local capability checks that turn unsupported preferred operators
  into fallback.
- Record counters with the same operator names as Metal/WebGPU/native:
  `mul_mv`, `mul_mv_ext`, `mul_mm`, and `fallback`.
- Do not add public per-format APIs such as `cudaQ4KMatmul`; keep one internal
  descriptor-driven dispatch.

The current CUDA backend also exposes the common dense/model primitives needed
by ClipClap, GLiNER2, and DeBERTa reranker sessions:

- dense f32 linear/bias, dense f16/bf16 weight paths, activation,
  normalization, embedding, concat, convolution, and attention helpers
- optional cuBLASLt f16/bf16 matmul dispatch for eligible dense weights
- GGUF `Q8_0`, `Q4_0`, and `Q4_K` linear kernels
- 5 benchmark-qualified compiler-generated `Q4_0` kernels (decode GEMV, prefill rows
  9-64, FFN gate+up pair, and the q8_1/DP4A E4B fused-FFN pair+down),
  runtime-default-off behind positive per-kernel opt-ins, with per-kernel and
  master disable gates; see `QUANT_KERNEL_COMPILER.md` (Current CUDA State) for
  measured speedups, qualification evidence, and exact gate names
- an opt-in generated GQA decode-attention candidate specialized for
  `q_seq_len=1`, `head_dim=256`, and the nullable device-scalar ABI. Enable it
  with `ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE=1`; module loading
  fails closed if the generated symbol is missing. On an NVIDIA L4 it measured
  modestly faster than the hand-written route with exact token parity; it
  remains opt-in pending broader model, context-length, and masking coverage.
- GLiNER-oriented DeBERTa attention/head helper kernels

Required common kernels are loaded eagerly when the CUDA module is loaded. If a
stale artifact bundle is missing the selected model family's required symbols,
session creation must fail with `CudaKernelUnavailable` instead of silently
falling back to an incomplete GPU path.

CUDA session creation now uses explicit capability profiles:

| Profile | Model families | Required capability group |
|---|---|---|
| `clipclap` | CLIP, CLAP, ClipCLAP embedding paths | dense linears, bias/activation fusions, embedding lookup, layer/RMS norm, concat, conv2d, SDPA |
| `deberta_reranker` | DeBERTa cross-encoder rerankers | `clipclap` primitives plus take-rows, DeBERTa attention, split-last-dim |
| `gliner2` | GLiNER2 recognition | `deberta_reranker` primitives plus GLiNER word embeddings and label GRU combine |
| `gliner25_boundary` | GLiNER2.5 Multi and Multi-Decide | Boundary encoder, extraction and classification device primitives |
| `gliner25_modern_bert` | GLiNER2.5 Decide-1B | ModernBERT primitives plus masked long-context encoder attention |

`antfly inference cuda-info --smoke` prints the loaded artifact's profile
capability booleans before running kernel smokes. Production validation should
require the relevant profile to be `true` before running real model fixtures.
## Quantization Coverage

The CUDA backend covers, in order of priority:

1. `Q8_0`: simplest correctness anchor; useful for activation-like data.
2. `Q4_0`: common legacy 4-bit format and simple 32-value blocks.
3. `Q4_K`: common modern GGUF target and the first K-quant proof.
4. `Q5_K`, `Q6_K`, `Q8_K`.
5. `Q4_1`, `Q5_0`, `Q5_1`, `Q8_1`.
6. `Q2_K`, `Q3_K`, `Q1_0`.

`IQ4_NL`, `IQ4_XS`, `I2_S`, `MXFP4`, `NVFP4`, `TQ1_0`, and `TQ2_0` are not yet
covered (see Open work).

Every CUDA-supported format needs:

- byte-size agreement with `gguf/tensor_types.zig`
- row-dequant parity with `gguf/quant_codec.zig`
- synthetic matrix parity against CPU dense reference
- real GGUF smoke counters proving the CUDA kernel executed
- fallback behavior for unsupported row shapes and packed expert variants

## Quantized GGUF Limitations

The important constraint is not "CUDA cannot run unquantized GGUF"; it can.
The issue is where performance and memory come from:

- Dense F16/BF16/F32 GGUF weights need dense GEMM. Optional cuBLASLt dispatch
  handles eligible F16/BF16 cases, while driver-only dense kernels remain a
  correctness fallback and are not expected to beat vendor libraries.
- Large unquantized models require much more VRAM than Q4/Q5/Q6 GGUF files, so
  the useful GKE target set is narrower unless we add robust CPU/GPU layer
  offload.
- StableHLO quantized types do not directly encode GGUF block layouts, scales,
  mins, lookup tables, and mixed per-tensor formats. A naive XLA route would
  either dequantize weights to dense buffers or require custom calls.
- Dequantizing all weights to f16/f32 on GPU discards GGUF's main memory
  advantage and can exceed VRAM.
- Custom calls can let XLA invoke our kernels, but then XLA becomes an
  orchestration layer around the same CUDA kernels and brings an experimental
  ABI plus plugin/runtime dependencies.

Therefore:

- Native CUDA should optimize GGUF packed-weight inference.
- XLA/PJRT should optimize dense/static graph inference and serve as a
  validation/packaging path.
- Do not block native CUDA on solving arbitrary unquantized LLM performance.

## Kernel Strategy

### Correctness Kernels

Start simple:

- One CTA computes one or a small group of output elements.
- Load GGUF-packed blocks from global memory.
- Decode in registers or shared memory.
- Accumulate in f32.
- Write f32 output.

These kernels establish memory ownership, module loading, launches, and parity.
They are allowed to be slower than ggml.

### Decoder Kernels

Then implement ggml-shaped decode kernels:

- `mul_mv` for `M = 1`.
- One block or warp group per output row, depending on format and `K`.
- Coalesced reads of packed weight blocks.
- Shared input vector cache when it improves reuse.
- Per-format dot helpers under one kernel family.
- Optional Q8 activation packing only after profiling shows it helps.

### Small Batch And Prefill

Add:

- `mul_mv_ext` for `M = 2..8`.
- `mul_mm` for prompt/prefill.
- Shared temporary activation layout for large `M` if it beats direct dense
  f32/f16 loads.
- Batched QKV and gate/up paired linears once single linear kernels are stable.

### Architecture-Specific Fast Paths

Only after generic kernels work:

- DP4A-style integer dot paths for T4 and later.
- Tensor-core-assisted paths where the quant format can be profitably repacked.
- `sm_80`, `sm_89`, and `sm_90` cubins selected at runtime.

Architecture-specific kernels are optional accelerators. They must fall back to
portable `compute_75` PTX.

## Backend Implementation

### Build and backend plumbing

`-Dcuda` and `-Dcuda-artifacts` wire to checked-in artifacts only (no `nvcc`
in normal builds). `cuda` is a first-class graph/backend contract:
`BackendKind.cuda`, `TensorStorageClass.cuda_buffer`, and partition/runtime
parsing for `"cuda"`. `cuda` is one of the session backend ordering options,
CLI choices, and `--backend cuda` is validated explicitly. The default `auto`
order does not include CUDA.

### Capability probe

`CudaDriver` is a dynamic loader (`dlopen("libcuda.so.1")`, driver-API symbol
resolution). `antfly-inference cuda-info --smoke` reports driver version,
selected device, compute capability, memory, and artifact mode. On machines
without CUDA the probe reports unavailable without crashing; on CUDA machines
it succeeds without a CUDA toolkit in the container.

### Buffers and kernel launch

Device allocation, free, H2D/D2H/D2D copies, stream sync, and module loading
are implemented (`src/ops/cuda/context.zig`, `buffer.zig`, `kernels.zig`).
Module loading captures CUDA JIT info/error logs so PTX problems are visible
on the first NVIDIA-box run.

### Dense linear correctness

Dense f32 `linearNoBias`/`linear` route through `--backend cuda` explicitly,
returning CUDA tensors from `fromFloat32Shape` and copying back through
`toFloat32`. Optional cuBLASLt f16/bf16 matmul dispatch
(`src/ops/cuda/dense_lt.zig`, `cublaslt.zig`) covers eligible dense weights.

### Quantized linear

CUDA tensor storage holds host-packed GGUF weight bytes. `Q8_0`, `Q4_0`, and
`Q4_K` linears run as CUDA kernels, routed through the shared
`quant_matmul.plan(...)` row-bucket planner, with counters for planned
operator, actual operator, format, row bucket, and fallback reason. Quantized
weights stay resident on device across tokens, with prepared linear slots for
QKV, output projection, FFN gate/up/down, and LM head. CPU fallback applies
per unsupported format/operator rather than per whole model. `Q5_K`, `Q6_K`,
and `Q8_K` are also supported; RMSNorm, RoPE, softmax, and attention run on
device using the same shared `mul_mv`/`mul_mv_ext`/`mul_mm` operator
vocabulary as Metal and native.

### XLA/PJRT NVIDIA lane

`--backend xla` maps to PJRT, with the CUDA GPU plugin supplied externally.
Required environment variables: `ANTFLY_INFERENCE_XLA_PLUGIN`,
`ANTFLY_INFERENCE_PJRT_PLUGIN`, `PJRT_PLUGIN_PATH`, `PJRT_PLUGIN`. `-Dpjrt=true`
is a real build option (`build.zig`'s `pjrt` option), and requesting
`--backend xla` without PJRT enabled fails with a clear `error.BackendUnavailable`
rather than falling through silently.

Use PJRT for:

- whole-model or partitioned dense graphs
- HLO/executable artifact packaging already present in `native_compile.zig`
- correctness comparison for dense paths
- eventual graph-level scheduling around native CUDA results if custom calls
  become worth the dependency

Do not use PJRT for:

- loading raw GGUF packed weights directly into XLA quantized tensors
- the first quantized decoder runtime
- dependency-free CUDA deployment

### GKE container validation

The Linux CUDA build compiles the backend in with no CUDA runtime libraries
included, and `libcuda.so.1` is expected to come from the NVIDIA driver
mount. This has been validated on GKE L4 (see History and Evidence):
no-CUDA startup fallback, CUDA smoke probe, dense linear parity,
`Q8_0`/`Q4_0`/`Q4_K` synthetic parity, and real GGUF generation with CUDA
counters all pass. Validation on T4, A100, and H100 GKE nodes is open (see
Open work), along with remaining PJRT-lane polish (NVIDIA-specific docs for
`ANTFLY_INFERENCE_XLA_PLUGIN`/`PJRT_PLUGIN_PATH`, a dense-graph smoke on an
actual CUDA PJRT plugin, and keeping PJRT artifacts separate from native CUDA
artifacts in manifests).

## Testing Matrix

Most tests should run without NVIDIA hardware:

- no-CUDA dynamic loader test
- CUDA symbol table construction test with a fake loader where possible
- GGUF tensor type and byte-size tests
- quant row-dequant tests against `quant_codec.zig`
- quant matmul planner tests in `graph/quant_matmul.zig`
- CUDA artifact presence/currentness test that does not execute GPU code

CUDA-present tests:

- capability probe
- vector fill/add launch
- dense f32 linear parity
- `Q8_0`, `Q4_0`, `Q4_K` matmul parity
- fallback-on-unsupported-format test
- real GGUF generation with fixed prompt/settings and CUDA counters
- GKE L4 container smoke

## Generated Decode And Continuous Batching

Gemma 4 CUDA decode can opt into the generated head-dimension-256 online
softmax attention kernel with
`ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE=1`. The generated Q4_0
Q8_1 pair/down FFN kernels accept runtime row, input, and output dimensions,
but the fused Q8_1 precompute route remains experimental because its long-run
E2B throughput gate is slower than the established FFN route. Enable it with
`ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE=1`; force it
off with `ANTFLY_INFERENCE_CUDA_DISABLE_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE=1`.

Server continuous batching is typed configuration and defaults off:

```json
{
  "generation_batching": {
    "mode": "on",
    "max_step_items": 2,
    "max_step_query_tokens": 512,
    "max_decode_wait_us": 1000
  }
}
```

With `mode: on`, the current safety envelope admits homogeneous two-row decode;
prefill stays singleton and more than two active requests use the optimized
singleton route. The row-two path has repeated paged-KV growth coverage but
remains experimental because long generation may differ from singleton token
output and the optimized singleton decoder is currently faster. Set
`ANTFLY_INFERENCE_DISABLE_CONTINUOUS_BATCHING=1` for the global rollback.
See [docs/CUDA_BATCHING.md](CUDA_BATCHING.md) for the canonical rollout
contract and promotion gate.

Run the hardware gate with:

```sh
python3 scripts/gemma4/benchmark_gemma4_cuda_batching.py
```

The gate records response fingerprints, latency and aggregate throughput by
concurrency, scheduler metrics, and the c1/c4 acceptance decision. A wider
batch must not be promoted merely because it is faster: response equivalence
and the single-request p95 bound are mandatory.

## Correctness Rules

- Dense f32 matmul: tight absolute/relative tolerance.
- Quantized matmul: compare against CPU dequantized or native quant reference
  with explicit per-format tolerance.
- Generation smoke: stable token IDs for fixed seed/settings where sampling is
  deterministic.
- No-CUDA startup behavior: byte-for-byte same CLI behavior where practical,
  except debug/probe logs.

## Telemetry And Debugging

Add counters early:

- device name and compute capability
- selected artifact kind: PTX or cubin/fatbin
- loaded capability profiles: `clipclap`, `gliner2`
- planned quant operator
- actual CUDA operator
- fallback reason
- per-format kernel counts
- H2D/D2H bytes during generation
- resident weight bytes
- peak device bytes

Expose these in existing smoke/generate timing output so acceptance tests can
prove GPU execution instead of just proving successful text generation.

Fallback defaults are intentionally strict:

- Planned quant matmul fallback is rejected unless
  `ANTFLY_CUDA_ALLOW_PLANNED_FALLBACK=1` is set.
- RoPE/GQA host fallback remains debug-only behind
  `ANTFLY_CUDA_ALLOW_HOST_ATTENTION_FALLBACK=1`.
- Any production smoke that enables either flag must report fallback counters
  and should not be used as a release gate unless the fallback is the behavior
  being tested.

## Acceptance Criteria

CUDA meets its minimal-usefulness bar:

- `antfly-inference` starts on machines without CUDA and behaves as before.
- The same binary starts on a GKE L4 node and reports CUDA availability when
  requested.
- The container image contains no CUDA runtime, cuBLAS, cuDNN, TensorRT, ONNX
  Runtime, or XLA libraries for the native CUDA path.
- `Q8_0`, `Q4_0`, and `Q4_K` GGUF linears run on the GPU.
- A real GGUF generation smoke shows CUDA quantized matmul counters.
- CPU fallback remains available for unsupported formats and devices.
- XLA/PJRT remains independently usable for dense compiled graph inference when
  a PJRT plugin is supplied.

## History and Evidence

> **Relocated:** The dated L4 TurboQuant validation status and measurement
> tables that previously lived here (32 lines, checked 2026-06-21) are
> preserved verbatim in
> [docs/design/inference/history/cuda/turboquant-l4.md](../../../docs/design/inference/history/cuda/turboquant-l4.md).
> Durable decisions from it are in Gemma4 And TurboQuant KV Status in this
> document.

## Open work

- Promote `-Dcuda=true` to the default `auto` backend order once broader
  hardware validation lands.
- Promote `polar4` KV to the CUDA default after an explicit quality/parity
  acceptance gate (E2B f32/polar4 output currently diverges).
- `turbo3` KV remains experimental; needs a faster or higher-quality path
  before it is a default candidate.
- GKE container validation on T4, A100, and H100 has not been run (only L4
  is confirmed).
- `IQ4_NL`, `IQ4_XS`, `I2_S`, `MXFP4`, `NVFP4`, `TQ1_0`, and `TQ2_0` GGUF
  formats are not yet covered by CUDA kernels.
- Remaining PJRT-lane polish: NVIDIA-specific docs for
  `ANTFLY_INFERENCE_XLA_PLUGIN`/`PJRT_PLUGIN_PATH`, a dense-graph smoke on an
  actual CUDA PJRT plugin, and keeping PJRT artifacts separate from native
  CUDA artifacts in manifests.
- Which GGUF model defines first acceptance: small deterministic fixture, a
  common 7B Q4_K model, or both?
- Should release builds ship only portable PTX at first, or PTX plus L4/T4
  cubins once CI can generate them?
- How aggressive should per-op fallback be before the cost of CPU/GPU
  transfers makes whole-layer fallback preferable?
- Should CUDA direct sessions load before or after Metal in `auto` when
  running on multi-platform developer machines?
- When should CUDA graph launch be introduced for decoder token loops?
