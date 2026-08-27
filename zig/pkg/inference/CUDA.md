# Antfly inference NVIDIA Inference Plan

## Goal

Support inference on NVIDIA GPUs while preserving Antfly inference's current portability
model:

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
- `-Dcuda=false`: default until the backend is mature.
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
documented in [`docs/GLINER2_CUDA.md`](docs/GLINER2_CUDA.md).

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

Status checked on 2026-06-21 on an NVIDIA L4 (`sm_89`) with CUDA Toolkit 13.2
and driver R580:

- `zig build -Dcuda=true`, `regen-cuda-artifacts.sh --check --all`,
  `antfly-inference cuda-info --smoke`, and `zig build test -Dcuda=true` pass.
- `polar4` is a production-candidate opt-in compressed-K/compressed-V path. It
  stays fully resident on CUDA with zero host attention fallback in the E2B and
  12B Q4 checks below.
- `turbo3` is functional and resident, but remains experimental. It is slower
  than `polar4` on the current L4 decode workloads and can change output quality
  more aggressively.
- `polar4` is not the CUDA default yet. Deterministic 12B Q4 f32/polar4 output
  matched in the 32-token raw check, but E2B f32/polar4 output diverged. Promote
  only after an explicit quality/parity acceptance gate, not just the runtime
  gate.

Measured L4 results from `/tmp/antfly-cuda-turboquant-prod`:

| Workload | Cache | Tokens | Load | Warm TTFT | Cold TTFT | Decode tok/s | CUDA KV status |
| --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| E2B Korean summary | f32 | 128 | 8.56s | 0.30s | 8.86s | 17.11 | 4480/4480 device KV successes |
| E2B Korean summary | polar4 | 128 | 8.43s | 0.30s | 8.73s | 16.70 | 1920 compressed-V writes, 4480 reads |
| 12B Q4 Korean summary | f32 | 40 | 17.01s | 2.01s | 19.02s | 8.67 | 1968/1968 device KV successes |
| 12B Q4 Korean summary | polar4 | 30 | 17.10s | 1.98s | 19.08s | 8.66 | 1488 compressed-V writes, 1488 reads |
| 12B Q4 raw repeat 1 | polar4 | 32 | 17.09s | 0.63s | 17.71s | 9.16 | zero fallback |
| 12B Q4 raw repeat 2 | polar4 | 32 | 17.02s | 0.63s | 17.65s | 9.06 | zero fallback |
| 12B Q4 raw repeat 3 | polar4 | 32 | 16.82s | 0.63s | 17.45s | 9.04 | zero fallback |

The current performance win is memory residency and lower metadata overhead, not
higher tok/s. The block-table upload cache reduced E2B 16-token `polar4`
block-table uploads to 30, and the longer 128-token E2B `polar4` stress run used
135 uploads while completing 4480 device-KV reads with zero fallback.

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
  fails closed if the generated symbol is missing. On an NVIDIA L4 with Gemma 4
  E2B QAT `UD-Q4_K_XL`, three 128-token runs measured a median 64.91 tok/s
  versus 63.18 tok/s for the hand-written route (+2.7%), with 3,556/4,480
  attention launches generated and exact 32-token ID parity. It remains opt-in
  pending broader model, context-length, and masking coverage.
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

`antfly inference cuda-info --smoke` prints the loaded artifact's profile
capability booleans before running kernel smokes. Production validation should
require the relevant profile to be `true` before running real model fixtures.
## Quantization Priorities

The minimum useful GGUF set is:

1. `Q8_0`: simplest correctness anchor; useful for activation-like data.
2. `Q4_0`: common legacy 4-bit format and simple 32-value blocks.
3. `Q4_K`: common modern GGUF target and the first K-quant proof.

Then broaden:

4. `Q5_K`, `Q6_K`, `Q8_K`.
5. `Q4_1`, `Q5_0`, `Q5_1`, `Q8_1`.
6. `Q2_K`, `Q3_K`, `IQ4_NL`, `IQ4_XS`, `I2_S`, `Q1_0` as target models
   require them.
7. `MXFP4`, `NVFP4`, `TQ1_0`, `TQ2_0`, and other newer formats only after
   their CPU references and model demand are clear.

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

## Integration Phases

### Phase 0: Build And Backend Plumbing

- Keep `-Dcuda` and `-Dcuda-artifacts` wired to checked-in artifacts only.
- Add `cuda` to graph/backend contracts:
  - `BackendKind.cuda`
  - `TensorStorageClass.cuda_buffer`
  - partition/runtime parsing for `"cuda"`
- Add `cuda` to session backend ordering, CLI choices, and explicit
  `--backend cuda` validation.
- Keep default `auto` order unchanged until CUDA passes real smoke tests.

### Phase 1: Capability Probe

- Implement `CudaDriver` dynamic loader.
- Add an internal smoke probe that prints driver version, selected
  device, compute capability, total memory if available, and artifact mode.
- Test no-CUDA machines: probe returns unavailable without crashing.
- Test CUDA machines: probe succeeds without CUDA toolkit in the container.

### Phase 2: Buffers And Kernel Launch

- Implement device allocation, free, H2D/D2H/D2D copies, stream sync, and module
  loading.
- Embed one tiny PTX kernel such as fill or vector add.
- Capture CUDA JIT info/error logs during module loading so PTX problems are
  visible on the first NVIDIA-box run.
- Add skipped/fallback-safe tests for host copy, kernel launch, and output
  parity.

### Phase 3: Dense Linear Correctness

- Implement basic f32 `linearNoBias` and `linear` for dense weights.
- Return CUDA tensors from `fromFloat32Shape` and copy back through
  `toFloat32`.
- Route only explicit `--backend cuda` to this path.
- Compare small/medium shapes against native CPU.

### Phase 4: Quantized Linear MVP

- Implement CUDA tensor storage for host-packed GGUF weight bytes.
- Implement `Q8_0` and `Q4_0` `mul_mv`.
- Route through `quant_matmul.plan(...)`.
- Add counters for planned operator, actual operator, format, row bucket, and
  fallback reason.
- Add synthetic tests and one real GGUF smoke where CUDA quantized matmul is
  observed.

### Phase 5: First Production GGUF Path

- Implement `Q4_K` `mul_mv`.
- Keep quantized weights resident on device across tokens.
- Add prepared linear slots for decoder runtime requests:
  - QKV
  - output projection
  - FFN gate/up/down
  - LM head
- Add CPU fallback per unsupported format/operator, not per whole model when
  possible.
- Validate fixed-token generation on L4 and T4.

### Phase 6: Broader Format And Operator Coverage

- Add `Q5_K`, `Q6_K`, `Q8_K`.
- Add `mul_mv_ext` for small batches.
- Add `mul_mm` for prefill.
- Add RMSNorm, RoPE, softmax, and attention only after quantized linears are
  stable and measured.

### Phase 7: XLA/PJRT NVIDIA Lane

- Keep `--backend xla` as PJRT, with CUDA GPU plugin supplied externally.
- Document required environment variables:
  - `ANTFLY_INFERENCE_XLA_PLUGIN`
  - `ANTFLY_INFERENCE_PJRT_PLUGIN`
  - `PJRT_PLUGIN_PATH`
  - `PJRT_PLUGIN`
- Use XLA first for dense/static graph models and compiled artifact workflows.
- Do not use XLA as the default GGUF path unless a model is exported to dense
  graph artifacts and fits memory.
- Revisit XLA custom calls only after native CUDA kernels exist and there is a
  concrete need to run them inside compiled graph partitions.

### Phase 8: GKE Container Validation

- Build one Linux image with CUDA backend compiled in and no CUDA runtime
  libraries included.
- Deploy on GKE L4 first.
- Validate that `libcuda.so.1` comes from the NVIDIA driver mount.
- Run:
  - no-CUDA startup fallback
  - CUDA smoke probe
  - dense linear parity smoke
  - `Q8_0`, `Q4_0`, `Q4_K` synthetic parity
  - real GGUF generation with CUDA counters
- Repeat on T4, then A100/H100.

## XLA/PJRT Capability Plan

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

Concrete PJRT work:

- Make `-Dpjrt=true` a real configurable build option if it is intended for
  NVIDIA deployments; today `build.zig` hardcodes `enable_pjrt` false.
- Add NVIDIA-specific docs for `ANTFLY_INFERENCE_XLA_PLUGIN`/`PJRT_PLUGIN_PATH`.
- Add a dense graph smoke on a CUDA PJRT plugin once available.
- Add a clear error when `--backend xla` is requested without a plugin.
- Keep PJRT artifacts and native CUDA artifacts separate in manifests.

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
See [docs/CUDA_BATCHING.md](docs/CUDA_BATCHING.md) for the canonical rollout
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

CUDA is minimally useful when:

- `termite` starts on machines without CUDA and behaves as before.
- The same binary starts on a GKE L4 node and reports CUDA availability when
  requested.
- The container image contains no CUDA runtime, cuBLAS, cuDNN, TensorRT, ONNX
  Runtime, or XLA libraries for the native CUDA path.
- `Q8_0`, `Q4_0`, and `Q4_K` GGUF linears run on the GPU.
- A real GGUF generation smoke shows CUDA quantized matmul counters.
- CPU fallback remains available for unsupported formats and devices.
- XLA/PJRT remains independently usable for dense compiled graph inference when
  a PJRT plugin is supplied.

## Open Questions

- Which GGUF model defines first acceptance: small deterministic fixture,
  a common 7B Q4_K model, or both?
- Should release builds ship only portable PTX at first, or PTX plus L4/T4
  cubins once CI can generate them?
- How aggressive should per-op fallback be before the cost of CPU/GPU transfers
  makes whole-layer fallback preferable?
- Should CUDA direct sessions load before or after Metal in `auto` when
  running on multi-platform developer machines?
- When should CUDA graph launch be introduced for decoder token loops?
