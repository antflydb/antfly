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

Do not rely on cuBLAS for the first production path. That forces runtime
libraries into the image and does not solve GGUF packed-weight formats. Dense
F16/F32 matmul without cuBLAS is still needed for correctness and smaller
models, but unquantized large models are not the initial performance target.

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
- `pkg/inference/src/gguf/tensor_types.zig` and
  `pkg/inference/src/gguf/quant_codec.zig` are the canonical GGUF type and
  dequantization references.
- `pkg/inference/src/ops/native_compute.zig`,
  `pkg/inference/src/ops/metal_compute.zig`, and
  `pkg/inference/src/ops/wasm_compute.zig` already contain quantized matmul
  behavior and fallback patterns.
- `pkg/inference/src/graph/backend_contracts.zig` does not yet include
  `cuda` as a graph `BackendKind` or `cuda_buffer` storage class.
- `pkg/inference/src/backends/backends.zig` does not yet expose CUDA as a direct
  session backend.
- `build.zig` does not yet have `-Dcuda` or CUDA artifact options.

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

The first portable artifacts should be PTX for `compute_75`. The NVIDIA driver
can JIT that PTX on later devices. This keeps one checked-in artifact path for
T4, L4, A100, and H100.

Later, add optional cubins/fatbins. The current implementation rejects
`-Dcuda-artifacts=fatbin` until those artifacts exist so builds cannot silently
fall back to PTX while reporting a fatbin mode.

- `sm_75` baseline cubin for T4 startup latency.
- `sm_80` cubin for A100.
- `sm_89` cubin for L4.
- `sm_90` cubin for H100.

Keep the `compute_75` PTX path even after adding cubins. Architecture-specific
PTX or cubins must never be the only artifact.

## Kernel Artifact Policy

Normal builds must not invoke `nvcc`, `ptxas`, `clang --cuda`, or network
downloads.

Use this layout:

| Path | Purpose |
|---|---|
| `pkg/inference/src/ops/cuda/driver.zig` | Driver API dynamic loader |
| `pkg/inference/src/ops/cuda/context.zig` | Device/context/stream lifecycle |
| `pkg/inference/src/ops/cuda/buffer.zig` | Device memory and host copies |
| `pkg/inference/src/ops/cuda/kernels.zig` | Embedded PTX module loading and JIT diagnostics |
| `pkg/inference/src/ops/cuda/quant.zig` | GGUF format descriptors for CUDA |
| `pkg/inference/src/ops/cuda/cuda_compute.zig` | `ComputeBackend` implementation |
| `pkg/inference/src/ops/cuda/kernels/*.cu` | Developer kernel sources |
| `pkg/inference/src/ops/cuda/artifacts/*.ptx` | Checked-in portable PTX |
| `pkg/inference/src/ops/cuda/artifacts/*.fatbin` | Optional checked-in fatbins |

Build flags:

- `-Dcuda=true`: compile CUDA backend Zig code and embed checked-in artifacts.
- `-Dcuda=false`: default until the backend is mature.
- `-Dcuda-artifacts=portable`: embed the checked-in portable PTX only.
- `-Dcuda-artifacts=fatbin`: reserved for future multi-arch artifacts; rejected
  today.

Add a developer-only regeneration step for CUDA artifacts. CUDA-enabled CI may
verify checked-in artifact freshness, but normal CI should not need CUDA.

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

- Dense F16/F32 GGUF weights need dense GEMM. Without cuBLAS, our first dense
  kernels will be correctness-grade, not competitive with vendor libraries or
  XLA.
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

- Add `-Dcuda` and `-Dcuda-artifacts`.
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

Correctness rules:

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
- planned quant operator
- actual CUDA operator
- fallback reason
- per-format kernel counts
- H2D/D2H bytes during generation
- resident weight bytes
- peak device bytes

Expose these in existing smoke/generate timing output so acceptance tests can
prove GPU execution instead of just proving successful text generation.

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
- Should CUDA direct sessions load before or after MLX/Metal in `auto` when
  running on multi-platform developer machines?
- When should CUDA graph launch be introduced for decoder token loops?
