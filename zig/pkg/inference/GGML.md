# GGML Reference And Quantization Support

## Scope

This document has three jobs:

- Track GGUF/GGML tensor format compatibility.
- Record the ggml/llama.cpp execution shape Antfly inference should follow where it is
  useful.
- Document the graph-execution partitioning and backend-executor design that
  implements that shape (the implementation history is in
  [ggml-graph-execution-history.md](../../../work-log/completed/inference/ggml-graph-execution-history.md)).

It fits with:

- [GRAPH.md](GRAPH.md): generic graph/runtime ownership.
- [METAL.md](METAL.md): the concrete pure-Metal backend plan.

The main architectural lesson from ggml is that the frontend graph stays
structural while the backend chooses kernels from tensor type, shape, quant
format, and device capability. Antfly inference should copy that shape, not the exact
file layout or every local constant.

## Execution Shape To Borrow

The useful ggml Metal pattern is:

- one graph or layer walk owns command encoding and synchronization
- tensor views are metadata where possible
- memory reuse is planned from liveness, not ad hoc temporary ownership
- quantized weights stay packed in backend-native storage
- matmul routes through a shared `mul_mat` surface
- the backend dispatches by shape:
  - `qLen = 1`: decode MMV/qgemv
  - small prompt rows: MMV-ext style kernels
  - larger prompt rows: MM/GEMM-style kernels
- fused attention/FFN/layer paths call the same quantized matmul primitive
  instead of embedding private quant decoding logic

For the local reference checkout, the most relevant files are:

- `../ggml/src/ggml-metal/ggml-metal-context.m`
- `../ggml/src/ggml-metal/ggml-metal-ops.cpp`
- `../ggml/src/ggml-metal/ggml-metal-device.cpp`
- `../ggml/src/ggml-metal/ggml-metal.metal`

The Antfly inference target is not "clone ggml inside Antfly inference". The target is the same
performance shape expressed through Antfly inference's graph/runtime boundaries:

- [GRAPH.md](GRAPH.md) owns graph/runtime policy.
- [METAL.md](METAL.md) owns the pure-Metal backend implementation plan.
- This file owns quant format compatibility and the upstream reference notes.

## MoE Packed Expert Layout

`llama.cpp` treats MoE expert weights as packed 3D tensors and routes them
through `ggml_mul_mat_id` with the selected expert IDs. Gemma4 uses the ggml
dimension order:

- `ffn_gate_up_exps.weight`: `[hidden, 2 * expert_ff, expert_count]`
- `ffn_down_exps.weight`: `[expert_ff, hidden, expert_count]`

That makes the expert axis the third dimension and the quantized input axis the
first dimension. This is normal GGUF/ggml layout, not an unsupported variant.
Antfly inference treats packed experts as first-class 3D weights plus expert
IDs, not as a bundle of independently contiguous 2D tensors.

Implementation implications:

- `expert_axis == 2` is valid for ggml-compatible Gemma4/Unsloth GGUF files.
- Native and Metal paths must not compute selected expert bytes as
  `total_bytes / expert_count` unless the layout proves that slice is
  contiguous.
- Fused gate/up tensors project once over the packed expert tensor, then split
  gate and up rows (or use an equivalent backend implementation).
- Backend kernels route through Antfly inference's `mulMatId` contract: full packed 3D
  weight tensor, selected expert IDs, logical `in_dim/out_dim`, and
  backend-owned layout handling. `moeLinearNoBias` remains as a compatibility
  wrapper for older call sites.
- Native CPU `mulMatId` must not materialize the full packed expert tensor. For
  quantized GGUF weights it keeps storage packed and performs selected-row block
  dot products directly against the quantized bytes.

### Implementation Status

Packed MoE routing uses this `mul_mat_id`-style contract end to end: the
generic `ComputeBackend` API, the native reference path, Metal native-quant
providers, and Metal grouped kernels all dispatch through it.

- Weight loading adds shared GGUF packed-expert layout metadata for both
  row-major legacy views and ggml expert-last views.
- Native packed quantized linear accepts `expert_axis == 2` when the input
  axis is the first GGUF dimension; dense packed-expert materialization
  supports the same layout, including fused gate/up row offsets.
- Metal packed-weight slicing uses the shared offset calculation, and Metal
  grouped dispatch can index full packed storage directly for
  `expert_axis == 2` instead of staging selected expert slabs.
- Gemma4 packed GGUF MoE registers one lazy weight per layer/projection
  (`packed.w1`, `packed.w2`, `packed.w3`) instead of one per expert/projection;
  grouped MoE lookup prefers the packed projection weights and keeps legacy
  per-expert names as fallback, and GGUF inspection treats the synthetic
  packed projection names as required weights so packed models report
  complete required-tensor coverage.
- `ComputeBackend` exposes `mulMatId` generically; Metal native-quant
  providers expose `mulMatId` and route grouped kernels through it. Native CPU
  has a `mulMatId` path covering legacy 2D packed expert weights and ggml
  expert-last 3D weights.
- Tests cover both existing row-major packed views and ggml expert-last Q8_0
  views.

## Graph Alignment Design

Antfly inference aligns with ggml at the execution-contract level, not by copying
ggml's graph structs or backend file layout. The shape is:

```text
Antfly inference frontend/tracing
  -> ml.graph.Graph
  -> canonical lowered op set
  -> capability + profitability partitioning
  -> liveness/buffer plan
  -> backend partition executor
  -> backend kernel picker and command encoder
```

The rough concept map is:

| ggml concept | Antfly inference equivalent |
| --- | --- |
| `ggml_cgraph` | `ml.graph.Graph` |
| `ggml_tensor` shape/type/view metadata | graph tensor descriptor + backend tensor handle |
| `ggml_backend_t` | `PartitionExecutor`, `ModelRuntime`, or backend runtime |
| `ggml_backend_buffer_type_t` | backend storage class / buffer allocator |
| `ggml_backend_sched` | `graph.Runtime` + partition planner + buffer planner |
| `GGML_OP_MUL_MAT`, `VIEW`, `RMS_NORM`, etc. | canonical lowered graph ops |
| Metal/WebGPU/CUDA kernel dispatch by type and shape | backend kernel registry |
| CPU/BLAS fallback | native/cblas host partition |

The important boundary is that graph policy stays in `src/graph/`, while raw
kernel ownership stays in backend-specific modules. The eager `ComputeBackend`
surface remains the correctness fallback and model-facing API, but static graph
execution should prefer backend-owned partition executors.

### Canonical Lowered Ops

Antfly inference's graph can keep fused model-level ops for tracing and optimization, but
compiled/static execution needs a small ggml-like lowered op set that every
backend can reason about:

- metadata/view ops: `view`, `reshape`, `transpose`, `permute`, `slice`
- lookup and movement ops: `get_rows`, `copy`, `contiguous`
- dense math: `matmul`, `matmul_trans_b`, `matmul_trans_a`
- quant math: `quant_matmul`, `grouped_quant_matmul`, `moe_grouped_matmul`
- elementwise ops: `add`, `sub`, `mul`, `div`, `scale`, unary activations
- normalization: `rms_norm`, `layer_norm`, `group_norm` where needed
- attention pieces: `softmax`, `rope`, attention/flash-attention fused forms
- model-specific primitives as needed: `conv`, `im2col`, `pool`, gather/scatter

Fused Antfly inference ops should lower to these primitives unless a backend explicitly
advertises a fused implementation. That keeps the graph portable while still
allowing Metal/WebGPU/native paths to use larger kernels when profitable.

### Backend Capability Matrix

Each backend should advertise support by operation plus the properties that
actually determine kernel viability:

```text
op + dtype + rank + layout/view form + storage class + quant type + shape constraints
```

Examples:

- native supports all lowered ops through f32/scalar/SIMD fallbacks
- cblas supports dense f32 GEMM-style host matmuls only when the shape is large
  enough to beat native/SIMD overhead
- Metal supports packed quant matmul only for quant families with native kernels
- WebGPU supports the subset of quant/dense kernels mirrored in browser shaders
- metadata-only views are supported when the backend tensor layout can express
  the resulting shape and stride without materialization

The profitability check matters. ggml's BLAS backend, for example, does not claim
every f32 matmul: it gates BLAS use on contiguity, f32 RHS, convertible LHS, and
a minimum matrix size. Antfly inference should make this explicit instead of treating
`supports(op)` as only a correctness predicate.

The storage class matters just as much as the op. ggml scheduling is tied to
backend buffer types: an op is cheap only if its inputs already live in a
compatible buffer, or if the transfer cost is justified by the following
partition. Antfly inference's capability model should therefore expose both:

- `canExecute`: the backend can produce correct results for this op
- `shouldExecute`: the backend is expected to be faster after transfer/setup
  costs for this shape and residency state

This capability matrix should drive graph partitioning and diagnostics. A
coverage report should answer: which nodes stayed on the target backend, which
nodes fell back, and which op/type/shape rule caused the fallback.

## Graph Execution: Partitioning And Backend Executors

Antfly inference's graph runtime implements the alignment model above: a
capability-based partitioner, tensor/storage descriptors, a liveness-driven
buffer plan, and backend-owned partition executors for native/cblas, Metal,
and WebGPU.

### Capability Decisions

Graph partitioning asks two questions per node instead of a single
`supports(OpCode)` check:

```text
Can this backend execute this graph node correctly?
Should this backend execute it for this shape/storage state?
Why was it accepted or rejected?
```

`CapabilityQuery` and `CapabilityDecision` carry that split. Assignment
requires both `can_execute` and `should_execute`, with diagnostic reason
buckets (`unsupported_op`, `unprofitable_shape`, `wrong_storage`,
`missing_quant_kernel`, `backend_disabled`) recorded for partition reports.
Native, cblas/Accelerate-style, and Metal each have eager graph decision
helpers; the legacy `supports(OpCode)` callback remains as a compatibility
adapter. This lets cblas accept a matmul it can run but reject it as too
small to beat call/thread-pool overhead, and lets Metal/WebGPU reject
supported ops when the transfer would be unprofitable.

### Tensor And Storage Descriptors

A graph-runtime tensor descriptor (`TensorStorageClass`, `TensorStrides`,
`TensorDesc`) tracks dtype, shape, stride/view metadata, storage class, quant
format, view source, and residency for every node. Storage classes
distinguish host f32, host packed quant, Metal buffer, WebGPU buffer, runtime
input, constant, and metadata view. Descriptor inference covers parameters,
constants, and reshape/transpose/slice/broadcast/range/shape-of nodes, and
seeded external residency/quant metadata expands into the full table before
partitioning, so every `CapabilityQuery` can inspect it.

### Profitability-Aware Partitioning

Native accepts all supported graph ops as the correctness fallback. cblas is
modeled as a host kernel provider and claims only profitable dense f32 GEMMs.
Metal/WebGPU claim device partitions only when the region is large enough to
amortize upload/dispatch cost, and keep parameters/metadata-view ops with
their resident storage owner where possible. Metal accepts tiny
already-resident device chains but rejects small host-input islands that
would not amortize transfer overhead.

### Native Partition Executor

`src/graph/native_partition_executor.zig` is the first real graph
`PartitionExecutor`: native/cblas partitions without a compiled executor run
through it, reusing interpreter node dispatch, backend vtable ops, runtime
input transfer, donation-aware liveness freeing, attention layer state, and
pair outputs. cblas/Accelerate stays inside native execution rather than
becoming a separate device runtime.

### Graph Buffer Plan

`src/graph/buffer_plan.zig` turns a graph, partition plan, tensor
descriptors, and output nodes into logical buffer slots, liveness intervals,
storage classes, and cross-partition transfer edges. Backends map logical
slots to concrete allocations: native/cblas to host buffers, Metal to
`MTLBuffer` ranges or scratch-pool slots, WebGPU to GPU buffers, and quant
weights to prepared packed storage. The plan reuses physical allocations
across non-overlapping lifetimes, keeps view slots attached to their source
allocation, and is threaded through `PartitionExecutor.ExecutionContext` and
validated in `MultiExecutor` before partition executors run.

### Metal Partition Executor

`src/graph/metal_partition_executor.zig` implements resident Metal execution
for profitable partitions: it validates the buffer plan, materializes
partition runtime inputs onto Metal, dispatches supported commands (dense
linear, layer/RMS norm, softmax, elementwise ops, RoPE, attention-block
composition) through the backend op surface, and synchronizes only at
partition/output boundaries. It uses the backend frame hooks
`decoderRuntimeBeginFrame`/`decoderRuntimeSubmitAndWaitFrame` when available,
with cancellation on errors, and evaluates only partition boundary outputs
after submission rather than synchronizing per node. `MetalCompute` can still
opportunistically route eager interpreter ops through resident Metal runtime
slots as a correctness/performance bridge (used by CLIPCLAP-style paths); new
operations should be added as backend primitives the partition executor can
call rather than as model-specific eager helpers.

The resident-execution policy is: Metal partition boundary outputs stay
device-resident until the final caller readback, and shared cross-device
transfers into Metal explicitly upload into private Metal buffers rather than
materializing to host in between. `TERMITE_GRAPH_EXECUTOR_STATS=1` prints
per-run counters (command dispatches, interpreter fallbacks,
runtime/cross-device transfers, device-resident outputs, host materialized
outputs, and boundary output materializations) from the shared `MultiExecutor`
path, so a real graph/model execution can be checked for whether it stayed
resident or silently fell back/materialized. See History below for the
detailed implementation log.

### Quant Matmul Routing

`src/graph/quant_matmul.zig` is the shared shape/format planner: it chooses
`mul_mv` for decode rows, `mul_mv_ext` for small prompt rows, `mul_mm` for
larger prompt rows, and fallback for unsupported/unprofitable shapes.
`CapabilityDecision` can carry the selected `OperatorPlan`, and
`PartitionPlan` persists a per-node `OperatorPlan` for later command
dispatch. `ComputeBackend` exposes `linearPlanned`/`linearNoBiasPlanned` so
graph executors can pass the selected plan into backend dispatch without
changing ordinary model code. Metal's Q8_0 path is the most complete consumer
today (`mul_mv`/`mul_mv_ext`/`mul_mm` dispatch, fused gate/up activation MM,
paired QKV projections); Q4_0, Q4_1, and Q5_K packed buckets are promoted
beyond decode into `mul_mv_ext`/`mul_mm`. Packed quant metadata survives graph
metadata views (reshape/slice/etc.) so it cannot be silently dequantized into
a generic dense primitive. The Q8_0 `mul_mm` kernel uses a conservative
16-output by 8-row Metal reduction tile; the fused Q8_0 gate/up activation MM
kernel uses the same reduction tile and is enabled by default. See History
below for the detailed implementation log.

### WebGPU Partition Executor

`src/graph/webgpu_partition_executor.zig` mirrors the Metal graph executor
shape for browser execution: the same capability model, storage descriptors,
and buffer planning, with WebGPU-specific command encoding and shader
dispatch. WebGPU is selectable as a compiled partition backend in Wasm/WebGPU
builds — `compiled` means graph compilation/planning/fusion/partitioning runs
and attaches `WebGpuPartitionExecutor`, mirroring Metal's compiled partition
path rather than emitting an offline WebGPU artifact. See History below for
the detailed implementation log.

### Validation

Each phase of graph-execution work keeps native/cblas as the oracle. Minimum
checks: partition tests for capability/profitability decisions, graph
execution parity against the interpreter, per-backend rejection-reason
reports, buffer-plan liveness tests, Metal/WebGPU output parity for each
newly claimed op family, and real model smoke tests once matmul, norm,
softmax, and quant linear are routed through the graph executor.

### Debug And Bisection Controls

| Variable | Effect |
|---|---|
| `TERMITE_GRAPH_EXECUTOR_STATS=1` | Prints `MultiExecutor` per-run counters: command dispatches, interpreter fallbacks, runtime/cross-device transfers, device-resident outputs, host materialized outputs, boundary output materializations, and Metal graph-plan slot/byte reservations. Real direct CLI paths that bypass `MultiExecutor` (`termite.generate`, `termite.embed`) report an explicit bypass line instead of zero stats. |
| `TERMITE_GRAPH_MODE=1` | Routes ordinary single-device traced graph replay through `MultiExecutor` instead of the interpreter replay path; combine with `TERMITE_GRAPH_EXECUTOR_STATS=1` to see real graph executor stats for the full traced graph. |
| `TERMITE_METAL_DISABLE_Q8_MM=1` | Forces Q8_0 rows >= 9 back onto the verified small-batch/MMV paths for bisection; the default path enables tiled Q8_0 `mul_mm`. |
| `TERMITE_METAL_DISABLE_Q8_PAIR_ACTIVATION_MM=1` | Forces the split gate/up path instead of the default-on fused Q8_0 gate/up activation MM kernel, for bisection. |

## Native Direct Quant Kernel Coverage

Native CPU GGUF quantized matrix multiplies prefer direct quant kernels by
default. The production path is organized behind top-level native graph/runtime
operations so encoder workloads can execute through a ggml-style dispatch model
instead of model-specific one-off kernel calls.

Current coverage includes:

- unified native quant dispatch for single, pair, and triple linear operations
- keyed prepared-layout storage in `QuantizedStorage`, so runtime layouts are
  cached by physical layout instead of one field per optimized quant kernel
- prepared quant weight panels and reusable Q8 activation panels for hot GGUF
  formats, including Q4_K/Q5_K/Q6_K/Q8_K and legacy Q4/Q5/Q8 routes
- packed/grouped projection paths for CLIP/CLAP- and GLiNER-shaped Q/K/V and
  FFN buckets

Grouped direct kernels store cross-weight prepared layouts in
`QuantizedStorage.PreparedGroupCache`. The cache is format-tagged and records
owned partner keys plus layout metadata, so generic weight storage does not grow
Q4/Q5-specific Q/K/V fields. This mirrors ggml's split between generic tensor
storage and backend-prepared layout extras.

Remaining work should stay benchmark-driven:

- Extend graph-style native execution beyond the GLiNER2 and CLIP/CLAP encoder
  paths covered today.
- Broaden Q4_K/Q5_K packed-panel selector promotion only after real-bundle
  benchmarks prove wins for the new row/hidden-size buckets.
- Continue reducing duplicated low-level kernel plumbing inside
  `native_compute.zig` now that prepared layouts share one keyed cache.
- Add longer-run regression benchmarks for pair/triple projection workloads so
  selector changes cannot regress CLIP/CLAP or GLiNER shapes.

### Unified Tensor And Buffer Contract

Static execution needs a backend-neutral tensor descriptor richer than "shape
plus `CT`". It should track:

- dtype and logical shape
- stride/view metadata and optional view source
- storage class: host, Metal, WebGPU, packed quant, constant, runtime input
- quant tensor type and block layout when applicable
- backend residency and transfer requirements
- liveness interval and planned buffer slot

Views should remain metadata whenever possible. Materialization should be an
explicit graph/runtime decision, not a side effect hidden in an individual op.
This is one of the key ggml lessons: keep structure visible long enough for the
backend to avoid copies.

### Memory Planning And Residency

The graph runtime should own intermediate lifetime:

```text
liveness analysis -> reusable logical slots -> backend allocation
```

Backends then map logical slots to real storage:

- native/cblas maps slots to host f32 buffers
- Metal maps slots to `MTLBuffer` ranges or scratch-pool allocations
- WebGPU maps slots to GPU buffers
- packed quant weights stay in backend-native prepared storage

Weights and constants should be uploaded/prepared once per graph/runtime cache
entry when possible. Runtime inputs should transfer once at graph entry, and
requested outputs should transfer once at graph exit. Fallback islands are the
only normal reason to cross device/host boundaries during execution.

### Backend Kernel Registry

Matmul and quantized linear execution should route through one backend-owned
selection surface instead of spreading shape rules across fused model paths:

```text
selectMatmulKernel(M, N, K, dtype, quant_type, phase, layout)
```

The dispatch rules should mirror ggml's practical split:

- `rows == 1`: decode MMV/qgemv path
- small prompt rows: small-batch matvec/matmul path
- larger prompt rows: GEMM/MM path
- dense f32 host: cblas or native Zig
- packed quant weights: direct quant kernel if supported, otherwise fallback

Attention, FFN, MoE, and output-head paths should call the same matmul/quant
matmul primitive. They should not each own private quant decode or kernel
selection logic.

Kernel selection should also carry backend setup costs. cblas has call and
thread-pool overhead, Metal has command-buffer and pipeline costs, and WebGPU has
dispatch and browser queue overhead. For small shapes, a local native kernel can
be faster even when a nominal accelerator supports the op.

### Command Encoding And Fusion

ggml's Metal backend is not just a pile of kernels. It walks graph nodes,
treats metadata ops as no-ops, checks backend support, then encodes supported
ops into command buffers. It also has local fusion/concurrency logic around the
graph walk.

Antfly inference should preserve that idea in backend executors:

- graph/runtime owns partition boundaries and buffer lifetime
- backend executor owns command encoding for a partition
- backend executor may fuse adjacent supported ops when the tensors and ranges
  make that safe
- backend executor may batch command submission for a full partition instead of
  submitting per op
- synchronization happens at partition/output boundaries, not after every node

This is necessary for performance. A graph runtime that calls one Metal/WebGPU
kernel at a time through the eager `ComputeBackend` surface will be correct but
will leave too much performance on the table.

### Partitioned Execution Strategy

The graph runtime should prefer large backend-owned partitions:

```text
target backend partition
  -> explicit fallback host partition where unsupported
  -> transfer edges only at partition boundaries
```

cblas should be modeled as a host kernel provider, not a separate device
runtime. It accelerates dense f32 host partitions through the native backend.
The native interpreter remains the universal correctness fallback and parity
oracle for Metal/WebGPU/cblas behavior.

For Metal and WebGPU, the goal is resident graph execution:

- upload weights/constants once
- upload request inputs once
- execute supported partitions without host materialization
- represent view-compatible ops as metadata
- reuse planned scratch/intermediate buffers
- download only requested outputs

The partitioner should avoid fallback islands for tiny unsupported ops when the
transfer cost would dominate. In those cases it can be faster to keep a larger
region on native/cblas, or to delay offload until a profitable accelerator
region begins. This mirrors ggml's scheduler bias toward backend priority and
buffer compatibility, with explicit split points when copies are unavoidable.

### Practical Rollout

Start with embedding or CLIP/CLAP graphs rather than decoder generation, because
they exercise static graph execution without stateful token scheduling.

1. Lower one traced graph to the canonical op set.
2. Add capability reporting for native, cblas, Metal, and WebGPU.
3. Execute dense `matmul`, elementwise ops, metadata views, norm, and softmax
   through the partition executor.
4. Add planned host/device intermediate buffers.
5. Add quant matmul routing using existing GGUF quant metadata.
6. Use native/cblas interpreter output as the parity oracle for each backend.

Decoder generation can then reuse the same pieces for static phase graphs while
`ModelRuntime` continues to own KV mutation, sampling, phase selection, and
request-level scheduling.

## Quantization Support

Antfly inference's GGUF loader already understands GGML tensor type ids and stores raw
quantized bytes for lazy execution. The remaining work for a quantization type is
to make every execution path either dequantize it correctly or explicitly fall
back to a supported path.

## Q4_1 Status

GGML `Q4_1` is a legacy 4-bit block format:

- 32 values per block.
- 20 bytes per block.
- Layout: fp16 scale `d`, fp16 minimum `m`, then 16 packed bytes containing
  32 unsigned 4-bit values.
- Decode rule: `x = d * q + m`.
- Nibble order follows the other legacy GGML formats: low nibbles decode
  elements `0..15`, high nibbles decode elements `16..31`.

Antfly inference already recognizes the `Q4_1` tensor type and block sizing in
`src/gguf/tensor_types.zig`, but support needs to be present in these layers:

- GGUF codec materialization and row dequantization.
- Native CPU direct quantized matmul.
- Pure-Metal device kernels, including grouped packed-expert MoE
  kernels where the backend supports them.
- WASM/WebGPU quantized matmul if browser inference needs the same model.

Gemma 4 GGUFs from ggml-org/llama.cpp can use legacy `Q4_1` for tensors whose
last dimension does not fit K-quant block requirements. This matters for MoE
expert matrices such as per-expert dimensions that are multiples of 32 but not
256.

## Coverage Roadmap

Full GGML quantization coverage should be treated as a matrix across three
concerns:

1. File compatibility: GGUF can parse the tensor type and compute byte length.
2. Correctness fallback: codec and native CPU paths can produce correct f32
   results without full model-specific fast kernels.
3. Fast execution: pure Metal and WebGPU can execute common linear and
   MoE paths without materializing whole tensors.

Current practical priority:

- Complete `Q4_1` across codec, native, pure Metal, and WebGPU.
- Add the sibling legacy formats `Q5_1` and `Q8_1` next, because the parser
  already recognizes them and their layouts are close to existing `Q5_0` and
  `Q8_0` support.
- Keep K-quants covered for direct linear execution and grouped MoE where tensor
  shapes permit 256-value blocks.

Validation should include synthetic block tests, row dequant tests, native
matmul-vs-dense tests, pure-Metal kernel tests, and at least one real
GGUF smoke test that verifies quantized execution counters are hit.

## Quantization Task List

Antfly inference should prioritize formats by how much real GGUF compatibility they
unlock and how close they are to already-covered paths.

- [x] Finish `Q4_1` across GGUF codec, native CPU, Metal, Antfly inference WebGPU,
  and the embedded WebGPU mirror.
- [x] Add WebGPU `Q4_K` support in Antfly inference and the embedded mirror. `Q4_K` is a
  common K-quant format and already has codec/native coverage.
- [x] Add fast-path parity for legacy `Q5_0`, `Q5_1`, and `Q8_1`, starting with
  WebGPU where missing.
  - [x] Antfly inference WebGPU `Q5_0` direct linear shader and WASM dispatch.
  - [x] Antfly inference WebGPU `Q5_1` direct linear shader and WASM dispatch.
  - [x] Antfly inference WebGPU `Q8_1` direct linear shader and WASM dispatch.
  - [x] Embedded WebGPU mirror and install packaging for `Q5_0`, `Q5_1`, and
    `Q8_1`.
- [x] Add WebGPU parity for `Q2_K`, `Q3_K`, and `Q8_K` so browser execution
  covers the same K-quant family as codec/native paths.
  - [x] Antfly inference WebGPU `Q2_K` direct linear shader and WASM dispatch.
  - [x] Antfly inference WebGPU `Q3_K` direct linear shader and WASM dispatch.
  - [x] Antfly inference WebGPU `Q8_K` direct linear shader and WASM dispatch.
  - [x] Embedded WebGPU mirror and install packaging for `Q2_K`, `Q3_K`, and
    `Q8_K`.
- [x] Add type ids, byte sizing, and CPU dequant correctness for `IQ4_NL` and
  `IQ4_XS`.
- [x] Add fast kernels for the `IQ4_*` formats that show up in real target
  GGUFs.
  - [x] Antfly inference WebGPU direct linear shaders and WASM dispatch for `IQ4_NL` and
    `IQ4_XS`.
  - [x] Embedded WebGPU mirror and install packaging for `IQ4_NL` and
    `IQ4_XS`.
- [x] Add correctness support for lower-bit I-quants: `IQ3_S`, `IQ3_XXS`,
  `IQ2_S`, `IQ2_XS`, `IQ2_XXS`, `IQ1_S`, and `IQ1_M`.
  - [x] Add GGUF type ids, values-per-block, byte sizing, and byte-length
    tests for the lower-bit I-quant layouts.
  - [x] Add CPU dequantization/materialization using the upstream IQ lookup
    tables and bit layouts.
    - [x] Add `IQ2_XXS` codec materialization and row dequantization.
    - [x] Add `IQ2_XS` codec materialization and row dequantization.
    - [x] Add `IQ2_S` codec materialization and row dequantization.
    - [x] Add `IQ3_XXS` codec materialization and row dequantization.
    - [x] Add `IQ3_S` codec materialization and row dequantization.
    - [x] Add `IQ1_S` codec materialization and row dequantization.
    - [x] Add `IQ1_M` codec materialization and row dequantization.
  - [ ] Add native dot-product tests and fast paths where the lower-bit formats
    appear in target GGUFs.
- [ ] Track newer upstream GGML types (`MXFP4`, `NVFP4`, `Q1_0`, `TQ1_0`,
  `TQ2_0`, `I2_S`, `I8_S`, `TL1`, `TL2`) and implement them when a target
  model requires them.
  - [x] Add confirmed GGUF type ids, values-per-block, byte sizing, and
    byte-length tests for `MXFP4`, `NVFP4`, `Q1_0`, `TQ1_0`, and `TQ2_0`.
  - [x] Add codec correctness fallbacks for `MXFP4`, `NVFP4`, `Q1_0`,
    `TQ1_0`, and `TQ2_0`.
  - [x] Confirm BitNet fork ids for `I2_S` = 36, `I8_S` = 37, `TL1` = 38,
    and `TL2` = 39.
  - [x] Add parser metadata and byte sizing for unambiguous BitNet ids
    `I2_S`, `I8_S`, and `TL1`.
  - [x] Add slow CPU materialization and row dequantization for `I2_S`.
  - [x] Add dialect-aware parsing for `TL2`, because BitNet fork id 39
    conflicts with upstream ggml-org `MXFP4`.
  - [ ] Prioritize fast kernels only for newer formats found in target GGUFs.
- [x] Parse non-quant scalar tensor types (`I8`, `I16`, `I32`, `I64`, `F64`) for
  file compatibility, without treating them as matmul fast-path priorities.
  - [x] Add GGUF type ids, dense byte sizing, and byte-length tests for scalar
    tensor metadata.
  - [x] Add native runtime dtypes and GGUF materialization for `I8`, `I16`,
    `I32`, `I64`, and `F64`.


> **Relocated:** The bullet-by-bullet implementation history for the Metal,
> quant-matmul, and WebGPU partition executors that previously lived here
> (320 lines) is preserved verbatim in
> [work-log/completed/inference/ggml-graph-execution-history.md](../../../work-log/completed/inference/ggml-graph-execution-history.md).
> Durable decisions from it are in Metal Partition Executor, Quant Matmul
> Routing, WebGPU Partition Executor, and Debug And Bisection Controls above.

## Open work

- Make graph execution the default hot path where it is at least as reliable
  and fast as the direct runtime path. Generation keeps the eager/direct
  runtime default unless `TERMITE_GRAPH_MODE`, an explicit compiled partition
  backend, or a graph-runtime option selects the graph path; embedding
  similarly reports a direct-runtime bypass when no graph runtime strategy is
  requested.
- Broaden real-model graph-mode smokes for Metal and WebGPU. Focused graph
  executor and browser smokes cover the promoted command families, but full
  model layouts should be exercised under `TERMITE_GRAPH_EXECUTOR_STATS=1` so
  regressions show up as unexpected interpreter fallbacks, boundary
  materializations, or direct-runtime bypasses.
- Continue the Metal FFN precision migration: command plans distinguish f32
  scratch from f16 FFN intermediates, and Q8_0 pair-activation dispatch
  prefers the fused pair kernel over the split simdgroup fallback. The first
  executable kernel slice adds a Q8_0 FFN gated activation MM kernel that
  writes f16 and a matching f16-input Q8_0 down projection MM kernel that
  writes f32 for the existing residual/RMS epilogue. The planner selects this
  route automatically for supported descriptors; runtime descriptor/pipeline
  checks fail closed when a specific shape, quant family, or kernel variant is
  unsupported.
- Tune profitability thresholds from benchmark data. The current native/cblas,
  Metal, and WebGPU thresholds are conservative constants; existing bench
