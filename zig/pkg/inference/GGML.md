# GGML Reference And Quantization Support

## Scope

This document has three jobs:

- Track GGUF/GGML tensor format compatibility.
- Record the ggml/llama.cpp execution shape Antfly inference should follow where it is
  useful.
- Document the graph-execution partitioning and backend-executor design that
  implements that shape (originally planned in a separate document; see
  [GGML_PLAN.md](GGML_PLAN.md)).

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
partition/output boundaries. `MetalCompute` can still opportunistically route
eager interpreter ops through resident Metal runtime slots as a
correctness/performance bridge (used by CLIPCLAP-style paths); new operations
should be added as backend primitives the partition executor can call rather
than as model-specific eager helpers. See History below for the detailed
implementation log.

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
a generic dense primitive. See History below for the detailed implementation
log.

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

## History

The graph-execution work in "Graph Execution: Partitioning And Backend
Executors" above landed as a sequence of implementation slices. This section
keeps the detailed, bullet-by-bullet record for each subsystem; the section
above is the current-state summary.

### Metal Partition Executor

- added `src/graph/metal_partition_executor.zig`
- routed Metal graph partitions through `PartitionExecutor` instead of the
  generic per-node fallback in `MultiExecutor`
- extended `PartitionExecutor.ExecutionContext` with the partition plan so
  backend executors can request their `PartitionBufferView`
- Metal executor requires and validates the buffer plan, including
  partition-local slots and boundary outputs
- materializes partition runtime inputs onto the target backend before command
  execution
- uses backend frame hooks (`decoderRuntimeBeginFrame` /
  `decoderRuntimeSubmitAndWaitFrame`) when available, with cancellation on
  errors
- evaluates only partition boundary outputs after submission, avoiding
  per-node synchronization in the executor loop
- keeps existing Metal eager backend kernels as the initial op implementation
  surface while moving orchestration to the graph partition executor
- derives a Metal graph-plan slot table from the physical allocations
  referenced by the partition view
- added a backend graph-plan reservation hook and wired Metal to reserve
  persistent runtime `MTLBuffer` graph-plan slots before partition execution
- added executor-local command dispatch for metadata reshape/transpose,
  runtime activation, runtime add, multiply, negation, and softmax/log-softmax
  before falling back to interpreter execution
- added backend-owned dynamic runtime slot hooks so graph executors can prepare
  linear, layer-norm, and RMS-norm slots without managing Metal slot IDs
- added Metal executor command dispatch for dense linear/no-bias linear, layer
  norm, and RMS norm through the backend op surface; MetalCompute still takes
  resident runtime fast paths for device-backed tensors, while host tensors use
  the safe native/Accelerate fallback instead of the low-level host Metal norm
  ABI
- aligned Zig's layer-norm runtime slot capacity with the Objective-C Metal
  runtime constant so dynamic slot allocation does not probe invalid slots
- Metal partition boundary transfers explicitly make f32 tensors
  device-resident on the target Metal backend, so runtime inputs entering a
  Metal partition do not depend on the eager upload environment flag for
  residency
- added a row-wise Metal softmax/log-softmax runtime primitive and routed
  `MetalCompute.primSoftmax` / `primLogSoftmax` through it when the input is
  already device-backed
- added resident executor coverage for a native-to-Metal input transfer followed
  by `linear -> silu -> add -> softmax` in one decoder runtime frame, with the
  final graph output still device-resident before host readback
- generalized the resident materialization policy into `MultiExecutor`: Metal
  partition boundary outputs stay device-resident until the final caller
  readback, and shared cross-device transfers to Metal explicitly upload into
  private Metal buffers
- added partition execution counters for command dispatches, interpreter
  fallbacks, runtime/cross-device transfers, device-resident outputs, host
  materialized outputs, and boundary output materializations
- `TERMITE_GRAPH_EXECUTOR_STATS=1` prints those counters from the shared
  `MultiExecutor` path, so real graph/model executions can expose whether a
  run stayed resident or silently fell back/materialized
- expanded Metal command dispatch coverage for primitive unary ops, subtract,
  divide, less-than, where-select, and last-dimension slice lowering, so common
  transformer-side arithmetic no longer has to fall through the interpreter
  path when backend primitives already exist
- added a resident primitive-chain smoke test and a `MultiExecutor` smoke test
  that assert zero interpreter fallbacks, zero boundary materializations, and
  device-resident graph outputs before explicit host readback
- added resident `less_than -> where_select` coverage for masking-style
  elementwise chains with scalar constants broadcast on the Metal command path,
  also asserting zero interpreter fallbacks and zero host materialized outputs
- replaced full-device scalar expansion for Metal `sub`, `div`, `less_than`,
  and `where_select` with scalar-aware runtime dispatch flags, so scalar
  constants can stay as one-element device tensors while the command writes the
  full resident output
- added a row-wise Metal last-dimension reduction primitive for f32
  `reduce_sum`, `reduce_max`, and `reduce_mean`; MetalCompute keeps
  device-backed last-axis reductions resident instead of routing them through
  host fallback, and the partition executor has regression coverage proving all
  three reductions stay device-backed before final readback
- added resident attention-glue coverage for `less_than -> where_select -> add
  mask bias -> softmax -> linearNoBias`, with scalar mask constants, zero
  interpreter fallbacks, and device-resident output before readback
- graph executor stats include Metal graph-plan slot and byte reservations,
  making buffer-plan handoff observable from executor tests and
  `TERMITE_GRAPH_EXECUTOR_STATS=1`
- real Metal model smokes were run under the Metal debug wrapper for Gemma
  generation and CLIPCLAP text embedding; both passed validation with no
  diagnostic reports, and neither emitted `graph_executor_stats`, confirming
  those CLI paths still use direct model/runtime executors rather than the
  shared `MultiExecutor`
- `TERMITE_GRAPH_EXECUTOR_STATS=1` reports an explicit bypass line for those
  real direct paths (`termite.generate` and `termite.embed`) when they do not
  request graph execution, so smokes distinguish "graph executor produced zero
  stats" from "this CLI path intentionally bypassed `MultiExecutor`"
- added a resident Metal last-dimension broadcast primitive for f32
  `[rows, 1] -> [rows, dim]` and identity last-dim expansion; decomposed softmax
  has regression coverage for
  `reduceMax -> broadcast -> sub -> exp -> reduceSum -> broadcast -> div`
  staying device-backed with zero interpreter fallbacks
- admitted RoPE into the conservative Metal eager graph capability set and
  routed `fused_rope` through the Metal partition command path, matching the
  interpreter's attention-aware position-offset handling; focused coverage
  proves a resident RoPE output stays device-backed with no interpreter
  fallback
- graph-mode generation routes ordinary single-device traced graph replay
  through `MultiExecutor` instead of falling back to the interpreter replay
  path; `TERMITE_GRAPH_MODE=1 TERMITE_GRAPH_EXECUTOR_STATS=1` on Gemma/native
  emits real graph executor stats for the full traced graph
- graph-mode generation skips the direct live whole-model executor when graph
  mode is explicitly requested, so graph-mode smokes no longer silently exit
  before the graph executor can run
- added resident attention-block composition coverage for
  `Q/K/V linearNoBias -> reshape -> transpose -> transpose -> RoPE -> add ->
  softmax -> add V -> output linearNoBias`, with zero Metal interpreter
  fallbacks and a device-resident output before readback. Transpose is no
  longer the attention-layout promotion blocker for this coverage; the
  remaining gap is broader real-model layout coverage and performance tuning
  rather than this primitive's residency.

### Quant Matmul Routing

- Metal partition capability consults the shared quant matmul planner for
  packed-weight linear nodes instead of rejecting all packed quant inputs
- supported packed formats/row buckets can stay in Metal partitions with a
  persisted `OperatorPlan`; unsupported formats or storage combinations still
  reject with `missing_quant_kernel` instead of entering an accidental dense
  fallback
- dense activation plus packed quant weight is the accepted matmul shape; packed
  activation inputs still do not enter quant matmul directly, but Metal has
  shared row/copy operators for supported packed 2D tensors. Quant embedding
  lookup, Metal tensor materialization, and `takeRows` route through those
  prepared-slot `get_rows` / `cpy_q_to_f32` ops before falling back to host
  diagnostics paths
- `CapabilityDecision` can carry the selected shared `OperatorPlan`, and
  `CapabilityDiagnostics` reports accepted operator counts so partition tests
  can prove that a Q8_0 prompt linear is admitted because it maps to
  `mul_mm`, not merely because Metal accepted the node generically
- `PartitionPlan` persists a per-node optional `OperatorPlan` selected by the
  winning backend capability decision; non-capability planners fill this table
  with nulls, while Metal Q8_0 linear nodes expose the concrete
  `mul_mv`/`mul_mv_ext`/`mul_mm` plan for later command dispatch
- `ComputeBackend` exposes optional planned linear hooks
  (`linearPlanned`/`linearNoBiasPlanned`) with fallback wrappers, so graph
  executors can pass the selected `OperatorPlan` into backend dispatch without
  changing ordinary model code
- the Metal partition executor consumes the persisted plan for planned
  quantized linear nodes by validating rows, dimensions, operator, and packed
  weight format, then dispatching through `linearWithPlan`/`linearNoBiasWithPlan`
- Metal implements the planned linear hooks as validating wrappers over its
  existing quant-aware linear path
- the raw Metal Q8_0 provider path has a planned-dispatch entry point that
  forwards the selected `mul_mv`/`mul_mv_ext`/`mul_mm` dispatch byte into the
  shared Q8 command encoder; the provider loads the same MMV, small-batch, and
  MM pipelines used by the decode runtime
- the Q8_0 `mul_mm` kernel uses a conservative 16-output by 8-row Metal
  reduction tile; validator coverage with varied activation columns catches
  the old simdgroup-matrix under-accumulation pattern
- the fused Q8_0 gate/up activation MM kernel uses the same reduction tile and
  is enabled by default; `TERMITE_METAL_DISABLE_Q8_PAIR_ACTIVATION_MM=1` forces
  the split gate/up path for bisection
- `TERMITE_METAL_DISABLE_Q8_MM=1` forces rows >= 9 back onto the verified
  small-batch/MMV paths for bisection; the default path enables tiled Q8_0 MM
- runtime tests assert the plain linear, QKV, and fused gate/up rows >= 9 paths
  increment the expected Q8_0 MM dispatch-family counters
- `decoderRuntimeApplyLinearPair` accepts batched 2D inputs instead of
  decode-only rows, and Q8_0 paired projections route through the runtime pair
  encoder so rows >= 9 are counted under the pair-family tiled MM path; a
  focused runtime test checks the pair MM counter and matches two separate
  linear calls
- partition tests cover all Q8_0 row buckets at the backend decision boundary:
  decode `mul_mv`, small prompt `mul_mv_ext`, and larger prompt `mul_mm`
- added a Metal partition executor smoke test that feeds a Q8_0 packed weight
  through the normal backend weight store, seeds the partition descriptor so the
  Q8_0 operator plan is present, and verifies the graph executor's planned
  quantized `linearNoBias` command path
- fixed the shared quant support table so Q4_1 has one support entry with row
  and copy operator coverage instead of a duplicate switch case
- hardened planned Q8_0 graph-executor coverage across all row buckets:
  decode `mul_mv`, small-prompt `mul_mv_ext`, and prompt `mul_mm` are asserted
  at the persisted `OperatorPlan` boundary and then executed through the Metal
  partition executor
- added non-Q8 quant routing diagnostics at the capability boundary. Current
  coverage expects Q4_0, Q4_1, and Q5_K planned row buckets to route through
  the graph executor where kernels exist, while truly unsupported formats still
  report `missing_quant_kernel`
- packed quant metadata survives graph metadata views, so reshape/slice/etc.
  cannot erase the quant format and accidentally route a packed tensor into a
  generic dense primitive; unsupported consumers reject with
  `missing_quant_kernel` unless an explicit quant row/matmul plan exists
- promoted Q4_0, Q4_1, and Q5_K planned graph matmul buckets beyond decode:
  `mul_mv_ext` and `mul_mm` admit explicit packed-weight execution through the
  Metal quant descriptor, using each format's direct packed kernel instead of
  dequantizing weights or falling back to generic dense primitives
- graph executor stats include `planned_commands`, incremented when a Metal
  partition command consumes a persisted `OperatorPlan`; this makes the shared
  direct-runtime/graph-executor quant planning contract visible in graph tests
- hardened the native oracle for the same packed formats: native quant linear
  exposes test-only dispatch counters, and bucket coverage for rows 1/4/9/64
  asserts Q4_0, Q4_1, Q4_K, Q5_K, and Q8_0 route through packed native kernels
  without entering dense-dequant SGEMM fallback
- extended the native oracle to Q/K pair and Q/K/V triple projections for rows
  1/4/9, asserting those CLIP/CLIPCLAP attention paths avoid dense-dequant
  fallback and match the separate linear outputs
- added a focused CLIPCLAP kernel bench mode for native quant buckets
  (`--only-native-quant-buckets`) covering linear, pair, and triple paths for
  Q4_0, Q4_1, Q4_K, Q5_K, and Q8_0 against f32/separate-call baselines across
  rows 1/4/9/64
- promoted the Q4/Q5_K two-row prepared Q8_K activation path to consume
  prepared panel8 blocks when aligned, while preserving the existing NR=4 tail
  path for partial/unaligned output ranges
- fixed the prepared Q4/Q5_K and Q6_K parallel worker contexts to carry
  `prepared_panel8_packed_bytes`; column-parallel and row-parallel dispatch
  preserve the same panel8 kernel path as serial and pair/triple execution
- added a fused prepared-panel8 path for Q4_K/Q5_K pair and triple projections;
  Q/K and Q/K/V traverse the shared Q8_K activation rows once per output tile
  and write all participating projections in the same loop, while Q6_K and
  non-panel8 cases keep the existing split fallback
- focused bucket timings after the fused path show Q4_K/Q5_K pair/triple
  improvements on CLIPCLAP-shaped rows: Q4_K pair rows 64 improved to about
  1.50x over the separate-call baseline, Q4_K triple rows 64 to about 1.63x,
  Q5_K pair rows 64 to about 1.45x, and Q5_K triple rows 64 to about 1.55x
- promoted `fused_embedding_lookup` into the Metal graph command path, so
  Gemma-style graph partitions can consume Metal-resident embedding weights
  without forcing a native boundary
- promoted `concat_prim` into the Metal graph command path with two resident
  cases: dense device-backed concat for activation tensors, and packed
  axis-0 quantized row concat for parameter/expert weights; packed concat
  preserves both `quantized_storage` and `runtime_quantized_storage`, so the
  following linear preparation continues to see native Metal quant metadata
  instead of dequantizing through f32
- fixed graph-plan reservation for large resident parameter partitions:
  transfer-in buffers are no longer double-counted as scratch reservations, and
  local graph-plan reservations keep the largest slots within the Metal runtime
  slot cap instead of aborting execution when a partition has many live values
- cross-device transfers use the graph's static output shape when available
  instead of requiring backend tensor-shape metadata; this keeps scalar/native
  constants transferable after partition boundaries and avoids shape-probe
  failures on constant buffers
- added opt-in debug tracing for partition nodes, transfer stages, concat path
  selection, and graph-plan reservation sizing to make Metal crash bundles show
  the exact partition/node/stage reached without changing normal execution
- split the Metal debug wrapper validation modes: default remains full API plus
  shader validation, while `--api-validate` keeps `MTL_DEBUG_LAYER=1` without
  `MTL_SHADER_VALIDATION=1` for environments where shader validation prevents
  device creation before the model path runs

### WebGPU Partition Executor

- added `src/graph/webgpu_partition_executor.zig`
- routed `.webgpu` partitions through the named partition-executor path in
  `MultiExecutor`
- replaced the original type alias with a real `WebGpuPartitionExecutor` that
  requires and validates the buffer-plan partition view before delegating
  through the native partition executor
- added coverage for the WebGPU executor entry point proving it uses the shared
  partition executor path, preserves stats plumbing, and returns correct output
  through the conservative delegate path
- promoted the first WebGPU executor command family: simple elementwise
  add/multiply/unary nodes execute inside `WebGpuPartitionExecutor` after
  buffer-plan validation, increment backend command stats, and avoid the native
  delegate path for that partition shape
- promoted the first transformer dense WebGPU command chain: dense
  `linear`/`linearNoBias`, `rmsNorm`, `layerNorm`, and `gelu` dispatch directly
  through `WebGpuPartitionExecutor`, with native-oracle coverage for a
  linear -> norm -> activation -> linear -> norm partition and command stats
- added a conservative WebGPU graph capability decision that mirrors the
  promoted executor command surface, rejects unsupported packed quant/projected
  linear shapes, and only claims host inputs when the shape is large enough to
  amortize transfer overhead; explicit `.webgpu` partition targets use this
  decision hook instead of `supportsAll`
- promoted WebGPU mask glue commands for `less_than` and `where_select`, giving
  the executor a direct path for scalar-broadcast masking chains used around
  attention softmax
- promoted WebGPU view/movement commands for `reshape`, `slice`, and
  `concat_prim`, so dense/mask/reduction chains can remain in a WebGPU
  partition across shape changes instead of materializing back to host/native
- added WebGPU graph-plan reservation from buffer-plan allocations: the
  partition executor derives backend slot reservations from local tensor
  allocations, and the WASM/WebGPU backend reserves or grows GPU buffers through
  the existing `reserveGraphPlanSlots` hook
- added a WebGPU command encoder classification layer for the claimed shader
  families, covering elementwise/mask/unary/view/copy/reduction/softmax/dense
  matmul/norm/GELU commands before dispatching through the backend's WebGPU
  shader-backed WASM ops
- promoted browser WGSL/import coverage for additional WebGPU shader families:
  broadcast-compatible `sub`/`div`/`less_than`, scalar-mask `where_select`,
  primitive unary ops, row-wise `softmax`/`log_softmax`, rank <= 8
  arbitrary-axis `reduce_sum`/`reduce_max`/`reduce_mean`, and rank <= 8
  `broadcast_in_dim`
- audited the claimed WebGPU graph command surface against the browser extern
  layer and closed the elementwise gaps: broadcast-compatible `add` and `mul`
  have WGSL entry points, direct/worker imports, and browser smoke parity.
  Biased `fused_linear` applies bias as a resident broadcast add after the
  dense or packed-quant matmul instead of downloading the matmul output first.
- added a command-classification breadth test so each claimed WebGPU graph op is
  either mapped to a concrete executor command family or explicitly treated as
  the runtime-only `fused_from_float32` placeholder
- promoted packed quant `linearNoBias` admission into WebGPU graph capability:
  supported browser quant shader formats carry a persisted shared
  `quant_matmul` operator plan through partitioning and WebGPU executor command
  dispatch
- promoted planned WebGPU quant projection families for transformer graph
  blocks: grouped QKV/GQA-style `fused_linear_no_bias`, paired
  `fused_linear_no_bias_pair`, and the `fused_to_float32` pair side-channel
  stay on the WebGPU command executor when projection metadata is shape-valid;
  focused executor tests assert planned dispatch counts, no interpreter
  fallback, and resident graph outputs for grouped and pair chains
- added WebGPU transformer-block parity coverage at both executor and browser
  levels: the graph executor has a planned quant projection block smoke
  covering grouped projection, RMS norm, GELU, residual add, softmax, and output
  projection, while the Chromium smoke runs the same style of q4/RMS/GELU/
  residual/softmax/projection shader chain with no intermediate downloads
  before the final parity readback
- WebGPU/WASM GPU-producing graph ops keep the GPU buffer as the source of
  truth and defer host downloads until `toFloat32`/export, so chained WebGPU ops
  can reuse resident buffers instead of materializing after every command
- no known claimed WebGPU graph op currently depends on a missing browser
  extern; future op claims should land with the matching WGSL/import and
  executor classification coverage in the same slice

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
  harnesses should be used to compare graph executor, direct runtime, and
  native fallback behavior across decode, prompt, embedding, and quant row
  buckets.
- Promote additional WebGPU graph families only when the complete chain is
  present. Browser attention shaders/imports exist, and WebGPU can be selected
  for compiled partition execution in Wasm/WebGPU builds, but graph capability
  currently claims only the promoted resident command surface; attention and
  MoE/`mul_mat_id`-style graph promotion remain explicit follow-up work.
- Keep this document reconciled with implemented slices: revise historical
  fallback notes when later work promotes the same operator family.
- Extend graph-style native execution beyond the GLiNER2 and CLIP/CLAP encoder
  paths covered today.
- Broaden Q4_K/Q5_K packed-panel selector promotion only after real-bundle
  benchmarks prove wins for the new row/hidden-size buckets.
- Continue reducing duplicated low-level kernel plumbing inside
  `native_compute.zig` now that prepared layouts share one keyed cache.
- Add longer-run regression benchmarks for pair/triple projection workloads so
  selector changes cannot regress CLIP/CLAP or GLiNER shapes.
- Add native dot-product tests and fast paths where the lower-bit I-quant
  formats (`IQ3_S`, `IQ3_XXS`, `IQ2_S`, `IQ2_XS`, `IQ2_XXS`, `IQ1_S`, `IQ1_M`)
  appear in target GGUFs.
- Prioritize fast kernels for newer upstream GGML types (`MXFP4`, `NVFP4`,
  `Q1_0`, `TQ1_0`, `TQ2_0`, `I2_S`, `I8_S`, `TL1`, `TL2`) only when a target
  model requires them.
