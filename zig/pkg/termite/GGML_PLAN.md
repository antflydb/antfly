# GGML-Style Graph Execution Plan

This plan turns the design in [GGML.md](GGML.md) into implementation phases.
The goal is to copy ggml's performance shape, not its exact internal layout:
structural graph, backend-aware buffer planning, profitable partitioning, and
batched backend-owned execution.

## Target Architecture

```text
frontend/tracing
  -> ml.graph.Graph
  -> canonical lowered op set
  -> capability + profitability partitioning
  -> liveness/buffer plan
  -> backend partition executor
  -> backend kernel picker and command encoder
```

Termite should keep graph policy in `src/graph/`. Backend modules should own
device buffers, command submission, kernel selection, and packed-weight
preparation.

## MoE Packed Expert Alignment

Status: packed MoE now uses an explicit `mul_mat_id`-style contract across the
generic compute API, native reference path, Metal native-quant provider, and
Metal grouped kernels.

`llama.cpp` keeps Gemma4 MoE weights as 3D packed tensors and applies selected
expert IDs through `ggml_mul_mat_id`. The important layouts are:

- `ffn_gate_up_exps.weight`: `[hidden, 2 * expert_ff, expert_count]`
- `ffn_down_exps.weight`: `[expert_ff, hidden, expert_count]`

The production target is a Termite `mul_mat_id`-style primitive where routed
MoE execution receives the full packed tensor and selected expert IDs. The
current grouped MoE path follows that contract for packed GGUF weights at the
weight-registration, graph dispatch, native reference, Metal provider, and Metal
kernel-selection levels.

Implemented slice:

- added shared GGUF packed-expert layout metadata for row-major legacy views and
  ggml expert-last views
- native packed quantized linear accepts `expert_axis == 2` when the input axis
  is the first GGUF dimension
- dense packed-expert materialization supports the same layout, including
  fused gate/up row offsets
- Metal packed-weight slicing uses the shared offset calculation
- Gemma4 packed GGUF MoE registers one lazy weight per layer/projection
  (`packed.w1`, `packed.w2`, `packed.w3`) instead of one per expert/projection
- grouped MoE lookup prefers the packed projection weights and keeps legacy
  per-expert names as fallback
- GGUF inspection treats the synthetic packed projection names as required
  weights, so packed models report complete required tensor coverage
- Metal grouped dispatch can index full packed storage directly for
  `expert_axis == 2` instead of staging selected expert slabs
- generic `ComputeBackend` exposes `mulMatId`, with `moeLinearNoBias` retained
  as a compatibility wrapper
- Metal native-quant providers expose `mulMatId`, and the Metal provider routes
  grouped kernels through that contract
- native CPU has a `mulMatId` path covering legacy 2D packed expert weights and
  ggml expert-last 3D weights; quantized GGUF weights stay packed and execute
  selected-row block dot products instead of full f32 expert materialization
- tests cover both existing row-major packed views and ggml expert-last Q8_0
  views

## Phase 1: Rich Capability Decisions

Status: implemented for graph partitioning; backend-specific tuning continues in later phases.

Current partitioning is op-tag based. Phase 1 makes the partitioner ask a richer
question:

```text
Can this backend execute this graph node correctly?
Should this backend execute it for this shape/storage state?
Why was it accepted or rejected?
```

Implementation:

- add `CapabilityQuery`
- add `CapabilityDecision`
- keep the existing `supports(OpCode)` callback as a compatibility adapter
- make assignment require both `can_execute` and `should_execute`
- add diagnostic reason buckets such as `unsupported_op`, `unprofitable_shape`,
  `wrong_storage`, and `missing_quant_kernel`
- pass inferred tensor descriptors into capability decisions
- record capability rejection diagnostics for partition reports
- use estimated cost to break ties between same-priority backends
- add native, cblas/Accelerate-style, and Metal eager graph decision helpers

This lets cblas say "I can run this matmul, but it is too small to be worth the
call/thread-pool overhead" and lets Metal/WebGPU reject supported ops when input
residency would make the transfer unprofitable.

## Phase 2: Tensor And Storage Descriptors

Status: implemented for graph partition inputs and static descriptor inference.

Add a graph-runtime tensor descriptor in `backend_contracts.zig` or a nearby
graph-owned module:

```text
dtype
shape
stride/view metadata
storage class
quant format
view source
residency
```

Storage classes should distinguish at least:

- host f32
- host packed quant
- Metal buffer
- WebGPU buffer
- runtime input
- constant
- metadata view

Implementation:

- `TensorStorageClass`, `TensorStrides`, and `TensorDesc`
- dense stride inference
- per-node descriptor table construction
- seed descriptor validation for externally supplied residency/quant metadata
- parameter, constant, reshape, transpose, slice, broadcast, range, and
  shape-of descriptor inference
- descriptor shape/dtype validation against graph node outputs
- partition wiring so every `CapabilityQuery` can inspect descriptor metadata

This is the missing equivalent of ggml's tensor metadata plus backend buffer
type. Without it, the partitioner cannot reason about transfer cost.

## Phase 3: Profitability-Aware Partitioning

Status: initial profitability and storage heuristics implemented for native,
cblas/Accelerate-style graph, and Metal eager graph decisions.

Extend the rich capability path to include shape and storage heuristics.

Initial policies:

- native accepts all supported graph ops as the correctness fallback
- cblas is a host kernel provider and only claims profitable dense f32 GEMMs
- Metal/WebGPU claim device partitions only when the following region is large
  enough to amortize upload/dispatch costs
- metadata/view ops stay with the storage owner where possible

Partition reports should aggregate rejection reasons:

```text
unsupported_op
unprofitable_shape
wrong_storage
missing_quant_kernel
backend_disabled
```

Implemented slice:

- descriptor seeds are expanded into a full inferred descriptor table before
  partitioning, so seeded residency/quant metadata propagates into capability
  decisions for downstream nodes
- dense tensor byte estimates and simple compute/transfer estimates
- cblas/Accelerate-style graph decisions claim only profitable dense f32 host
  GEMM/linear nodes
- Metal eager graph decisions keep parameters and metadata/view ops with their
  resident storage owner where possible
- Metal accepts tiny already-resident device chains but rejects small host input
  islands that would not amortize upload/dispatch overhead
- partition tests cover tiny cblas rejection, large cblas acceptance,
  device-resident Metal chains, small host-to-Metal rejection, and quant fallback

## Phase 4: Native Partition Executor

Status: implemented and hardened for the native/cblas baseline path.

Implement the first real graph `PartitionExecutor` for native/cblas. It should
execute a full partition using the existing interpreter/backend operations, but
through the same executor path that Metal and WebGPU will use later.

This gives a correctness baseline for:

- partition executor plumbing
- output value filling
- transfer boundaries
- future buffer-plan integration

cblas remains inside native execution. It should not become a separate device
runtime.

Implemented slice:

- added `src/graph/native_partition_executor.zig`
- extended `PartitionExecutor.ExecutionContext` with graph/runtime state needed
  by full-partition execution
- native/graph partitions without compiled executors now run through the
  `PartitionExecutor` path
- reused interpreter node dispatch, backend vtable ops, runtime input transfer,
  donation-aware liveness freeing, attention layer state, and pair outputs
- kept cblas/Accelerate inside native execution rather than modeling it as a
  separate device runtime
- added a backend-op regression test for native partition execution
- added cblas/Accelerate-shaped linear coverage through the native executor
- added native/graph multi-executor coverage through the executor path
- added runtime-input transfer coverage across devices
- added owned executor lifecycle coverage

## Phase 5: Graph Buffer Plan

Status: executor-facing buffer plan implemented for Phase 5.

Add `src/graph/buffer_plan.zig`.

Inputs:

- graph
- partition plan
- tensor descriptors
- output nodes

Outputs:

- logical buffer slots
- liveness intervals
- storage class per slot
- transfer edges

Backends map logical slots to concrete allocations:

- native/cblas: host buffers
- Metal: `MTLBuffer` ranges or scratch-pool slots
- WebGPU: GPU buffers
- quant weights: prepared packed storage

Implemented slice:

- added `src/graph/buffer_plan.zig`
- creates one logical slot per graph value with node-to-slot mapping
- records liveness intervals from graph consumers and output retention
- preserves tensor descriptor shape/storage metadata on every slot
- represents reshape/transpose/view-style values as metadata slots pointing to
  source slots
- marks reusable allocation slots when they are not graph outputs or borrowed
  runtime/constant storage
- records cross-partition transfer edges from `Partition.external_inputs` with
  source/target partition, backend, slot, and storage metadata
- assigns physical allocation IDs to materialized tensor slots
- reuses physical allocations for same-backend/storage tensors when lifetimes
  do not overlap and the allocation has enough capacity
- keeps view slots attached to their source allocation instead of allocating
  separate storage
- assigns transfer-edge target allocations with byte sizes and target
  backend/storage metadata
- reports allocation byte totals by backend/storage for backend memory planning
- records explicit logical slot roles for runtime inputs, constants, partition
  inputs/outputs, graph outputs, transfer sources, and metadata views
- exposes partition-local buffer views with local/input/output/transfer
  boundary roles plus incoming and outgoing transfer edges
- validates node-slot coverage, descriptor shape consistency, allocation
  attachment, view allocation aliasing, graph-output liveness, transfer
  endpoints, and transfer target allocations
- threads the buffer plan through `PartitionExecutor.ExecutionContext`
- builds and validates a graph buffer plan in `MultiExecutor` before invoking
  partition executors

## Phase 6: Metal Partition Executor

Status: initial partition executor, graph-plan scratch reservation, backend-op
command dispatch, and focused Metal validation implemented.

Implement resident Metal execution for profitable partitions.

The executor should:

- prepare/upload constants and weights once per runtime cache entry
- allocate planned intermediate buffers
- treat view-compatible ops as metadata
- encode all supported partition commands before submit where possible
- synchronize only at partition/output boundaries
- download only requested outputs

Start with:

- dense matmul (single-row decoder linear first)
- elementwise arithmetic
- reshape/view metadata
- layer norm / rms norm
- softmax

Then add quant matmul through the shared quant planner.

Interim bridge:

- `MetalCompute` may opportunistically route eager interpreter ops through
  resident Metal runtime slots when inputs are already device-backed. This is
  allowed as a correctness/performance bridge for model paths such as CLIPCLAP,
  but it is not the final execution model.
- These eager bridges should use the same primitives as the future partition
  executor: prepared linear slots, quantized matmul descriptors, layer/rms norm
  slots, attention kernels, and device-resident outputs.
- Do not grow model-specific Metal helper APIs for every architecture. If an
  eager bridge needs a new operation, prefer adding the backend primitive that
  the Phase 6 partition executor will call later.
- Once a graph partition executor owns the same op family, the eager bridge
  should become fallback coverage rather than the primary hot path.

Implemented slice:

- added `src/graph/metal_partition_executor.zig`
- routed Metal graph partitions through `PartitionExecutor` instead of the
  generic per-node fallback in `MultiExecutor`
- extended `PartitionExecutor.ExecutionContext` with the partition plan so
  backend executors can request their `PartitionBufferView`
- Metal executor requires and validates the Phase 5 buffer plan, including
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
- derives a Metal graph-plan slot table from the Phase 5 physical allocations
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
- Metal partition boundary transfers now explicitly make f32 tensors
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
  partition boundary outputs now stay device-resident until the final caller
  readback, and shared cross-device transfers to Metal explicitly upload into
  private Metal buffers
- added partition execution counters for command dispatches, interpreter
  fallbacks, runtime/cross-device transfers, device-resident outputs, host
  materialized outputs, and boundary output materializations
- `TERMITE_GRAPH_EXECUTOR_STATS=1` now prints those counters from the shared
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
  `reduce_sum`, `reduce_max`, and `reduce_mean`; MetalCompute now keeps
  device-backed last-axis reductions resident instead of routing them through
  host fallback, and the partition executor has regression coverage proving all
  three reductions stay device-backed before final readback
- added resident attention-glue coverage for `less_than -> where_select -> add
  mask bias -> softmax -> linearNoBias`, with scalar mask constants, zero
  interpreter fallbacks, and device-resident output before readback
- graph executor stats now include Metal graph-plan slot and byte reservations,
  making Phase 5 buffer-plan handoff observable from executor tests and
  `TERMITE_GRAPH_EXECUTOR_STATS=1`
- real Metal model smokes were run under the Metal debug wrapper for Gemma
  generation and CLIPCLAP text embedding; both passed validation with no
  diagnostic reports, and neither emitted `graph_executor_stats`, confirming
  those CLI paths still use direct model/runtime executors rather than the
  shared `MultiExecutor`
- `TERMITE_GRAPH_EXECUTOR_STATS=1` now reports an explicit bypass line for
  those real direct paths (`termite.generate` and `termite.embed`) when they do
  not request graph execution, so smokes distinguish "graph executor produced
  zero stats" from "this CLI path intentionally bypassed `MultiExecutor`"
- added a resident Metal last-dimension broadcast primitive for f32
  `[rows, 1] -> [rows, dim]` and identity last-dim expansion; decomposed softmax
  now has regression coverage for
  `reduceMax -> broadcast -> sub -> exp -> reduceSum -> broadcast -> div`
  staying device-backed with zero interpreter fallbacks
- admitted RoPE into the conservative Metal eager graph capability set and
  routed `fused_rope` through the Metal partition command path, matching the
  interpreter's attention-aware position-offset handling; focused coverage
  proves a resident RoPE output stays device-backed with no interpreter
  fallback
- graph-mode generation now routes ordinary single-device traced graph replay
  through `MultiExecutor` instead of falling back to the interpreter replay
  path; `TERMITE_GRAPH_MODE=1 TERMITE_GRAPH_EXECUTOR_STATS=1` on Gemma/native
  emits real graph executor stats for the full traced graph
- graph-mode generation skips the direct live whole-model executor when graph
  mode is explicitly requested, so graph-mode smokes no longer silently exit
  before the graph executor can run
- added resident attention-block composition coverage for
  `Q/K/V linearNoBias -> reshape -> transpose -> transpose -> RoPE -> add ->
  softmax -> add V -> output linearNoBias`, with zero Metal interpreter
  fallbacks and a device-resident output before readback. Transpose is no longer
  the attention-layout promotion blocker for this coverage; the remaining gap is
  broader real-model layout coverage and performance tuning rather than this
  primitive's residency.

## Phase 7: Shared Quant Matmul Routing

Use `src/graph/quant_matmul.zig` as the shared shape and format planner.

The planner should choose:

- decode rows: `mul_mv`
- small prompt rows: `mul_mv_ext`
- larger prompt rows: `mul_mm`
- unsupported or unprofitable shapes: fallback

Backends then answer whether they support the chosen operator for the selected
format. Attention, FFN, MoE, and output-head paths should all call the same
matmul/quant-matmul routing path.

Implemented slice:

- Metal partition capability now consults the shared quant matmul planner for
  packed-weight linear nodes instead of rejecting all packed quant inputs
- supported packed formats/row buckets can stay in Metal partitions with a
  persisted `OperatorPlan`; unsupported formats or storage combinations still
  reject with `missing_quant_kernel` instead of entering an accidental dense
  fallback
- dense activation plus packed quant weight is the accepted matmul shape; packed
  activation inputs still do not enter quant matmul directly, but Metal now has
  shared row/copy operators for supported packed 2D tensors. Quant embedding
  lookup, Metal tensor materialization, and `takeRows` route through those
  prepared-slot `get_rows` / `cpy_q_to_f32` ops before falling back to host
  diagnostics paths
- `CapabilityDecision` can carry the selected shared `OperatorPlan`, and
  `CapabilityDiagnostics` now reports accepted operator counts so partition
  tests can prove that a Q8_0 prompt linear is admitted because it maps to
  `mul_mm`, not merely because Metal accepted the node generically
- `PartitionPlan` now persists a per-node optional `OperatorPlan` selected by
  the winning backend capability decision; non-capability planners fill this
  table with nulls, while Metal Q8_0 linear nodes expose the concrete
  `mul_mv`/`mul_mv_ext`/`mul_mm` plan for later command dispatch
- `ComputeBackend` now exposes optional planned linear hooks
  (`linearPlanned`/`linearNoBiasPlanned`) with fallback wrappers, so graph
  executors can pass the selected `OperatorPlan` into backend dispatch without
  changing ordinary model code
- the Metal partition executor now consumes the persisted plan for planned
  quantized linear nodes by validating rows, dimensions, operator, and packed
  weight format, then dispatching through `linearWithPlan`/`linearNoBiasWithPlan`
- Metal implements the planned linear hooks as validating wrappers over its
  existing quant-aware linear path
- the raw Metal Q8_0 provider path now has a planned-dispatch entry point that
  forwards the selected `mul_mv`/`mul_mv_ext`/`mul_mm` dispatch byte into the
  shared Q8 command encoder; the provider loads the same MMV, small-batch, and
  MM pipelines used by the decode runtime
- the Q8_0 `mul_mm` kernel now uses a conservative 16-output by 8-row Metal
  reduction tile; validator coverage with varied activation columns catches
  the old simdgroup-matrix under-accumulation pattern
- the fused Q8_0 gate/up activation MM kernel uses the same reduction tile and
  is enabled by default; `TERMITE_METAL_DISABLE_Q8_PAIR_ACTIVATION_MM=1`
  forces the split gate/up path for bisection
- `TERMITE_METAL_DISABLE_Q8_MM=1` forces rows >= 9 back onto the verified
  small-batch/MMV paths for bisection; the default path enables tiled Q8_0 MM
- runtime tests assert the plain linear, QKV, and fused gate/up rows >= 9 paths
  increment the expected Q8_0 MM dispatch-family counters
- `decoderRuntimeApplyLinearPair` now accepts batched 2D inputs instead of
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
- added non-Q8 quant routing diagnostics at the capability boundary. The early
  diagnostics covered Q4/Q5 fallback behavior before promotion; current
  coverage expects Q4_0, Q4_1, and Q5_K planned row buckets to route through
  the graph executor where kernels exist, while truly unsupported formats still
  report `missing_quant_kernel`
- packed quant metadata now survives graph metadata views, so reshape/slice/etc.
  cannot erase the quant format and accidentally route a packed tensor into a
  generic dense primitive; unsupported consumers reject with
  `missing_quant_kernel` unless an explicit quant row/matmul plan exists
- promoted Q4_0, Q4_1, and Q5_K planned graph matmul buckets beyond decode:
  `mul_mv_ext` and `mul_mm` now admit explicit packed-weight execution through
  the Metal quant descriptor, using each format's direct packed kernel instead
  of dequantizing weights or falling back to generic dense primitives
- graph executor stats now include `planned_commands`, incremented when a Metal
  partition command consumes a persisted `OperatorPlan`; this makes the shared
  direct-runtime/graph-executor quant planning contract visible in graph tests
- hardened the native oracle for the same packed formats: native quant linear
  now exposes test-only dispatch counters, and bucket coverage for rows
  1/4/9/64 asserts Q4_0, Q4_1, Q4_K, Q5_K, and Q8_0 route through packed
  native kernels without entering dense-dequant SGEMM fallback
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
  `prepared_panel8_packed_bytes`; column-parallel and row-parallel dispatch now
  preserve the same panel8 kernel path as serial and pair/triple execution
- added a fused prepared-panel8 path for Q4_K/Q5_K pair and triple projections;
  Q/K and Q/K/V now traverse the shared Q8_K activation rows once per output
  tile and write all participating projections in the same loop, while Q6_K and
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
- cross-device transfers now use the graph's static output shape when available
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

## Phase 8: WebGPU Partition Executor

Mirror the Metal graph executor shape for browser execution:

- same capability model
- same storage descriptors
- same buffer planning
- WebGPU-specific command encoding and shader dispatch

The goal is not feature parity on day one. The goal is one planning model with
backend-specific executor implementations.

Implemented slice:

- added `src/graph/webgpu_partition_executor.zig`
- routed `.webgpu` partitions through the named partition-executor path in
  `MultiExecutor`
- replaced the original type alias with a real `WebGpuPartitionExecutor` that
  requires and validates the Phase 5 buffer-plan partition view before
  delegating through the native partition executor
- added coverage for the WebGPU executor entry point proving it uses the shared
  partition executor path, preserves stats plumbing, and returns correct output
  through the conservative delegate path
- promoted the first WebGPU executor command family: simple elementwise
  add/multiply/unary nodes now execute inside `WebGpuPartitionExecutor` after
  buffer-plan validation, increment backend command stats, and avoid the native
  delegate path for that partition shape
- promoted the first transformer dense WebGPU command chain: dense
  `linear`/`linearNoBias`, `rmsNorm`, `layerNorm`, and `gelu` now dispatch
  directly through `WebGpuPartitionExecutor`, with native-oracle coverage for a
  linear -> norm -> activation -> linear -> norm partition and command stats
- added a conservative WebGPU graph capability decision that mirrors the
  promoted executor command surface, rejects unsupported packed quant/projected
  linear shapes, and only claims host inputs when the shape is large enough to
  amortize transfer overhead; explicit `.webgpu` partition targets now use this
  decision hook instead of `supportsAll`
- promoted WebGPU mask glue commands for `less_than` and `where_select`, giving
  the executor a direct path for scalar-broadcast masking chains used around
  attention softmax
- promoted WebGPU view/movement commands for `reshape`, `slice`, and
  `concat_prim`, so dense/mask/reduction chains can remain in a WebGPU
  partition across shape changes instead of materializing back to host/native
- added WebGPU graph-plan reservation from Phase 5 buffer allocations: the
  partition executor now derives backend slot reservations from local tensor
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
  now have WGSL entry points, direct/worker imports, and browser smoke parity.
  Biased `fused_linear` applies bias as a resident broadcast add after the
  dense or packed-quant matmul instead of downloading the matmul output first.
- added a command-classification breadth test so each claimed WebGPU graph op is
  either mapped to a concrete executor command family or explicitly treated as
  the runtime-only `fused_from_float32` placeholder.
- promoted packed quant `linearNoBias` admission into WebGPU graph capability:
  supported browser quant shader formats now carry a persisted shared
  `quant_matmul` operator plan through partitioning and WebGPU executor command
  dispatch
- promoted planned WebGPU quant projection families for transformer graph
  blocks: grouped QKV/GQA-style `fused_linear_no_bias`, paired
  `fused_linear_no_bias_pair`, and the `fused_to_float32` pair side-channel now
  stay on the WebGPU command executor when projection metadata is shape-valid;
  focused executor tests assert planned dispatch counts, no interpreter
  fallback, and resident graph outputs for grouped and pair chains
- added WebGPU transformer-block parity coverage at both executor and browser
  levels: the graph executor now has a planned quant projection block smoke
  covering grouped projection, RMS norm, GELU, residual add, softmax, and output
  projection, while the Chromium smoke runs the same style of q4/RMS/GELU/
  residual/softmax/projection shader chain with no intermediate downloads
  before the final parity readback
- WebGPU/WASM GPU-producing graph ops now keep the GPU buffer as the source of
  truth and defer host downloads until `toFloat32`/export, so chained WebGPU ops
  can reuse resident buffers instead of materializing after every command
- no known claimed WebGPU graph op currently depends on a missing browser extern;
  future op claims should land with the matching WGSL/import and executor
  classification coverage in the same slice
- WebGPU is selectable as a compiled partition backend in Wasm/WebGPU builds:
  `compiled` means Termite runs graph compilation/planning/fusion/partitioning
  and attaches the `WebGpuPartitionExecutor`, not that it emits an offline
  WebGPU artifact. This mirrors Metal's compiled partition path; offline
  artifacts remain the ONNX/PJRT-style backend model.

## Current Remaining Work

The original Phase 1-8 architecture is in place. Remaining work is production
adoption and coverage rather than a missing planning phase:

- make graph execution the default hot path where it is at least as reliable and
  fast as the direct runtime path. Today generation keeps the eager/direct
  runtime default unless `TERMITE_GRAPH_MODE`, an explicit compiled partition
  backend, or graph-runtime option selects the graph path; embedding similarly
  reports a direct-runtime bypass when no graph runtime strategy is requested.
- broaden real-model graph-mode smokes for Metal and WebGPU. Focused graph
  executor and browser smokes cover the promoted command families, but full
  model layouts should be exercised under `TERMITE_GRAPH_EXECUTOR_STATS=1` so
  regressions show up as unexpected interpreter fallbacks, boundary
  materializations, or direct-runtime bypasses.
- continue the Metal FFN precision migration. Command plans now distinguish f32
  scratch from f16 FFN intermediates, and Q8_0 pair-activation dispatch now
  prefers the fused pair kernel over the split simdgroup fallback. Planned
  command contracts also carry input/output activation dtype metadata through
  the Zig/C Metal ABI. The first executable kernel slice adds a Q8_0 FFN gated
  activation MM kernel that writes f16 and a matching f16-input Q8_0 down
  projection MM kernel that writes f32 for the existing residual/RMS epilogue.
  The planner selects this route automatically for supported descriptors; runtime
  descriptor/pipeline checks fail closed when a specific shape, quant family, or
  kernel variant is unsupported.
- tune profitability thresholds from benchmark data. The current native/cblas,
  Metal, and WebGPU thresholds are conservative constants; existing bench
  harnesses should be used to compare graph executor, direct runtime, and native
  fallback behavior across decode, prompt, embedding, and quant row buckets.
- promote additional WebGPU graph families only when the complete chain is
  present. Browser attention shaders/imports exist, and WebGPU can now be
  selected for compiled partition execution in Wasm/WebGPU builds, but graph
  capability currently claims only the promoted resident command surface;
  attention and MoE/`mul_mat_id`-style graph promotion remain explicit
  follow-up work.
- keep this document reconciled with implemented slices. Historical fallback
  notes should be revised when later bullets promote the same operator family.

## Validation

Each phase should keep native/cblas as the oracle.

Minimum checks:

- partition tests for capability/profitability decisions
- graph execution parity against interpreter
- per-backend rejection reason reports
- buffer-plan liveness tests
- Metal/WebGPU output parity for each newly claimed op family
- real model smoke tests once matmul, norm, softmax, and quant linear are routed
