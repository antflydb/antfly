# GGML Graph Execution Implementation History

> Relocated verbatim from `zig/pkg/inference/GGML.md` (lines 635–954 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`GGML.md`](../../../zig/pkg/inference/GGML.md). Durable decisions from this log were folded into that document before the move.

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
