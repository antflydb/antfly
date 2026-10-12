# Metal ggml-Shaped Checklist, Phase Plan, and Performance Diagnosis

> Relocated verbatim from `zig/pkg/inference/METAL.md` (lines 617–1313 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [METAL.md](../../../../../zig/pkg/inference/METAL.md) before the move.

### ggml-Shaped Metal Checklist

This is the concrete gap list from comparing the current Metal backend against
`../ggml`/llama.cpp's production shape:

Implementation order:

1. Build the runtime-owned `MetalGraph` / `MetalCommandPlan` path and make
   active decode/prefill lower into op records instead of nested helper calls.
2. Move scratch, KV views, tensor liveness, and command encoder scopes into
   that plan.
3. Lift the existing Q8_0 row-shape selector into the shared Zig
   `QuantMatmulShape -> QuantMatmulKind` helper and keep Metal's current
   counters behavior-equivalent.
4. Route every hot linear through one descriptor-driven quant matmul dispatch.
5. Replace scalar-ish Q8_0 and small-prompt kernels with ggml-shaped MMV,
   MMV-ext, and simdgroup MM kernels.
6. Add graph/runtime pattern fusions only after the op plan and shared kernels
   own the path, so fused kernels call the same quant/attention primitives.

- [x] Add descriptor-style packed quant slots for the active Metal runtime.
- [x] Keep Q8_0 weights packed/resident on the active Metal hot path.
- [x] Add a first graph-planned scratch allocator so prefill/decode helpers can
  reserve reusable frame slots up front.
- [x] Add a first Graph/Metal command planner model that records op resource
  ranges, groups compatible ops into encoder scopes, and marks required
  dependency barriers.
- [x] Fix the command planner's barrier semantics so a planned buffer barrier
  resets the active hazard set. Later sibling consumers of the same producer no
  longer receive redundant barriers after the first dependency barrier.
- [x] Add an allocation-free command-plan build path for hot runtime callers
  that provide bounded op/scope/resource buffers.
- [x] Add a shared Zig quant matmul selector with ggml-style bucket names and
  block-alignment validation. `GraphCommandOp` records can now carry planned
  quant dispatch metadata independent of the concrete Metal kernel.
- [x] Thread planned quant dispatch metadata through the layer-contract ABI and
  into the first Metal Q8_0 raw-linear consumers: prefill setup, direct
  attention/FFN/PLE block linears, and the final LM head.
- [x] Thread the same planned dispatch metadata into the fused Q8_0 FFN gate/up
  activation and PLE-gate activation helpers. The wrappers preserve legacy
  fallback behavior when a preferred pipeline is unavailable.
- [x] Add an encoder-local quant descriptor path for planned Q8_0 direct block
  ops. The active block now describes attention output, FFN gate/up, FFN down,
  PLE gate, and PLE projection through descriptors instead of calling each
  helper as a standalone dispatch policy surface.
- [x] Move planned Q8_0 Q/QKV setup and tail LM-head encoding onto the same
  encoder-local descriptor path.
- [ ] Retire `termite_metal_select_q8_0_linear_dispatch` after the Metal encoder
  consumes the shared planned quant matmul metadata directly.
- [x] Add planner regression coverage for the active decode attention setup
  dependency pattern so pre-norm -> QKV -> Q/K/V consumers stay at one planned
  scope and the minimal dependency barriers.
- [x] Move the active decode attention setup plan builder into the graph
  command planner module and test both QKV and shared-KV variants there. The
  decode path now consumes a graph-owned plan shape instead of defining
  resource semantics inside `metal_compute.zig`.
- [x] Add graph-owned dependency contracts for the other active row-1 planned
  helper shapes: attention output projection + post-norm residual, FFN + PLE,
  and final RMS + Q8_0 LM head + argmax tail. These are tested planner shapes
  and should be the migration target for replacing remaining Objective-C
  manual barrier sequences with runtime-owned op records.
- [x] Replace the single-thread greedy logits argmax with a two-stage parallel
  Metal reduction. The Q8_0 LM head still writes logits, but the tail no longer
  serially scans the vocabulary on one GPU thread.
- [x] Move final greedy RMS in the Q8_0 tail onto the parallel reduce RMS
  kernel while keeping the same planned tail scope.
- [x] Add Metal runtime planned-scope ABI hooks and counters:
  begin planned compute scope, insert planned buffer barrier, end scope, and
  report planned scopes/barriers in frame telemetry.
- [x] Move the planned-scope cursor into the Metal runtime layer so planned op
  records can be consumed by multiple hot paths rather than a private helper in
  `metal_compute.zig`.
- [x] Attach real row-1 active decode layer subpaths to planned scopes:
  pre-attention RMS + Q8_0 QKV/shared-Q projection + head RMS/RoPE, attention
  apply + Q8_0 output projection + post-attention RMS/add, FFN pre-gate RMS
  scale + Q8_0 gate/up activation + Q8_0 down projection + post-down RMS/add,
  and Q8_0 PLE gate/activation + projection + post-norm residual/output-scale
  now share planned compute encoders with explicit buffer barriers. The
  4-token Gemma4 anchor keeps token IDs `10979 236888 2088 740` and reports
  `planned_scopes=36`, `planned_barriers=422`, with last-frame compute
  encoders down to `41`.
- [x] Collapse the active Q8_0/f32-KV block's attention setup, attention
  residual, and FFN/PLE helpers into one layer-owned planned scope. This removes
  the old per-layer attention and FFN planned encoders from the active decode
  frame and exposes `layer=35` in compute source/region telemetry so the
  remaining command boundaries are visible instead of hidden in source totals.
- [x] Add a full active Q8_0/f32-KV layer dependency contract to the Graph/Metal
  command planner: attention setup, attention apply/projection, FFN, PLE, and
  output-scale are represented as one 15-op layer scope with 12 explicit
  producer-consumer barriers. This matches the current active row-1 barrier
  shape. The live active path builds this full layer plan when the Q8_0/f32-KV
  + PLE contract matches.
- [x] Route the full active Q8_0/f32-KV layer plan's barrier flags into the
  Metal runtime block helper. The helper still encodes the individual dispatches
  directly, but it no longer owns a separate internal barrier schedule for the
  active planned path.
- [x] Add typed planner op IDs for decode layer/tail contracts and route the
  active Q8_0/f32-KV layer op sequence into the Metal helper. The helper now
  validates that it is consuming the expected graph-planned operation contract,
  not just a positional barrier list.
- [x] Move active contract export into `PlannedComputeSequence` so the runtime
  cursor owns typed op/barrier handoff state. `metal_compute.zig` no longer
  assembles parallel planned-op arrays by hand for the active layer bridge.
- [x] Collapse planned layer op IDs, barrier flags, and cursor start into one
  backend contract field. Hot-path block requests now pass a single
  `planned_layer_contract` value instead of three loosely related fields.
- [x] Mirror that contract shape across the Metal runtime ABI. The Objective-C
  block entry point now receives one planned-layer contract struct instead of
  five separate op/barrier/count/cursor arguments.
- [x] Extract a reusable Metal planned-layer contract cursor. The active layer
  helper now consumes typed op IDs and planner barriers through shared cursor
  helpers instead of embedding ad hoc validation/barrier cursor logic inline.
- [x] Add an explicit Q8_0/f32-KV layer step table on the Metal side. The active
  block now consumes named steps from that table, keeping expected op IDs in one
  contract definition instead of scattering raw op-kind constants through the
  helper body.
- [ ] Replace the scratch-only graph plan with a runtime-owned command plan:
  op records, resource ranges, encoder scopes, explicit barriers, and planned
  scratch lifetimes for the whole decode/prefill frame.
- [x] Add the runtime command-plan data model on top of the existing planner:
  `GraphCommandOp` records now carry op kind, source/region, planned op
  index, scope index, barrier flag, resource range offsets, and optional
  quant-matmul dispatch metadata.
- [x] Add planned scratch lifetime records to command plans. The active
  Q8_0/f32-KV layer builder emits logical scratch slot sizes and the planner
  records first/last op use for those slots.
- [x] Add a frame-owned command-plan aggregation object. It concatenates layer
  command plans into one frame view, renumbers op/resource/scope indexes, sums
  explicit barriers, and merges scratch lifetimes across layer boundaries.
- [ ] Convert current layer helpers into graph op builders. During migration,
  helpers may consume planned encoder scopes, but the final shape is op records
  encoded by the runtime plan rather than direct helper orchestration.
- [x] Move the active Q8_0/f32-KV layer helper contract behind a command-plan
  builder. The live decode path now consumes `commandView().planView()` for
  the active layer scope instead of the legacy scratch-only `PlanView`.
- [x] Move the active Q8_0 greedy tail behind the same command-plan contract
  shape. The Zig tail builder emits RMS -> LM head -> argmax op records and
  scratch lifetimes, and the Metal Q8_0 tail helper validates op IDs/barriers
  through the shared planned-contract cursor instead of owning a private
  hard-coded barrier sequence.
- [x] Make runtime encoding iterate `GraphCommandOp` records directly for the
  active command-plan consumers. Planned contracts now carry packed command-op
  records (`kind`, `source`, `region`, `scope`, barrier flag, resource slice,
  and quant dispatch). The Q8_0 greedy tail and active Q8_0/f32-KV row-1 layer
  helper both run encoder loops over those records and dispatch by op kind; the
  older cursor path remains only as a compatibility fallback for callers that
  have not supplied command-op records yet.
- [ ] Extend command-plan builders to the qLen>1 prefill layer contract and
  non-Q8/dense tail variants, then assemble embedding -> PLE -> layers -> tail
  as one frame command plan before any runtime encoding starts.
- [x] Give qLen>1 Q8_0/f32-KV prefill its own named command-plan builder.
  `PrefillGatedLayerCommandLowerer` now rejects row-1 decode use, emits the same
  setup-plus-block command records/scratch lifetimes for prompt batches, and
  the active prefill path routes through that builder instead of the
  decode-named layer plan.
- [x] Add a runtime-owned qLen>1 prefill frame-plan hook before frontend layer
  encoding starts. The gated runtime now calls
  `decoderRuntimePlanPrefillFrame` immediately after opening a prefill frame;
  Metal builds a `GatedFrameCommandLowerer` for all layers plus the tail,
  preserves quant dispatch metadata while aggregating command plans, and
  reserves graph-plan scratch slots from the whole frame view before layer
  helpers encode work.
- [x] Make active qLen>1 prefill layer helpers consume frame-plan layer slices.
  The Metal backend keeps the prefill frame plan alive for the active frame,
  hands each successful direct prefill layer a layer-local command view, and
  uses frame-owned setup/block offsets instead of deriving the active layer
  contract from helper-local op lists. The frame-level layer includes attention
  pre-norm; the current helper receives that tensor already normalized, so its
  setup cursor starts after the planned pre-norm op.
- [x] Consume the qLen>1 prefill frame tail slice for prepared final logits.
  `GatedFrameCommandLowerer` now exposes a logits-only tail command view
  (final RMS -> LM head), records the tail slot/dimension contract, and the
  Gemma prefill direct path can keep the active Metal frame open through final
  logits. The Q8_0 tail encoder writes logits into the planned sample-logits
  buffer, then the frontend submits the frame before host materialization.
- [x] Re-enable the active qLen>1 prefill frame after fixing activation
  lowering. `gelu_new` is now carried through the backend vtable and Metal
  runtime activation enum, so Gemma4 prompt FFN activation no longer takes the
  decomposed frontend path that could produce a single NaN and collapse logits
  to token `0`.
- [x] Start separating frame-plan quant format from activation/KV layout.
  Prefill frame metadata now carries per-linear quant formats and a tail quant
  format through `QuantMatmulPlan`; activation dtype and KV layout are separate
  frame options. The active Gemma path still builds the Q8_0/f32-KV
  specialization by default, but planner tests now prove the same frame shape
  can carry Q4_K layer ops and a Q5_K tail op without changing the graph
  contract.
- [x] Wire prefill frame quant metadata from actual Metal slot descriptors.
  The Metal frame planner now asks prepared runtime linear slots for their real
  quant format and rejects dense/unsupported slots instead of default-labeling
  the frame Q8_0. This keeps the command plan truthful for future Q4/Q5/Q6/IQ
  active kernels while preserving the current Q8_0 execution guard.
- [x] Add a dedicated no-blit Q8_0 prefill setup encoder. It consumes the
  planner-produced setup op contract and encodes Q/QKV projection, Q/K head
  RMS/RoPE, and optional V norm in one planned compute scope before prompt KV
  seed and block apply.
- [x] Thread qLen>1 Q8_0 prefill setup and block apply through one continuous
  layer command contract. The block helper now receives the same planned op list
  with a cursor start after setup, instead of a separate empty/default contract.
- [x] Move active decode attention setup for row-1 layers onto the bounded
  command planner: pre-attention RMS, Q/QKV projection, Q head norm/RoPE, K
  head norm/RoPE, and V norm now advance through planned op records instead of
  hand-placed begin/barrier/end state.
- [x] Extract the active attention setup op-list construction into a
  stack-owned graph-planner builder so the decode path consumes a named plan
  object rather than assembling resource arrays inline.
- [x] Add Objective-C/Metal planned-scope encoding helpers so fused layer
  kernels can share one encoder and insert planner-selected buffer barriers.
- [x] Attach planned scopes to real active decode layer kernels instead of only
  testing the runtime scope/barrier bridge.
- [ ] Replace the current attention kernels with a flash-attention-class Metal
  family that owns scale, mask/bias, softcap/sinks where needed, softmax, and
  PV accumulation.
- [ ] Finish the ggml-style quant matmul family behind one descriptor dispatch:
  `qLen=1` MMV, `qLen=2..8` MMV-ext, `qLen>8` MM/GEMM, then extend beyond Q8_0.
- [ ] Move QKV, attention output, gate/up/down FFN, PLE, and LM head through
  that common quant dispatch surface.
  Current status: Q8_0 command-plan metadata covers those logical ops for the
  active Gemma paths, and the Metal helpers consume the metadata for raw Q8_0
  Q/QKV, attention output, FFN gate/up activation, FFN down, PLE gate
  activation, PLE projection, and LM-head linears. The active setup, block, and
  tail Q8_0 linears now route through encoder-local quant descriptors. The
  `NONE`, `PAIR`, `QKV`, `PAIR_ACTIVATION_MUL`, `ACTIVATION_RHS_MUL`, and
  `PAIR_ACTIVATION_RMS_SCALE_1X` epilogues now use descriptor-native shared
  implementation templates. Non-Q8 `NONE` epilogues now use the same
  encoder-local descriptor surface for the scalar quant format set, including
  existing row-1 reduce kernels for Q4_0/Q4_K/Q5_K/Q6_K. The next work is
  extending pair/QKV/fused epilogues beyond Q8_0 and replacing scalar kernels
  with ggml-quality per-format kernels behind the same descriptor ABI.
- [ ] Promote RMS/norm + mul/add and GLU/FFN fusions into graph/runtime pattern
  selection instead of hand-adding one-off helper variants. Fused kernels must
  call shared descriptor-driven quant kernels or share their implementation
  templates; they must not fork per-format quant decoding.
- [ ] Add standalone graph-level Metal softmax with scale/mask/bias support for
  non-attention graph users and fallback paths.
- [ ] Add indexed/MoE matmul equivalents (`mul_mat_id` / `mul_mv_id`) when a
  supported model requires them.
- [ ] Broaden RoPE coverage to the ggml variant set we need: normal/neox,
  mrope/vision, YaRN/frequency scaling, and dtype/shape buckets.
- [ ] Add fusion/bucket regression tests that assert both numeric parity and
  selected fast-path counters, so faster paths cannot silently stop being used.

## Remaining Plan

### Operator Family Completion Checklist

This is the production operator surface we are migrating toward. ggml remains
the reference implementation shape, but these names are Antfly inference graph/backend
operators so the same plan can later target Metal, CUDA, WebGPU, Wasm, or native
fallback.

- [ ] `mul_mv` / `mul_mv_ext` family.
  - [x] Q8_0 qLen=1 and qLen=2..8 route through shared operator metadata.
  - [x] Split Q8_0 qLen=2..5 `mul_mv_ext` into ggml-style row-count
    specialized kernels for `NONE`, pair, and gate/up activation epilogues.
    The descriptor encoder picks the matching r2/r3/r4/r5 pipeline and falls
    back to r5 if an optional specialized pipeline is unavailable.
  - [x] Add Q5_0 row-1 `mul_mv` coverage behind the common descriptor path.
    The Q5_0 `NONE` encoder now selects a packed reducer for qLen=1 instead
    of the scalar output-thread kernel, and graph support marks Q5_0 as a real
    `mul_mv` format.
  - [x] Add row-1 `mul_mv` coverage for Q4_1, Q5_1, Q8_1, Q8_K,
    IQ4_NL, IQ4_XS, and MXFP4.
    These formats now route qLen=1 through packed SIMD-reduction kernels under
    the same descriptor ABI instead of the scalar one-output-thread fallback.
    They are intentionally marked as `mul_mv` only; `mul_mv_ext` and `mul_mm`
    still require separate small-prompt and large-prompt kernels.
  - [ ] Replace Q8_0 reducer kernels with production-quality shared
    format-helper kernels.
  - [ ] Add real `mul_mv_ext` kernels for non-Q8 formats and finish
    `mul_mv` coverage for the remaining scalar-only formats.
- [ ] `mul_mm` simdgroup family.
  - [x] Q8_0 qLen>8 routes through the shared `mul_mm` bucket.
  - [ ] Replace the current Q8_0 large-prompt path with a ggml-quality
    simdgroup matmul/GEMM primitive.
  - [ ] Add `mul_mm` kernels for the broader quant format set.
- [ ] Quantized row and copy ops.
  - [ ] Implement backend kernels for `get_rows`, `set_rows`,
    `cpy_q_to_f32`, and `cpy_f32_to_q`.
    Current status: Q8_0, Q4_0, Q5_0, Q4_K, Q5_K, and Q6_K now have Metal
    backend kernels for `get_rows` and contiguous `cpy_q_to_f32` from prepared
    quant linear slots. The runtime entrypoint is format-tagged so the graph op
    stays common and backend dispatch chooses the per-format dequant-row helper,
    matching the ggml shape (`get_rows`/`cpy`/`mul_mat` as common ops, quant
    formats as helpers underneath). Q8_0, Q4_0, Q4_1, and Q5_0 also have device
    `set_rows` and contiguous `cpy_f32_to_q` writeback kernels for prepared quant linear
    slots, validated by copying/scattering f32 rows into a private packed slot
    and reading them back through the same format-tagged `get_rows` /
    `cpy_q_to_f32` paths. Q5_1 and Q8_1 now use the same format-tagged row/copy
    surface, including `set_rows` and contiguous `cpy_f32_to_q` writeback.
    Q4_K, Q5_K, and Q6_K also have `set_rows` and contiguous `cpy_f32_to_q`
    writeback. IQ, MXFP4, and NVFP4 still need real backend row/copy kernels.
  - [ ] Use those ops for embeddings, prompt/KV materialization, and
    diagnostics instead of ad hoc helper paths.
    Current status: quantized embedding lookup now routes through the
    format-tagged `get_rows` runtime path instead of the Q8_0-only embedding
    helper when the embedding weight is quantized; the obsolete Q8_0-specific
    embedding kernel/pipeline wrappers have been removed so quant embedding
    gather has one operator-backed path. Dense embedding lookup is still a
    separate f32 gather path. Runtime diagnostics/materialization now have
    generic format-tagged `get_rows` and `cpy_q_to_f32` helpers for prepared
    quant linear slots, plus Q8_0 f32-to-packed contiguous and row-id scatter
    writeback for diagnostics and future graph-owned materialization. Metal
    backend tensor materialization now prepares/fetches quant linear slots on
    demand and reads supported 2D quant tensors through the generic
    `cpy_q_to_f32` runtime op before falling back to the host quant codec. Metal
    `takeRows` now uses the same prepared-slot `get_rows` op for supported
    quant tensors, so graph/MoE row gathers no longer have to miss Metal solely
    because the input is packed. The Metal graph partitioner also now admits
    packed `fused_take_rows` with a concrete `quant_row.get_rows` operator plan,
    and the Metal partition executor validates that plan before calling the
    backend row-gather path. Prompt KV seed paths now publish device-resident
    K/V through the backend `writeLayerKvSuffixDevice` hook for non-framed
    calls, while the active prefill block path passes prepared K/V directly
    into the paged attention/FFN runtime block so the planned `decode_kv_seed`
    op owns in-frame publication. This covers the `MetalKvStorage` formats
    f32/f16/int8/polar4/turbo3 and leaves gathered spans as a compatibility
    attention source outside the active planned path. The full-layer and
    prefill-frame command planners now
    include an explicit `decode_kv_seed` op between K/V preparation and
    attention, so the frame contract describes KV publication instead of
    hiding it as frontend orchestration. The planned Q8_0 attention/FFN/PLE
    command loop now performs the paged slot update when that `decode_kv_seed`
    record is reached, with the attention op only retaining a compatibility
    fallback for older partial plans. The f32 paged KV seed path now encodes as
    a no-blit compute kernel on the active planned command encoder, using the
    physical block table to publish suffix K/V directly into the paged slot.
    Active-frame last-dim helper slicing now uses `termite_slice_last_dim_f32_2d`
    compute instead of row-wise blit copies, and active prefill K/V no longer
    queues copied seed tensors for a post-submit flush. Active qLen>1 prefill
    also bypasses the copy-based reserved hidden carrier, so the `hi
    --max-tokens 4` Metal validator anchor remains correct with `token_ids:
    10979 236888 2088 740` and no `TERMITE_METAL_TRACE_FRAME_BLITS=1` frame
    blit traces. The paged-KV metadata hook now allows raw f32 `MetalKvStorage`
    pages too, and it is per-layer shape aware for mixed KV dimensions, so
    eligible planned attention can consume the same physical page table path as
    compressed KV instead of gathering f32 spans. During an active frame the
    hook can reserve/expose a slot before committed token metadata exists
    because the planned command stream writes it via `decode_kv_seed` before
    `attention_paged`. qLen>1 direct block execution is now gated away from the
    legacy gathered monolithic Q8_0/f32-KV block; unsupported prompt-layer
    shapes return to the safe staged path rather than issuing failed
    direct-block submissions. Remaining work is to replace the active-frame
    hidden ping-pong/returned-output allocation pattern with runtime-owned
    planned scratch/output slots, replace the intermediate materialized PLE
    slice with a true strided PLE operand, and investigate the remaining
    shared-KV setup miss visible under `TERMITE_METAL_TRACE_Q80_BLOCK=1`.
- [ ] Attention operators.
  - [x] Add graph-level `attention_flash`, `attention_paged`, and
    `attention_quantized_kv` command ops.
    Current status: `OperatorPlan` can now carry attention records through the
    same runtime command-plan view as quant matmul, row, and copy ops. The
    active Q8_0/f32-KV row-1 layer now tags its attention stage as
    `attention_flash` in the runtime command contract, and the Objective-C
    command-loop validates that operator/format metadata before encoding the
    f32 attention kernel. The planner now also records attention storage
    (`dense` versus `paged`) separately from KV dtype, so f32 paged KV emits an
    `attention_paged` operator rather than being misclassified as dense
    `attention_flash`. Dense f32 graph `fused_sdpa` nodes now also get an
    `attention_flash` `OperatorPlan` from `decideMetalEagerGraph`.
  - [ ] Replace bespoke active attention helpers with backend kernels that own
    scale, mask/bias, softcap/sinks, softmax, and PV accumulation.
    Current status: f32 attention now routes qLen=1 decode and qLen>1 prefill
    through the same tiled backend kernel when `kv_len <= 2048` and
    `head_dim <= 1024`, so row-1 decode no longer falls back to the scalar
    per-head f32 attention kernel for the active Gemma shape. A new
    format-aware `attention_paged` Metal kernel/runtime entrypoint now owns
    causal/sliding mask, optional per-head sinks, optional softcap, softmax, and
    PV accumulation against a block-table ABI. It supports raw f32, f16,
    int8-per-head, Polar4, and Turbo3 KV slot layouts behind one dispatch
    surface. The active planned Q8_0/f32-KV command loop now routes
    `decode_kv_seed -> attention_paged` without closing the planned compute
    encoder for the f32 suffix seed, and the reserved-output `Into` path keeps
    the active decode loop on the correct device buffer. The remaining paged
    attention helper now accepts the active layer contract and rejects stale
    command records that are not `attention_paged`/`attention_quantized_kv`,
    so this fallback path is also tied to planned operator metadata instead of
    silently encoding helper-local attention. Remaining work here is arbitrary
    mask/bias support and replacing the older f32 tiled helper wherever the
    paged operator is eligible.
  - [ ] Preserve Polar4 and Turbo3 KV support under the quantized-KV attention
    operator.
    Current status: the active compressed decode path now routes device-resident
    Polar4/Turbo3 KV attention through one backend `attention_quantized_kv`
    shaped runtime wrapper. That wrapper now encodes/update the compressed KV
    slot and executes the shared paged-attention kernel with an identity block
    table for the current span-backed storage. `KvStorageRuntime` now exposes a
    backend paged-KV metadata hook, and `MetalKvStorage` implements it so the
    generic Metal attention op can consume resident slot/format metadata
    directly after a device KV suffix write instead of gathering a f32 span.
    qLen>1 is represented in the runtime ABI, but production prompt use still
    needs physical page tables from `MetalKvStorage` and graph-owned attention
    command encoding.
- [ ] Graph execution shape.
  - [x] Make command plans carry all operator-family records plus fallback
    diagnostics.
    Current status: runtime command ops now carry quant matmul, quantized row,
    quantized copy, and attention operator records; `operatorStats()` reports
    selected operators and explicit fallback counts so unsupported kernels are
    visible instead of silently blending into normal decode/prefill paths.
  - [x] Move `OperatorPlan` out of the Metal command planner and into graph
    planning code.
    Current status: `src/graph/operator_plan.zig` owns the backend-neutral
    operator union and stats. The Metal command planner imports that module and
    remains the first active consumer.
  - [ ] Make backend-specific runtimes encode operator records and select
    kernels under the same plan.
    Current status: active row-1 Q8_0/f32-KV layer and tail command loops now
    consume `operator` and `format` fields from `PlannedCommandOp`. Q8-only
    matmul encoders reject non-matmul or non-Q8 graph records; f32 attention
    rejects non-`attention_flash`/non-f32 records. This makes the current Metal
    path graph-owned enough to catch wrong planner records instead of silently
    executing helper-local assumptions. The paged attention slot helper now
    consumes the same active layer contract and validates the planned paged
    attention operator before encoding. Active f32-KV layer execution now fails
    closed when a planned contract is present and the planned direct block is
    unavailable, instead of falling into staged helper orchestration under a
    stale command cursor. The active decode tail now follows the same rule:
    final norm -> LM head -> argmax must be encoded by the planned tail command,
    and the old split helper tail is no longer used as the active-frame fallback.
    Dense f32 `fused_sdpa` is now the first generic graph attention op executed
    by `metal_partition_executor` through a planned operator record with a
    zero-fallback validator test.
    Remaining work is replacing more helper entry points with generic operator
    encoders and adding real non-Q8 kernels behind the same records.
  - [ ] Keep generic fallback as an explicit unsupported-format/diagnostic
    path, not the normal decode or prefill route.

### Phase 1: Rejoin The Graph/Runtime Shape

- [ ] Route the Gemma4 direct path behind the shared `ModelRuntime` /
  `metal_executor` contract instead of bespoke frontend loops.
- [ ] Keep model-family files responsible for structural layer metadata only.
- [ ] Move remaining KV mutation, rollback, token IO, and sampling ownership to
  graph/runtime helpers.
- [ ] Make `decoder_gated_runtime.zig` lower layer contracts instead of acting
  as a parallel executor.

Acceptance: a backend-owned decode or prefill call can be described as a graph
or layer contract, and the frontend no longer orchestrates per-op CT helpers in
the hot loop.

### Phase 2: Build The ggml-Shaped Quant Matmul Core

- [x] Keep Q8_0 weights packed and resident on the Metal path.
- [x] Add descriptor-style runtime quant slots for the active Q8_0 path.
- [x] Route 9+ row Q8_0 prompt linears to the simdgroup MM dispatch bucket.
- [x] Centralize Metal quant slot prepare validation behind a packed descriptor
  and block-layout table for the currently wired formats.
- [x] Collapse per-format prepared booleans into one prepared-format slot state.
- [x] Replace per-format runtime prepare entry points with one format-tagged
  quantized linear prepare ABI.
- [x] Add generic Objective-C quant slot storage for the shared device apply
  path and runtime memory accounting.
- [x] Remove duplicate Q8_0/Q8_1/Q8_K Objective-C per-format slot storage from
  execution; those paths now use the generic quant slot record.
- [x] Route host fallback single-linear execution through one format-tagged
  quantized linear ABI for every currently wired generic format and remove the
  unused per-format host wrapper symbols.
- [x] Route I2_S/Q4_0/Q4_K/Q5_K/Q6_K pair, QKV, attention projection, and FFN
  execution through generic quant slot views, then remove their duplicate
  Objective-C per-format slot storage.
- [x] Replace non-Q8 pair public entry points with one format-tagged pair ABI.
  Current status: I2_S, Q4_0, Q4_K, and Q6_K pair dispatches use the same
  descriptor epilogue path; unsupported formats reject instead of adding
  per-format wrapper symbols.
- [x] Define the shared quant-matmul operator surface. `quant_matmul.zig`
  now exposes backend-neutral operator buckets (`mul_mv`, `mul_mv_ext`,
  `mul_mm`, and `fallback`) plus packed format descriptors/load-helper tags.
  The `OperatorPlan` metadata is intentionally not named after ggml: ggml is
  the reference shape, but this plan should migrate into the shared graph layer
  so Metal, CUDA, WebGPU, and Wasm backends can choose their own kernels behind
  the same op surface. The Objective-C runtime selector now uses the same
  generic quant-matmul dispatch validation before routing Q8_0 to its current
  pipeline family.
- [x] Move Q8_0 onto the shared primitive surface first. Q8_0 remains the only
  fully supported packed helper in this slice, but it now flows through the
  generic descriptor/dispatch path instead of a Q8-only selector. The shader
  source has explicit Q8_0 scale/value helpers so additional formats can add
  load helpers behind the same primitive boundary.
- [ ] Replace remaining scalar-ish Q8_0 reducers with a production packed
  `mul_mv` / `mul_mm` family.
  Current status: the row-1 Q8_0 `NONE` MMV kernel is back on the same
  two-output-row shape ggml uses for `N_R0_Q8_0`. A local trial widening the
  MMV tile to four columns preserved the 4-token Gemma4 anchor but slowed the
  smoke run, so the active Q8_0 MMV path stays on the ggml-shaped 2-column
  geometry until it can be replaced by a measured common primitive rather than
  another isolated tile tweak. The kernel still needs ggml-grade tuning and
  benchmarking before this item is complete.
- [x] Match ggml's quant matmul selection as a system, not as isolated kernels:
  qLen=1 mat-vec, qLen=2..8 ext mat-vec, qLen>8 simdgroup MM, all behind one
  descriptor ABI and exercised in the active graph.
  Current status: Q8_0 descriptor dispatch now selects row-1 MMV, qLen 2..8
  small-batch MMV-ext, and qLen 9+ simdgroup MM for `NONE` linears. The
  qLen=2..5 Q8_0 small-batch path now has separate r2/r3/r4/r5 Metal kernels,
  matching ggml's row-count-specialized `mul_mv_ext` structure for `NONE`,
  pair, and gate/up activation epilogues. Q8_0 QKV keeps the fused row-1 QKV
  kernel for decode, but qLen 2+ now decomposes into descriptor-owned `NONE`
  Q/K/V submissions so small prompts and large prompts use the same bucket
  selector as the rest of the active graph. The raw two-token Gemma4 smoke now
  reports `small_batch=240`, `rows_2_8=240`; the chat-template `hi` anchor
  reports 9+ row MM dispatch for prompt linears.
- [x] Use one dispatch ABI for QKV, attention output, gate/up/down FFN, PLE,
  and LM head.
  Current status: active Q8_0 linears use descriptor records across setup,
  layer, and tail; every Q8_0 descriptor epilogue used by the active paths now
  shares descriptor-native encoder implementation. Non-Q8 `NONE` linears also
  route through the descriptor encoder path. Non-Q8 `PAIR` descriptors are now
  supported for the pair kernels we already ship (`I2_S`, `Q4_0`, `Q4_K`,
  `Q6_K`), and Q4/Q6 FFN pair stages encode through that descriptor path
  instead of hand-selecting those pair kernels. The runtime pair helper now
  calls one format-tagged pair ABI for those formats. Q8_0 QKV, attention
  output, FFN gate/up/down, PLE gate/projection, and LM head all enter through
  the same descriptor encoder surface; format-specific kernels are selected
  below that ABI.
- Q8_0, Q4_K, and mixed Q5_K/Q4_K QKV now route through one format-tagged
  device QKV ABI. Q8_0 keeps its fused QKV descriptor epilogue; mixed-format
  QKV lowers as descriptor-composed `NONE + PAIR` internally. The older
  per-combination public wrappers were removed or made internal helpers.
- [x] Add kernel-mix counters that show which dispatch bucket each Q8_0 linear
  family uses.
  Current status: runtime stats now report family x dispatch-bucket counts for
  plain linear, pair activation, pair activation plus RMS scale, activation RHS
  multiply, pair, QKV, and RMS-scale linears. The counters are diagnostics over
  the common descriptor dispatch, not a new public ABI.
- [ ] Extend the same descriptor surface to Q4_0, Q5_0, Q8_1, K-quants,
  I-quants, MXFP4/NVFP4, and BitNet formats only after Q8_0 is proven.
  Current status: non-Q8 `NONE` is descriptor-native for all wired scalar
  quant kernels; non-Q8 `PAIR` is descriptor-native for `I2_S`, `Q4_0`,
  `Q4_K`, and `Q6_K`. Q4_K and Q5_K/Q4_K QKV are descriptor-composed from
  `NONE + PAIR` behind the same format-tagged ABI. Remaining epilogues still
  need format-generic lowering or explicit slow-path rejection.

Acceptance: prefill and decode linears route through the same quant dispatch
surface, with no dense dequant fallback in the hot path.

### Phase 3: Make Layer Submission Coarse

- [x] Add an explicit Gemma4 prefill-layer contract.
- [x] Move non-shared and shared-KV prefill layers under that contract.
- [x] Plan reusable scratch before layer work.
- [x] Make graph-plan slot readiness capacity-based and use geometric
  allocation growth so later larger prefill reservations reuse the same planned
  frame slots.
- [x] Move active decode per-layer output scale into the direct block contract
  instead of applying it as a frontend post-block multiply.
- [x] Add a Graph/Metal command planner data model for op resource ranges,
  compatible encoder scopes, and dependency barriers.
- [x] Add runtime ABI hooks for planned compute scopes and dependency barriers,
  plus counters in `metal_runtime_encoders`.
- [x] Move the row-1 active FFN down projection plus post-down RMS/add sequence
  under a planned compute scope, with a parity test that asserts planned-scope
  and barrier counters.
- [x] Collapse active Q8_0/f32-KV attention setup, attention residual, and
  FFN/PLE into a layer-owned planned scope, removing the separate attention and
  FFN planned encoders from the active decode frame.
- [x] Add the full active Q8_0/f32-KV layer op contract to the command planner so
  the remaining Objective-C helper can be replaced by a runtime-owned op stream
  without changing the dependency shape. The active path now selects that full
  plan for matching row-1 layers.
- [x] Encode norm, QKV/shared-Q, RoPE, attention, output projection, FFN, PLE,
  residual, and output-scale as one runtime-owned layer submission where
  dependencies and scratch lifetimes are Metal-owned for the active row-1
  command-plan path. Current status: the active Q8_0/f32-KV row-1 path has one
  layer-owned planned scope per layer whose suffix ops are encoded by iterating
  command-op records. qLen>1 Q8_0 prefill setup plus block apply still consume
  one continuous planner contract, but embedding/setup/KV seed/tail frame
  assembly is not yet a single runtime-owned graph stream.
- [x] Feed active layer contracts into the command planner before encoding, and
  use the resulting scopes/barriers to drive Objective-C command submission.
  Current status: the row-1 active decode layer handoff now exports the full
  `GraphCommandPlanView` contract, including op records and quant dispatch
  hints, into the Objective-C cursor. Command-record paths rely on planned
  `barrier_before` dependencies instead of adding fallback barriers after every
  op.
- [x] Remove per-stage host-pointer runtime ABI from active decode and prefill.
  Current status: the active Q8_0/f32-KV layer and prefill block contracts use
  device-buffer inputs/outputs and command records. Active-frame decode now
  aborts the active attempt on a direct-layer or final-tail miss instead of
  falling through to staged host-capable helpers. Remaining host-pointer ABI
  surface is legacy fallback/oracle code plus setup-time host sources.
- [x] Keep the safe staged path only as a correctness oracle. The staged prefill
  route is still covered by the `test-metal-gemma4-prefill-frame` stage-sync
  anchor, but normal active-frame decode no longer consumes the staged per-op
  path after a direct-path miss.

Acceptance: the hot layer path does not bounce through host-visible tensors
between sub-ops.

### Phase 4: Make Frames Production-Safe

- [x] Make `MetalTensor` and runtime scratch frame-aware.
- [x] Stop reusing scratch slots while submitted frames may still reference
  them.
- [x] Gate the known unsafe active-frame path away from the safe oracle.
- [ ] Replace the conservative gate with a graph/frame allocator that plans the
  whole prefill or decode frame before encoding.
- [ ] Add tests that intentionally stress retained views, scratch reuse,
  gathered spans, and tail logits across submitted frames.
- [ ] Re-enable safe framed execution only after parity tests prove lifetime
  correctness.

Acceptance: frame enablement is a runtime property, not an unsafe environment
experiment.

### Phase 5: Fix Prefill Throughput

- [x] Move prompt KV publication into backend-owned device buffers.
- [x] Add tiled f32 prompt attention.
- [x] Keep multi-row prompt PLE on the direct prepared-slot path.
- [x] Route 9+ row Q8_0 prompt linears to the existing batched packed MM kernel.
- [ ] Add tuned small-prompt kernels for `qLen=2..8` and finish replacing
  remaining qLen>1 reducer paths.
- [ ] Collapse per-layer command boundaries into a runtime-owned prefill graph
  or block submission.
- [x] Add prompt benchmark buckets for `pp10`, `pp128`, and `pp512`.
  `zig build bench-metal-prefill-buckets -Dmetal=true` runs the
  real Metal CLI against fixed prompt buckets plus a short-prompt decode bucket
  so future kernel work is measured against pp/tg buckets instead of noisy
    4-token smoke runs. Current local sample on Gemma4 Q8_0 after the Q8_0
    r2/r3/r4/r5 small-prompt split:
  - pp10: `decoder_gated_prefill_ops: tokens=10`, `generate=1150ms`,
    `prefill_direct_family=926ms`
  - pp128: `decoder_gated_prefill_ops: tokens=128`, `generate=1945ms`,
    `prefill_direct_family=979ms`
  - pp512: `decoder_gated_prefill_ops: tokens=512`, `generate=5196ms`,
    `prefill_direct_family=1774ms`
  - tg16: `generate=1731ms`, `greedy_direct=736ms`, token sequence
    `10979 236888 2088 740 564 1601 611 3124 236881 103453 106 106 106 106 106 106`
  - tg16: `target_prompt_tokens=10`, `--max-tokens=16`, used for decode-path
    comparisons after the same short prompt.

Acceptance: prefill speed improves because rows are processed as prompt batches,
not because another local scalar fusion hides overhead.

### Phase 6: Broaden Quant Format Coverage

- [ ] Use [GGML.md](../../../../../zig/pkg/inference/GGML.md) as the source of truth for GGUF/GGML format
  compatibility.
- [ ] Keep correctness fallback coverage broad in codec/native paths.
- [ ] Add Metal fast paths by real model demand and measured hot-path value.
- [x] Prefer one packed descriptor and dispatch table over per-model quant
  entrypoints.

Acceptance: adding a quant format extends the descriptor/kernels/tests, not the
model executor API.

## Current Performance Diagnosis

The likely largest remaining costs are:

- The active decode call is still effectively a frontend-orchestrated layer
  loop. It owns one Metal command buffer per token now, but inside that buffer
  it still emits `41` compute encoders on the current Gemma4 row-1 frame. The
  ggml-shaped target is a
  backend-owned graph/program that owns the layer loop, scratch lifetimes,
  dependency ranges, and encoder scopes.
- Q8_0 decode and prefill kernels are not yet ggml-quality packed `mul_mv` /
  `mul_mm` kernels. The target is ggml-style common `mul_mv` / `mul_mm`
  dispatch with format-specific dequant functions, not another isolated Q8_0
  reducer variant.
- qLen=2..8 prefill now uses descriptor-owned Q8_0 small-batch kernels with
  r2/r3/r4/r5 row-count specialization, but those kernels are still
  reducer-style. Larger prompt linears still need ggml-quality kernel tuning
  beyond the first MM routing fix.
- Current prefill has resident attention pieces, but not a production
  flash-attention-class prefill kernel family. The open item is to make the
  backend own scale, mask/windowing, softmax, PV accumulation, KV page/span
  metadata, and residual/output projection scheduling as one planned attention
  region instead of relying on local helper composition.
- Row-1 f32 attention now uses the tiled backend kernel for bounded KV/head
  shapes, which materially reduces decode wait time on the Gemma4 smoke
  (`greedy_direct` dropped from roughly `110ms` to `36ms` for the 3-token
  decode slice in one local run). Full ggml-style paged attention still needs
  block-table/page metadata and a backend kernel that can own quantized KV,
  sinks/softcap, and long-context tiling.
- The hot token path still has too many command boundaries and too much runtime
  orchestration around small operations.
- The active decode path has very high encoder churn inside each single command
  buffer. The next planner step is to track per-op read/write ranges, group
  compatible compute dispatches into larger encoder scopes, and insert explicit
  memory barriers where ranges conflict, matching ggml's dependency-aware
  encoding shape more closely.
- Blind persistent compute-encoder reuse is not sufficient; it lowered encoder
  count but hurt elapsed time. Treat encoder count as a diagnostic and reduce
  it through real fused layer/prefill kernels and planned command regions.
- The active decode gathered-KV append path is capacity-backed and the
  non-shared K/V suffix rows are written directly into planned cache
  destination views. The decode frame no longer has KV suffix blit encoders.
  Prefill and less common fallback paths still need the same destination-owned
  treatment where they rebuild or concatenate KV rows.
- The f32-KV/Q8_0 fused attention-residual path is active and faster than the
  staged path on warm short prompts, but the current `mul_mv` / `mul_mm`
  kernels are still far from llama.cpp/ggml throughput.
- Some fused paths are local fusions around slow primitives. They reduce
  overhead but do not close the llama.cpp-class gap without better quant
  kernels.
