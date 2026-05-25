# Termite Metal Plan

## Scope

Metal is Termite's pure Apple GPU backend. This document tracks the current
production direction, benchmark anchors, and remaining work for the Metal
runtime. It is not an experiment journal; old debugging trails belong in git
history.

Status bullets below may mention older benchmark counters when they explain a
decision. They are evidence, not the architectural target. The target is the
ggml-shaped graph/allocator/command-planner backend described at the top of
this file.

This file fits with the other architecture docs:

- [GRAPH.md](GRAPH.md): generic graph/runtime ownership, model runtime
  boundaries, compiled backend attachment, and graph memory planning.
- [GGML.md](GGML.md): GGUF/GGML format compatibility plus the upstream ggml
  execution shape we use as a reference.
- this file: the concrete Metal backend plan, kernels, runtime
  session shape, scratch/frame ownership, and performance gap.

## Direction

The durable architecture is: simplify upward, specialize downward.

- `lib/ml/src/graph` owns the generic graph IR, passes, liveness, and graph
  execution contracts.
- `pkg/termite/src/graph` owns model/session runtime concerns: phase-aware
  prefill/decode, KV mutation, token IO, sampling, scheduling, rollback, and
  `ModelRuntime` / `ModelExecutor` attachment.
- Metal owns backend-native storage, quantized weights, kernels, command
  submission, scratch lifetime, and device-resident KV/cache buffers.
- Model-family code should express structural contracts such as decoder layer,
  attention, PLE, MoE, quantized matmul, and decode batch. It should not grow a
  second model executor inside backend helper files.

The ggml lesson is not one magic tile constant. ggml keeps a simple structural
graph above the backend, then lets the Metal backend choose kernels by tensor
type, quant format, shape, and device capability. Termite should follow that
shape: generic runtime contract above, format-specific packed kernels below.
This is a ggml-shaped dispatch target inside Termite, not a dependency on
`libggml`.

### North Star

Termite's Metal backend should converge on the same broad shape as upstream
ggml's Metal backend:

- The model/frontend builds a graph or layer contract. It does not orchestrate
  individual Metal helper calls in the token loop.
- The Metal runtime receives the whole prefill/decode frame, plans tensor
  lifetimes and scratch slots, then encodes backend ops in dependency order.
- The command planner tracks read/write resource ranges, groups compatible ops
  into compute encoder scopes, and inserts explicit Metal buffer barriers only
  at real hazards.
- Quantized matmul is one shared backend primitive with descriptor-driven
  format dispatch. QKV, attention output, gate/up/down FFN, PLE, embedding
  projection, and LM head all use that surface.
- Fusions are graph/runtime pattern selections around shared primitives:
  `MUL_MAT`, `MUL_MAT_ID`, `RMS_NORM`, `ROPE`, `FLASH_ATTN`, `GLU`, residual
  epilogues, and token tail. They should not duplicate quant decoding logic or
  grow into model-named public APIs.
- Generic staged helpers remain only as correctness or unsupported-shape
  fallbacks. They are not the normal decode/prefill path.

This means the main work is not more isolated "collapse this helper" changes.
The main work is to replace helper orchestration with a runtime-owned
MetalGraph/MetalCommandPlan, then improve the shared quant/attention kernels
inside that graph shape.

Local ggml inspection on 2026-05-01 confirmed the production shape:

- `ggml_metal_graph_compute` runs whole ggml graphs, calls graph optimize,
  keeps device memory alive, splits node ranges across a small number of
  command buffers, and encodes asynchronously. It is not a per-helper
  immediate-mode runtime. The relevant local references are
  `../ggml/src/ggml-metal/ggml-metal-context.m`,
  `../ggml/src/ggml-metal/ggml-metal-ops.cpp`, and
  `../ggml/src/ggml-alloc.c`.
- `ggml_metal_op_encode_impl` tracks read/write memory ranges and inserts
  Metal memory barriers when ops cannot safely overlap. This is the right
  model for reducing our active-frame compute encoders: planned encoder
  regions with dependency barriers, not one global persistent encoder. The
  failed persistent-encoder experiment proved that simply keeping one encoder
  open is not equivalent to ggml's dependency planner.
- ggml reserves graph extra buffers for attention padding/block/temp scratch
  up front to avoid reallocating while encoding. Termite's graph-plan slots
  should keep moving in that direction rather than allocating scratch inside
  individual layer helpers.
- `Q8_0 x F32` qLen=1 uses `kernel_mul_mv_q8_0_f32` with `N_R0_Q8_0=2`,
  `N_SG_Q8_0=4`, shared-memory reduction, and normal `MUL_MAT` graph nodes.
- qLen=2..8 uses `kernel_mul_mv_ext_*` small-batch mat-vec variants.
- qLen>8 uses the simdgroup matrix path when supported.
- ggml's Metal quant coverage is a templated kernel family: per-format
  dequant functions feed common `mul_mv`, `mul_mm`, and `mul_mm_id` surfaces.
  Termite should continue toward one packed-weight descriptor and one quant
  dispatch surface, not one-off model or format entrypoints.
- ggml does not get its speed from a Gemma-specific monolithic transformer
  layer kernel. It gets the shape from graph allocation, dependency-aware Metal
  encoding, selected graph-op fusions, flash-attention-class kernels, and tuned
  per-format matmul kernels.
- Termite's Q8_0 qLen=1 MMV path now uses the same `N_R0=2` / `N_SG=4`
  shared-memory reduction shape as ggml. It is the canonical path because it is
  correctness-equivalent and keeps future kernel work aligned with the reference,
  even though this swap alone did not improve the 16-token Gemma4 decode anchor.
  The previous Q8_0 qLen=1 reduce kernel has been removed from the runtime so
  single-row Q8_0 dispatch has one implementation: `termite_q8_0_linear_mmv`.
  The Q8_0 pair and QKV qLen=1 kernels now use the same naming convention
  (`termite_q8_0_pair_linear_mmv`, `termite_q8_0_qkv_linear_mmv`) because they
  are the same MMV-style shared-memory dispatch shape, not scalar fallbacks.
  A trial switch of the Q8_0 gate/up activation qLen=1 path from the old
  `1r_ext` kernel to the shared-memory `reduce4` shape preserved token IDs but
  regressed the 4-token Gemma4 anchor badly, so that path stays on `1r_ext`
  until we build a real ggml-style fused FFN kernel rather than swapping reducer
  shapes in isolation.
  The remaining work is a broader ggml-shaped graph/command planner plus
  production packed quant kernels under one matmul dispatch surface.
- The largest evidenced gap is structural: Termite's active decode still
  enters a Zig layer loop and emits `41` compute encoders for one
  decode-token frame, while ggml executes a planned graph through backend op
  dispatch. Kernel quality matters, especially for qLen=2..8 prompt batches,
  but the current command/encoder explosion is already large enough to explain
  a major part of the order-of-magnitude difference.

## Current Status

- `--backend metal` builds without an external Apple tensor-runtime dependency
  when configured with `-Dmetal=true`.
- The Gemma4 4-token anchor is correct on the current safe path:
  `Hi! How can`, token ids `10979 236888 2088 740`.
- `gelu_new` now lowers as a backend activation kind instead of decomposing
  into frontend elementwise `x^3 -> tanh -> multiply` stages. This fixed the
  observed qLen>1 prompt NaN where `ffn_gate=11.367456` became
  `ffn_gate_act=NaN` and then poisoned the first prompt row.
- The default Gemma4 path keeps the 4-token anchor correct on the Metal safe
  path while qLen>1 prefill coverage is still mixed between planned runtime
  pieces and staged fallbacks.
- The qLen>1 f32-KV/Q8_0 prefill path no longer probes the old gathered-F32
  monolithic direct block. That block was decode-shaped and produced
  `rows=10 rc=-13 stage=4` failures before falling back. The current validator
  anchor has `f32_q80_direct_fail=0`, token ids `10979 236888 2088 740`, and no
  frame-blit traces. Remaining work is to broaden the planned paged-attention
  and FFN block coverage so qLen>1 uses real planned ops rather than safe
  staged fallback.
- `MetalKvStorage` paged metadata is now per-layer shape aware. The hook uses
  each layer's `num_kv_heads` and `head_dim` for raw f32, f16, int8-per-head,
  Polar4, and Turbo3 row layout instead of rejecting mixed Gemma layer shapes
  against one storage-wide KV shape. While an active frame is open it can also
  reserve/expose the physical slot metadata before the slot has committed
  tokens, which lets planned `decode_kv_seed -> attention_paged` consume the
  same in-frame physical page table.
- Q8_0 weights remain quantized and resident. The hot path should not
  dequantize whole dense weights.
- The Gemma4 prefill layer contract now owns QKV or shared-Q projection,
  row-aware head norm/RoPE, prompt KV span seed/update, attention, FFN, PLE,
  scalar output scale, and reusable layer scratch.
- Gemma4 qLen>1 Q8_0 prefill setup now has a dedicated no-blit Metal setup
  encoder for Q/QKV projection, Q/K head RMS/RoPE, and optional V norm. The
  staged setup helpers remain as fallback, but the active prefill route no longer
  opens helper blit encoders around that setup sequence.
- qLen>1 Q8_0 prefill setup and block apply can consume one continuous
  planner-produced layer contract when the full Q8_0/f32-KV + PLE shape matches:
  setup starts at op 0, and the block helper starts at the attention op in the
  same plan. Unsupported qLen>1 block shapes now return to the caller's safe
  staged path instead of attempting the legacy gathered monolithic direct block.
- qLen>1 prefill frames are enabled again for the Gemma4 Q8_0/f32-KV path. The
  current `hi --max-tokens 4` anchor passes with one prefill frame submit
  (`metal_decoder_frame: begins=1 submits=1`) and token ids
  `10979 236888 2088 740`.
- Attention planning now separates KV dtype from KV storage layout. Dense f32
  KV still selects `attention_flash`, while paged f32 KV selects
  `attention_paged`; Polar4/Turbo3 remain under the quantized-KV attention
  family. This matches the ggml-shaped distinction between tensor type and
  backend storage rather than treating raw f32 KV as inherently dense.
- Dense f32 graph SDPA now carries an `attention_flash` `OperatorPlan` at
  partition time, and `metal_partition_executor` consumes that plan directly
  instead of falling through the interpreter. The validator-backed regression
  test asserts a device-resident output, one planned operator dispatch, and zero
  interpreter fallbacks for a fused-SDPA graph node. The same pass also fixed
  the raw `termite_sdpa_f32` encoder's optional bias/mask bindings so Metal
  validation does not abort when those optional inputs are disabled.
- Shared-KV prefill frame plans now carry a `kv_layer_index` donor, so planned
  shared-KV attention consumes the donor layer's KV resources instead of
  reconstructing shared-Q/shared-KV setup in frontend code.
- The active qLen=1 paged Q8_0/f32-KV decode block is enabled by default. The
  Metal validator anchor remains correct (`token_ids: 10979 236888 2088 740`)
  while dispatching successfully through
  `decode_kv_seed -> attention_paged -> Q8_0 FFN/PLE` and the 4-token Gemma4
  validator anchor is correct. The correctness bug was the active paged block
  writing a freshly allocated output while the decode loop consumed the
  untouched reserved hidden buffer; the paged runtime now has an `Into` form
  that writes the caller-owned MetalTensor.
- The graph planner now has a backend-neutral quant matmul selector
  (`quant_matmul.zig`) with ggml-style dispatch buckets: scalar fallback, MMV,
  small-batch, and MM. Runtime command ops can carry that planned dispatch
  metadata, so Q8_0 is the first populated format rather than the only
  architectural target.
- Planned layer contracts now carry quant-matmul dispatch metadata across the
  Zig -> Metal ABI. The planned Q8_0 setup, direct layer block, and tail helpers
  can consume planner-selected dispatch buckets, with local shape validation
  before falling back to the runtime selector.
- The active direct Q8_0 block now threads those planned buckets through the
  fused FFN gate/up activation and PLE-gate activation helpers too. These
  helpers still have Q8_0-specific kernel bodies, but their dispatch bucket is
  now part of the shared Graph/Metal contract.
- The active direct Q8_0 block now builds encoder-local quant-matmul
  descriptors for Q/QKV setup, attention output, FFN gate/up, FFN down, PLE
  gate, PLE projection, and the tail LM head. The descriptors carry epilogue
  kind, buffers, activation metadata, and planned dispatch while reusing the
  already-open planned encoder.
- Q8_0 `NONE`, `PAIR`, `QKV`, `PAIR_ACTIVATION_MUL`,
  `ACTIVATION_RHS_MUL`, and `PAIR_ACTIVATION_RMS_SCALE_1X` encoder paths now
  share descriptor-native implementation templates. The older Q8_0 raw-linear,
  pair, QKV, gate/up, FFN RMS-scale gate/up, and PLE gate helper entry points
  are compatibility callers that build descriptors, so the active linears are no
  longer split between descriptor routing and separate dispatch-selection bodies
  for those epilogues.
- Unused command-buffer-only helper functions for Q4_0/Q4_K/Q5_K/Q6_K linears
  and the Q8_0 pair/QKV/pair-activation raw wrappers have been removed; callers
  now go through descriptor records.
- `NONE` descriptors now also cover the broader scalar quant format set on an
  encoder-local path: Q1_0, I2_S, I8_S, Q2_K, Q3_K, Q4_0, Q4_1, Q4_K,
  Q5_0, Q5_1, Q5_K, Q6_K, Q8_1, Q8_K, IQ4_NL, IQ4_XS, and MXFP4. Existing
  Q4_0/Q4_K/Q5_K/Q6_K row-1 reduce kernels are still selected through that
  descriptor path instead of via command-buffer-only helper functions.
- Shared-KV prefill layers now use the same structural layer contract instead
  of doing shared-Q setup in frontend code.
- Prompt KV/span refresh can consume device-backed Q/K/V tensors directly.
- Dense f32 prompt attention has a tiled `qLen > 1` Metal prefill path.
- Q8_0 prompt linears with 9 or more rows route to the simdgroup MM bucket
  instead of the decode-style MMV path. The 10-token Gemma4 anchor now shows
  `metal_q8_0_dispatch: mm=270` for prefill-shaped linears.
- Runtime quant slot preparation now uses one packed-weight descriptor and
  block-layout table across the currently wired Metal quant formats instead of
  per-format validation copies.
- Runtime quant slot prepared state is now one prepared-format array per slot,
  not one boolean array per quant type.
- The Metal runtime prepare ABI is now format-tagged
  `prepare_quantized_linear_slot(format, ...)`; the new code does not keep
  per-format prepare wrappers.
- The Objective-C runtime now has a generic quant linear slot record for the
  shared device apply path and memory accounting: format, prepared bit, in/out
  dims, block layout, and packed weight buffer are no longer sourced from a
  per-format switch there.
- Q8_0/Q8_1/Q8_K runtime execution no longer keeps duplicate per-format slot
  arrays; the active Q8 paths read packed weights through the generic slot view.
- Dense weight handles that carry a backend-native quantized view now pass that
  view into decoder runtime linear preparation generically, rather than only
  for the final LM head. Unsupported quant formats still stay on the explicit
  dense path until the Metal quant kernel exists.
- `termite smoke --inspect-only` now reports the largest non-quantized GGUF
  tensors as well as quantized samples. Use that when checking whether a
  "Q8_0" model file still contains dense 2D tensors that the Metal backend
  should treat as explicit dense matmuls.
- GGUF BF16 tensors are now preserved by `tensor_store` instead of being
  widened to f32 during lazy loading. Metal dense linear slots can upload BF16
  weights directly and select BF16 dense kernels. On the Gemma4 anchor, the
  PLE model projection slot moved from one f32 dense slot
  (`dense_f32_mb=52`) to one BF16 dense slot (`dense_bf16_mb=26`) while
  preserving token IDs. `--print-timing` now reports dense f32/BF16 slot
  counts and requested weight bytes separately from Metal's allocation bucket.
- Active decoder frames now expose encoder-count and source attribution
  telemetry. The 4-token Gemma4 anchor is down to one command buffer per decode
  token and the latest active frame has `0` blit encoders. Planned encoder
  scopes now cover active row-1 attention setup from pre-attention RMS through
  Q8_0 QKV/shared-Q projection and head RMS/RoPE, row-1 attention apply +
  Q8_0 output projection + post-attention RMS/add, row-1 FFN pre-gate RMS
  scale + Q8_0 gate/up activation + Q8_0 down projection + post-down RMS/add,
  and row-1 PLE gate/activation + projection + post-norm residual/output-scale.
  Attention setup, attention apply, attention output projection, FFN, and PLE
  now encode through a single layer-owned planned scope for the active
  Q8_0/f32-KV block, so the old per-layer attention/FFN planned encoders are no
  longer present in the decode frame. The layer block now consumes the
  planner-produced barrier flags for its internal attention/FFN/PLE ops, so the
  live barrier placement comes from the Graph/Metal dependency contract instead
  of a second hard-coded sequence in the Objective-C helper.
  Final Q8_0 greedy tail now also uses one planned tail encoder for final RMS,
  LM head, and argmax. Greedy argmax now uses a parallel block reduction over
  logits instead of scanning the whole vocabulary on one GPU thread; on the
  4-token anchor this moved `greedy_direct` from roughly `938ms` to `134ms`
  and total generation from roughly `18.6s` to `1.76s` while keeping token IDs
  stable. The final greedy RMS also uses the parallel reduce RMS kernel instead
  of the old single-thread row kernel. The latest 4-token anchor keeps token IDs
  `10979 236888 2088 740`, reports `planned_scopes=36`,
  `planned_barriers=422`, and brings last-frame compute encoders down to
  `41`. The 16-token correctness anchor remains
  token IDs `10979 236888 2088 740 564 1601 611 3124 236881 103453 106 106
  106 106 106 106`; recent 16-token timing is noisy because prompt prefill
  still dominates and jitters, so use the 4-token row-1 counters as the active
  decode command-shape anchor. Current attribution has split out the main active
  buckets:
  `quant_linear=0`, `quant_qkv=0`, `quant_pair_act=0`, `attention=0`,
  `rms_norm=1`, `head_rope=0`, `ffn=0`, `ple=1`, `tail=1`, `embedding=2`,
  `dense_linear=1`, `layer=35`, and `other=0`. Compute-region attribution now
  also shows the planned layer regions: `attention=0`, `attention_project=0`,
  `ffn_norm=0`, `ffn=0`, `ple=4`, `tail=1`, `embedding=1`, `layer=35`, and
  `other=0`.
  The FFN pre-norm is now owned by the direct FFN runtime path instead of being
  orchestrated by the outer block, and per-layer output scale is owned by the
  active direct block instead of a post-block frontend multiply. Q8_0 embedding
  lookup now also accepts the model embedding scale so token and PLE embedding
  setup do not need separate scale kernels. That confirms the remaining
  ggml-shaped planner work is mostly kernel quality and larger runtime-owned
  graph/layer submissions rather than frontend cleanup around the active
  single-token layer loop.
- `TERMITE_METAL_TRACE_FRAME=1` now enables a generic debug trace for
  substantial frames that prints the last frame's `region x source`
  compute-encoder matrix. `TERMITE_METAL_TRACE_FRAME=all` includes small
  prefill/setup frames too. Use it when deciding which layer contract to
  collapse next; source-only counters cannot distinguish, for example, `other`
  encoders in attention setup from `other` encoders in FFN or tail work.
- Planned compute barriers are now range-driven in the same broad shape as
  ggml. The active planned compute encoder tracks read/write byte ranges for
  encoded operations; source/source overlap is allowed, but any overlap
  involving a previous write emits `memoryBarrierWithScope:MTLBarrierScopeBuffers`
  and clears the tracker. The sweep now covers the planned Q8_0 layer path,
  paged KV seed/attention, prefill V value norm, embeddings, dense and quant
  linear including pair/QKV dense helpers, RMS/layer norm, head/RoPE,
  elementwise helpers, PLE/FFN fallback scoped ops, dense attention fallback,
  ternary `where_select`, tail fallback,
  slice helpers, and argmax partial/reduce handoff. Quant matmul descriptor
  leaves now prepare their own ranges instead of relying on the descriptor
  router, so future direct helper use keeps the same invariant. Remaining explicit barriers are limited to the range
  tracker itself, the public emergency barrier hook, internal multi-dispatch
  kernels, and standalone non-planned single-encoder tail helpers. A 2026-05-07
  bisection found that treating planned barriers as metadata-only could let the
  realistic Q8_0 framed gated-FFN test pass and then trigger a delayed SoC
  watchdog reset roughly 90 seconds later. Build-only confirmations:
  `metal-command-20260508-000751` for the initial tracker,
  `metal-command-20260508-002316` for the full planned-helper sweep, and
  `metal-command-20260508-074520` for the follow-up direct-dispatch helper
  closure, `metal-command-20260508-075225` for the ternary helper closure, and
  `metal-command-20260508-110049` for the prefill V value-norm helper closure,
  and `metal-command-20260508-110925` for self-preparing quant descriptor
  helpers;
  no GPU rerun after the watchdog.
- PLE/token setup now uses the same planned-scope encoder coalescing as the
  layer graph. On the 2026-05-07 Gemma4 compiled smoke, the prefill PLE frame
  dropped from 7 compute encoders to 1, and the following decode frame dropped
  from 6 compute encoders to 1 (`metal-command-20260507-225419`). This removes
  command submission as the dominant explanation for the remaining 130ms-class
  prefill frame; the remaining gap is the dense BF16 PLE model projection kernel
  and layer math.
- `TERMITE_METAL_TRACE_GRAPH_PLAN=1` prints graph-plan commit summaries, and
  `TERMITE_METAL_TRACE_GRAPH_PLAN=all` also prints requested slot sizes. Graph
  plan readiness now uses allocated capacity rather than the last request set,
  and graph-plan buffers grow geometrically. On the 4-token Gemma4 anchor this
  collapsed scratch planning from `graph_plan_count=3`, `graph_plan_allocs=41`,
  `graph_plan_mb=5` to `graph_plan_count=1`, `graph_plan_allocs=21`,
  `graph_plan_mb=6`, while preserving token IDs.
- The first trace run on the 4-token Gemma4 anchor showed too much setup work
  in `other`: `attention=100`, `attention_project=70`, `ffn=140`, `ple=105`,
  `tail=1`, and `other=115`. Region scopes now cover active attention setup,
  decode-frame PLE setup, final tail, and output-scale fallback. Moving the
  active per-layer output scale into the direct block contract, fusing Q8_0
  embedding scale, and fusing PLE setup `add + scale` dropped the latest trace
  to: `attention=170`, `attention_project=70`, `ffn=140`, `ple=109`,
  `tail=3`, `embedding=1`, and `other=0`. Source-level attribution now shows
  `dense_linear=1` inside PLE setup instead of an unnamed helper. This makes
  the next work concrete: either make that PLE model projection arrive as a
  quantized/backend-packed tensor like ggml would, or keep it as an explicit
  dense backend matmul if the model contract truly requires dense; then collapse
  FFN/PLE/attention-projection region kernels and move embedding/PLE setup into
  the runtime-owned decode program, rather than chasing another isolated qLen=1
  reducer.
- The first real ggml-style RMS fusion is now in the PLE block: PLE post
  `rms_norm + residual add + layer_output_scale` uses fused Metal kernels for
  both `qLen == 1` and row-batched prefill. This removed two compute encoders
  per prefill PLE layer on the 4-token Gemma4 anchor (`total_compute_encoders`
  `2283 -> 2213`) while preserving token IDs. It does not reduce the latest
  decode frame's `493` compute encoders yet; the next collapse needs to target
  the active single-token decode layer regions, not the prompt prefill PLE
  tail alone.
- Row-batched attention/FFN residual epilogues now use the same fused
  `rms_norm + residual add` row kernel instead of separate row RMS plus add
  dispatches. On the same 4-token Gemma4 anchor this reduced total compute
  encoders again (`2213 -> 2143`) with unchanged token IDs. The latest
  single-token decode frame is still `493` compute encoders, which means the
  next material decode improvement is not another standalone RMS/add epilogue;
  it is `mul_mv`-owned norm handling or larger matmul+epilogue kernels with a
  tiling scheme that can respect the full-vector reduction.
- A Q8 gate/up kernel that recomputed FFN pre-RMS inside every output tile was
  correct, but it was the wrong kernel shape: last-frame encoders fell
  `531 -> 496`, while the 4-token anchor regressed badly because the full
  hidden-vector RMS was reread for each tile. That path was removed. The
  production direction is either materialize pre-RMS once in runtime-owned
  scratch, as now, or build a larger layer kernel whose tiling computes the
  reduction once and reuses it across the quantized projections.
- The bounded ggml-shaped replacement now computes the FFN pre-RMS inverse
  scale once per single-token row, then feeds that scalar plus the norm weights
  into the Q8 gate/up pair kernel. This keeps correctness anchored
  (`10979 236888 2088 740`) and avoids the per-output-tile RMS reread. Warm
  4-token Gemma4 `hi` is about `1260ms` on the current machine, with the same
  `531` last-frame compute encoders at the time. This was a small
  kernel-quality win, not
  the larger ggml-style graph/kernel fix.
- The Q8_0 direct FFN path now also has the matching post-gate RMS fusion for
  contracts that use it: compute the gated-vector inverse RMS once, then feed
  that scalar and the post-gate norm weights directly into the Q8_0 down MMV
  kernel. This avoids materializing `normed_gated_buffer` on the single-token
  post-gate path while preserving ggml's shape: reductions are computed once,
  quantized projections still use the shared packed matmul primitive, and the
  intermediate gated vector is not recomputed per output tile.
- The generic PLE fallback now uses the backend `rms_norm + residual add`
  primitive instead of orchestrating post-PLE RMS and add as two runtime calls.
  This gives non-Q8 PLE formats the same epilogue shape as the Q8_0 direct PLE
  path without adding a format-specific public API. The public runtime wrapper
  also retains the RMS-add params buffer when encoding into an active frame, so
  row-batched frame users do not rely on Objective-C autorelease lifetime.
- The `.metal` backend uses the native Metal session/provider path. GGUF Metal
  runtime availability is tied to Termite's native `MTLDevice` probe before
  model load.
- Active decode now passes per-layer output scale into the direct
  f32-KV/Q8_0 gated block. That removes the separate post-block scale multiply
  from the active layer loop and drops the latest single-token frame from
  `531` to `496` compute encoders while preserving the 4-token anchor token
  IDs.
- Q8_0 embedding lookup now takes an embedding scale and writes scaled f32
  output directly. This removes the separate active setup scale kernels for
  token and PLE embeddings. PLE setup also uses a fused `add + scale` device
  helper, so the latest single-token frame is `493` compute encoders.
  Embedding has its own compute source/region now, and the active frame's
  region-level `other` bucket is `0`.
- A naive persistent compute encoder experiment reduced the last-frame compute
  encoder count from `531` to `17`, but regressed the 4-token anchor from
  roughly `1.3s` to `7.6s`. Do not blindly keep one encoder open across the
  frame; the production fix needs explicit fused kernels / planned encoder
  scopes with correct barriers, not generic encoder reuse.
- Active-frame blit attribution showed the last-frame blits were generic
  buffer copies, not KV span encoder copies. A capacity-backed gathered-KV
  append path reduced the 4-token anchor's last-frame blits from `61` to `30`
  by avoiding full prefix recopy on every decode append. Grouping the K/V
  suffix append into one runtime blit encoder per layer reduced that to `15`.
  The active decode layer now reserves the gathered-KV destination row before
  K/V post-processing, so K head-norm/RoPE and V norm write directly into the
  cache. The 4-token anchor's last-frame blits are now `0`; do not spend more
  time on blit cleanup until compute command planning is addressed.
- Host fallback single-linear execution now has one format-tagged quantized
  linear ABI for Q1_0, I8_S, Q2_K, Q3_K, Q4_0, Q4_1, Q4_K, Q5_0, Q5_1, Q5_K,
  Q6_K, Q8_0, Q8_1, Q8_K, IQ4_NL, IQ4_XS, and MXFP4. The old per-format host
  wrapper symbols are gone. I2_S still keeps its special activation-quantized
  host path.
- I2_S, Q4_0, Q4_K, Q5_K, and Q6_K pair/QKV/attention/FFN execution now read
  packed weights through the generic quant slot view too. Their duplicate
  Objective-C per-format slot arrays have been removed.
- `test-metal-gemma4-prefill-block-parity` validates staged-vs-block behavior
  and the direct Q8_0 block path.
- Active-frame batching is still gated for the conservative safe oracle. When
  `TERMITE_METAL_DISABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK=1` selects the safe
  staged path, both the decoder-runtime layer frame and backend-owned active
  decode frame are disabled. That is a correctness guard, not the final runtime
  shape.
- Graph-planned scratch now covers projection buffers, direct Q, direct block
  hidden scratch, sample-tail logits, hot hidden scratch, and hot FFN/PLE
  scratch. Hot helpers reject unplanned allocation instead of growing the graph
  mid-frame.
- Prefill-layer scratch planning must reserve hot hidden slots for the larger
  of `rows * hidden_size` and `rows * attention_input_size`. Gemma4 uses
  attention input width 2048 with hidden width 1536, and under-reserving this
  scratch caused the fused attention-residual path to fail at stage 3 while an
  active frame was open.
- `--print-timing` reports Metal memory and scratch pressure, including runtime
  prepared quant slots, lazy host mirrors, gathered spans, and pending frame
  scratch. Quant runtime prepare also reports private-upload vs mapped-shared
  slot counts/bytes/timing. GGUF quant weights are already mmap-backed; the
  Metal runtime now tries `newBufferWithBytesNoCopy` for borrowed, unpacked
  quant storage and falls back to private upload unless
  `TERMITE_METAL_FORCE_MAPPED_QUANT_WEIGHTS=1` is set. Use
  `TERMITE_METAL_DISABLE_MAPPED_QUANT_WEIGHTS=1` to force the old private path
  for A/B timing.

## Benchmark Anchors

These are local directional anchors, not absolute device claims.

- Current compiled partitioned graph anchor, Gemma4 Q8_0 short prompt:
  `TERMITE_GRAPH_EXECUTOR_STATS=1` with `--backend metal --mode compiled
  --compiled-target partitioned --max-tokens 1 --temperature 0` reports
  `interpreter_fallbacks=0`, `host_outputs=0`, `device_outputs=819`, and
  `planned_commands=141` on the default fused path. A current local run on
  2026-05-05 reported `prefill=998ms`, `total=998ms`, and token id `10979`.
  This is the right residency milestone, but it is not the same as ggml-class
  throughput.
- A detailed timing run with `TERMITE_DEBUG_METAL_TIMING=1` reported
  `metal_decoder_frame: begins=1 submits=1 wait_ms=23 gpu_ms=22
  last_compute_encoders=15 total_compute_encoders=942 total_blit_encoders=53`
  for the same short prefill. That means the slow prefill gap is mostly not
  raw GPU kernel time in one attention op. It is command/encoder volume,
  many small planned graph commands, remaining device blits/copies, and
  non-ggml-quality quant matmul kernels.
- The latest fused gated-FFN graph path is enabled by default for the matched
  Gemma gated FFN pattern. Use
  `TERMITE_METAL_DISABLE_GATED_FFN_GRAPH_FUSION=1` to compare against the
  staged path. A recent local A/B dropped graph executor commands from `1134`
  to `924` and planned commands from `211` to `176`; elapsed time is still
  noisy enough that command reduction is the stronger regression signal.
- The latest fused attention-output-residual graph path is also enabled by
  default for matched Gemma attention output strips:
  `fused_gqa_causal_attention -> optional rms_norm -> o_proj -> optional
  rms_norm -> residual add`. Use
  `TERMITE_METAL_DISABLE_ATTENTION_OUTPUT_RESIDUAL_GRAPH_FUSION=1` for A/B
  comparisons. A local validation run reduced graph executor commands from
  `980` to `819`, planned commands from `176` to `141`, and warm prefill from
  `1034ms` to `998ms`; correctness stayed at token id `10979` with zero
  interpreter fallbacks and zero host outputs.
- The recent wrong-token fast path was a runtime slot-key bug, not a math
  difference in the fused FFN path. Native dense byte-only RMS weights had empty
  host slices and collided when the dynamic RMS slot key used `data.ptr`; the
  key now uses the native dense buffer identity.
- Termite Gemma4 short prompt prefill: after enabling the fused
  f32-KV/Q8_0 attention-residual block, the Debug 10-token chat-template `hi`
  anchor is correct and fully fused. Recent warm runs show roughly `0.49s`
  `decoder_gated_prefill_ms.block` and roughly `1.2s` prefill-family time.
  Cold runs after rebuild can still be much slower from Metal/runtime setup
  noise.
- Termite Gemma4 greedy decode: roughly `15-17 tok/s` on the small anchor.
- The 2026-05-07 RMS-add PLE fallback change passed `zig build test-bin`
  through the Metal wrapper (`metal-command-20260507-220244`) and rebuilt
  binary validation smokes for 1, 2, and 3 generated tokens
  (`metal-command-20260507-220836`, `metal-command-20260507-221011`,
  `metal-command-20260507-221023`). The repeated 4-token validation command
  failed during session creation with `MetalDeviceUnavailable`, before kernel
  execution, and produced no diagnostic reports
  (`metal-command-20260507-221033`).
- The native-provider boundary fix rebuilt successfully through the Metal
  wrapper (`metal-command-20260507-222912`) and `zig build test-bin`
  passed (`metal-command-20260507-223021`). The 4-token Gemma4 compiled
  whole-model smoke now passes API validation (`metal-command-20260507-222950`)
  with token IDs `10979 236888 2088 740`, `prefill=157ms`, `decode=149ms`,
  `total=1006ms`, and no diagnostic reports.
- Recent llama.cpp reference on the same model class:
  - prompt processing `pp10`: about `346 tok/s`
  - token generation `tg16`: about `101 tok/s`

Interpretation:

- Decode is still roughly `7-8x` slower than llama.cpp.
- Short-prompt prefill remains much farther behind than decode.
- "Attention owns the full path" currently means specific active attention
  subpaths are resident and fused enough to avoid host fallback. It does not
  mean the whole qLen>1 prefill frame is a ggml-style backend-owned graph with
  compact encoder scopes and flash-attention-class kernels.
- The remaining gap is kernel quality plus graph/submission structure, not
  missing one-off ownership cuts.
- Copying ggml's qLen=1 Q8_0 reduction shape alone is not sufficient in this
  runtime: the active decode benchmark did not improve. Treat that as evidence
  that the larger graph/kernel system is the target.

## llama.cpp Comparison

Yes: compare against Homebrew llama.cpp now. Termite has enough device-resident
coverage that the remaining gap is a real performance gap, not just an artifact
of obvious host fallback.

Use `../ggml` for implementation inspection, but use the Homebrew llama.cpp
binaries for measured baselines on this machine:

```sh
llama-bench \
  -m ~/.termite/models/ggml-org/gemma-4-e2b-it-gguf/gemma-4-E2B-it-Q8_0.gguf \
  -ngl 99 \
  -p 10,128,512 \
  -n 16 \
  -r 3 \
  -o md
```

For the smallest local anchor, run:

```sh
llama-bench \
  -m ~/.termite/models/ggml-org/gemma-4-e2b-it-gguf/gemma-4-E2B-it-Q8_0.gguf \
  -ngl 99 \
  -p 10 \
  -n 16 \
  -r 1 \
  -o md
```

A current Homebrew llama.cpp build (`41a63be28`, package revision `8980`) on
the local M4 Max reports:

```text
pp10: 345.84 tok/s
tg16: 100.96 tok/s
```

Run the comparable Termite commands through the debug wrapper so a bad Metal
run leaves a bundle:

```sh
TERMITE_GRAPH_EXECUTOR_STATS=1 \
bash pkg/termite/scripts/debug_metal_command.sh command \
  --label termite-gemma4-metal-pp10-tg1 \
  --timeout 60 \
  --no-validate \
  --cwd "$PWD" \
  -- pkg/termite/zig-out/bin/termite generate \
    ~/.termite/models/ggml-org/gemma-4-e2b-it-gguf \
    hi \
    --backend metal \
    --mode compiled \
    --compiled-target partitioned \
    --max-tokens 1 \
    --temperature 0 \
    --print-token-ids \
    --print-timing
```

For A/B checks against the gated-FFN fusion:

```sh
TERMITE_METAL_DISABLE_GATED_FFN_GRAPH_FUSION=1 \
TERMITE_GRAPH_EXECUTOR_STATS=1 \
bash pkg/termite/scripts/debug_metal_command.sh command \
  --label termite-gemma4-metal-pp10-tg1-no-gated-ffn-fusion \
  --timeout 60 \
  --no-validate \
  --cwd "$PWD" \
  -- pkg/termite/zig-out/bin/termite generate \
    ~/.termite/models/ggml-org/gemma-4-e2b-it-gguf \
    hi \
    --backend metal \
    --mode compiled \
    --compiled-target partitioned \
    --max-tokens 1 \
    --temperature 0 \
    --print-token-ids \
    --print-timing
```

Compare these fields, in order:

- llama.cpp: `pp` tokens/sec and `tg` tokens/sec from `llama-bench`.
- Termite: `generate_timing_ms`, `prefill`, decode timing, token IDs, and
  `graph_executor_stats`.
- Termite residency counters: `interpreter_fallbacks=0` and `host_outputs=0`
  should stay true before treating timing as a backend-performance signal.
- Termite command shape: total `commands` and `planned_commands` should not
  regress when adding fusions or planner regions.

If llama.cpp stays much faster while Termite reports zero host outputs and zero
interpreter fallbacks, the next fixes are not more residency work. They are:

- ggml-quality packed quant `mul_mv` / `mul_mm` kernels for Q8_0 and the other
  hot quant formats.
- fewer Metal command/encoder regions through dependency-aware graph planning,
  not one helper call per small op.
- flash-attention/paged-attention kernels that own the long-context path rather
  than staged helper composition.
- larger fused graph patterns only when they select shared backend primitives;
  avoid model-named monoliths that duplicate quant decoding logic.

## Production Architecture

### Runtime Boundary

`ModelRuntime.decodeBatch` / `decoderRuntimeDecodeBatch` is the long-term
stateful boundary.

Inputs should be backend-owned or imported once:

- token ids
- positions
- sequence ids
- KV logical views and cache offsets
- attention mode metadata
- sampling/argmax policy

The runtime call should own:

- embedding
- optional PLE
- all decoder layers
- final norm
- LM head
- sampling or argmax
- token writeback
- KV mutation results

For single-stream greedy decode, a token helper can be a batch-size-1 wrapper.
The implementation should not loop over batch items and call a token executor N
times.

### Backend Primitives

Metal should expose a small set of structural backend primitives:

- `quantizedMatmul`
- `rmsNorm`
- `rope`
- `attention`
- `kvUpdate`
- `gatedFfn`
- `ple`
- `decodeLayer`
- `decodeBatch`

Model-family code may select contracts and metadata, but the backend owns the
packed-weight layout, command encoding, scratch plan, and device lifetimes.

### Quantized Weights

The hot path should use one descriptor-driven quantized `mul_mat` surface shared
in shape with CUDA, WebGPU, and native fallback:

- format
- block size
- row stride
- scale/min layout
- packed byte layout
- input/output dtype
- row and column shape
- qLen dispatch bucket

Dispatch buckets should be selected by a backend-agnostic Zig helper, not by
private backend threshold tables:

```zig
pub const QuantMatmulKind = enum {
    scalar,
    mmv,
    small_batch,
    mm,
};

pub const QuantMatmulShape = struct {
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    qtype: gguf_tensor_types.KnownTensorType,
};

pub fn selectQuantMatmul(shape: QuantMatmulShape) QuantMatmulKind;
```

The selector should mirror the useful ggml shape:

- `qLen = 1`: decode MMV / `mul_mv`
- small `qLen` such as `2..8`: MMV-ext style prompt batches
- larger `qLen`: batched MM/GEMM-style kernels

Metal may still choose a different actual kernel when a pipeline is unavailable,
but the preferred bucket and accounting vocabulary should be shared across
Metal, WebGPU, CUDA, and native fallback.

Fused blocks must call this shared quant primitive. They should not duplicate
quant decoding logic.

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
the reference implementation shape, but these names are Termite graph/backend
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

- [ ] Use [GGML.md](GGML.md) as the source of truth for GGUF/GGML format
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

## Reference Files

Termite:

- `pkg/termite/src/graph/backend_contracts.zig`
- `pkg/termite/src/ops/metal_compute.zig`
- `pkg/termite/src/backends/metal_runtime.zig`
- `pkg/termite/src/backends/decoder_gated_runtime.zig`
- `pkg/termite/src/backends/metal_executor.zig`
- `pkg/termite/src/graph/decode_state_runtime.zig`
- `pkg/termite/src/gguf/tensor_types.zig`

Local ggml reference:

- `../ggml/src/ggml-metal/ggml-metal-context.m`
- `../ggml/src/ggml-metal/ggml-metal-ops.cpp`
- `../ggml/src/ggml-metal/ggml-metal-device.cpp`
- `../ggml/src/ggml-metal/ggml-metal.metal`

## Verification

Use these checks after changing Metal runtime behavior:

```sh
zig build test-metal-gemma4-prefill-block-parity -Dmetal=true --summary failures
zig build -Dmetal=true -Donnx=false --summary failures
LIST_ONLY=1 bash pkg/termite/scripts/debug_metal_command.sh unit 'metal|Metal'
RUN_MODE=isolated USE_PREBUILT_UNIT=1 bash pkg/termite/scripts/debug_metal_command.sh unit --api-validate 'metal|Metal'
./zig-out/bin/termite generate ~/.termite/models/ggml-org/gemma-4-e2b-it-gguf hi --backend metal --max-tokens 4 --print-token-ids --print-timing
```

Run Gemma4 Metal timing through the crash-debug wrapper:

```sh
env TIMEOUT_SECS=180 LABEL=metal-gemma4-mapped-quant-default \
  bash pkg/termite/scripts/debug_metal_command.sh command --api-validate \
  --cwd /Users/ajroetker/go/src/github.com/antflydb/antfly-zig \
  -- ./pkg/termite/zig-out/bin/termite generate \
  /Users/ajroetker/.termite/models/ggml-org/gemma-4-e2b-it-gguf hi \
  --backend metal --mode compiled --compiled-target whole-model \
  --max-tokens 4 --print-token-ids --print-timing

env TIMEOUT_SECS=180 LABEL=metal-gemma4-private-quant-baseline \
  TERMITE_METAL_DISABLE_MAPPED_QUANT_WEIGHTS=1 \
  bash pkg/termite/scripts/debug_metal_command.sh command --api-validate \
  --cwd /Users/ajroetker/go/src/github.com/antflydb/antfly-zig \
  -- ./pkg/termite/zig-out/bin/termite generate \
  /Users/ajroetker/.termite/models/ggml-org/gemma-4-e2b-it-gguf hi \
  --backend metal --mode compiled --compiled-target whole-model \
  --max-tokens 4 --print-token-ids --print-timing

env TIMEOUT_SECS=180 LABEL=metal-gemma4-force-mapped-quant \
  TERMITE_METAL_FORCE_MAPPED_QUANT_WEIGHTS=1 \
  bash pkg/termite/scripts/debug_metal_command.sh command --api-validate \
  --cwd /Users/ajroetker/go/src/github.com/antflydb/antfly-zig \
  -- ./pkg/termite/zig-out/bin/termite generate \
  /Users/ajroetker/.termite/models/ggml-org/gemma-4-e2b-it-gguf hi \
  --backend metal --mode compiled --compiled-target whole-model \
  --max-tokens 4 --print-token-ids --print-timing
```

The 4-token Gemma4 anchor should remain:

```text
10979 236888 2088 740
```

## Crash Debug Tooling

Use `pkg/termite/scripts/debug_metal_command.sh` for Metal commands that may
hang, abort, or trigger GPU validation failures. It captures stdout/stderr,
process samples, filtered unified logs, and new DiagnosticReports into one
bundle under `pkg/termite/.debug` by default. Use `--out-dir` only when you
need a different location.

Examples:

```sh
bash pkg/termite/scripts/debug_metal_command.sh command -- ./zig-out/bin/termite --help
bash pkg/termite/scripts/debug_metal_command.sh command --api-validate -- ./zig-out/bin/termite embed ~/.termite/models/antflydb/clipclap --text "hello"
RUN_MODE=chunked CHUNK_SIZE=4 bash pkg/termite/scripts/debug_metal_command.sh unit --no-validate 'metal eager graph|metal_compute'
```

Do not run unfiltered `zig build test` through `command` mode for Metal
debugging. A broad test run can launch many Metal runtime tests in one process
and has produced SoC watchdog reboots without preserving the failing test name.
The wrapper refuses that shape by default; use `unit` mode so
`current_test.txt`, `progress.tsv`, and per-test bundles identify the last
started test or chunk. `TERMITE_ALLOW_BROAD_METAL_TEST=1` exists only for
deliberate override.

If a machine-level reset interrupts `unit` mode, resume from the same output
directory instead of starting over:

```sh
RESUME=1 RESUME_SKIP_CURRENT=1 \
RUN_MODE=isolated USE_PREBUILT_UNIT=1 \
bash pkg/termite/scripts/debug_metal_command.sh unit \
  --out-dir pkg/termite/.debug/metal-unit-YYYYMMDD-HHMMSS \
  --api-validate 'metal|Metal'
```

`RESUME=1` preserves `progress.tsv` and skips entries already marked `PASS`.
`RESUME_SKIP_CURRENT=1` also skips the label left in `current_test.txt`, which
is the best suspect after a watchdog reboot.

For reboot bisection, prefer disabling post-capture during unit isolation:

```sh
TERMITE_METAL_SKIP_POSTCAPTURE=1 \
RESUME=1 RESUME_SKIP_CURRENT=1 \
RUN_MODE=isolated USE_PREBUILT_UNIT=1 \
bash pkg/termite/scripts/debug_metal_command.sh unit \
  --out-dir pkg/termite/.debug/metal-unit-YYYYMMDD-HHMMSS \
  --api-validate 'metal|Metal'
```

This still writes each test's stdout, exit status, `current_test.txt`, and
`progress.tsv`, but skips `log show` and DiagnosticReports copying after each
successful command. If the machine reboots, inspect system
`/Library/Logs/DiagnosticReports/Retired/panic-base-*.panic` directly.

Validation modes:

- Default sets both `MTL_DEBUG_LAYER=1` and `MTL_SHADER_VALIDATION=1`.
- `--api-validate` sets `MTL_DEBUG_LAYER=1` without shader validation. Prefer
  this when shader validation prevents device creation or makes a small repro
  much slower.
- `--no-validate` disables both validation variables but still captures stdout,
  logs, samples, exit status, and crash reports.

The wrapper now propagates the captured command exit code. Unit mode should stop
on the first failing chunk or isolated test instead of reporting success when a
bundle contains `exitcode.txt != 0`.

For machine-level resets, check both the bundle and system panic reports. The
wrapper writes `started_epoch.txt` and `diagnostic-reports-before.txt`, and it
copies new user/system `.ips`, `.crash`, `.panic`, and `.diag` reports into
`diagnostic-reports/` when the command exits normally. If the machine reboots
before the wrapper can finish, compare `started_epoch.txt` with
`/Library/Logs/DiagnosticReports/Retired/panic-base-*.panic`; recent SoC
watchdog resets have shown up there rather than as `termite-*.ips` reports.

The default sample delay is `5s`. That keeps short compile-only `zig build test`
filters from being sampled while the compiler is reading cache/std-lib files,
but still captures long-running Metal commands. In this sandbox, backgrounded
`zig build` invocations can fail with `PermissionDenied` even when the same test
passes in the foreground; use foreground `zig build test -Dmetal=true ...` for
compile-only capability filters, and use the debug wrapper for already-built
Termite commands or long-running Metal executions.

## Rejected Directions

- Do not bring another Apple tensor runtime back into the pure Metal backend.
- Do not use environment variables as the primary backend selection surface.
- Do not dequantize whole dense weights in the Metal hot path.
- Do not add model-named public runtime APIs when a structural graph/layer
  contract can express the same work.
- Do not add new monolithic transformer-layer helpers as the primary
  optimization strategy. If a fused kernel is needed, it should be selected by
  the graph/runtime planner and built on the shared backend primitives.
- Do not keep disabled experimental kernels in production files unless they are
  behind tests and part of the descriptor dispatch plan.
- Do not treat one local fusion win as a substitute for the ggml-shaped packed
  quant matmul and graph/frame allocator work.

# Generalize Existing Metal Command Planner

## Summary

The Metal command planner is the canonical graph command-plan abstraction; do
not introduce a parallel planner. `GraphCommandPlan`, `GraphCommandPlanView`,
and `GraphCommandOp` own ordered op records, resource ranges, encoder scopes,
barrier placement, scratch lifetimes, and operator metadata. Model-specific
paths lower into this generic command plan through temporary lowerers.

Success criteria:
- No new parallel planner is introduced.
- `GraphCommandPlan` remains the canonical generic command plan.
- Specialized types such as `GatedFrameCommandLowerer` remain lowering helpers
  or disappear as the generic frame executor takes over.
- Metal prefill consumes one generic frame command plan, not per-layer specialized slices.
- Existing token/residency anchors remain correct.

## Key Changes

- Generic planner surface.
  - Use `GraphCommandPlan`, `GraphCommandPlanView`, and `GraphCommandOp` in
    production code.
  - Keep `ResourceRange`, `ResourceUse`, `EncoderScope`, `ScratchSlotLifetime`, `OperatorPlan`, and `QuantMatmulPlan` as shared concepts.
  - Do not add compatibility aliases for the old runtime-command names.

- Generalize op kinds.
  - Replace decode-specific `OpKind` names with structural names:
    `rms_norm`, `qkv_linear`, `head_norm_rope`, `kv_seed`, `attention`, `attention_output_linear`, `residual_norm_add`, `ffn_gate_up`, `ffn_down`, `ple_gate`, `ple_projection`, `tail_norm`, `lm_head`, `argmax`, `sample`, `quant_get_rows`, `quant_set_rows`, `quant_copy`, etc.
  - Keep phase, qLen, KV layout, quant format, and activation in metadata, not in the enum name.
  - Update tests to assert structural op names plus operator metadata rather than decode-prefixed names.

- Add a real `FrameDescriptor`.
  - Define one backend-neutral frame descriptor for command-plan lowering.
  - Include frame mode (`prefill`, `decode`, `embedding`, `classification`), batch/query lengths, sequence positions, requested outputs, KV mutation policy, KV layout, activation dtype, and backend target.
  - Replace scattered prefill/decode-only fields where practical; keep model-specific layer specs separate from backend execution policy.

- Convert specialized builders into generic lowerers.
  - Keep current Gemma/Q8/f32-KV logic as a lowering path initially, but make it emit generic `GraphCommandOp`s into `GraphCommandPlan`.
  - Keep `GatedFrameCommandLowerer`, `GatedLayerCommandLowerer`,
    `PrefillGatedLayerCommandLowerer`, and related helpers as lowerers, not as
    the public planner abstraction.
  - The specialized lowering helper may still validate Q8_0/f32-KV support, but the output plan must be generic.

- Make Metal consume the generic frame plan.
  - Change Metal runtime entrypoints to accept `GraphCommandPlanView`.
  - Execute the whole prefill frame from that view instead of slicing per-layer command views and returning to Zig orchestration.
  - Use the existing resource ranges/scopes/barriers to group encoder work and reduce command/encoder churn.
  - Unsupported op/operator combinations must return explicit unsupported diagnostics and counters.

## Migration Order

1. Mechanical rename.
   - Use generic type names at imports/call sites.
   - Keep old names out of production source.
   - Run unit tests to verify no behavior change.

2. Structural op-kind migration.
   - Introduce generic op names and map old decode-prefixed values to the new names.
   - Update planner tests and Metal cursor validation.
   - Keep operator plans unchanged.

3. Frame descriptor introduction.
   - Add `FrameDescriptor`.
   - Use it in the Gemma prefill/decode lowering path.
   - Preserve existing `DecoderRuntimePrefillFramePlanRequest` as an adapter until all call sites migrate.

4. Generic Gemma prefill lowering.
   - Build a generic `GraphCommandPlan` using `FrameDescriptor` plus Gemma
     layer specs.
   - Keep the existing specialized lowerer only while it is the adapter from
     Gemma metadata to graph command records.

5. Whole-frame Metal execution.
   - Make Metal execute the generic prefill plan directly.
   - Remove per-layer command-view slicing from the active prefill path.
   - Track `commands`, `planned_commands`, `total_compute_encoders`, and `total_blit_encoders`.

6. Cleanup.
   - Remove deprecated aliases and specialized production planner names.
   - Update `METAL.md` and `GRAPH.md` to describe the final abstraction.

## Test Plan

- Mechanical tests:
  - Run existing planner and Metal unit tests after rename.
  - Add compile-time checks or grep-style tests only if the repo already has that pattern; otherwise keep tests behavioral.

- Planner tests:
  - Assert Gemma4 prefill and decode emit the same structural op names where appropriate.
  - Assert qLen 2..8 selects `mul_mv_ext`, qLen >= 9 selects `mul_mm`.
  - Assert attention operator metadata selects `attention_flash` or `attention_paged` correctly.

- Metal correctness tests:
  - Run `test-metal-gemma4-prefill-block-parity`.
  - Run `hi --max-tokens 1 --temperature 0` and assert token `10979`.
  - Run existing 4-token anchor and assert `10979 236888 2088 740`.
  - Assert `interpreter_fallbacks=0` and `host_outputs=0`.

- Performance checks:
  - Run with `TERMITE_GRAPH_EXECUTOR_STATS=1 TERMITE_DEBUG_METAL_TIMING=1`.
  - Record `commands`, `planned_commands`, `total_compute_encoders`, `total_blit_encoders`, `prefill`, and `gpu_ms`.
  - Compare against current local baseline: `commands=924`, `planned_commands=176`, `total_compute_encoders=942`, `total_blit_encoders=53`, `prefill≈1194-1331ms`.

## Assumptions

- This is a refactor plus wiring change, not a new planner implementation.
- Existing graph command-plan semantics are the source of truth.
- Metal is the first consumer, but names and contracts should be backend-neutral enough for WebGPU/native later.
- Specialized Q8_0/f32-KV checks can remain internally during migration, but not in the final public abstraction names.

# Whole-Frame Metal Graph Execution Plan

## Summary
The current refactor made `GraphCommandPlanView` the generic command-plan shape, and the Metal backend now has frame-level Gemma4 prefill planning/execution hooks wired through `ComputeBackend`. That is real infrastructure, not just a design target. Accepted qLen>1 Q8_0 prefill layers now route setup plus attention/FFN/PLE block work through one composed runtime layer dispatch, so the fast path no longer bounces from Zig into separate setup and block runtime calls. The remaining gap is that the accepted frame still gets sliced back into per-layer `PlannedLayerContract` windows. The next larger performance step is to make Metal consume the full prefill `GraphCommandPlanView` as one backend-owned op stream, with explicit timing that separates host encode/orchestration cost from GPU work.

Target outcome for the Gemma4 `hi --max-tokens 1` Metal smoke:
- Correct token remains `10979`.
- `interpreter_fallbacks=0`, `host_outputs=0`.
- `commands` and `total_compute_encoders` drop materially from the current `924` / `942`.
- Prefill improves only if command/encoder count drops; do not claim success from naming/refactor alone.

## Key Changes
- Continue the frame-level Metal execution entrypoint.
  - `decoderRuntimePlanPrefillFrame` and `decoderRuntimeExecuteGraphCommandPlanFrame` exist and are called by Gemma4 direct prefill for qLen>1.
  - The first supported contract remains Gemma gated RMS + PLE shared-KV prefill only; unsupported plans return `false` with diagnostics, preserving current fallback behavior.
  - The executor now derives layer and tail windows with a structural cursor over `frame_plan.view()`, so it no longer depends on the lowerer's side-channel layer starts for accepted frames.
  - The cursor now feeds one composed runtime dispatch for each accepted Q8_0 prefill layer, carrying the setup and block contracts together. The remaining part is direct execution of the full cursor as one op stream instead of per-layer windows plus a tail contract.

- Move planning-to-execution ownership into Metal.
  - Keep `GatedFrameCommandLowerer` as the temporary Gemma-to-`GraphCommandPlan` lowerer.
  - Stop using per-layer `PlannedLayerContract` slices for the accepted prefill fast path.
  - Build a Metal-side command-plan cursor over `GraphCommandOp` records and encode by structural op kind: setup/QKV, KV seed, attention, attention output, FFN, PLE, tail norm/head.
  - Keep existing helper kernels initially, but call them from one frame executor so scope/barrier decisions are centralized.

- Collapse encoder scopes before adding new kernels.
  - Use `GraphCommandPlanView.scopes` as the source of truth for compute encoder grouping.
  - Within a scope, encode all supported ops into the active encoder and insert planned barriers only where `barrier_before` requires it.
  - Do not add model-named monolithic kernels in this slice; use existing quant/attention/norm primitives behind structural op dispatch.

- Add missing timing and counters.
  - Add counters for `frame_plan_ops`, `frame_plan_scopes`, `frame_plan_scope_encoders`, `frame_plan_encode_ms`, `frame_plan_submit_ms`, `frame_plan_wait_ms`, and `frame_plan_gpu_ms`.
  - Keep existing `graph_executor_stats` fields, but distinguish node-level graph commands from frame-plan commands.
  - Print a diagnostic reason when full-frame execution declines: unsupported op kind, unsupported operator plan, shape mismatch, missing prepared slot, scratch reservation failure.

## Test Plan
- Unit planner tests:
  - Assert `GatedFrameCommandLowerer.view()` emits one contiguous frame plan with expected structural op kinds and scratch lifetimes.
  - Add a test that full-frame eligibility rejects unsupported op/operator combinations without mutating frame state.
  - Add a test that frame-scope cursor groups ops by `scope_index` and preserves planned barriers.
  - Keep accepted-frame cursor tests that prove execution derives layer/tail windows from the full frame plan rather than lowerer side-channel views.

- Metal executor tests:
  - Add a focused mock/fake runtime test for `decoderRuntimeExecuteGraphCommandPlanFrame` that verifies op dispatch order, scope begin/end counts, and barrier count.
  - Keep fallback tests proving unsupported frames still run through current per-layer helpers.

- Runtime smoke:
  - Build `pkg/termite` with `-Dmetal=true -Doptimize=ReleaseFast`.
  - Run Gemma4 unsandboxed through `debug_metal_command.sh`.
  - Acceptance for this slice: token `10979`, no fallbacks/host outputs, no diagnostic reports, and reduced command/encoder counts versus `commands=924`, `total_compute_encoders=942`.
  - Record both cold and warm runs; use warm run for performance comparison.

## Assumptions
- Optimize command/encoder orchestration before adding new Metal kernels.
- Preserve current correctness fallback paths until full-frame execution is proven.
- Scope this slice to Gemma gated prefill; decode and ClipClap/ONNX graph execution use the same abstractions later but are not required for first success.
- Existing backend hooks and active-frame plumbing are not the blocker. The blocker is replacing per-layer contract slicing inside the accepted frame with a single command-plan cursor and scope/barrier executor.
- Performance success is measured by command/encoder reduction plus warm prefill timing, not by GPU time alone, because current `gpu_ms≈17` while prefill is about `1s`.

# Metal Graph Command-Volume Reduction Plan

## Summary
Reduce real Gemma4 Metal graph command volume by moving from per-node partition execution toward ggml-style whole-graph region planning. Use `../ggml` as the reference model: optimize the graph first, fuse only when liveness/aliasing proves safety, reorder independent regions by memory ranges, then encode larger runtime regions instead of many helper calls.

Current anchor: Gemma4 compiled partitioned Metal is around `commands=819`, `planned_commands=141`, `interpreter_fallbacks=0`, `host_outputs=0`, `prefill=998ms`.

Target for this chunk: keep correctness and residency, reduce enabled command volume to `<=500` with a stretch target of `<=250`.

## Key Changes
- Continue the Metal graph region planner on top of the existing partition plan.
  - Runtime region planning/execution already exists in the partition executor for Q linear, QKV, RMS/grouped QKV, attention-output residual, FFN residual, and PLE residual patterns.
  - Existing diagnostics already track region counts and fallbacks, so new work should extend those counters instead of adding parallel statistics.
  - The remaining region work is integration: promote compatible regions into a larger whole-frame command sequence instead of executing many small planned scopes.

- Implement ggml-style fusion eligibility.
  - Use existing use-count/last-use checks plus buffer/resource ranges as Termite’s equivalent of `ggml_can_fuse`.
  - Fuse only when intermediates have no escaping uses.
  - Reject write/read or write/write overlap; allow source-source overlap.
  - Preserve stateful order for KV writes, paged attention, rope position mutation, and requested graph outputs.

- Extend frame/region execution for Metal.
  - The backend frame hook exists for Gemma4 prefill; extend the implementation so it consumes the region/frame plan directly.
  - Other backends keep the existing interpreter/partition path.
  - If a region is not supported, it falls back to the current node executor with an explicit fallback reason.
  - Keep this backend-neutral at the graph interface level; no Gemma-specific public API.

- Fuse the highest-volume real regions first.
  - Attention region: QKV projection, Q/K normalization or reshape/transpose layout ops, rope, fused/paged GQA attention, output projection, residual/norm where eligible.
  - FFN region: up/gate projections, activation, elementwise multiply, down projection, residual/norm where eligible.
  - Tail region: final norm, LM head, argmax/sampling setup where eligible.
  - Prefer existing kernels initially; only add a small fused GLU/activation-multiply kernel if current activation+mul still creates avoidable command churn.

- Add command-volume diagnostics.
  - Track `graph_regions`, `graph_region_ops`, `graph_region_fallbacks`, per-region counts, compute encoders, command buffers, frame encode/wait/GPU time, and top fallback reasons.
  - Keep `interpreter_fallbacks`, `host_outputs`, and `device_outputs` semantics correct after region execution.
- Keep FFN intermediates explicitly typed in command plans.
  - FFN scratch now carries activation dtype intent, with f16 currently enabled for the gated activation buffer and f32 retained for projected/residual-facing buffers.
  - Planned command contracts now carry input/output activation dtype metadata through the Zig/C Metal ABI.
  - Multi-row Q8_0 prefill uses a f16 FFN route for supported descriptors, with a dedicated fused pair-activation MM output kernel and a matching f16-input Q8_0 down-projection MM kernel. The down projection still writes f32 until residual/RMS epilogues support f16 inputs.
  - The planner selects f16 automatically; runtime descriptor/pipeline checks fail closed when a specific shape, quant family, or kernel variant is unsupported.
  - Track `pair_act_mm_out_f16` and `linear_mm_in_f16` counters to prove the real prefill path is using those kernels.
  - Q8_0 pair-activation selection should prefer the fused pair-activation kernel; the split simdgroup two-matmul plus activation/multiply path is a fallback only when the fused pair kernel is unavailable.

## Test Plan
- Add planner unit tests for synthetic attention, FFN, and tail regions.
- Add negative tests for escaped intermediates, unsafe resource overlap, requested intermediate outputs, and KV-state ordering.
- Add real-model planner coverage for Gemma4 graph layout so synthetic coverage cannot drift away from production topology.
- Run CPU unit suite to ensure non-Metal graph behavior is unchanged.
- Run Metal validation through the repo’s debug wrapper only, with API validation and crash bundle capture enabled.
- A/B runtime with region execution enabled and disabled.

## Acceptance Criteria
- Gemma4 Metal compiled partitioned generation still produces the known smoke token output, including token `10979` for the existing short prompt check.
- `interpreter_fallbacks=0` and `host_outputs=0` remain true on the Gemma4 Metal smoke path.
- Enabled command volume drops from `819` to `<=500`; stretch target `<=250`.
- `planned_commands` drops from `141` to `<=100`; stretch target `<=75`.
- No Metal diagnostic crash reports from validation runs.
- If command/encoder volume improves but latency does not, accept this chunk as structural progress and record the remaining bottleneck as kernel quality or scheduling, not graph residency.

## Assumptions
- We use `../ggml` as a design reference, not as a linked dependency.
- The first implementation prioritizes Gemma4 prefill/decode graph shape, but abstractions must stay graph/backend-oriented for CLIP/CLAP/ClipClap and ONNX/GGUF/Safetensors paths.
- The current per-node executor remains the correctness fallback until each region type is proven safe.

# Metal Command Reduction Implementation Plan

## Summary
Reduce Gemma4 partitioned Metal command volume by matching ggml’s execution model more closely: view-like ops become metadata-only, frame-time descriptor construction moves into planning/load paths, and attention/epilogue chains are fused into larger resident regions. Target success is fewer Metal commands with unchanged token output, `interpreter_fallbacks=0`, `host_outputs=0`, and no Metal diagnostic reports.

## Key Changes
- Treat shape/view-only graph ops as aliases in the Metal partition executor: `reshape`, simple last-dim `slice`, and quantized `concat_prim` descriptors should not increment command dispatch or encode kernels when they can be represented as retained tensor views or descriptor metadata.
- Add a planner/cache path for grouped concat QKV weights so concat descriptor construction happens once per graph/runtime slot, not every frame.
- Add an attention-prep graph region that matches exact Gemma layouts: grouped QKV outputs, Q/K/V head RMSNorm, Q scale, Q/K rope, GQA, output projection/norm/residual.
- Extend existing region execution so scalar multiply/add epilogues fold into producer regions when the scalar/broadcast shape is safe and the result has a single expected use.
- Keep all new fusions conservative: exact op sequence, exact shapes, single-use checks, same backend device, and fall back to current execution if any condition fails.

## Implementation Steps
- First implement metadata-only view handling in `metal_partition_executor`: detect no-copy reshapes/slices and publish aliases without treating them as backend commands; add ownership tests to prevent double-free and leaked aliases.
- Move quantized concat QKV descriptors into a reusable slot/cache keyed by concat tree and quant metadata; executor should reuse the descriptor instead of rebuilding it as a command.
- Add matcher tests for real Gemma-like attention prep graphs, then implement the region in stages: QKV outputs through head norms, then rope/scale, then GQA plus existing output residual.
- Add epilogue folding for scalar `mul`/`add` after RMSNorm, attention, FFN, and modulation patterns only when current trace proves the exact producer/consumer shape.
- Update stats to distinguish `metadata_aliases`, `planned_descriptors`, and real command dispatches so reductions are visible and not hidden by counter semantics.

## Test Plan
- Run `zig build termite-test -Dmetal=false --summary failures` after each stage.
- Run focused Metal validation smoke through `pkg/termite/scripts/debug_metal_command.sh command --api-validate -- ... --backend metal --mode compiled --compiled-target partitioned`.
- Acceptance checks for Gemma4 smoke: token id remains `10979`, `interpreter_fallbacks=0`, `host_outputs=0`, no diagnostic reports, command count decreases from current `724`.
- Add unit tests for alias ownership, concat descriptor reuse, attention-prep matcher rejection on extra uses, and scalar epilogue rejection on non-scalar/broadcast-unsafe inputs.
- Keep a traced command histogram before/after each slice and document the command deltas.

## Assumptions
- Prioritize command-count and residency correctness over timing until API-validation noise is removed.
- Do not introduce broad graph rewrites yet; implement conservative executor/planner regions first.
- Treat ggml as the behavioral model for views and descriptors: metadata-only unless a real contiguous copy is required.

## Current Status: Planned Graph Region Scopes
- Gemma4 Metal compiled partitioned remains fully resident for the smoke path:
  `interpreter_fallbacks=0`, `host_outputs=0`, `graph_region_fallbacks=0`.
- PLE residual execution now supports the Q8 fused fast path and a generic device descriptor path for Q4 and other supported single-stage quant formats. Focused Metal validation covers both Q8 and Q4 PLE residual paths.
- The partition executor now pre-materializes constants/zero tensors before opening the active Metal frame, avoiding constant uploads during the hot frame where possible.
- Empty active frames are no longer submitted/waited: `flush_active_frame` cancels/restarts empty frames, and submit-and-wait cancels empty active frames before submit. The traced Gemma4 one-token run dropped from `456` frame traces with `350` empty frames to `106` frame traces with `0` empty-frame entries.
- Fused graph regions now enter planned Metal compute scopes. The graph executor opens a planned region scope for attention-output residual, FFN residual, and PLE residual. If the partition-level frame has already been flushed by preparation/runtime paths, the planned scope owns a small frame and submits it safely at scope exit.
- The traced Gemma4 one-token run now reports planned scopes in `105` of `106` frame traces, with no empty-frame entries:
  `graph_regions=120`, `graph_region_ops=635`, `graph_region_fallbacks=0`, `interpreter_fallbacks=0`, `host_outputs=0`.
- Current non-validation 8-token Gemma4 Metal compiled partitioned measurement:
  `prefill=1200ms`, `decode=4574ms`, `total=5775ms`.
- This is structural progress, not the final ggml-style execution model. The remaining bottleneck is still frame fragmentation: most graph regions submit one small planned frame each instead of one whole-frame command sequence.

## Next Required Slice
- Promote per-region planned scopes into a whole-frame `GraphCommandPlanView` executor for Gemma prefill/decode so compatible attention/FFN/PLE/tail regions share a command buffer and encoder scopes.
- Use `GraphCommandPlanView.scopes` as the source of truth for grouping; region-local scopes are the fallback, not the destination.
- Move per-frame quant descriptor/slot preparation out of execution hot paths; execution should reference prepared resident slots/descriptors.
- Add frame counters for real submitted frame count, empty-frame cancels, planned-scope count, and top frame-break reasons so regressions are visible without verbose `TERMITE_METAL_TRACE_FRAME=all`.
