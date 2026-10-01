# Metal Backend Status History

> Relocated verbatim from `zig/pkg/inference/METAL.md` (lines 131–478 and 490–548 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`METAL.md`](../../../../zig/pkg/inference/METAL.md). Durable decisions from this log were folded into that document before the move.

## Current Status bullets (relocated from METAL.md)

- `--backend metal` builds with `-Dmetal=true` (there is no `-Dmlx` flag now
  that the MLX backend has been removed).
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
- `antfly inference smoke --inspect-only` now reports the largest non-quantized GGUF
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
- In builds that include both MLX and native Metal, the `.metal` backend now
  uses the native Metal session/provider path instead of opening an MLX stream
  and constructing an MLX-backed Metal provider. This keeps GGUF Metal runtime
  availability tied to Antfly inference's native `MTLDevice` probe and prevents native
  Metal sessions from failing with `MlxMetalUnavailable` before model load.
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

## Benchmark Anchors (dated measurements)

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
- Antfly inference Gemma4 short prompt prefill: after enabling the fused
  f32-KV/Q8_0 attention-residual block, the Debug 10-token chat-template `hi`
  anchor is correct and fully fused. Recent warm runs show roughly `0.49s`
  `decoder_gated_prefill_ms.block` and roughly `1.2s` prefill-family time.
  Cold runs after rebuild can still be much slower from Metal/runtime setup
  noise.
- Antfly inference Gemma4 greedy decode: roughly `15-17 tok/s` on the small anchor.
- The 2026-05-07 RMS-add PLE fallback change passed `zig build test-bin`
  through the Metal wrapper (`metal-command-20260507-220244`) and rebuilt
  binary validation smokes for 1, 2, and 3 generated tokens
  (`metal-command-20260507-220836`, `metal-command-20260507-221011`,
  `metal-command-20260507-221023`). The repeated 4-token validation command
  failed during session creation with `MetalDeviceUnavailable`, before kernel
  execution, and produced no diagnostic reports
  (`metal-command-20260507-221033`).
- The native-provider-with-MLX boundary fix rebuilt successfully through the
  Metal wrapper (`metal-command-20260507-222912`) and `zig build test-bin`
  passed (`metal-command-20260507-223021`). The 4-token Gemma4 compiled
  whole-model smoke now passes API validation (`metal-command-20260507-222950`)
  with token IDs `10979 236888 2088 740`, `prefill=157ms`, `decode=149ms`,
  `total=1006ms`, and no diagnostic reports.
- Recent llama.cpp reference on the same model class:
  - prompt processing `pp10`: about `346 tok/s`
  - token generation `tg16`: about `101 tok/s`

