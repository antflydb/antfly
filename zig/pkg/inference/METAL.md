# Metal Backend

## Scope

Metal is Antfly inference's pure Apple GPU backend. Most of what this document
describes is shipped; it also tracks the current production direction,
benchmark anchors, and remaining work for the Metal runtime — open checklist
items are called out explicitly rather than assumed. It is not an experiment
journal; dated status and benchmark history lives under
[docs/design/inference/history/metal/](../../../docs/design/inference/history/metal/).

Status bullets below may mention older benchmark counters when they explain a
decision. They are evidence, not the architectural target. The target is the
ggml-shaped graph/allocator/command-planner backend described at the top of
this file.

This file fits with the other architecture docs:

- [GRAPH.md](GRAPH.md): generic graph/runtime ownership, model runtime
  boundaries, compiled backend attachment, and graph memory planning.
- [GGML.md](GGML.md): GGUF/GGML format compatibility plus the upstream ggml
  execution shape we use as a reference.
- [QUANT_KERNEL_COMPILER.md](QUANT_KERNEL_COMPILER.md): build-time quant kernel
  specs, generated artifacts, Metal promotion gates, and evidence flow.
- this file: the concrete Metal backend plan, kernels, runtime
  session shape, scratch/frame ownership, and performance gap.

## Direction

Portable fast paths use GPU capabilities, not device marketing names. The
shared Apple SIMD-group policy checks inherited `MTLGPUFamilyApple7` support
(M1 and later), following [Apple's feature tables](https://developer.apple.com/metal/capabilities/).
Qwen, Gemma, Florence, embedding and LightOnOCR callers retain their model and
shape contracts; dispatch still requires the compiled pipeline's SIMD width,
thread count, memory limits and buffer layout. Optional Metal 4 pipelines can
fail to compile without replacing a supported fallback with an invalid launch.
Explicit feature overrides and rollback flags remain authoritative.

LightOn local decode pipelines initialize once when the model enables its
projection policy, rather than eagerly on one named GPU. Qualification checks
live in `scripts/lightonocr3/check_metal_projections.m`: `--check-device-policy`
tests inherited capabilities, unsupported families, missing pipelines and
rollback without querying a device name; the default invocation runs numeric
and fallback oracles on the actual GPU. Device-specific benchmark numbers
elsewhere in this document describe the measured host, not an eligibility list.

The durable architecture is: simplify upward, specialize downward.

- `lib/ml/src/graph` owns the generic graph IR, passes, liveness, and graph
  execution contracts.
- `pkg/inference/src/graph` owns model/session runtime concerns: phase-aware
  prefill/decode, KV mutation, token IO, sampling, scheduling, rollback, and
  `ModelRuntime` / `ModelExecutor` attachment.
- Metal owns backend-native storage, quantized weights, kernels, command
  submission, scratch lifetime, and device-resident KV/cache buffers.
- Model-family code should express structural contracts such as decoder layer,
  attention, PLE, MoE, quantized matmul, and decode batch. It should not grow a
  second model executor inside backend helper files.

The ggml lesson is not one magic tile constant. ggml keeps a simple structural
graph above the backend, then lets the Metal backend choose kernels by tensor
type, quant format, shape, and device capability. Antfly inference should follow that
shape: generic runtime contract above, format-specific packed kernels below.
This is a ggml-shaped dispatch target inside Antfly inference, not a dependency on
`libggml`.

### North Star

Antfly inference's Metal backend should converge on the same broad shape as upstream
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
  up front to avoid reallocating while encoding. Antfly inference's graph-plan slots
  should keep moving in that direction rather than allocating scratch inside
  individual layer helpers.
- `Q8_0 x F32` qLen=1 uses `kernel_mul_mv_q8_0_f32` with `N_R0_Q8_0=2`,
  `N_SG_Q8_0=4`, shared-memory reduction, and normal `MUL_MAT` graph nodes.
- qLen=2..8 uses `kernel_mul_mv_ext_*` small-batch mat-vec variants.
- qLen>8 uses the simdgroup matrix path when supported.
- ggml's Metal quant coverage is a templated kernel family: per-format
  dequant functions feed common `mul_mv`, `mul_mm`, and `mul_mm_id` surfaces.
  Antfly inference should continue toward one packed-weight descriptor and one quant
  dispatch surface, not one-off model or format entrypoints.
- ggml does not get its speed from a Gemma-specific monolithic transformer
  layer kernel. It gets the shape from graph allocation, dependency-aware Metal
  encoding, selected graph-op fusions, flash-attention-class kernels, and tuned
  per-format matmul kernels.
- Antfly inference's Q8_0 qLen=1 MMV path now uses the same `N_R0=2` / `N_SG=4`
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
- The largest evidenced gap is structural: Antfly inference's active decode still
  enters a Zig layer loop and emits `41` compute encoders for one
  decode-token frame, while ggml executes a planned graph through backend op
  dispatch. Kernel quality matters, especially for qLen=2..8 prompt batches,
  but the current command/encoder explosion is already large enough to explain
  a major part of the order-of-magnitude difference.

### Command Plan Abstraction

`GraphCommandPlan` (with `GraphCommandPlanView` and `GraphCommandOp`) is the
canonical graph command-plan abstraction for Metal: it owns ordered op
records, resource ranges, encoder scopes, barrier placement, scratch
lifetimes, and operator metadata. No parallel planner should be introduced;
model-specific paths (Gemma gated prefill/decode today) lower into this
generic plan through temporary lowerers such as `GatedFrameCommandLowerer`
rather than becoming a second public planner abstraction.

Op kinds in the plan are named structurally, not by decode/prefill role:
`rms_norm`, `qkv_linear`, `head_norm_rope`, `kv_seed`, `attention`,
`attention_output_linear`, `residual_norm_add`, `ffn_gate_up`, `ffn_down`,
`ple_gate`, `ple_projection`, `tail_norm`, `lm_head`, `argmax`, `sample`,
`quant_get_rows`, `quant_set_rows`, `quant_copy`, and similar. Phase, qLen, KV
layout, quant format, and activation dtype live in per-op metadata, not in
the op-kind enum, so the same op-kind vocabulary works across prefill,
decode, and non-decode frame kinds.

A backend-neutral `FrameDescriptor` describes command-plan lowering inputs:
frame mode (`prefill`, `decode`, `embedding`, `classification`), batch/query
lengths, sequence positions, requested outputs, KV mutation policy, KV
layout, activation dtype, and backend target. It keeps model-specific layer
specs separate from backend execution policy so the same lowering shape can
serve Gemma today and other model families later.

Planned compute barriers are range-driven: the active planned compute encoder
tracks read/write byte ranges for encoded operations, source/source overlap is
allowed, and any overlap involving a previous write emits a Metal buffer
barrier (`memoryBarrierWithScope:MTLBarrierScopeBuffers`) and clears the
tracker. This is the mechanism behind the barrier placement described above,
not a separate scheme.

## Current Status

Metal builds with `-Dmetal=true` (the removed MLX backend no longer has a
corresponding build flag). The Gemma4 short-prompt correctness anchor stays
stable across the default safe path, the planned Q8_0/f32-KV decode block,
and the default-on fused gated-FFN and attention-output-residual graph
paths. Decode already runs through the planned Q8_0/f32-KV block by default;
qLen>1 prefill still mixes planned runtime layer contracts with staged
fallback coverage, and broadening planned paged-attention/FFN block coverage
for prefill remains the main open item. The KV storage, attention-planning,
quant-matmul dispatch, and BF16-handling rules that came out of this work are
now documented in Production Architecture below.

> **Relocated:** The accreted "X now does Y" status bullets that previously
> lived here (346 lines) are preserved verbatim in
> [docs/design/inference/history/metal/status-history.md](../../../docs/design/inference/history/metal/status-history.md)
> under "Current Status bullets (relocated from METAL.md)". Durable decisions
> from them are in Command Plan Abstraction, Production Architecture, and the
> Debug And Rollback Env Vars subsection below.

**Build/backends**
- `.metal` sessions use the native Metal session/provider path, so GGUF Metal
  availability tracks Antfly inference's own `MTLDevice` probe.

**Decode path**
- Anchor: `Hi! How can`, token ids `10979 236888 2088 740` (16-token:
  `... 564 1601 611 3124 236881 103453 106 106 106 106 106 106`).
- The qLen=1 paged Q8_0/f32-KV decode block is enabled by default, dispatching
  `decode_kv_seed -> attention_paged -> Q8_0 FFN/PLE`, and writes K/V directly
  into the gathered-KV cache destination (no blit copies) by reserving the
  row before K/V post-processing.
- PLE post-block and attention/FFN residual epilogues each run as one fused
  `rms_norm + residual-add(+ output-scale)` row kernel, single-token and
  row-batched alike.
- Q8_0 embedding lookup applies the model's embedding scale directly, so
  token/PLE embedding setup needs no separate scale kernel.
- Greedy argmax and the final RMS use parallel block-reduction kernels rather
  than a single-thread scan/row kernel.

**Prefill path**
- `gelu_new` lowers as a backend activation kind, not a decomposed frontend
  `x^3 -> tanh -> multiply` sequence.
- qLen>1 Q8_0 setup runs through a dedicated no-blit encoder (Q/QKV
  projection, Q/K head RMS/RoPE, optional V norm); when the full
  Q8_0/f32-KV + PLE shape matches, setup and block apply share one continuous
  planner-produced contract, otherwise it falls back to the safe staged path.
- Dense f32 SDPA graph nodes get an `attention_flash` `OperatorPlan` at
  partition time that `metal_partition_executor` consumes directly.
- Shared-KV prefill frame plans carry a `kv_layer_index` donor so planned
  shared-KV attention reuses the donor layer's KV resources instead of
  reconstructing setup in frontend code.
- Dense f32 prompt attention has a tiled `qLen > 1` path; Q8_0 prompt linears
  with 9+ rows route to the simdgroup MM bucket instead of decode-style MMV.

**Quant matmul**
- Q8_0 weights stay quantized and resident; the hot path never dequantizes
  whole dense weights.
- Q8_0 linears (setup, layer block, tail) share one encoder-local descriptor
  path across the `NONE`/`PAIR`/`QKV`/`PAIR_ACTIVATION_MUL`/
  `ACTIVATION_RHS_MUL`/`PAIR_ACTIVATION_RMS_SCALE_1X` epilogues; `NONE` also
  covers the broader scalar format set (Q1_0, I2_S, I8_S, Q2_K, Q3_K, Q4_0,
  Q4_1, Q4_K, Q5_0, Q5_1, Q5_K, Q6_K, Q8_1, Q8_K, IQ4_NL, IQ4_XS, MXFP4).
- Metal prefers no-copy mapped buffers for already-mmap-backed GGUF quant
  weights, falling back to private upload when unsupported (rollback env
  vars below). Quant slot storage/prepare unification and the host-fallback
  ABI are in Production Architecture > Quantized Weights.
- `antfly inference smoke --inspect-only` reports the largest non-quantized
  GGUF tensors alongside quantized samples, for spotting dense 2D tensors
  inside a nominally quantized model file.

**Frames/planner**
- Graph-plan scratch readiness is capacity-based (not tied to the last
  request set) and graph-plan buffers grow geometrically.
- Graph-planned scratch covers projection buffers, direct-Q/direct-block
  hidden scratch, sample-tail logits, and hot hidden/FFN/PLE scratch; hot
  helpers reject unplanned allocation instead of growing the graph mid-frame.

**Known gaps**
- Active-frame batching still falls back to a conservative safe oracle via
  `TERMITE_METAL_DISABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK=1` (see Debug And
  Rollback Env Vars) — a correctness guard, not the target shape.
- Broadening planned paged-attention/FFN block coverage for qLen>1 prefill
  beyond the safe staged fallback remains the main open item (see the opening
  paragraph above).

## Benchmark Anchors

These are local directional anchors, not absolute device claims.

The current Gemma 4 QAT baseline/no-MTP plan, canonical 2K+300 comparator, and
promotion gates live in
[GEMMA4.md, Metal Performance Plan](models/gemma4/GEMMA4.md#metal-performance-plan). Older anchors
below remain useful implementation history, but they are not the current
llama.cpp gap unless rerun under that contract.

> **Relocated:** The dated per-run benchmark numbers that previously lived
> here (59 lines, 2026-05-05 through 2026-05-07) are preserved verbatim in
> [docs/design/inference/history/metal/status-history.md](../../../docs/design/inference/history/metal/status-history.md)
> under "Benchmark Anchors (dated measurements)". Durable decisions from them
> are captured in the interpretation below and in GEMMA4.md under Metal Performance Plan.

Interpretation of the Gemma4 E2B Q8_0 anchors (the Q4_0 QAT production
workload is much closer; see GEMMA4.md):

- On that anchor, decode was roughly `7-8x` slower than llama.cpp.
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

Yes: compare against Homebrew llama.cpp now. Antfly inference has enough device-resident
coverage that the remaining gap is a real performance gap, not just an artifact
of obvious host fallback.

Use `../ggml` for implementation inspection, but use the Homebrew llama.cpp
binaries for measured baselines on this machine:

```sh
llama-bench \
  -m ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf/gemma-4-E2B-it-Q8_0.gguf \
  -ngl 99 \
  -p 10,128,512 \
  -n 16 \
  -r 3 \
  -o md
```

For the smallest local anchor, run:

```sh
llama-bench \
  -m ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf/gemma-4-E2B-it-Q8_0.gguf \
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

Run the comparable Antfly inference commands through the debug wrapper so a bad Metal
run leaves a bundle:

```sh
TERMITE_GRAPH_EXECUTOR_STATS=1 \
bash pkg/inference/scripts/debug_metal_command.sh command \
  --label termite-gemma4-metal-pp10-tg1 \
  --timeout 60 \
  --no-validate \
  --cwd "$PWD" \
  -- pkg/inference/zig-out/bin/antfly inference generate \
    ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf \
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
bash pkg/inference/scripts/debug_metal_command.sh command \
  --label termite-gemma4-metal-pp10-tg1-no-gated-ffn-fusion \
  --timeout 60 \
  --no-validate \
  --cwd "$PWD" \
  -- pkg/inference/zig-out/bin/antfly inference generate \
    ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf \
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
- Antfly inference: `generate_timing_ms`, `prefill`, decode timing, token IDs, and
  `graph_executor_stats`.
- Antfly inference residency counters: `interpreter_fallbacks=0` and `host_outputs=0`
  should stay true before treating timing as a backend-performance signal.
- Antfly inference command shape: total `commands` and `planned_commands` should not
  regress when adding fusions or planner regions.

If llama.cpp stays much faster while Antfly inference reports zero host outputs and zero
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

The Gemma4 prefill layer contract is the concrete instance of this boundary:
it owns QKV or shared-Q projection, row-aware head norm/RoPE, prompt KV span
seed/update, attention, FFN, PLE, scalar output scale, and reusable layer
scratch. The decode layer contract owns per-layer output scale the same way,
as part of the layer rather than a frontend post-block multiply.

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

Prefill-layer scratch planning must size hot hidden slots to the larger of
`rows * hidden_size` and `rows * attention_input_size`: a model's attention
input width can exceed its hidden width (Gemma4 uses 2048 versus 1536), and
under-reserving this scratch fails a fused attention-residual path mid-frame
instead of at plan time.

### KV Storage And Attention Planning

`MetalKvStorage` paged metadata is per-layer shape aware: it uses each
layer's `num_kv_heads` and `head_dim` to select the row layout (raw f32, f16,
int8-per-head, Polar4, or Turbo3) instead of validating every layer against
one storage-wide KV shape, so mixed per-layer Gemma shapes are supported.
While an active frame is open, storage can also reserve/expose physical slot
metadata before the slot has committed tokens, which lets a planned
`decode_kv_seed -> attention_paged` sequence consume the same in-frame
physical page table.

Attention planning separates KV dtype from KV storage layout: dense f32 KV
selects `attention_flash`, while paged f32 KV selects `attention_paged`, and
Polar4/Turbo3 remain under the quantized-KV attention family. This matches
the ggml-shaped distinction between tensor type and backend storage rather
than treating raw f32 KV as inherently dense.

Any paged decode block must write into the caller-owned output buffer
through an explicit `Into` form rather than returning a freshly allocated
output. The decode loop reads the caller's reserved hidden buffer directly,
so a block that allocates its own output silently diverges from what the
loop consumes instead of failing loudly — treat "writes to a fresh
allocation instead of the caller's buffer" as a correctness bug class for
any new paged/backend-owned block, not just historical instances of it.

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

This selector is implemented as `quant_matmul.zig`, the backend-neutral quant
matmul selector with these exact dispatch buckets (scalar fallback, MMV,
small-batch, MM). Planned layer contracts carry the selected dispatch bucket
across the Zig/Metal ABI, with local shape validation before falling back to
the runtime selector; Q8_0 is the first fully populated format rather than
the only architectural target.

GGUF BF16 tensors are preserved by `tensor_store` instead of being widened to
f32 during lazy loading, so Metal dense linear slots can upload BF16 weights
directly and select BF16 dense kernels instead of falling back to f32.

Quant slot storage and prepare paths are unified rather than per-format: one
packed-weight descriptor and block-layout table covers slot validation, one
prepared-format array (not one boolean per quant type) tracks prepared state
per slot, and a single format-tagged `prepare_quantized_linear_slot(format,
...)` entrypoint replaces the old per-format prepare wrappers. The
Objective-C runtime holds one generic quant linear slot record (format,
prepared bit, in/out dims, block layout, packed weight buffer) for the shared
device apply path and memory accounting instead of per-format switches.  Host
fallback single-linear execution uses that same format-tagged quantized
linear ABI across every wired quant format; I2_S keeps a separate
activation-quantized host path.

### Qwen3 Embedding Q8_0 Kernel Defaults

The Qwen3 embedding path promotes these Q8_0 kernels as production defaults.
Each has a startup-time rollback flag; a promoted pipeline is not constructed
at all when its rollback flag is set, and a refuted opt-in pipeline is only
constructed when its enable flag is set (construct-on-demand, not a runtime
branch). The measurement protocol behind each promotion lives in
[`scripts/qwen3_embedding/BASELINE.md`](scripts/qwen3_embedding/BASELINE.md).

Default-on:

- Single-linear SG-v2: vectorized Q8_0 block dequantization, direct simdgroup
  matrix loads, barrier-free 32-row bulk store, separate ragged tail.
  Rollback `TERMITE_METAL_DISABLE_Q8_0_SG_V2=1`.
- Fused gate+up SG-v2: one activation tile feeds both Q8_0 projections plus the
  SiLU/multiply epilogue (f32 and f16-output variants), removing duplicate
  activation traffic. Rollback `TERMITE_METAL_DISABLE_Q8_PAIR_ACTIVATION_SG_V2=1`.
- F32-input M64 single-linear schedule: 64 rows x 64 output columns, eight
  simdgroups, K=32 panels, separate bulk/tail stores.
  Rollback `TERMITE_METAL_DISABLE_Q8_0_SG_M64=1`.
- Zero-bias elision: qwen3 GGUF slots carry synthesized zero biases, so skipping
  the add is a correctness-preserving identity.
  Rollback `TERMITE_METAL_DISABLE_ZERO_BIAS_ELISION=1`.
- `tryDeviceQuantizedGatedFfnResidual` batched Q8_0 device-encode path replaces
  the per-row loop and cuts dispatch count on long sequences.
  Rollback (forces the per-row path) `TERMITE_METAL_FORCE_Q8_0_GATED_FFN_ROWWISE=1`.
- `termite_attention_f32_dense_causal_sg[_q16]` flash attention replaces naive
  and tiled f32 attention for full causal self-attention from position zero.
  Eligibility: `q_len >= 8`, no bias/mask/window, `head_dim % 32 == 0`; the q16
  variant needs `q_len >= 16` and `head_dim <= 128`.
  Rollback `TERMITE_METAL_DISABLE_DENSE_CAUSAL_SG_ATTENTION=1`, or
  `TERMITE_METAL_DISABLE_DENSE_CAUSAL_SG_ATTENTION_Q16=1` for the q16 variant only.
- f16 K/V flash attention (`termite_attention_f32_dense_causal_sg_q16_f16kv`):
  per-layer f32->f16 K/V convert into two persistent private buffers, direct
  device simdgroup loads, staged tail chunk.
  Rollback `TERMITE_METAL_DISABLE_DENSE_CAUSAL_SG_ATTENTION_F16KV=1`.

Measured and refuted as defaults, kept opt-in for future hardware or schedule
work (do not re-propose as defaults without new evidence):

- F16-input M64 single-linear schedule was slower than the default f16-input
  SG-v2 M32 route at long sequence lengths.
  Opt-in `TERMITE_METAL_ENABLE_Q8_0_SG_M64_F16=1`.
- Sharing one activation tile across the QKV K/V projections was neutral.
  Opt-in `TERMITE_METAL_ENABLE_Q8_KV_PAIR_SG=1`.
- GQA head-pair attention (`..._sg_q16_f16kv_gqa2`, K/V reads shared across two
  query heads per 256-thread threadgroup) was materially slower because its
  threadgroup footprint halves occupancy.
  Opt-in `TERMITE_METAL_ENABLE_DENSE_CAUSAL_SG_ATTENTION_GQA_PAIR=1`.
- Preferring the split mm_sg route over the fused scalar
  `q8_0_pair_activation_multiply_mm` for gate+up at rows >= 129 was slower
  end-to-end; the fused kernel's per-row-group weight re-reads stay
  cache-resident. No flag; documented as refuted.

### Gemma 4 A4B High-Memory Bundle

`TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH` is the opt-in, off-by-default
umbrella for Gemma 4 26B-A4B Metal decode. It bundles: a no-copy model-wide
Metal buffer backed by a residency set, mapped routed-expert weights, fused
expert gate/up activation, cached adjusted norm weights, zero-bias elision,
shared-FFN fusion, SIMD-group RMSNorm/head-RoPE kernels, triple parallel-FFN
pre-norm, parallel-FFN post/residual fusion, RMSNorm/residual fusion, the
qualified M4 Q4_0 schedules, the split-GQA decode kernel for A4B's exact
local (16 query heads, 8 KV heads, head dim 256, window 1024) and global (16
query heads, 2 KV heads, head dim 512) geometries at KV lengths of 512 or
more, the register-based route-select kernel, the prepared A4B
concurrent-hazard executor, and the Q6_K NR4/NSG1 LM head. Every component
has its own `TERMITE_METAL_DISABLE_A4B_*` rollback (for example
`TERMITE_METAL_DISABLE_A4B_DECODE_GQA_SPLIT_FRAME_SCRATCH=1` for the
frame-owned split-GQA scratch that lets frame N+1 encode while frame N is
still submitted, or `TERMITE_METAL_DISABLE_A4B_ROUTE_SELECT_REGISTER=1` for
the route selector). The backend-owned prepared executor itself additionally
requires `TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE=1`. Both opt-ins default
off.

### M4 Serving Defaults

These serving defaults apply only on an Apple M4-family Metal device (Apple GPU
family 9 and a device name starting with `Apple M4`); CPU and other Apple
generations keep their prior defaults, and the explicit enable flags remain
available for qualification elsewhere. Device qualification is independent of
the prepared-frame rollback switches.

| Feature | Default eligibility | Set to `0` to roll back |
|---|---|---|
| Qwen Q8_0 short projections, norms, and encoder scope | Eligible dense text embedding; admitted bounded workspace; existing row/shape checks | `TERMITE_METAL_ENABLE_Q8_0_SMALL_ROWS` |
| Qwen head norm/rotary | Bounded 9–64 rows, HD128, full rotary, supported head count/SIMD width | `TERMITE_METAL_ENABLE_QWEN3_HEAD_NORM_SG` |
| Qwen bounded batching/admission | Eligible resident dense Qwen3 embedder; dynamic unpadded tokenizer; no projection | `TERMITE_METAL_ENABLE_QWEN3_EMBED_BATCHING` |
| Gemma4 E2B local flash prefill | Exact E2B MQA HD256/window-512 geometry | `TERMITE_METAL_ENABLE_E2B_FLASH_PREFILL_HD256` |
| E2B aligned Q4_0 projections | E2B dimensions, dtype, buffer alignment and pipeline checks | `TERMITE_METAL_ENABLE_E2B_Q4_0_MM_SG_ALIGNED` |
| E2B fused Q4_0 gate/up | E2B FFN dimensions and pipeline checks | `TERMITE_METAL_ENABLE_E2B_Q4_0_PAIR_ACTIVATION_MM` |
| E2B resident nucleus sampling | Exact E2B geometry and a supported compiled Metal sampling request | `ANTFLY_INFERENCE_METAL_RESIDENT_NUCLEUS` |

Empty, `false`, `no`, and `off` also disable these flags. Feature-specific
`DISABLE` flags still win; restart the server after changing controls. Missing
optional default pipelines use the existing fallback, while explicit pipeline
requests stay fail-closed. M64 short-row comparisons remain opt-in. Resident
nucleus sampling requires temperature > 0, top-k 0, 0 < top-p < 1, no min-p or
penalties, and no grammar/static suppression; other requests keep their
existing sampling path, and API sampling defaults are unchanged. E4B/A4B
policies are unchanged.

Reference result (MacBook Air M4, 16 GiB, 2026-10-02): Qwen3-Embedding-0.6B
Q8_0 short requests ran at 1.03–1.17× llama.cpp throughput (95% lower bound);
Gemma4 E2B QAT long-input greedy TTFT fell 59% (19.9 s → 8.1 s) and short
sampled decode rose 2.66× (20.3 → 54.0 tok/s) with greedy decode unchanged
within ±1% and every greedy output hash identical. The historical absolute
long-sampling targets (55 tok/s, 7 s TTFT) remain unmet. M4 Pro/Max share the
device policy but were not measured.

> **Relocated:** The full qualification receipt (protocol, per-cell results,
> soak, artifact hashes) → [history/metal/serving-defaults-m4-2026-10.md](../../../docs/design/inference/history/metal/serving-defaults-m4-2026-10.md).

### ggml-Shaped Metal Checklist

This is the concrete gap list from comparing the current Metal backend against
`../ggml`/llama.cpp's production shape.

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

In place today: descriptor-style packed quant slots with Q8_0 weights resident;
the graph-planned scratch allocator; the `GraphCommandPlan` planner (resource
ranges, encoder scopes, barriers that reset the hazard set, allocation-free
build path, scratch lifetime records, frame-level aggregation); planned quant
dispatch metadata threaded through the layer-contract ABI into every active
Q8_0 linear (setup, attention output, FFN gate/up/down, PLE gate/projection, LM
head); one packed `planned_layer_contract` across the Zig/Objective-C ABI with
typed op IDs and a shared contract cursor; runtime encoding that iterates
`GraphCommandOp` records for the active row-1 Q8_0/f32-KV layer and greedy tail
(one 15-op layer scope with 12 producer-consumer barriers per layer); a
dedicated qLen>1 `PrefillGatedLayerCommandLowerer` plus a runtime-owned prefill
frame plan (`decoderRuntimePlanPrefillFrame`, `GatedFrameCommandLowerer`) whose
layer and logits-tail slices the active prefill helpers consume; per-linear
quant formats in frame metadata, read from real Metal slot descriptors; and
two-stage parallel argmax plus parallel-reduce final RMS in the tail.

Open:

- [ ] Retire `termite_metal_select_q8_0_linear_dispatch` after the Metal encoder
  consumes the shared planned quant matmul metadata directly.
- [ ] Replace the scratch-only graph plan with a runtime-owned command plan:
  op records, resource ranges, encoder scopes, explicit barriers, and planned
  scratch lifetimes for the whole decode/prefill frame.
- [ ] Convert current layer helpers into graph op builders. During migration,
  helpers may consume planned encoder scopes, but the final shape is op records
  encoded by the runtime plan rather than direct helper orchestration.
- [ ] Extend command-plan builders to non-Q8/dense tail variants, then assemble
  embedding -> PLE -> layers -> tail as one frame command plan before any
  runtime encoding starts.
- [ ] Replace the current attention kernels with a flash-attention-class Metal
  family that owns scale, mask/bias, softcap/sinks where needed, softmax, and
  PV accumulation.
- [ ] Finish the ggml-style quant matmul family behind one descriptor dispatch:
  `qLen=1` MMV, `qLen=2..8` MMV-ext, `qLen>8` MM/GEMM, then extend beyond Q8_0.
- [ ] Extend pair/QKV/fused epilogues beyond Q8_0 and replace scalar kernels
  with ggml-quality per-format kernels behind the same descriptor ABI. (Q8_0
  QKV, attention output, FFN, PLE, and LM head already share the descriptor
  surface; non-Q8 `NONE` epilogues and the `I2_S`/`Q4_0`/`Q4_K`/`Q6_K` `PAIR`
  epilogues are descriptor-native.)
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

> **Relocated:** The completed checklist items with their per-item status
> notes, the operator-family checklist detail, and the Phase 1–6 item ledgers
> (697 lines) → [history/metal/ggml-checklist-and-phases.md](../../../docs/design/inference/history/metal/ggml-checklist-and-phases.md).

## Remaining Plan

### Operator Family Completion Checklist

This is the production operator surface we are migrating toward. ggml remains
the reference implementation shape, but these names are Antfly inference
graph/backend operators so the same plan can later target Metal, CUDA, WebGPU,
Wasm, or native fallback. `src/graph/operator_plan.zig` owns the
backend-neutral operator union and stats; command plans carry quant matmul,
quantized row, quantized copy, and attention records, and `operatorStats()`
reports selected operators and explicit fallback counts.

| Family | Covered | Open |
|---|---|---|
| `mul_mv` / `mul_mv_ext` | Q8_0 qLen=1 and r2/r3/r4/r5-specialized qLen=2..5 (`NONE`, pair, gate/up activation); packed row-1 `mul_mv` for Q5_0, Q4_1, Q5_1, Q8_1, Q8_K, IQ4_NL, IQ4_XS, MXFP4 | Production-quality shared format-helper kernels replacing the Q8_0 reducers; `mul_mv_ext` for non-Q8 formats; `mul_mv` for the remaining scalar-only formats |
| `mul_mm` | Q8_0 qLen>8 routes through the shared bucket | ggml-quality simdgroup GEMM for Q8_0; `mul_mm` for the broader quant set |
| `get_rows` / `set_rows` / `cpy_q_to_f32` / `cpy_f32_to_q` | Format-tagged prepared-slot kernels for Q8_0, Q4_0, Q4_1, Q5_0, Q5_1, Q8_1, Q4_K, Q5_K, Q6_K (Q4_1/Q5_1/Q8_1 writeback included); quantized embedding lookup, `takeRows`, and packed `fused_take_rows` use `get_rows` | IQ, MXFP4, NVFP4 row/copy kernels; replace the active-frame hidden ping-pong/returned-output pattern with planned scratch/output slots; a true strided PLE operand; the shared-KV setup miss visible under `TERMITE_METAL_TRACE_Q80_BLOCK=1` |
| Attention | `attention_flash`, `attention_paged`, `attention_quantized_kv` command ops; format-aware `attention_paged` kernel (causal/sliding mask, sinks, softcap, f32/f16/int8/Polar4/Turbo3 slots) fed by an in-frame `decode_kv_seed` op; dense f32 `fused_sdpa` executed by `metal_partition_executor` through a planned operator record | Arbitrary mask/bias; replace the older f32 tiled helper wherever the paged operator is eligible; physical page tables and graph-owned encoding for qLen>1 quantized-KV prompts |
| Graph execution | Active row-1 layer/tail command loops validate `operator`/`format` fields and fail closed when a planned contract is present but the direct block is unavailable | Replace more helper entry points with generic operator encoders and add real non-Q8 kernels behind the same records; keep generic fallback only as an explicit unsupported-format/diagnostic path |

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

- [ ] Replace remaining scalar-ish Q8_0 reducers with a production packed
  `mul_mv` / `mul_mm` family. The row-1 Q8_0 `NONE` MMV kernel uses ggml's
  two-output-row `N_R0_Q8_0` geometry; widening it to four columns preserved
  tokens but slowed the smoke run, so further tile changes need a measured
  common primitive rather than another isolated tweak.
- [ ] Extend the descriptor surface to Q4_0, Q5_0, Q8_1, K-quants, I-quants,
  MXFP4/NVFP4, and BitNet epilogues beyond `NONE`/`PAIR`, or reject them
  explicitly as slow paths.

Acceptance: prefill and decode linears route through the same quant dispatch
surface, with no dense dequant fallback in the hot path.

### Phase 3: Make Layer Submission Coarse

The active row-1 Q8_0/f32-KV path encodes one layer-owned planned scope per
layer from command-op records, uses device-buffer inputs/outputs, and aborts the
active attempt on a direct-layer or tail miss instead of falling through to
host-capable helpers. The staged prefill route survives only as the
correctness oracle covered by `test-metal-gemma4-prefill-frame`.

- [ ] Assemble embedding, setup, KV seed, layers, and tail into a single
  runtime-owned graph stream for qLen>1 prefill.

Acceptance: the hot layer path does not bounce through host-visible tensors
between sub-ops.

### Phase 4: Make Frames Production-Safe

`MetalTensor` and runtime scratch are frame-aware, scratch slots are not reused
while submitted frames may reference them, and the known unsafe active-frame
path is gated away from the safe oracle.

- [ ] Replace the conservative gate with a graph/frame allocator that plans the
  whole prefill or decode frame before encoding.
- [ ] Add tests that intentionally stress retained views, scratch reuse,
  gathered spans, and tail logits across submitted frames.
- [ ] Re-enable safe framed execution only after parity tests prove lifetime
  correctness.

Acceptance: frame enablement is a runtime property, not an unsafe environment
experiment.

### Phase 5: Fix Prefill Throughput

Prompt KV publication is backend-owned, f32 prompt attention is tiled,
multi-row PLE stays on the prepared-slot path, and 9+ row Q8_0 prompt linears
use the batched MM kernel. `zig build bench-metal-prefill-buckets -Dmetal=true`
runs the real Metal CLI against fixed `pp10`/`pp128`/`pp512` prompt buckets
plus a `tg16` decode bucket (10-token prompt, `--max-tokens=16`); measure
kernel work against those buckets rather than 4-token smoke runs.

- [ ] Add tuned small-prompt kernels for `qLen=2..8` and finish replacing
  remaining qLen>1 reducer paths.
- [ ] Collapse per-layer command boundaries into a runtime-owned prefill graph
  or block submission.

Acceptance: prefill speed improves because rows are processed as prompt batches,
not because another local scalar fusion hides overhead.

### Phase 6: Broaden Quant Format Coverage

- [ ] Use [GGML.md](GGML.md) as the source of truth for GGUF/GGML format
  compatibility.
- [ ] Keep correctness fallback coverage broad in codec/native paths.
- [ ] Add Metal fast paths by real model demand and measured hot-path value.

Prefer one packed descriptor and dispatch table over per-model quant
entrypoints. Acceptance: adding a quant format extends the
descriptor/kernels/tests, not the model executor API.

## Current Performance Diagnosis

The likely largest remaining costs are:

- The active decode call is still effectively a frontend-orchestrated layer
  loop: one Metal command buffer per token, but `41` compute encoders inside it
  on the Gemma4 E2B Q8_0 row-1 frame. The target is a backend-owned
  graph/program that owns the layer loop, scratch lifetimes, dependency ranges,
  and encoder scopes.
- Q8_0 decode and prefill kernels (including the r2–r5 small-batch kernels) are
  still reducer-style, not ggml-quality packed `mul_mv` / `mul_mm` kernels.
  The target is common `mul_mv` / `mul_mm` dispatch with format-specific
  dequant functions, not another isolated Q8_0 reducer variant.
- Prefill has resident attention pieces but no production flash-attention-class
  prefill family that owns scale, mask/windowing, softmax, PV accumulation, KV
  page metadata, and output-projection scheduling as one planned region.
- Encoder count is a diagnostic, not a goal: blind persistent compute-encoder
  reuse lowered encoder count but hurt elapsed time. Reduce it through fused
  layer/prefill kernels and range-tracked planned command regions.
- Decode KV suffix rows are written directly into planned cache destinations
  (no KV blit encoders); prefill and less common fallback paths still need the
  same destination-owned treatment where they rebuild or concatenate KV rows.
- Local fusions around slow primitives reduce overhead but do not close the
  llama.cpp-class gap without better quant kernels.

For Gemma 4 QAT Q4_0 (the production Metal workload), the current
llama.cpp comparison, gap attribution, and ranked next work are owned by
[GEMMA4.md, Metal Performance Plan](models/gemma4/GEMMA4.md#metal-performance-plan).

## Reference Files

Antfly inference:

- `pkg/inference/src/graph/backend_contracts.zig`
- `pkg/inference/src/ops/metal_compute.zig`
- `pkg/inference/src/backends/metal_runtime.zig`
- `pkg/inference/src/backends/decoder_gated_runtime.zig`
- `zig/pkg/inference/src/graph/metal_executor.zig`
- `pkg/inference/src/graph/decode_state_runtime.zig`
- `pkg/inference/src/gguf/tensor_types.zig`

Local ggml reference:

- `../ggml/src/ggml-metal/ggml-metal-context.m`
- `../ggml/src/ggml-metal/ggml-metal-ops.cpp`
- `../ggml/src/ggml-metal/ggml-metal-device.cpp`
- `../ggml/src/ggml-metal/ggml-metal.metal`

## Verification

Use these checks after changing Metal runtime behavior:

```sh
zig build test-metal-gemma4-prefill-block-parity -Dmetal=true --summary failures
zig build -Dmetal=true --summary failures
LIST_ONLY=1 bash pkg/inference/scripts/debug_metal_command.sh unit 'metal|Metal'
RUN_MODE=isolated USE_PREBUILT_UNIT=1 bash pkg/inference/scripts/debug_metal_command.sh unit --api-validate 'metal|Metal'
./zig-out/bin/antfly inference generate ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf hi --backend metal --max-tokens 4 --print-token-ids --print-timing
```

Run Gemma4 Metal timing through the crash-debug wrapper:

```sh
env TIMEOUT_SECS=180 LABEL=metal-gemma4-mapped-quant-default \
  bash pkg/inference/scripts/debug_metal_command.sh command --api-validate \
  --cwd "$ANTFLY_REPO/zig" \
  -- ./pkg/inference/zig-out/bin/antfly inference generate \
  ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf hi \
  --backend metal --mode compiled --compiled-target whole-model \
  --max-tokens 4 --print-token-ids --print-timing

env TIMEOUT_SECS=180 LABEL=metal-gemma4-private-quant-baseline \
  TERMITE_METAL_DISABLE_MAPPED_QUANT_WEIGHTS=1 \
  bash pkg/inference/scripts/debug_metal_command.sh command --api-validate \
  --cwd "$ANTFLY_REPO/zig" \
  -- ./pkg/inference/zig-out/bin/antfly inference generate \
  ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf hi \
  --backend metal --mode compiled --compiled-target whole-model \
  --max-tokens 4 --print-token-ids --print-timing

env TIMEOUT_SECS=180 LABEL=metal-gemma4-force-mapped-quant \
  TERMITE_METAL_FORCE_MAPPED_QUANT_WEIGHTS=1 \
  bash pkg/inference/scripts/debug_metal_command.sh command --api-validate \
  --cwd "$ANTFLY_REPO/zig" \
  -- ./pkg/inference/zig-out/bin/antfly inference generate \
  ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf hi \
  --backend metal --mode compiled --compiled-target whole-model \
  --max-tokens 4 --print-token-ids --print-timing
```

The 4-token Gemma4 anchor should remain:

```text
10979 236888 2088 740
```

### Debug And Rollback Env Vars

| Variable | Effect |
|---|---|
| `TERMITE_METAL_TRACE_FRAME=1` / `=all` | Dumps the last frame's `region x source` compute-encoder matrix; `all` also includes small prefill/setup frames. |
| `TERMITE_METAL_TRACE_FRAME_BLITS=1` | Traces frame blit-encoder attribution. |
| `TERMITE_METAL_TRACE_GRAPH_PLAN=1` / `=all` | Prints graph-plan commit summaries; `all` also prints requested slot sizes. |
| `TERMITE_METAL_TRACE_Q80_BLOCK=1` | Traces the Q8_0 direct block path, including shared-KV setup misses. |
| `TERMITE_GRAPH_EXECUTOR_STATS=1` | Prints graph executor stats: commands, planned commands, encoder/fallback counts. |
| `TERMITE_DEBUG_METAL_TIMING=1` | Prints detailed per-frame Metal timing (begins/submits/wait_ms/gpu_ms/encoder counts). |
| `TERMITE_METAL_DISABLE_MAPPED_QUANT_WEIGHTS=1` | Forces private-upload quant weight storage instead of no-copy mapped buffers, for A/B timing. |
| `TERMITE_METAL_FORCE_MAPPED_QUANT_WEIGHTS=1` | Forces no-copy mapped quant weight storage even when the default heuristic would use private upload. |
| `TERMITE_METAL_DISABLE_GATED_FFN_GRAPH_FUSION=1` | Rollback for the default-on fused gated-FFN graph path. |
| `TERMITE_METAL_DISABLE_ATTENTION_OUTPUT_RESIDUAL_GRAPH_FUSION=1` | Rollback for the default-on fused attention-output-residual graph path. |
| `TERMITE_METAL_DISABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK=1` | Forces the safe staged prefill path, disabling both the decoder-runtime layer frame and the backend-owned active decode frame. This is a correctness guard, not the target runtime shape. |
| `ANTFLY_INFERENCE_ALLOW_BROAD_METAL_TEST=1` | Overrides the debug wrapper's refusal to run unfiltered `zig build test` in `command` mode (which has produced SoC watchdog reboots); use `unit` mode instead by default. |
| `ANTFLY_INFERENCE_METAL_SKIP_POSTCAPTURE=1` | Skips post-capture log/DiagnosticReports collection during `unit` isolation, for reboot bisection. |
| `TERMITE_METAL_DISABLE_Q8_0_SG_V2=1` / `..._Q8_PAIR_ACTIVATION_SG_V2=1` / `..._Q8_0_SG_M64=1` / `..._ZERO_BIAS_ELISION=1` / `TERMITE_METAL_FORCE_Q8_0_GATED_FFN_ROWWISE=1` | Rollbacks for the default-on Qwen3 embedding Q8_0 kernels; see Qwen3 Embedding Q8_0 Kernel Defaults. |
| `TERMITE_METAL_DISABLE_DENSE_CAUSAL_SG_ATTENTION=1` / `..._Q16=1` / `..._F16KV=1` | Rollbacks for the default-on dense causal flash-attention kernels. |
| `TERMITE_METAL_ENABLE_Q8_0_SG_M64_F16=1` / `TERMITE_METAL_ENABLE_Q8_KV_PAIR_SG=1` / `TERMITE_METAL_ENABLE_DENSE_CAUSAL_SG_ATTENTION_GQA_PAIR=1` | Opt-in for refuted kernel candidates; constructed only when set. |

## Crash Debug Tooling

Use `pkg/inference/scripts/debug_metal_command.sh` for Metal commands that may
hang, abort, or trigger GPU validation failures. It captures stdout/stderr,
process samples, filtered unified logs, and new DiagnosticReports into one
bundle under `pkg/inference/.debug` by default. Use `--out-dir` only when you
need a different location.

Examples:

```sh
bash pkg/inference/scripts/debug_metal_command.sh command -- ./zig-out/bin/antfly inference --help
bash pkg/inference/scripts/debug_metal_command.sh command --api-validate -- ./zig-out/bin/antfly inference embed ~/.antfly/inference/models/antflydb/clipclap --text "hello"
RUN_MODE=chunked CHUNK_SIZE=4 bash pkg/inference/scripts/debug_metal_command.sh unit --no-validate 'metal eager graph|metal_compute'
```

Do not run unfiltered `zig build test` through `command` mode for Metal
debugging. A broad test run can launch many Metal runtime tests in one process
and has produced SoC watchdog reboots without preserving the failing test name.
The wrapper refuses that shape by default; use `unit` mode so
`current_test.txt`, `progress.tsv`, and per-test bundles identify the last
started test or chunk. `ANTFLY_INFERENCE_ALLOW_BROAD_METAL_TEST=1` exists only for
deliberate override.

If a machine-level reset interrupts `unit` mode, resume from the same output
directory instead of starting over:

```sh
RESUME=1 RESUME_SKIP_CURRENT=1 \
RUN_MODE=isolated USE_PREBUILT_UNIT=1 \
bash pkg/inference/scripts/debug_metal_command.sh unit \
  --out-dir pkg/inference/.debug/metal-unit-YYYYMMDD-HHMMSS \
  --api-validate 'metal|Metal'
```

`RESUME=1` preserves `progress.tsv` and skips entries already marked `PASS`.
`RESUME_SKIP_CURRENT=1` also skips the label left in `current_test.txt`, which
is the best suspect after a watchdog reboot.

For reboot bisection, prefer disabling post-capture during unit isolation:

```sh
ANTFLY_INFERENCE_METAL_SKIP_POSTCAPTURE=1 \
RESUME=1 RESUME_SKIP_CURRENT=1 \
RUN_MODE=isolated USE_PREBUILT_UNIT=1 \
bash pkg/inference/scripts/debug_metal_command.sh unit \
  --out-dir pkg/inference/.debug/metal-unit-YYYYMMDD-HHMMSS \
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
Antfly inference commands or long-running Metal executions.

## Rejected Directions

- Do not bring MLX back into the pure Metal backend.
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

> **Relocated:** The four appended command-planner and frame-execution slice
> plans that previously lived here (283 lines: "Generalize Existing Metal
> Command Planner", "Whole-Frame Metal Graph Execution Plan", "Metal Graph
> Command-Volume Reduction Plan", and "Metal Command Reduction Implementation
> Plan") are preserved verbatim in
> [docs/design/inference/history/metal/slice-plans.md](../../../docs/design/inference/history/metal/slice-plans.md).
> Durable decisions from them are in Command Plan Abstraction near the top of
> this document.
