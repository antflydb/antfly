# GLiNER2 on the Metal backend

Status (2026-07-16): at batch 8-16 on Apple silicon, Metal beats the native
CPU backend by 33-43%. Against Fastino PyTorch-MPS it is effectively tied on
215-token documents and 10-42% faster on 430-token documents. Batch-1
short-text is at parity with native; batch-1 long-text trails native by ~13%
(attention-bound, see Remaining Work). All numbers below are local, unattested
MacBook Air M4 (8-core GPU, 16 GB) measurements.

## Execution architecture

GLiNER2 = DeBERTa-v3-base encoder (12 layers, hidden 768, disentangled
attention) + span-scoring head, loaded from a split Q4_K GGUF bundle. On Metal
it runs the eager per-op path (`session_factory.zig` `.gliner` branch), with
three structural fast paths:

### 1. Encoder slot fast paths (`architectures/deberta.zig`)

`preplanMetalDebertaEncoderFrame` prepares 6 linear + 2 LayerNorm slots per
layer with dense-f16 MPS weight-mirror preference (the same machinery
`bert.zig` uses for bge-m3; ~162 MB for all 72 slots, under the 768 MB
ceiling). `encoderLayer` consumes them via the slot-based decoder-runtime ops,
falling back to the explicit-weight path per op if a slot op returns null:

- QKV: `decoderRuntimeApplyLinearQkv` (packed, one MPS GEMM for Q/K/V)
- Relative Q_r/K_r: `decoderRuntimeApplyLinearPair` on the same q/k slots
  (share_att_key semantics preserved)
- Attention output + residual + LN: `decoderRuntimeApplyLinearLayerNorm`
- FFN + GELU + residual + LN: `decoderRuntimeApplyFfnLayerNorm`
  (fallback `runDenseFfnResidual` + LayerNorm)

The whole encoder runs inside one decoder-runtime frame (one command buffer).

### 2. Disentangled-attention dispatch (`backends/metal_kernels.m`)

One dispatch function selects between four kernels. Defaults were set from
interleaved ABBA measurements at seq {128, 215, 430} x batch {1..16}:

- `_tg` threadgroup kernel: seq < 384 at every batch, and batch == 1 at any
  seq <= 512 (batch-1 was previously routed to flash4 at seq >= 384; measured
  slower on M4).
- `_flash4`: batch >= 2 && seq >= 384 (13-18% over tg/gemm at batch 8-16,
  where tg hits a threadgroup-occupancy cliff). No structural batch limit
  (batch rides `tg.z`); the old `batch <= 8` gate was heuristic and wrong.
- GEMM-score pair: seq > 512 only (never won a clean interleaved cell inside
  flash4's envelope; scratch is B*h*S^2 + 2*B*h*S*rel floats and falls back
  down the chain on reservation failure).
- naive: last resort.

### 3. Device-resident span head

- `glinerWordEmbeddings` (`ops/metal_compute.zig`): host-computes the
  first-subtoken row index per (batch, word), then reuses the axis-0 gather
  device kernel. Words with no tokens gather an out-of-range row that the
  kernel zero-fills. This unlocks the fully device head path in
  `gliner_head.zig` (label takeRows, span ops, logits).
- **Multi-row device column-concat**: previously `concatOp` handled only
  `total == 1` on device; any multi-row concat drained the active frame,
  materialized both inputs to host, and returned host tensors — which silently
  pulled the entire span MLP (batch*num_words*width rows, e.g. ~25k x
  768->3072->768 at batch 8 / 430 tokens) onto the CPU. This single fallback
  was ~2x of end-to-end time at large shapes. `concatOp` now routes through
  the pre-existing (previously unwired) `concat_lastdim_f32_2d` device op,
  and warns once if the host fallback ever re-engages.
- Dynamic (eager) linear slots request the dense-f16 MPS mirror for quantized
  weights (32 MB per-weight cap) when the backend's `preferEagerQuantMirrors`
  hint is set — the `.gliner` session branch sets it, so the head span-MLP
  GEMMs run on MPS like the encoder while other models keep their existing
  eager-path behavior. Slots a backend instance allocates are released on its
  deinit.
- The head runs inside its own decoder-runtime frame (`session_factory.zig`
  `.gliner` branch); host-I/O ops (label GRU readback) drain and reopen it.

## Environment flags

All default-on fast paths have kill switches; force knobs exist for A/B runs.

| Flag | Effect |
|---|---|
| `TERMITE_METAL_DISABLE_DEBERTA_WEIGHT_MIRRORS` | GLiNER encoder slots stay quantized (memory-constrained mode; slower) |
| `TERMITE_METAL_DEBERTA_USE_{Q8,BF16,F32}_MIRRORS` | Mirror dtype A/B knobs (default f16 MPS) |
| `TERMITE_METAL_DEBERTA_WEIGHT_MIRROR_MAX_MB` | Mirror budget ceiling (default 768) |
| `TERMITE_METAL_DISABLE_DEBERTA_SLOT_OPS` | Umbrella: encoder falls back to explicit-weight per-op path |
| `TERMITE_METAL_DEBERTA_ATTENTION_FORCE=tg\|flash4\|gemm\|naive` | Bypass attention shape gates (hard kernel limits still apply) |
| `TERMITE_METAL_DISABLE_DEBERTA_{GEMM,FLASH,TG}_ATTENTION` | Disable individual attention variants |
| `TERMITE_METAL_DISABLE_GLINER_HEAD_DEVICE` | Head word-gather falls back to host extraction |
| `TERMITE_METAL_DISABLE_DYNAMIC_SLOT_MIRRORS` | Eager/dynamic linear slots stay quantized |
| `TERMITE_METAL_DISABLE_GLINER_ENCODER_FRAME` | Pre-existing: disable the encoder frame |
| `TERMITE_GLINER_PROFILE=1` | Pipeline phase timing (prepare/pack/session_run/decode) |
| `TERMITE_METAL_TRACE_FRAME=all` | Per-frame encoder counts + GPU-busy ms |

In text mode, `bench-gliner2-e2e` prints a `provider_stats:` line (mps_linears,
dense-f16 mirror MB/slots, qkv_packed, deberta_ffn_fused, attention variant
counters, compute-encoder counts) that proves which paths fired.

## Measured results (2026-07-16, warm p50, interleaved lanes)

| shape | Metal | native CPU | Fastino MPS 1.2.4 |
|---|---:|---:|---:|
| 215tok b1 | 120.5 ms | 120.9 | 87.1 |
| 215tok b8 | 624.5 ms | 859.6 | 639.0 |
| 215tok b16 | 1248.8 ms | 1662.6 | 1240.8 |
| 430tok b1 | 287.0 ms | 249.9 | 183.8 |
| 430tok b8 | 1252.1 ms | 1751.1 | 1374.0 |
| 430tok b16 | 2375.3 ms | 3389.7 | 3384.3 |

Cumulative vs the pre-optimization Metal baseline (same day): 215tok b16
8600 -> 1249 ms; 430tok b16 ~16342 -> 2375 ms (~6.9x). Attribution: encoder
slot fast paths ~2.3-2.7x (same-binary kill-switch A/B), attention gate retune
13-18% at batch >= 8 / seq >= 384, device concat ~2.5x at large shapes,
dynamic-slot mirrors ~10% at b16.

Measurement discipline on fanless Apple silicon: sustained-load thermal drift
reached 2x on identical configs across an afternoon; only same-session
interleaved (ABBA) comparisons were treated as evidence, and one ordered
45-cell sweep initially attributed the attention win to the wrong kernel
before interleaved runs reversed it.

## Correctness status

- Entity sets match the native backend exactly at batch 1 and batch 16
  (368 == 368 at both text lengths); one measured b8 cell had 3 extra
  near-threshold entities (187 vs 184). score_sum deltas <= 0.3%.
- Metal<->native score drift is accumulation-order drift (MPS f16 mirrors +
  different attention reduction order); it predates this work. Near-threshold
  spans can flip run-to-run at batch 16 under heavy thermal load.
- Attribution knobs: `TERMITE_METAL_DEBERTA_USE_F32_MIRRORS=1` isolates the
  f16-mirror contribution; `TERMITE_METAL_DEBERTA_ATTENTION_FORCE` isolates
  attention kernel choice.
- Gates green: test filters `deberta` 36/36, `gliner` 91/91, `bert` 52/52,
  `concat` 33/33, `Metal JIT` 30/30; bge-m3 e2e bench unregressed.

## Review outcome (2026-07-16, multi-agent review of `gliner go fast`)

13 findings confirmed by adversarial verification; fixed on top of the commit:

- **Concurrency (critical)**: the recognize path had no session serialization
  while the server admits 32 concurrent requests — `GlinerPipeline` now takes
  a `recognize_session_lock` from `LoadedModel` (same pattern as embedding /
  reranking) around every `session.run`.
- **Ragged batches (major, scalability)**: `glinerWordEmbeddings` bailed to
  the host path whenever any batch item had fewer words than the batch max —
  i.e. on virtually every real batch (benches replicate one text and never
  caught it). Missing words now gather an out-of-range row that the existing
  gather kernel zero-fills, matching CPU semantics. Bench gained a `--ragged`
  flag; measured ragged b8 = 596 ms vs native 1085 ms.
- **Duplication (minor)**: the committed `termite_concat_cols_f32` kernel
  duplicated the pre-existing (never-wired) `concat_lastdim_f32_2d` device op;
  `concatOp` now routes through the existing binding and the duplicate stack
  was deleted.
- **Blast radius (major)**: encoder and dynamic-slot f16 mirrors are explicit
  GLiNER opt-ins; generic DeBERTa sessions keep their existing quantized
  execution, numerics, and memory use.
- **Slot stranding (major)**: per-request backends release their dynamic
  linear, LayerNorm, and RMSNorm slots on deinit, so the shared provider pools
  cannot exhaust across requests.
- **Lifetimes (major/minor)**: fixed a latent double-free in
  `decoderRuntimeApplyLinearQkvOp`'s error path (overlapping errdefers);
  `attn_normed` in the DeBERTa FFN section is now owned by a single `defer`;
  three device wrappers on the new paths released their output buffer on
  `rc != 0` (`return null` skips `errdefer`).
- **Limits (minor)**: the seq>512 GEMM attention gate now also requires score
  counts <= u32 max (the kernels index in 32-bit), falling through safely.
- **Observability (major)**: warn-once logs when the multi-row concat or word
  gather silently re-engage a host fallback — the exact regression class this
  work fixed is now operator-visible.
- **Tests (major)**: added Metal device tests for multi-row concat parity
  (asserting the result stays device-resident) and ragged word-gather parity
  vs CPU semantics.

Deferred with rationale: file-wide `if (rc != 0) return null;` leak-pattern
sweep in metal_runtime.zig (pervasive pre-existing pattern, needs its own
pass); provider-persistent dynamic-slot dedup (would avoid per-request mirror
re-upload, ~ms); server metrics export for the provider counters; seq 513-1024
attention envelope (unreachable for GLiNER — max_length 512 — but other
DeBERTa users would land on GEMM-or-naive there).

## Remaining work

1. **Batch-1 long-text attention**: b1@430tok trails native by ~13%; batch-1
   runs the `_tg` kernel. Levers (specced, unbuilt): flash4 PV-stage
   parallelization over key chunks, a `query_block=8` flash variant for large
   batch, simdgroup-tiled GEMM-score upgrade. These would also stretch the
   batch >= 8 lead.
2. **Backend auto-routing**: `auto` still maps GLiNER2 to native. Metal clearly
   wins for long documents at batch 8-16 but only ties Fastino on short
   documents, while batch 1 is a tie/loss. Backend selection happens at model
   load, not per request, so the current table does not justify a single
   batch-only crossover rule.
3. Rel Q_r/K_r projections (12 layers x 2 GEMMs on the shared q/k slots) are
   recomputed every forward; cacheable per seq-len if profiling ever shows it.
4. Generated/JIT quant kernels intentionally play no role in the GLiNER2 Metal
   path anymore: encoder and head both run dense-f16 MPS mirrors. The
   quant-resident (mirror-disabled) mode still uses handwritten quant kernels
   and is unoptimized by design this round.
