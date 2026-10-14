# Metal Backend Status History

> Summary of a verbatim log. Full text: [docs/design/inference/history/metal/status-history.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/metal/status-history.md). Originally relocated verbatim from `zig/pkg/inference/METAL.md` (lines 131–478 and 490–548 at commit 271838a195) on 2026-09-16 during the documentation cleanup.

Status bullets and dated benchmark anchors (mostly 2026-05) from bringing the native Metal backend's Gemma4 Q8_0/f32-KV decode and prefill paths onto planned, ggml-shaped encoder scopes. The living design is [`METAL.md`](../../../../../zig/pkg/inference/METAL.md). Correctness anchor throughout: the 4-token Gemma4 `hi` output `Hi! How can`, token ids `10979 236888 2088 740`.

## Decisions

- `--backend metal` builds with `-Dmetal=true`; the MLX backend was removed. In builds with both, `.metal` uses the native Metal session/provider (not MLX), avoiding `MlxMetalUnavailable` before model load.
- Q8_0 weights stay quantized and resident; the hot path never dequantizes whole dense weights. GGUF BF16 tensors are preserved (not widened to f32); on the anchor the PLE projection slot went from `dense_f32_mb=52` to `dense_bf16_mb=26`.
- `gelu_new` lowers as a backend activation kind — decomposing into `x^3 -> tanh -> multiply` caused a qLen>1 prompt NaN (`ffn_gate=11.367456` -> `ffn_gate_act=NaN`).
- Attention planning separates KV dtype from KV storage layout: dense f32 KV selects `attention_flash`, paged f32 KV selects `attention_paged`, Polar4/Turbo3 stay quantized-KV (ggml-shaped type vs storage distinction).
- Backend-neutral quant matmul selector (`quant_matmul.zig`) with ggml-style buckets (scalar, MMV, small-batch, MM); Q8_0 prompt linears with 9 or more rows use the simdgroup MM bucket (10-token anchor: `metal_q8_0_dispatch: mm=270`). Quant slots use one format-tagged descriptor/ABI (`prepare_quantized_linear_slot(format, ...)`) instead of per-format copies.
- Unsupported qLen>1 block shapes return to the safe staged path rather than the legacy decode-shaped gathered monolithic block (which failed with `rows=10 rc=-13 stage=4`).
- Planned compute barriers are range-driven like ggml: overlap involving a previous write emits a buffer-scope barrier. A 2026-05-07 bisection found that treating planned barriers as metadata-only let a realistic Q8_0 framed gated-FFN test pass and then triggered a delayed SoC watchdog reset roughly 90 seconds later (no GPU rerun after the watchdog).
- Graph-planned scratch covers hot buffers; hot helpers reject unplanned allocation mid-frame. Prefill scratch must reserve `max(rows * hidden_size, rows * attention_input_size)` — Gemma4's 2048 attention input vs 1536 hidden under-reservation broke the fused attention-residual path at stage 3.
- Safe staged oracle (`TERMITE_METAL_DISABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK=1`) disables active-frame batching as a correctness guard.

## Rejected approaches

- **Naive persistent compute encoder** across the frame — encoders `531` -> `17`, but the 4-token anchor regressed from roughly `1.3s` to `7.6s`. Use explicit fused kernels / planned scopes with correct barriers, not generic encoder reuse.
- **Q8 gate/up kernel recomputing FFN pre-RMS per output tile** — encoders `531 -> 496` but badly regressed latency from rereading the hidden vector per tile; removed. Replacement computes the inverse RMS scale once per row and feeds it into the pair kernel (warm 4-token `hi` about `1260ms`).
- **More blit cleanup** — last-frame blits went `61` -> `30` -> `15` -> `0` (gathered-KV append, grouped suffix append, direct write into cache); stop there and address compute command planning.

## Defects found

- Active paged decode block wrote a freshly allocated output while the decode loop consumed the untouched reserved hidden buffer; fixed with an `Into` form.
- Wrong-token fast path was a runtime slot-key bug: native dense byte-only RMS weights had empty host slices and collided when keyed by `data.ptr`; now keyed by native dense buffer identity.
- `termite_sdpa_f32` optional bias/mask bindings aborted Metal validation when disabled.

## Key measurements

**Q8_0/f32-KV greedy decode-shape anchor** (4-token Gemma4; the source of `ts/apps/model-explorer` frame `decode-q8_0-anchor`): `planned_scopes=36`, `planned_barriers=422`, last-frame compute encoders `41`, one command buffer per decode token, `0` blit encoders. Source attribution: `layer=35`, `embedding=2`, `rms_norm=1`, `ple=1`, `tail=1`, `dense_linear=1`, `other=0`. Parallel greedy argmax moved `greedy_direct` from roughly `938ms` to `134ms` and total generation from roughly `18.6s` to `1.76s`. Use the 4-token row-1 counters (not noisy 16-token timing) as the decode command-shape anchor.

| Anchor (date) | Result |
| --- | --- |
| Compiled partitioned graph, Gemma4 Q8_0 short prompt, 1 token (2026-05-05) | `interpreter_fallbacks=0`, `host_outputs=0`, `device_outputs=819`, `planned_commands=141`, `prefill=998ms` |
| Same, `TERMITE_DEBUG_METAL_TIMING=1` | `gpu_ms=22`, `total_compute_encoders=942`, `total_blit_encoders=53` — the gap is command/encoder volume, not one attention op |
| Fused gated-FFN graph A/B | commands `1134` -> `924`, planned `211` -> `176` |
| Fused attention-output-residual A/B | commands `980` -> `819`, planned `176` -> `141`, warm prefill `1034ms` -> `998ms` |
| PLE/token setup coalescing (2026-05-07) | prefill PLE frame 7 -> 1 compute encoders, decode frame 6 -> 1 |
| Graph-plan geometric growth | `graph_plan_count=3`, `graph_plan_allocs=41` -> `1`, `21` |
| 4-token compiled whole-model smoke under API validation (2026-05-07) | `prefill=157ms`, `decode=149ms`, `total=1006ms` |
| Greedy decode, small anchor | roughly `15-17 tok/s` |
| llama.cpp reference, same model class | `pp10` about `346 tok/s`, `tg16` about `101 tok/s` |

Remaining gap at the time: kernel quality and larger runtime-owned graph/layer submissions, not frontend cleanup.

## Debug switches

`TERMITE_METAL_TRACE_FRAME=1|all` (region × source encoder matrix), `TERMITE_METAL_TRACE_GRAPH_PLAN=1|all`, `TERMITE_METAL_DISABLE_GATED_FFN_GRAPH_FUSION=1`, `TERMITE_METAL_DISABLE_ATTENTION_OUTPUT_RESIDUAL_GRAPH_FUSION=1`, `TERMITE_METAL_FORCE_MAPPED_QUANT_WEIGHTS=1` / `TERMITE_METAL_DISABLE_MAPPED_QUANT_WEIGHTS=1` (mmap `newBufferWithBytesNoCopy` vs private upload), `antfly inference smoke --inspect-only` (largest non-quantized GGUF tensors).
