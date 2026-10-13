# Metal ggml-Shaped Checklist, Phase Plan, and Performance Diagnosis

> Summary of a verbatim log. Full text: [docs/design/inference/history/metal/ggml-checklist-and-phases.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/metal/ggml-checklist-and-phases.md). Originally relocated verbatim from `zig/pkg/inference/METAL.md` (lines 617–1313 at commit 3ce5aff4ea) on 2026-10-12.

The gap list from comparing the Metal backend against `../ggml`/llama.cpp, the operator-family and Phase 1–6 ledgers, and a performance diagnosis. Most checked items moved the active Gemma4 Q8_0/f32-KV decode and prefill paths from helper orchestration onto graph-owned command plans. Durable decisions were folded into [METAL.md](../../../../../zig/pkg/inference/METAL.md) before the move.

## Decisions

- **Implementation order:** runtime-owned `MetalGraph`/`MetalCommandPlan` first; then scratch, KV views, liveness, and encoder scopes in that plan; then the shared Zig quant matmul selector; then one descriptor-driven dispatch for every hot linear; then ggml-shaped MMV / MMV-ext / simdgroup MM kernels; pattern fusions only after the op plan and shared kernels own the path, so fused kernels call the same primitives.
- **Operator names are Antfly's, not ggml's.** `OperatorPlan` lives in `src/graph/operator_plan.zig` (backend-neutral) with buckets `mul_mv`, `mul_mv_ext`, `mul_mm`, `fallback`, so Metal, CUDA, WebGPU, and Wasm choose kernels behind one op surface. ggml is the reference shape only.
- **Selection as a system:** qLen=1 mat-vec, qLen=2..8 ext mat-vec (r2/r3/r4/r5 row-count-specialized kernels for qLen=2..5), qLen>8 simdgroup MM, all behind one descriptor ABI. Q8_0 QKV keeps the fused row-1 kernel; qLen 2+ decomposes into descriptor-owned `NONE` Q/K/V submissions.
- **Fused kernels must not fork per-format quant decoding;** they call shared descriptor-driven kernels or share their templates. Quant formats are helpers under common ops (`get_rows`/`cpy`/`mul_mat`), and quant embedding lookup has one operator-backed `get_rows` path (the Q8_0-only embedding kernel was removed).
- **Planner barriers:** a planned buffer barrier resets the active hazard set, so later sibling consumers of the same producer get no redundant barriers. The full active layer contract is one 15-op scope with 12 explicit producer-consumer barriers.
- **Fail closed in active frames:** when a planned contract is present and the planned direct block or tail is unavailable, the active attempt aborts instead of falling into staged helper orchestration under a stale cursor. The staged prefill path is kept only as a correctness oracle (`test-metal-gemma4-prefill-frame`). Generic fallback is an explicit unsupported-format/diagnostic path, not a normal route.
- **Planner records are validated:** Q8-only encoders reject non-matmul or non-Q8 records; f32 attention rejects non-`attention_flash` records; paged attention helpers reject stale records.
- **KV publication is a planned op:** `decode_kv_seed` sits between K/V preparation and attention in the frame contract; the f32 paged seed encodes as a no-blit compute kernel on the active encoder.
- Phase acceptance criteria: frontend no longer orchestrates per-op helpers (Phase 1); prefill and decode linears share the quant dispatch surface with no dense dequant fallback (2); no host-visible bounce between sub-ops (3); frame enablement is a runtime property, not an unsafe env experiment (4); prefill improves because rows are batched, not because a scalar fusion hides overhead (5); a new quant format extends descriptor/kernels/tests, not the executor API (6).

## Rejected approaches

- Widening the row-1 Q8_0 `NONE` MMV tile to four columns — preserved the anchor but slowed the smoke run; stays on ggml's two-output-row (`N_R0_Q8_0`) geometry until replaced by a measured common primitive.
- Blind persistent compute-encoder reuse — lowered encoder count but hurt elapsed time; encoder count is a diagnostic, reduced through real fused kernels and planned regions.
- Decomposed frontend `gelu_new` — could produce a single NaN and collapse logits to token `0`; now a backend activation.

## Key measurements

- 4-token Gemma4 anchor with row-1 decode on planned scopes: token IDs `10979 236888 2088 740`, `planned_scopes=36`, `planned_barriers=422`, last-frame compute encoders `41`, `layer=35` region.
- Row-1 f32 attention on the tiled kernel (`kv_len <= 2048`, `head_dim <= 1024`): `greedy_direct` roughly `110ms` -> `36ms` for a 3-token decode slice (one local run).
- Raw two-token smoke: `small_batch=240`, `rows_2_8=240`.
- `zig build bench-metal-prefill-buckets -Dmetal=true`, Gemma4 Q8_0 after the r2–r5 split:

| Bucket | generate | prefill_direct_family / greedy_direct |
| --- | ---: | ---: |
| pp10 | 1150ms | 926ms |
| pp128 | 1945ms | 979ms |
| pp512 | 5196ms | 1774ms |
| tg16 (10-token prompt) | 1731ms | `greedy_direct=736ms` |

## Open items at the time of the move

- Retire `termite_metal_select_q8_0_linear_dispatch`; convert remaining layer helpers into graph op builders; assemble embedding -> PLE -> layers -> tail as one frame command plan (including qLen>1 and non-Q8/dense tails).
- Flash-attention-class kernel family (scale, mask/bias, softcap/sinks, softmax, PV); arbitrary mask/bias in `attention_paged`; physical page tables for qLen>1 quantized-KV prompts.
- ggml-quality Q8_0 `mul_mv`/`mul_mm`; `mul_mv_ext` and `mul_mm` for non-Q8 formats; IQ, MXFP4, NVFP4 row/copy kernels; pair/QKV/fused epilogues beyond Q8_0.
- Graph-level standalone Metal softmax; `mul_mat_id`/`mul_mv_id` when a model needs them; broader RoPE variants (neox, mrope/vision, YaRN); fusion/bucket regression tests that assert both parity and fast-path counters.
- Phase 1 (route Gemma4 through shared `ModelRuntime`/`metal_executor`), Phase 4 (whole-frame allocator, lifetime stress tests before re-enabling framed execution), Phase 5 (tuned qLen=2..8 kernels, runtime-owned prefill submission), Phase 6 (format coverage by real model demand, with [GGML.md](../../../../../zig/pkg/inference/GGML.md) as the format source of truth).
- Replace the active-frame hidden ping-pong/returned-output allocation with planned slots; true strided PLE operand; the shared-KV setup miss visible under `TERMITE_METAL_TRACE_Q80_BLOCK=1`.

## Performance diagnosis (at the move)

The largest remaining costs were a still frontend-orchestrated decode layer loop (one command buffer per token but `41` compute encoders), Q8_0 kernels not yet ggml-quality, reducer-style small-prompt kernels, no flash-attention-class prefill family, and fused paths that are local fusions around slow primitives — they reduce overhead but do not close the llama.cpp-class gap without better quant kernels. Decode frames no longer have KV suffix blits; prefill and fallback paths still need destination-owned KV writes.
