# Gemma 4 26B-A4B Performance Analysis

> Summary of a verbatim log. Full text: [docs/design/inference/history/gemma4/a4b-perf.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/gemma4/a4b-perf.md). Originally relocated verbatim from `zig/pkg/inference/PERF.md` (lines 1–900 at commit 271838a195) on 2026-09-16; the living design is [`CUDA.md`](../../../../../zig/pkg/inference/CUDA.md) (A4B defaults and rollback) and [`METAL.md`](../../../../../zig/pkg/inference/METAL.md) (`TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH` bundle).

Dated performance sessions for Gemma 4 26B-A4B (30 MoE layers, 128 experts, top-8, hidden 2816, intermediate 704). Metal sessions ran on a Mac mini (Mac16,11), Apple M4 Pro, 24 GB, with `gemma-4-26B_q4_0-it.gguf` against llama.cpp build 10342 (`38278078c`); the CUDA lane ran on NVIDIA L4 (SM89). Metal decode went from 8.516867 tok/s (branch baseline) to 60.1145 tok/s (88.25% of llama.cpp) between 2026-08-24 and 2026-08-25; llama.cpp parity gates (95% aggregate, 90% per prompt) were never met. An April 2026 Q5_K_M report is retained at the end.

## Decisions
- **CUDA: one fail-closed qualified A4B configuration** (SM89, Q4_0 experts), full residency only — a streamed request, mismatched geometry/quantization, missing kernel, or insufficient memory fails model load rather than selecting host MoE. Full residency is the default policy; `--a4b-load-strategy legacy` is the rollback (`pipeline` fails closed, `auto` falls back only on staging/worker/host-allocation failure). (2026-08-25)
- **CUDA loader** orders mmap-backed dense uploads by file offset and streams packed experts through a bounded pinned-host pipeline (four workers, 256 MiB staging) — load ~1,746 s → ~124 s → 90.77 s; the expert phase is storage-bound (~176 MiB/s), so more workers are not expected to help.
- **Optional offline expert pack** (`a4b-pack --shards 4`, `$MODEL/a4b-cuda-pack-v2`, `--a4b-prepared-pack auto|required|off`); the real 12.25 GiB pack was never materialized on the qualification host (no free space), so production packs must be built, verified, and timed on deployment storage. `startup_strategy: "prefetch"` is cache warming, not a promised restart speedup.
- **CUDA post-FFN norm fusion isolation**: `ANTFLY_INFERENCE_CUDA_DISABLE_A4B_PARALLEL_FFN_POST_RESIDUAL=1`.
- **Metal high-memory bundle stays opt-in** behind `TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH`, with a `TERMITE_METAL_DISABLE_A4B_*` rollback per component; the prepared executor separately needs `TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE=1`. Members: model-wide no-copy buffer + residency set, mapped routed experts, fused gate/up activation, shared-FFN and norm fusions, qualified M4 Q4_0 schedules, split-GQA decode for A4B local/global geometries at KV ≥ 512, exact specialized `MUL_MV_ID` kernels, register route selector (`termite_moe_route_select_tg_register`), concurrent hazard tracking, Q6_K LM head NR4/NSG1, pipelined decode frame, and frame-owned split-GQA scratch.
- **Frame-owned split-GQA scratch promoted** (two private slots; `TERMITE_METAL_DISABLE_A4B_DECODE_GQA_SPLIT_FRAME_SCRATCH=1` rollback) — a single scratch rejected every N+1 dispatch while frame N was submitted, causing the prepared long-context cliff. (2026-08-25)
- **A4B local HD256 flash-prefill: explicit-only** (`TERMITE_METAL_ENABLE_A4B_FLASH_PREFILL_HD256=1`), not inherited from the umbrella — kernel and routing qualified, but it changes tokens at index 3 and gained only 1.16% (below the 2% floor); blocked on the eight-prompt external-quality gate and human transcript sign-off.
- **Speculative A4B target verification fails closed** (`Gemma4A4bMetalSpeculativeVerificationNotQualified`) before target KV mutation.

## Rejected approaches
- **Routed Q4_0 launch-shape portfolio** — 16-combination sweep and finalists (41.721–41.899 tok/s) below retained NR4/NSG2; a source port of llama.cpp's Q4_0 block arithmetic regressed (41.953 → 41.451). Kernels and flags removed; future work should target fewer expert weight reads or a different routed-FFN layout. (2026-08-24)
- **Packed common-Q4 `[tile4][block][row][18]` layout** — exact but 1.0228x combined, below the 1.15x floor.
- **Concurrent A4B command DAG** — −1.07% (42.000 vs 42.453 tok/s); remains behind `TERMITE_METAL_ENABLE_A4B_DAG_SCHEDULER=1`. Resource-barrier DAG mode later 59.322 vs 60.870 tok/s.
- **Post-promotion experiments (2026-08-25)**: NR1 LM head (−3.45%) and other NR/NSG widths; selector v2 (saves ~0.023 ms/token); route-slot-map folding (never activates); routed/shared NQ8 schedules, fused MoE activation/reduce epilogues, selective FFN interleave, unretained command buffers, router SIMD projection (all slower); packed QKV + direct KV write + embedding-scale fusion (60.000 vs 61.047 tok/s) — so no Class C token re-baseline was requested.

## Key measurements

Metal A4B short-prompt decode, 128 greedy tokens, F16 KV, fresh process per sample:

| Date / stage | Antfly tok/s | llama.cpp tok/s | Parity |
|---|---:|---:|---:|
| 2026-08-24 branch baseline | 8.516867 | 68.87 | 12.37% |
| 2026-08-24 high-memory lane | 39.481801 | 68.87 | 57.33% |
| 2026-08-24 prepared high-memory lane | 42.189 | 68.87 | 61.26% |
| 2026-08-24 + exact specialized expert-ID kernels | 42.4465 | 68.0500 | 62.38% |
| 2026-08-24 prepared executor + specialized ID (3-prompt) | 42.4749 | 68.0500 | 62.42% |
| 2026-08-25 promoted bundle (3-prompt strict gate) | **60.1145** | 68.1200 | **88.25%** |

- **Route selection** was the surprise bottleneck in the 2026-08-24 full-operation ledger (~7.30 ms/token; router top-k 18.254 ms across three local-layer frames), while routed Q4 reads were already near practical bandwidth (routed gate/up 214.43 GB/s). The exact one-SIMD-group selector cut 216.738 → 27.800 µs/layer; the register-resident version reached 18.852 µs/layer. Expanded bundle: 1.0463x vs rollback, 9/9 wins.
- Specialized `MUL_MV_ID` replay: merged gate/up 1.1633x, down 1.1265x, bit-identical; routed time −11.98% in-model; end-to-end +1.9719%.
- Post-promotion profile: 666 dispatches/token, median frame 17.274 ms; largest buckets LM head 3.151 ms, routed gate/up 2.511 ms, attention Q/O 2.462 ms, shared FFN 1.968 ms.
- **Long context**: split-GQA vs paged rollback at a 603-token prompt 40.050 vs 28.070 tok/s (+42.68%). On 2026-08-25 a 661-token prompt still showed a prepared cliff (33.642 tok/s vs compiled control 49.244, llama.cpp 65.80) — a separate production blocker; frame-owned scratch then reached 59.821 tok/s on a 618-token prompt (1.7235x the single-scratch rollback, 90.16% of llama.cpp's 66.350).
- Dense controls (2026-08-24): E2B Q4_0 82.254 vs llama.cpp 106.070 (77.55%), E4B 50.138 vs 61.240 (81.87%); A4B changes were dense-neutral, but these ratios did not reproduce the previously expected near-parity dense result and need reconciling.
- Memory: model-wide no-copy allocation 14,302,248,960 bytes; residency set 14,937,423,872 bytes (13.91 GiB). Short-context deficit is GPU work (FFN 14.125 ms, attention 6.375 ms of 23.559 ms/token); host/submit gap ~0.143 ms.
- **CUDA (L4)**: 63.18 tok/s vs llama.cpp 69.41 tok/s; peak combined envelope about 30,154 MiB, retained CUDA envelope 16,384 MiB.

## Historical report: 2026-04-08 Q5_K_M lane
Commit dfaca28, gemma-4-26B-A4B-it Q5_K_M GGUF, Apple Silicon 36GB, ReleaseFast Metal. Decode ~7.8 s/token (non-graph) vs ~8.1 s/token (graph mode), ~87x slower than the bandwidth limit. Non-graph: 15 eval barriers/token × 214 ms dominated (82%). Graph mode removed the barriers but the interpreter dispatched 2011 lazy Metal ops per token (~8 s/token), so it did not help. Key insight recorded: the dominant cost was Metal kernel dispatch overhead for ~930 small single-row matmuls per token; the path forward was fusing into fewer kernels or a direct Metal compute pipeline.
