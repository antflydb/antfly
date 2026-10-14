# Gemma 4 MTP and CUDA Branch Status History

> Summary of a verbatim log. Full text: [docs/design/inference/history/gemma4/mtp-cuda.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/inference/history/gemma4/mtp-cuda.md). Originally relocated verbatim from `zig/pkg/inference/GEMMA4.md` (lines 379–405, 407–625, and 649–701 at commit 271838a195) on 2026-09-16; the living design is [`GEMMA4.md`](../../../../../zig/pkg/inference/models/gemma4/GEMMA4.md).

Dated MTP smoke transcripts (Metal, May 2026), CUDA branch updates (June–August 2026, NVIDIA L4 `sm_89`), and Metal GGUF validator smokes. CUDA MTP remains experimental: its diagnostics are not a production-readiness certification or a paired llama.cpp comparison, and no CUDA throughput result should be described as superiority over llama.cpp. The CUDA release contract covers target-only Gemma 4 QAT.

## Decisions
- **MTP activation handoff uses final-RMSNorm hidden states** of the token that predicted the current token — a pre-final-RMSNorm variant (2026-05-05) was Metal-stable but did not improve acceptance; source comparison with the extracted PyTorch drafter moved it back.
- **Acceptance-rate conclusions should prefer official target+assistant pairs** and the proper Gemma 4 chat template; mixed GGUF target + safetensors assistant is for smoke coverage only.
- **Metal BF16 safetensors**: keep rank-2 dense weights as mmap-backed native bytes; the earlier f32 expansion plus duplicated BF16 caused 30GB+ footprints (traced smoke afterwards ~2.6GB).
- **Generic quant runtime surface**: public scratch/setup exports, env vars, and timing labels use `quant` names; Q8_0 fused kernels stay internal fast paths, and new formats extend the quant-format dispatch rather than adding public `q80` APIs. The direct whole-layer planner selects Q8_0 only when every linear slot is Q8_0; mixed families with fused staged kernels are marked direct-eligible.
- **Native Metal GGUF must not depend on MLX** when both backends are built — `.metal` sessions stay on the native provider/stream path (fixed a `MlxMetalUnavailable` failure).
- **CUDA generated Q4_0 kernels (2026-07-13, `codex/quant-kernel-metal-compiler`)** ship as runtime opt-ins (`ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_{MMV,MM,PAIR,PAIR_Q8,DOWN_Q8}=1`; `ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0=1` disables all). Details in `QUANT_KERNEL_COMPILER.md`.
- **SM89 GQA flash-prefill F16 composites promoted to default (2026-07-31)** via an automatic selector when the qualified contract holds; evidence: `quant-kernel-cuda-paged-prefill-diff` 90 cases bitwise-identical. Rollback `ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=off`.
- **W4A16 BF16 tensor-core prefill projections promoted default-on for SM89 (2026-08-01)** (`q4_0_tc_hmma_prefill`; DP4A stays for decode rows==1). Rollback `ANTFLY_INFERENCE_CUDA_Q4_0_TC_HMMA_PREFILL=0`.
- Gemma4 CUDA defaults remain `f32` KV for exactness; `--cache-dtype polar4` is the production-candidate compressed opt-in, `turbo3` experimental (status in `CUDA.md`).

## Rejected approaches
- **Standalone prepared-tail greedy shortcut** (`rms_norm + quantized lm_head + argmax` outside a planned frame) — backed out after a 2026-05-07 SoC watchdog reset under Metal API validation; keep the materialized-logits argmax route until the command lifetime/barrier issue is isolated.
- **DP4A q8_1 for prefill projections** — matched only 30/96 greedy tokens vs the F32-activation reference (diverges at token 29); BF16 tensor-core matched 96/96.

## Key measurements
- MTP smoke, E2B Q8_0 GGUF target + safetensors assistant: `--speculative-k 6 --max-tokens 12` accepted 2 of 41 drafted tokens — far below published numbers. Official target + official assistant (2026-05-06): `rounds=1 drafted=2 matched=2 rejected=0 accepted=3 corrections=0 bonus=1`.
- CUDA generated Q4_0 (E2B QAT, 128 tokens, L4): prefill 150 ms vs 200 ms (about -25%), decode about +2.4%, bit-identical. E4B QAT: E2E median 8197 ms vs 8370 ms, decode 63.4 vs about 61.5 tok/s.
- Long context (2026-07-31, 2,051-token prompt, 300 greedy tokens, F16 K/V, L4): 740.8 ms TTFT and 110.2 decode tok/s vs llama.cpp 329.5 ms and 114.4 tok/s; 1.173 total-latency ratio — prefill is the whole gap. Paired statistics in `docs/CUDA_TUNING.md`.
- Prefill nsys (2026-08-01): GPU-bound at 99.7% utilization; FFN gate/up `termite_linear_q4_0_pair_nobias_q8_1_f32_tile4` ~52% of prefill; tensor-core route cut prefill 1.81x (2297->1269 ms on a 1131-token chunked run).
- Metal GGUF validator smoke (2026-05-07, compiled whole-model, 4 tokens): `compute=21 planned_scopes=35 planned_barriers=457`, prefill=875 decode=149 ms.

## Operational notes
- Smoke wrapper: `scripts/gemma4/test_metal_gemma4_assistant_speculative.sh` (`--backend auto` by default; the official target needs the default `ANTFLY_INFERENCE_GEMMA4_ASSISTANT_HOST_BUDGET_MB=12288` and `ANTFLY_INFERENCE_GEMMA4_ASSISTANT_COMBINED_BUDGET_MB=17408`).
- CUDA validation ladder: `zig build -Dcuda=true` → `cuda-info --smoke` → `cuda-info --gemma4-parity` → `compare --runtime-parity` → greedy `generate` → `generate --draft-model ... --debug-mtp` (full commands in the verbatim text).
- CUDA/MTP isolation flags: `ANTFLY_CUDA_DISABLE_GEMMA4_MTP_DEVICE=1`, `ANTFLY_CUDA_ENABLE_Q4K_DECODE_FAST=1`, `ANTFLY_CUDA_DISABLE_HEAD_NORM_ROPE_FUSION=1`, `ANTFLY_CUDA_ENABLE_ADD_MUL_SCALAR_FUSION=1`, `ANTFLY_CUDA_ENABLE_RMSNORM_ADD_MUL_SCALAR_FUSION=1`, `ANTFLY_GEMMA4_MTP_ALLOW_UNSHARED_TARGET=1`.
