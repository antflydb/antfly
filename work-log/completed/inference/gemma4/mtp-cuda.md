# Gemma 4 MTP and CUDA Branch Status History

> Relocated verbatim from `zig/pkg/inference/GEMMA4.md` (lines 379–405, 407–625, and 649–701 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`GEMMA4.md`](../../../../zig/pkg/inference/models/gemma4/GEMMA4.md). Durable decisions from this log were folded into that document before the move.

## Gemma 4 MTP Runtime: current smoke result

Current smoke result:

```sh
antfly inference generate ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf \
  "Write one short sentence about databases." \
  --backend metal \
  --draft-model ~/.antfly/inference/models/google/gemma-4-E2B-it-assistant \
  --speculative-k 2 \
  --max-tokens 4 \
  --print-token-ids \
  --print-timing
```

This runs end-to-end and preserves target-owned verification. After fixing the
activation handoff to use the hidden state that predicted the current token, the
same short smoke accepted one draft token:

```text
speculative: rounds=2 drafted=4 matched=1 rejected=3 accepted=3 corrections=2 bonus=0
```

A longer `--speculative-k 6 --max-tokens 12` smoke accepted 2 of 41 drafted
tokens against the local `ggml-org` Q8_0 GGUF target. The runtime is now
productive, but acceptance is still far below the published best-case numbers.
The remaining likely causes are source/model pairing differences between the
official safetensors assistant and the local GGUF target, quantization effects in
the target, or a still-missing detail in the clustered output head.

### CUDA Branch Status

CUDA MTP remains experimental. Its diagnostics are not a production-readiness
certification and are not a paired llama.cpp comparison; no throughput result
from that path should be described as superiority over llama.cpp. The CUDA
release contract covers target-only Gemma 4 QAT, while strict MTP certification
and promotion remain follow-up work.

Update 2026-07-13 on branch `codex/quant-kernel-metal-compiler`: the quant
kernel compiler ships 5 promoted generated CUDA Q4_0 artifacts as runtime
opt-ins for the Gemma4 QAT path (`antfly_q4_0_mmv_f32_v1`,
`antfly_q4_0_mm_f32_v1`,
`antfly_q4_0_pair_mmv_f32_v1`, plus the q8_1/DP4A pair
`antfly_q4_0_pair_activation_q8_1_mmv_v1` and
`antfly_q4_0_down_q8_1_mmv_v1` inside the opt-in
`ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE` tuned path).
Measured on E2B QAT (`gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf`, 128 tokens,
NVIDIA L4) versus the handwritten routes: prefill 150 ms vs 200 ms
(about -25%), decode about +2.4%, bit-identical output, zero fallbacks.
Measured on E4B QAT in the tuned llama.cpp pair-harness config: E2E median
8197 ms vs 8370 ms with the q8_1 kernels disabled (about -2.1%), decode 63.4
vs about 61.5 tok/s; the paired margin vs llama.cpp roughly halved. Launch
counts appear in `--print-timing`/`--json-timing` as
`cuda_q4_0_generated_counts:`. Enable one route at a time with
`ANTFLY_INFERENCE_CUDA_GENERATED_Q4_0_{MMV,MM,PAIR,PAIR_Q8,DOWN_Q8}=1` for
model-level validation. The matching per-kernel `DISABLE_` variables override
the opt-in; `ANTFLY_INFERENCE_CUDA_DISABLE_GENERATED_Q4_0=1` disables every
generated Q4_0 route and candidate.
Details and promotion evidence: `QUANT_KERNEL_COMPILER.md` (Current CUDA
State).

Update 2026-07-31 on branch `quant-kernel-long-context`: ten-pair warm-server
long-context evidence (2,051-token prompt, 300 greedy tokens, F16 K/V, L4)
puts the tuned frontier profile at 740.8 ms TTFT and 110.2 decode tokens/s
versus llama.cpp at 329.5 ms and 114.4 tokens/s, a 1.173 total-latency ratio —
decode is within 4% and prefill projections remain the whole gap. This is not
a superiority result. See `docs/CUDA_TUNING.md` ("Gemma 4 E2B SM89
optimization status") for the paired statistics, the promoted-routes-only
measurement, and the F32-cache decode caveat.

Update 2026-07-31 (flash-prefill promotion): the SM89 GQA flash-prefill F16
composites (`attention_prefill_flash_sm89_hd{256,512}`) are promoted to
production runtime defaults. With `ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE`
unset, the runtime resolves an automatic selector that engages
`flash-f16-sm89` whenever the qualified contract holds (SM89, page-16 paged
F16 K/V, GQA 8:1, q512/q3 query-length policy, matching sliding-window/global
geometry, symbols loaded) and otherwise silently keeps the previous unset
launch topology. Promotion evidence: `zig build
quant-kernel-cuda-paged-prefill-diff` passed all 90
guard/page-table/adversarial/determinism cases bitwise-identical (also run in
CI on the L4 lane). Rollback: `ANTFLY_INFERENCE_CUDA_GQA_PREFILL_PROFILE=off`;
all explicit profile values behave exactly as before.

Update 2026-08-01 (W4A16 tensor-core prefill projections promoted): non-perturbing
nsys profiling showed prefill is GPU-bound (99.7% device utilization — the earlier
"host overhead" was un-bucketed GPU kernels, not host idle), with the FFN gate/up
projection (`termite_linear_q4_0_pair_nobias_q8_1_f32_tile4`, DP4A/SIMT int8) the
single dominant kernel at ~52% of prefill. Routing Q4_0 prefill projections through
the BF16 tensor-core (WMMA) kernel instead cuts prefill 1.81x (2297->1269 ms on a
1131-token chunked run) AND raises quality: vs the F32-activation reference the BF16
tensor-core path matches 96/96 greedy tokens (100%) while the DP4A q8_1 default
matches only 30/96 (31%, diverges at token 29) — bf16 activations track f32's argmax;
q8_1 int8 is too coarse. `q4_0_tc_hmma_prefill` is therefore promoted default-on for
SM89 (compute 8.9): W4A16 handles prefill projections (rows>1); DP4A stays for decode
(rows==1). Rollback: `ANTFLY_INFERENCE_CUDA_Q4_0_TC_HMMA_PREFILL=0`.

Status checked on 2026-06-21 on branch `gemma4_gpu_stuff`:

- `zig build -Dcuda=true` succeeds.
- `antfly-inference cuda-info --smoke` succeeds on an NVIDIA L4 (`sm_89`).
- The CUDA smoke now covers the Gemma4-specific WIP primitives:
  add-multiply-scalar, RMSNorm-add-multiply-scalar, head-norm+RoPE, GQA, RoPE,
  and MTP masked argmax.
- Local validation artifacts used in this pass:
  `.models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf`
  for E2B CUDA smoke, `.models/google/gemma-4-12B-it-q4_k` for 12B Q4 CUDA
  validation, and `.models/unsloth/gemma-4-E2B-it-qat-GGUF/MTP/` for E2B MTP
  assistant GGUFs.
- CUDA uses the current device-side Gemma4 fast paths for Q4_K Q/K/V
  projection, Q4_K embedding lookup, fused head-norm+RoPE, device KV
  read/write, dense GQA attention, MTP masked argmax, and optional paged
  TurboQuant KV.
- CUDA TurboQuant KV status, measurements, and validation steps live in
  `CUDA.md` under "Gemma4 And TurboQuant KV Status". Gemma4 CUDA defaults remain
  `f32` KV for exactness. `--cache-dtype polar4` is the current
  production-candidate opt-in compressed-K/compressed-V path; `--cache-dtype
  turbo3` is resident and functional but still experimental.

The most direct user-facing E2B smoke is:

```sh
zig/pkg/inference/zig-out/bin/antfly-inference generate \
  .models/unsloth/gemma-4-E2B-it-qat-GGUF/gemma-4-E2B-it-qat-UD-Q4_K_XL.gguf \
  "Give a one sentence summary of Korean history." \
  --backend cuda \
  --max-tokens 128 \
  --print-timing \
  --print-token-count
```

If running from `zig/pkg/inference/zig-out/bin`, use the absolute model path
instead of `.models/...`; CUDA.md includes that copy-paste form.

Once the model artifacts are available, run the CUDA validation ladder in this
order:

```sh
zig build -Dcuda=true

zig/pkg/inference/zig-out/bin/antfly-inference cuda-info --smoke

zig/pkg/inference/zig-out/bin/antfly-inference cuda-info \
  --gemma4-parity /path/to/gemma4-12b-target.gguf

zig/pkg/inference/zig-out/bin/antfly-inference compare \
  /path/to/gemma4-12b-target /path/to/gemma4-12b-target \
  "Write one sentence about ants." \
  --runtime-parity \
  --backend cuda \
  --top-k 8 \
  --no-chat-template

zig/pkg/inference/zig-out/bin/antfly-inference generate \
  /path/to/gemma4-12b-target \
  "Write one sentence about ants." \
  --backend cuda \
  --max-tokens 16 \
  --temperature 0 \
  --print-token-ids \
  --print-timing

zig/pkg/inference/zig-out/bin/antfly-inference generate \
  /path/to/gemma4-12b-target \
  "Write one sentence about ants." \
  --backend cuda \
  --draft-model /path/to/gemma4-assistant \
  --speculative-k 2 \
  --max-tokens 16 \
  --temperature 0 \
  --debug-mtp \
  --print-token-ids \
  --print-timing
```

Useful CUDA/MTP isolation flags:

- `ANTFLY_CUDA_DISABLE_GEMMA4_MTP_DEVICE=1`: use the host clustered-output
  fallback instead of the CUDA MTP masked-argmax kernel.
- `ANTFLY_CUDA_ENABLE_Q4K_DECODE_FAST=1`: enable the experimental Q4_K tile8
  decode path.
- `ANTFLY_CUDA_DISABLE_HEAD_NORM_ROPE_FUSION=1`: disable fused
  head-norm+RoPE.
- `ANTFLY_CUDA_ENABLE_ADD_MUL_SCALAR_FUSION=1` and
  `ANTFLY_CUDA_ENABLE_RMSNORM_ADD_MUL_SCALAR_FUSION=1`: enable experimental
  output-scale fusions.
- `ANTFLY_GEMMA4_MTP_ALLOW_UNSHARED_TARGET=1`: force experimental MTP against
  targets missing shared-KV metadata.

Follow-up smoke after adding the earlier pre-final-RMSNorm target activation
path:

```text
bundle: pkg/inference/.debug/metal-command-20260505-162324
validation: MTL_DEBUG_LAYER=1, MTL_SHADER_VALIDATION=0
exit_code=0
diagnostic-reports: none
speculative: rounds=3 drafted=5 matched=0 rejected=5 accepted=3 corrections=3 bonus=0
```

This confirms the path is Metal-stable under API validation for the local repro,
but it did not improve acceptance against the quantized GGUF target. Later
source comparison with the extracted PyTorch drafter moved the handoff back to
final-RMSNorm hidden states.

Official safetensors target status:

```text
bundle: pkg/inference/.debug/metal-command-20260505-170606
validation: MTL_DEBUG_LAYER=1, MTL_SHADER_VALIDATION=0
command: antfly inference generate ~/.antfly/inference/models/google/gemma-4-E2B-it hi --backend metal --max-tokens 1
exit_code=0
diagnostic-reports: none
token_ids: 239863
timing_ms: load_model=2591 generate=1330 total=3926
```

Memory note: the first Metal-only safetensors attempt preserved BF16 in the
tensor store but then expanded rank-2 BF16 weights into cached f32 host slices
and duplicated BF16 bytes for the decoder runtime. That explains the observed
multi-10GB footprint. The Metal cache now keeps BF16 rank-2 dense weights as
mmap-backed native bytes, only materializing f32 for vectors and fallback paths
that actually require host math. A traced smoke peaked around 2.6GB physical
footprint instead of the earlier 30GB+ behavior.

Official target + official assistant status:

```text
bundle: pkg/inference/.debug/metal-command-20260506-161752
validation: MTL_DEBUG_LAYER=1, MTL_SHADER_VALIDATION=0
command: antfly inference generate ~/.antfly/inference/models/google/gemma-4-E2B-it hi --backend auto --draft-model ~/.antfly/inference/models/google/gemma-4-E2B-it-assistant --speculative-k 2 --max-tokens 4
exit_code=0
diagnostic-reports: none
token_ids: 10979 236888 2088 740
speculative: rounds=1 drafted=2 matched=2 rejected=0 accepted=3 corrections=0 bonus=1
```

This confirms the full Metal/safetensors target + assistant runtime runs without
Metal diagnostic reports and can accept the assistant's drafted span on the
short anchor prompt. Mixed GGUF target plus official safetensors assistant is
also supported for local smoke coverage, but acceptance-rate conclusions should
prefer official target+assistant pairs and the proper Gemma 4 chat template.

The repo smoke wrapper is
`scripts/gemma4/test_metal_gemma4_assistant_speculative.sh`. It uses `--backend auto`
by default so the normal backend selector can pick Metal when available; set
`ANTFLY_INFERENCE_GEMMA4_ASSISTANT_BACKEND=metal` to force Metal for crash/debug runs.
The official target currently needs the wrapper's default
`ANTFLY_INFERENCE_GEMMA4_ASSISTANT_HOST_BUDGET_MB=12288` and
`ANTFLY_INFERENCE_GEMMA4_ASSISTANT_COMBINED_BUDGET_MB=17408` preflight budgets.

## Metal GGUF Runtime Status: dated validator smoke history

Validator smoke on 2026-05-07:

```text
bundle: pkg/inference/.debug/metal-command-20260507-142101
validation: MTL_DEBUG_LAYER=1, MTL_SHADER_VALIDATION=0
command: antfly inference generate ~/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf hi --backend metal --mode compiled --compiled-target whole-model --max-tokens 4
exit_code=0
diagnostic-reports: none
token_ids: 10979 236888 2088 740
generate_timing_ms: prefill=875 decode=149 total=1024
metal_executor_ms: prefill_direct_family=871 greedy_calls=3 greedy_direct=149 greedy_fallback=0
metal_runtime_encoders: compute=21 blit=0 planned_scopes=35 planned_barriers=457
```

The generic quant runtime surface has been separated from the Q8_0-specific
kernel implementation. Public runtime scratch/setup exports, debug env vars,
and timing labels use `quant` names. The existing Q8_0 fused kernels remain
internal fast paths; adding Q4/K-quants should extend the quant-format dispatch
behind those generic entrypoints instead of creating more public `q80` API.
The direct whole-layer block planner now follows that shape too: it asks for a
direct quantized block format and currently selects the Q8_0 implementation
only when every participating linear slot is Q8_0. Unsupported or mixed formats
fall back through the staged generic quant linear path.
The staged FFN side can still use existing fused Metal kernels for non-Q8
families: homogeneous Q4_K, Q6_K, I2_S, TL1/TL2, Q8_0, plus mixed
Q4_K/Q5_K-down, Q4_K/Q6_K-down, and Q4_0/Q8_0-down layouts. The planner now
marks those combinations as direct-eligible instead of logging them as mixed or
unsupported before the runtime has a chance to use the fused path.
The device-resident FFN residual path follows the same generic shape: Q8_0
keeps the monolithic fused kernel, while non-Q8 formats that have staged pair
and single-stage Metal kernels compose gate/up, activation, multiply,
optional RMS norms, down projection, and residual add without leaving device
memory. That removes the old Q8-only boundary without adding format-specific
public APIs.

A standalone prepared-tail greedy shortcut that directly encoded
`rms_norm + quantized lm_head + argmax` outside a planned frame was tested and
backed out after a 2026-05-07 SoC watchdog reset under Metal API validation
(`pkg/inference/.debug/metal-command-20260507-214829`, panic
`/Library/Logs/DiagnosticReports/Retired/panic-base-2026-05-07-214909.panic`).
Keep that path on the materialized-logits argmax route until the command
lifetime/barrier issue is isolated. After reverting, minimal API-validation
smoke `pkg/inference/.debug/metal-command-20260507-215452` completed with
`token_ids: 10979` and no new diagnostic reports.

The native Metal GGUF route must not depend on MLX availability when Antfly inference is
built with both backends enabled. A later 4-token compiled whole-model smoke was
failing before model execution with `MlxMetalUnavailable`; the long-term fix is
to keep `.metal` sessions on the native Metal provider/stream path and reserve
MLX streams/providers for the `.mlx` backend. The repaired smoke
`pkg/inference/.debug/metal-command-20260507-222950` passed API validation with
`token_ids: 10979 236888 2088 740`, `prefill=157ms`, `decode=149ms`,
`total=1006ms`, and no diagnostic reports.
