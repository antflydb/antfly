#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
usage: gemma4_qat_llamacpp_pair_benchmark.sh

Alternates Antfly and llama.cpp on the Gemma 4 E4B QAT Q4_0 CUDA benchmark
and writes raw outputs plus paired summary artifacts.

Environment overrides:
  ANTFLY_BIN              antfly-inference binary
  LLAMA_CPP_BIN           llama.cpp llama-completion binary
  MODEL                   Gemma 4 E4B QAT q4_0 GGUF file or model directory
  OUT_DIR                 output directory
  REPEATS                 paired samples per engine (default: 5)
  PROMPT                  raw prompt
  PROMPT_REPEAT           repeat PROMPT this many times before running (default: 1)
  ANTFLY_PREFILL_CHUNK_SIZE
                          Antfly prefill chunk size (default: 32)
  ANTFLY_TOKENS           Antfly generated-token request (default: 511)
  LLAMA_TOKENS            llama.cpp n_predict request (default: 512)
  ANTFLY_CACHE_DTYPE      Antfly KV cache dtype (default: polar4)
  LLAMA_CACHE_TYPE_K      llama.cpp K cache type (default: q4_0)
  LLAMA_CACHE_TYPE_V      llama.cpp V cache type (default: q4_0)
  ANTFLY_Q4_0_Q8_1_PREFILL_ROWS
                          1 to enable row-batched Q8_1 QAT prefill kernels (default: 1)
  ANTFLY_GQA_PREFILL_FAST
                          1 to enable the fast row-batched GQA prefill kernel (default: 1)
  ANTFLY_Q4_0_LINEAR_Q8_1_TILE4_W8_MIN_IN_DIM
                          Minimum input dimension for W8 row-prefill linear kernels (default: 2048)
  ANTFLY_Q4_0_LINEAR_Q8_1_ROWS8_C4
                          1 to enable rows8/c4 row-prefill linear kernels (default: 1)
  ANTFLY_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS8_C2
                          1 to enable lower-register rows8/c2 FFN pair activation (default: 1)
  ANTFLY_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS16_C1
                          1 to enable rows16/c1 FFN pair activation (default: 1)
  ANTFLY_CUDA_GEMMA_PREFILL_PREWARM
                          1 to move CUDA Gemma residency/prewarm before measured prefill (default: 1)
  ANTFLY_CUDA_PREFILL_FIRST_TOKEN
                          1 to allow CUDA max-token=1 prefill to coalesce past scheduler chunks (default: 1)
  ANTFLY_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS
                          Max prompt tokens eligible for first-token coalescing (default: 2048)
  ANTFLY_CUDA_PROFILE_PREFILL_OPS
                          1 to collect CUDA-event prefill op timing buckets; diagnostic only (default: 0)
  ANTFLY_RMS_NORM_BF16_MIRROR
                          1 to let RMSNorm write a BF16 activation mirror for cuBLASLt staging reuse (default: 0)
  ANTFLY_GQA_PREFILL_TILED
                          1 to use the tiled turboquant prefill attention kernel (default: 0)
  ANTFLY_GQA_PREFILL_MMA
                          1 to use the tensor-core (wmma) turboquant prefill attention kernel for head_dim <= 256 (default: 0)
  ANTFLY_BF16_RESIDENT_WEIGHTS
                          1 to dequantize Q4_0 matrix weights to BF16 at upload (BF16-resident prefill path) (default: 0)
  ANTFLY_HYBRID_BF16_PREFILL
                          1 to keep Q4_0 weights for decode and attach BF16 copies used by prefill matmuls (default: 0)
  ANTFLY_PLE_MODEL_PROJ_BF16
                          1 to convert F32 PLE model projections to BF16 at upload (default: same as ANTFLY_BF16_RESIDENT_WEIGHTS)
  TIMEOUT                 per-command timeout, or off (default: 360s)
  REQUIRE_ANTFLY_WIN      1 to fail when Antfly median is not faster (default: 0)
  MIN_WIN_MS              required median E2E win in ms when enforcing (default: 0)
  REQUIRE_ANTFLY_PREFILL_WIN
                          1 to fail when Antfly median prefill is not faster (default: 0)
  MIN_PREFILL_WIN_MS      required median prefill win in ms when enforcing (default: 0)
  REQUIRE_ANTFLY_DECODE_WIN
                          1 to fail when Antfly median decode is not faster (default: 0)
  MIN_DECODE_WIN_MS       required median decode win in ms when enforcing (default: 0)
  REQUIRE_QAT_PREFILL_ROWS4
                          1 to fail unless QAT rows4 prefill counters are active (default: 0)
  REQUIRE_QAT_PREFILL_ROWS16_C1
                          1 to fail unless rows16/c1 FFN pair counters are active (default: 0)
  REQUIRE_QAT_PREFILL_LINEAR_ROWS8_C4
                          1 to fail unless rows8/c4 linear/gated-down counters are active (default: 0)
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi
if [[ $# -ne 0 ]]; then
  usage >&2
  exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
inference_dir="$repo_root/zig/pkg/inference"
antfly_bin="${ANTFLY_BIN:-${ANTFY_BIN:-$inference_dir/zig-out/bin/antfly-inference}}"
llama_cpp_bin="${LLAMA_CPP_BIN:-/tmp/llama.cpp/build/bin/llama-completion}"
model="${MODEL:-$repo_root/.models/google/gemma-4-E4B-it-qat-q4_0-gguf/gemma-4-E4B_q4_0-it.gguf}"
out_dir="${OUT_DIR:-/tmp/antfly-gemma4-qat-llamacpp-paired-$(date -u +%Y%m%dT%H%M%SZ)}"
repeats="${REPEATS:-5}"
base_prompt="${PROMPT:-Here is a sentence about ants:}"
prompt_repeat="${PROMPT_REPEAT:-1}"
antfly_prefill_chunk_size="${ANTFLY_PREFILL_CHUNK_SIZE:-32}"
antfly_tokens="${ANTFLY_TOKENS:-511}"
llama_tokens="${LLAMA_TOKENS:-512}"
antfly_cache_dtype="${ANTFLY_CACHE_DTYPE:-polar4}"
llama_cache_type_k="${LLAMA_CACHE_TYPE_K:-q4_0}"
llama_cache_type_v="${LLAMA_CACHE_TYPE_V:-q4_0}"
antfly_q4_0_q8_1_prefill_rows="${ANTFLY_Q4_0_Q8_1_PREFILL_ROWS:-1}"
antfly_gqa_prefill_fast="${ANTFLY_GQA_PREFILL_FAST:-1}"
# Wrapper vars fall back to the raw production env name so callers that
# export ANTFLY_INFERENCE_CUDA_* directly are honored rather than clobbered.
antfly_gqa_prefill_tiled="${ANTFLY_GQA_PREFILL_TILED:-${ANTFLY_INFERENCE_CUDA_GQA_PREFILL_TILED:-0}}"
antfly_gqa_prefill_mma="${ANTFLY_GQA_PREFILL_MMA:-${ANTFLY_INFERENCE_CUDA_GQA_PREFILL_MMA:-0}}"
antfly_q4_0_linear_q8_1_tile4_w8_min_in_dim="${ANTFLY_Q4_0_LINEAR_Q8_1_TILE4_W8_MIN_IN_DIM:-2048}"
antfly_q4_0_linear_q8_1_rows8_c4="${ANTFLY_Q4_0_LINEAR_Q8_1_ROWS8_C4:-1}"
antfly_q4_0_pair_activation_q8_1_rows8_c2="${ANTFLY_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS8_C2:-1}"
antfly_q4_0_pair_activation_q8_1_rows16_c1="${ANTFLY_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS16_C1:-1}"
antfly_cuda_gemma_prefill_prewarm="${ANTFLY_CUDA_GEMMA_PREFILL_PREWARM:-1}"
antfly_cuda_prefill_first_token="${ANTFLY_CUDA_PREFILL_FIRST_TOKEN:-1}"
antfly_cuda_prefill_first_token_coalesce_tokens="${ANTFLY_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS:-2048}"
antfly_cuda_profile_prefill_ops="${ANTFLY_CUDA_PROFILE_PREFILL_OPS:-0}"
antfly_rms_norm_bf16_mirror="${ANTFLY_RMS_NORM_BF16_MIRROR:-${ANTFLY_INFERENCE_CUDA_RMS_NORM_BF16_MIRROR:-0}}"
antfly_bf16_resident_weights="${ANTFLY_BF16_RESIDENT_WEIGHTS:-${ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16:-0}}"
antfly_hybrid_bf16_prefill="${ANTFLY_HYBRID_BF16_PREFILL:-${ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL:-0}}"
antfly_ple_model_proj_bf16="${ANTFLY_PLE_MODEL_PROJ_BF16:-${ANTFLY_INFERENCE_CUDA_PLE_MODEL_PROJ_BF16:-$antfly_bf16_resident_weights}}"
command_timeout="${TIMEOUT:-360s}"
require_antfly_win="${REQUIRE_ANTFLY_WIN:-0}"
min_win_ms="${MIN_WIN_MS:-0}"
require_antfly_prefill_win="${REQUIRE_ANTFLY_PREFILL_WIN:-0}"
min_prefill_win_ms="${MIN_PREFILL_WIN_MS:-0}"
require_antfly_decode_win="${REQUIRE_ANTFLY_DECODE_WIN:-0}"
min_decode_win_ms="${MIN_DECODE_WIN_MS:-0}"
require_qat_prefill_rows4="${REQUIRE_QAT_PREFILL_ROWS4:-0}"
require_qat_prefill_rows16_c1="${REQUIRE_QAT_PREFILL_ROWS16_C1:-0}"
require_qat_prefill_linear_rows8_c4="${REQUIRE_QAT_PREFILL_LINEAR_ROWS8_C4:-0}"

case "$repeats" in
  ''|*[!0-9]*)
    echo "REPEATS must be a positive integer" >&2
    exit 2
    ;;
esac
if [[ "$repeats" -lt 1 ]]; then
  echo "REPEATS must be a positive integer" >&2
  exit 2
fi
case "$prompt_repeat" in
  ''|*[!0-9]*)
    echo "PROMPT_REPEAT must be a positive integer" >&2
    exit 2
    ;;
esac
if [[ "$prompt_repeat" -lt 1 ]]; then
  echo "PROMPT_REPEAT must be a positive integer" >&2
  exit 2
fi
case "$antfly_prefill_chunk_size" in
  ''|*[!0-9]*)
    echo "ANTFLY_PREFILL_CHUNK_SIZE must be a positive integer" >&2
    exit 2
    ;;
esac
if [[ "$antfly_prefill_chunk_size" -lt 1 ]]; then
  echo "ANTFLY_PREFILL_CHUNK_SIZE must be a positive integer" >&2
  exit 2
fi

prompt=""
for ((prompt_index = 0; prompt_index < prompt_repeat; prompt_index++)); do
  prompt+="$base_prompt"
  if [[ "$prompt_index" -lt $((prompt_repeat - 1)) ]]; then
    prompt+=" "
  fi
done
prompt_bytes="${#prompt}"

# Decode graph replay stops silently once total sequence exceeds the forced
# KV capture capacity, so default it to cover the estimated prompt plus all
# generated tokens (rounded up to a page) instead of a fixed constant.
prompt_token_estimate=$(((prompt_bytes + 3) / 4))
capacity_estimate=$(((prompt_token_estimate + antfly_tokens + 64 + 31) / 32 * 32))
if [[ "$capacity_estimate" -lt 544 ]]; then
  capacity_estimate=544
fi

require_path() {
  local label="$1"
  local path="$2"
  if [[ ! -e "$path" ]]; then
    echo "missing $label: $path" >&2
    exit 1
  fi
}

run_maybe_timeout() {
  if [[ -n "$command_timeout" && "$command_timeout" != "0" && "$command_timeout" != "off" && "$command_timeout" != "none" ]]; then
    timeout "$command_timeout" "$@"
  else
    "$@"
  fi
}

require_path "antfly-inference binary" "$antfly_bin"
require_path "llama.cpp binary" "$llama_cpp_bin"
require_path "Gemma 4 E4B QAT model" "$model"
mkdir -p "$out_dir"

common_antfly_env=(
  ANTFLY_INFERENCE_CUDA_ASYNC_I32_DOWNLOAD_STAGING=1
  ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION=1
  ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION_CHUNK=12
  ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_PRECOMPUTE=1
  ANTFLY_INFERENCE_CUDA_Q4_0_GATE_UP_ACTIVATION_Q8_1_PRECOMPUTE=1
  ANTFLY_INFERENCE_CUDA_Q4_0_Q8_1_PREFILL_ROWS="$antfly_q4_0_q8_1_prefill_rows"
  ANTFLY_INFERENCE_CUDA_GQA_PREFILL_FAST="$antfly_gqa_prefill_fast"
  ANTFLY_INFERENCE_CUDA_GQA_PREFILL_TILED="$antfly_gqa_prefill_tiled"
  ANTFLY_INFERENCE_CUDA_GQA_PREFILL_MMA="$antfly_gqa_prefill_mma"
  ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8=1
  ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W8_MIN_IN_DIM="$antfly_q4_0_linear_q8_1_tile4_w8_min_in_dim"
  ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_ROWS8_C4="$antfly_q4_0_linear_q8_1_rows8_c4"
  ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS8_C2="$antfly_q4_0_pair_activation_q8_1_rows8_c2"
  ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS16_C1="$antfly_q4_0_pair_activation_q8_1_rows16_c1"
  ANTFLY_INFERENCE_CUDA_GEMMA_PREFILL_PREWARM="$antfly_cuda_gemma_prefill_prewarm"
  ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN="$antfly_cuda_prefill_first_token"
  ANTFLY_INFERENCE_CUDA_PREFILL_FIRST_TOKEN_COALESCE_TOKENS="$antfly_cuda_prefill_first_token_coalesce_tokens"
  ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS="$antfly_cuda_profile_prefill_ops"
  ANTFLY_INFERENCE_CUDA_RMS_NORM_BF16_MIRROR="$antfly_rms_norm_bf16_mirror"
  ANTFLY_INFERENCE_CUDA_DEQUANTIZE_Q4_0_MATRIX_WEIGHTS_BF16="$antfly_bf16_resident_weights"
  ANTFLY_INFERENCE_CUDA_Q4_0_WEIGHTS_BF16_PREFILL="$antfly_hybrid_bf16_prefill"
  ANTFLY_INFERENCE_CUDA_PLE_MODEL_PROJ_BF16="$antfly_ple_model_proj_bf16"
  ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W10_E4B_DOWN=1
  ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_TILE4_W8=1
  ANTFLY_INFERENCE_CUDA_GREEDY_PENDING_TOKEN_READBACK=1
  ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_DP4A=1
  ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_DP4A=1
  ANTFLY_INFERENCE_CUDA_Q4_0_QKV_Q8_1_TILE8=1
  ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_Q8_1_DP4A=1
  ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_DP4A=1
  ANTFLY_INFERENCE_CUDA_Q4_0_ACTIVATION_SLICE_Q8_1_DP4A=1
  ANTFLY_INFERENCE_CUDA_Q4_0_GATED_DOWN_Q8_1_DP4A=1
  ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1=1
  ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1_TILE8_EXACT_THREADS=1
  ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY=required
  ANTFLY_INFERENCE_CUDA_CAPTURE_FINAL_HIDDEN=1
  ANTFLY_INFERENCE_CUDA_CAPTURE_UPDATE_EXEC=1
  ANTFLY_INFERENCE_CUDA_CAPTURE_DEVICE_SCALARS=1
  ANTFLY_INFERENCE_CUDA_CAPTURE_PERSISTENT_REPLAY=1
  ANTFLY_INFERENCE_CUDA_CAPTURE_GREEDY_TOKEN=1
  ANTFLY_INFERENCE_CUDA_TEMP_SLOT_PERIOD=863
  ANTFLY_INFERENCE_CUDA_TEMP_SLOT_SKIP=2500
  ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY="${ANTFLY_CAPTURE_FORCE_KV_CAPACITY:-$capacity_estimate}"
)

run_antfly_command() {
  if [[ -n "$command_timeout" && "$command_timeout" != "0" && "$command_timeout" != "off" && "$command_timeout" != "none" ]]; then
    env "${common_antfly_env[@]}" timeout "$command_timeout" "$@"
  else
    env "${common_antfly_env[@]}" "$@"
  fi
}

run_antfly() {
  local index="$1"
  local json_path="$out_dir/antfly_${index}.json"
  local log_path="$out_dir/antfly_${index}.log"
  rm -f "$json_path" "$log_path"
  echo "RUN antfly sample=$index"
  run_antfly_command "$antfly_bin" generate "$model" "$prompt" \
    --backend cuda \
    --combined-budget-mb 22000 \
    --backend-budget-mb 19000 \
    --kv-budget-mb 1024 \
    --scratch-budget-mb 2048 \
    --prefill-chunk-size "$antfly_prefill_chunk_size" \
    --max-tokens "$antfly_tokens" \
    --temperature 0 \
    --raw-prompt \
    --no-chat-template \
    --ignore-eos \
    --cache-dtype "$antfly_cache_dtype" \
    --print-timing \
    --print-token-count \
    --json-timing "$json_path" >"$log_path" 2>&1
}

run_llama() {
  local index="$1"
  local log_path="$out_dir/llama_${index}.log"
  rm -f "$log_path"
  echo "RUN llama.cpp sample=$index"
  run_maybe_timeout "$llama_cpp_bin" \
    -m "$model" \
    -p "$prompt" \
    -n "$llama_tokens" \
    -c 2048 \
    -ngl 999 \
    -ctk "$llama_cache_type_k" \
    -ctv "$llama_cache_type_v" \
    --temp 0 \
    --top-k 64 \
    --top-p 0.95 \
    --min-p 0.05 \
    -s 3060418694 \
    -no-cnv \
    --no-display-prompt \
    --ignore-eos >"$log_path" 2>&1
}

for ((i = 1; i <= repeats; i++)); do
  run_antfly "$i"
  run_llama "$i"
done

python3 - "$out_dir" "$repeats" "$require_antfly_win" "$min_win_ms" \
  "$require_antfly_prefill_win" "$min_prefill_win_ms" \
  "$require_antfly_decode_win" "$min_decode_win_ms" \
  "$antfly_tokens" "$llama_tokens" "$antfly_cache_dtype" "$llama_cache_type_k" "$llama_cache_type_v" \
  "$prompt_repeat" "$prompt_bytes" "$antfly_prefill_chunk_size" "$antfly_q4_0_q8_1_prefill_rows" "$antfly_gqa_prefill_fast" \
  "$antfly_q4_0_linear_q8_1_tile4_w8_min_in_dim" "$antfly_q4_0_linear_q8_1_rows8_c4" "$antfly_q4_0_pair_activation_q8_1_rows8_c2" \
  "$antfly_q4_0_pair_activation_q8_1_rows16_c1" "$antfly_cuda_gemma_prefill_prewarm" \
  "$antfly_cuda_prefill_first_token" "$antfly_cuda_prefill_first_token_coalesce_tokens" \
  "$antfly_cuda_profile_prefill_ops" "$antfly_rms_norm_bf16_mirror" \
  "$require_qat_prefill_rows4" "$require_qat_prefill_rows16_c1" "$require_qat_prefill_linear_rows8_c4" <<'PY'
import json
import math
import pathlib
import re
import statistics
import sys

out_dir = pathlib.Path(sys.argv[1])
repeats = int(sys.argv[2])
require_win = sys.argv[3].lower() not in {"0", "false", "off", "no"}
min_win_ms = float(sys.argv[4])
require_prefill_win = sys.argv[5].lower() not in {"0", "false", "off", "no"}
min_prefill_win_ms = float(sys.argv[6])
require_decode_win = sys.argv[7].lower() not in {"0", "false", "off", "no"}
min_decode_win_ms = float(sys.argv[8])
antfly_tokens = int(sys.argv[9])
llama_tokens = int(sys.argv[10])
antfly_cache_dtype = sys.argv[11]
llama_cache_type_k = sys.argv[12]
llama_cache_type_v = sys.argv[13]
prompt_repeat = int(sys.argv[14])
prompt_bytes = int(sys.argv[15])
antfly_prefill_chunk_size = int(sys.argv[16])
antfly_q4_0_q8_1_prefill_rows = sys.argv[17]
antfly_gqa_prefill_fast = sys.argv[18]
antfly_q4_0_linear_q8_1_tile4_w8_min_in_dim = sys.argv[19]
antfly_q4_0_linear_q8_1_rows8_c4 = sys.argv[20]
antfly_q4_0_pair_activation_q8_1_rows8_c2 = sys.argv[21]
antfly_q4_0_pair_activation_q8_1_rows16_c1 = sys.argv[22]
antfly_cuda_gemma_prefill_prewarm = sys.argv[23]
antfly_cuda_prefill_first_token = sys.argv[24]
antfly_cuda_prefill_first_token_coalesce_tokens = sys.argv[25]
antfly_cuda_profile_prefill_ops = sys.argv[26]
antfly_rms_norm_bf16_mirror = sys.argv[27]
require_qat_prefill_rows4 = sys.argv[28].lower() not in {"0", "false", "off", "no"}
require_qat_prefill_rows16_c1 = sys.argv[29].lower() not in {"0", "false", "off", "no"}
require_qat_prefill_linear_rows8_c4 = sys.argv[30].lower() not in {"0", "false", "off", "no"}

llama_patterns = {
    "sampling_ms": re.compile(r"(?m)^[^\n]*perf_print:\s+sampling time =\s+([0-9.]+) ms"),
    "prompt_eval": re.compile(
        r"(?m)^[^\n]*perf_print:\s+prompt eval time =\s+([0-9.]+) ms\s*/\s*([0-9]+) [^\n]*?,\s*([0-9.]+) tokens per second"
    ),
    "eval": re.compile(
        r"(?m)^[^\n]*perf_print:\s+eval time =\s+([0-9.]+) ms\s*/\s*([0-9]+) [^\n]*?,\s*([0-9.]+) tokens per second"
    ),
    "total_ms": re.compile(r"(?m)^[^\n]*perf_print:\s+total time =\s+([0-9.]+) ms"),
    "graphs_reused": re.compile(r"(?m)^[^\n]*perf_print:\s+graphs reused =\s+([0-9]+)"),
}

def percentile(values, pct):
    ordered = sorted(values)
    if not ordered:
        return None
    index = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return ordered[index]

def stats(values):
    return {
        "min": min(values),
        "median": statistics.median(values),
        "avg": sum(values) / len(values),
        "max": max(values),
        "p95": percentile(values, 95),
    }

rows = []
antfly_totals = []
antfly_prefills = []
antfly_decodes = []
llama_totals = []
llama_prompts = []
llama_evals = []
llama_decode_plus_samplings = []
errors = []
for index in range(1, repeats + 1):
    antfly_path = out_dir / f"antfly_{index}.json"
    llama_path = out_dir / f"llama_{index}.log"
    if not antfly_path.exists():
        errors.append(f"missing {antfly_path}")
        continue
    if not llama_path.exists():
        errors.append(f"missing {llama_path}")
        continue

    antfly = json.loads(antfly_path.read_text(encoding="utf-8"))
    timing = antfly.get("timing_ms") or {}
    antfly_total = float(timing.get("generate") or timing.get("total_inner") or 0.0)
    antfly_prefill = float(timing.get("prefill_inner") or 0.0)
    antfly_decode = float(timing.get("decode_inner") or 0.0)
    antfly_tps = float(antfly.get("decode_tok_per_s") or 0.0)
    cuda = antfly.get("cuda") or {}
    antfly_replays = int(cuda.get("graph_capture_replays") or 0)
    antfly_discards = int(cuda.get("graph_capture_discards") or 0)
    antfly_precompute = int(cuda.get("gated_down_fused_q4_0_precompute") or 0)
    antfly_tile4 = int(cuda.get("gated_down_fused_q4_0_tile4") or 0)
    antfly_q8_1_prefill_linear = int(cuda.get("q4_0_q8_1_prefill_linear_hits") or 0)
    antfly_q8_1_prefill_linear_rows4 = int(cuda.get("q4_0_q8_1_prefill_linear_rows4_hits") or 0)
    antfly_q8_1_prefill_linear_rows8_c4 = int(cuda.get("q4_0_q8_1_prefill_linear_rows8_c4_hits") or 0)
    antfly_q8_1_prefill_linear_generic_rows = int(cuda.get("q4_0_q8_1_prefill_linear_generic_rows_hits") or 0)
    antfly_q8_1_prefill_qkv = int(cuda.get("q4_0_q8_1_prefill_qkv_hits") or 0)
    antfly_q8_1_prefill_qkv_rows4 = int(cuda.get("q4_0_q8_1_prefill_qkv_rows4_hits") or 0)
    antfly_q8_1_prefill_pair = int(cuda.get("q4_0_q8_1_prefill_pair_hits") or 0)
    antfly_q8_1_prefill_pair_rows4 = int(cuda.get("q4_0_q8_1_prefill_pair_rows4_hits") or 0)
    antfly_q8_1_prefill_pair_rows8_c2 = int(cuda.get("q4_0_q8_1_prefill_pair_rows8_c2_hits") or 0)
    antfly_q8_1_prefill_pair_rows16_c1 = int(cuda.get("q4_0_q8_1_prefill_pair_rows16_c1_hits") or 0)
    antfly_q8_1_prefill_gated_down = int(cuda.get("q4_0_q8_1_prefill_gated_down_hits") or 0)
    antfly_q8_1_prefill_gated_down_rows4 = int(cuda.get("q4_0_q8_1_prefill_gated_down_rows4_hits") or 0)
    antfly_q8_1_prefill_gated_down_rows8_c4 = int(cuda.get("q4_0_q8_1_prefill_gated_down_rows8_c4_hits") or 0)
    antfly_gqa_prefill_fast_hits = int(cuda.get("launch_attention_gqa_prefill_fast") or 0)
    antfly_prefill_profile_events = int(cuda.get("prefill_profile_events") or 0)
    antfly_prefill_profile_q4_linear_us = int(cuda.get("prefill_profile_q4_linear_us") or 0)
    antfly_prefill_profile_q4_qkv_us = int(cuda.get("prefill_profile_q4_qkv_us") or 0)
    antfly_prefill_profile_q4_pair_us = int(cuda.get("prefill_profile_q4_pair_us") or 0)
    antfly_prefill_profile_q4_gated_down_us = int(cuda.get("prefill_profile_q4_gated_down_us") or 0)
    antfly_prefill_profile_attention_us = int(cuda.get("prefill_profile_attention_us") or 0)
    antfly_prefill_profile_ple_dense_us = int(cuda.get("prefill_profile_ple_dense_us") or 0)
    antfly_bf16_cublaslt_activation_staging = int(cuda.get("bf16_cublaslt_activation_staging_calls") or 0)
    antfly_bf16_cublaslt_activation_mirror = int(cuda.get("bf16_cublaslt_activation_mirror_hits") or 0)
    antfly_rms_norm_bf16_mirror_hits = int(cuda.get("rms_norm_bf16_mirror_hits") or 0)

    text = llama_path.read_text(encoding="utf-8", errors="replace")
    parsed = {}
    for name, pattern in llama_patterns.items():
        match = pattern.search(text)
        if not match:
            if name in {"prompt_eval", "eval"}:
                parsed[name] = {"ms": 0.0, "runs": 0, "tok_s": 0.0}
            else:
                parsed[name] = 0.0
            if not (name == "eval" and llama_tokens <= 1):
                errors.append(f"missing {name} in {llama_path}")
            continue
        if name in {"prompt_eval", "eval"}:
            parsed[name] = {
                "ms": float(match.group(1)),
                "runs": int(match.group(2)),
                "tok_s": float(match.group(3)),
            }
        else:
            parsed[name] = float(match.group(1))

    llama_total = parsed["total_ms"]
    llama_sampling = parsed["sampling_ms"]
    llama_eval = parsed["eval"]["ms"]
    llama_eval_runs = parsed["eval"]["runs"]
    llama_eval_tps = parsed["eval"]["tok_s"]
    llama_prompt = parsed["prompt_eval"]["ms"]
    llama_prompt_runs = parsed["prompt_eval"]["runs"]
    llama_prompt_tps = parsed["prompt_eval"]["tok_s"]
    llama_graphs = int(parsed["graphs_reused"])
    antfly_totals.append(antfly_total)
    antfly_prefills.append(antfly_prefill)
    antfly_decodes.append(antfly_decode)
    llama_totals.append(llama_total)
    llama_prompts.append(llama_prompt)
    llama_evals.append(llama_eval)
    llama_decode_plus_samplings.append(llama_eval + llama_sampling)
    rows.append(
        {
            "sample": index,
            "antfly_total_ms": antfly_total,
            "antfly_prefill_ms": antfly_prefill,
            "antfly_decode_ms": antfly_decode,
            "antfly_tok_s": antfly_tps,
            "antfly_replays": antfly_replays,
            "antfly_discards": antfly_discards,
            "antfly_gated_down_precompute": antfly_precompute,
            "antfly_gated_down_tile4": antfly_tile4,
            "antfly_q8_1_prefill_linear": antfly_q8_1_prefill_linear,
            "antfly_q8_1_prefill_linear_rows4": antfly_q8_1_prefill_linear_rows4,
            "antfly_q8_1_prefill_linear_rows8_c4": antfly_q8_1_prefill_linear_rows8_c4,
            "antfly_q8_1_prefill_linear_generic_rows": antfly_q8_1_prefill_linear_generic_rows,
            "antfly_q8_1_prefill_qkv": antfly_q8_1_prefill_qkv,
            "antfly_q8_1_prefill_qkv_rows4": antfly_q8_1_prefill_qkv_rows4,
            "antfly_q8_1_prefill_pair": antfly_q8_1_prefill_pair,
            "antfly_q8_1_prefill_pair_rows4": antfly_q8_1_prefill_pair_rows4,
            "antfly_q8_1_prefill_pair_rows8_c2": antfly_q8_1_prefill_pair_rows8_c2,
            "antfly_q8_1_prefill_pair_rows16_c1": antfly_q8_1_prefill_pair_rows16_c1,
            "antfly_q8_1_prefill_gated_down": antfly_q8_1_prefill_gated_down,
            "antfly_q8_1_prefill_gated_down_rows4": antfly_q8_1_prefill_gated_down_rows4,
            "antfly_q8_1_prefill_gated_down_rows8_c4": antfly_q8_1_prefill_gated_down_rows8_c4,
            "antfly_gqa_prefill_fast_hits": antfly_gqa_prefill_fast_hits,
            "antfly_prefill_profile_events": antfly_prefill_profile_events,
            "antfly_prefill_profile_q4_linear_us": antfly_prefill_profile_q4_linear_us,
            "antfly_prefill_profile_q4_qkv_us": antfly_prefill_profile_q4_qkv_us,
            "antfly_prefill_profile_q4_pair_us": antfly_prefill_profile_q4_pair_us,
            "antfly_prefill_profile_q4_gated_down_us": antfly_prefill_profile_q4_gated_down_us,
            "antfly_prefill_profile_attention_us": antfly_prefill_profile_attention_us,
            "antfly_prefill_profile_ple_dense_us": antfly_prefill_profile_ple_dense_us,
            "antfly_bf16_cublaslt_activation_staging": antfly_bf16_cublaslt_activation_staging,
            "antfly_bf16_cublaslt_activation_mirror": antfly_bf16_cublaslt_activation_mirror,
            "antfly_rms_norm_bf16_mirror_hits": antfly_rms_norm_bf16_mirror_hits,
            "llama_total_ms": llama_total,
            "llama_sampling_ms": llama_sampling,
            "llama_prompt_eval_ms": llama_prompt,
            "llama_prompt_eval_runs": llama_prompt_runs,
            "llama_prompt_eval_tok_s": llama_prompt_tps,
            "llama_eval_ms": llama_eval,
            "llama_eval_runs": llama_eval_runs,
            "llama_eval_tok_s": llama_eval_tps,
            "llama_graphs_reused": llama_graphs,
            "antfly_win_ms": llama_total - antfly_total,
            "antfly_prefill_win_ms": llama_prompt - antfly_prefill,
            "antfly_decode_win_ms": llama_eval - antfly_decode,
            "antfly_decode_plus_sampling_win_ms": llama_eval + llama_sampling - antfly_decode,
        }
    )

if errors:
    for error in errors:
        print(error, file=sys.stderr)
    raise SystemExit(1)

summary = {
    "repeats": repeats,
    "comparison": {
        "prompt": "raw prompt, no chat template",
        "prompt_repeat": prompt_repeat,
        "prompt_bytes": prompt_bytes,
        "antfly_prefill_chunk_size": antfly_prefill_chunk_size,
        "antfly_tokens": antfly_tokens,
        "llama_tokens": llama_tokens,
        "antfly_cache_dtype": antfly_cache_dtype,
        "llama_cache_type_k": llama_cache_type_k,
        "llama_cache_type_v": llama_cache_type_v,
        "antfly_q4_0_q8_1_prefill_rows": antfly_q4_0_q8_1_prefill_rows,
        "antfly_gqa_prefill_fast": antfly_gqa_prefill_fast,
        "antfly_q4_0_linear_q8_1_tile4_w8_min_in_dim": antfly_q4_0_linear_q8_1_tile4_w8_min_in_dim,
        "antfly_q4_0_linear_q8_1_rows8_c4": antfly_q4_0_linear_q8_1_rows8_c4,
        "antfly_q4_0_pair_activation_q8_1_rows8_c2": antfly_q4_0_pair_activation_q8_1_rows8_c2,
        "antfly_q4_0_pair_activation_q8_1_rows16_c1": antfly_q4_0_pair_activation_q8_1_rows16_c1,
        "antfly_cuda_gemma_prefill_prewarm": antfly_cuda_gemma_prefill_prewarm,
        "antfly_cuda_prefill_first_token": antfly_cuda_prefill_first_token,
        "antfly_cuda_prefill_first_token_coalesce_tokens": antfly_cuda_prefill_first_token_coalesce_tokens,
        "antfly_cuda_profile_prefill_ops": antfly_cuda_profile_prefill_ops,
        "antfly_rms_norm_bf16_mirror": antfly_rms_norm_bf16_mirror,
    },
    "antfly_total_ms": stats(antfly_totals),
    "antfly_prefill_ms": stats(antfly_prefills),
    "antfly_decode_ms": stats(antfly_decodes),
    "llama_total_ms": stats(llama_totals),
    "llama_prompt_eval_ms": stats(llama_prompts),
    "llama_eval_ms": stats(llama_evals),
    "llama_decode_plus_sampling_ms": stats(llama_decode_plus_samplings),
    "win_ms": stats([row["antfly_win_ms"] for row in rows]),
    "prefill_win_ms": stats([row["antfly_prefill_win_ms"] for row in rows]),
    "decode_win_ms": stats([row["antfly_decode_win_ms"] for row in rows]),
    "decode_plus_sampling_win_ms": stats([row["antfly_decode_plus_sampling_win_ms"] for row in rows]),
    "rows": rows,
}
summary["antfly_median_win_ms"] = summary["llama_total_ms"]["median"] - summary["antfly_total_ms"]["median"]
summary["antfly_median_prefill_win_ms"] = summary["llama_prompt_eval_ms"]["median"] - summary["antfly_prefill_ms"]["median"]
summary["antfly_median_decode_win_ms"] = summary["llama_eval_ms"]["median"] - summary["antfly_decode_ms"]["median"]
summary["antfly_median_decode_plus_sampling_win_ms"] = summary["llama_decode_plus_sampling_ms"]["median"] - summary["antfly_decode_ms"]["median"]
summary["ok_total"] = (not require_win) or summary["antfly_median_win_ms"] >= min_win_ms
summary["ok_prefill"] = (not require_prefill_win) or summary["antfly_median_prefill_win_ms"] >= min_prefill_win_ms
summary["ok_decode"] = (not require_decode_win) or summary["antfly_median_decode_win_ms"] >= min_decode_win_ms
summary["ok_qat_prefill_rows4"] = (not require_qat_prefill_rows4) or all(
    (row["antfly_q8_1_prefill_linear_rows4"] > 0 or row["antfly_q8_1_prefill_linear_rows8_c4"] > 0)
    and row["antfly_q8_1_prefill_qkv_rows4"] > 0
    and (
        row["antfly_q8_1_prefill_pair_rows4"] > 0
        or row["antfly_q8_1_prefill_pair_rows8_c2"] > 0
        or row["antfly_q8_1_prefill_pair_rows16_c1"] > 0
    )
    and (row["antfly_q8_1_prefill_gated_down_rows4"] > 0 or row["antfly_q8_1_prefill_gated_down_rows8_c4"] > 0)
    for row in rows
)
summary["ok_qat_prefill_rows16_c1"] = (not require_qat_prefill_rows16_c1) or all(
    row["antfly_q8_1_prefill_pair_rows16_c1"] > 0
    for row in rows
)
summary["ok_qat_prefill_linear_rows8_c4"] = (not require_qat_prefill_linear_rows8_c4) or all(
    row["antfly_q8_1_prefill_linear_rows8_c4"] > 0
    and row["antfly_q8_1_prefill_gated_down_rows8_c4"] > 0
    for row in rows
)
summary["ok"] = summary["ok_total"] and summary["ok_prefill"] and summary["ok_decode"] and summary["ok_qat_prefill_rows4"] and summary["ok_qat_prefill_rows16_c1"] and summary["ok_qat_prefill_linear_rows8_c4"]

(out_dir / "paired_summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
with (out_dir / "paired_summary.tsv").open("w", encoding="utf-8") as f:
    fields = [
        "sample",
        "antfly_total_ms",
        "antfly_prefill_ms",
        "antfly_decode_ms",
        "antfly_tok_s",
        "antfly_replays",
        "antfly_discards",
        "antfly_gated_down_precompute",
        "antfly_gated_down_tile4",
        "antfly_q8_1_prefill_linear",
        "antfly_q8_1_prefill_linear_rows4",
        "antfly_q8_1_prefill_linear_rows8_c4",
        "antfly_q8_1_prefill_linear_generic_rows",
        "antfly_q8_1_prefill_qkv",
        "antfly_q8_1_prefill_qkv_rows4",
        "antfly_q8_1_prefill_pair",
        "antfly_q8_1_prefill_pair_rows4",
        "antfly_q8_1_prefill_pair_rows8_c2",
        "antfly_q8_1_prefill_pair_rows16_c1",
        "antfly_q8_1_prefill_gated_down",
        "antfly_q8_1_prefill_gated_down_rows4",
        "antfly_q8_1_prefill_gated_down_rows8_c4",
        "antfly_gqa_prefill_fast_hits",
        "antfly_prefill_profile_events",
        "antfly_prefill_profile_q4_linear_us",
        "antfly_prefill_profile_q4_qkv_us",
        "antfly_prefill_profile_q4_pair_us",
        "antfly_prefill_profile_q4_gated_down_us",
        "antfly_prefill_profile_attention_us",
        "antfly_prefill_profile_ple_dense_us",
        "antfly_bf16_cublaslt_activation_staging",
        "antfly_bf16_cublaslt_activation_mirror",
        "antfly_rms_norm_bf16_mirror_hits",
        "llama_total_ms",
        "llama_sampling_ms",
        "llama_prompt_eval_ms",
        "llama_prompt_eval_runs",
        "llama_prompt_eval_tok_s",
        "llama_eval_ms",
        "llama_eval_runs",
        "llama_eval_tok_s",
        "llama_graphs_reused",
        "antfly_win_ms",
        "antfly_prefill_win_ms",
        "antfly_decode_win_ms",
        "antfly_decode_plus_sampling_win_ms",
    ]
    f.write("\t".join(fields) + "\n")
    for row in rows:
        f.write("\t".join(str(row[field]) for field in fields) + "\n")

print(
    "paired_benchmark "
    f"antfly_median_ms={summary['antfly_total_ms']['median']:.2f} "
    f"llama_median_ms={summary['llama_total_ms']['median']:.2f} "
    f"median_win_ms={summary['antfly_median_win_ms']:.2f} "
    f"prefill_win_ms={summary['antfly_median_prefill_win_ms']:.2f} "
    f"decode_win_ms={summary['antfly_median_decode_win_ms']:.2f} "
    f"decode_plus_sampling_win_ms={summary['antfly_median_decode_plus_sampling_win_ms']:.2f} "
    f"out_dir={out_dir}"
)
if not summary["ok"]:
    if not summary["ok_total"]:
        print(
            f"Antfly median total win {summary['antfly_median_win_ms']:.2f} ms "
            f"is below required {min_win_ms:.2f} ms",
            file=sys.stderr,
        )
    if not summary["ok_prefill"]:
        print(
            f"Antfly median prefill win {summary['antfly_median_prefill_win_ms']:.2f} ms "
            f"is below required {min_prefill_win_ms:.2f} ms",
            file=sys.stderr,
        )
    if not summary["ok_decode"]:
        print(
            f"Antfly median decode win {summary['antfly_median_decode_win_ms']:.2f} ms "
            f"is below required {min_decode_win_ms:.2f} ms",
            file=sys.stderr,
        )
    if not summary["ok_qat_prefill_rows4"]:
        print(
            "QAT rows4 prefill gate failed: expected nonzero linear_rows4 or linear_rows8_c4, qkv_rows4, "
            "pair_rows4, pair_rows8_c2, or pair_rows16_c1, and gated_down_rows4 or gated_down_rows8_c4 counters in every sample",
            file=sys.stderr,
        )
    if not summary["ok_qat_prefill_rows16_c1"]:
        print(
            "QAT rows16/c1 prefill gate failed: expected nonzero pair_rows16_c1 counters in every sample",
            file=sys.stderr,
        )
    if not summary["ok_qat_prefill_linear_rows8_c4"]:
        print(
            "QAT rows8/c4 prefill gate failed: expected nonzero linear_rows8_c4 and gated_down_rows8_c4 counters in every sample",
            file=sys.stderr,
        )
    raise SystemExit(1)
PY
