#!/usr/bin/env bash
# Copyright 2026 Antfly, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../inference_cli.sh"
ROOT_OUT_DIR="${OUT_DIR:-/tmp/antfly-inference-gemma4-e4b-qat-compare-$(date -u +%Y%m%d-%H%M%S)}"
DEFAULT_MODELS_DIR="${ANTFLY_INFERENCE_GEMMA4_MODELS_DIR:-$HOME/.antfly/inference/models}"
LEGACY_MODELS_DIR="${ANTFLY_INFERENCE_GEMMA4_LEGACY_MODELS_DIR:-/private/tmp/antfly-inference-models}"
RUNS="${ANTFLY_INFERENCE_GEMMA4_COMPARE_RUNS:-3}"
TOKENS="${ANTFLY_INFERENCE_GEMMA4_COMPARE_TOKENS:-256}"
MIN_SPEEDUP="${ANTFLY_INFERENCE_GEMMA4_COMPARE_MIN_SPEEDUP:-1.10}"
RUN_LLAMA_BENCH="${ANTFLY_INFERENCE_GEMMA4_COMPARE_LLAMA_BENCH:-auto}"
LLAMA_BENCH_BIN="${ANTFLY_INFERENCE_GEMMA4_LLAMA_BENCH_BIN:-llama-bench}"
LLAMA_PROMPT_TOKENS="${ANTFLY_INFERENCE_GEMMA4_LLAMA_PROMPT_TOKENS:-37}"
LLAMA_MIN_SPEEDUP="${ANTFLY_INFERENCE_GEMMA4_LLAMA_MIN_SPEEDUP:-1.0}"
RUN_LLAMA_COMPLETION="${ANTFLY_INFERENCE_GEMMA4_COMPARE_LLAMA_COMPLETION:-0}"
LLAMA_COMPLETION_BIN="${ANTFLY_INFERENCE_GEMMA4_LLAMA_COMPLETION_BIN:-llama-completion}"
LLAMA_COMPLETION_MIN_SPEEDUP="${ANTFLY_INFERENCE_GEMMA4_LLAMA_COMPLETION_MIN_SPEEDUP:-1.0}"
LLAMA_COMPLETION_MIN_EVAL_RUNS="${ANTFLY_INFERENCE_GEMMA4_LLAMA_COMPLETION_MIN_EVAL_RUNS:-}"
RUN_MLX="${ANTFLY_INFERENCE_GEMMA4_COMPARE_MLX:-0}"
MLX_PYTHON="${ANTFLY_INFERENCE_GEMMA4_MLX_PYTHON:-python3}"
MLX_MODEL="${ANTFLY_INFERENCE_GEMMA4_MLX_MODEL:-mlx-community/gemma-4-e4b-it-4bit}"
MLX_PROMPT="${ANTFLY_INFERENCE_GEMMA4_MLX_PROMPT:-Write one short paragraph about local inference.}"
MLX_MIN_SPEEDUP="${ANTFLY_INFERENCE_GEMMA4_MLX_MIN_SPEEDUP:-0}"
PROMPT="${ANTFLY_INFERENCE_GEMMA4_COMPARE_PROMPT:-Continue this sequence for many words: local inference benchmark continues, local inference benchmark continues,}"
RUN_QAT_ORACLE="${ANTFLY_INFERENCE_GEMMA4_QAT_ORACLE:-1}"
RUN_Q4K="${ANTFLY_INFERENCE_GEMMA4_COMPARE_Q4K:-1}"
RUN_LLAMA_FIRST="${ANTFLY_INFERENCE_GEMMA4_COMPARE_LLAMA_FIRST:-0}"
PHASE_COOLDOWN_SECONDS="${ANTFLY_INFERENCE_GEMMA4_COMPARE_PHASE_COOLDOWN_SECONDS:-0}"
QUIET_WAIT_SECONDS="${ANTFLY_INFERENCE_GEMMA4_COMPARE_QUIET_WAIT_SECONDS:-0}"
QUIET_MAX_CPU="${ANTFLY_INFERENCE_GEMMA4_COMPARE_QUIET_MAX_CPU:-20}"
QUIET_PROCESS_PATTERN="${ANTFLY_INFERENCE_GEMMA4_COMPARE_QUIET_PROCESS_PATTERN:-syspolicyd|trustd|Xprotect|antfly-quant-kernel-metal-runtime-check|antfly-inference|/git$|zig|duetexpertd|airportd|mds|mdworker|mds_stores|fseventsd|deleted}"
ORACLE_PROMPT="${ANTFLY_INFERENCE_GEMMA4_QAT_ORACLE_PROMPT:-Write one short paragraph about local inference.}"
ORACLE_TOKENS="${ANTFLY_INFERENCE_GEMMA4_QAT_ORACLE_TOKENS:-8}"
ORACLE_RENDERED_PROMPT="${ANTFLY_INFERENCE_GEMMA4_QAT_ORACLE_RENDERED_PROMPT:-}"
if [[ -z "$ORACLE_RENDERED_PROMPT" ]]; then
  printf -v ORACLE_RENDERED_PROMPT '<|turn>user\n%s<turn|>\n<|turn>model\n<|channel>thought\n<channel|>' "$ORACLE_PROMPT"
fi
ORACLE_EXPECTED="${ANTFLY_INFERENCE_GEMMA4_QAT_ORACLE_EXPECTED:-}"
if [[ -z "$ORACLE_EXPECTED" ]]; then
  ORACLE_EXPECTED="Here's a thinking"
fi
SHORT_COMPARE="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT:-0}"
# Policy-only mode: run ONLY the MTP identity gates (plain-target golden vs
# auto/force/force-k1 token ids) and exit. Fast hard-gate for speculative-decode
# changes; skips the llama oracle, benchmarks, and variant runs.
POLICY_ONLY_COMPARE="${ANTFLY_INFERENCE_GEMMA4_COMPARE_POLICY_ONLY:-0}"
SHORT_TOKENS="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_TOKENS:-32}"
SHORT_PROMPT="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_PROMPT:-$ORACLE_RENDERED_PROMPT}"
SHORT_EXPECTED_TOKEN_IDS="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_EXPECTED_TOKEN_IDS-}"
SHORT_EXPECTED_TOKEN_IDS_PREFIX="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_EXPECTED_TOKEN_IDS_PREFIX-}"
if [[ -z "${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_EXPECTED_TOKEN_IDS+x}" && "$SHORT_TOKENS" == "32" && "$SHORT_PROMPT" == "$ORACLE_RENDERED_PROMPT" ]]; then
  SHORT_EXPECTED_TOKEN_IDS="8291 236789 236751 496 6972 1657 600 9025 531 506 10340 15649 236787 108 236770 236761 138 1018 115863 506 16499 53121 669 2430 8150 623 811 2822 15649 1003 2263 34711"
fi
if [[ -z "${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_EXPECTED_TOKEN_IDS_PREFIX+x}" && "$SHORT_TOKENS" =~ ^[0-9]+$ && "$SHORT_TOKENS" -ge 32 && "$SHORT_PROMPT" == "$ORACLE_RENDERED_PROMPT" ]]; then
  SHORT_EXPECTED_TOKEN_IDS_PREFIX="8291 236789 236751 496 6972 1657 600 9025 531 506 10340 15649 236787 108 236770 236761 138 1018 115863 506 16499 53121 669 2430 8150 623 811 2822 15649 1003 2263 34711"
fi
SHORT_MIN_DECODE_SPEEDUP="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_MIN_DECODE_SPEEDUP:-1.0}"
SHORT_MIN_GENERATE_SPEEDUP="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_MIN_GENERATE_SPEEDUP:-1.0}"
SHORT_MIN_TOTAL_WITH_LOAD_SPEEDUP="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_MIN_TOTAL_WITH_LOAD_SPEEDUP:-0}"
SHORT_DISABLE_PLANNED_COMPUTE_BARRIERS="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_DISABLE_PLANNED_COMPUTE_BARRIERS-}"
SHORT_LLAMA_FIRST="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SHORT_LLAMA_FIRST:-1}"
DRAFT_MODEL="${ANTFLY_INFERENCE_GEMMA4_COMPARE_DRAFT_MODEL:-${ANTFLY_INFERENCE_GEMMA4_DRAFT_MODEL:-${ANTFLY_INFERENCE_GEMMA4_E4B_DRAFT_MODEL:-}}}"
MTP_POLICY_DRAFT_MODEL="${ANTFLY_INFERENCE_GEMMA4_MTP_POLICY_DRAFT_MODEL:-$DRAFT_MODEL}"
RUN_MTP_POLICY_CHECK="${ANTFLY_INFERENCE_GEMMA4_QAT_MTP_POLICY_CHECK:-1}"
MIN_HOT_DECODE_TOK_S="${ANTFLY_INFERENCE_GEMMA4_MIN_HOT_DECODE_TOK_S:-1}"
MIN_QAT_PLE_ACTIVATION_RHS_F16="${ANTFLY_INFERENCE_GEMMA4_COMPARE_MIN_QAT_PLE_ACTIVATION_RHS_F16:-${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_PLE_ACTIVATION_RHS_REDUCE_OUT_F16:-1}}"
MIN_QAT_PLE_LINEAR_F16="${ANTFLY_INFERENCE_GEMMA4_COMPARE_MIN_QAT_PLE_LINEAR_F16:-${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_PLE_LINEAR_REDUCE_IN_F16:-1}}"
HOST_LOAD_SNAPSHOTS="${ANTFLY_INFERENCE_GEMMA4_COMPARE_HOST_LOAD_SNAPSHOTS:-1}"
if [[ -z "$LLAMA_COMPLETION_MIN_EVAL_RUNS" ]]; then
  if [[ "$TOKENS" =~ ^[0-9]+$ && "$TOKENS" -gt 1 ]]; then
    LLAMA_COMPLETION_MIN_EVAL_RUNS=$((TOKENS - 1))
  else
    LLAMA_COMPLETION_MIN_EVAL_RUNS=0
  fi
fi

case "${ANTFLY_INFERENCE_GEMMA4_COMPARE_CAFFEINATE:-1}:${ANTFLY_INFERENCE_GEMMA4_COMPARE_IN_CAFFEINATE:-0}" in
  0:*|false:*|FALSE:*|no:*|NO:*|off:*|OFF:*|*:1) ;;
  *)
    if command -v caffeinate >/dev/null 2>&1; then
      exec caffeinate -ims env ANTFLY_INFERENCE_GEMMA4_COMPARE_IN_CAFFEINATE=1 "$0" "$@"
    fi
    ;;
esac

mkdir -p "$ROOT_OUT_DIR"

flag_enabled() {
  case "${1:-}" in
    ""|0|false|FALSE|no|NO|off|OFF) return 1 ;;
    *) return 0 ;;
  esac
}

if flag_enabled "$RUN_LLAMA_COMPLETION" && [[ -z "${ANTFLY_INFERENCE_GEMMA4_RAW_PROMPT+x}" ]]; then
  export ANTFLY_INFERENCE_GEMMA4_RAW_PROMPT=1
fi

capture_host_load() {
  if [[ "$HOST_LOAD_SNAPSHOTS" == "0" ]]; then
    return
  fi
  local label="$1"
  {
    date -u '+timestamp_utc=%Y-%m-%dT%H:%M:%SZ'
    ps -axo pid,pcpu,pmem,comm -r | head -20
  } >"$ROOT_OUT_DIR/host-load-$label.txt" 2>&1 || true
}

phase_cooldown() {
  local label="$1"
  if [[ "$PHASE_COOLDOWN_SECONDS" =~ ^[0-9]+$ && "$PHASE_COOLDOWN_SECONDS" -gt 0 ]]; then
    sleep "$PHASE_COOLDOWN_SECONDS"
    capture_host_load "after-cooldown-$label"
  fi
}

wait_for_quiet() {
  local label="$1"
  if ! [[ "$QUIET_WAIT_SECONDS" =~ ^[0-9]+$ && "$QUIET_WAIT_SECONDS" -gt 0 ]]; then
    return
  fi
  local deadline=$((SECONDS + QUIET_WAIT_SECONDS))
  while true; do
    if ps -axo pcpu,comm -r | awk -v max_cpu="$QUIET_MAX_CPU" -v pattern="$QUIET_PROCESS_PATTERN" '
      NR > 1 && ($1 + 0) > max_cpu && $0 ~ pattern { bad = 1 }
      END { exit bad ? 1 : 0 }
    '; then
      capture_host_load "quiet-$label"
      return
    fi
    if [[ "$SECONDS" -ge "$deadline" ]]; then
      capture_host_load "quiet-timeout-$label"
      return
    fi
    sleep 5
  done
}

resolve_text_gguf() {
  local model="$1"
  if [[ -f "$model" ]]; then
    printf '%s\n' "$model"
    return
  fi
  find "$model" -maxdepth 1 -type f -name '*.gguf' ! -name '*mmproj*' | sort | head -n 1
}

run_qat_oracle() {
  local model="$1"
  if [[ "$RUN_QAT_ORACLE" == "0" ]]; then
    return
  fi
  if ! command -v llama-completion >/dev/null 2>&1; then
    echo "Gemma4 QAT oracle needs llama-completion; set ANTFLY_INFERENCE_GEMMA4_QAT_ORACLE=0 to skip" >&2
    exit 2
  fi
  local gguf
  gguf="$(resolve_text_gguf "$model")"
  if [[ -z "$gguf" ]]; then
    echo "Gemma4 QAT oracle could not find text GGUF under: $model" >&2
    exit 2
  fi
  local out="$ROOT_OUT_DIR/qat-oracle"
  mkdir -p "$out"
  local antfly_out="$out/antfly.txt"
  local llama_out="$out/llama.txt"
  run_antfly_inference generate "$model" "$ORACLE_RENDERED_PROMPT" \
    --backend metal \
    --max-tokens "$ORACLE_TOKENS" \
    --temperature 0 \
    --raw-prompt \
    --print-token-count \
    --print-finish-reason \
    --print-token-ids \
    >"$antfly_out" 2>&1
  llama-completion -m "$gguf" \
    --no-conversation \
    --no-jinja \
    --special \
    -p "$ORACLE_RENDERED_PROMPT" \
    -n "$ORACLE_TOKENS" \
    --temp 0 \
    --no-display-prompt \
    --no-perf \
    >"$llama_out" 2>&1
  if ! grep -Fq "$ORACLE_EXPECTED" "$llama_out"; then
    echo "Gemma4 QAT oracle reference did not contain expected text: $ORACLE_EXPECTED" >&2
    echo "llama output: $llama_out" >&2
    exit 1
  fi
  if ! grep -Fq "$ORACLE_EXPECTED" "$antfly_out"; then
    echo "Gemma4 QAT Metal output failed oracle text check: expected $ORACLE_EXPECTED" >&2
    echo "antfly output: $antfly_out" >&2
    echo "llama output: $llama_out" >&2
    exit 1
  fi
}

token_ids_from_output() {
  awk '/^token_ids:/ { sub(/^token_ids:[[:space:]]*/, ""); print; exit }' "$1"
}

run_mtp_policy_check() {
  local model="$1"
  local draft="$2"
  if [[ "$RUN_MTP_POLICY_CHECK" == "0" || -z "$draft" || ! -e "$draft" ]]; then
    return
  fi
  local out="$ROOT_OUT_DIR/qat-mtp-policy"
  mkdir -p "$out"
  local policy_tokens="${ANTFLY_INFERENCE_GEMMA4_MTP_POLICY_TOKENS:-32}"
  local target_out="$out/target.txt"
  local auto_out="$out/auto-positive.txt"
  local uncalibrated_out="$out/auto-uncalibrated.txt"
  local force_out="$out/force.txt"
  local force_k1_out="$out/force-k1.txt"
  run_antfly_inference generate "$model" "$ORACLE_RENDERED_PROMPT" \
    --backend metal \
    --max-tokens "$policy_tokens" \
    --temperature 0 \
    --raw-prompt \
    --print-token-count \
    --print-finish-reason \
    --print-token-ids \
    >"$target_out" 2>&1
  ANTFLY_GEMMA4_MTP_AUTO_MIN_TOKENS=1 \
  ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO=1 \
  ANTFLY_GEMMA4_MTP_PROFILE=1 \
  run_antfly_inference generate "$model" "$ORACLE_RENDERED_PROMPT" \
    --backend metal \
    --draft-model "$draft" \
    --speculative-k "${ANTFLY_INFERENCE_GEMMA4_COMPARE_SPECULATIVE_K:-${ANTFLY_INFERENCE_GEMMA4_SPECULATIVE_K:-2}}" \
    --speculation-policy auto \
    --speculation-calibration positive \
    --max-tokens "$policy_tokens" \
    --temperature 0 \
    --raw-prompt \
    --print-token-count \
    --print-finish-reason \
    --print-token-ids \
    --print-timing \
    >"$auto_out" 2>&1
  local target_ids auto_ids
  target_ids="$(token_ids_from_output "$target_out")"
  auto_ids="$(token_ids_from_output "$auto_out")"
  if [[ -z "$target_ids" || -z "$auto_ids" || "$target_ids" != "$auto_ids" ]]; then
    echo "Gemma4 QAT calibrated MTP auto policy changed token IDs" >&2
    echo "target: ${target_ids:-<missing>} ($target_out)" >&2
    echo "auto:   ${auto_ids:-<missing>} ($auto_out)" >&2
    exit 1
  fi
  if ! grep -Eq '^speculative: .*decision=(active|disabled_slow|disabled_low_acceptance|disabled_zero_match|disabled_insufficient_probe).*rounds=[1-9][0-9]*.*drafted=[1-9][0-9]*.*mtp_enabled=true' "$auto_out"; then
    echo "Gemma4 QAT calibrated Metal MTP auto did not execute: $auto_out" >&2
    exit 1
  fi
  ANTFLY_GEMMA4_MTP_AUTO_MIN_TOKENS=1 \
  ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO=1 \
  ANTFLY_GEMMA4_MTP_PROFILE=1 \
  run_antfly_inference generate "$model" "$ORACLE_RENDERED_PROMPT" \
    --backend metal \
    --draft-model "$draft" \
    --speculative-k "${ANTFLY_INFERENCE_GEMMA4_COMPARE_SPECULATIVE_K:-${ANTFLY_INFERENCE_GEMMA4_SPECULATIVE_K:-2}}" \
    --speculation-policy auto \
    --speculation-calibration none \
    --max-tokens "$policy_tokens" \
    --temperature 0 \
    --raw-prompt \
    --print-token-count \
    --print-finish-reason \
    --print-token-ids \
    --print-timing \
    >"$uncalibrated_out" 2>&1
  local uncalibrated_ids
  uncalibrated_ids="$(token_ids_from_output "$uncalibrated_out")"
  if [[ -z "$uncalibrated_ids" || "$target_ids" != "$uncalibrated_ids" ]]; then
    echo "Gemma4 QAT uncalibrated MTP auto policy changed token IDs" >&2
    echo "target:       ${target_ids:-<missing>} ($target_out)" >&2
    echo "uncalibrated: ${uncalibrated_ids:-<missing>} ($uncalibrated_out)" >&2
    exit 1
  fi
  if ! grep -Eq '^speculative: .*decision=disabled_uncalibrated.*mtp_enabled=false' "$uncalibrated_out"; then
    echo "Gemma4 QAT uncalibrated Metal MTP auto did not stay off: $uncalibrated_out" >&2
    exit 1
  fi
  ANTFLY_GEMMA4_MTP_PROFILE=1 \
  run_antfly_inference generate "$model" "$ORACLE_RENDERED_PROMPT" \
    --backend metal \
    --draft-model "$draft" \
    --speculative-k "${ANTFLY_INFERENCE_GEMMA4_COMPARE_SPECULATIVE_K:-${ANTFLY_INFERENCE_GEMMA4_SPECULATIVE_K:-2}}" \
    --speculation-policy force \
    --max-tokens "$policy_tokens" \
    --temperature 0 \
    --raw-prompt \
    --print-token-count \
    --print-finish-reason \
    --print-token-ids \
    --print-timing \
    >"$force_out" 2>&1
  local force_ids
  force_ids="$(token_ids_from_output "$force_out")"
  if [[ -z "$force_ids" || "$target_ids" != "$force_ids" ]]; then
    echo "Gemma4 QAT forced MTP changed token IDs" >&2
    echo "target: ${target_ids:-<missing>} ($target_out)" >&2
    echo "force:  ${force_ids:-<missing>} ($force_out)" >&2
    exit 1
  fi
  if ! grep -Eq '^speculative: .*mtp_enabled=true' "$force_out"; then
    echo "Gemma4 QAT forced MTP did not enable MTP: $force_out" >&2
    exit 1
  fi
  ANTFLY_GEMMA4_MTP_PROFILE=1 \
  run_antfly_inference generate "$model" "$ORACLE_RENDERED_PROMPT" \
    --backend metal \
    --draft-model "$draft" \
    --speculative-k 1 \
    --speculation-policy force \
    --max-tokens "$policy_tokens" \
    --temperature 0 \
    --raw-prompt \
    --print-token-count \
    --print-finish-reason \
    --print-token-ids \
    --print-timing \
    >"$force_k1_out" 2>&1
  local force_k1_ids
  force_k1_ids="$(token_ids_from_output "$force_k1_out")"
  if [[ -z "$force_k1_ids" || "$target_ids" != "$force_k1_ids" ]]; then
    echo "Gemma4 QAT forced MTP k=1 changed token IDs" >&2
    echo "target:   ${target_ids:-<missing>} ($target_out)" >&2
    echo "force k1: ${force_k1_ids:-<missing>} ($force_k1_out)" >&2
    exit 1
  fi
  if ! grep -Eq '^speculative: .*mtp_enabled=true' "$force_k1_out"; then
    echo "Gemma4 QAT forced MTP k=1 did not enable MTP: $force_k1_out" >&2
    exit 1
  fi
}

run_variant() {
  local variant="$1"
  local model="$2"
  local out="$3"
  local q4_pair_gate=0
  local q6_gate=0
  local q4_0_ple_activation_gate=0
  local q4_0_ple_linear_gate=0
  local q4_0_sumsq_enable=0
  local q4_0_sumsq_disable=1
  local q4_0_sumsq_min=0
  local q4_0_sumsq_max=0
  local q4_0_f16_project="${TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_F16_PROJECT:-0}"
  local q4_0_f16_project_experiment="${TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_F16_PROJECT_EXPERIMENT:-$q4_0_f16_project}"
  if [[ "$variant" == "standard" ]]; then
    q4_pair_gate="${ANTFLY_INFERENCE_GEMMA4_MIN_Q4_PAIR_ACT_REDUCE_OUT_F16:-1}"
    q6_gate="${ANTFLY_INFERENCE_GEMMA4_MIN_Q6_REDUCE_IN_F16:-1}"
  elif [[ "$variant" == "qat-q4_0" ]]; then
    q4_0_ple_activation_gate="$MIN_QAT_PLE_ACTIVATION_RHS_F16"
    q4_0_ple_linear_gate="$MIN_QAT_PLE_LINEAR_F16"
  fi
  env \
    ANTFLY_INFERENCE_GEMMA4_E4B_VARIANT="$variant" \
    ANTFLY_INFERENCE_GEMMA4_MODEL="$model" \
    ANTFLY_INFERENCE_GEMMA4_E4B_REQUIRE_FULL_TOKENS=1 \
    ANTFLY_INFERENCE_GEMMA4_BENCH_PROMPT="$PROMPT" \
    ANTFLY_INFERENCE_GEMMA4_BENCH_MAX_TOKENS="$TOKENS" \
    ANTFLY_INFERENCE_GEMMA4_BENCH_WARMUP_TOKENS="${ANTFLY_INFERENCE_GEMMA4_COMPARE_WARMUP_TOKENS:-16}" \
    ANTFLY_INFERENCE_GEMMA4_BENCH_RUNS="$RUNS" \
    ANTFLY_INFERENCE_GEMMA4_REUSE_PROBE="${ANTFLY_INFERENCE_GEMMA4_REUSE_PROBE:-0}" \
    ANTFLY_INFERENCE_GEMMA4_DRAFT_MODEL="" \
    ANTFLY_INFERENCE_GEMMA4_SPECULATIVE_K="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SPECULATIVE_K:-${ANTFLY_INFERENCE_GEMMA4_SPECULATIVE_K:-4}}" \
    ANTFLY_INFERENCE_GEMMA4_SPECULATION_POLICY="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SPECULATION_POLICY:-${ANTFLY_INFERENCE_GEMMA4_SPECULATION_POLICY:-auto}}" \
    ANTFLY_INFERENCE_GEMMA4_SPECULATION_CALIBRATION="${ANTFLY_INFERENCE_GEMMA4_COMPARE_SPECULATION_CALIBRATION:-${ANTFLY_INFERENCE_GEMMA4_SPECULATION_CALIBRATION:-positive}}" \
    ANTFLY_INFERENCE_GEMMA4_REQUIRE_MTP_ENABLED=0 \
    ANTFLY_INFERENCE_GEMMA4_MIN_SPECULATIVE_ROUNDS=0 \
    ANTFLY_INFERENCE_GEMMA4_MIN_SPECULATIVE_DRAFTED=0 \
    ANTFLY_INFERENCE_GEMMA4_MIN_SPECULATIVE_ACCEPTED=0 \
    ANTFLY_INFERENCE_GEMMA4_MIN_SPECULATIVE_MATCHED=0 \
    ANTFLY_INFERENCE_GEMMA4_MIN_Q4_PAIR_ACT_REDUCE_OUT_F16="$q4_pair_gate" \
    ANTFLY_INFERENCE_GEMMA4_MIN_Q6_REDUCE_IN_F16="$q6_gate" \
    ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_PLE_ACTIVATION_RHS_REDUCE_OUT_F16="$q4_0_ple_activation_gate" \
    ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_PLE_LINEAR_REDUCE_IN_F16="$q4_0_ple_linear_gate" \
    ANTFLY_INFERENCE_GEMMA4_MIN_Q4_0_LINEAR_REDUCE_SUMSQ="$q4_0_sumsq_min" \
    ANTFLY_INFERENCE_GEMMA4_MIN_RMS_NORM_ADD_SUMSQ="$q4_0_sumsq_min" \
    ANTFLY_INFERENCE_GEMMA4_MAX_Q4_0_LINEAR_REDUCE_SUMSQ="$q4_0_sumsq_max" \
    ANTFLY_INFERENCE_GEMMA4_MAX_RMS_NORM_ADD_SUMSQ="$q4_0_sumsq_max" \
    ANTFLY_INFERENCE_GEMMA4_MIN_DECODE_TOK_S="${ANTFLY_INFERENCE_GEMMA4_MIN_DECODE_TOK_S:-1}" \
    ANTFLY_INFERENCE_GEMMA4_MIN_HOT_DECODE_TOK_S="$MIN_HOT_DECODE_TOK_S" \
    TERMITE_METAL_DISABLE_Q4_0_LINEAR_RMS_ADD_SUMSQ="$q4_0_sumsq_disable" \
    TERMITE_METAL_ENABLE_Q4_0_LINEAR_RMS_ADD_SUMSQ="$q4_0_sumsq_enable" \
    TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_SUMSQ_EXPERIMENT="$q4_0_sumsq_enable" \
    TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_F16_PROJECT="$q4_0_f16_project" \
    TERMITE_METAL_Q4_0_LINEAR_RMS_ADD_F16_PROJECT_EXPERIMENT="$q4_0_f16_project_experiment" \
    ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO=0 \
    OUT_DIR="$out" \
    "$SCRIPT_DIR/bench_metal_gemma4_e4b.sh"
}

run_llama_bench() {
  local model="$1"
  case "$RUN_LLAMA_BENCH" in
    0|false|FALSE|no|NO|off|OFF) return ;;
    auto)
      command -v "$LLAMA_BENCH_BIN" >/dev/null 2>&1 || return
      ;;
    *)
      if ! command -v "$LLAMA_BENCH_BIN" >/dev/null 2>&1; then
        echo "llama-bench not found: $LLAMA_BENCH_BIN" >&2
        exit 2
      fi
      ;;
  esac
  local gguf
  gguf="$(resolve_text_gguf "$model")"
  if [[ -z "$gguf" ]]; then
    echo "llama-bench could not find text GGUF under: $model" >&2
    exit 2
  fi
  "$LLAMA_BENCH_BIN" -m "$gguf" \
    -p "$LLAMA_PROMPT_TOKENS" \
    -n "$TOKENS" \
    -pg "$LLAMA_PROMPT_TOKENS,$TOKENS" \
    -r "$RUNS" \
    -o json \
	    >"$ROOT_OUT_DIR/llama-bench.json" 2>&1
}

run_llama_completion() {
  local model="$1"
  if ! flag_enabled "$RUN_LLAMA_COMPLETION"; then
    return
  fi
  if ! command -v "$LLAMA_COMPLETION_BIN" >/dev/null 2>&1; then
    echo "llama-completion not found: $LLAMA_COMPLETION_BIN" >&2
    exit 2
  fi
  local gguf
  gguf="$(resolve_text_gguf "$model")"
  if [[ -z "$gguf" ]]; then
    echo "llama-completion could not find text GGUF under: $model" >&2
    exit 2
  fi
  for i in $(seq 1 "$RUNS"); do
    "$LLAMA_COMPLETION_BIN" -m "$gguf" \
      --no-conversation \
      --no-jinja \
      --special \
      -p "$PROMPT" \
      -n "$TOKENS" \
      --temp 0 \
      --no-display-prompt \
      >"$ROOT_OUT_DIR/llama-completion-run-$i.txt" 2>&1
  done
}

run_short_compare() {
  local model="$1"
  if ! command -v "$LLAMA_COMPLETION_BIN" >/dev/null 2>&1; then
    echo "llama-completion not found: $LLAMA_COMPLETION_BIN" >&2
    exit 2
  fi
  local gguf
  gguf="$(resolve_text_gguf "$model")"
  if [[ -z "$gguf" ]]; then
    echo "short compare could not find text GGUF under: $model" >&2
    exit 2
  fi
  local antfly_out="$ROOT_OUT_DIR/short-antfly.txt"
  local antfly_json="$ROOT_OUT_DIR/short-antfly.json"
  local llama_out="$ROOT_OUT_DIR/short-llama.txt"
  run_short_antfly() {
    (
      if [[ -n "$SHORT_DISABLE_PLANNED_COMPUTE_BARRIERS" ]]; then
        export TERMITE_METAL_DISABLE_PLANNED_COMPUTE_BARRIERS="$SHORT_DISABLE_PLANNED_COMPUTE_BARRIERS"
      fi
      run_antfly_inference generate "$model" "$SHORT_PROMPT" \
        --backend metal \
        --max-tokens "$SHORT_TOKENS" \
        --temperature 0 \
        --raw-prompt \
        --print-token-count \
        --print-finish-reason \
        --print-token-ids \
        --print-timing \
        --json-timing "$antfly_json"
    ) >"$antfly_out" 2>&1
  }
  run_short_llama() {
    "$LLAMA_COMPLETION_BIN" -m "$gguf" \
      --no-conversation \
      --no-jinja \
      --special \
      -p "$SHORT_PROMPT" \
      -n "$SHORT_TOKENS" \
      --temp 0 \
      --no-display-prompt \
      >"$llama_out" 2>&1
  }
  local run_order="antfly_first"
  if flag_enabled "$SHORT_LLAMA_FIRST"; then
    run_order="llama_first"
    run_short_llama
    run_short_antfly
  else
    run_short_antfly
    run_short_llama
  fi
  python3 - "$ROOT_OUT_DIR" "$SHORT_TOKENS" "$SHORT_EXPECTED_TOKEN_IDS" "$SHORT_EXPECTED_TOKEN_IDS_PREFIX" "$SHORT_MIN_DECODE_SPEEDUP" "$SHORT_MIN_GENERATE_SPEEDUP" "$SHORT_MIN_TOTAL_WITH_LOAD_SPEEDUP" "$run_order" <<'PY'
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
requested_tokens = int(sys.argv[2])
expected_ids = sys.argv[3].strip()
expected_prefix = sys.argv[4].strip()
min_decode_speedup = float(sys.argv[5])
min_generate_speedup = float(sys.argv[6])
min_total_with_load_speedup = float(sys.argv[7])
run_order = sys.argv[8]
antfly_text = (root / "short-antfly.txt").read_text(encoding="utf-8", errors="replace")
llama_text = (root / "short-llama.txt").read_text(encoding="utf-8", errors="replace")
antfly = json.loads((root / "short-antfly.json").read_text())

ids_match = re.search(r"^token_ids:\s*(.*)$", antfly_text, re.MULTILINE)
actual_ids = ids_match.group(1).strip() if ids_match else ""
actual_id_parts = actual_ids.split() if actual_ids else []
if not actual_id_parts:
    raise SystemExit(f"short compare output missing token IDs: {root / 'short-antfly.txt'}")
if expected_ids and actual_ids != expected_ids:
    raise SystemExit(
        "short compare token IDs changed\n"
        f"expected: {expected_ids}\n"
        f"actual:   {actual_ids or '<missing>'}\n"
        f"antfly output: {root / 'short-antfly.txt'}"
    )
if expected_prefix:
    expected_prefix_parts = expected_prefix.split()
    actual_prefix = " ".join(actual_id_parts[: len(expected_prefix_parts)])
    if actual_prefix != expected_prefix:
        raise SystemExit(
            "short compare token ID prefix changed\n"
            f"expected prefix: {expected_prefix}\n"
            f"actual prefix:   {actual_prefix or '<missing>'}\n"
            f"antfly output:   {root / 'short-antfly.txt'}"
        )
if int(antfly.get("tokens", 0)) != requested_tokens:
    raise SystemExit(f"short compare generated {antfly.get('tokens')} tokens, expected {requested_tokens}")
if antfly.get("finish_reason") != "length":
    raise SystemExit(f"short compare finish_reason={antfly.get('finish_reason')!r}, expected 'length'")
planned_barriers_match = re.search(r"metal_runtime_encoders:.*\bplanned_barriers=(\d+)", antfly_text)
planned_barriers = int(planned_barriers_match.group(1)) if planned_barriers_match else None

eval_match = re.search(
    r"eval time =\s*([0-9.]+) ms /\s*(\d+) runs\s*\([^)]*?([0-9.]+) tokens per second\)",
    llama_text,
)
if not eval_match:
    raise SystemExit(f"llama-completion output missing eval timing: {root / 'short-llama.txt'}")
total_match = re.search(r"total time =\s*([0-9.]+) ms(?:\s*/\s*(\d+) tokens)?", llama_text)
if not total_match:
    raise SystemExit(f"llama-completion output missing total timing: {root / 'short-llama.txt'}")
prompt_match = re.search(r"prompt eval time =\s*([0-9.]+) ms /\s*(\d+) tokens", llama_text)

timing = antfly.get("timing_ms", {})
antfly_decode = float(antfly.get("decode_tok_per_s", 0.0))
antfly_generate_ms = float(timing.get("generate", 0.0))
antfly_total_ms = float(timing.get("total", 0.0))
antfly_prefill_ms = float(timing.get("prefill_inner", 0.0))
antfly_decode_inner_ms = float(timing.get("decode_inner", 0.0))
antfly_total_inner_ms = float(timing.get("total_inner", 0.0))
llama_prompt_eval_ms = float(prompt_match.group(1)) if prompt_match else None
llama_prompt_tokens = int(prompt_match.group(2)) if prompt_match else None
llama_eval_ms = float(eval_match.group(1))
llama_eval_runs = int(eval_match.group(2))
llama_eval_tok_s = float(eval_match.group(3))
llama_total_ms = float(total_match.group(1))
llama_total_tokens = int(total_match.group(2)) if total_match.group(2) else None
llama_min_eval_runs = max(0, requested_tokens - 1)
if llama_eval_runs < llama_min_eval_runs:
    raise SystemExit(f"short compare llama eval runs {llama_eval_runs} below expected {llama_min_eval_runs}")
decode_speedup = antfly_decode / llama_eval_tok_s if llama_eval_tok_s else 0.0
generate_speedup = llama_total_ms / antfly_generate_ms if antfly_generate_ms else 0.0
total_with_load_speedup = llama_total_ms / antfly_total_ms if antfly_total_ms else 0.0
prefill_gap_ms = antfly_prefill_ms - llama_prompt_eval_ms if llama_prompt_eval_ms is not None else None
decode_gap_ms = antfly_decode_inner_ms - llama_eval_ms

summary = {
    "antfly_decode_tok_s": antfly_decode,
    "antfly_prefill_ms": antfly_prefill_ms,
    "antfly_decode_inner_ms": antfly_decode_inner_ms,
    "antfly_total_inner_ms": antfly_total_inner_ms,
    "antfly_generate_ms": antfly_generate_ms,
    "antfly_total_with_load_ms": antfly_total_ms,
    "llama_prompt_eval_ms": llama_prompt_eval_ms,
    "llama_prompt_tokens": llama_prompt_tokens,
    "llama_eval_tok_s": llama_eval_tok_s,
    "llama_eval_ms": llama_eval_ms,
    "llama_eval_runs": llama_eval_runs,
    "llama_min_eval_runs": llama_min_eval_runs,
    "llama_total_ms": llama_total_ms,
    "llama_total_tokens": llama_total_tokens,
    "decode_speedup": decode_speedup,
    "generate_speedup": generate_speedup,
    "total_with_load_speedup": total_with_load_speedup,
    "prefill_gap_ms": prefill_gap_ms,
    "decode_gap_ms": decode_gap_ms,
    "min_decode_speedup": min_decode_speedup,
    "min_generate_speedup": min_generate_speedup,
    "min_total_with_load_speedup": min_total_with_load_speedup,
    "run_order": run_order,
    "token_ids": actual_ids,
    "antfly_planned_barriers": planned_barriers,
    "antfly_output": str(root / "short-antfly.txt"),
    "llama_output": str(root / "short-llama.txt"),
}
(root / "short-compare-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(f"short compare summary: {root / 'short-compare-summary.json'}")
print(
    f"antfly_decode_tok_s={antfly_decode:.3f} llama_eval_tok_s={llama_eval_tok_s:.3f} "
    f"decode_speedup={decode_speedup:.3f} min_decode_speedup={min_decode_speedup:.3f}"
)
print(
    f"antfly_generate_ms={antfly_generate_ms:.1f} llama_total_ms={llama_total_ms:.1f} "
    f"generate_speedup={generate_speedup:.3f} min_generate_speedup={min_generate_speedup:.3f}"
)
if llama_prompt_eval_ms is not None:
    print(
        f"prefill_gap_ms={prefill_gap_ms:.1f} "
        f"decode_gap_ms={decode_gap_ms:.1f}"
    )
print(
    f"antfly_total_with_load_ms={antfly_total_ms:.1f} llama_total_ms={llama_total_ms:.1f} "
    f"total_with_load_speedup={total_with_load_speedup:.3f} min_total_with_load_speedup={min_total_with_load_speedup:.3f}"
)
if min_decode_speedup > 0 and decode_speedup < min_decode_speedup:
    raise SystemExit(f"short decode speedup {decode_speedup:.3f} below gate {min_decode_speedup:.3f}")
if min_generate_speedup > 0 and generate_speedup < min_generate_speedup:
    raise SystemExit(f"short generate speedup {generate_speedup:.3f} below gate {min_generate_speedup:.3f}")
if min_total_with_load_speedup > 0 and total_with_load_speedup < min_total_with_load_speedup:
    raise SystemExit(
        f"short total-with-load speedup {total_with_load_speedup:.3f} below gate {min_total_with_load_speedup:.3f}"
    )
PY
}

run_mlx_generate() {
  if ! flag_enabled "$RUN_MLX"; then
    return
  fi
  if ! "$MLX_PYTHON" -c 'import mlx_vlm' >/dev/null 2>&1; then
    echo "mlx-vlm not available via $MLX_PYTHON; set ANTFLY_INFERENCE_GEMMA4_MLX_PYTHON" >&2
    exit 2
  fi
  for i in $(seq 1 "$RUNS"); do
    "$MLX_PYTHON" -m mlx_vlm.generate \
      --model "$MLX_MODEL" \
      --prompt "$MLX_PROMPT" \
      --max-tokens "$TOKENS" \
      --temperature 0 \
      --verbose \
      >"$ROOT_OUT_DIR/mlx-run-$i.txt" 2>&1
  done
}

QAT_MODEL_DEFAULT="$DEFAULT_MODELS_DIR/google/gemma-4-E4B-it-qat-q4_0-gguf"
Q4K_MODEL_DEFAULT="$DEFAULT_MODELS_DIR/ggml-org/gemma-4-E4B-it-GGUF/gemma-4-E4B-it-Q4_K_M.gguf"
[[ -e "$QAT_MODEL_DEFAULT" ]] || QAT_MODEL_DEFAULT="$LEGACY_MODELS_DIR/google/gemma-4-E4B-it-qat-q4_0-gguf"
[[ -e "$Q4K_MODEL_DEFAULT" ]] || Q4K_MODEL_DEFAULT="$LEGACY_MODELS_DIR/ggml-org/gemma-4-E4B-it-GGUF/gemma-4-E4B-it-Q4_K_M.gguf"
QAT_MODEL="${ANTFLY_INFERENCE_GEMMA4_QAT_MODEL:-$QAT_MODEL_DEFAULT}"
Q4K_MODEL="${ANTFLY_INFERENCE_GEMMA4_Q4K_MODEL:-$Q4K_MODEL_DEFAULT}"
if [[ -z "$MTP_POLICY_DRAFT_MODEL" ]]; then
  candidate="${QAT_MODEL%-gguf}-unquantized-assistant"
  [[ -e "$candidate" ]] && MTP_POLICY_DRAFT_MODEL="$candidate"
fi

if flag_enabled "$SHORT_COMPARE"; then
  run_short_compare "$QAT_MODEL"
  echo "raw output: $ROOT_OUT_DIR"
  exit 0
fi

if flag_enabled "$POLICY_ONLY_COMPARE"; then
  # A hard gate must not pass vacuously: require the draft model and the check.
  if [[ "$RUN_MTP_POLICY_CHECK" == "0" ]]; then
    echo "policy-only mode requires RUN_MTP_POLICY_CHECK enabled" >&2
    exit 2
  fi
  if [[ -z "$MTP_POLICY_DRAFT_MODEL" || ! -e "$MTP_POLICY_DRAFT_MODEL" ]]; then
    echo "policy-only mode requires the MTP draft model (missing: '$MTP_POLICY_DRAFT_MODEL')" >&2
    exit 2
  fi
  run_mtp_policy_check "$QAT_MODEL" "$MTP_POLICY_DRAFT_MODEL"
  echo "mtp policy-only gates passed"
  echo "raw output: $ROOT_OUT_DIR"
  exit 0
fi

capture_host_load before-oracle
run_qat_oracle "$QAT_MODEL"
capture_host_load after-oracle
run_mtp_policy_check "$QAT_MODEL" "$MTP_POLICY_DRAFT_MODEL"
capture_host_load after-mtp-policy
if flag_enabled "$RUN_LLAMA_FIRST"; then
  wait_for_quiet before-llama-bench
  capture_host_load before-llama-bench
  run_llama_bench "$QAT_MODEL"
  run_llama_completion "$QAT_MODEL"
  capture_host_load after-llama-bench
  phase_cooldown llama-bench
fi
wait_for_quiet before-qat-q4_0
capture_host_load before-qat-q4_0
run_variant qat-q4_0 "$QAT_MODEL" "$ROOT_OUT_DIR/qat-q4_0"
capture_host_load after-qat-q4_0
phase_cooldown qat-q4_0
if flag_enabled "$RUN_Q4K"; then
  wait_for_quiet before-q4_k_m
  capture_host_load before-q4_k_m
  run_variant standard "$Q4K_MODEL" "$ROOT_OUT_DIR/q4_k_m"
  capture_host_load after-q4_k_m
  phase_cooldown q4_k_m
fi
if ! flag_enabled "$RUN_LLAMA_FIRST"; then
  wait_for_quiet before-llama-bench
  capture_host_load before-llama-bench
  run_llama_bench "$QAT_MODEL"
  run_llama_completion "$QAT_MODEL"
  capture_host_load after-llama-bench
fi
wait_for_quiet before-mlx
capture_host_load before-mlx
run_mlx_generate
capture_host_load after-mlx

python3 - "$ROOT_OUT_DIR" "$MIN_SPEEDUP" "$LLAMA_MIN_SPEEDUP" "$LLAMA_COMPLETION_MIN_SPEEDUP" "$TOKENS" "$MLX_MIN_SPEEDUP" "$LLAMA_COMPLETION_MIN_EVAL_RUNS" <<'PY'
import json
import re
import sys
from pathlib import Path

root = Path(sys.argv[1])
min_speedup = float(sys.argv[2])
llama_min_speedup = float(sys.argv[3])
llama_completion_min_speedup = float(sys.argv[4])
tokens = int(sys.argv[5])
mlx_min_speedup = float(sys.argv[6])
llama_completion_min_eval_runs = int(sys.argv[7])
qat = json.loads((root / "qat-q4_0" / "summary.json").read_text())
q4k_path = root / "q4_k_m" / "summary.json"
q4k = json.loads(q4k_path.read_text()) if q4k_path.exists() else None
qat_decode = float(qat["median_decode_tok_s"])
qat_hot = float(qat["median_hot_decode_tok_s"])
q4k_decode = float(q4k["median_decode_tok_s"]) if q4k else None
speedup = qat_decode / q4k_decode if q4k_decode else None
qat_rows = qat["rows"]
qat_ple_activation_min = min(int(r.get("q4_0_ple_activation_rhs_reduce_out_f16", 0)) for r in qat_rows)
qat_ple_linear_min = min(int(r.get("q4_0_ple_linear_reduce_in_f16", 0)) for r in qat_rows)

def mtp_policy_summary(path: Path):
    if not path.exists():
        return {}
    text = path.read_text(encoding="utf-8", errors="replace")
    line = next((ln for ln in text.splitlines() if ln.startswith("speculative:")), "")
    return {
        "enabled": "mtp_enabled=true" in line,
        "accepted": int((re.search(r"\baccepted=(\d+)", line) or [None, 0])[1]),
        "drafted": int((re.search(r"\bdrafted=(\d+)", line) or [None, 0])[1]),
        "matched": int((re.search(r"\bmatched=(\d+)", line) or [None, 0])[1]),
    }

def llama_generation_tok_s(path: Path, n_gen: int):
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8", errors="replace")
    start = text.find("[")
    end = text.rfind("]")
    if start < 0 or end < start:
        raise SystemExit(f"llama-bench output did not contain JSON array: {path}")
    rows = json.loads(text[start : end + 1])
    matches = [
        row for row in rows
        if int(row.get("n_prompt", -1)) == 0 and int(row.get("n_gen", -1)) == n_gen
    ]
    if not matches:
        raise SystemExit(f"llama-bench output missing tg row for n_gen={n_gen}: {path}")
    return float(matches[0]["avg_ts"])

def llama_completion_metrics(root: Path):
    values = []
    eval_runs = []
    for path in sorted(root.glob("llama-completion-run-*.txt")):
        text = path.read_text(encoding="utf-8", errors="replace")
        m = re.search(r"eval time =\s+[0-9.]+ ms /\s+(\d+) runs\s+\([^)]*?,\s*([0-9.]+) tokens per second\)", text)
        if not m:
            raise SystemExit(f"llama-completion output missing eval timing: {path}")
        eval_runs.append(int(m.group(1)))
        values.append(float(m.group(2)))
    if not values:
        return None, None, []
    values.sort()
    return values[len(values) // 2], min(eval_runs), eval_runs

def mlx_generation_tok_s(root: Path):
    values = []
    for path in sorted(root.glob("mlx-run-*.txt")):
        text = path.read_text(encoding="utf-8", errors="replace")
        m = re.search(r"Generation:\s+\d+\s+tokens,\s+([0-9.]+)\s+tokens-per-sec", text)
        if not m:
            raise SystemExit(f"MLX output missing generation timing: {path}")
        values.append(float(m.group(1)))
    if not values:
        return None
    values.sort()
    return values[len(values) // 2]

llama_tg = llama_generation_tok_s(root / "llama-bench.json", tokens)
llama_speedup = qat_hot / llama_tg if llama_tg else None
llama_completion_tg, llama_completion_eval_runs_min, llama_completion_eval_runs = llama_completion_metrics(root)
llama_completion_speedup = qat_hot / llama_completion_tg if llama_completion_tg else None
mlx_tg = mlx_generation_tok_s(root)
mlx_speedup = qat_hot / mlx_tg if mlx_tg else None

summary = {
    "qat_median_decode_tok_s": qat_decode,
    "qat_median_hot_decode_tok_s": qat_hot,
    "q4k_median_decode_tok_s": q4k_decode,
    "speedup": speedup,
    "min_speedup": min_speedup,
    "llama_generation_tok_s": llama_tg,
    "llama_speedup": llama_speedup,
    "llama_min_speedup": llama_min_speedup,
    "llama_completion_eval_tok_s": llama_completion_tg,
    "llama_completion_eval_runs": llama_completion_eval_runs,
    "llama_completion_eval_runs_min": llama_completion_eval_runs_min,
    "llama_completion_min_eval_runs": llama_completion_min_eval_runs,
    "llama_completion_speedup": llama_completion_speedup,
    "llama_completion_min_speedup": llama_completion_min_speedup,
    "mlx_generation_tok_s": mlx_tg,
    "mlx_speedup": mlx_speedup,
    "mlx_min_speedup": mlx_min_speedup,
    "qat_ple_activation_rhs_reduce_out_f16_min": qat_ple_activation_min,
    "qat_ple_linear_reduce_in_f16_min": qat_ple_linear_min,
    "mtp_force": mtp_policy_summary(root / "qat-mtp-policy" / "force.txt"),
    "mtp_force_k1": mtp_policy_summary(root / "qat-mtp-policy" / "force-k1.txt"),
    "qat_speculative_decision": qat["rows"][0].get("speculative_decision", ""),
    "q4k_speculative_decision": q4k["rows"][0].get("speculative_decision", "") if q4k else "",
    "qat_runtime_toggles": qat.get("runtime_toggles", {}),
    "q4k_runtime_toggles": q4k.get("runtime_toggles", {}) if q4k else {},
    "host_load_snapshots": [str(path) for path in sorted(root.glob("host-load-*.txt"))],
    "qat_summary": str(root / "qat-q4_0" / "summary.tsv"),
    "q4k_summary": str(root / "q4_k_m" / "summary.tsv") if q4k else "",
}
(root / "compare-summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(f"compare summary: {root / 'compare-summary.json'}")
if speedup is not None:
    print(f"qat_decode_tok_s={qat_decode:.3f} q4k_decode_tok_s={q4k_decode:.3f} speedup={speedup:.3f} min_speedup={min_speedup:.3f}")
    if speedup < min_speedup:
        raise SystemExit(f"QAT speedup {speedup:.3f} below gate {min_speedup:.3f}")
else:
    print(f"qat_decode_tok_s={qat_decode:.3f} q4k_decode_tok_s=skipped speedup=skipped")
if llama_speedup is not None:
    print(f"qat_hot_decode_tok_s={qat_hot:.3f} llama_generation_tok_s={llama_tg:.3f} llama_speedup={llama_speedup:.3f} min_llama_speedup={llama_min_speedup:.3f}")
    if llama_speedup < llama_min_speedup:
        raise SystemExit(f"QAT/llama.cpp speedup {llama_speedup:.3f} below gate {llama_min_speedup:.3f}")
if llama_completion_speedup is not None:
    print(f"qat_hot_decode_tok_s={qat_hot:.3f} llama_completion_eval_tok_s={llama_completion_tg:.3f} llama_completion_eval_runs_min={llama_completion_eval_runs_min} min_llama_completion_eval_runs={llama_completion_min_eval_runs} llama_completion_speedup={llama_completion_speedup:.3f} min_llama_completion_speedup={llama_completion_min_speedup:.3f}")
    if llama_completion_eval_runs_min is not None and llama_completion_eval_runs_min < llama_completion_min_eval_runs:
        raise SystemExit(f"llama-completion eval runs {llama_completion_eval_runs_min} below gate {llama_completion_min_eval_runs}")
    if llama_completion_speedup < llama_completion_min_speedup:
        raise SystemExit(f"QAT/llama-completion speedup {llama_completion_speedup:.3f} below gate {llama_completion_min_speedup:.3f}")
if mlx_speedup is not None:
    print(f"qat_hot_decode_tok_s={qat_hot:.3f} mlx_generation_tok_s={mlx_tg:.3f} mlx_speedup={mlx_speedup:.3f} min_mlx_speedup={mlx_min_speedup:.3f}")
    if mlx_speedup < mlx_min_speedup:
        raise SystemExit(f"QAT/MLX speedup {mlx_speedup:.3f} below gate {mlx_min_speedup:.3f}")
PY

echo "raw output: $ROOT_OUT_DIR"
