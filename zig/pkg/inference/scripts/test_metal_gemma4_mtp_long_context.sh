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

# Exact-token gate for chunked Gemma4 QAT prefill: frame target, live
# whole-model target, and forced Metal MTP must produce identical tokens.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/inference_cli.sh
source "$SCRIPT_DIR/inference_cli.sh"

DEFAULT_MODELS_DIR="${ANTFLY_INFERENCE_GEMMA4_MODELS_DIR:-$HOME/.antfly/inference/models}"
MODEL="${ANTFLY_INFERENCE_GEMMA4_QAT_MODEL:-$DEFAULT_MODELS_DIR/google/gemma-4-E4B-it-qat-q4_0-gguf}"
DRAFT="${ANTFLY_INFERENCE_GEMMA4_MTP_POLICY_DRAFT_MODEL:-${MODEL%-gguf}-unquantized-assistant}"
TOKENS="${ANTFLY_GEMMA4_MTP_LONG_CONTEXT_TOKENS:-64}"
EXPECTED_FINISH_REASON="${ANTFLY_GEMMA4_MTP_LONG_CONTEXT_EXPECT_FINISH_REASON:-length}"
PREFILL_CHUNK_SIZE="${ANTFLY_GEMMA4_MTP_LONG_CONTEXT_PREFILL_CHUNK_SIZE:-auto}"
AUTO_PREFILL_CHUNK_SIZE="${ANTFLY_GEMMA4_MTP_LONG_CONTEXT_AUTO_PREFILL_CHUNK_SIZE:-512}"
SPECULATIVE_K="${ANTFLY_GEMMA4_MTP_LONG_CONTEXT_SPECULATIVE_K:-4}"
# Rebaselined after honoring Gemma4's binary rope_freqs.weight mask. The first
# 16 greedy IDs match llama.cpp b8990 on this exact GGUF and 2,003-token prompt.
EXPECTED_TOKEN_PREFIX="${ANTFLY_GEMMA4_MTP_LONG_CONTEXT_EXPECTED_TOKEN_PREFIX-818 2430 563 10980 573 506 4563 529 623 4225 3192 7681 10091 3056 107 236777 1202 531 2426 506 3847 4914 573 2129 16412 4596 531 623 4225 3192 565 27337 32861 3056 108 71648 236787 107 236770 236761 2165 4225 3192 565 27337 32861 236761 10888 236929 563 528 86072 13465 236786 10696 4763 236744 236786 84682 21233 107 236778 236761 2165}"
# 36 repeats yields a query-bearing 2,003-token prompt with the E4B tokenizer.
# The gate also accepts larger repeats to exercise multi-chunk prefill.
PROMPT_REPEAT="${ANTFLY_GEMMA4_MTP_LONG_CONTEXT_PROMPT_REPEAT:-36}"
PRINT_TIMING="${ANTFLY_GEMMA4_MTP_LONG_CONTEXT_TIMING:-0}"
OUT_DIR="${OUT_DIR:-/tmp/antfly-gemma4-mtp-long-context-$(date -u +%Y%m%d-%H%M%S)}"
ring_gate_active=1
if [[ "${TERMITE_METAL_DISABLE_SPLIT_SWA_KV_RING:-0}" != "0" ]]; then
  ring_gate_active=0
fi
hd512_flash_gate_active=1
if [[ "${TERMITE_METAL_DISABLE_PREFILL_FLASH_HD512:-0}" != "0" ]]; then
  hd512_flash_gate_active=0
fi
local_flash_gate_active=1
if [[ "${TERMITE_METAL_DISABLE_FLASH_PREFILL_GENERATED:-0}" != "0" ]]; then
  local_flash_gate_active=0
fi
trace_kv="${TERMITE_METAL_TRACE_KV_GATHER:-0}"
if (( ring_gate_active )); then
  trace_kv=1
fi

for value in "$TOKENS" "$PROMPT_REPEAT" "$SPECULATIVE_K" "$AUTO_PREFILL_CHUNK_SIZE"; do
  case "$value" in
    ''|*[!0-9]*|0)
      echo "tokens, prompt repeat, and speculative k must be positive integers" >&2
      exit 2
      ;;
  esac
done
auto_prefill=0
if [[ "$PREFILL_CHUNK_SIZE" == "auto" ]]; then
  auto_prefill=1
else
  case "$PREFILL_CHUNK_SIZE" in
    ''|*[!0-9]*|0)
      echo "prefill chunk size must be a positive integer or auto" >&2
      exit 2
      ;;
  esac
fi
if (( SPECULATIVE_K > 16 )); then
  echo "speculative k must not exceed 16" >&2
  exit 2
fi
if [[ "$EXPECTED_FINISH_REASON" != "length" && "$EXPECTED_FINISH_REASON" != "stop" ]]; then
  echo "expected finish reason must be length or stop" >&2
  exit 2
fi

if [[ ! -e "$MODEL" ]]; then
  echo "missing target model: $MODEL" >&2
  exit 2
fi
if [[ ! -e "$DRAFT" ]]; then
  echo "missing MTP draft model: $DRAFT" >&2
  exit 2
fi

PROMPT="${ANTFLY_GEMMA4_MTP_LONG_CONTEXT_PROMPT:-}"
if [[ -z "$PROMPT" ]]; then
  evidence=""
  sentence='You answer questions about indexed files using only evidence. Evidence: Spella Caffe Logo.pdf is in /Users/timkaye/Downloads. Spella Caffe Logo Two Color.pdf is in /Users/timkaye/Downloads. Ignore unrelated source code. '
  for ((i = 0; i < PROMPT_REPEAT; i++)); do
    evidence+="$sentence"
  done
  printf -v PROMPT '<|turn>user\n%s\n\nwhere are my Spella coffee assets?<turn|>\n<|turn>model\n<|channel>thought\n<channel|>' "$evidence"
fi

mkdir -p "$OUT_DIR"
target_out="$OUT_DIR/target.txt"
whole_model_out="$OUT_DIR/whole-model-target.txt"
mtp_out="$OUT_DIR/mtp-k${SPECULATIVE_K}.txt"

common_args=(
  --backend metal
  --max-tokens "$TOKENS"
  --temperature 0
  --raw-prompt
  --print-token-count
  --print-finish-reason
  --print-token-ids
  # The CLI emits speculative execution stats with timing diagnostics. The
  # gate always needs those stats even when it omits timing from its summary.
  --print-timing
)
if (( ! auto_prefill )); then
  common_args+=(--prefill-chunk-size "$PREFILL_CHUNK_SIZE")
fi

TERMITE_GEN_STAGE_DEBUG=1 \
TERMITE_METAL_DISABLE_LIVE_WHOLE_MODEL_EXECUTOR=1 \
TERMITE_METAL_TRACE_KV_GATHER="$trace_kv" \
run_antfly_inference generate "$MODEL" "$PROMPT" \
  "${common_args[@]}" >"$target_out" 2>&1

(
  unset TERMITE_METAL_DISABLE_LIVE_WHOLE_MODEL_EXECUTOR
  TERMITE_GEN_STAGE_DEBUG=1 \
  TERMITE_METAL_TRACE_KV_GATHER="$trace_kv" \
  run_antfly_inference generate "$MODEL" "$PROMPT" \
    "${common_args[@]}"
) >"$whole_model_out" 2>&1

TERMITE_GEN_STAGE_DEBUG=1 \
TERMITE_METAL_DISABLE_LIVE_WHOLE_MODEL_EXECUTOR=1 \
TERMITE_METAL_TRACE_KV_GATHER="$trace_kv" \
run_antfly_inference generate "$MODEL" "$PROMPT" \
  --draft-model "$DRAFT" \
  --speculative-k "$SPECULATIVE_K" \
  --speculation-policy force \
  "${common_args[@]}" >"$mtp_out" 2>&1

fail() {
  echo "FAIL: $*" >&2
  echo "target:      $target_out" >&2
  echo "whole-model: $whole_model_out" >&2
  echo "MTP:         $mtp_out" >&2
  exit 1
}

token_ids_from_output() {
  awk '/^token_ids:/ { sub(/^token_ids:[[:space:]]*/, ""); print; exit }' "$1"
}

prompt_tokens_from_output() {
  sed -n 's/^gen_debug: encoded prompt .*actual_prompt_tokens=\([0-9][0-9]*\)$/\1/p' "$1" | head -n 1
}

chunk_size_from_output() {
  sed -n 's/^gen_debug: executePrefill chunk start .*current_chunk_size=\([0-9][0-9]*\)$/\1/p' "$1" | head -n 1
}

target_ids="$(token_ids_from_output "$target_out")"
whole_model_ids="$(token_ids_from_output "$whole_model_out")"
mtp_ids="$(token_ids_from_output "$mtp_out")"
[[ -n "$target_ids" ]] || fail "target run did not print token IDs"
[[ -n "$whole_model_ids" ]] || fail "whole-model target run did not print token IDs"
[[ -n "$mtp_ids" ]] || fail "MTP run did not print token IDs"
if [[ -n "$EXPECTED_TOKEN_PREFIX" ]]; then
  expected_count=$((TOKENS < 64 ? TOKENS : 64))
  read -r -a expected_oracle_ids <<<"$EXPECTED_TOKEN_PREFIX"
  read -r -a target_oracle_ids <<<"$target_ids"
  (( ${#expected_oracle_ids[@]} >= expected_count )) \
    || fail "expected token oracle has fewer than $expected_count IDs"
  [[ "${target_oracle_ids[*]:0:expected_count}" == "${expected_oracle_ids[*]:0:expected_count}" ]] \
    || fail "target's first $expected_count token IDs changed"
fi
[[ "$target_ids" == "$whole_model_ids" ]] || fail "live whole-model target changed greedy token IDs"
[[ "$target_ids" == "$mtp_ids" ]] || fail "forced MTP changed greedy token IDs"

grep -q '^generate-setup: live whole-model executor handled request$' "$whole_model_out" \
  || fail "live whole-model executor did not handle the target run"
if grep -q '^generate-setup: live whole-model executor skipped$' "$whole_model_out"; then
  fail "live whole-model executor silently fell back"
fi
grep -Eq '^metal_executor_ms: .*prefill_calls=[1-9][0-9]* ' "$whole_model_out" \
  || fail "live whole-model executor did not report prefill work"
grep -Eq '^metal_executor_ms2: .*ensure_prepared_calls=[1-9][0-9]* ' "$whole_model_out" \
  || fail "live whole-model executor did not prepare its decoder runtime"

if (( ring_gate_active )); then
  if grep -Eq 'kv-gather-fallback|^kv-read:|RingKvRequiresPagedAttention|KvRingAttentionUnavailable' "$target_out" "$mtp_out"; then
    fail "split SWA ring attempted a full-history KV fallback"
  fi
  for ring_out in "$target_out" "$mtp_out"; do
    grep -Eq '^kv-(write|reserve): .*layer=0 .*ring_pages=[1-9][0-9]*$' "$ring_out" \
      || fail "sliding layer did not use the SWA ring in $ring_out"
    grep -Eq '^kv-(write|reserve): .*layer=5 .*ring_pages=0$' "$ring_out" \
      || fail "global layer did not retain full-history KV in $ring_out"
  done
fi
if (( hd512_flash_gate_active )); then
  for flash_out in "$target_out" "$mtp_out"; do
    grep -Eq '^metal_attention_dispatch: .*generated_flash_prefill_hd512=[1-9][0-9]*' "$flash_out" \
      || fail "512-d global attention did not use the default flash-prefill route in $flash_out"
  done
fi
if (( local_flash_gate_active )); then
  for flash_out in "$target_out" "$mtp_out"; do
    grep -Eq '^metal_attention_dispatch: .*generated_flash_prefill=[1-9][0-9]*' "$flash_out" \
      || fail "256-d local attention did not use the default flash-prefill route in $flash_out"
  done
fi
for frame_out in "$target_out" "$mtp_out"; do
  grep -Eq '^metal_frame_fallbacks: .*prefill_execute=[1-9][0-9]*/[1-9][0-9]*' "$frame_out" \
    || fail "Gemma4 final-hidden prefill frame did not execute in $frame_out"
done

target_prompt_tokens="$(prompt_tokens_from_output "$target_out")"
mtp_prompt_tokens="$(prompt_tokens_from_output "$mtp_out")"
[[ -n "$target_prompt_tokens" && "$target_prompt_tokens" == "$mtp_prompt_tokens" ]] \
  || fail "frame target and MTP prompt token counts differ"
target_chunk="$(chunk_size_from_output "$target_out")"
mtp_chunk="$(chunk_size_from_output "$mtp_out")"
if (( auto_prefill )); then
  expected_chunk=$((target_prompt_tokens < AUTO_PREFILL_CHUNK_SIZE ? target_prompt_tokens : AUTO_PREFILL_CHUNK_SIZE))
  [[ "$target_chunk" == "$expected_chunk" && "$mtp_chunk" == "$expected_chunk" ]] \
    || fail "target and MTP did not use auto prefill chunk size $expected_chunk"
else
  (( target_prompt_tokens > PREFILL_CHUNK_SIZE )) \
    || fail "prompt is not long enough to exercise chunked prefill"
  [[ "$target_chunk" == "$PREFILL_CHUNK_SIZE" && "$mtp_chunk" == "$PREFILL_CHUNK_SIZE" ]] \
    || fail "target and MTP did not use requested prefill chunk size $PREFILL_CHUNK_SIZE"
fi

if [[ "$EXPECTED_FINISH_REASON" == "length" ]]; then
  grep -Eq "^finish_reason=length tokens=$TOKENS$" "$target_out" \
    || fail "target did not generate exactly $TOKENS tokens"
  grep -Eq "^finish_reason=length tokens=$TOKENS$" "$whole_model_out" \
    || fail "whole-model target did not generate exactly $TOKENS tokens"
  grep -Eq "^finish_reason=length tokens=$TOKENS$" "$mtp_out" \
    || fail "MTP did not generate exactly $TOKENS tokens"
  generated_tokens="$TOKENS"
else
  target_generated="$(sed -n 's/^finish_reason=stop tokens=\([0-9][0-9]*\)$/\1/p' "$target_out" | tail -n 1)"
  whole_model_generated="$(sed -n 's/^finish_reason=stop tokens=\([0-9][0-9]*\)$/\1/p' "$whole_model_out" | tail -n 1)"
  mtp_generated="$(sed -n 's/^finish_reason=stop tokens=\([0-9][0-9]*\)$/\1/p' "$mtp_out" | tail -n 1)"
  [[ -n "$target_generated" && "$target_generated" == "$whole_model_generated" && "$target_generated" == "$mtp_generated" ]] \
    || fail "target, whole-model, and MTP stop token counts differ"
  (( target_generated > 0 && target_generated < TOKENS )) \
    || fail "expected a natural stop before the $TOKENS-token budget"
  generated_tokens="$target_generated"
fi
grep -Eq '^speculative: .*rounds=[1-9][0-9]* .*drafted=[1-9][0-9]* .*matched=[1-9][0-9]* .*mtp_enabled=true' "$mtp_out" \
  || fail "forced MTP did not execute accepted speculative work"
if (( target_chunk == target_prompt_tokens )); then
  grep -q '^gen_debug: executePrefill metal_prepared_tail_greedy ' "$mtp_out" \
    || fail "whole-prefill MTP did not use the prepared tail"
else
  grep -q '^gen_debug: executePrefill captured target final-row logits ' "$target_out" \
    || fail "chunked target prefill did not use the final-row logits path"
  grep -q '^gen_debug: executePrefill captured final logits and MTP hidden ' "$mtp_out" \
    || fail "chunked MTP prefill did not capture final logits and hidden state"
  if grep -q '^gen_debug: executePrefill metal_prepared_tail_greedy ' "$mtp_out"; then
    fail "chunked MTP prefill incorrectly used the whole-prefill prepared tail"
  fi
fi

echo "metal Gemma4 MTP long-context token gate passed"
echo "prompt_tokens=$target_prompt_tokens prefill_chunk_size=$target_chunk requested_prefill=$PREFILL_CHUNK_SIZE output_tokens=$generated_tokens finish_reason=$EXPECTED_FINISH_REASON"
echo "whole_model_executor=handled"
if [[ "$PRINT_TIMING" != "0" ]]; then
  echo "target $(grep '^generate_timing_ms:' "$target_out" | tail -n 1)"
  echo "whole_model_target $(grep '^timing_ms:' "$whole_model_out" | tail -n 1)"
  echo "whole_model_executor $(grep '^metal_executor_ms:' "$whole_model_out" | tail -n 1)"
  echo "whole_model_executor $(grep '^metal_executor_ms2:' "$whole_model_out" | tail -n 1)"
  echo "mtp_k${SPECULATIVE_K} $(grep '^generate_timing_ms:' "$mtp_out" | tail -n 1)"
fi
echo "output: $OUT_DIR"
