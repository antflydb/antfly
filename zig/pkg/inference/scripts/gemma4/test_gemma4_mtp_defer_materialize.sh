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
#
# Deterministic gates for explicitly enabled ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE
# (spec-decode P2 fold): with the fold on, every run must produce token ids identical to the
# plain target, materializations must drop to 0 (replaced by
# deferred_materializations), and pending tokens must be flushed before the
# mid-generation standardDecode fallbacks.
#
# Covers the handoff's scripted scenarios:
#   1. correction -> pending -> next verify   (force k=1/2/4 identity runs)
#   3. bonus -> pending -> next verify        (same runs with ACCEPT_BONUS=1)
#   2. pending -> standardDecode fallback     (zero-match / acceptance / cost
#      gates; zero-match k=1 asserts pending_flushes=1)
#   5. EOS/max-tokens while pending           (all runs end at max-tokens with
#      fold rounds outstanding; clean exit + identity)
# (Scenario 4, verify-error rollback while pending, is unit-level:
#  "gemma4 mtp pending token bookkeeping holds kv invariant across round
#  shapes" in generation.zig.)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../inference_cli.sh"

OUT_DIR="${OUT_DIR:-/tmp/antfly-mtp-defer-materialize-$(date -u +%Y%m%d-%H%M%S)}"
mkdir -p "$OUT_DIR"

DEFAULT_MODELS_DIR="${ANTFLY_INFERENCE_GEMMA4_MODELS_DIR:-$HOME/.antfly/inference/models}"
MODEL="${ANTFLY_INFERENCE_GEMMA4_QAT_MODEL:-$DEFAULT_MODELS_DIR/google/gemma-4-E4B-it-qat-q4_0-gguf}"
DRAFT="${ANTFLY_INFERENCE_GEMMA4_MTP_POLICY_DRAFT_MODEL:-${MODEL%-gguf}-unquantized-assistant}"
TOKENS="${ANTFLY_GEMMA4_MTP_DEFER_TEST_TOKENS:-48}"
PROMPT="${ANTFLY_GEMMA4_MTP_DEFER_TEST_PROMPT:-}"
if [[ -z "$PROMPT" ]]; then
  printf -v PROMPT '<|turn>user\nWrite one short paragraph about local inference.<turn|>\n<|turn>model\n<|channel>thought\n<channel|>'
fi

if [[ ! -e "$MODEL" ]]; then
  echo "missing target model: $MODEL" >&2
  exit 2
fi
if [[ ! -e "$DRAFT" ]]; then
  echo "missing MTP draft model: $DRAFT" >&2
  exit 2
fi

token_ids_from_output() {
  awk '/^token_ids:/ { sub(/^token_ids:[[:space:]]*/, ""); print; exit }' "$1"
}

profile_counter() {
  # profile_counter <file> <name> -> value from the mtp_profile: line
  sed -n 's/^mtp_profile: .*[[:space:]]'"$2"'=\([0-9][0-9]*\).*/\1/p' "$1" | head -n 1
}

spec_counter() {
  # spec_counter <file> <name> -> value from the speculative: line
  sed -n 's/^speculative: .*[[:space:]]'"$2"'=\([0-9][0-9]*\).*/\1/p' "$1" | head -n 1
}

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

run_target() {
  local out="$1"
  run_antfly_inference generate "$MODEL" "$PROMPT" \
    --backend metal \
    --max-tokens "$TOKENS" \
    --temperature 0 \
    --raw-prompt \
    --print-token-count \
    --print-finish-reason \
    --print-token-ids \
    >"$out" 2>&1
}

run_spec() {
  # run_spec <out-file> <k> <policy> [extra env as VAR=VAL ...]
  local out="$1" k="$2" policy="$3"
  shift 3
  local calibration_args=""
  if [[ "$policy" == "auto" ]]; then
    calibration_args="--speculation-calibration positive"
  fi
  (
    export ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE=1
    export ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE_TARGET_ACTIVATION=1
    local kv
    for kv in "$@"; do export "${kv?}"; done
    export ANTFLY_GEMMA4_MTP_PROFILE=1
    # shellcheck disable=SC2086
    run_antfly_inference generate "$MODEL" "$PROMPT" \
      --backend metal \
      --draft-model "$DRAFT" \
      --speculative-k "$k" \
      --speculation-policy "$policy" \
      $calibration_args \
      --max-tokens "$TOKENS" \
      --temperature 0 \
      --raw-prompt \
      --print-token-count \
      --print-finish-reason \
      --print-token-ids \
      --print-timing \
      >"$out" 2>&1
  )
}

echo "== golden: plain target ($TOKENS tokens) =="
run_target "$OUT_DIR/target.txt"
TARGET_IDS="$(token_ids_from_output "$OUT_DIR/target.txt")"
[[ -n "$TARGET_IDS" ]] || fail "no token_ids from plain target run ($OUT_DIR/target.txt)"

check_identity() {
  local label="$1" out="$2"
  local ids
  ids="$(token_ids_from_output "$out")"
  if [[ -z "$ids" || "$ids" != "$TARGET_IDS" ]]; then
    echo "target: ${TARGET_IDS:-<missing>}" >&2
    echo "$label:  ${ids:-<missing>} ($out)" >&2
    fail "$label changed token IDs"
  fi
}

total_fold_corrections=0
total_fold_bonus=0
for K in 1 2 4; do
  echo "== fold-on identity: force k=$K + bonus =="
  out="$OUT_DIR/fold-k$K.txt"
  run_spec "$out" "$K" force \
    ANTFLY_GEMMA4_MTP_ACCEPT_BONUS=1
  check_identity "fold-k$K" "$out"
  mat="$(profile_counter "$out" materializations)"
  deferred="$(profile_counter "$out" deferred_materializations)"
  corrections="$(spec_counter "$out" corrections)"
  bonus="$(spec_counter "$out" bonus)"
  [[ "$mat" == "0" ]] || fail "fold-k$K materializations=$mat (want 0) ($out)"
  [[ -n "$deferred" && "$deferred" -ge 1 ]] || fail "fold-k$K deferred_materializations=$deferred (want >=1) ($out)"
  total_fold_corrections=$((total_fold_corrections + ${corrections:-0}))
  total_fold_bonus=$((total_fold_bonus + ${bonus:-0}))
  echo "   ok: corrections=$corrections bonus=$bonus deferred=$deferred"
done
[[ "$total_fold_corrections" -ge 1 ]] || fail "no correction round hit across fold runs; correction->pending->verify not exercised"
[[ "$total_fold_bonus" -ge 1 ]] || fail "no bonus round hit across fold runs; bonus->pending->verify not exercised"

echo "== fold-on + zero-match fallback (pending must flush before standardDecode) =="
out="$OUT_DIR/fold-fallback-zero-match.txt"
run_spec "$out" 1 auto \
  ANTFLY_GEMMA4_MTP_ACCEPT_BONUS=1 \
  ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO=1 \
  ANTFLY_GEMMA4_MTP_AUTO_MIN_TOKENS=1 \
  ANTFLY_GEMMA4_MTP_ADAPTIVE_K=0 \
  ANTFLY_GEMMA4_MTP_ZERO_MATCH_FALLBACK_ROUNDS=1 \
  ANTFLY_GEMMA4_MTP_MIN_ACCEPTANCE_PERMILLE=0 \
  ANTFLY_GEMMA4_MTP_AUTO_COST_PROBE_ROUNDS=0
check_identity "fold-fallback-zero-match" "$out"
grep -Eq '^speculative: .*decision=disabled_zero_match' "$out" \
  || fail "zero-match fallback did not trigger ($out)"
flushes="$(profile_counter "$out" pending_flushes)"
[[ "$flushes" == "1" ]] || fail "zero-match fallback pending_flushes=$flushes (want 1) ($out)"
mat="$(profile_counter "$out" materializations)"
[[ "$mat" == "0" ]] || fail "zero-match fallback materializations=$mat (want 0) ($out)"
echo "   ok: pending flushed once before fallback"

echo "== fold-on + acceptance-gate fallback =="
out="$OUT_DIR/fold-fallback-acceptance.txt"
run_spec "$out" 2 auto \
  ANTFLY_GEMMA4_MTP_ACCEPT_BONUS=1 \
  ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO=1 \
  ANTFLY_GEMMA4_MTP_AUTO_MIN_TOKENS=1 \
  ANTFLY_GEMMA4_MTP_ADAPTIVE_K=0 \
  ANTFLY_GEMMA4_MTP_ZERO_MATCH_FALLBACK_ROUNDS=0 \
  ANTFLY_GEMMA4_MTP_ACCEPTANCE_PROBE_DRAFTS=2 \
  ANTFLY_GEMMA4_MTP_MIN_ACCEPTANCE_PERMILLE=1001 \
  ANTFLY_GEMMA4_MTP_AUTO_COST_PROBE_ROUNDS=0
check_identity "fold-fallback-acceptance" "$out"
grep -Eq '^speculative: .*decision=disabled_low_acceptance' "$out" \
  || fail "acceptance-gate fallback did not trigger ($out)"
echo "   ok"

echo "== fold-on + cost-gate fallback =="
out="$OUT_DIR/fold-fallback-cost.txt"
run_spec "$out" 2 auto \
  ANTFLY_GEMMA4_MTP_ACCEPT_BONUS=1 \
  ANTFLY_GEMMA4_MTP_ENABLE_METAL_AUTO=1 \
  ANTFLY_GEMMA4_MTP_AUTO_MIN_TOKENS=1 \
  ANTFLY_GEMMA4_MTP_ADAPTIVE_K=0 \
  ANTFLY_GEMMA4_MTP_ZERO_MATCH_FALLBACK_ROUNDS=0 \
  ANTFLY_GEMMA4_MTP_MIN_ACCEPTANCE_PERMILLE=0 \
  ANTFLY_GEMMA4_MTP_AUTO_COST_PROBE_ROUNDS=1 \
  ANTFLY_GEMMA4_MTP_AUTO_MIN_ACCEPTED_PER_ROUND_MILLI=999000
check_identity "fold-fallback-cost" "$out"
grep -Eq '^speculative: .*decision=disabled_slow' "$out" \
  || fail "cost-gate fallback did not trigger ($out)"
echo "   ok"

echo "== fold-off control: force k=2 + bonus (no behavior change) =="
out="$OUT_DIR/foldoff-k2.txt"
run_spec "$out" 2 force \
  ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE=0 \
  ANTFLY_GEMMA4_MTP_DEFER_MATERIALIZE_TARGET_ACTIVATION=0 \
  ANTFLY_GEMMA4_MTP_ACCEPT_BONUS=1
check_identity "foldoff-k2" "$out"
deferred="$(profile_counter "$out" deferred_materializations)"
flushes="$(profile_counter "$out" pending_flushes)"
[[ "$deferred" == "0" ]] || fail "fold-off deferred_materializations=$deferred (want 0) ($out)"
[[ "$flushes" == "0" ]] || fail "fold-off pending_flushes=$flushes (want 0) ($out)"
echo "   ok"

echo "defer-materialize gates passed"
echo "raw output: $OUT_DIR"
