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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PKG_DIR="$ROOT_DIR/pkg/inference"

TERMITE_BIN="${TERMITE_BIN:-$PKG_DIR/zig-out/bin/termite}"
MODEL_DIR="${TERMITE_GEMMA4_MODEL:-$HOME/.termite/models/ggml-org/gemma-4-e2b-it-gguf}"
PROMPT="${TERMITE_GEMMA4_PREFILL_PROMPT:-hi}"
MAX_TOKENS="${TERMITE_GEMMA4_PREFILL_MAX_TOKENS:-4}"
EXPECTED_TOKEN_IDS="${TERMITE_GEMMA4_EXPECTED_TOKEN_IDS:-10979 236888 2088 740}"
COMPARE_LAYER="${TERMITE_GEMMA4_PREFILL_BLOCK_COMPARE_LAYER:-0}"
OUT_DIR="${OUT_DIR:-/tmp/termite-metal-gemma4-prefill-block-parity}"

if [[ ! -x "$TERMITE_BIN" ]]; then
  echo "termite binary not executable: $TERMITE_BIN" >&2
  echo "build it first, for example: cd pkg/inference && zig build -Doptimize=ReleaseFast -Dmetal=true -Dmlx=false -Donnx=false -Dpjrt=false" >&2
  exit 2
fi

if [[ ! -d "$MODEL_DIR" ]]; then
  echo "Gemma4 model directory not found: $MODEL_DIR" >&2
  echo "set TERMITE_GEMMA4_MODEL to the local GGUF model directory" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"

run_generate() {
  local label="$1"
  shift
  local out="$OUT_DIR/${label}.txt"
  echo "running $label..." >&2
  set +e
  (
    cd "$ROOT_DIR"
    "$@" "$TERMITE_BIN" generate "$MODEL_DIR" "$PROMPT" \
      --backend metal \
      --max-tokens "$MAX_TOKENS" \
      --print-token-ids \
      --print-token-count \
      --print-timing
  ) >"$out" 2>&1
  local rc=$?
  set -e
  echo "$rc" >"$OUT_DIR/${label}.rc"
  echo "$out"
}

token_ids() {
  awk '/^token_ids:/ { sub(/^token_ids:[[:space:]]*/, ""); print; exit }' "$1"
}

safe_out="$(run_generate safe env \
  TERMITE_DEBUG_METAL_TIMING=1 \
  TERMITE_METAL_DISABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK=1)"
safe_rc="$(cat "$OUT_DIR/safe.rc")"
if [[ "$safe_rc" != "0" ]]; then
  echo "safe staged prefill run failed; output: $safe_out" >&2
  sed -n '1,220p' "$safe_out" >&2
  exit 1
fi

safe_tokens="$(token_ids "$safe_out")"
if [[ "$safe_tokens" != "$EXPECTED_TOKEN_IDS" ]]; then
  echo "safe staged prefill token anchor failed" >&2
  echo "expected: $EXPECTED_TOKEN_IDS" >&2
  echo "actual:   ${safe_tokens:-<missing>}" >&2
  echo "output:   $safe_out" >&2
  exit 1
fi

if ! grep -q 'gemma_fused_attn_residual_hits=[1-9]' "$safe_out"; then
  echo "safe run did not exercise attention residual fusion; output: $safe_out" >&2
  exit 1
fi

block_out="$(run_generate block env \
  TERMITE_DEBUG_METAL_TIMING=1 \
  TERMITE_METAL_ENABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK=1)"
block_rc="$(cat "$OUT_DIR/block.rc")"
block_tokens="$(token_ids "$block_out")"

if [[ "$block_rc" != "0" ]]; then
  echo "full runtime prefill block failed before parity could be checked" >&2
  echo "safe output:  $safe_out" >&2
  echo "block output: $block_out" >&2
  sed -n '1,260p' "$block_out" >&2
  exit 1
fi

if [[ "$block_tokens" != "$safe_tokens" ]]; then
  diag_out="$(run_generate diag env \
    TERMITE_DEBUG_METAL_TIMING=1 \
    TERMITE_METAL_ENABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK=1 \
    TERMITE_METAL_COMPARE_GATED_FAMILY=1 \
    TERMITE_METAL_COMPARE_GATED_FAMILY_ALLOW_FRAME=1 \
    TERMITE_METAL_COMPARE_GATED_LAYER="$COMPARE_LAYER")"
  echo "full runtime prefill block token mismatch" >&2
  echo "safe tokens:  ${safe_tokens:-<missing>}" >&2
  echo "block tokens: ${block_tokens:-<missing>}" >&2
  echo "safe output:  $safe_out" >&2
  echo "block output: $block_out" >&2
  echo "diag output:  $diag_out" >&2
  grep -E 'gated-family-compare|token_ids:|decoder_gated_|metal_gated_|metal_direct_paths|metal_active_decode' "$block_out" >&2 || true
  grep -E 'gated-family-compare|token_ids:|decoder_gated_|metal_gated_|metal_direct_paths|metal_active_decode' "$diag_out" >&2 || true
  exit 1
fi

if ! grep -q 'metal_decoder_frame: begins=[1-9]' "$block_out"; then
  echo "planned Gemma4 prefill runtime did not open decoder frames" >&2
  echo "block output: $block_out" >&2
  exit 1
fi

if ! grep -q 'metal_runtime_encoders: compute=[1-9]' "$block_out"; then
  echo "planned Gemma4 prefill runtime did not issue Metal runtime encoders" >&2
  echo "block output: $block_out" >&2
  exit 1
fi

if ! grep -q 'f32_q80_direct_ok=[1-9]' "$block_out"; then
  echo "planned Gemma4 shared-KV prefill did not exercise the qLen>1 Q8_0 whole-frame block path" >&2
  echo "block output: $block_out" >&2
  exit 1
fi

if ! grep -q 'decoder_gated_prefill_ops: tokens=[1-9][0-9]* layers=0' "$block_out"; then
  echo "planned Gemma4 prefill did not bypass the layer-by-layer prefill loop" >&2
  echo "block output: $block_out" >&2
  exit 1
fi

echo "metal Gemma4 prefill-block parity passed"
echo "safe:  $safe_out"
echo "block: $block_out"
