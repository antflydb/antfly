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
PKG_DIR="$ROOT_DIR/pkg/termite"

TERMITE_BIN="${TERMITE_BIN:-$PKG_DIR/zig-out/bin/termite}"
MODEL_DIR="${TERMITE_GEMMA4_MODEL:-$HOME/.termite/models/ggml-org/gemma-4-e2b-it-gguf}"
PROMPT="${TERMITE_GEMMA4_PREFILL_PROMPT:-hi}"
MAX_TOKENS="${TERMITE_GEMMA4_PREFILL_MAX_TOKENS:-4}"
EXPECTED_TOKEN_IDS="${TERMITE_GEMMA4_EXPECTED_TOKEN_IDS:-10979 236888 2088 740}"
OUT_DIR="${OUT_DIR:-/tmp/termite-metal-gemma4-prefill-frame-test}"

if [[ ! -x "$TERMITE_BIN" ]]; then
  echo "termite binary not executable: $TERMITE_BIN" >&2
  echo "build it first, for example: cd pkg/termite && zig build -Doptimize=ReleaseFast -Dmetal=true -Donnx=false -Dpjrt=false" >&2
  exit 2
fi

if [[ ! -d "$MODEL_DIR" ]]; then
  echo "Gemma4 model directory not found: $MODEL_DIR" >&2
  echo "set TERMITE_GEMMA4_MODEL to the local GGUF model directory" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"

run_case() {
  local label="$1"
  shift
  local out="$OUT_DIR/${label}.txt"
  echo "running $label..." >&2
  (
    cd "$ROOT_DIR"
    "$@" "$TERMITE_BIN" generate "$MODEL_DIR" "$PROMPT" \
      --backend metal \
      --max-tokens "$MAX_TOKENS" \
      --print-token-ids \
      --print-token-count \
      --print-timing
  ) >"$out" 2>&1
  echo "$out"
}

assert_anchor() {
  local label="$1"
  local out="$2"
  local actual
  actual="$(awk '/^token_ids:/ { sub(/^token_ids:[[:space:]]*/, ""); print; exit }' "$out")"
  if [[ "$actual" != "$EXPECTED_TOKEN_IDS" ]]; then
    echo "unexpected token_ids for $label" >&2
    echo "expected: $EXPECTED_TOKEN_IDS" >&2
    echo "actual:   ${actual:-<missing>}" >&2
    echo "output:   $out" >&2
    sed -n '1,220p' "$out" >&2
    exit 1
  fi

  if ! grep -q '^metal_decoder_frame:' "$out"; then
    echo "missing metal_decoder_frame counters for $label" >&2
    echo "output: $out" >&2
    exit 1
  fi

  if ! grep -q 'prefill_direct_family=[1-9]' "$out"; then
    echo "prefill direct family path was not exercised for $label" >&2
    echo "output: $out" >&2
    exit 1
  fi

  if ! grep -q 'gemma_fused_attn_residual_hits=[1-9]' "$out"; then
    echo "attention residual fused path was not exercised for $label" >&2
    echo "output: $out" >&2
    exit 1
  fi

  if ! grep -q 'attn_out_linear=0 attn_post_norm=0 attn_residual_add=0' "$out"; then
    echo "attention residual path fell back to split prefill ops for $label" >&2
    echo "output: $out" >&2
    exit 1
  fi
}

default_out="$(run_case default env)"
assert_anchor default "$default_out"

sync_out="$(run_case stage-sync env TERMITE_METAL_SYNC_GATED_FAMILY_STAGES=1)"
assert_anchor stage-sync "$sync_out"

echo "metal Gemma4 prefill-frame anchor passed"
echo "default:    $default_out"
echo "stage-sync: $sync_out"
