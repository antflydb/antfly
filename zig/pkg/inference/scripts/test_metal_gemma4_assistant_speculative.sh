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

ANTFLY_BIN="${ANTFLY_BIN:-$PKG_DIR/zig-out/bin/antfly}"
TARGET_MODEL_DIR="${ANTFLY_INFERENCE_GEMMA4_TARGET_MODEL:-$HOME/.antfly/inference/models/google/gemma-4-E2B-it}"
DRAFT_MODEL_DIR="${ANTFLY_INFERENCE_GEMMA4_DRAFT_MODEL:-$HOME/.antfly/inference/models/google/gemma-4-E2B-it-assistant}"
PROMPT="${ANTFLY_INFERENCE_GEMMA4_ASSISTANT_PROMPT:-hi}"
MAX_TOKENS="${ANTFLY_INFERENCE_GEMMA4_ASSISTANT_MAX_TOKENS:-4}"
SPECULATIVE_K="${ANTFLY_INFERENCE_GEMMA4_ASSISTANT_SPECULATIVE_K:-2}"
EXPECTED_TOKEN_IDS="${ANTFLY_INFERENCE_GEMMA4_ASSISTANT_EXPECTED_TOKEN_IDS:-10979 236888 2088 740}"
BACKEND="${ANTFLY_INFERENCE_GEMMA4_ASSISTANT_BACKEND:-auto}"
HOST_BUDGET_MB="${ANTFLY_INFERENCE_GEMMA4_ASSISTANT_HOST_BUDGET_MB:-12288}"
COMBINED_BUDGET_MB="${ANTFLY_INFERENCE_GEMMA4_ASSISTANT_COMBINED_BUDGET_MB:-17408}"
OUT_DIR="${OUT_DIR:-/tmp/antfly-inference-metal-gemma4-assistant-speculative}"
DEBUG_METAL_SCRIPT="$PKG_DIR/scripts/debug_metal_command.sh"

if [[ ! -x "$ANTFLY_BIN" ]]; then
  echo "antfly binary not executable: $ANTFLY_BIN" >&2
  echo "build it first, for example: cd pkg/inference && zig build -Doptimize=ReleaseFast -Dmetal=true -Dmlx=false -Donnx=false -Dpjrt=false" >&2
  exit 2
fi

if [[ ! -d "$TARGET_MODEL_DIR" ]]; then
  echo "Gemma4 target model directory not found: $TARGET_MODEL_DIR" >&2
  echo "set ANTFLY_INFERENCE_GEMMA4_TARGET_MODEL to the local target model directory" >&2
  exit 2
fi

if [[ ! -d "$DRAFT_MODEL_DIR" ]]; then
  echo "Gemma4 assistant model directory not found: $DRAFT_MODEL_DIR" >&2
  echo "set ANTFLY_INFERENCE_GEMMA4_DRAFT_MODEL to the local assistant model directory" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"
out="$OUT_DIR/assistant.txt"
debug_out="$OUT_DIR/debug"

set +e
TERMITE_DEBUG_METAL_TIMING=1 \
bash "$DEBUG_METAL_SCRIPT" command \
  --label metal-gemma4-assistant-speculative \
  --out-dir "$debug_out" \
  --timeout "${ANTFLY_INFERENCE_GEMMA4_ASSISTANT_TIMEOUT_SECS:-60}" \
  --api-validate \
  --cwd "$ROOT_DIR" \
  -- "$ANTFLY_BIN" inference generate "$TARGET_MODEL_DIR" "$PROMPT" \
  --backend "$BACKEND" \
  --draft-model "$DRAFT_MODEL_DIR" \
  --speculative-k "$SPECULATIVE_K" \
  --max-tokens "$MAX_TOKENS" \
  --host-budget-mb "$HOST_BUDGET_MB" \
  --combined-budget-mb "$COMBINED_BUDGET_MB" \
  --print-token-ids \
  --print-token-count \
  --print-timing >"$out" 2>&1
rc=$?
set -e

if [[ "$rc" != "0" ]]; then
  echo "Gemma4 assistant speculative run failed; output: $out debug: $debug_out" >&2
  sed -n '1,260p' "$out" >&2
  if [[ -f "$debug_out/stdout.txt" ]]; then
    sed -n '1,260p' "$debug_out/stdout.txt" >&2
  fi
  exit 1
fi

token_ids="$(awk '/^token_ids:/ { sub(/^token_ids:[[:space:]]*/, ""); print; exit }' "$debug_out/stdout.txt")"
if [[ "$token_ids" != "$EXPECTED_TOKEN_IDS" ]]; then
  echo "Gemma4 assistant token anchor failed" >&2
  echo "expected: $EXPECTED_TOKEN_IDS" >&2
  echo "actual:   ${token_ids:-<missing>}" >&2
  echo "output:   $debug_out/stdout.txt" >&2
  exit 1
fi

if ! grep -Eq 'speculative: rounds=[1-9][0-9]* drafted=[1-9][0-9]* matched=[1-9][0-9]*' "$debug_out/stdout.txt"; then
  echo "Gemma4 assistant did not produce accepted speculative drafts; output: $debug_out/stdout.txt" >&2
  grep -E 'speculative:|token_ids:' "$debug_out/stdout.txt" >&2 || true
  exit 1
fi

if grep -q 'metal decoder-runtime prewarm failed' "$debug_out/stdout.txt"; then
  echo "Gemma4 assistant emitted a stale decoder-runtime prewarm warning; output: $debug_out/stdout.txt" >&2
  exit 1
fi

echo "metal Gemma4 assistant speculative smoke passed"
echo "output: $debug_out/stdout.txt"
echo "debug: $debug_out"
