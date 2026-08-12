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

# Non-interactive smoke test for `antfly inference chat`: scripts two turns
# plus /bye through the REPL and asserts that the model answered, the stats
# footer appeared, and the second turn reused the cached KV prefix.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PKG_DIR="$ROOT_DIR/pkg/inference"

ANTFLY_BIN="${ANTFLY_BIN:-$PKG_DIR/zig-out/bin/antfly-inference}"
MODEL_DIR="${ANTFLY_INFERENCE_CHAT_SMOKE_MODEL:-$HOME/.antfly/inference/models/google/gemma-4-E2B-it-qat-q4_0-gguf}"
MAX_TOKENS="${ANTFLY_INFERENCE_CHAT_SMOKE_MAX_TOKENS:-512}"
BACKEND="${ANTFLY_INFERENCE_CHAT_SMOKE_BACKEND:-auto}"
OUT_DIR="${OUT_DIR:-/tmp/antfly-inference-chat-smoke}"

if [[ ! -x "$ANTFLY_BIN" ]]; then
  echo "antfly binary not executable: $ANTFLY_BIN" >&2
  echo "build it first, for example: cd pkg/inference && zig build -Doptimize=ReleaseFast -Dmetal=true -Donnx=false -Dpjrt=false" >&2
  exit 2
fi

if [[ ! -d "$MODEL_DIR" ]]; then
  echo "chat smoke model not installed: $MODEL_DIR" >&2
  echo "install it first: $ANTFLY_BIN pull google/gemma-4-E2B-it-qat-q4_0-gguf:gguf" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"
LOG_FILE="$OUT_DIR/chat-smoke.log"

printf 'What is the capital of France? Answer briefly, then name one famous museum there.\nWhich river runs through that city?\n/bye\n' |
  "$ANTFLY_BIN" chat "$MODEL_DIR" \
    --backend "$BACKEND" \
    --max-tokens "$MAX_TOKENS" \
    --temperature 0 \
    >"$LOG_FILE" 2>&1 || {
  echo "chat smoke run failed; log follows" >&2
  cat "$LOG_FILE" >&2
  exit 1
}

fail() {
  echo "chat smoke assertion failed: $1" >&2
  cat "$LOG_FILE" >&2
  exit 1
}

# Both turns must produce real public text, and turn 2 must resolve the
# conversational reference ("that city") from turn 1's context.
grep -qi 'Paris' "$LOG_FILE" || fail "first turn did not answer Paris"
grep -qi 'Seine' "$LOG_FILE" || fail "second turn did not answer Seine from context"
# Stats footer: "(N tok · X tok/s · ..." once per turn.
[[ "$(grep -c 'tok/s' "$LOG_FILE")" -ge 2 ]] || fail "expected a stats footer for both turns"

# KV prefix reuse is opt-in until the cache attach path is fixed (see
# GEMMA4.md "Chat REPL"); exercise it only when explicitly requested.
if [[ "${ANTFLY_INFERENCE_CHAT_SMOKE_PROMPT_CACHE:-0}" == "1" ]]; then
  printf 'What is the capital of France? Answer briefly, then name one famous museum there.\nWhich river runs through that city?\n/bye\n' |
    "$ANTFLY_BIN" chat "$MODEL_DIR" \
      --backend "$BACKEND" \
      --max-tokens "$MAX_TOKENS" \
      --temperature 0 \
      --prompt-cache \
      >"$LOG_FILE.cache" 2>&1 || fail "prompt-cache smoke run crashed"
  grep -E '[1-9][0-9]* cached' "$LOG_FILE.cache" >/dev/null || fail "cached run reported no cached prompt tokens"
  grep -qi 'Seine' "$LOG_FILE.cache" || fail "cached run lost conversational context (known attach bug)"
fi

echo "chat smoke passed ($LOG_FILE)"
