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
# shellcheck source=scripts/inference_cli.sh
source "$SCRIPT_DIR/../inference_cli.sh"

ANTFLY_BIN="$(resolve_antfly_inference_bin)"

MODEL_DIR="${ANTFLY_INFERENCE_GEMMA4_MODEL:-$HOME/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf}"
PROMPT="${ANTFLY_INFERENCE_GEMMA4_TOOL_PROMPT:-Use the lookup_order tool for order_id A123. Return only the tool call.}"
MAX_TOKENS="${ANTFLY_INFERENCE_GEMMA4_TOOL_MAX_TOKENS:-64}"
OUT_DIR="${OUT_DIR:-/tmp/antfly-inference-metal-gemma4-cli-tool-calling-test}"

if [[ ! -x "$ANTFLY_BIN" ]]; then
  echo "antfly inference binary not executable: $ANTFLY_BIN" >&2
  echo "build it first, for example: cd pkg/inference && zig build -Dmetal=true -Donnx=false -Dpjrt=false" >&2
  exit 2
fi

if [[ ! -d "$MODEL_DIR" ]]; then
  echo "Gemma4 model directory not found: $MODEL_DIR" >&2
  echo "set ANTFLY_INFERENCE_GEMMA4_MODEL to the local GGUF model directory" >&2
  exit 2
fi

mkdir -p "$OUT_DIR"
TOOLS_JSON="$OUT_DIR/tools.json"
RESPONSE_JSON="$OUT_DIR/response.json"
STDERR_LOG="$OUT_DIR/stderr.log"

cat >"$TOOLS_JSON" <<'JSON'
[
  {
    "type": "function",
    "function": {
      "name": "lookup_order",
      "description": "Look up the status of a customer order.",
      "parameters": {
        "type": "object",
        "properties": {
          "order_id": {
            "type": "string",
            "description": "The order identifier."
          }
        },
        "required": ["order_id"]
      }
    }
  }
]
JSON

run_antfly() {
  run_antfly_inference "$@"
}

if ! run_antfly generate --help 2>&1 | grep -q -- '--tools'; then
  echo "metal Gemma4 CLI tool-calling diagnostic: generate CLI has no --tools support; server API coverage is test-metal-gemma4-tool-calling"
  exit 0
fi

if ! run_antfly generate "$MODEL_DIR" "$PROMPT" \
  --backend metal \
  --max-tokens "$MAX_TOKENS" \
  --print-timing \
  --tools "$TOOLS_JSON" \
  --tool-choice lookup_order \
  >"$RESPONSE_JSON" 2>"$STDERR_LOG"; then
  echo "CLI generate failed" >&2
  echo "stderr: $STDERR_LOG" >&2
  sed -n '1,220p' "$STDERR_LOG" >&2 || true
  exit 1
fi

if ! grep -q 'live_whole_model_executor=true' "$STDERR_LOG"; then
  echo "CLI generate did not report the live whole-model executor fast path" >&2
  echo "stderr: $STDERR_LOG" >&2
  sed -n '1,220p' "$STDERR_LOG" >&2 || true
  exit 1
fi

if grep -q 'live_whole_model_executor_tool_fallback=true' "$STDERR_LOG"; then
  echo "CLI generate fell back after the live whole-model executor fast path" >&2
  echo "stderr: $STDERR_LOG" >&2
  sed -n '1,220p' "$STDERR_LOG" >&2 || true
  exit 1
fi

python3 - "$RESPONSE_JSON" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    response = json.load(f)
if response.get("object") != "chat.completion":
    raise SystemExit(f"unexpected object: {response.get('object')!r}")
choice = response["choices"][0]
if choice.get("finish_reason") != "tool_calls":
    raise SystemExit(f"expected finish_reason=tool_calls, got {choice.get('finish_reason')!r}")
calls = choice["message"].get("tool_calls") or []
if not calls:
    raise SystemExit("missing normalized tool_calls")
call = calls[0]
if call.get("type") != "function":
    raise SystemExit(f"unexpected call type: {call.get('type')!r}")
function = call.get("function") or {}
if function.get("name") != "lookup_order":
    raise SystemExit(f"unexpected function name: {function.get('name')!r}")
arguments = function.get("arguments")
if not isinstance(arguments, str):
    raise SystemExit("function.arguments must be a JSON string")
try:
    json.loads(arguments)
except json.JSONDecodeError as exc:
    raise SystemExit(f"function.arguments is not valid JSON: {exc}") from exc
usage = response.get("usage") or {}
for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
    if key not in usage:
        raise SystemExit(f"missing usage.{key}")
PY

echo "metal Gemma4 CLI tool-calling smoke passed"
echo "response: $RESPONSE_JSON"
echo "stderr: $STDERR_LOG"
