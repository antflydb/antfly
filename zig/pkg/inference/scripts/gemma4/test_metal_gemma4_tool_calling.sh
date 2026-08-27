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

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
PKG_DIR="$ROOT_DIR/pkg/inference"

if [[ -z "${ANTFLY_BIN:-}" ]]; then
  if [[ -x "$PKG_DIR/zig-out/bin/antfly-inference" ]]; then
    ANTFLY_BIN="$PKG_DIR/zig-out/bin/antfly-inference"
  else
    ANTFLY_BIN="$PKG_DIR/zig-out/bin/antfly"
  fi
fi

MODEL_DIR="${ANTFLY_INFERENCE_GEMMA4_MODEL:-$HOME/.antfly/inference/models/ggml-org/gemma-4-e2b-it-gguf}"
MODEL_NAME="${ANTFLY_INFERENCE_GEMMA4_MODEL_NAME:-ggml-org/gemma-4-e2b-it-gguf}"
MODELS_DIR="${ANTFLY_INFERENCE_MODELS_DIR:-$(dirname "$(dirname "$MODEL_DIR")")}"
PORT="${ANTFLY_INFERENCE_GEMMA4_TOOL_PORT:-18091}"
HOST="${ANTFLY_INFERENCE_GEMMA4_TOOL_HOST:-127.0.0.1}"
MAX_TOKENS="${ANTFLY_INFERENCE_GEMMA4_TOOL_MAX_TOKENS:-64}"
OUT_DIR="${OUT_DIR:-/tmp/antfly-inference-metal-gemma4-tool-calling-test}"

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
REQUEST_JSON="$OUT_DIR/request.json"
RESPONSE_JSON="$OUT_DIR/response.json"
SERVER_LOG="$OUT_DIR/server.log"

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

python3 - "$MODEL_NAME" "$MAX_TOKENS" "$TOOLS_JSON" "$REQUEST_JSON" <<'PY'
import json
import sys

model, max_tokens, tools_path, request_path = sys.argv[1:5]
with open(tools_path, "r", encoding="utf-8") as f:
    tools = json.load(f)
body = {
    "model": model,
    "backend": "metal",
    "messages": [
        {
            "role": "user",
            "content": "Use the lookup_order tool for order_id A123. Return only the tool call.",
        }
    ],
    "tools": tools,
    "tool_choice": {"type": "function", "function": {"name": "lookup_order"}},
    "max_tokens": int(max_tokens),
    "temperature": 0,
}
with open(request_path, "w", encoding="utf-8") as f:
    json.dump(body, f)
PY

run_antfly() {
  local subcommand="$1"
  shift
  if [[ "$(basename "$ANTFLY_BIN")" == "antfly" ]]; then
    "$ANTFLY_BIN" inference "$subcommand" "$@"
  else
    "$ANTFLY_BIN" "$subcommand" "$@"
  fi
}

run_antfly run --models-dir "$MODELS_DIR" --host "$HOST" --port "$PORT" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!
cleanup() {
  kill "$SERVER_PID" >/dev/null 2>&1 || true
}
trap cleanup EXIT

for _ in {1..120}; do
  if curl -fsS "http://$HOST:$PORT/ai/v1/models" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    echo "server exited before becoming ready" >&2
    sed -n '1,220p' "$SERVER_LOG" >&2
    exit 1
  fi
  sleep 0.5
done

status="$(curl -sS -o "$RESPONSE_JSON" -w "%{http_code}" \
  -H "content-type: application/json" \
  -d @"$REQUEST_JSON" \
  "http://$HOST:$PORT/ai/v1/generate")"
if [[ "$status" != "200" ]]; then
  echo "generate request failed with HTTP $status" >&2
  echo "response: $RESPONSE_JSON" >&2
  sed -n '1,220p' "$RESPONSE_JSON" >&2 || true
  echo "server log: $SERVER_LOG" >&2
  sed -n '1,220p' "$SERVER_LOG" >&2 || true
  exit 1
fi

python3 - "$RESPONSE_JSON" <<'PY'
import json
import sys

with open(sys.argv[1], "r", encoding="utf-8") as f:
    response = json.load(f)
choice = response["choices"][0]
if choice.get("finish_reason") != "tool_calls":
    raise SystemExit(f"expected finish_reason=tool_calls, got {choice.get('finish_reason')!r}")
calls = choice["message"].get("tool_calls") or []
if not calls:
    raise SystemExit("missing tool_calls")
call = calls[0]
if call.get("type") != "function":
    raise SystemExit(f"unexpected call type: {call.get('type')!r}")
function = call.get("function") or {}
if function.get("name") != "lookup_order":
    raise SystemExit(f"unexpected function name: {function.get('name')!r}")
try:
    json.loads(function.get("arguments") or "")
except json.JSONDecodeError as exc:
    raise SystemExit(f"function.arguments is not valid JSON: {exc}") from exc
PY

echo "metal Gemma4 server tool-calling smoke passed"
echo "response: $RESPONSE_JSON"
echo "server log: $SERVER_LOG"
