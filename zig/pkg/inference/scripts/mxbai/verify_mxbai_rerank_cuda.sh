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

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$repo_root"

default_models_root="${ANTFLY_INFERENCE_MODELS_DIR:-${HOME:+$HOME/.antfly/inference/models}}"
model_dir="${ANTFLY_MXBAI_RERANK_MODEL_DIR:-${default_models_root:+$default_models_root/mixedbread-ai/mxbai-rerank-base-v1}}"
tolerance="${ANTFLY_MXBAI_RERANK_SCORE_TOLERANCE:-0.0005}"
command_timeout="${ANTFLY_MXBAI_RERANK_COMMAND_TIMEOUT:-900}"
verify_server="${ANTFLY_MXBAI_RERANK_VERIFY_SERVER:-0}"
server_port="${ANTFLY_MXBAI_RERANK_SERVER_PORT:-8097}"
include_slow_cases="${ANTFLY_MXBAI_RERANK_INCLUDE_SLOW_CASES:-0}"
verify_graphs="${ANTFLY_MXBAI_RERANK_VERIFY_GRAPHS:-1}"
cuda_artifacts="${ANTFLY_CUDA_ARTIFACTS:-fatbin}"
cuda_libraries="${ANTFLY_CUDA_LIBS:-auto}"
optimize="${ANTFLY_CUDA_VERIFY_OPTIMIZE:-ReleaseFast}"
zig_global_cache_dir="${ZIG_GLOBAL_CACHE_DIR:-${TMPDIR:-/tmp}/antfly-zig-global-cache}"

resolve_zig() {
  if [[ -n "${ZIG:-}" ]]; then
    printf '%s\n' "$ZIG"
  elif command -v zig >/dev/null 2>&1; then
    command -v zig
  elif [[ -x "../../../.tools/zig-x86_64-linux-0.17.0-dev.1933+e19a73c2d/zig" ]]; then
    printf '%s\n' "../../../.tools/zig-x86_64-linux-0.17.0-dev.1933+e19a73c2d/zig"
  else
    echo "zig not found; set ZIG=/path/to/zig" >&2
    return 1
  fi
}

if [[ -z "$model_dir" ]]; then
  echo "set ANTFLY_MXBAI_RERANK_MODEL_DIR, or set ANTFLY_INFERENCE_MODELS_DIR/HOME for the default model location" >&2
  exit 1
fi
if [[ ! -f "$model_dir/model.safetensors" && ! -f "$model_dir/model.gguf" && -z "$(find "$model_dir" -maxdepth 1 -type f -name '*.gguf' -print -quit 2>/dev/null)" ]]; then
  echo "missing model weights in $model_dir; expected model.safetensors, model.gguf, or a GGUF file" >&2
  exit 1
fi
if [[ ! -f "$model_dir/tokenizer.json" ]]; then
  echo "missing tokenizer at $model_dir/tokenizer.json" >&2
  exit 1
fi

zig_bin="$(resolve_zig)"
"$zig_bin" build --global-cache-dir "$zig_global_cache_dir" -Dcuda=true -Dcuda-artifacts="$cuda_artifacts" -Dcuda-libs="$cuda_libraries" -Doptimize="$optimize"

bin="./zig-out/bin/antfly-inference"

python3 - "$bin" "$model_dir" "$tolerance" "$command_timeout" "$include_slow_cases" "$verify_graphs" <<'PY'
import json
import math
import os
import subprocess
import sys

bin_path, model_dir, tolerance_raw, timeout_raw, include_slow_cases, verify_graphs = sys.argv[1:7]
tolerance = float(tolerance_raw)
timeout = float(timeout_raw)

cases = [
    {
        "name": "basic_relevance",
        "query": "what is CUDA",
        "docs": [
            "CUDA is a parallel computing platform for NVIDIA GPUs.",
            "A sourdough recipe uses flour and water.",
        ],
        "expect_order": True,
    },
    {
        "name": "empty_document",
        "query": "empty document behavior",
        "docs": ["", "This document has content about reranking."],
    },
    {
        "name": "long_truncation",
        "query": "GPU acceleration for rerankers",
        "docs": [
            "CUDA reranker acceleration " * 140,
            "Bread fermentation schedule " * 140,
        ],
        "expect_order": True,
    },
    {
        "name": "punctuation_unicode_escape",
        "query": "does punctuation change reranking?",
        "docs": [
            "GPU, CUDA, reranking: fast; stable; production.",
            "Resume cafe naive facade unrelated text.",
            "Unicode escaped text: \\u2603 \\u03c0 \\u2713",
        ],
    },
]

if include_slow_cases == "1":
    cases.append({
        "name": "many_documents",
        "query": "which documents discuss CUDA reranking",
        "docs": [
            "CUDA kernels can accelerate DeBERTa reranker inference.",
            "This note is about garden soil.",
            "GPU batches improve reranker throughput.",
            "The invoice was paid last Thursday.",
            "A cross encoder scores query and document pairs.",
            "Sourdough starter needs feeding.",
            "NVIDIA L4 can run CUDA inference workloads.",
            "This paragraph is intentionally generic.",
        ],
    })

def parse_cli_json(raw):
    for line in reversed(raw.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            return json.loads(line)
    raise RuntimeError(f"no JSON object found in output:\n{raw}")

def print_subprocess_output(exc):
    output = getattr(exc, "output", None)
    if not output:
        return
    if isinstance(output, bytes):
        output = output.decode(errors="replace")
    sys.stderr.write(output)
    if not output.endswith("\n"):
        sys.stderr.write("\n")

def checked_output(cmd, env=None):
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=timeout, env=env)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        print_subprocess_output(exc)
        raise

def run_cli(backend, case, env=None):
    cmd = [bin_path, "rerank", model_dir, "--query", case["query"], "--backend", backend]
    for doc in case["docs"]:
        cmd.extend(["--doc", doc])
    raw = checked_output(cmd, env=env)
    payload = parse_cli_json(raw)
    scores = [entry["score"] for entry in payload["scores"]]
    if len(scores) != len(case["docs"]):
        raise AssertionError(f"{case['name']} {backend}: expected {len(case['docs'])} scores, got {len(scores)}")
    if any(not math.isfinite(score) for score in scores):
        raise AssertionError(f"{case['name']} {backend}: non-finite scores {scores}")
    return scores

for case in cases:
    native = run_cli("native", case)
    cuda = run_cli("cuda", case)
    max_diff = max((abs(a - b) for a, b in zip(native, cuda)), default=0.0)
    print(f"case={case['name']} docs={len(case['docs'])} max_abs_diff={max_diff:.8f}", flush=True)
    if max_diff > tolerance:
        raise SystemExit(f"{case['name']}: native/cuda max diff {max_diff:.8f} exceeds tolerance {tolerance:.8f}")
    if case.get("expect_order") and not (cuda[0] > cuda[1]):
        raise SystemExit(f"{case['name']}: expected first CUDA score to exceed second score, got {cuda}")
    if verify_graphs == "1":
        graph_env = dict(os.environ)
        graph_env["ANTFLY_CUDA_ENABLE_DEBERTA_GRAPHS"] = "1"
        graph_env["ANTFLY_CUDA_REQUIRE_DEBERTA_GRAPHS"] = "1"
        cuda_graph = run_cli("cuda", case, env=graph_env)
        graph_max_diff = max((abs(a - b) for a, b in zip(cuda, cuda_graph)), default=0.0)
        print(f"case={case['name']} graph_required_max_abs_diff={graph_max_diff:.8f}", flush=True)
        if graph_max_diff > tolerance:
            raise SystemExit(f"{case['name']}: cuda/cuda_graph max diff {graph_max_diff:.8f} exceeds tolerance {tolerance:.8f}")

print("mxbai CLI native/cuda parity completed", flush=True)
PY

if [[ "$verify_server" == "1" ]]; then
  tmpdir="$(mktemp -d /tmp/antfly-mxbai-rerank-server.XXXXXX)"
  server_log="$tmpdir/server.log"
  TERMITE_PREFERRED_BACKEND=cuda "$bin" run \
    --host 127.0.0.1 \
    --port "$server_port" \
    --models-dir "$(dirname "$model_dir")" >"$server_log" 2>&1 &
  server_pid="$!"
  cleanup() {
    kill "$server_pid" >/dev/null 2>&1 || true
    wait "$server_pid" >/dev/null 2>&1 || true
    rm -rf "$tmpdir"
  }
  trap cleanup EXIT

  python3 - "$bin" "$model_dir" "$tolerance" "$command_timeout" "$server_port" "$server_log" <<'PY'
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

bin_path, model_dir, tolerance_raw, timeout_raw, port_raw, server_log = sys.argv[1:7]
tolerance = float(tolerance_raw)
timeout = float(timeout_raw)
port = int(port_raw)
query = "what is CUDA"
docs = [
    "CUDA is a parallel computing platform for NVIDIA GPUs.",
    "A sourdough recipe uses flour and water.",
]

def wait_ready():
    deadline = time.time() + 30.0
    url = f"http://127.0.0.1:{port}/models"
    last_error = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1.0) as response:
                if response.status < 500:
                    return
        except Exception as exc:
            last_error = exc
            time.sleep(0.25)
    raise RuntimeError(f"server did not become ready: {last_error}; log={server_log}")

def parse_cli_json(raw):
    for line in reversed(raw.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            return json.loads(line)
    raise RuntimeError(raw)

def print_subprocess_output(exc):
    output = getattr(exc, "output", None)
    if not output:
        return
    if isinstance(output, bytes):
        output = output.decode(errors="replace")
    sys.stderr.write(output)
    if not output.endswith("\n"):
        sys.stderr.write("\n")

def checked_output(cmd):
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=timeout)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        print_subprocess_output(exc)
        raise

def cli_scores():
    cmd = [bin_path, "rerank", model_dir, "--query", query, "--backend", "cuda"]
    for doc in docs:
        cmd.extend(["--doc", doc])
    raw = checked_output(cmd)
    return [entry["score"] for entry in parse_cli_json(raw)["scores"]]

def server_scores():
    body = json.dumps({"model": model_dir, "query": query, "prompts": docs}).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}/rerank",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        payload = json.loads(response.read().decode())
    rows = sorted(payload["data"], key=lambda item: item["index"])
    return [row["score"] for row in rows]

wait_ready()
cli = cli_scores()
server = server_scores()
max_diff = max(abs(a - b) for a, b in zip(cli, server))
print(f"server_parity docs={len(docs)} max_abs_diff={max_diff:.8f}")
if max_diff > tolerance:
    raise SystemExit(f"server/CLI CUDA max diff {max_diff:.8f} exceeds tolerance {tolerance:.8f}")
print("mxbai server rerank parity completed")
PY
fi

echo "mxbai reranker CUDA verification completed"
