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
model_dir="${ANTFLY_CLIPCLAP_MODEL_DIR:-${default_models_root:+$default_models_root/antflydb/clipclap}}"
tolerance="${ANTFLY_CLIPCLAP_EMBED_TOLERANCE:-0.05}"
min_cosine="${ANTFLY_CLIPCLAP_MIN_COSINE:-0.985}"
command_timeout="${ANTFLY_CLIPCLAP_COMMAND_TIMEOUT:-900}"
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
  echo "set ANTFLY_CLIPCLAP_MODEL_DIR, or set ANTFLY_INFERENCE_MODELS_DIR/HOME for the default model location" >&2
  exit 1
fi
if [[ ! -f "$model_dir/model_manifest.json" ]]; then
  echo "missing model manifest at $model_dir/model_manifest.json" >&2
  exit 1
fi
if [[ ! -f "$model_dir/tokenizer.json" ]]; then
  echo "missing tokenizer at $model_dir/tokenizer.json" >&2
  exit 1
fi

zig_bin="$(resolve_zig)"
"$zig_bin" build --global-cache-dir "$zig_global_cache_dir" -Dcuda=true -Dcuda-artifacts="$cuda_artifacts" -Dcuda-libs="$cuda_libraries" -Doptimize="$optimize"

bin="./zig-out/bin/antfly-inference"

python3 - "$bin" "$model_dir" "$tolerance" "$min_cosine" "$command_timeout" <<'PY'
import json
import math
import subprocess
import sys

bin_path, model_dir, tolerance_raw, min_cosine_raw, timeout_raw = sys.argv[1:6]
tolerance = float(tolerance_raw)
min_cosine = float(min_cosine_raw)
timeout = float(timeout_raw)

cases = [
    {
        "name": "text_semantic",
        "texts": [
            "a photo of a document with audio metadata",
            "a clean product screenshot with charts and labels",
        ],
    },
    {
        "name": "punctuation_numbers",
        "texts": [
            "CUDA 13.2 kernel parity: fp16, bf16, and quantized projections.",
            "invoice #A-1049, total $42.17, due Friday",
        ],
    },
]

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

def checked_output(cmd):
    try:
        return subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=timeout)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
        print_subprocess_output(exc)
        raise

def run_cli(backend, texts):
    cmd = [bin_path, "embed", model_dir, "--backend", backend]
    for text in texts:
        cmd.extend(["--text", text])
    raw = checked_output(cmd)
    payload = parse_cli_json(raw)
    embeddings = payload.get("embeddings")
    if not isinstance(embeddings, list):
        raise AssertionError(f"{backend}: missing embeddings")
    if len(embeddings) != len(texts):
        raise AssertionError(f"{backend}: expected {len(texts)} embeddings, got {len(embeddings)}")
    for i, emb in enumerate(embeddings):
        if not emb:
            raise AssertionError(f"{backend}: empty embedding {i}")
        if any(not math.isfinite(v) for v in emb):
            raise AssertionError(f"{backend}: non-finite embedding {i}")
    return embeddings

for case in cases:
    native = run_cli("native", case["texts"])
    cuda = run_cli("cuda", case["texts"])
    if [len(v) for v in native] != [len(v) for v in cuda]:
        raise SystemExit(f"{case['name']}: native/cuda dimensions differ")
    max_diff = max(
        abs(a - b)
        for native_emb, cuda_emb in zip(native, cuda)
        for a, b in zip(native_emb, cuda_emb)
    )
    cosines = []
    for native_emb, cuda_emb in zip(native, cuda):
        dot = sum(a * b for a, b in zip(native_emb, cuda_emb))
        native_norm = math.sqrt(sum(a * a for a in native_emb))
        cuda_norm = math.sqrt(sum(b * b for b in cuda_emb))
        cosines.append(dot / (native_norm * cuda_norm))
    min_case_cosine = min(cosines)
    print(f"case={case['name']} embeddings={len(cuda)} dim={len(cuda[0])} max_abs_diff={max_diff:.8f} min_cosine={min_case_cosine:.8f}", flush=True)
    if max_diff > tolerance:
        raise SystemExit(f"{case['name']}: native/cuda max diff {max_diff:.8f} exceeds tolerance {tolerance:.8f}")
    if min_case_cosine < min_cosine:
        raise SystemExit(f"{case['name']}: native/cuda cosine {min_case_cosine:.8f} below minimum {min_cosine:.8f}")

print("clipclap CLI native/cuda parity completed", flush=True)
PY
