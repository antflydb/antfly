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

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
cd "$repo_root"

default_models_root="${ANTFLY_INFERENCE_MODELS_DIR:-${HOME:+$HOME/.antfly/inference/models}}"
model_dir="${ANTFLY_GLINER2_MODEL_DIR:-${default_models_root:+$default_models_root/antflydb/gliner2-base-v1}}"
score_tolerance="${ANTFLY_GLINER2_SCORE_TOLERANCE:-0.01}"
command_timeout="${ANTFLY_GLINER2_COMMAND_TIMEOUT:-1200}"
warmup_iters="${ANTFLY_GLINER2_WARMUP_ITERS:-1}"
measure_iters="${ANTFLY_GLINER2_MEASURE_ITERS:-1}"
cuda_artifacts="${ANTFLY_CUDA_ARTIFACTS:-fatbin}"
cuda_libraries="${ANTFLY_CUDA_LIBS:-auto}"
optimize="${ANTFLY_CUDA_VERIFY_OPTIMIZE:-ReleaseFast}"
zig_global_cache_dir="${ZIG_GLOBAL_CACHE_DIR:-${TMPDIR:-/tmp}/antfly-zig-global-cache}"

resolve_zig() {
  if [[ -n "${ZIG:-}" ]]; then
    printf '%s\n' "$ZIG"
  elif command -v zig >/dev/null 2>&1; then
    command -v zig
  elif [[ -x "../../../.tools/zig-x86_64-linux-0.16.0/zig" ]]; then
    printf '%s\n' "../../../.tools/zig-x86_64-linux-0.16.0/zig"
  else
    echo "zig not found; set ZIG=/path/to/zig" >&2
    return 1
  fi
}

if [[ -z "$model_dir" ]]; then
  echo "set ANTFLY_GLINER2_MODEL_DIR, or set ANTFLY_INFERENCE_MODELS_DIR/HOME for the default model location" >&2
  exit 1
fi
if [[ ! -f "$model_dir/gliner_config.json" ]]; then
  echo "missing GLiNER config at $model_dir/gliner_config.json" >&2
  exit 1
fi
if [[ ! -f "$model_dir/gliner2-encoder.Q4_K.gguf" ]]; then
  echo "missing GLiNER encoder weights at $model_dir/gliner2-encoder.Q4_K.gguf" >&2
  exit 1
fi
if [[ ! -f "$model_dir/gliner2-head.Q4_K.gguf" ]]; then
  echo "missing GLiNER head weights at $model_dir/gliner2-head.Q4_K.gguf" >&2
  exit 1
fi

zig_bin="$(resolve_zig)"

python3 - "$zig_bin" "$model_dir" "$score_tolerance" "$command_timeout" "$warmup_iters" "$measure_iters" "$cuda_artifacts" "$cuda_libraries" "$optimize" "$zig_global_cache_dir" <<'PY'
import csv
import io
import math
import subprocess
import sys

zig_bin, model_dir, tolerance_raw, timeout_raw, warmup_iters, measure_iters, cuda_artifacts, cuda_libraries, optimize, zig_global_cache_dir = sys.argv[1:11]
tolerance = float(tolerance_raw)
timeout = float(timeout_raw)

cases = [
    {
        "name": "organizations_locations",
        "text": "John Smith works for Apple Inc. in San Francisco. Apple Inc. is based in Cupertino.",
        "labels": ["person", "organization", "location"],
    },
    {
        "name": "technical_entities",
        "text": "NVIDIA released CUDA 13.2 for Blackwell and Hopper production inference workloads.",
        "labels": ["company", "product", "hardware", "software"],
    },
]

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

def run_case(backend, case):
    cmd = [
        zig_bin,
        "build",
        "--global-cache-dir",
        zig_global_cache_dir,
        "-Dcuda=true",
        f"-Dcuda-artifacts={cuda_artifacts}",
        f"-Dcuda-libs={cuda_libraries}",
        f"-Doptimize={optimize}",
        "bench-gliner2-e2e",
        "--",
        "--model-dir",
        model_dir,
        "--backend",
        backend,
        "--task",
        "entities",
        "--text",
        case["text"],
        "--batch-size",
        "1",
        "--warmup-iters",
        warmup_iters,
        "--measure-iters",
        measure_iters,
        "--format",
        "csv",
    ]
    for label in case["labels"]:
        cmd.extend(["--label", label])

    raw = checked_output(cmd)
    csv_lines = []
    keep = False
    for line in raw.splitlines():
        if line.startswith("task,mode,"):
            csv_lines = [line]
            keep = True
            continue
        if keep and line.startswith("entities,"):
            csv_lines.append(line)
    if len(csv_lines) < 2:
        raise RuntimeError(f"{case['name']} {backend}: no CSV rows found:\n{raw}")
    rows = list(csv.DictReader(io.StringIO("\n".join(csv_lines))))
    warm = next((row for row in rows if row["mode"] == "warm_loaded_session"), rows[-1])
    entity_count = int(warm["entity_count"])
    score_sum = float(warm["score_sum"])
    if entity_count < 0 or not math.isfinite(score_sum):
        raise AssertionError(f"{case['name']} {backend}: invalid row {warm}")
    return {"entity_count": entity_count, "score_sum": score_sum}

for case in cases:
    native = run_case("native", case)
    cuda = run_case("cuda", case)
    score_diff = abs(native["score_sum"] - cuda["score_sum"])
    print(
        f"case={case['name']} native_entities={native['entity_count']} cuda_entities={cuda['entity_count']} score_diff={score_diff:.8f}",
        flush=True,
    )
    if native["entity_count"] != cuda["entity_count"]:
        raise SystemExit(f"{case['name']}: native/cuda entity counts differ: {native} vs {cuda}")
    if score_diff > tolerance:
        raise SystemExit(f"{case['name']}: native/cuda score diff {score_diff:.8f} exceeds tolerance {tolerance:.8f}")

print("gliner2 E2E native/cuda parity completed", flush=True)
PY
