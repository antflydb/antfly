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
umask 077

usage() {
  cat <<'USAGE'
usage: benchmark_florence2_metal_kernels.sh

Runs an exact-output A/B benchmark of the promoted Florence-2 Metal frame,
stage-frame, high-row Q4_K, decode-1x cross-attention, and window-attention
paths. The baseline disables all five paths; the candidate uses the production
defaults. Writes raw JSON, stderr logs, hardware.json, and a stable summary.json to
ANTFLY_FLORENCE2_OUT_DIR.

Environment overrides:
  ANTFLY_BIN                         prebuilt antfly-inference binary (skips build)
  ANTFLY_FLORENCE2_MODEL_DIR         Florence-2 GGUF model directory
  ANTFLY_FLORENCE2_IMAGE             input image
  ANTFLY_FLORENCE2_PROMPT            reader prompt (default: <MORE_DETAILED_CAPTION>)
  ANTFLY_FLORENCE2_MAX_TOKENS        maximum generated tokens (default: 32)
  ANTFLY_FLORENCE2_WARMUP_ITERS      warmup reads per case (default: 3)
  ANTFLY_FLORENCE2_MEASURE_ITERS     measured reads per case (default: 10)
  ANTFLY_FLORENCE2_MIN_P50_SPEEDUP   required baseline/candidate ratio (default: 1.05)
  ANTFLY_FLORENCE2_OUT_DIR           artifact directory (default: private temporary directory)
  ZIG                                Zig executable
  ZIG_LOCAL_CACHE_DIR                Zig local cache directory
  ZIG_GLOBAL_CACHE_DIR               Zig global cache directory
USAGE
}

case "${1:-}" in
  --help|-h)
    usage
    exit 0
    ;;
  "") ;;
  *)
    usage >&2
    exit 2
    ;;
esac

pkg_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
repo_root="$(cd "$pkg_root/../../.." && pwd)"
cd "$pkg_root"

workspace_model_dir="$repo_root/.models/antflydb/Florence-2-base"
cached_model_dir="${ANTFLY_INFERENCE_MODELS_DIR:-${HOME:+$HOME/.antfly/inference/models}}/antflydb/Florence-2-base"
if [[ -n "${ANTFLY_FLORENCE2_MODEL_DIR:-}" ]]; then
  model_dir="$ANTFLY_FLORENCE2_MODEL_DIR"
elif [[ -d "$workspace_model_dir" ]]; then
  model_dir="$workspace_model_dir"
else
  model_dir="$cached_model_dir"
fi

image_path="${ANTFLY_FLORENCE2_IMAGE:-$repo_root/zig/testdata/image/jpeg/upstream/libjpeg_turbo/testorig.jpg}"
prompt="${ANTFLY_FLORENCE2_PROMPT:-<MORE_DETAILED_CAPTION>}"
max_tokens="${ANTFLY_FLORENCE2_MAX_TOKENS:-32}"
warmup_iters="${ANTFLY_FLORENCE2_WARMUP_ITERS:-3}"
measure_iters="${ANTFLY_FLORENCE2_MEASURE_ITERS:-10}"
min_p50_speedup="${ANTFLY_FLORENCE2_MIN_P50_SPEEDUP:-1.05}"
out_dir="${ANTFLY_FLORENCE2_OUT_DIR:-}"
zig_local_cache_dir="${ZIG_LOCAL_CACHE_DIR:-${TMPDIR:-/tmp}/antfly-florence2-kernels-zig-local}"
zig_global_cache_dir="${ZIG_GLOBAL_CACHE_DIR:-${TMPDIR:-/tmp}/antfly-zig-global-cache}"

python3 - "$max_tokens" "$warmup_iters" "$measure_iters" "$min_p50_speedup" <<'PY'
import math
import sys

names = ("max tokens", "warmup iterations", "measure iterations")
for name, raw in zip(names, sys.argv[1:4]):
    try:
        value = int(raw)
    except ValueError:
        raise SystemExit(f"{name} must be a positive integer")
    if value <= 0:
        raise SystemExit(f"{name} must be a positive integer")
try:
    minimum = float(sys.argv[4])
except ValueError:
    raise SystemExit("minimum p50 speedup must be a finite positive number")
if not math.isfinite(minimum) or minimum <= 0:
    raise SystemExit("minimum p50 speedup must be a finite positive number")
PY

if [[ ! -d "$model_dir" ]]; then
  echo "missing Florence-2 model directory; set ANTFLY_FLORENCE2_MODEL_DIR" >&2
  exit 1
fi
if [[ ! -f "$image_path" ]]; then
  echo "missing benchmark image: $image_path" >&2
  exit 1
fi

if [[ -z "$out_dir" ]]; then
  out_dir="$(mktemp -d "${TMPDIR:-/tmp}/florence2-metal-kernels.XXXXXX")"
fi
mkdir -p "$out_dir"

if [[ -n "${ANTFLY_BIN:-}" ]]; then
  antfly_bin="$ANTFLY_BIN"
else
  if [[ -n "${ZIG:-}" ]]; then
    zig_bin="$ZIG"
  elif command -v zig >/dev/null 2>&1; then
    zig_bin="$(command -v zig)"
  else
    echo "zig not found; set ZIG=/path/to/zig" >&2
    exit 1
  fi
  env ZIG_LOCAL_CACHE_DIR="$zig_local_cache_dir" ZIG_GLOBAL_CACHE_DIR="$zig_global_cache_dir" \
    "$zig_bin" build -Dmetal=true -Donnx=false -Dcuda=false -Doptimize=ReleaseFast
  antfly_bin="$pkg_root/zig-out/bin/antfly-inference"
fi
if [[ ! -x "$antfly_bin" ]]; then
  echo "missing antfly-inference binary: $antfly_bin" >&2
  exit 1
fi

python3 - "$out_dir/hardware.json" <<'PY'
import json
import platform
import shutil
import subprocess
import sys

hardware = {"os_version": platform.mac_ver()[0]}
if shutil.which("system_profiler"):
    raw = subprocess.run(
        ["system_profiler", "SPHardwareDataType", "-json"],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    )
    payload = json.loads(raw.stdout)
    entries = payload.get("SPHardwareDataType", [])
    if entries:
        overview = entries[0]
        for key in ("chip_type", "machine_model", "machine_name", "number_processors", "physical_memory"):
            if key in overview:
                hardware[key] = overview[key]
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    json.dump(hardware, handle, indent=2, sort_keys=True)
    handle.write("\n")
PY

command=(
  "$antfly_bin" read "$model_dir" "$image_path"
  --backend metal
  --prompt "$prompt"
  --max-tokens "$max_tokens"
  --warmup-iters "$warmup_iters"
  --measure-iters "$measure_iters"
)

run_case() {
  local name="$1"
  printf 'running %s...\n' "$name" >&2
  if [[ "$name" == "baseline" ]]; then
    env \
      -u TERMITE_FLORENCE2_METAL_FRAMES \
      -u TERMITE_FLORENCE2_METAL_Q4_K_MM \
      -u TERMITE_FLORENCE2_METAL_STAGE_FRAMES \
      -u TERMITE_FLORENCE2_METAL_FUSED_LM_HEAD \
      TERMITE_FLORENCE2_METAL_DISABLE_FRAMES=1 \
      TERMITE_FLORENCE2_METAL_DISABLE_STAGE_FRAMES=1 \
      TERMITE_FLORENCE2_METAL_DISABLE_Q4_K_MM=1 \
      TERMITE_FLORENCE2_METAL_DISABLE_Q4_K_MM_MATRIX=1 \
      TERMITE_FLORENCE2_METAL_DISABLE_Q4_K_MM_NR4=1 \
      TERMITE_FLORENCE2_METAL_DISABLE_CROSS_ATTN_1X=1 \
      TERMITE_FLORENCE2_METAL_DISABLE_WINDOW_SDPA_1SG=1 \
      TERMITE_FLORENCE2_METAL_STRICT_RESIDENT=1 \
      ANTFLY_INFERENCE_READ_PROFILE=0 \
      "${command[@]}" >"$out_dir/$name.json" 2>"$out_dir/$name.stderr.log"
  else
    env \
      -u TERMITE_FLORENCE2_METAL_FRAMES \
      -u TERMITE_FLORENCE2_METAL_Q4_K_MM \
      -u TERMITE_FLORENCE2_METAL_STAGE_FRAMES \
      -u TERMITE_FLORENCE2_METAL_FUSED_LM_HEAD \
      -u TERMITE_FLORENCE2_METAL_DISABLE_FRAMES \
      -u TERMITE_FLORENCE2_METAL_DISABLE_STAGE_FRAMES \
      -u TERMITE_FLORENCE2_METAL_DISABLE_Q4_K_MM \
      -u TERMITE_FLORENCE2_METAL_DISABLE_Q4_K_MM_MATRIX \
      -u TERMITE_FLORENCE2_METAL_DISABLE_Q4_K_MM_NR4 \
      -u TERMITE_FLORENCE2_METAL_DISABLE_CROSS_ATTN_1X \
      -u TERMITE_FLORENCE2_METAL_DISABLE_WINDOW_SDPA_1SG \
      TERMITE_FLORENCE2_METAL_STRICT_RESIDENT=1 \
      ANTFLY_INFERENCE_READ_PROFILE=0 \
      "${command[@]}" >"$out_dir/$name.json" 2>"$out_dir/$name.stderr.log"
  fi
}

run_case baseline
run_case candidate

python3 - \
  "$out_dir/baseline.json" \
  "$out_dir/candidate.json" \
  "$out_dir/hardware.json" \
  "$out_dir/summary.json" \
  "$repo_root" \
  "$antfly_bin" \
  "$model_dir" \
  "$image_path" \
  "$prompt" \
  "$max_tokens" \
  "$warmup_iters" \
  "$measure_iters" \
  "$min_p50_speedup" <<'PY'
import datetime
import hashlib
import json
import math
import os
import pathlib
import subprocess
import sys

(
    baseline_path,
    candidate_path,
    hardware_path,
    summary_path,
    repo_root,
    binary_path,
    model_dir,
    image_path,
    prompt,
    max_tokens_raw,
    warmup_raw,
    measure_raw,
    min_speedup_raw,
) = sys.argv[1:]


def load(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def sha256(path):
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def artifact(path):
    value = pathlib.Path(path).resolve()
    return {"path": str(value), "bytes": value.stat().st_size, "sha256": sha256(value)}


def git(*args):
    return subprocess.run(
        ["git", "-C", repo_root, *args],
        check=True,
        stdout=subprocess.PIPE,
        text=True,
    ).stdout.rstrip("\n")


baseline = load(baseline_path)
candidate = load(candidate_path)
warmup_iters = int(warmup_raw)
measure_iters = int(measure_raw)
for name, payload in (("baseline", baseline), ("candidate", candidate)):
    if payload.get("backend") != "metal":
        raise SystemExit(f"{name}: expected backend='metal'")
    if payload.get("mode") != "warm_read":
        raise SystemExit(f"{name}: expected mode='warm_read'")
    if payload.get("resident_decoder") is not True:
        raise SystemExit(f"{name}: expected resident_decoder=true")
    if payload.get("iterations_consistent") is not True:
        raise SystemExit(f"{name}: measured iterations were inconsistent")
    if payload.get("warmup_iters") != warmup_iters or payload.get("measure_iters") != measure_iters:
        raise SystemExit(f"{name}: iteration metadata does not match the request")
    for key in ("avg_ms", "p50_ms", "p95_ms", "min_ms", "max_ms"):
        value = payload.get(key)
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
            raise SystemExit(f"{name}: invalid {key}={value!r}")

parity_keys = ("last_text", "generated_tokens")
for key in parity_keys:
    if baseline.get(key) != candidate.get(key):
        raise SystemExit(
            f"exact-output parity failed for {key}: "
            f"baseline={baseline.get(key)!r} candidate={candidate.get(key)!r}"
        )
if not baseline.get("last_text"):
    raise SystemExit("benchmark produced empty reader output")
generated_tokens = baseline.get("generated_tokens")
if not isinstance(generated_tokens, int) or generated_tokens < 4:
    raise SystemExit(f"benchmark requires at least 4 generated tokens, got {generated_tokens!r}")

candidate_generated = candidate.get("metal_generated_q4_k")
if not isinstance(candidate_generated, int) or candidate_generated <= 0:
    raise SystemExit(
        "candidate did not dispatch the Florence high-row Q4_K path: "
        f"metal_generated_q4_k={candidate_generated!r}"
    )
baseline_generated = baseline.get("metal_generated_q4_k")
if not isinstance(baseline_generated, int) or candidate_generated <= baseline_generated:
    raise SystemExit(
        "candidate did not add Florence high-row Q4_K dispatches: "
        f"baseline={baseline_generated!r} candidate={candidate_generated!r}"
    )

p50_speedup = baseline["p50_ms"] / candidate["p50_ms"]
avg_speedup = baseline["avg_ms"] / candidate["avg_ms"]
minimum = float(min_speedup_raw)
if p50_speedup < minimum:
    raise SystemExit(f"p50 speedup {p50_speedup:.3f}x below required {minimum:.3f}x")

model_artifacts = []
for path in sorted(pathlib.Path(model_dir).iterdir()):
    if path.is_file() and (path.suffix == ".gguf" or path.name.endswith(".json")):
        model_artifacts.append(artifact(path))
if not any(item["path"].endswith(".gguf") for item in model_artifacts):
    raise SystemExit(f"no GGUF weights found in {model_dir}")

summary = {
    "schema": "florence2-metal-kernel-ab/v1",
    "created_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
    "result": "pass",
    "exact_output_parity": True,
    "speedup": {"avg": avg_speedup, "p50": p50_speedup, "minimum_p50": minimum},
    "workload": {
        "prompt": prompt,
        "max_tokens": int(max_tokens_raw),
        "warmup_iters": warmup_iters,
        "measure_iters": measure_iters,
        "strict_resident": True,
    },
    "features": {
        "baseline": {
            "frames": False,
            "stage_frames": False,
            "florence_high_row_q4_k": False,
            "florence_q4_k_matrix": False,
            "florence_q4_k_nr4": False,
            "decode_cross_attention_1x": False,
            "window_sdpa_1sg": False,
        },
        "candidate": {
            "frames": True,
            "stage_frames": True,
            "florence_high_row_q4_k": True,
            "florence_q4_k_matrix": True,
            "florence_q4_k_nr4": False,
            "decode_cross_attention_1x": True,
            "window_sdpa_1sg": True,
        },
        "fused_lm_head": False,
    },
    "provenance": {
        "git_head": git("rev-parse", "HEAD"),
        "git_status_porcelain": git("status", "--short"),
        "binary": artifact(binary_path),
        "image": artifact(image_path),
        "model_artifacts": model_artifacts,
        "hardware": load(hardware_path),
    },
    "baseline": baseline,
    "candidate": candidate,
}
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(summary, handle, ensure_ascii=False, indent=2, sort_keys=True)
    handle.write("\n")

print("Florence-2 Metal kernel A/B: PASS")
print(f"baseline:  avg_ms={baseline['avg_ms']:.3f} p50_ms={baseline['p50_ms']:.3f}")
print(f"candidate: avg_ms={candidate['avg_ms']:.3f} p50_ms={candidate['p50_ms']:.3f}")
print(f"speedup:   avg={avg_speedup:.3f}x p50={p50_speedup:.3f}x")
print(f"summary:   {summary_path}")
PY
