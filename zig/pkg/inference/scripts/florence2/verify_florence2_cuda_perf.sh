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

pkg_root="$(cd "$(dirname "$0")/../.." && pwd)"
repo_root="$(cd "$pkg_root/../../.." && pwd)"
cd "$pkg_root"

workspace_model_dir="$repo_root/.models/antflydb/Florence-2-base"
default_models_root="${ANTFLY_INFERENCE_MODELS_DIR:-${HOME:+$HOME/.antfly/inference/models}}"
cache_model_dir="${default_models_root:+$default_models_root/antflydb/Florence-2-base}"
if [[ -n "${ANTFLY_FLORENCE2_MODEL_DIR:-}" ]]; then
  model_dir="$ANTFLY_FLORENCE2_MODEL_DIR"
elif [[ -d "$workspace_model_dir" ]]; then
  model_dir="$workspace_model_dir"
else
  model_dir="$cache_model_dir"
fi

image_path="${ANTFLY_FLORENCE2_IMAGE:-$repo_root/zig/testdata/image/png/basic/red-2x2.png}"
prompt="${ANTFLY_FLORENCE2_PROMPT:-<CAPTION>}"
max_tokens="${ANTFLY_FLORENCE2_MAX_TOKENS:-32}"
warmup_iters="${ANTFLY_FLORENCE2_WARMUP_ITERS:-2}"
measure_iters="${ANTFLY_FLORENCE2_MEASURE_ITERS:-5}"
command_timeout="${ANTFLY_FLORENCE2_COMMAND_TIMEOUT:-900}"
json_file="${ANTFLY_FLORENCE2_JSON_TIMING:-${TMPDIR:-/tmp}/florence2-cuda-read.json}"
optimize="${ANTFLY_FLORENCE2_OPTIMIZE:-ReleaseSafe}"
cuda_artifacts="${ANTFLY_CUDA_ARTIFACTS:-fatbin}"
cuda_libraries="${ANTFLY_CUDA_LIBS:-auto}"
zig_global_cache_dir="${ZIG_GLOBAL_CACHE_DIR:-${TMPDIR:-/tmp}/antfly-zig-global-cache}"

resolve_zig() {
  if [[ -n "${ZIG:-}" ]]; then
    printf '%s\n' "$ZIG"
  elif command -v zig >/dev/null 2>&1; then
    command -v zig
  elif [[ -x "$repo_root/.tools/zig-x86_64-linux-0.17.0-dev.1936+5a625d5f3/zig" ]]; then
    printf '%s\n' "$repo_root/.tools/zig-x86_64-linux-0.17.0-dev.1936+5a625d5f3/zig"
  else
    echo "zig not found; set ZIG=/path/to/zig" >&2
    return 1
  fi
}

if [[ -z "$model_dir" || ! -d "$model_dir" ]]; then
  echo "missing Florence-2 model directory; set ANTFLY_FLORENCE2_MODEL_DIR" >&2
  exit 1
fi
if [[ -z "$(find "$model_dir" -maxdepth 1 -type f -name '*.gguf' -print -quit 2>/dev/null)" && ! -f "$model_dir/model.gguf" ]]; then
  echo "missing Florence-2 GGUF weights in $model_dir" >&2
  exit 1
fi
if [[ ! -f "$model_dir/tokenizer.json" ]]; then
  echo "missing tokenizer at $model_dir/tokenizer.json" >&2
  exit 1
fi
if [[ ! -f "$image_path" ]]; then
  echo "missing test image: $image_path" >&2
  exit 1
fi

zig_bin="$(resolve_zig)"
"$zig_bin" build --global-cache-dir "$zig_global_cache_dir" -Dcuda=true -Dcuda-artifacts="$cuda_artifacts" -Dcuda-libs="$cuda_libraries" -Doptimize="$optimize"

bin="$pkg_root/zig-out/bin/antfly-inference"
ANTFLY_INFERENCE_READ_PROFILE="${ANTFLY_INFERENCE_READ_PROFILE:-0}" timeout "$command_timeout" "$bin" read "$model_dir" "$image_path" \
  --backend cuda \
  --prompt "$prompt" \
  --max-tokens "$max_tokens" \
  --warmup-iters "$warmup_iters" \
  --measure-iters "$measure_iters" \
  --json-timing "$json_file"

python3 - "$json_file" "${ANTFLY_FLORENCE2_MAX_P50_MS:-}" <<'PY'
import json
import math
import sys

json_file, max_p50_raw = sys.argv[1:3]
with open(json_file, "r", encoding="utf-8") as f:
    payload = json.load(f)

required_true = ["resident_decoder", "kv_cache"]
for key in required_true:
    if payload.get(key) is not True:
        raise SystemExit(f"expected {key}=true, got {payload.get(key)!r}")

if payload.get("kv_cache_mode") != "preallocated":
    raise SystemExit(f"expected kv_cache_mode=preallocated, got {payload.get('kv_cache_mode')!r}")

if payload.get("backend") != "cuda":
    raise SystemExit(f"expected backend=cuda, got {payload.get('backend')!r}")

generated_tokens = payload.get("generated_tokens")
if not isinstance(generated_tokens, int) or generated_tokens <= 0:
    raise SystemExit(f"expected generated_tokens > 0, got {generated_tokens!r}")

p50_ms = payload.get("p50_ms")
if not isinstance(p50_ms, (int, float)) or not math.isfinite(p50_ms):
    raise SystemExit(f"expected finite p50_ms, got {p50_ms!r}")

if max_p50_raw:
    max_p50_ms = float(max_p50_raw)
    if p50_ms > max_p50_ms:
        raise SystemExit(f"p50_ms {p50_ms:.3f} exceeded budget {max_p50_ms:.3f}")

fallback = payload.get("cuda_graph_fallback_reason")
print(
    "florence2 cuda read ok: "
    f"p50_ms={p50_ms:.3f} avg_ms={payload.get('avg_ms'):.3f} "
    f"generated_tokens={generated_tokens} lm_head={payload.get('lm_head_path')!r} "
    f"graph_replay={payload.get('cuda_graph_replay')!r} graph_fallback={fallback!r} "
    f"json={json_file}"
)
PY
