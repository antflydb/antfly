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
usage: verify_florence2_metal_kv_cache.sh

Benchmarks Florence-2 Metal reads with the default KV cache and with
ANTFLY_INFERENCE_FLORENCE_DISABLE_KV_CACHE=1, then requires exact output parity.
Writes cached.json, full_sequence.json, stderr logs, and summary.json to OUT_DIR.

Environment overrides:
  ANTFLY_BIN                         prebuilt antfly-inference binary (skips build)
  ANTFLY_FLORENCE2_MODEL_DIR         Florence-2 GGUF model directory
  ANTFLY_FLORENCE2_IMAGE             input image
  ANTFLY_FLORENCE2_PROMPT            reader prompt (default: <MORE_DETAILED_CAPTION>)
  ANTFLY_FLORENCE2_MAX_TOKENS        maximum generated tokens (default: 64)
  ANTFLY_FLORENCE2_WARMUP_ITERS      warmup reads per case (default: 1)
  ANTFLY_FLORENCE2_MEASURE_ITERS     measured reads per case (default: 3)
  ANTFLY_FLORENCE2_OPTIMIZE          Zig optimization mode (default: ReleaseSafe)
  ANTFLY_FLORENCE2_OUT_DIR           artifact directory (default: private temporary directory)
  ANTFLY_FLORENCE2_STRICT_RESIDENT   require resident Metal ops (default: 1)
  ANTFLY_FLORENCE2_MIN_KV_SPEEDUP    minimum full/cache p50 ratio (default: 1.05)
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

pkg_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
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

image_path="${ANTFLY_FLORENCE2_IMAGE:-$repo_root/zig/testdata/image/jpeg/upstream/libjpeg_turbo/testorig.jpg}"
prompt="${ANTFLY_FLORENCE2_PROMPT:-<MORE_DETAILED_CAPTION>}"
max_tokens="${ANTFLY_FLORENCE2_MAX_TOKENS:-64}"
warmup_iters="${ANTFLY_FLORENCE2_WARMUP_ITERS:-1}"
measure_iters="${ANTFLY_FLORENCE2_MEASURE_ITERS:-3}"
optimize="${ANTFLY_FLORENCE2_OPTIMIZE:-ReleaseSafe}"
strict_resident="${ANTFLY_FLORENCE2_STRICT_RESIDENT:-1}"
min_kv_speedup="${ANTFLY_FLORENCE2_MIN_KV_SPEEDUP:-1.05}"
out_dir="${ANTFLY_FLORENCE2_OUT_DIR:-}"
zig_local_cache_dir="${ZIG_LOCAL_CACHE_DIR:-${TMPDIR:-/tmp}/antfly-florence2-metal-kv-zig-local}"
zig_global_cache_dir="${ZIG_GLOBAL_CACHE_DIR:-${TMPDIR:-/tmp}/antfly-zig-global-cache}"

python3 - "$min_kv_speedup" <<'PY'
import math
import sys

try:
    minimum = float(sys.argv[1])
except ValueError:
    raise SystemExit("ANTFLY_FLORENCE2_MIN_KV_SPEEDUP must be a finite positive number")
if not math.isfinite(minimum) or minimum <= 0:
    raise SystemExit("ANTFLY_FLORENCE2_MIN_KV_SPEEDUP must be a finite positive number")
PY

resolve_zig() {
  if [[ -n "${ZIG:-}" ]]; then
    printf '%s\n' "$ZIG"
  elif command -v zig >/dev/null 2>&1; then
    command -v zig
  else
    echo "zig not found; set ZIG=/path/to/zig" >&2
    return 1
  fi
}

if [[ -z "$model_dir" || ! -d "$model_dir" ]]; then
  echo "missing Florence-2 model directory; set ANTFLY_FLORENCE2_MODEL_DIR" >&2
  exit 1
fi
has_gguf=0
for gguf_file in "$model_dir"/*.gguf; do
  if [[ -f "$gguf_file" ]]; then
    has_gguf=1
    break
  fi
done
if (( ! has_gguf )); then
  echo "missing Florence-2 GGUF weights in $model_dir" >&2
  exit 1
fi
for file_name in config.json model_manifest.json tokenizer.json tokenizer_config.json vocab.json preprocessor_config.json; do
  if [[ ! -f "$model_dir/$file_name" ]]; then
    echo "incomplete Florence-2 bundle: missing $model_dir/$file_name" >&2
    exit 1
  fi
done
if [[ ! -f "$model_dir/antfly_inference_bundle.json" && ! -f "$model_dir/antfly_inference_variants.json" ]]; then
  echo "incomplete Florence-2 bundle: missing Antfly bundle or variants manifest in $model_dir" >&2
  exit 1
fi
if [[ ! -f "$image_path" ]]; then
  echo "missing test image: $image_path" >&2
  exit 1
fi

if [[ -z "$out_dir" ]]; then
  out_dir="$(mktemp -d "${TMPDIR:-/tmp}/florence2-metal-kv.XXXXXX")"
fi
mkdir -p "$out_dir"
if [[ -n "${ANTFLY_BIN:-}" ]]; then
  antfly_bin="$ANTFLY_BIN"
else
  zig_bin="$(resolve_zig)"
  env ZIG_LOCAL_CACHE_DIR="$zig_local_cache_dir" ZIG_GLOBAL_CACHE_DIR="$zig_global_cache_dir" \
    "$zig_bin" build -Dmetal=true -Doptimize="$optimize"
  antfly_bin="$pkg_root/zig-out/bin/antfly-inference"
fi
if [[ ! -x "$antfly_bin" ]]; then
  echo "missing antfly-inference binary: $antfly_bin" >&2
  exit 1
fi

run_case() {
  local name="$1"
  local disable_kv_cache="$2"
  local json_file="$out_dir/$name.json"
  local stderr_file="$out_dir/$name.stderr.log"
  local -a command=(
    "$antfly_bin" read "$model_dir" "$image_path"
    --backend metal
    --prompt "$prompt"
    --max-tokens "$max_tokens"
    --warmup-iters "$warmup_iters"
    --measure-iters "$measure_iters"
  )

  printf 'running %s...\n' "$name" >&2
  if [[ "$disable_kv_cache" == "1" ]]; then
    env ANTFLY_INFERENCE_FLORENCE_DISABLE_KV_CACHE=1 \
      TERMITE_FLORENCE2_METAL_STRICT_RESIDENT="$strict_resident" \
      ANTFLY_INFERENCE_READ_PROFILE="${ANTFLY_INFERENCE_READ_PROFILE:-0}" \
      "${command[@]}" >"$json_file" 2>"$stderr_file"
  else
    env -u ANTFLY_INFERENCE_FLORENCE_DISABLE_KV_CACHE \
      TERMITE_FLORENCE2_METAL_STRICT_RESIDENT="$strict_resident" \
      ANTFLY_INFERENCE_READ_PROFILE="${ANTFLY_INFERENCE_READ_PROFILE:-0}" \
      "${command[@]}" >"$json_file" 2>"$stderr_file"
  fi
}

run_case cached 0
run_case full_sequence 1

python3 - \
  "$out_dir/cached.json" \
  "$out_dir/full_sequence.json" \
  "$out_dir/summary.json" \
  "$min_kv_speedup" \
  "$warmup_iters" \
  "$measure_iters" <<'PY'
import json
import math
import sys

cached_path, full_path, summary_path, min_speedup_raw, warmup_raw, measure_raw = sys.argv[1:]


def load(path):
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


cached = load(cached_path)
full = load(full_path)

warmup_iters = int(warmup_raw)
measure_iters = int(measure_raw)
for name, payload in (("cached", cached), ("full_sequence", full)):
    if payload.get("backend") != "metal":
        raise SystemExit(f"{name}: expected backend='metal', got {payload.get('backend')!r}")
    if payload.get("mode") != "warm_read":
        raise SystemExit(f"{name}: expected mode='warm_read', got {payload.get('mode')!r}")
    if payload.get("resident_decoder") is not True:
        raise SystemExit(f"{name}: expected resident_decoder=true")
    if payload.get("warmup_iters") != warmup_iters or payload.get("measure_iters") != measure_iters:
        raise SystemExit(f"{name}: benchmark iteration metadata does not match the request")
    if payload.get("iterations_consistent") is not True:
        raise SystemExit(f"{name}: benchmark did not verify measured-iteration consistency")
    for key in ("avg_ms", "p50_ms", "p95_ms", "min_ms", "max_ms"):
        value = payload.get(key)
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise SystemExit(f"{name}: expected finite non-negative {key}, got {value!r}")
    if payload["avg_ms"] <= 0 or payload["p50_ms"] <= 0:
        raise SystemExit(f"{name}: expected positive avg_ms and p50_ms")

if cached.get("kv_cache") is not True:
    raise SystemExit(f"cached: expected kv_cache=true, got {cached.get('kv_cache')!r}")
if cached.get("kv_cache_mode") not in ("preallocated", "concat"):
    raise SystemExit(f"cached: unexpected kv_cache_mode={cached.get('kv_cache_mode')!r}")
if full.get("kv_cache") is not False:
    raise SystemExit(f"full_sequence: expected kv_cache=false, got {full.get('kv_cache')!r}")
if full.get("kv_cache_mode") is not None:
    raise SystemExit(f"full_sequence: expected kv_cache_mode=null, got {full.get('kv_cache_mode')!r}")

generated_tokens = cached.get("generated_tokens")
if not isinstance(generated_tokens, int) or generated_tokens < 0:
    raise SystemExit(f"cached: expected non-negative generated_tokens, got {generated_tokens!r}")
if generated_tokens != full.get("generated_tokens"):
    raise SystemExit(
        "generated-token count changed with the KV cache: "
        f"cached={cached.get('generated_tokens')!r} full={full.get('generated_tokens')!r}"
    )
if generated_tokens < 4:
    raise SystemExit(
        f"benchmark requires at least 4 generated tokens, got {generated_tokens}; "
        "use a representative image/prompt or increase max tokens"
    )
if not isinstance(cached.get("last_text"), str) or cached.get("last_text") != full.get("last_text"):
    raise SystemExit(
        "reader output changed with the KV cache:\n"
        f"cached={cached.get('last_text')!r}\n"
        f"full={full.get('last_text')!r}"
    )

avg_speedup = full["avg_ms"] / cached["avg_ms"]
p50_speedup = full["p50_ms"] / cached["p50_ms"]
minimum_speedup = float(min_speedup_raw)
if p50_speedup < minimum_speedup:
    raise SystemExit(f"p50 KV-cache speedup {p50_speedup:.3f} below required {minimum_speedup:.3f}")

summary = {
    "output_parity": True,
    "generated_tokens": cached.get("generated_tokens"),
    "minimum_p50_speedup": minimum_speedup,
    "avg_speedup": avg_speedup,
    "p50_speedup": p50_speedup,
    "cached": cached,
    "full_sequence": full,
}
with open(summary_path, "w", encoding="utf-8") as handle:
    json.dump(summary, handle, ensure_ascii=False, indent=2, sort_keys=True)
    handle.write("\n")

print("Florence-2 Metal KV-cache parity: PASS")
print(
    f"cached:        avg_ms={cached['avg_ms']:.3f} p50_ms={cached['p50_ms']:.3f} "
    f"tokens={cached.get('generated_tokens')!r} mode={cached.get('kv_cache_mode')!r}"
)
print(f"full_sequence: avg_ms={full['avg_ms']:.3f} p50_ms={full['p50_ms']:.3f}")
print(f"speedup:       avg={avg_speedup:.3f}x p50={p50_speedup:.3f}x")
print(f"summary:       {summary_path}")
PY
