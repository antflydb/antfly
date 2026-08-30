#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
inference_dir="$(cd "$script_dir/../.." && pwd)"
repo_dir="$(cd "$inference_dir/../../.." && pwd)"
zig_bin="${ZIG_BIN:-$repo_dir/.tools/zig-x86_64-linux-0.17.0-dev.1933+e19a73c2d/zig}"
default_binary="$inference_dir/zig-out/bin/antfly-inference"
binary="${ANTFLY_INFERENCE_BINARY:-$default_binary}"
optimize="${OPTIMIZE:-ReleaseFast}"
artifact_dir="${ARTIFACT_DIR:-$inference_dir/src/ops/cuda/artifacts}"
warmups="${WARMUPS:-20}"
measure="${MEASURE:-200}"
repeats="${REPEATS:-5}"

if [[ ! -x "$zig_bin" ]]; then
  printf 'error: Zig compiler is not executable: %s\n' "$zig_bin" >&2
  exit 1
fi
(
  cd "$inference_dir"
  "$zig_bin" build quant-kernel-codegen -- --check
)
"$script_dir/../regen-cuda-artifacts.sh" --check --sm89

if [[ ! -f "$artifact_dir/inference_cuda_kernels_sm89.cubin" ]]; then
  printf 'error: canonical SM89 artifact missing: %s\n' "$artifact_dir/inference_cuda_kernels_sm89.cubin" >&2
  printf 'regenerate only via: %s --write --sm89\n' "$script_dir/../regen-cuda-artifacts.sh" >&2
  exit 1
fi

if [[ -z "${ANTFLY_INFERENCE_BINARY+x}" ]]; then
  printf 'building CUDA benchmark binary (%s): %s\n' "$optimize" "$default_binary"
  (
    cd "$inference_dir"
    "$zig_bin" build -Dcuda=true -Dmetal=false -Dcuda-artifacts=sm89 -Doptimize="$optimize"
  )
else
  if [[ ! -x "$binary" ]]; then
    printf 'error: custom inference binary is not executable: %s\n' "$binary" >&2
    exit 1
  fi
  stale_input=""
  while IFS= read -r -d '' input; do
    if [[ -z "$stale_input" && "$input" -nt "$binary" ]]; then
      stale_input="$input"
    fi
  done < <(find "$inference_dir/src" "$inference_dir/build" "$inference_dir/build.zig" "$inference_dir/build.zig.zon" -type f -print0)
  if [[ -n "$stale_input" ]]; then
    printf 'error: custom inference binary is stale: %s\n' "$binary" >&2
    printf 'newer build input: %s\n' "$stale_input" >&2
    printf 'rebuild the custom binary or unset ANTFLY_INFERENCE_BINARY to let this script build one\n' >&2
    exit 1
  fi
fi

if [[ ! -x "$binary" ]]; then
  printf 'error: inference binary is not executable: %s\n' "$binary" >&2
  exit 1
fi

cd "$repo_dir"
exec "$binary" bench-cuda \
  --q4-0-q8-1-e2b-ffn-sm89 "$artifact_dir" \
  --warmup-iters "$warmups" \
  --measure-iters "$measure" \
  --repeat-runs "$repeats"
