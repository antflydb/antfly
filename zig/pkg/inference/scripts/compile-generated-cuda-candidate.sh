#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 2 ]; then
  printf 'usage: %s GENERATED_SOURCE OUTPUT_CUBIN\n' "$0" >&2
  exit 2
fi

src="$1"
out="$2"

case "$src" in
  *src/ops/cuda/generated/attention_decode_score_prework_hd256.cu|\
  *src/ops/cuda/generated/attention_decode_score_prework_hd512.cu) ;;
  *)
    printf 'error: unsupported generated CUDA candidate: %s\n' "$src" >&2
    exit 1
    ;;
esac

if ! grep -q '^// production_enabled=false$' "$src"; then
  printf 'error: candidate source is missing its default-off promotion marker: %s\n' "$src" >&2
  exit 1
fi

cuda_home="${CUDA_HOME:-}"
if [ -n "${NVCC:-}" ]; then
  nvcc="$NVCC"
elif [ -n "$cuda_home" ] && [ -x "$cuda_home/bin/nvcc" ]; then
  nvcc="$cuda_home/bin/nvcc"
elif [ -x /usr/local/cuda-13.2/bin/nvcc ]; then
  nvcc=/usr/local/cuda-13.2/bin/nvcc
else
  nvcc="$(command -v nvcc || true)"
fi

if [ -z "$nvcc" ]; then
  printf 'error: nvcc not found. Set CUDA_HOME or NVCC to a CUDA 13.2 toolkit.\n' >&2
  exit 1
fi

version="$($nvcc --version)"
case "$version" in
  *"release 13.2"*) ;;
  *)
    printf 'error: expected CUDA toolkit 13.2 for generated candidate checks.\n' >&2
    printf '%s\n' "$version" >&2
    exit 1
    ;;
esac

mkdir -p "$(dirname "$out")"
"$nvcc" -cubin -arch=sm_89 -diag-suppress 550 "$src" -o "$out"
