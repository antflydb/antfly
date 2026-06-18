#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'usage: %s [--check|--write] [--portable|--fatbin|--all]\n' "$0"
  printf 'Regenerates checked-in CUDA artifacts with the pinned CUDA 13.2 toolkit contract.\n'
}

mode="check"
artifact_mode="all"
while [ $# -gt 0 ]; do
  case "$1" in
    --help|-h)
      usage
      exit 0
      ;;
    --check)
      mode="check"
      ;;
    --write)
      mode="write"
      ;;
    --portable)
      artifact_mode="portable"
      ;;
    --fatbin)
      artifact_mode="fatbin"
      ;;
    --all)
      artifact_mode="all"
      ;;
    *)
      usage
      exit 2
      ;;
  esac
  shift
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pkg_dir="$(cd "$script_dir/.." && pwd)"
src="$pkg_dir/src/ops/cuda/artifacts/inference_cuda_kernels.cu"
ptx_dst="$pkg_dir/src/ops/cuda/artifacts/inference_cuda_kernels.ptx"
fatbin_dst="$pkg_dir/src/ops/cuda/artifacts/inference_cuda_kernels.fatbin"

cuda_home="${CUDA_HOME:-}"

if [ -n "${NVCC:-}" ]; then
  nvcc="$NVCC"
elif [ -n "$cuda_home" ] && [ -x "$cuda_home/bin/nvcc" ]; then
  nvcc="$cuda_home/bin/nvcc"
elif [ -x /usr/local/cuda-13.2/bin/nvcc ]; then
  nvcc="/usr/local/cuda-13.2/bin/nvcc"
else
  nvcc="$(command -v nvcc || true)"
fi

if [ -z "$nvcc" ]; then
  printf 'error: nvcc not found. Set CUDA_HOME or NVCC to a CUDA 13.2 toolkit.\n' >&2
  exit 1
fi

version="$("$nvcc" --version)"
if ! printf '%s\n' "$version" | grep -q 'release 13\.2'; then
  printf 'error: expected CUDA toolkit 13.2 for artifact regeneration.\n' >&2
  printf '%s\n' "$version" >&2
  exit 1
fi

cuobjdump=""
if [ -n "${CUOBJDUMP:-}" ]; then
  cuobjdump="$CUOBJDUMP"
elif [ -n "$cuda_home" ] && [ -x "$cuda_home/bin/cuobjdump" ]; then
  cuobjdump="$cuda_home/bin/cuobjdump"
elif [ -x /usr/local/cuda-13.2/bin/cuobjdump ]; then
  cuobjdump="/usr/local/cuda-13.2/bin/cuobjdump"
else
  cuobjdump="$(command -v cuobjdump || true)"
fi

tmp_ptx="$(mktemp "${TMPDIR:-/tmp}/inference_cuda_kernels.XXXXXX.ptx")"
tmp_fatbin="$(mktemp "${TMPDIR:-/tmp}/inference_cuda_kernels.XXXXXX.fatbin")"
trap 'rm -f "$tmp_ptx" "$tmp_fatbin"' EXIT

required_symbols=(
  termite_fill_f32
  termite_cast_f32_to_f16
  termite_cast_f32_to_bf16
  termite_embedding_lookup_weight_f16_f32
  termite_embedding_lookup_weight_bf16_f32
  termite_linear_weight_f16_f32
  termite_linear_bias_weight_f16_f32
  termite_linear_weight_bf16_f32
  termite_linear_bias_weight_bf16_f32
  termite_attention_f32_block
  termite_deberta_attention_f32
  termite_linear_q4_k_span_bias_f32_tile8_r2
  termite_linear_q4_k_span_bias_relu_f32_tile8_r2
  termite_linear_q4_k_span_bias_f32_tile4_r8
  termite_linear_q4_k_span_bias_relu_f32_tile4_r8
  termite_linear_q4_k_span_pair_bias_f32_tile8_r2
  termite_linear_q4_k_span_pair_bias_relu_f32_tile8_r2
  termite_linear_q4_k_span_pair2_bias_f32_tile8_r2
)

check_required_symbols() {
  local file="$1"
  for symbol in "${required_symbols[@]}"; do
    if ! grep -q "$symbol" "$file"; then
      printf 'error: generated PTX is missing symbol %s\n' "$symbol" >&2
      exit 1
    fi
  done
}

write_or_check() {
  local tmp="$1"
  local dst="$2"
  local label="$3"
  if [ "$mode" = "write" ]; then
    cp "$tmp" "$dst"
    chmod 0644 "$dst"
    printf 'wrote %s\n' "$dst"
  else
    if cmp -s "$tmp" "$dst"; then
      printf 'CUDA %s artifact is up to date: %s\n' "$label" "$dst"
    else
      printf 'error: CUDA %s artifact is stale: %s\n' "$label" "$dst" >&2
      printf 'run %s --write --%s with CUDA 13.2 after reviewing the diff\n' "$0" "$label" >&2
      exit 1
    fi
  fi
}

build_portable_ptx() {
  "$nvcc" -ptx -arch=compute_75 "$src" -o "$tmp_ptx"
  perl -0pi -e 's/\n+\z/\n/' "$tmp_ptx"

  if ! grep -q '^\.version 9\.2' "$tmp_ptx"; then
    printf 'error: generated PTX is not PTX ISA 9.2\n' >&2
    exit 1
  fi
  if ! grep -q '^\.target sm_75' "$tmp_ptx"; then
    printf 'error: generated PTX target is not .target sm_75\n' >&2
    exit 1
  fi
  check_required_symbols "$tmp_ptx"
  write_or_check "$tmp_ptx" "$ptx_dst" "portable"
}

build_fatbin() {
  "$nvcc" -fatbin "$src" -o "$tmp_fatbin" \
    -gencode=arch=compute_75,code=sm_75 \
    -gencode=arch=compute_80,code=sm_80 \
    -gencode=arch=compute_89,code=sm_89 \
    -gencode=arch=compute_90,code=sm_90 \
    -gencode=arch=compute_100,code=sm_100 \
    -gencode=arch=compute_110,code=sm_110 \
    -gencode=arch=compute_120,code=sm_120 \
    -gencode=arch=compute_75,code=compute_75

  if [ ! -s "$tmp_fatbin" ]; then
    printf 'error: generated fatbin is empty\n' >&2
    exit 1
  fi
  if [ -n "$cuobjdump" ]; then
    dump="$("$cuobjdump" --dump-elf "$tmp_fatbin" 2>/dev/null || true)"
    for arch in sm_75 sm_80 sm_89 sm_90 sm_100 sm_110 sm_120; do
      if ! grep -q "$arch" <<<"$dump"; then
        printf 'error: generated fatbin is missing %s\n' "$arch" >&2
        exit 1
      fi
    done
  fi
  write_or_check "$tmp_fatbin" "$fatbin_dst" "fatbin"
}

case "$artifact_mode" in
  portable)
    build_portable_ptx
    ;;
  fatbin)
    build_fatbin
    ;;
  all)
    build_portable_ptx
    build_fatbin
    ;;
  *)
    printf 'error: invalid artifact mode\n' >&2
    exit 2
    ;;
esac
