#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'usage: %s [--check|--write] [--portable|--fatbin|--sm89|--all]\n' "$0"
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
    --sm89)
      artifact_mode="sm89"
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
sm89_dst="$pkg_dir/src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin"

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
case "$version" in
  *"release 13.2"*) ;;
  *)
    printf 'error: expected CUDA toolkit 13.2 for artifact regeneration.\n' >&2
    printf '%s\n' "$version" >&2
    exit 1
    ;;
esac

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
tmp_sm89="$(mktemp "${TMPDIR:-/tmp}/inference_cuda_kernels.XXXXXX.sm89.cubin")"
trap 'rm -f "$tmp_ptx" "$tmp_fatbin" "$tmp_sm89"' EXIT

required_symbols=(
  termite_fill_f32
  termite_copy_f32
  termite_copy_u8
  termite_f32_to_bf16
  termite_add_weighted_scalars_f32
  termite_linear_bf16_weight_f32_tiled
  termite_embedding_lookup_bf16_weight_f32
  termite_attention_f32_block
  termite_cross_attention_f32
  termite_cross_attention_q1_f32
  termite_token_to_nchw_f32
  termite_nchw_to_token_f32
  termite_pack_windows_f32
  termite_unpad_windows_f32
  termite_channel_scores_softmax_f32
  termite_channel_apply_f32
  termite_florence_vision_tail_sources_f32
  termite_linear_q4_k_pair_bias_f32_tc_hmma
  termite_deberta_attention_f32
  termite_gliner_gather_concat_relu_f32
  termite_split_last_dim3_f32
  termite_rope_per_item_f32
  termite_rope_decode_scalars_f32
  termite_rope_scaled_decode_scalars_f32
  termite_rms_norm_heads_rope_decode_scalars_f32
  termite_activation_multiply_slice_last_dim_f32
  termite_gqa_attention_decode_scalars_fast_f32
  termite_gqa_attention_prefill_fast_f32
  termite_gqa_attention_decode_scalars_f32
  termite_kv_write_suffix_decode_scalars_f32
  termite_gqa_attention_decode_turboquant_fast_f32
  termite_gqa_attention_prefill_turboquant_fast_f32
  termite_gqa_attention_prefill_turboquant_tiled_f32
  termite_gqa_attention_prefill_turboquant_mma_f32
  termite_gqa_attention_prefill_turboquant_mma_m32_f32
  termite_dequant_q4_0_bf16
  termite_linear_q6_k_q8_1_f32_tile8_e4b
  termite_gqa_attention_decode_turboquant_split_stage1_f32
  termite_gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32
  termite_gqa_attention_decode_turboquant_split_stage2_f32
  termite_gqa_attention_decode_turboquant_f32
  termite_kv_write_suffix_turboquant_f32
  termite_gemma4_mtp_preproject_f32
  termite_gemma4_mtp_masked_select_f32
  termite_gemma4_mtp_centroid_scores_hidden_f32
  termite_gemma4_mtp_centroid_topk_u32
  termite_gemma4_mtp_restricted_lm_head_scores_f32
  termite_gemma4_mtp_reduce_token_scores_f32
  termite_gemma4_mtp_masked_select_hidden_f32
  termite_gemma4_mtp_masked_argmax_f32
  termite_gemma4_mtp_verify_commit_u32
  termite_linear_q8_0_argmax_rows_stage1_tile4
  termite_linear_q4_0_argmax_rows_stage1_tile4
  termite_linear_q4_0_argmax_rows_stage1_tile16
  termite_linear_q4_k_argmax_rows_stage1_tile4
  termite_argmax_reduce_rows_pairs_f32
  termite_linear_q8_0_f32_tile4_r2
  termite_linear_q6_k_f32_tile4
  termite_linear_q6_k_gated_down_f32_tile4
  termite_linear_q8_0_bias_f32_tile4_r2
  termite_linear_q8_0_bias_gelu_f32_tile4_r2
  termite_linear_q8_0_bias_add_f32_tile4_r2
  termite_linear_q8_0_f32_fast_r2c8
  termite_linear_q8_0_bias_f32_fast_r2c8
  termite_linear_q8_0_bias_gelu_f32_fast_r2c8
  termite_linear_q8_0_bias_add_f32_fast_r2c8
  termite_linear_q8_0_f32_fast_r4c4
  termite_linear_q8_0_bias_f32_fast_r4c4
  termite_linear_q8_0_bias_gelu_f32_fast_r4c4
  termite_linear_q8_0_bias_add_f32_fast_r4c4
  termite_linear_q8_0_f32_tc_hmma
  termite_linear_q8_0_bias_f32_tc_hmma
  termite_linear_q8_0_bias_gelu_f32_tc_hmma
  termite_linear_q8_0_bias_add_f32_tc_hmma
  termite_rms_norm_add_weighted_embedding_i32_q6_k_f32
  termite_rms_norm_f32_bf16
  termite_rms_norm_add_f32_bf16
  termite_linear_q4_0_gated_down_f32_tile4_w4
  termite_linear_q4_0_activation_slice_last_dim_f32_tile4
  termite_linear_q4_0_activation_slice_last_dim_f32_tile4_w4
  termite_linear_q4_0_pair_nobias_f32_tile4_w4
  termite_linear_q4_0_pair_activation_f32_tile4_w4
  termite_linear_q4_0_f32_tile8
  termite_linear_q4_0_q8_1_f32_tile4_w10_e4b_down
  termite_linear_q4_0_q8_1_f32_tile4_w8_rows2
  termite_linear_q4_0_q8_1_f32_tile4_w8_rows4
  termite_linear_q4_0_q8_1_f32_tile4_w8_rows8_c4
  termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows
  termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn
  termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn
  termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2
  termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4
  termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2
  termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1
  termite_linear_q4_0_pair_nobias_f32_tile8
  termite_linear_q4_0_qkv_nobias_f32_tile8
  termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4
  termite_linear_q4_0_gated_down_f32_tile8
  termite_linear_q4_0_gated_down_f32_tile16
  termite_linear_q4_k_bias_gelu_f32_tile4_r2
  termite_linear_q4_k_bias_add_f32_tile4_r2
  termite_linear_q4_k_bias_f32_fast_r2c8
  termite_linear_q4_k_bias_gelu_f32_fast_r2c8
  termite_linear_q4_k_bias_add_f32_fast_r2c8
  termite_linear_q4_k_bias_f32_fast_r4c4
  termite_linear_q4_k_bias_gelu_f32_fast_r4c4
  termite_linear_q4_k_bias_add_f32_fast_r4c4
  termite_linear_q4_k_f32_tc_hmma
  termite_linear_q4_k_bias_f32_tc_hmma
  termite_linear_q4_k_bias_gelu_f32_tc_hmma
  termite_linear_q4_k_bias_add_f32_tc_hmma
  termite_linear_q4_k_bias_quick_gelu_f32_tc_hmma
  termite_linear_q4_k_bias_relu_f32_tc_hmma
  termite_linear_q4_k_span_bias_f32_tile8_r2
  termite_linear_q4_k_span_bias_relu_f32_tile8_r2
  termite_linear_q4_k_span_bias_f32_tile4_r8
  termite_linear_q4_k_span_bias_relu_f32_tile4_r8
  termite_linear_q4_k_span_pair_bias_f32_tile8_r2
  termite_linear_q4_k_span_pair_bias_relu_f32_tile8_r2
  termite_linear_q4_k_span_pair2_bias_f32_tile8_r2
  termite_linear_bf16_weight_f32_qkv_nobias_tiled
  termite_linear_q4_k_q4_k_f32_qkv_nobias_tiled
  termite_embedding_lookup_i32_q4_k_f32
  termite_embedding_lookup_q6_k_f32
  termite_embedding_lookup_i32_q6_k_f32
  termite_embedding_add_weighted_i32_q6_k_f32
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

build_sm89() {
  "$nvcc" -cubin -arch=sm_89 "$src" -o "$tmp_sm89"

  if [ ! -s "$tmp_sm89" ]; then
    printf 'error: generated sm89 cubin is empty\n' >&2
    exit 1
  fi
  if [ -n "$cuobjdump" ]; then
    dump="$("$cuobjdump" --dump-elf "$tmp_sm89" 2>/dev/null || true)"
    if ! grep -q "sm_89" <<<"$dump"; then
      printf 'error: generated sm89 cubin is missing sm_89\n' >&2
      exit 1
    fi
  fi
  write_or_check "$tmp_sm89" "$sm89_dst" "sm89"
}

case "$artifact_mode" in
  portable)
    build_portable_ptx
    ;;
  fatbin)
    build_fatbin
    ;;
  sm89)
    build_sm89
    ;;
  all)
    build_portable_ptx
    build_fatbin
    build_sm89
    ;;
  *)
    printf 'error: invalid artifact mode\n' >&2
    exit 2
    ;;
esac
