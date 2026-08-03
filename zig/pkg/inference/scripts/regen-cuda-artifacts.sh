#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf 'usage: %s [--check|--write|--check-source-policy] [--portable|--fatbin|--sm89|--all]\n' "$0"
  printf 'Regenerates checked-in CUDA artifacts with the pinned CUDA 13.2 toolkit contract.\n'
}

mode="check"
artifact_mode="all"
source_policy_only="false"
while [ $# -gt 0 ]; do
  case "$1" in
    --help|-h)
      usage
      exit 0
      ;;
    --check)
      mode="check"
      ;;
    --check-source-policy)
      source_policy_only="true"
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
repo_dir="$(cd "$pkg_dir/../../.." && pwd)"
artifact_src_dir="$pkg_dir/src/ops/cuda/artifacts"
dev_generated_src_dir="$pkg_dir/src/ops/cuda/generated"
src="$pkg_dir/src/ops/cuda/artifacts/inference_cuda_kernels.cu"
ptx_dst="$pkg_dir/src/ops/cuda/artifacts/inference_cuda_kernels.ptx"
fatbin_dst="$pkg_dir/src/ops/cuda/artifacts/inference_cuda_kernels.fatbin"
sm89_dst="$pkg_dir/src/ops/cuda/artifacts/inference_cuda_kernels_sm89.cubin"

check_source_policy() {
  if [ ! -f "$src" ]; then
    printf 'error: CUDA artifact source does not exist: %s\n' "$src" >&2
    exit 1
  fi

  case "$src" in
    "$artifact_src_dir"/*) ;;
    *)
      printf 'error: CUDA artifact source must live under %s\n' "$artifact_src_dir" >&2
      printf 'found: %s\n' "$src" >&2
      exit 1
      ;;
  esac

  case "$src" in
    "$dev_generated_src_dir"/*)
      printf 'error: standalone dev-generated CUDA files are not direct artifact inputs: %s\n' "$src" >&2
      exit 1
      ;;
  esac

  if grep -Eq '^[[:space:]]*#include[[:space:]]*[<"][^">]*generated/' "$src"; then
    printf 'error: CUDA artifact source must not directly include standalone generated kernels\n' >&2
    exit 1
  fi

  # Benchmark-qualified generated kernels live in the canonical section.
  # Dev-only runtime-wired candidates are permitted only in the compiler-owned
  # marker region; standalone generated files must never be copied wholesale.
  if grep -Eq 'Dev-only generated .*candidate from graph/quant_kernel_compiler\.zig|^[[:space:]]*//[[:space:]]*production_enabled=false' "$src"; then
    printf 'error: standalone dev-only source was copied into the CUDA artifact bundle; use the compiler-managed runtime region\n' >&2
    exit 1
  fi

  local runtime_dev_begin='// quant-kernel-codegen:begin generated CUDA runtime-wired dev matmul candidates (do not edit; run: zig build quant-kernel-codegen -- --write)'
  local runtime_dev_end='// quant-kernel-codegen:end generated CUDA runtime-wired dev matmul candidates'
  local runtime_dev_begin_count runtime_dev_end_count runtime_dev_candidate_count
  runtime_dev_begin_count=$(grep -Fxc "$runtime_dev_begin" "$src" || true)
  runtime_dev_end_count=$(grep -Fxc "$runtime_dev_end" "$src" || true)
  runtime_dev_candidate_count=$(grep -Fc '// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.' "$src" || true)
  if [ "$runtime_dev_begin_count" -ne 1 ] || [ "$runtime_dev_end_count" -ne 1 ]; then
    printf 'error: CUDA artifact source must contain one compiler-managed runtime-wired candidate region (found begin=%s end=%s)\n' \
      "$runtime_dev_begin_count" "$runtime_dev_end_count" >&2
    exit 1
  fi

  CUDA_RUNTIME_WIRED_DEV_CANDIDATE_COUNT="$runtime_dev_candidate_count"
}

check_source_policy
if [ "$source_policy_only" = "true" ]; then
  printf 'CUDA artifact source policy passes: %s (%s compiler-managed dev-only runtime-wired candidates included)\n' \
    "$src" "$CUDA_RUNTIME_WIRED_DEV_CANDIDATE_COUNT"
  exit 0
fi

# Artifact freshness is meaningful only after the compiler-owned source and
# manifest regions are current. Fail before invoking nvcc so --check cannot
# bless stale CUDA source and --write cannot publish artifacts from it. Source
# regeneration remains a separate, explicit review step:
#   zig build quant-kernel-codegen -- --write
if [ -n "${ZIG_BIN:-}" ]; then
  zig_bin="$ZIG_BIN"
elif [ -n "${ANTFLY_ZIG:-}" ]; then
  zig_bin="$ANTFLY_ZIG"
elif [ -n "${ZIG:-}" ]; then
  zig_bin="$ZIG"
elif [ -x "$repo_dir/.tools/zig-x86_64-linux-0.16.0/zig" ]; then
  zig_bin="$repo_dir/.tools/zig-x86_64-linux-0.16.0/zig"
elif command -v zig >/dev/null 2>&1; then
  zig_bin="$(command -v zig)"
else
  zig_bin="zig"
fi
if [ ! -x "$zig_bin" ]; then
  printf 'error: Zig compiler is not executable: %s\n' "$zig_bin" >&2
  printf 'set ZIG_BIN, ANTFLY_ZIG, or ZIG to the pinned compiler path\n' >&2
  exit 1
fi
(
  cd "$pkg_dir"
  "$zig_bin" build quant-kernel-codegen -- --check
)

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
  termite_f32_to_f16
  termite_add_bias_relu_rows_f32
  termite_add_weighted_scalars_f32
  termite_linear_bf16_weight_f32_tiled
  termite_linear_f16_weight_f32_tiled
  termite_embedding_lookup_bf16_weight_f32
  termite_embedding_lookup_f16_weight_f32
  termite_embedding_lookup_i32_f16_weight_f32
  termite_attention_f32_block
  termite_attention_f32_bert_prefill_s256_hd64_q16
  termite_attention_f32_bert_prefill_s256_hd64_mma
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
  termite_deberta_attention_fused_f32
  termite_deberta_attention_tc_f16_m16n32
  termite_deberta_attention_tc_f16_m32n16
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
  antfly_gqa_attention_decode_scalars_hd256_f32_v1
  antfly_gqa_attention_decode_split_kv_hd256_f32_stage1_v1
  antfly_gqa_attention_decode_split_kv_hd256_f32_stage2_v1
  antfly_gqa_attention_decode_scalars_hd512_f32_v1
  antfly_gqa_attention_decode_split_kv_hd512_f32_stage1_v1
  antfly_gqa_attention_decode_split_kv_hd512_f32_stage2_v1
  antfly_gqa_attention_decode_scalars_split2_hd256_f32_v1
  antfly_gqa_attention_decode_split2_kv_hd256_f32_stage1_v1
  antfly_gqa_attention_decode_split2_kv_hd256_f32_stage2_v1
  antfly_gqa_attention_decode_scalars_split2_hd512_f32_v1
  antfly_gqa_attention_decode_split2_kv_hd512_f32_stage1_v1
  antfly_gqa_attention_decode_split2_kv_hd512_f32_stage2_v1
  antfly_gqa_attention_decode_scalars_split4_hd256_f32_v1
  antfly_gqa_attention_decode_split4_kv_hd256_f32_stage1_v1
  antfly_gqa_attention_decode_split4_kv_hd256_f32_stage2_v1
  antfly_gqa_attention_decode_scalars_split4_hd512_f32_v1
  antfly_gqa_attention_decode_split4_kv_hd512_f32_stage1_v1
  antfly_gqa_attention_decode_split4_kv_hd512_f32_stage2_v1
  antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v1
  antfly_gqa_attention_decode_turboquant_score_prework_serial_hd256_f32_v1
  antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd256_f32_v1
  antfly_gqa_attention_decode_turboquant_score_prework_hd512_f32_v1
  antfly_gqa_attention_decode_turboquant_score_prework_serial_hd512_f32_v1
  antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd512_f32_v1
  antfly_gqa_attention_prefill_flash_sm89_hd256_swa512_f32_v1
  antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1
  antfly_gqa_attention_decode_splitk_online_sm89_hd256_swa512_f16_f32_v1
  antfly_gqa_attention_decode_splitk_online_sm89_hd512_global_f16_f32_v1
  termite_kv_write_suffix_decode_scalars_f32
  termite_gqa_attention_decode_turboquant_fast_f32
  termite_gqa_attention_prefill_turboquant_fast_f32
  termite_gqa_attention_prefill_turboquant_tiled_f32
  termite_gqa_attention_prefill_turboquant_mma_f32
  termite_gqa_attention_prefill_turboquant_mma_m32_f32
  termite_gqa_attention_prefill_tiled_f16_exact_f32
  termite_gqa_attention_prefill_tiled_f16_warp_f32
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
  termite_linear_q4_0_f32_tc_hmma
  termite_linear_q4_0_bias_f32_tc_hmma
  termite_linear_q4_0_bias_gelu_f32_tc_hmma
  termite_linear_q4_0_bias_add_f32_tc_hmma
  termite_linear_q4_0_f32_tc_hmma_bf16
  termite_linear_q4_0_bias_f32_tc_hmma_bf16
  termite_linear_q4_0_bias_gelu_f32_tc_hmma_bf16
  termite_linear_q4_0_bias_add_f32_tc_hmma_bf16
  termite_activation_multiply_fused_gate_up_f32
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
  antfly_q4_0_pair_activation_q8_1_e2b_6144_mmv_v1
  antfly_q4_0_pair_activation_q8_1_e2b_12288_mmv_v1
  antfly_q4_0_down_q8_1_e2b_6144_mmv_v1
  antfly_q4_0_down_q8_1_e2b_12288_mmv_v1
  antfly_quantize_f32_ggml_q8_1_rows_v1
  antfly_q4_0_pair_activation_ggml_q8_1_e2b_6144_mmv_v1
  antfly_q4_0_pair_activation_ggml_q8_1_e2b_12288_mmv_v1
  antfly_q4_0_down_ggml_q8_1_e2b_6144_mmv_v1
  antfly_q4_0_down_ggml_q8_1_e2b_12288_mmv_v1
  antfly_q4_0_q8_1_argmax_rows_stage1_tile8_v1
  antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1
  antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1
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
  antfly_q4_0_mmv_f32_v1
  antfly_q4_0_mm_f32_v1
  antfly_q4_0_pair_mmv_f32_v1
  antfly_q4_0_pair_activation_q8_1_mmv_v1
  antfly_q4_0_down_q8_1_mmv_v1
)

check_required_symbols() {
  local file="$1"
  for symbol in "${required_symbols[@]}"; do
    if ! grep -a -q "$symbol" "$file"; then
      printf 'error: generated CUDA artifact is missing symbol %s\n' "$symbol" >&2
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
  check_required_symbols "$tmp_fatbin"
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
  check_required_symbols "$tmp_sm89"
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
