// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
const buffer_mod = @import("buffer.zig");
const context_mod = @import("context.zig");
const driver_mod = @import("driver.zig");
const cuda_artifact = @import("artifact.zig");
const platform = @import("antfly_platform");
const turboquant = @import("../../runtime/kv/turboquant.zig");
const kv_pool_mod = @import("../../runtime/kv/pool.zig");

fn captureParamTraceIndex(ctx: *context_mod.CudaContext) ?usize {
    if (!ctx.debug_graph_capture_active) return null;
    if (!platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_PARAM_TRACE", false)) return null;
    const limit = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_PARAM_TRACE_LIMIT") orelse 256;
    if (ctx.debug_graph_capture_param_trace_count >= limit) return null;
    const index = ctx.debug_graph_capture_param_trace_count;
    ctx.debug_graph_capture_param_trace_count += 1;
    return index;
}

fn fastGqaDecodeDisabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_CUDA_DISABLE_FAST_GQA_DECODE", false);
}

fn fastGqaPrefillEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_FAST", false);
}

fn tiledGqaPrefillEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_TILED", false);
}

fn mmaGqaPrefillEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_MMA", false);
}

fn mmaM32GqaPrefillEnabled() bool {
    return platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GQA_PREFILL_MMA_M32", true);
}

fn fastGqaDecodeEligible(batch: usize, q_seq_len: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize, mask_len: usize, bias_mode: u32) bool {
    return batch == 1 and
        q_seq_len == 1 and
        mask_len == 0 and
        bias_mode == 0 and
        num_kv_heads != 0 and
        num_heads % num_kv_heads == 0 and
        head_dim <= 512 and
        head_dim % 32 == 0;
}

fn gqaPrefillShapeEligible(batch: usize, q_seq_len: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize, mask_len: usize, bias_mode: u32) bool {
    return batch == 1 and
        q_seq_len > 1 and
        mask_len == 0 and
        bias_mode == 0 and
        num_kv_heads != 0 and
        num_heads % num_kv_heads == 0 and
        head_dim <= 512 and
        head_dim % 32 == 0;
}

fn fastGqaPrefillEligible(batch: usize, q_seq_len: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize, mask_len: usize, bias_mode: u32) bool {
    return fastGqaPrefillEnabled() and
        gqaPrefillShapeEligible(batch, q_seq_len, num_heads, num_kv_heads, head_dim, mask_len, bias_mode);
}

pub const GqaAttentionLaunchKind = enum {
    none,
    decode,
    decode_fast,
    decode_fast_fallback,
    prefill_fast,
    prefill_tiled,
    prefill_mma,
    prefill_mma_m32,
    scalar,
};

pub const QMatmulVariant = enum {
    legacy,
    fast_r2c4,
    fast_r2c8,
    fast_r4c4,
    tc_hmma,
};

pub const Q4_0Q8_1PrefillRowsVariant = enum {
    none,
    single,
    generic_rows,
    tile8_rows,
    tile8_w8_rows,
    rows2,
    rows4,
    rows8_c2,
    rows8_c4,
    rows16_c1,
    e4b_down_rows,
};

pub const KernelModule = struct {
    module: driver_mod.CUmodule = null,
    fill_f32: driver_mod.CUfunction = null,
    copy_f32: driver_mod.CUfunction = null,
    copy_u8: driver_mod.CUfunction = null,
    f32_to_bf16: driver_mod.CUfunction = null,
    dequant_q4_0_bf16: driver_mod.CUfunction = null,
    scale_f32: driver_mod.CUfunction = null,
    add_scalar_f32: driver_mod.CUfunction = null,
    binary_scalar_f32: driver_mod.CUfunction = null,
    add_mul_scalar_f32: driver_mod.CUfunction = null,
    add_weighted_scalars_f32: driver_mod.CUfunction = null,
    linear_f32: driver_mod.CUfunction = null,
    linear_f32_tiled: driver_mod.CUfunction = null,
    linear_bf16_weight_f32_tiled: driver_mod.CUfunction = null,
    argmax_last_row_f32: driver_mod.CUfunction = null,
    argmax_rows_f32: driver_mod.CUfunction = null,
    argmax_rows_suppress_f32: driver_mod.CUfunction = null,
    argmax_last_row_suppress_f32: driver_mod.CUfunction = null,
    linear_q8_0_argmax_stage1_tile4: driver_mod.CUfunction = null,
    linear_q4_k_argmax_stage1_tile4: driver_mod.CUfunction = null,
    argmax_reduce_pairs_f32: driver_mod.CUfunction = null,
    linear_q8_0_argmax_rows_stage1_tile4: driver_mod.CUfunction = null,
    linear_q4_0_argmax_rows_stage1_tile4: driver_mod.CUfunction = null,
    linear_q4_0_argmax_rows_stage1_tile16: driver_mod.CUfunction = null,
    linear_q4_k_argmax_rows_stage1_tile4: driver_mod.CUfunction = null,
    linear_q6_k_argmax_rows_stage1_tile4: driver_mod.CUfunction = null,
    linear_q6_k_argmax_rows_stage1_tile8: driver_mod.CUfunction = null,
    linear_q6_k_argmax_rows_stage1_tile16: driver_mod.CUfunction = null,
    linear_q6_k_q8_1_argmax_rows_stage1_tile8: driver_mod.CUfunction = null,
    linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b: driver_mod.CUfunction = null,
    linear_q6_k_q8_1_f32_tile8_e4b: driver_mod.CUfunction = null,
    argmax_reduce_rows_pairs_f32: driver_mod.CUfunction = null,
    argmax_reduce_rows_pairs_f32_w16: driver_mod.CUfunction = null,
    gemma4_mtp_preproject_f32: driver_mod.CUfunction = null,
    gemma4_mtp_masked_select_f32: driver_mod.CUfunction = null,
    gemma4_mtp_masked_select_hidden_f32: driver_mod.CUfunction = null,
    gemma4_mtp_centroid_scores_hidden_f32: driver_mod.CUfunction = null,
    gemma4_mtp_centroid_topk_u32: driver_mod.CUfunction = null,
    gemma4_mtp_restricted_lm_head_scores_f32: driver_mod.CUfunction = null,
    gemma4_mtp_reduce_token_scores_f32: driver_mod.CUfunction = null,
    gemma4_mtp_masked_argmax_f32: driver_mod.CUfunction = null,
    gemma4_mtp_verify_commit_u32: driver_mod.CUfunction = null,
    linear_bias_f32: driver_mod.CUfunction = null,
    add_bias_rows_f32: driver_mod.CUfunction = null,
    linear_bias_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_bias_relu_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_bias_gelu_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_bias_add_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_pair_bias_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_triple_bias_f32_tile4_r2: driver_mod.CUfunction = null,
    rms_norm_f32: driver_mod.CUfunction = null,
    rms_norm_f32_bf16: driver_mod.CUfunction = null,
    rms_norm_add_f32_bf16: driver_mod.CUfunction = null,
    rms_norm_add_f32: driver_mod.CUfunction = null,
    rms_norm_add_mul_scalar_f32: driver_mod.CUfunction = null,
    rms_norm_add_output_scale_f32: driver_mod.CUfunction = null,
    rms_norm_bare_f32: driver_mod.CUfunction = null,
    layer_norm_f32: driver_mod.CUfunction = null,
    add_layer_norm_f32: driver_mod.CUfunction = null,
    elementwise_f32: driver_mod.CUfunction = null,
    silu_multiply_f32: driver_mod.CUfunction = null,
    activation_multiply_f32: driver_mod.CUfunction = null,
    activation_multiply_slice_last_dim_f32: driver_mod.CUfunction = null,
    embedding_lookup_f32: driver_mod.CUfunction = null,
    embedding_lookup_bf16_weight_f32: driver_mod.CUfunction = null,
    embedding_lookup_i32_f32: driver_mod.CUfunction = null,
    take_rows_f32: driver_mod.CUfunction = null,
    gliner_gather_concat_relu_f32: driver_mod.CUfunction = null,
    gliner_word_embeddings_f32: driver_mod.CUfunction = null,
    repeat_first_row_f32: driver_mod.CUfunction = null,
    gliner_gru_combine_f32: driver_mod.CUfunction = null,
    concat_lastdim_f32: driver_mod.CUfunction = null,
    conv2d_f32: driver_mod.CUfunction = null,
    attention_f32: driver_mod.CUfunction = null,
    attention_f32_block: driver_mod.CUfunction = null,
    cross_attention_f32: driver_mod.CUfunction = null,
    cross_attention_q1_f32: driver_mod.CUfunction = null,
    token_to_nchw_f32: driver_mod.CUfunction = null,
    nchw_to_token_f32: driver_mod.CUfunction = null,
    pack_windows_f32: driver_mod.CUfunction = null,
    unpad_windows_f32: driver_mod.CUfunction = null,
    channel_scores_softmax_f32: driver_mod.CUfunction = null,
    channel_apply_f32: driver_mod.CUfunction = null,
    florence_vision_tail_sources_f32: driver_mod.CUfunction = null,
    rope_f32: driver_mod.CUfunction = null,
    rope_decode_scalars_f32: driver_mod.CUfunction = null,
    decode_scalars_advance: driver_mod.CUfunction = null,
    rope_scaled_f32: driver_mod.CUfunction = null,
    rope_scaled_decode_scalars_f32: driver_mod.CUfunction = null,
    rope_per_item_f32: driver_mod.CUfunction = null,
    rms_norm_heads_rope_f32: driver_mod.CUfunction = null,
    rms_norm_heads_rope_decode_scalars_f32: driver_mod.CUfunction = null,
    gqa_attention_f32: driver_mod.CUfunction = null,
    gqa_attention_decode_f32: driver_mod.CUfunction = null,
    gqa_attention_decode_scalars_fast_f32: driver_mod.CUfunction = null,
    gqa_attention_prefill_fast_f32: driver_mod.CUfunction = null,
    gqa_attention_decode_scalars_f32: driver_mod.CUfunction = null,
    kv_write_suffix_decode_scalars_f32: driver_mod.CUfunction = null,
    gqa_attention_decode_turboquant_fast_f32: driver_mod.CUfunction = null,
    gqa_attention_prefill_turboquant_fast_f32: driver_mod.CUfunction = null,
    gqa_attention_prefill_turboquant_tiled_f32: driver_mod.CUfunction = null,
    gqa_attention_prefill_turboquant_mma_f32: driver_mod.CUfunction = null,
    gqa_attention_prefill_turboquant_mma_m32_f32: driver_mod.CUfunction = null,
    /// The mma prefill kernels need 64-95KB dynamic smem, above the 48KB
    /// default; CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES is raised
    /// once per module load (per function).
    gqa_prefill_mma_smem_opted_in: bool = false,
    gqa_prefill_mma_m32_smem_opted_in: bool = false,
    gqa_attention_decode_turboquant_split_stage1_f32: driver_mod.CUfunction = null,
    gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32: driver_mod.CUfunction = null,
    gqa_attention_decode_turboquant_split_stage2_f32: driver_mod.CUfunction = null,
    gqa_attention_decode_turboquant_f32: driver_mod.CUfunction = null,
    kv_write_suffix_turboquant_f32: driver_mod.CUfunction = null,
    deberta_attention_f32: driver_mod.CUfunction = null,
    split_last_dim3_f32: driver_mod.CUfunction = null,
    linear_q8_0_f32: driver_mod.CUfunction = null,

    linear_q8_0_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q8_0_bias_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q8_0_bias_gelu_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q8_0_bias_add_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q8_0_f32_fast_r2c8: driver_mod.CUfunction = null,
    linear_q8_0_bias_f32_fast_r2c8: driver_mod.CUfunction = null,
    linear_q8_0_bias_gelu_f32_fast_r2c8: driver_mod.CUfunction = null,
    linear_q8_0_bias_add_f32_fast_r2c8: driver_mod.CUfunction = null,
    linear_q8_0_f32_fast_r4c4: driver_mod.CUfunction = null,
    linear_q8_0_bias_f32_fast_r4c4: driver_mod.CUfunction = null,
    linear_q8_0_bias_gelu_f32_fast_r4c4: driver_mod.CUfunction = null,
    linear_q8_0_bias_add_f32_fast_r4c4: driver_mod.CUfunction = null,
    linear_q8_0_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q8_0_bias_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q8_0_bias_gelu_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q8_0_bias_add_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q8_0_f32_tile4: driver_mod.CUfunction = null,
    linear_q8_0_gated_down_f32_tile4: driver_mod.CUfunction = null,
    quantize_f32_q8_1_rows: driver_mod.CUfunction = null,
    quantize_gated_f32_q8_1_rows: driver_mod.CUfunction = null,
    linear_q4_0_f32: driver_mod.CUfunction = null,
    linear_q4_0_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_f32_tile4_w4: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4_e4b_attn_2048: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4_e4b_attn_4096: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4_w8: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4_w8_rows2: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4_w8_rows4: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4_w8_rows8_c4: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4_w8_e4b_down: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile4_w10_e4b_down: driver_mod.CUfunction = null,
    linear_q4_0_q8_1_f32_tile8: driver_mod.CUfunction = null,
    linear_q4_0_f32_tile8: driver_mod.CUfunction = null,
    linear_q4_0_activation_slice_last_dim_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_activation_slice_last_dim_f32_tile4_w4: driver_mod.CUfunction = null,
    linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_gated_down_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_gated_down_f32_tile4_w4: driver_mod.CUfunction = null,
    linear_q4_0_gated_down_f32_tile8: driver_mod.CUfunction = null,
    linear_q4_0_gated_down_f32_tile16: driver_mod.CUfunction = null,
    linear_q4_k_f32: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32: driver_mod.CUfunction = null,
    linear_q4_k_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_f32_tile4: driver_mod.CUfunction = null,
    linear_q6_k_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_gated_down_f32_tile4: driver_mod.CUfunction = null,
    linear_q6_k_gated_down_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_tile4_r2: driver_mod.CUfunction = null,

    linear_q4_k_bias_gelu_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q4_k_bias_add_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_fast_r2c8: driver_mod.CUfunction = null,
    linear_q4_k_bias_gelu_f32_fast_r2c8: driver_mod.CUfunction = null,
    linear_q4_k_bias_add_f32_fast_r2c8: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_fast_r4c4: driver_mod.CUfunction = null,
    linear_q4_k_bias_gelu_f32_fast_r4c4: driver_mod.CUfunction = null,
    linear_q4_k_bias_add_f32_fast_r4c4: driver_mod.CUfunction = null,
    linear_q4_k_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q4_k_bias_gelu_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q4_k_bias_add_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q4_k_bias_relu_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q4_k_triple_bias_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q4_k_pair_bias_f32_tc_hmma: driver_mod.CUfunction = null,
    linear_q4_k_span_bias_f32_tile8_r2: driver_mod.CUfunction = null,
    linear_q4_k_span_bias_relu_f32_tile8_r2: driver_mod.CUfunction = null,
    linear_q4_k_span_bias_f32_tile4_r8: driver_mod.CUfunction = null,
    linear_q4_k_span_bias_relu_f32_tile4_r8: driver_mod.CUfunction = null,
    linear_q4_k_span_pair_bias_f32_tile8_r2: driver_mod.CUfunction = null,
    linear_q4_k_span_pair_bias_relu_f32_tile8_r2: driver_mod.CUfunction = null,
    linear_q4_k_span_pair2_bias_f32_tile8_r2: driver_mod.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_relu_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_relu_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q4_k_bias_add_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_f32_tile8: driver_mod.CUfunction = null,
    linear_q4_k_triple_bias_f32: driver_mod.CUfunction = null,
    linear_q4_k_triple_bias_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_pair_bias_f32_tiled: driver_mod.CUfunction = null,
    linear_q8_0_pair_nobias_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_pair_nobias_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_pair_nobias_f32_tile4_w4: driver_mod.CUfunction = null,
    linear_q4_0_pair_nobias_q8_1_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_pair_nobias_q8_1_f32_tile4_w8: driver_mod.CUfunction = null,
    linear_q4_0_pair_nobias_q8_1_f32_tile8: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_f32_tile4_w4: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_f32_tile4_w8: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1: driver_mod.CUfunction = null,
    linear_q4_0_pair_activation_q8_1_f32_tile8: driver_mod.CUfunction = null,
    linear_q4_0_pair_nobias_f32_tile8: driver_mod.CUfunction = null,
    linear_q4_k_pair_nobias_f32_tile4: driver_mod.CUfunction = null,
    linear_f32_qkv_nobias_tiled: driver_mod.CUfunction = null,
    linear_bf16_weight_f32_qkv_nobias_tiled: driver_mod.CUfunction = null,
    linear_q8_0_qkv_nobias_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_qkv_nobias_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_qkv_nobias_f32_tile4_w4: driver_mod.CUfunction = null,
    linear_q4_0_qkv_nobias_q8_1_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_qkv_nobias_q8_1_f32_tile8: driver_mod.CUfunction = null,
    linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4: driver_mod.CUfunction = null,
    linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8: driver_mod.CUfunction = null,
    linear_q4_0_qkv_nobias_f32_tile8: driver_mod.CUfunction = null,
    linear_q4_k_qkv_nobias_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_q4_k_f32_qkv_nobias_tiled: driver_mod.CUfunction = null,
    embedding_lookup_q8_0_f32: driver_mod.CUfunction = null,
    embedding_lookup_i32_q8_0_f32: driver_mod.CUfunction = null,
    embedding_lookup_q4_0_f32: driver_mod.CUfunction = null,
    embedding_lookup_i32_q4_0_f32: driver_mod.CUfunction = null,
    embedding_lookup_q4_k_f32: driver_mod.CUfunction = null,
    embedding_lookup_i32_q4_k_f32: driver_mod.CUfunction = null,
    embedding_lookup_q6_k_f32: driver_mod.CUfunction = null,
    embedding_lookup_i32_q6_k_f32: driver_mod.CUfunction = null,
    embedding_add_weighted_i32_q6_k_f32: driver_mod.CUfunction = null,
    rms_norm_add_weighted_embedding_i32_q6_k_f32: driver_mod.CUfunction = null,
    slice_last_dim_f32: driver_mod.CUfunction = null,

    pub fn load(ctx: *context_mod.CudaContext) driver_mod.Error!KernelModule {
        try ctx.makeCurrent();
        var module: driver_mod.CUmodule = null;
        try loadModuleWithJitLog(ctx, &module);
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);

        var fill_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&fill_f32, module, "termite_fill_f32"));
        const copy_f32 = loadOptionalFunction(ctx, module, "termite_copy_f32");
        const copy_u8 = loadOptionalFunction(ctx, module, "termite_copy_u8");
        const f32_to_bf16 = loadOptionalFunction(ctx, module, "termite_f32_to_bf16");
        const dequant_q4_0_bf16 = loadOptionalFunction(ctx, module, "termite_dequant_q4_0_bf16");
        var scale_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&scale_f32, module, "termite_scale_f32"));
        var add_scalar_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&add_scalar_f32, module, "termite_add_scalar_f32"));
        var binary_scalar_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&binary_scalar_f32, module, "termite_binary_scalar_f32"));
        var add_mul_scalar_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&add_mul_scalar_f32, module, "termite_add_mul_scalar_f32"));
        var add_weighted_scalars_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&add_weighted_scalars_f32, module, "termite_add_weighted_scalars_f32"));
        var linear_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_f32, module, "termite_linear_f32"));
        const linear_f32_tiled = loadOptionalFunction(ctx, module, "termite_linear_f32_tiled");
        const linear_bf16_weight_f32_tiled = loadOptionalFunction(ctx, module, "termite_linear_bf16_weight_f32_tiled");
        var argmax_last_row_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&argmax_last_row_f32, module, "termite_argmax_last_row_f32"));
        const argmax_rows_f32 = loadOptionalFunction(ctx, module, "termite_argmax_rows_f32");
        const argmax_rows_suppress_f32 = loadOptionalFunction(ctx, module, "termite_argmax_rows_suppress_f32");
        const argmax_last_row_suppress_f32 = loadOptionalFunction(ctx, module, "termite_argmax_last_row_suppress_f32");
        const linear_q8_0_argmax_stage1_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_argmax_stage1_tile4");
        const linear_q4_k_argmax_stage1_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_argmax_stage1_tile4");
        const argmax_reduce_pairs_f32 = loadOptionalFunction(ctx, module, "termite_argmax_reduce_pairs_f32");
        const linear_q8_0_argmax_rows_stage1_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_argmax_rows_stage1_tile4");
        const linear_q4_0_argmax_rows_stage1_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_argmax_rows_stage1_tile4");
        const linear_q4_0_argmax_rows_stage1_tile16 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_argmax_rows_stage1_tile16");
        const linear_q4_k_argmax_rows_stage1_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_argmax_rows_stage1_tile4");
        const linear_q6_k_argmax_rows_stage1_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q6_k_argmax_rows_stage1_tile4");
        const linear_q6_k_argmax_rows_stage1_tile8 = loadOptionalFunction(ctx, module, "termite_linear_q6_k_argmax_rows_stage1_tile8");
        const linear_q6_k_argmax_rows_stage1_tile16 = loadOptionalFunction(ctx, module, "termite_linear_q6_k_argmax_rows_stage1_tile16");
        const linear_q6_k_q8_1_argmax_rows_stage1_tile8 = loadOptionalFunction(ctx, module, "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8");
        const linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b = loadOptionalFunction(ctx, module, "termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b");
        const linear_q6_k_q8_1_f32_tile8_e4b = loadOptionalFunction(ctx, module, "termite_linear_q6_k_q8_1_f32_tile8_e4b");
        const argmax_reduce_rows_pairs_f32 = loadOptionalFunction(ctx, module, "termite_argmax_reduce_rows_pairs_f32");
        const argmax_reduce_rows_pairs_f32_w16 = loadOptionalFunction(ctx, module, "termite_argmax_reduce_rows_pairs_f32_w16");
        const gemma4_mtp_preproject_f32 = loadOptionalFunction(ctx, module, "termite_gemma4_mtp_preproject_f32");
        const gemma4_mtp_masked_select_f32 = loadOptionalFunction(ctx, module, "termite_gemma4_mtp_masked_select_f32");
        const gemma4_mtp_masked_select_hidden_f32 = loadOptionalFunction(ctx, module, "termite_gemma4_mtp_masked_select_hidden_f32");
        const gemma4_mtp_centroid_scores_hidden_f32 = loadOptionalFunction(ctx, module, "termite_gemma4_mtp_centroid_scores_hidden_f32");
        const gemma4_mtp_centroid_topk_u32 = loadOptionalFunction(ctx, module, "termite_gemma4_mtp_centroid_topk_u32");
        const gemma4_mtp_restricted_lm_head_scores_f32 = loadOptionalFunction(ctx, module, "termite_gemma4_mtp_restricted_lm_head_scores_f32");
        const gemma4_mtp_reduce_token_scores_f32 = loadOptionalFunction(ctx, module, "termite_gemma4_mtp_reduce_token_scores_f32");
        const gemma4_mtp_masked_argmax_f32 = loadOptionalFunction(ctx, module, "termite_gemma4_mtp_masked_argmax_f32");
        const gemma4_mtp_verify_commit_u32 = loadOptionalFunction(ctx, module, "termite_gemma4_mtp_verify_commit_u32");
        var linear_bias_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_bias_f32, module, "termite_linear_bias_f32"));
        var add_bias_rows_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&add_bias_rows_f32, module, "termite_add_bias_rows_f32"));
        var linear_bias_f32_tile4_r2: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_bias_f32_tile4_r2, module, "termite_linear_bias_f32_tile4_r2"));
        var linear_bias_relu_f32_tile4_r2: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_bias_relu_f32_tile4_r2, module, "termite_linear_bias_relu_f32_tile4_r2"));
        var linear_bias_gelu_f32_tile4_r2: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_bias_gelu_f32_tile4_r2, module, "termite_linear_bias_gelu_f32_tile4_r2"));
        var linear_bias_add_f32_tile4_r2: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_bias_add_f32_tile4_r2, module, "termite_linear_bias_add_f32_tile4_r2"));
        var linear_pair_bias_f32_tile4_r2: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_pair_bias_f32_tile4_r2, module, "termite_linear_pair_bias_f32_tile4_r2"));
        var linear_triple_bias_f32_tile4_r2: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_triple_bias_f32_tile4_r2, module, "termite_linear_triple_bias_f32_tile4_r2"));
        var rms_norm_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&rms_norm_f32, module, "termite_rms_norm_f32"));
        const rms_norm_f32_bf16 = loadOptionalFunction(ctx, module, "termite_rms_norm_f32_bf16");
        const rms_norm_add_f32_bf16 = loadOptionalFunction(ctx, module, "termite_rms_norm_add_f32_bf16");
        var rms_norm_add_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&rms_norm_add_f32, module, "termite_rms_norm_add_f32"));
        var rms_norm_add_mul_scalar_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&rms_norm_add_mul_scalar_f32, module, "termite_rms_norm_add_mul_scalar_f32"));
        const rms_norm_add_output_scale_f32 = loadOptionalFunction(ctx, module, "termite_rms_norm_add_output_scale_f32");
        var rms_norm_bare_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&rms_norm_bare_f32, module, "termite_rms_norm_bare_f32"));
        var layer_norm_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&layer_norm_f32, module, "termite_layer_norm_f32"));
        var add_layer_norm_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&add_layer_norm_f32, module, "termite_add_layer_norm_f32"));
        var elementwise_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&elementwise_f32, module, "termite_elementwise_f32"));
        var silu_multiply_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&silu_multiply_f32, module, "termite_silu_multiply_f32"));
        var activation_multiply_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&activation_multiply_f32, module, "termite_activation_multiply_f32"));
        const activation_multiply_slice_last_dim_f32 = loadOptionalFunction(ctx, module, "termite_activation_multiply_slice_last_dim_f32");
        var embedding_lookup_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_f32, module, "termite_embedding_lookup_f32"));
        const embedding_lookup_bf16_weight_f32 = loadOptionalFunction(ctx, module, "termite_embedding_lookup_bf16_weight_f32");
        var embedding_lookup_i32_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_i32_f32, module, "termite_embedding_lookup_i32_f32"));
        var take_rows_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&take_rows_f32, module, "termite_take_rows_f32"));
        const gliner_gather_concat_relu_f32 = loadOptionalFunction(ctx, module, "termite_gliner_gather_concat_relu_f32");
        var gliner_word_embeddings_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&gliner_word_embeddings_f32, module, "termite_gliner_word_embeddings_f32"));
        var repeat_first_row_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&repeat_first_row_f32, module, "termite_repeat_first_row_f32"));
        var gliner_gru_combine_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&gliner_gru_combine_f32, module, "termite_gliner_gru_combine_f32"));
        var concat_lastdim_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&concat_lastdim_f32, module, "termite_concat_lastdim_f32"));
        var conv2d_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&conv2d_f32, module, "termite_conv2d_f32"));
        var attention_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&attention_f32, module, "termite_attention_f32"));
        var attention_f32_block: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&attention_f32_block, module, "termite_attention_f32_block"));
        const cross_attention_f32 = loadOptionalFunction(ctx, module, "termite_cross_attention_f32");
        const cross_attention_q1_f32 = loadOptionalFunction(ctx, module, "termite_cross_attention_q1_f32");
        const token_to_nchw_f32 = loadOptionalFunction(ctx, module, "termite_token_to_nchw_f32");
        const nchw_to_token_f32 = loadOptionalFunction(ctx, module, "termite_nchw_to_token_f32");
        const pack_windows_f32 = loadOptionalFunction(ctx, module, "termite_pack_windows_f32");
        const unpad_windows_f32 = loadOptionalFunction(ctx, module, "termite_unpad_windows_f32");
        const channel_scores_softmax_f32 = loadOptionalFunction(ctx, module, "termite_channel_scores_softmax_f32");
        const channel_apply_f32 = loadOptionalFunction(ctx, module, "termite_channel_apply_f32");
        const florence_vision_tail_sources_f32 = loadOptionalFunction(ctx, module, "termite_florence_vision_tail_sources_f32");
        const rope_f32 = loadOptionalFunction(ctx, module, "termite_rope_f32");
        const rope_decode_scalars_f32 = loadOptionalFunction(ctx, module, "termite_rope_decode_scalars_f32");
        const decode_scalars_advance = loadOptionalFunction(ctx, module, "termite_decode_scalars_advance");
        const rope_scaled_f32 = loadOptionalFunction(ctx, module, "termite_rope_scaled_f32");
        const rope_scaled_decode_scalars_f32 = loadOptionalFunction(ctx, module, "termite_rope_scaled_decode_scalars_f32");
        const rope_per_item_f32 = loadOptionalFunction(ctx, module, "termite_rope_per_item_f32");
        const rms_norm_heads_rope_f32 = loadOptionalFunction(ctx, module, "termite_rms_norm_heads_rope_f32");
        const rms_norm_heads_rope_decode_scalars_f32 = loadOptionalFunction(ctx, module, "termite_rms_norm_heads_rope_decode_scalars_f32");
        const gqa_attention_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_f32");
        const gqa_attention_decode_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_decode_f32");
        const gqa_attention_decode_scalars_fast_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_decode_scalars_fast_f32");
        const gqa_attention_prefill_fast_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_prefill_fast_f32");
        const gqa_attention_decode_scalars_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_decode_scalars_f32");
        const kv_write_suffix_decode_scalars_f32 = loadOptionalFunction(ctx, module, "termite_kv_write_suffix_decode_scalars_f32");
        const gqa_attention_decode_turboquant_fast_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_decode_turboquant_fast_f32");
        const gqa_attention_prefill_turboquant_fast_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_prefill_turboquant_fast_f32");
        const gqa_attention_prefill_turboquant_tiled_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_prefill_turboquant_tiled_f32");
        const gqa_attention_prefill_turboquant_mma_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_prefill_turboquant_mma_f32");
        const gqa_attention_prefill_turboquant_mma_m32_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_prefill_turboquant_mma_m32_f32");
        const gqa_attention_decode_turboquant_split_stage1_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_decode_turboquant_split_stage1_f32");
        const gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32");
        const gqa_attention_decode_turboquant_split_stage2_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_decode_turboquant_split_stage2_f32");
        const gqa_attention_decode_turboquant_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_decode_turboquant_f32");
        const kv_write_suffix_turboquant_f32 = loadOptionalFunction(ctx, module, "termite_kv_write_suffix_turboquant_f32");
        var deberta_attention_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&deberta_attention_f32, module, "termite_deberta_attention_f32"));
        var split_last_dim3_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&split_last_dim3_f32, module, "termite_split_last_dim3_f32"));
        var linear_q8_0_f32: driver_mod.CUfunction = null;
        const q8_result = ctx.driver.fns.cuModuleGetFunction(&linear_q8_0_f32, module, "termite_linear_q8_0_f32");
        if (q8_result != driver_mod.CUDA_SUCCESS) {
            linear_q8_0_f32 = null;
        } else {
            try ctx.driver.check(q8_result);
        }
        const linear_q8_0_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_f32_tile4");
        const linear_q8_0_gated_down_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_gated_down_f32_tile4");

        const linear_q8_0_f32_tile4_r2 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_f32_tile4_r2");
        const linear_q8_0_bias_f32_tile4_r2 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_f32_tile4_r2");
        const linear_q8_0_bias_gelu_f32_tile4_r2 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_gelu_f32_tile4_r2");
        const linear_q8_0_bias_add_f32_tile4_r2 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_add_f32_tile4_r2");
        const linear_q8_0_f32_fast_r2c8 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_f32_fast_r2c8");
        const linear_q8_0_bias_f32_fast_r2c8 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_f32_fast_r2c8");
        const linear_q8_0_bias_gelu_f32_fast_r2c8 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_gelu_f32_fast_r2c8");
        const linear_q8_0_bias_add_f32_fast_r2c8 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_add_f32_fast_r2c8");
        const linear_q8_0_f32_fast_r4c4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_f32_fast_r4c4");
        const linear_q8_0_bias_f32_fast_r4c4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_f32_fast_r4c4");
        const linear_q8_0_bias_gelu_f32_fast_r4c4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_gelu_f32_fast_r4c4");
        const linear_q8_0_bias_add_f32_fast_r4c4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_add_f32_fast_r4c4");
        const linear_q8_0_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q8_0_f32_tc_hmma");
        const linear_q8_0_bias_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_f32_tc_hmma");
        const linear_q8_0_bias_gelu_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_gelu_f32_tc_hmma");
        const linear_q8_0_bias_add_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q8_0_bias_add_f32_tc_hmma");
        const quantize_f32_q8_1_rows = loadOptionalFunction(ctx, module, "termite_quantize_f32_q8_1_rows");
        const quantize_gated_f32_q8_1_rows = loadOptionalFunction(ctx, module, "termite_quantize_gated_f32_q8_1_rows");
        var linear_q4_0_f32: driver_mod.CUfunction = null;
        const q4_result = ctx.driver.fns.cuModuleGetFunction(&linear_q4_0_f32, module, "termite_linear_q4_0_f32");
        if (q4_result != driver_mod.CUDA_SUCCESS) {
            linear_q4_0_f32 = null;
        } else {
            try ctx.driver.check(q4_result);
        }
        const linear_q4_0_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_f32_tile4");
        const linear_q4_0_f32_tile4_w4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_f32_tile4_w4");
        const linear_q4_0_q8_1_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile4");
        const linear_q4_0_q8_1_f32_tile4_e4b_attn_2048 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile4_e4b_attn_2048");
        const linear_q4_0_q8_1_f32_tile4_e4b_attn_4096 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile4_e4b_attn_4096");
        const linear_q4_0_q8_1_f32_tile4_w8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile4_w8");
        const linear_q4_0_q8_1_f32_tile4_w8_rows2 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile4_w8_rows2");
        const linear_q4_0_q8_1_f32_tile4_w8_rows4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile4_w8_rows4");
        const linear_q4_0_q8_1_f32_tile4_w8_rows8_c4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile4_w8_rows8_c4");
        const linear_q4_0_q8_1_f32_tile4_w8_e4b_down = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down");
        const linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows");
        const linear_q4_0_q8_1_f32_tile4_w10_e4b_down = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile4_w10_e4b_down");
        const linear_q4_0_q8_1_f32_tile8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_q8_1_f32_tile8");
        const linear_q4_0_f32_tile8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_f32_tile8");
        const linear_q4_0_activation_slice_last_dim_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_activation_slice_last_dim_f32_tile4");
        const linear_q4_0_activation_slice_last_dim_f32_tile4_w4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_activation_slice_last_dim_f32_tile4_w4");
        const linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4");
        const linear_q4_0_gated_down_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_gated_down_f32_tile4");
        const linear_q4_0_gated_down_f32_tile4_w4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_gated_down_f32_tile4_w4");
        const linear_q4_0_gated_down_f32_tile8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_gated_down_f32_tile8");
        const linear_q4_0_gated_down_f32_tile16 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_gated_down_f32_tile16");
        var linear_q4_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32, module, "termite_linear_q4_k_f32"));
        var linear_q4_k_bias_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_f32, module, "termite_linear_q4_k_bias_f32"));
        var linear_q4_k_f32_tiled: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32_tiled, module, "termite_linear_q4_k_f32_tiled"));
        var linear_q4_k_bias_f32_tiled: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_f32_tiled, module, "termite_linear_q4_k_bias_f32_tiled"));
        var linear_q4_k_bias_quick_gelu_f32_tiled: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_quick_gelu_f32_tiled, module, "termite_linear_q4_k_bias_quick_gelu_f32_tiled"));
        var linear_q4_k_f32_tile4: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32_tile4, module, "termite_linear_q4_k_f32_tile4"));
        const linear_q6_k_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q6_k_f32_tile4");
        const linear_q4_k_gated_down_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_gated_down_f32_tile4");
        const linear_q6_k_gated_down_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q6_k_gated_down_f32_tile4");
        var linear_q4_k_bias_f32_tile4: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_f32_tile4, module, "termite_linear_q4_k_bias_f32_tile4"));
        var linear_q4_k_bias_f32_tile4_r2: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_f32_tile4_r2, module, "termite_linear_q4_k_bias_f32_tile4_r2"));
        var linear_q4_k_bias_quick_gelu_f32_tile4: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_quick_gelu_f32_tile4, module, "termite_linear_q4_k_bias_quick_gelu_f32_tile4"));
        var linear_q4_k_bias_relu_f32_tile4: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_relu_f32_tile4, module, "termite_linear_q4_k_bias_relu_f32_tile4"));
        var linear_q4_k_bias_relu_f32_tile4_r2: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_relu_f32_tile4_r2, module, "termite_linear_q4_k_bias_relu_f32_tile4_r2"));
        var linear_q4_k_bias_add_f32_tile4: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_bias_add_f32_tile4, module, "termite_linear_q4_k_bias_add_f32_tile4"));

        const linear_q4_k_bias_gelu_f32_tile4_r2 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_gelu_f32_tile4_r2");
        const linear_q4_k_bias_add_f32_tile4_r2 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_add_f32_tile4_r2");
        const linear_q4_k_bias_f32_fast_r2c8 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_f32_fast_r2c8");
        const linear_q4_k_bias_gelu_f32_fast_r2c8 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_gelu_f32_fast_r2c8");
        const linear_q4_k_bias_add_f32_fast_r2c8 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_add_f32_fast_r2c8");
        const linear_q4_k_bias_f32_fast_r4c4 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_f32_fast_r4c4");
        const linear_q4_k_bias_gelu_f32_fast_r4c4 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_gelu_f32_fast_r4c4");
        const linear_q4_k_bias_add_f32_fast_r4c4 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_add_f32_fast_r4c4");
        const linear_q4_k_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q4_k_f32_tc_hmma");
        const linear_q4_k_bias_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_f32_tc_hmma");
        const linear_q4_k_bias_gelu_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_gelu_f32_tc_hmma");
        const linear_q4_k_bias_add_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_add_f32_tc_hmma");
        const linear_q4_k_bias_quick_gelu_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_quick_gelu_f32_tc_hmma");
        const linear_q4_k_bias_relu_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q4_k_bias_relu_f32_tc_hmma");
        const linear_q4_k_triple_bias_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q4_k_triple_bias_f32_tc_hmma");
        const linear_q4_k_pair_bias_f32_tc_hmma = loadOptionalFunction(ctx, module, "termite_linear_q4_k_pair_bias_f32_tc_hmma");
        const linear_q4_k_span_bias_f32_tile8_r2 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_span_bias_f32_tile8_r2");
        const linear_q4_k_span_bias_relu_f32_tile8_r2 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_span_bias_relu_f32_tile8_r2");
        const linear_q4_k_span_bias_f32_tile4_r8 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_span_bias_f32_tile4_r8");
        const linear_q4_k_span_bias_relu_f32_tile4_r8 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_span_bias_relu_f32_tile4_r8");
        const linear_q4_k_span_pair_bias_f32_tile8_r2 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_span_pair_bias_f32_tile8_r2");
        const linear_q4_k_span_pair_bias_relu_f32_tile8_r2 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_span_pair_bias_relu_f32_tile8_r2");
        const linear_q4_k_span_pair2_bias_f32_tile8_r2 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_span_pair2_bias_f32_tile8_r2");
        var linear_q4_k_f32_tile8: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32_tile8, module, "termite_linear_q4_k_f32_tile8"));
        var linear_q4_k_triple_bias_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_triple_bias_f32, module, "termite_linear_q4_k_triple_bias_f32"));
        var linear_q4_k_triple_bias_f32_tiled: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_triple_bias_f32_tiled, module, "termite_linear_q4_k_triple_bias_f32_tiled"));
        var linear_q4_k_pair_bias_f32_tiled: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_pair_bias_f32_tiled, module, "termite_linear_q4_k_pair_bias_f32_tiled"));
        const linear_q8_0_pair_nobias_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_pair_nobias_f32_tile4");
        const linear_q4_0_pair_nobias_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_nobias_f32_tile4");
        const linear_q4_0_pair_nobias_f32_tile4_w4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_nobias_f32_tile4_w4");
        const linear_q4_0_pair_nobias_q8_1_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_nobias_q8_1_f32_tile4");
        const linear_q4_0_pair_nobias_q8_1_f32_tile4_w8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_nobias_q8_1_f32_tile4_w8");
        const linear_q4_0_pair_nobias_q8_1_f32_tile8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_nobias_q8_1_f32_tile8");
        const linear_q4_0_pair_activation_f32_tile4_w4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_f32_tile4_w4");
        const linear_q4_0_pair_activation_q8_1_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_q8_1_f32_tile4");
        const linear_q4_0_pair_activation_q8_1_f32_tile4_w8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8");
        const linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn");
        const linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn");
        const linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn");
        const linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2");
        const linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4");
        const linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2");
        const linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1");
        const linear_q4_0_pair_activation_q8_1_f32_tile8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_activation_q8_1_f32_tile8");
        const linear_q4_0_pair_nobias_f32_tile8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_pair_nobias_f32_tile8");
        const linear_q4_k_pair_nobias_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_pair_nobias_f32_tile4");
        const linear_f32_qkv_nobias_tiled = loadOptionalFunction(ctx, module, "termite_linear_f32_qkv_nobias_tiled");
        const linear_bf16_weight_f32_qkv_nobias_tiled = loadOptionalFunction(ctx, module, "termite_linear_bf16_weight_f32_qkv_nobias_tiled");
        const linear_q8_0_qkv_nobias_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_qkv_nobias_f32_tile4");
        const linear_q4_0_qkv_nobias_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_qkv_nobias_f32_tile4");
        const linear_q4_0_qkv_nobias_f32_tile4_w4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_qkv_nobias_f32_tile4_w4");
        const linear_q4_0_qkv_nobias_q8_1_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_qkv_nobias_q8_1_f32_tile4");
        const linear_q4_0_qkv_nobias_q8_1_f32_tile8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8");
        const linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4");
        const linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8");
        const linear_q4_0_qkv_nobias_f32_tile8 = loadOptionalFunction(ctx, module, "termite_linear_q4_0_qkv_nobias_f32_tile8");
        const linear_q4_k_qkv_nobias_f32_tiled = loadOptionalFunction(ctx, module, "termite_linear_q4_k_qkv_nobias_f32_tiled");
        const linear_q4_k_q4_k_f32_qkv_nobias_tiled = loadOptionalFunction(ctx, module, "termite_linear_q4_k_q4_k_f32_qkv_nobias_tiled");
        var embedding_lookup_q8_0_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_q8_0_f32, module, "termite_embedding_lookup_q8_0_f32"));
        var embedding_lookup_i32_q8_0_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_i32_q8_0_f32, module, "termite_embedding_lookup_i32_q8_0_f32"));
        var embedding_lookup_q4_0_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_q4_0_f32, module, "termite_embedding_lookup_q4_0_f32"));
        var embedding_lookup_i32_q4_0_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_i32_q4_0_f32, module, "termite_embedding_lookup_i32_q4_0_f32"));
        var embedding_lookup_q4_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_q4_k_f32, module, "termite_embedding_lookup_q4_k_f32"));
        var embedding_lookup_i32_q4_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_i32_q4_k_f32, module, "termite_embedding_lookup_i32_q4_k_f32"));
        var embedding_lookup_q6_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_q6_k_f32, module, "termite_embedding_lookup_q6_k_f32"));
        var embedding_lookup_i32_q6_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_i32_q6_k_f32, module, "termite_embedding_lookup_i32_q6_k_f32"));
        var embedding_add_weighted_i32_q6_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_add_weighted_i32_q6_k_f32, module, "termite_embedding_add_weighted_i32_q6_k_f32"));
        const rms_norm_add_weighted_embedding_i32_q6_k_f32 = loadOptionalFunction(ctx, module, "termite_rms_norm_add_weighted_embedding_i32_q6_k_f32");
        var slice_last_dim_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&slice_last_dim_f32, module, "termite_slice_last_dim_f32"));

        return .{
            .module = module,
            .fill_f32 = fill_f32,
            .copy_f32 = copy_f32,
            .copy_u8 = copy_u8,
            .f32_to_bf16 = f32_to_bf16,
            .dequant_q4_0_bf16 = dequant_q4_0_bf16,
            .scale_f32 = scale_f32,
            .add_scalar_f32 = add_scalar_f32,
            .binary_scalar_f32 = binary_scalar_f32,
            .add_mul_scalar_f32 = add_mul_scalar_f32,
            .add_weighted_scalars_f32 = add_weighted_scalars_f32,
            .linear_f32 = linear_f32,
            .linear_f32_tiled = linear_f32_tiled,
            .linear_bf16_weight_f32_tiled = linear_bf16_weight_f32_tiled,
            .argmax_last_row_f32 = argmax_last_row_f32,
            .argmax_rows_f32 = argmax_rows_f32,
            .argmax_rows_suppress_f32 = argmax_rows_suppress_f32,
            .argmax_last_row_suppress_f32 = argmax_last_row_suppress_f32,
            .linear_q8_0_argmax_stage1_tile4 = linear_q8_0_argmax_stage1_tile4,
            .linear_q4_k_argmax_stage1_tile4 = linear_q4_k_argmax_stage1_tile4,
            .argmax_reduce_pairs_f32 = argmax_reduce_pairs_f32,
            .linear_q8_0_argmax_rows_stage1_tile4 = linear_q8_0_argmax_rows_stage1_tile4,
            .linear_q4_0_argmax_rows_stage1_tile4 = linear_q4_0_argmax_rows_stage1_tile4,
            .linear_q4_0_argmax_rows_stage1_tile16 = linear_q4_0_argmax_rows_stage1_tile16,
            .linear_q4_k_argmax_rows_stage1_tile4 = linear_q4_k_argmax_rows_stage1_tile4,
            .linear_q6_k_argmax_rows_stage1_tile4 = linear_q6_k_argmax_rows_stage1_tile4,
            .linear_q6_k_argmax_rows_stage1_tile8 = linear_q6_k_argmax_rows_stage1_tile8,
            .linear_q6_k_argmax_rows_stage1_tile16 = linear_q6_k_argmax_rows_stage1_tile16,
            .linear_q6_k_q8_1_argmax_rows_stage1_tile8 = linear_q6_k_q8_1_argmax_rows_stage1_tile8,
            .linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b = linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b,
            .linear_q6_k_q8_1_f32_tile8_e4b = linear_q6_k_q8_1_f32_tile8_e4b,
            .argmax_reduce_rows_pairs_f32 = argmax_reduce_rows_pairs_f32,
            .argmax_reduce_rows_pairs_f32_w16 = argmax_reduce_rows_pairs_f32_w16,
            .gemma4_mtp_preproject_f32 = gemma4_mtp_preproject_f32,
            .gemma4_mtp_masked_select_f32 = gemma4_mtp_masked_select_f32,
            .gemma4_mtp_masked_select_hidden_f32 = gemma4_mtp_masked_select_hidden_f32,
            .gemma4_mtp_centroid_scores_hidden_f32 = gemma4_mtp_centroid_scores_hidden_f32,
            .gemma4_mtp_centroid_topk_u32 = gemma4_mtp_centroid_topk_u32,
            .gemma4_mtp_restricted_lm_head_scores_f32 = gemma4_mtp_restricted_lm_head_scores_f32,
            .gemma4_mtp_reduce_token_scores_f32 = gemma4_mtp_reduce_token_scores_f32,
            .gemma4_mtp_masked_argmax_f32 = gemma4_mtp_masked_argmax_f32,
            .gemma4_mtp_verify_commit_u32 = gemma4_mtp_verify_commit_u32,
            .linear_bias_f32 = linear_bias_f32,
            .add_bias_rows_f32 = add_bias_rows_f32,
            .linear_bias_f32_tile4_r2 = linear_bias_f32_tile4_r2,
            .linear_bias_relu_f32_tile4_r2 = linear_bias_relu_f32_tile4_r2,
            .linear_bias_gelu_f32_tile4_r2 = linear_bias_gelu_f32_tile4_r2,
            .linear_bias_add_f32_tile4_r2 = linear_bias_add_f32_tile4_r2,
            .linear_pair_bias_f32_tile4_r2 = linear_pair_bias_f32_tile4_r2,
            .linear_triple_bias_f32_tile4_r2 = linear_triple_bias_f32_tile4_r2,
            .rms_norm_f32 = rms_norm_f32,
            .rms_norm_f32_bf16 = rms_norm_f32_bf16,
            .rms_norm_add_f32_bf16 = rms_norm_add_f32_bf16,
            .rms_norm_add_f32 = rms_norm_add_f32,
            .rms_norm_add_mul_scalar_f32 = rms_norm_add_mul_scalar_f32,
            .rms_norm_add_output_scale_f32 = rms_norm_add_output_scale_f32,
            .rms_norm_bare_f32 = rms_norm_bare_f32,
            .layer_norm_f32 = layer_norm_f32,
            .add_layer_norm_f32 = add_layer_norm_f32,
            .elementwise_f32 = elementwise_f32,
            .silu_multiply_f32 = silu_multiply_f32,
            .activation_multiply_f32 = activation_multiply_f32,
            .activation_multiply_slice_last_dim_f32 = activation_multiply_slice_last_dim_f32,
            .embedding_lookup_f32 = embedding_lookup_f32,
            .embedding_lookup_bf16_weight_f32 = embedding_lookup_bf16_weight_f32,
            .embedding_lookup_i32_f32 = embedding_lookup_i32_f32,
            .take_rows_f32 = take_rows_f32,
            .gliner_gather_concat_relu_f32 = gliner_gather_concat_relu_f32,
            .gliner_word_embeddings_f32 = gliner_word_embeddings_f32,
            .repeat_first_row_f32 = repeat_first_row_f32,
            .gliner_gru_combine_f32 = gliner_gru_combine_f32,
            .concat_lastdim_f32 = concat_lastdim_f32,
            .conv2d_f32 = conv2d_f32,
            .attention_f32 = attention_f32,
            .attention_f32_block = attention_f32_block,
            .cross_attention_f32 = cross_attention_f32,
            .cross_attention_q1_f32 = cross_attention_q1_f32,
            .token_to_nchw_f32 = token_to_nchw_f32,
            .nchw_to_token_f32 = nchw_to_token_f32,
            .pack_windows_f32 = pack_windows_f32,
            .unpad_windows_f32 = unpad_windows_f32,
            .channel_scores_softmax_f32 = channel_scores_softmax_f32,
            .channel_apply_f32 = channel_apply_f32,
            .florence_vision_tail_sources_f32 = florence_vision_tail_sources_f32,
            .rope_f32 = rope_f32,
            .rope_decode_scalars_f32 = rope_decode_scalars_f32,
            .decode_scalars_advance = decode_scalars_advance,
            .rope_scaled_f32 = rope_scaled_f32,
            .rope_scaled_decode_scalars_f32 = rope_scaled_decode_scalars_f32,
            .rope_per_item_f32 = rope_per_item_f32,
            .rms_norm_heads_rope_f32 = rms_norm_heads_rope_f32,
            .rms_norm_heads_rope_decode_scalars_f32 = rms_norm_heads_rope_decode_scalars_f32,
            .gqa_attention_f32 = gqa_attention_f32,
            .gqa_attention_decode_f32 = gqa_attention_decode_f32,
            .gqa_attention_decode_scalars_fast_f32 = gqa_attention_decode_scalars_fast_f32,
            .gqa_attention_prefill_fast_f32 = gqa_attention_prefill_fast_f32,
            .gqa_attention_decode_scalars_f32 = gqa_attention_decode_scalars_f32,
            .kv_write_suffix_decode_scalars_f32 = kv_write_suffix_decode_scalars_f32,
            .gqa_attention_decode_turboquant_fast_f32 = gqa_attention_decode_turboquant_fast_f32,
            .gqa_attention_prefill_turboquant_fast_f32 = gqa_attention_prefill_turboquant_fast_f32,
            .gqa_attention_prefill_turboquant_tiled_f32 = gqa_attention_prefill_turboquant_tiled_f32,
            .gqa_attention_prefill_turboquant_mma_f32 = gqa_attention_prefill_turboquant_mma_f32,
            .gqa_attention_prefill_turboquant_mma_m32_f32 = gqa_attention_prefill_turboquant_mma_m32_f32,
            .gqa_attention_decode_turboquant_split_stage1_f32 = gqa_attention_decode_turboquant_split_stage1_f32,
            .gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32 = gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32,
            .gqa_attention_decode_turboquant_split_stage2_f32 = gqa_attention_decode_turboquant_split_stage2_f32,
            .gqa_attention_decode_turboquant_f32 = gqa_attention_decode_turboquant_f32,
            .kv_write_suffix_turboquant_f32 = kv_write_suffix_turboquant_f32,
            .deberta_attention_f32 = deberta_attention_f32,
            .split_last_dim3_f32 = split_last_dim3_f32,
            .linear_q8_0_f32 = linear_q8_0_f32,
            .linear_q8_0_f32_tile4 = linear_q8_0_f32_tile4,
            .linear_q8_0_gated_down_f32_tile4 = linear_q8_0_gated_down_f32_tile4,
            .linear_q8_0_f32_tile4_r2 = linear_q8_0_f32_tile4_r2,
            .linear_q8_0_bias_f32_tile4_r2 = linear_q8_0_bias_f32_tile4_r2,
            .linear_q8_0_bias_gelu_f32_tile4_r2 = linear_q8_0_bias_gelu_f32_tile4_r2,
            .linear_q8_0_bias_add_f32_tile4_r2 = linear_q8_0_bias_add_f32_tile4_r2,
            .linear_q8_0_f32_fast_r2c8 = linear_q8_0_f32_fast_r2c8,
            .linear_q8_0_bias_f32_fast_r2c8 = linear_q8_0_bias_f32_fast_r2c8,
            .linear_q8_0_bias_gelu_f32_fast_r2c8 = linear_q8_0_bias_gelu_f32_fast_r2c8,
            .linear_q8_0_bias_add_f32_fast_r2c8 = linear_q8_0_bias_add_f32_fast_r2c8,
            .linear_q8_0_f32_fast_r4c4 = linear_q8_0_f32_fast_r4c4,
            .linear_q8_0_bias_f32_fast_r4c4 = linear_q8_0_bias_f32_fast_r4c4,
            .linear_q8_0_bias_gelu_f32_fast_r4c4 = linear_q8_0_bias_gelu_f32_fast_r4c4,
            .linear_q8_0_bias_add_f32_fast_r4c4 = linear_q8_0_bias_add_f32_fast_r4c4,
            .linear_q8_0_f32_tc_hmma = linear_q8_0_f32_tc_hmma,
            .linear_q8_0_bias_f32_tc_hmma = linear_q8_0_bias_f32_tc_hmma,
            .linear_q8_0_bias_gelu_f32_tc_hmma = linear_q8_0_bias_gelu_f32_tc_hmma,
            .linear_q8_0_bias_add_f32_tc_hmma = linear_q8_0_bias_add_f32_tc_hmma,
            .quantize_f32_q8_1_rows = quantize_f32_q8_1_rows,
            .quantize_gated_f32_q8_1_rows = quantize_gated_f32_q8_1_rows,
            .linear_q4_0_f32 = linear_q4_0_f32,
            .linear_q4_0_f32_tile4 = linear_q4_0_f32_tile4,
            .linear_q4_0_f32_tile4_w4 = linear_q4_0_f32_tile4_w4,
            .linear_q4_0_q8_1_f32_tile4 = linear_q4_0_q8_1_f32_tile4,
            .linear_q4_0_q8_1_f32_tile4_e4b_attn_2048 = linear_q4_0_q8_1_f32_tile4_e4b_attn_2048,
            .linear_q4_0_q8_1_f32_tile4_e4b_attn_4096 = linear_q4_0_q8_1_f32_tile4_e4b_attn_4096,
            .linear_q4_0_q8_1_f32_tile4_w8 = linear_q4_0_q8_1_f32_tile4_w8,
            .linear_q4_0_q8_1_f32_tile4_w8_rows2 = linear_q4_0_q8_1_f32_tile4_w8_rows2,
            .linear_q4_0_q8_1_f32_tile4_w8_rows4 = linear_q4_0_q8_1_f32_tile4_w8_rows4,
            .linear_q4_0_q8_1_f32_tile4_w8_rows8_c4 = linear_q4_0_q8_1_f32_tile4_w8_rows8_c4,
            .linear_q4_0_q8_1_f32_tile4_w8_e4b_down = linear_q4_0_q8_1_f32_tile4_w8_e4b_down,
            .linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows = linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows,
            .linear_q4_0_q8_1_f32_tile4_w10_e4b_down = linear_q4_0_q8_1_f32_tile4_w10_e4b_down,
            .linear_q4_0_q8_1_f32_tile8 = linear_q4_0_q8_1_f32_tile8,
            .linear_q4_0_f32_tile8 = linear_q4_0_f32_tile8,
            .linear_q4_0_activation_slice_last_dim_f32_tile4 = linear_q4_0_activation_slice_last_dim_f32_tile4,
            .linear_q4_0_activation_slice_last_dim_f32_tile4_w4 = linear_q4_0_activation_slice_last_dim_f32_tile4_w4,
            .linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4 = linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4,
            .linear_q4_0_gated_down_f32_tile4 = linear_q4_0_gated_down_f32_tile4,
            .linear_q4_0_gated_down_f32_tile4_w4 = linear_q4_0_gated_down_f32_tile4_w4,
            .linear_q4_0_gated_down_f32_tile8 = linear_q4_0_gated_down_f32_tile8,
            .linear_q4_0_gated_down_f32_tile16 = linear_q4_0_gated_down_f32_tile16,
            .linear_q4_k_f32 = linear_q4_k_f32,
            .linear_q4_k_bias_f32 = linear_q4_k_bias_f32,
            .linear_q4_k_f32_tiled = linear_q4_k_f32_tiled,
            .linear_q4_k_bias_f32_tiled = linear_q4_k_bias_f32_tiled,
            .linear_q4_k_bias_quick_gelu_f32_tiled = linear_q4_k_bias_quick_gelu_f32_tiled,
            .linear_q4_k_f32_tile4 = linear_q4_k_f32_tile4,
            .linear_q6_k_f32_tile4 = linear_q6_k_f32_tile4,
            .linear_q4_k_gated_down_f32_tile4 = linear_q4_k_gated_down_f32_tile4,
            .linear_q6_k_gated_down_f32_tile4 = linear_q6_k_gated_down_f32_tile4,
            .linear_q4_k_bias_f32_tile4 = linear_q4_k_bias_f32_tile4,
            .linear_q4_k_bias_f32_tile4_r2 = linear_q4_k_bias_f32_tile4_r2,
            .linear_q4_k_bias_quick_gelu_f32_tile4 = linear_q4_k_bias_quick_gelu_f32_tile4,
            .linear_q4_k_bias_relu_f32_tile4 = linear_q4_k_bias_relu_f32_tile4,
            .linear_q4_k_bias_relu_f32_tile4_r2 = linear_q4_k_bias_relu_f32_tile4_r2,
            .linear_q4_k_bias_add_f32_tile4 = linear_q4_k_bias_add_f32_tile4,
            .linear_q4_k_bias_gelu_f32_tile4_r2 = linear_q4_k_bias_gelu_f32_tile4_r2,
            .linear_q4_k_bias_add_f32_tile4_r2 = linear_q4_k_bias_add_f32_tile4_r2,
            .linear_q4_k_bias_f32_fast_r2c8 = linear_q4_k_bias_f32_fast_r2c8,
            .linear_q4_k_bias_gelu_f32_fast_r2c8 = linear_q4_k_bias_gelu_f32_fast_r2c8,
            .linear_q4_k_bias_add_f32_fast_r2c8 = linear_q4_k_bias_add_f32_fast_r2c8,
            .linear_q4_k_bias_f32_fast_r4c4 = linear_q4_k_bias_f32_fast_r4c4,
            .linear_q4_k_bias_gelu_f32_fast_r4c4 = linear_q4_k_bias_gelu_f32_fast_r4c4,
            .linear_q4_k_bias_add_f32_fast_r4c4 = linear_q4_k_bias_add_f32_fast_r4c4,
            .linear_q4_k_f32_tc_hmma = linear_q4_k_f32_tc_hmma,
            .linear_q4_k_bias_f32_tc_hmma = linear_q4_k_bias_f32_tc_hmma,
            .linear_q4_k_bias_gelu_f32_tc_hmma = linear_q4_k_bias_gelu_f32_tc_hmma,
            .linear_q4_k_bias_add_f32_tc_hmma = linear_q4_k_bias_add_f32_tc_hmma,
            .linear_q4_k_bias_quick_gelu_f32_tc_hmma = linear_q4_k_bias_quick_gelu_f32_tc_hmma,
            .linear_q4_k_bias_relu_f32_tc_hmma = linear_q4_k_bias_relu_f32_tc_hmma,
            .linear_q4_k_triple_bias_f32_tc_hmma = linear_q4_k_triple_bias_f32_tc_hmma,
            .linear_q4_k_pair_bias_f32_tc_hmma = linear_q4_k_pair_bias_f32_tc_hmma,
            .linear_q4_k_span_bias_f32_tile8_r2 = linear_q4_k_span_bias_f32_tile8_r2,
            .linear_q4_k_span_bias_relu_f32_tile8_r2 = linear_q4_k_span_bias_relu_f32_tile8_r2,
            .linear_q4_k_span_bias_f32_tile4_r8 = linear_q4_k_span_bias_f32_tile4_r8,
            .linear_q4_k_span_bias_relu_f32_tile4_r8 = linear_q4_k_span_bias_relu_f32_tile4_r8,
            .linear_q4_k_span_pair_bias_f32_tile8_r2 = linear_q4_k_span_pair_bias_f32_tile8_r2,
            .linear_q4_k_span_pair_bias_relu_f32_tile8_r2 = linear_q4_k_span_pair_bias_relu_f32_tile8_r2,
            .linear_q4_k_span_pair2_bias_f32_tile8_r2 = linear_q4_k_span_pair2_bias_f32_tile8_r2,
            .linear_q4_k_f32_tile8 = linear_q4_k_f32_tile8,
            .linear_q4_k_triple_bias_f32 = linear_q4_k_triple_bias_f32,
            .linear_q4_k_triple_bias_f32_tiled = linear_q4_k_triple_bias_f32_tiled,
            .linear_q4_k_pair_bias_f32_tiled = linear_q4_k_pair_bias_f32_tiled,
            .linear_q8_0_pair_nobias_f32_tile4 = linear_q8_0_pair_nobias_f32_tile4,
            .linear_q4_0_pair_nobias_f32_tile4 = linear_q4_0_pair_nobias_f32_tile4,
            .linear_q4_0_pair_nobias_f32_tile4_w4 = linear_q4_0_pair_nobias_f32_tile4_w4,
            .linear_q4_0_pair_nobias_q8_1_f32_tile4 = linear_q4_0_pair_nobias_q8_1_f32_tile4,
            .linear_q4_0_pair_nobias_q8_1_f32_tile4_w8 = linear_q4_0_pair_nobias_q8_1_f32_tile4_w8,
            .linear_q4_0_pair_nobias_q8_1_f32_tile8 = linear_q4_0_pair_nobias_q8_1_f32_tile8,
            .linear_q4_0_pair_activation_f32_tile4_w4 = linear_q4_0_pair_activation_f32_tile4_w4,
            .linear_q4_0_pair_activation_q8_1_f32_tile4 = linear_q4_0_pair_activation_q8_1_f32_tile4,
            .linear_q4_0_pair_activation_q8_1_f32_tile4_w8 = linear_q4_0_pair_activation_q8_1_f32_tile4_w8,
            .linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn = linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn,
            .linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn = linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn,
            .linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn = linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn,
            .linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2 = linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2,
            .linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4 = linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4,
            .linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2 = linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2,
            .linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1 = linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1,
            .linear_q4_0_pair_activation_q8_1_f32_tile8 = linear_q4_0_pair_activation_q8_1_f32_tile8,
            .linear_q4_0_pair_nobias_f32_tile8 = linear_q4_0_pair_nobias_f32_tile8,
            .linear_q4_k_pair_nobias_f32_tile4 = linear_q4_k_pair_nobias_f32_tile4,
            .linear_f32_qkv_nobias_tiled = linear_f32_qkv_nobias_tiled,
            .linear_bf16_weight_f32_qkv_nobias_tiled = linear_bf16_weight_f32_qkv_nobias_tiled,
            .linear_q8_0_qkv_nobias_f32_tile4 = linear_q8_0_qkv_nobias_f32_tile4,
            .linear_q4_0_qkv_nobias_f32_tile4 = linear_q4_0_qkv_nobias_f32_tile4,
            .linear_q4_0_qkv_nobias_f32_tile4_w4 = linear_q4_0_qkv_nobias_f32_tile4_w4,
            .linear_q4_0_qkv_nobias_q8_1_f32_tile4 = linear_q4_0_qkv_nobias_q8_1_f32_tile4,
            .linear_q4_0_qkv_nobias_q8_1_f32_tile8 = linear_q4_0_qkv_nobias_q8_1_f32_tile8,
            .linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4 = linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4,
            .linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8 = linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8,
            .linear_q4_0_qkv_nobias_f32_tile8 = linear_q4_0_qkv_nobias_f32_tile8,
            .linear_q4_k_qkv_nobias_f32_tiled = linear_q4_k_qkv_nobias_f32_tiled,
            .linear_q4_k_q4_k_f32_qkv_nobias_tiled = linear_q4_k_q4_k_f32_qkv_nobias_tiled,
            .embedding_lookup_q8_0_f32 = embedding_lookup_q8_0_f32,
            .embedding_lookup_i32_q8_0_f32 = embedding_lookup_i32_q8_0_f32,
            .embedding_lookup_q4_0_f32 = embedding_lookup_q4_0_f32,
            .embedding_lookup_i32_q4_0_f32 = embedding_lookup_i32_q4_0_f32,
            .embedding_lookup_q4_k_f32 = embedding_lookup_q4_k_f32,
            .embedding_lookup_i32_q4_k_f32 = embedding_lookup_i32_q4_k_f32,
            .embedding_lookup_q6_k_f32 = embedding_lookup_q6_k_f32,
            .embedding_lookup_i32_q6_k_f32 = embedding_lookup_i32_q6_k_f32,
            .embedding_add_weighted_i32_q6_k_f32 = embedding_add_weighted_i32_q6_k_f32,
            .rms_norm_add_weighted_embedding_i32_q6_k_f32 = rms_norm_add_weighted_embedding_i32_q6_k_f32,
            .slice_last_dim_f32 = slice_last_dim_f32,
        };
    }

    pub fn unload(self: *KernelModule, ctx: *context_mod.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.module = null;
            self.fill_f32 = null;
            self.f32_to_bf16 = null;
            self.dequant_q4_0_bf16 = null;
            self.scale_f32 = null;
            self.add_scalar_f32 = null;
            self.binary_scalar_f32 = null;
            self.add_mul_scalar_f32 = null;
            self.add_weighted_scalars_f32 = null;
            self.linear_f32 = null;
            self.linear_f32_tiled = null;
            self.linear_bf16_weight_f32_tiled = null;
            self.argmax_last_row_f32 = null;
            self.argmax_rows_f32 = null;
            self.argmax_rows_suppress_f32 = null;
            self.argmax_last_row_suppress_f32 = null;
            self.linear_q8_0_argmax_stage1_tile4 = null;
            self.linear_q4_k_argmax_stage1_tile4 = null;
            self.argmax_reduce_pairs_f32 = null;
            self.linear_q8_0_argmax_rows_stage1_tile4 = null;
            self.linear_q4_0_argmax_rows_stage1_tile4 = null;
            self.linear_q4_0_argmax_rows_stage1_tile16 = null;
            self.linear_q4_k_argmax_rows_stage1_tile4 = null;
            self.linear_q6_k_argmax_rows_stage1_tile4 = null;
            self.linear_q6_k_argmax_rows_stage1_tile8 = null;
            self.linear_q6_k_argmax_rows_stage1_tile16 = null;
            self.linear_q6_k_q8_1_argmax_rows_stage1_tile8 = null;
            self.linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b = null;
            self.linear_q6_k_q8_1_f32_tile8_e4b = null;
            self.argmax_reduce_rows_pairs_f32 = null;
            self.argmax_reduce_rows_pairs_f32_w16 = null;
            self.gemma4_mtp_preproject_f32 = null;
            self.gemma4_mtp_masked_select_f32 = null;
            self.gemma4_mtp_masked_select_hidden_f32 = null;
            self.gemma4_mtp_centroid_scores_hidden_f32 = null;
            self.gemma4_mtp_centroid_topk_u32 = null;
            self.gemma4_mtp_restricted_lm_head_scores_f32 = null;
            self.gemma4_mtp_reduce_token_scores_f32 = null;
            self.gemma4_mtp_masked_argmax_f32 = null;
            self.gemma4_mtp_verify_commit_u32 = null;
            self.linear_bias_f32 = null;
            self.add_bias_rows_f32 = null;
            self.linear_bias_f32_tile4_r2 = null;
            self.linear_bias_relu_f32_tile4_r2 = null;
            self.linear_bias_gelu_f32_tile4_r2 = null;
            self.linear_bias_add_f32_tile4_r2 = null;
            self.linear_pair_bias_f32_tile4_r2 = null;
            self.linear_triple_bias_f32_tile4_r2 = null;
            self.rms_norm_f32 = null;
            self.rms_norm_f32_bf16 = null;
            self.rms_norm_add_f32_bf16 = null;
            self.rms_norm_add_mul_scalar_f32 = null;
            self.rms_norm_add_output_scale_f32 = null;
            self.rms_norm_bare_f32 = null;
            self.layer_norm_f32 = null;
            self.add_layer_norm_f32 = null;
            self.elementwise_f32 = null;
            self.silu_multiply_f32 = null;
            self.activation_multiply_f32 = null;
            self.activation_multiply_slice_last_dim_f32 = null;
            self.embedding_lookup_f32 = null;
            self.embedding_lookup_bf16_weight_f32 = null;
            self.embedding_lookup_i32_f32 = null;
            self.take_rows_f32 = null;
            self.gliner_gather_concat_relu_f32 = null;
            self.gliner_word_embeddings_f32 = null;
            self.repeat_first_row_f32 = null;
            self.gliner_gru_combine_f32 = null;
            self.concat_lastdim_f32 = null;
            self.conv2d_f32 = null;
            self.attention_f32 = null;
            self.attention_f32_block = null;
            self.cross_attention_f32 = null;
            self.cross_attention_q1_f32 = null;
            self.token_to_nchw_f32 = null;
            self.nchw_to_token_f32 = null;
            self.pack_windows_f32 = null;
            self.unpad_windows_f32 = null;
            self.channel_scores_softmax_f32 = null;
            self.channel_apply_f32 = null;
            self.florence_vision_tail_sources_f32 = null;
            self.rope_f32 = null;
            self.rope_decode_scalars_f32 = null;
            self.rope_scaled_f32 = null;
            self.rope_scaled_decode_scalars_f32 = null;
            self.rope_per_item_f32 = null;
            self.rms_norm_heads_rope_f32 = null;
            self.rms_norm_heads_rope_decode_scalars_f32 = null;
            self.gqa_attention_f32 = null;
            self.gqa_attention_decode_f32 = null;
            self.gqa_attention_decode_scalars_fast_f32 = null;
            self.gqa_attention_prefill_fast_f32 = null;
            self.gqa_attention_decode_scalars_f32 = null;
            self.kv_write_suffix_decode_scalars_f32 = null;
            self.gqa_attention_decode_turboquant_fast_f32 = null;
            self.gqa_attention_prefill_turboquant_fast_f32 = null;
            self.gqa_attention_prefill_turboquant_tiled_f32 = null;
            self.gqa_attention_prefill_turboquant_mma_f32 = null;
            self.gqa_attention_prefill_turboquant_mma_m32_f32 = null;
            self.gqa_prefill_mma_smem_opted_in = false;
            self.gqa_prefill_mma_m32_smem_opted_in = false;
            self.gqa_attention_decode_turboquant_split_stage1_f32 = null;
            self.gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32 = null;
            self.gqa_attention_decode_turboquant_split_stage2_f32 = null;
            self.gqa_attention_decode_turboquant_f32 = null;
            self.kv_write_suffix_turboquant_f32 = null;
            self.deberta_attention_f32 = null;
            self.split_last_dim3_f32 = null;
            self.linear_q8_0_f32 = null;
            self.linear_q8_0_f32_tile4 = null;
            self.linear_q8_0_gated_down_f32_tile4 = null;
            self.linear_q8_0_f32_tile4_r2 = null;
            self.linear_q8_0_bias_f32_tile4_r2 = null;
            self.linear_q8_0_bias_gelu_f32_tile4_r2 = null;
            self.linear_q8_0_bias_add_f32_tile4_r2 = null;
            self.linear_q8_0_f32_fast_r2c8 = null;
            self.linear_q8_0_bias_f32_fast_r2c8 = null;
            self.linear_q8_0_bias_gelu_f32_fast_r2c8 = null;
            self.linear_q8_0_bias_add_f32_fast_r2c8 = null;
            self.linear_q8_0_f32_fast_r4c4 = null;
            self.linear_q8_0_bias_f32_fast_r4c4 = null;
            self.linear_q8_0_bias_gelu_f32_fast_r4c4 = null;
            self.linear_q8_0_bias_add_f32_fast_r4c4 = null;
            self.linear_q8_0_f32_tc_hmma = null;
            self.linear_q8_0_bias_f32_tc_hmma = null;
            self.linear_q8_0_bias_gelu_f32_tc_hmma = null;
            self.linear_q8_0_bias_add_f32_tc_hmma = null;
            self.quantize_f32_q8_1_rows = null;
            self.quantize_gated_f32_q8_1_rows = null;
            self.linear_q4_0_f32 = null;
            self.linear_q4_0_f32_tile4 = null;
            self.linear_q4_0_f32_tile4_w4 = null;
            self.linear_q4_0_q8_1_f32_tile4 = null;
            self.linear_q4_0_q8_1_f32_tile4_e4b_attn_2048 = null;
            self.linear_q4_0_q8_1_f32_tile4_e4b_attn_4096 = null;
            self.linear_q4_0_q8_1_f32_tile4_w8 = null;
            self.linear_q4_0_q8_1_f32_tile4_w8_rows2 = null;
            self.linear_q4_0_q8_1_f32_tile4_w8_rows4 = null;
            self.linear_q4_0_q8_1_f32_tile4_w8_rows8_c4 = null;
            self.linear_q4_0_q8_1_f32_tile4_w8_e4b_down = null;
            self.linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows = null;
            self.linear_q4_0_q8_1_f32_tile4_w10_e4b_down = null;
            self.linear_q4_0_q8_1_f32_tile8 = null;
            self.linear_q4_0_f32_tile8 = null;
            self.linear_q4_0_activation_slice_last_dim_f32_tile4 = null;
            self.linear_q4_0_activation_slice_last_dim_f32_tile4_w4 = null;
            self.linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4 = null;
            self.linear_q4_0_gated_down_f32_tile4 = null;
            self.linear_q4_0_gated_down_f32_tile4_w4 = null;
            self.linear_q4_0_gated_down_f32_tile8 = null;
            self.linear_q4_0_gated_down_f32_tile16 = null;
            self.linear_q4_k_f32 = null;
            self.linear_q4_k_bias_f32 = null;
            self.linear_q4_k_f32_tiled = null;
            self.linear_q4_k_bias_f32_tiled = null;
            self.linear_q4_k_bias_quick_gelu_f32_tiled = null;
            self.linear_q4_k_f32_tile4 = null;
            self.linear_q6_k_f32_tile4 = null;
            self.linear_q4_k_gated_down_f32_tile4 = null;
            self.linear_q6_k_gated_down_f32_tile4 = null;
            self.linear_q4_k_bias_f32_tile4 = null;
            self.linear_q4_k_bias_f32_tile4_r2 = null;
            self.linear_q4_k_bias_quick_gelu_f32_tile4 = null;
            self.linear_q4_k_bias_relu_f32_tile4 = null;
            self.linear_q4_k_bias_relu_f32_tile4_r2 = null;
            self.linear_q4_k_bias_add_f32_tile4 = null;
            self.linear_q4_k_bias_gelu_f32_tile4_r2 = null;
            self.linear_q4_k_bias_add_f32_tile4_r2 = null;
            self.linear_q4_k_bias_f32_fast_r2c8 = null;
            self.linear_q4_k_bias_gelu_f32_fast_r2c8 = null;
            self.linear_q4_k_bias_add_f32_fast_r2c8 = null;
            self.linear_q4_k_bias_f32_fast_r4c4 = null;
            self.linear_q4_k_bias_gelu_f32_fast_r4c4 = null;
            self.linear_q4_k_bias_add_f32_fast_r4c4 = null;
            self.linear_q4_k_f32_tc_hmma = null;
            self.linear_q4_k_bias_f32_tc_hmma = null;
            self.linear_q4_k_bias_gelu_f32_tc_hmma = null;
            self.linear_q4_k_bias_add_f32_tc_hmma = null;
            self.linear_q4_k_bias_quick_gelu_f32_tc_hmma = null;
            self.linear_q4_k_bias_relu_f32_tc_hmma = null;
            self.linear_q4_k_triple_bias_f32_tc_hmma = null;
            self.linear_q4_k_pair_bias_f32_tc_hmma = null;
            self.linear_q4_k_span_bias_f32_tile8_r2 = null;
            self.linear_q4_k_span_bias_relu_f32_tile8_r2 = null;
            self.linear_q4_k_span_bias_f32_tile4_r8 = null;
            self.linear_q4_k_span_bias_relu_f32_tile4_r8 = null;
            self.linear_q4_k_span_pair_bias_f32_tile8_r2 = null;
            self.linear_q4_k_span_pair_bias_relu_f32_tile8_r2 = null;
            self.linear_q4_k_span_pair2_bias_f32_tile8_r2 = null;
            self.linear_q4_k_f32_tile8 = null;
            self.linear_q4_k_triple_bias_f32 = null;
            self.linear_q4_k_triple_bias_f32_tiled = null;
            self.linear_q4_k_pair_bias_f32_tiled = null;
            self.linear_q8_0_pair_nobias_f32_tile4 = null;
            self.linear_q4_0_pair_nobias_f32_tile4 = null;
            self.linear_q4_0_pair_nobias_f32_tile4_w4 = null;
            self.linear_q4_0_pair_nobias_q8_1_f32_tile4 = null;
            self.linear_q4_0_pair_nobias_q8_1_f32_tile4_w8 = null;
            self.linear_q4_0_pair_nobias_q8_1_f32_tile8 = null;
            self.linear_q4_0_pair_activation_f32_tile4_w4 = null;
            self.linear_q4_0_pair_activation_q8_1_f32_tile4 = null;
            self.linear_q4_0_pair_activation_q8_1_f32_tile4_w8 = null;
            self.linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn = null;
            self.linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn = null;
            self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn = null;
            self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2 = null;
            self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4 = null;
            self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2 = null;
            self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1 = null;
            self.linear_q4_0_pair_activation_q8_1_f32_tile8 = null;
            self.linear_q4_0_pair_nobias_f32_tile8 = null;
            self.linear_q4_k_pair_nobias_f32_tile4 = null;
            self.linear_f32_qkv_nobias_tiled = null;
            self.linear_bf16_weight_f32_qkv_nobias_tiled = null;
            self.linear_q8_0_qkv_nobias_f32_tile4 = null;
            self.linear_q4_0_qkv_nobias_f32_tile4 = null;
            self.linear_q4_0_qkv_nobias_f32_tile4_w4 = null;
            self.linear_q4_0_qkv_nobias_q8_1_f32_tile4 = null;
            self.linear_q4_0_qkv_nobias_q8_1_f32_tile8 = null;
            self.linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4 = null;
            self.linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8 = null;
            self.linear_q4_0_qkv_nobias_f32_tile8 = null;
            self.linear_q4_k_qkv_nobias_f32_tiled = null;
            self.linear_q4_k_q4_k_f32_qkv_nobias_tiled = null;
            self.embedding_lookup_q8_0_f32 = null;
            self.embedding_lookup_i32_q8_0_f32 = null;
            self.embedding_lookup_q4_0_f32 = null;
            self.embedding_lookup_i32_q4_0_f32 = null;
            self.embedding_lookup_q4_k_f32 = null;
            self.embedding_lookup_i32_q4_k_f32 = null;
            self.embedding_lookup_q6_k_f32 = null;
            self.embedding_lookup_i32_q6_k_f32 = null;
            self.embedding_add_weighted_i32_q6_k_f32 = null;
            self.rms_norm_add_weighted_embedding_i32_q6_k_f32 = null;
            self.slice_last_dim_f32 = null;
        }
    }

    pub fn hasGemma4DecoderPrimitives(self: *const KernelModule) bool {
        return self.rope_f32 != null and
            self.rope_per_item_f32 != null and
            self.gqa_attention_f32 != null and
            self.add_mul_scalar_f32 != null and
            self.add_weighted_scalars_f32 != null and
            self.rms_norm_add_mul_scalar_f32 != null and
            self.rms_norm_heads_rope_f32 != null and
            self.gemma4_mtp_masked_argmax_f32 != null;
    }

    pub fn hasBf16WeightPrimitives(self: *const KernelModule) bool {
        return self.linear_bf16_weight_f32_tiled != null and
            self.embedding_lookup_bf16_weight_f32 != null and
            self.linear_bf16_weight_f32_qkv_nobias_tiled != null;
    }

    pub fn hasClipClapPrimitives(self: *const KernelModule) bool {
        return self.linear_f32 != null and
            self.linear_bias_f32 != null and
            self.linear_bias_f32_tile4_r2 != null and
            self.linear_bias_relu_f32_tile4_r2 != null and
            self.linear_bias_gelu_f32_tile4_r2 != null and
            self.rms_norm_f32 != null and
            self.layer_norm_f32 != null and
            self.elementwise_f32 != null and
            self.embedding_lookup_f32 != null and
            self.concat_lastdim_f32 != null and
            self.conv2d_f32 != null and
            self.attention_f32 != null and
            self.attention_f32_block != null;
    }

    pub fn hasDebertaRerankerPrimitives(self: *const KernelModule) bool {
        return self.hasClipClapPrimitives() and
            self.take_rows_f32 != null and
            self.deberta_attention_f32 != null and
            self.split_last_dim3_f32 != null;
    }

    pub fn hasGliner2Primitives(self: *const KernelModule) bool {
        return self.hasDebertaRerankerPrimitives() and
            self.gliner_word_embeddings_f32 != null and
            self.repeat_first_row_f32 != null and
            self.gliner_gru_combine_f32 != null;
    }

    pub fn hasFlorence2Primitives(self: *const KernelModule) bool {
        return self.hasClipClapPrimitives() and
            self.hasQuantMatmulMvpPrimitives() and
            self.hasQ4KTensorCorePrimitives() and
            self.split_last_dim3_f32 != null and
            self.cross_attention_f32 != null and
            self.cross_attention_q1_f32 != null and
            self.token_to_nchw_f32 != null and
            self.nchw_to_token_f32 != null and
            self.pack_windows_f32 != null and
            self.unpad_windows_f32 != null and
            self.channel_scores_softmax_f32 != null and
            self.channel_apply_f32 != null and
            self.florence_vision_tail_sources_f32 != null;
    }

    pub fn hasGlinerSpanQ4KPrimitives(self: *const KernelModule) bool {
        return self.linear_q4_k_span_bias_f32_tile8_r2 != null and
            self.linear_q4_k_span_bias_relu_f32_tile8_r2 != null and
            self.linear_q4_k_span_bias_f32_tile4_r8 != null and
            self.linear_q4_k_span_bias_relu_f32_tile4_r8 != null and
            self.linear_q4_k_span_pair_bias_f32_tile8_r2 != null and
            self.linear_q4_k_span_pair_bias_relu_f32_tile8_r2 != null and
            self.linear_q4_k_span_pair2_bias_f32_tile8_r2 != null;
    }

    pub fn hasQ4KTensorCorePrimitives(self: *const KernelModule) bool {
        return self.linear_q4_k_f32_tc_hmma != null and
            self.linear_q4_k_bias_f32_tc_hmma != null and
            self.linear_q4_k_bias_gelu_f32_tc_hmma != null and
            self.linear_q4_k_bias_add_f32_tc_hmma != null and
            self.linear_q4_k_bias_quick_gelu_f32_tc_hmma != null and
            self.linear_q4_k_bias_relu_f32_tc_hmma != null and
            self.linear_q4_k_triple_bias_f32_tc_hmma != null and
            self.linear_q4_k_pair_bias_f32_tc_hmma != null;
    }

    pub fn hasQuantMatmulMvpPrimitives(self: *const KernelModule) bool {
        return self.linear_q8_0_f32 != null and
            self.linear_q4_0_f32 != null and
            self.linear_q4_k_f32 != null and
            self.linear_q4_k_bias_f32 != null and
            self.linear_q4_k_f32_tiled != null and
            self.linear_q4_k_bias_f32_tiled != null and
            self.linear_q4_k_f32_tile4 != null and
            self.linear_q4_k_bias_f32_tile4 != null and
            self.linear_q4_k_bias_f32_tile4_r2 != null and
            self.linear_q4_k_f32_tile8 != null and
            self.linear_q4_k_pair_bias_f32_tiled != null and
            self.linear_q4_k_triple_bias_f32_tiled != null and
            self.embedding_lookup_q8_0_f32 != null and
            self.embedding_lookup_q4_0_f32 != null and
            self.embedding_lookup_q4_k_f32 != null and
            self.embedding_lookup_q6_k_f32 != null and
            self.slice_last_dim_f32 != null;
    }

    pub fn q4_0Q8_1Tile4W8PrefillRowsVariant(self: *const KernelModule, rows: usize, in_dim: usize, out_dim: usize) Q4_0Q8_1PrefillRowsVariant {
        if (rows <= 1) return .single;
        if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_ROWS8_C4", false) and
            self.linear_q4_0_q8_1_f32_tile4_w8_rows8_c4 != null) return .rows8_c4;
        if (self.linear_q4_0_q8_1_f32_tile4_w8_rows4 != null) return .rows4;
        if (self.linear_q4_0_q8_1_f32_tile4_w8_rows2 != null) return .rows2;
        if (in_dim == 10240 and out_dim == 2560 and self.linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows != null) return .e4b_down_rows;
        if (self.linear_q4_0_q8_1_f32_tile4_w8 != null) return .generic_rows;
        return .none;
    }

    pub fn q4_0PairActivationQ8_1Tile32W5E4BPrefillRowsVariant(self: *const KernelModule, rows: usize) Q4_0Q8_1PrefillRowsVariant {
        if (rows <= 1) return .single;
        if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS16_C1", false) and
            self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1 != null) return .rows16_c1;
        if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS8_C2", false) and
            self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2 != null) return .rows8_c2;
        if (self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4 != null) return .rows4;
        if (self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2 != null) return .rows2;
        if (self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn != null) return .generic_rows;
        return .none;
    }

    pub fn q4_0QkvNoBiasQ8_1Tile8PrefillRowsVariant(self: *const KernelModule, rows: usize) Q4_0Q8_1PrefillRowsVariant {
        if (rows <= 1) return .single;
        if (self.linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4 != null) return .rows4;
        if (self.linear_q4_0_qkv_nobias_q8_1_f32_tile8 != null) return .tile8_rows;
        return .none;
    }

    pub fn requireGemma4DecoderPrimitives(self: *const KernelModule) driver_mod.Error!void {
        if (!self.hasGemma4DecoderPrimitives()) return error.CudaKernelUnavailable;
    }

    pub fn requireFlorence2Primitives(self: *const KernelModule) driver_mod.Error!void {
        if (!self.hasFlorence2Primitives()) return error.CudaKernelUnavailable;
    }

    pub fn launchFillF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        count: usize,
        value: f32,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        if (count == 0) return;
        var dst_ptr = dst.ptr;
        var n = try toU32(count);
        var fill_value = value;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&n),
            @ptrCast(&fill_value),
        };
        const block: c_uint = 256;
        const grid: c_uint = try toU32((count + block - 1) / block);
        try ctx.makeCurrent();
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
            self.fill_f32,
            grid,
            1,
            1,
            block,
            1,
            1,
            0,
            ctx.stream,
            &params,
            null,
        ));
        ctx.noteKernelLaunch();
    }

    pub fn launchCopyF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        src: buffer_mod.DeviceBuffer,
        count: usize,
    ) driver_mod.Error!void {
        const function = self.copy_f32 orelse return error.CudaKernelUnavailable;
        try checkBytes(dst, count);
        try checkBytes(src, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var src_ptr = src.ptr;
        var n = try toU32(count);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&src_ptr),
            @ptrCast(&n),
        };
        try launch1d(function, ctx, count, &params);
    }

    pub fn launchCopyBytes(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        src: buffer_mod.DeviceBuffer,
        len: usize,
    ) driver_mod.Error!void {
        const function = self.copy_u8 orelse return error.CudaKernelUnavailable;
        try checkRawBytes(dst, len);
        try checkRawBytes(src, len);
        if (len == 0) return;

        var dst_ptr = dst.ptr;
        var src_ptr = src.ptr;
        var n = try toU32(len);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&src_ptr),
            @ptrCast(&n),
        };
        try launch1d(function, ctx, len, &params);
    }

    pub fn launchLinearF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(weight, try checkedTensorElements(out_dim, in_dim));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch1d(self.linear_f32, ctx, out_count, &params);
    }

    pub fn launchLinearF32Tiled(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_f32_tiled orelse return error.CudaKernelUnavailable;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(weight, try checkedTensorElements(out_dim, in_dim));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, out_count, f32_tiled_threads, &params);
    }

    pub fn launchDequantQ4_0Bf16(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        src: buffer_mod.DeviceBuffer,
        block_count: usize,
    ) driver_mod.Error!void {
        const function = self.dequant_q4_0_bf16 orelse return error.CudaKernelUnavailable;
        try checkRawBytes(dst, try checkedTensorElements(block_count, 32 * @sizeOf(u16)));
        try checkRawBytes(src, try checkedTensorElements(block_count, 18));
        if (block_count == 0) return;

        var dst_ptr = dst.ptr;
        var src_ptr = src.ptr;
        var block_count_u32 = try toU32(block_count);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&src_ptr),
            @ptrCast(&block_count_u32),
        };
        const threads: usize = 256;
        const max_grid: usize = 65535;
        const blocks = @min((block_count + threads - 1) / threads, max_grid);
        try launchBlocks(function, ctx, blocks, threads, &params);
    }

    pub fn launchF32ToBf16(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        count: usize,
    ) driver_mod.Error!void {
        const function = self.f32_to_bf16 orelse return error.CudaKernelUnavailable;
        try checkRawBytes(dst, try checkedTensorElements(count, @sizeOf(u16)));
        try checkBytes(input, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var count_u32 = try toU32(count);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&count_u32),
        };
        try launch1d(function, ctx, count, &params);
    }

    pub fn launchLinearBf16WeightF32Tiled(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_bf16_weight_f32_tiled orelse return error.CudaKernelUnavailable;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight, try checkedTensorElements(try checkedTensorElements(out_dim, in_dim), @sizeOf(u16)));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, out_count, f32_tiled_threads, &params);
    }

    pub fn launchScaleF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        count: usize,
        scale: f32,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        try checkBytes(input, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var count_u32 = try toU32(count);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&count_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.scale_f32, ctx, count, &params);
    }

    pub fn launchAddScalarF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        count: usize,
        value: f32,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        try checkBytes(input, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var count_u32 = try toU32(count);
        var add_value = value;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&count_u32),
            @ptrCast(&add_value),
        };
        try launch1d(self.add_scalar_f32, ctx, count, &params);
    }

    pub fn launchBinaryScalarF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        scalar: buffer_mod.DeviceBuffer,
        count: usize,
        op: ElementwiseOp,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkBytes(scalar, 1);
        if (count == 0) return;
        if (op != .add and op != .multiply) return error.InvalidCudaState;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var scalar_ptr = scalar.ptr;
        var count_u32 = try toU32(count);
        var op_u32: u32 = @intFromEnum(op);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&scalar_ptr),
            @ptrCast(&count_u32),
            @ptrCast(&op_u32),
        };
        try launch1d(self.binary_scalar_f32, ctx, count, &params);
    }

    pub fn launchAddMulScalarF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        a: buffer_mod.DeviceBuffer,
        b: buffer_mod.DeviceBuffer,
        scalar: buffer_mod.DeviceBuffer,
        count: usize,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        try checkBytes(a, count);
        try checkBytes(b, count);
        try checkBytes(scalar, 1);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var a_ptr = a.ptr;
        var b_ptr = b.ptr;
        var scalar_ptr = scalar.ptr;
        var count_u32 = try toU32(count);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&a_ptr),
            @ptrCast(&b_ptr),
            @ptrCast(&scalar_ptr),
            @ptrCast(&count_u32),
        };
        try launch1d(self.add_mul_scalar_f32, ctx, count, &params);
    }

    pub fn launchAddWeightedScalarsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        a: buffer_mod.DeviceBuffer,
        b: buffer_mod.DeviceBuffer,
        scale_a: f32,
        scale_b: f32,
        count: usize,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        try checkBytes(a, count);
        try checkBytes(b, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var a_ptr = a.ptr;
        var b_ptr = b.ptr;
        var scale_a_value = scale_a;
        var scale_b_value = scale_b;
        var count_u32 = try toU32(count);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&a_ptr),
            @ptrCast(&b_ptr),
            @ptrCast(&scale_a_value),
            @ptrCast(&scale_b_value),
            @ptrCast(&count_u32),
        };
        try launch1d(self.add_weighted_scalars_f32, ctx, count, &params);
    }

    pub fn launchSiluMultiplyF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        up: buffer_mod.DeviceBuffer,
        count: usize,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        try checkBytes(gate, count);
        try checkBytes(up, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var gate_ptr = gate.ptr;
        var up_ptr = up.ptr;
        var count_u32 = try toU32(count);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&up_ptr),
            @ptrCast(&count_u32),
        };
        try launch1d(self.silu_multiply_f32, ctx, count, &params);
    }

    pub fn launchActivationMultiplyF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        up: buffer_mod.DeviceBuffer,
        count: usize,
        activation: u32,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        try checkBytes(gate, count);
        try checkBytes(up, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var gate_ptr = gate.ptr;
        var up_ptr = up.ptr;
        var count_u32 = try toU32(count);
        var activation_u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&up_ptr),
            @ptrCast(&count_u32),
            @ptrCast(&activation_u32),
        };
        try launch1d(self.activation_multiply_f32, ctx, count, &params);
    }

    pub fn launchActivationMultiplySliceLastDimF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        source: buffer_mod.DeviceBuffer,
        rows: usize,
        source_cols: usize,
        start: usize,
        out_cols: usize,
        activation: u32,
    ) driver_mod.Error!void {
        const function = self.activation_multiply_slice_last_dim_f32 orelse return error.CudaKernelUnavailable;
        if (start > source_cols or out_cols > source_cols - start) return error.InvalidCudaState;
        const count = try checkedTensorElements(rows, out_cols);
        try checkBytes(dst, count);
        try checkBytes(gate, count);
        try checkBytes(source, try checkedTensorElements(rows, source_cols));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var gate_ptr = gate.ptr;
        var source_ptr = source.ptr;
        var rows_u32 = try toU32(rows);
        var source_cols_u32 = try toU32(source_cols);
        var start_u32 = try toU32(start);
        var out_cols_u32 = try toU32(out_cols);
        var activation_u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&source_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&source_cols_u32),
            @ptrCast(&start_u32),
            @ptrCast(&out_cols_u32),
            @ptrCast(&activation_u32),
        };
        try launch1d(function, ctx, count, &params);
    }

    pub fn launchArgmaxLastRowF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        rows: usize,
        dim: usize,
    ) driver_mod.Error!void {
        if (rows == 0 or dim == 0) return error.InvalidCudaState;
        try checkRawBytes(dst, @sizeOf(u32));
        try checkBytes(input, try checkedTensorElements(rows, dim));

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
        };
        try launchBlocks(self.argmax_last_row_f32, ctx, 1, f32_tiled_threads, &params);
    }

    pub fn launchArgmaxRowsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        row_start: usize,
        row_count: usize,
        dim: usize,
    ) driver_mod.Error!bool {
        const function = self.argmax_rows_f32 orelse return false;
        if (row_count == 0 or dim == 0) return error.InvalidCudaState;
        const row_end = std.math.add(usize, row_start, row_count) catch return error.InvalidCudaState;
        try checkRawBytes(dst, try checkedTensorElements(row_count, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(row_end, dim));

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var row_start_u32 = try toU32(row_start);
        var row_count_u32 = try toU32(row_count);
        var dim_u32 = try toU32(dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&row_start_u32),
            @ptrCast(&row_count_u32),
            @ptrCast(&dim_u32),
        };
        try launchBlocks(function, ctx, row_count, f32_tiled_threads, &params);
        return true;
    }

    pub fn launchArgmaxRowsSuppressF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        row_start: usize,
        row_count: usize,
        dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!bool {
        const function = self.argmax_rows_suppress_f32 orelse return false;
        if (row_count == 0 or dim == 0 or suppress_count == 0) return error.InvalidCudaState;
        const row_end = std.math.add(usize, row_start, row_count) catch return error.InvalidCudaState;
        try checkRawBytes(dst, try checkedTensorElements(row_count, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(row_end, dim));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var row_start_u32 = try toU32(row_start);
        var row_count_u32 = try toU32(row_count);
        var dim_u32 = try toU32(dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&row_start_u32),
            @ptrCast(&row_count_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(function, ctx, row_count, f32_tiled_threads, &params);
        return true;
    }

    pub fn launchArgmaxLastRowSuppressF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const function = self.argmax_last_row_suppress_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or dim == 0 or suppress_count == 0) return error.InvalidCudaState;
        try checkRawBytes(dst, @sizeOf(u32));
        try checkBytes(input, try checkedTensorElements(rows, dim));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(function, ctx, 1, f32_tiled_threads, &params);
    }

    pub fn launchLinearQ8_0ArgmaxTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_indices: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const stage1 = self.linear_q8_0_argmax_stage1_tile4 orelse return error.CudaKernelUnavailable;
        const reduce = self.argmax_reduce_pairs_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or in_dim == 0 or out_dim == 0 or in_dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_0_values_per_block;
        const col_tiles = (out_dim + q8_0_col_tile - 1) / q8_0_col_tile;
        try checkRawBytes(dst, @sizeOf(u32));
        try checkBytes(partial_values, col_tiles);
        try checkRawBytes(partial_indices, try checkedTensorElements(col_tiles, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q8_0_block_bytes));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var partial_values_ptr = partial_values.ptr;
        var partial_indices_ptr = partial_indices.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(stage1, ctx, col_tiles, q8_0_tiled_threads, &stage1_params);

        var dst_ptr = dst.ptr;
        var col_tiles_u32 = try toU32(col_tiles);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&col_tiles_u32),
        };
        try launchBlocks(reduce, ctx, 1, f32_tiled_threads, &reduce_params);
    }

    pub fn launchLinearQ4KArgmaxTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_indices: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const stage1 = self.linear_q4_k_argmax_stage1_tile4 orelse return error.CudaKernelUnavailable;
        const reduce = self.argmax_reduce_pairs_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or in_dim == 0 or out_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const col_tiles = (out_dim + q4_k_col_tile - 1) / q4_k_col_tile;
        try checkRawBytes(dst, @sizeOf(u32));
        try checkBytes(partial_values, col_tiles);
        try checkRawBytes(partial_indices, try checkedTensorElements(col_tiles, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var partial_values_ptr = partial_values.ptr;
        var partial_indices_ptr = partial_indices.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(stage1, ctx, col_tiles, q4_k_tiled_threads, &stage1_params);

        var dst_ptr = dst.ptr;
        var col_tiles_u32 = try toU32(col_tiles);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&col_tiles_u32),
        };
        try launchBlocks(reduce, ctx, 1, f32_tiled_threads, &reduce_params);
    }

    pub fn launchLinearQ8_0ArgmaxRowsTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_indices: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const stage1 = self.linear_q8_0_argmax_rows_stage1_tile4 orelse return error.CudaKernelUnavailable;
        const reduce = self.argmax_reduce_rows_pairs_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or in_dim == 0 or out_dim == 0 or in_dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_0_values_per_block;
        const col_tiles = (out_dim + q8_0_col_tile - 1) / q8_0_col_tile;
        const partial_count = try checkedTensorElements(rows, col_tiles);
        try checkRawBytes(dst, try checkedTensorElements(rows, @sizeOf(u32)));
        try checkBytes(partial_values, partial_count);
        try checkRawBytes(partial_indices, try checkedTensorElements(partial_count, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q8_0_block_bytes));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var partial_values_ptr = partial_values.ptr;
        var partial_indices_ptr = partial_indices.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(stage1, ctx, partial_count, q8_0_tiled_threads, &stage1_params);

        var dst_ptr = dst.ptr;
        var col_tiles_u32 = try toU32(col_tiles);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&col_tiles_u32),
        };
        try launchBlocks(reduce, ctx, rows, f32_tiled_threads, &reduce_params);
    }

    pub fn launchLinearQ4_0ArgmaxRowsTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_indices: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const stage1 = self.linear_q4_0_argmax_rows_stage1_tile4 orelse return error.CudaKernelUnavailable;
        const reduce = self.argmax_reduce_rows_pairs_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or in_dim == 0 or out_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const col_tiles = (out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const partial_count = try checkedTensorElements(rows, col_tiles);
        try checkRawBytes(dst, try checkedTensorElements(rows, @sizeOf(u32)));
        try checkBytes(partial_values, partial_count);
        try checkRawBytes(partial_indices, try checkedTensorElements(partial_count, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var partial_values_ptr = partial_values.ptr;
        var partial_indices_ptr = partial_indices.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(stage1, ctx, partial_count, q8_0_tiled_threads, &stage1_params);

        var dst_ptr = dst.ptr;
        var col_tiles_u32 = try toU32(col_tiles);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&col_tiles_u32),
        };
        try launchBlocks(reduce, ctx, rows, f32_tiled_threads, &reduce_params);
    }

    pub fn launchLinearQ4_0ArgmaxRowsTile16F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_indices: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const stage1 = self.linear_q4_0_argmax_rows_stage1_tile16 orelse return error.CudaKernelUnavailable;
        const reduce = self.argmax_reduce_rows_pairs_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or in_dim == 0 or out_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const col_tiles = (out_dim + q4_0_col_tile16 - 1) / q4_0_col_tile16;
        const partial_count = try checkedTensorElements(rows, col_tiles);
        try checkRawBytes(dst, try checkedTensorElements(rows, @sizeOf(u32)));
        try checkBytes(partial_values, partial_count);
        try checkRawBytes(partial_indices, try checkedTensorElements(partial_count, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var partial_values_ptr = partial_values.ptr;
        var partial_indices_ptr = partial_indices.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(stage1, ctx, partial_count, q8_0_tiled_threads, &stage1_params);

        var dst_ptr = dst.ptr;
        var col_tiles_u32 = try toU32(col_tiles);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&col_tiles_u32),
        };
        try launchBlocks(reduce, ctx, rows, f32_tiled_threads, &reduce_params);
    }

    pub fn launchLinearQ4KArgmaxRowsTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_indices: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const stage1 = self.linear_q4_k_argmax_rows_stage1_tile4 orelse return error.CudaKernelUnavailable;
        const reduce = self.argmax_reduce_rows_pairs_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or in_dim == 0 or out_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const col_tiles = (out_dim + q4_k_col_tile - 1) / q4_k_col_tile;
        const partial_count = try checkedTensorElements(rows, col_tiles);
        try checkRawBytes(dst, try checkedTensorElements(rows, @sizeOf(u32)));
        try checkBytes(partial_values, partial_count);
        try checkRawBytes(partial_indices, try checkedTensorElements(partial_count, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var partial_values_ptr = partial_values.ptr;
        var partial_indices_ptr = partial_indices.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(stage1, ctx, partial_count, q4_k_tiled_threads, &stage1_params);

        var dst_ptr = dst.ptr;
        var col_tiles_u32 = try toU32(col_tiles);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&col_tiles_u32),
        };
        try launchBlocks(reduce, ctx, rows, f32_tiled_threads, &reduce_params);
    }

    pub fn launchLinearQ6KArgmaxRowsTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_indices: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const stage1 = self.linear_q6_k_argmax_rows_stage1_tile4 orelse return error.CudaKernelUnavailable;
        const reduce = self.argmax_reduce_rows_pairs_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or in_dim == 0 or out_dim == 0 or in_dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q6_k_values_per_block;
        const col_tiles = (out_dim + q4_k_col_tile - 1) / q4_k_col_tile;
        const partial_count = try checkedTensorElements(rows, col_tiles);
        try checkRawBytes(dst, try checkedTensorElements(rows, @sizeOf(u32)));
        try checkBytes(partial_values, partial_count);
        try checkRawBytes(partial_indices, try checkedTensorElements(partial_count, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q6_k_block_bytes));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var partial_values_ptr = partial_values.ptr;
        var partial_indices_ptr = partial_indices.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(stage1, ctx, partial_count, q4_k_tiled_threads, &stage1_params);

        var dst_ptr = dst.ptr;
        var col_tiles_u32 = try toU32(col_tiles);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&col_tiles_u32),
        };
        try launchBlocks(reduce, ctx, rows, f32_tiled_threads, &reduce_params);
    }

    pub fn launchLinearQ6KArgmaxRowsTile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_indices: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const stage1 = self.linear_q6_k_argmax_rows_stage1_tile8 orelse return error.CudaKernelUnavailable;
        const reduce = self.argmax_reduce_rows_pairs_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or in_dim == 0 or out_dim == 0 or in_dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q6_k_values_per_block;
        const col_tiles = (out_dim + q6_k_col_tile8 - 1) / q6_k_col_tile8;
        const partial_count = try checkedTensorElements(rows, col_tiles);
        try checkRawBytes(dst, try checkedTensorElements(rows, @sizeOf(u32)));
        try checkBytes(partial_values, partial_count);
        try checkRawBytes(partial_indices, try checkedTensorElements(partial_count, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q6_k_block_bytes));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var partial_values_ptr = partial_values.ptr;
        var partial_indices_ptr = partial_indices.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(stage1, ctx, partial_count, q4_k_tiled_threads, &stage1_params);

        var dst_ptr = dst.ptr;
        var col_tiles_u32 = try toU32(col_tiles);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&col_tiles_u32),
        };
        try launchBlocks(reduce, ctx, rows, f32_tiled_threads, &reduce_params);
    }

    pub fn launchLinearQ6KArgmaxRowsTile16F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_indices: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const stage1 = self.linear_q6_k_argmax_rows_stage1_tile16 orelse return error.CudaKernelUnavailable;
        const reduce = self.argmax_reduce_rows_pairs_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or in_dim == 0 or out_dim == 0 or in_dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q6_k_values_per_block;
        const col_tiles = (out_dim + q6_k_col_tile16 - 1) / q6_k_col_tile16;
        const partial_count = try checkedTensorElements(rows, col_tiles);
        try checkRawBytes(dst, try checkedTensorElements(rows, @sizeOf(u32)));
        try checkBytes(partial_values, partial_count);
        try checkRawBytes(partial_indices, try checkedTensorElements(partial_count, @sizeOf(u32)));
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q6_k_block_bytes));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var partial_values_ptr = partial_values.ptr;
        var partial_indices_ptr = partial_indices.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        try launchBlocks(stage1, ctx, partial_count, q4_k_tiled_threads, &stage1_params);

        var dst_ptr = dst.ptr;
        var col_tiles_u32 = try toU32(col_tiles);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&col_tiles_u32),
        };
        try launchBlocks(reduce, ctx, rows, f32_tiled_threads, &reduce_params);
    }

    pub fn launchLinearQ6KQ8_1F32Tile8E4B(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q6_k_q8_1_f32_tile8_e4b orelse return error.CudaKernelUnavailable;
        // The kernel body hardcodes 10 Q6_K blocks per column (in_dim 2560).
        if (in_dim != 2560 or out_dim == 0 or out_dim % 8 != 0) return error.CudaKernelUnavailable;
        try checkBytes(dst, out_dim);
        try checkRawBytes(input_q8_1, (in_dim / 32) * 36);
        try checkRawBytes(weight_raw, try checkedTensorElements(out_dim, (in_dim / 256) * 210));
        var dst_ptr = dst.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_ptr = weight_raw.ptr;
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, out_dim / 8, 160, &params);
    }

    pub fn launchLinearQ6KQ8_1ArgmaxRowsTile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_indices: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        suppress_token_ids: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        suppress_count: usize,
    ) driver_mod.Error!void {
        const use_e4b_stage1 =
            rows == 1 and in_dim == 2560 and out_dim == 262144 and suppress_count == 0 and
            self.linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b != null;
        const stage1 = if (use_e4b_stage1)
            self.linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b orelse return error.CudaKernelUnavailable
        else
            self.linear_q6_k_q8_1_argmax_rows_stage1_tile8 orelse return error.CudaKernelUnavailable;
        const reduce = self.argmax_reduce_rows_pairs_f32_w16 orelse (self.argmax_reduce_rows_pairs_f32 orelse return error.CudaKernelUnavailable);
        if (rows == 0 or in_dim == 0 or out_dim == 0 or in_dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q6_k_values_per_block;
        const q8_row_blocks = in_dim / q8_1_values_per_block;
        const col_tiles = (out_dim + q6_k_col_tile8 - 1) / q6_k_col_tile8;
        const partial_count = try checkedTensorElements(rows, col_tiles);
        try checkRawBytes(dst, try checkedTensorElements(rows, @sizeOf(u32)));
        try checkBytes(partial_values, partial_count);
        try checkRawBytes(partial_indices, try checkedTensorElements(partial_count, @sizeOf(u32)));
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, q8_row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q6_k_block_bytes));
        try checkRawBytes(suppress_token_ids, try checkedTensorElements(suppress_count, @sizeOf(i32)));

        var partial_values_ptr = partial_values.ptr;
        var partial_indices_ptr = partial_indices.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_ptr = weight_raw.ptr;
        var suppress_ptr = suppress_token_ids.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var suppress_count_u32 = try toU32(suppress_count);
        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&suppress_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&suppress_count_u32),
        };
        const stage1_threads: usize = if (use_e4b_stage1) 160 else q6KQ8_1Tile8Stage1Threads(row_blocks);
        try launchBlocks(stage1, ctx, partial_count, stage1_threads, &stage1_params);

        var dst_ptr = dst.ptr;
        var col_tiles_u32 = try toU32(col_tiles);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_indices_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&col_tiles_u32),
        };
        const reduce_threads: usize = if (self.argmax_reduce_rows_pairs_f32_w16 != null) 512 else f32_tiled_threads;
        try launchBlocks(reduce, ctx, rows, reduce_threads, &reduce_params);
    }

    pub fn launchGemma4MtpPreprojectF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        target_embedding: buffer_mod.DeviceBuffer,
        activation: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        backbone_hidden: usize,
        draft_hidden: usize,
        concat_order: u32,
        weight_dtype: u32,
    ) driver_mod.Error!bool {
        if (self.gemma4_mtp_preproject_f32 == null) return false;
        if (backbone_hidden == 0 or draft_hidden == 0) return error.InvalidCudaState;
        try checkBytes(dst, draft_hidden);
        try checkBytes(target_embedding, backbone_hidden);
        try checkBytes(activation, backbone_hidden);
        const weight_elems = try checkedTensorElements(draft_hidden, try checkedTensorElements(backbone_hidden, 2));
        try checkTypedTailWeightBytes(weight, weight_elems, weight_dtype);

        var dst_ptr = dst.ptr;
        var target_embedding_ptr = target_embedding.ptr;
        var activation_ptr = activation.ptr;
        var weight_ptr = weight.ptr;
        var backbone_hidden_u32 = try toU32(backbone_hidden);
        var draft_hidden_u32 = try toU32(draft_hidden);
        var concat_order_u32 = concat_order;
        var weight_dtype_u32 = weight_dtype;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&target_embedding_ptr),
            @ptrCast(&activation_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&backbone_hidden_u32),
            @ptrCast(&draft_hidden_u32),
            @ptrCast(&concat_order_u32),
            @ptrCast(&weight_dtype_u32),
        };
        try launchBlocks(self.gemma4_mtp_preproject_f32, ctx, draft_hidden, f32_tiled_threads, &params);
        return true;
    }

    pub fn launchGemma4MtpMaskedSelectF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        assistant_hidden: buffer_mod.DeviceBuffer,
        logits: buffer_mod.DeviceBuffer,
        centroid_weight: buffer_mod.DeviceBuffer,
        token_ordering: buffer_mod.DeviceBuffer,
        hidden_size: usize,
        vocab_size: usize,
        num_centroids: usize,
        top_k: usize,
        use_inverse_ordering: bool,
        centroid_weight_dtype: u32,
    ) driver_mod.Error!bool {
        if (self.gemma4_mtp_masked_select_f32 == null) return false;
        if (hidden_size == 0 or vocab_size == 0 or num_centroids == 0 or top_k == 0) return error.InvalidCudaState;
        if (num_centroids > 4096) return error.InvalidCudaState;
        try checkRawBytes(dst, @sizeOf(u32));
        try checkBytes(assistant_hidden, hidden_size);
        try checkBytes(logits, vocab_size);
        try checkBytes(token_ordering, vocab_size);
        const centroid_weight_elems = try checkedTensorElements(num_centroids, hidden_size);
        try checkTypedTailWeightBytes(centroid_weight, centroid_weight_elems, centroid_weight_dtype);

        var dst_ptr = dst.ptr;
        var assistant_hidden_ptr = assistant_hidden.ptr;
        var logits_ptr = logits.ptr;
        var centroid_weight_ptr = centroid_weight.ptr;
        var token_ordering_ptr = token_ordering.ptr;
        var hidden_size_u32 = try toU32(hidden_size);
        var vocab_size_u32 = try toU32(vocab_size);
        var num_centroids_u32 = try toU32(num_centroids);
        var top_k_u32 = try toU32(top_k);
        var inverse_u32: u32 = if (use_inverse_ordering) 1 else 0;
        var centroid_weight_dtype_u32 = centroid_weight_dtype;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&assistant_hidden_ptr),
            @ptrCast(&logits_ptr),
            @ptrCast(&centroid_weight_ptr),
            @ptrCast(&token_ordering_ptr),
            @ptrCast(&hidden_size_u32),
            @ptrCast(&vocab_size_u32),
            @ptrCast(&num_centroids_u32),
            @ptrCast(&top_k_u32),
            @ptrCast(&inverse_u32),
            @ptrCast(&centroid_weight_dtype_u32),
        };
        try launchBlocks(self.gemma4_mtp_masked_select_f32, ctx, 1, f32_tiled_threads, &params);
        return true;
    }

    pub fn launchGemma4MtpMaskedSelectHiddenF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        assistant_hidden: buffer_mod.DeviceBuffer,
        lm_head_weight: buffer_mod.DeviceBuffer,
        centroid_weight: buffer_mod.DeviceBuffer,
        token_ordering: buffer_mod.DeviceBuffer,
        hidden_size: usize,
        vocab_size: usize,
        num_centroids: usize,
        top_k: usize,
        use_inverse_ordering: bool,
        lm_head_dtype: u32,
        centroid_weight_dtype: u32,
    ) driver_mod.Error!bool {
        if (self.gemma4_mtp_masked_select_hidden_f32 == null) return false;
        if (hidden_size == 0 or vocab_size == 0 or num_centroids == 0 or top_k == 0) return error.InvalidCudaState;
        if (num_centroids > 4096) return error.InvalidCudaState;
        try checkRawBytes(dst, @sizeOf(u32));
        try checkBytes(assistant_hidden, hidden_size);
        try checkBytes(token_ordering, vocab_size);
        try checkTypedTailWeightBytes(lm_head_weight, try checkedTensorElements(vocab_size, hidden_size), lm_head_dtype);
        try checkTypedTailWeightBytes(centroid_weight, try checkedTensorElements(num_centroids, hidden_size), centroid_weight_dtype);

        var dst_ptr = dst.ptr;
        var assistant_hidden_ptr = assistant_hidden.ptr;
        var lm_head_weight_ptr = lm_head_weight.ptr;
        var centroid_weight_ptr = centroid_weight.ptr;
        var token_ordering_ptr = token_ordering.ptr;
        var hidden_size_u32 = try toU32(hidden_size);
        var vocab_size_u32 = try toU32(vocab_size);
        var num_centroids_u32 = try toU32(num_centroids);
        var top_k_u32 = try toU32(top_k);
        var inverse_u32: u32 = if (use_inverse_ordering) 1 else 0;
        var lm_head_dtype_u32 = lm_head_dtype;
        var centroid_weight_dtype_u32 = centroid_weight_dtype;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&assistant_hidden_ptr),
            @ptrCast(&lm_head_weight_ptr),
            @ptrCast(&centroid_weight_ptr),
            @ptrCast(&token_ordering_ptr),
            @ptrCast(&hidden_size_u32),
            @ptrCast(&vocab_size_u32),
            @ptrCast(&num_centroids_u32),
            @ptrCast(&top_k_u32),
            @ptrCast(&inverse_u32),
            @ptrCast(&lm_head_dtype_u32),
            @ptrCast(&centroid_weight_dtype_u32),
        };
        try launchBlocks(self.gemma4_mtp_masked_select_hidden_f32, ctx, 1, f32_tiled_threads, &params);
        return true;
    }

    pub fn launchGemma4MtpMaskedSelectHiddenMultiF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        centroid_scores: buffer_mod.DeviceBuffer,
        top_centroids: buffer_mod.DeviceBuffer,
        partial_values: buffer_mod.DeviceBuffer,
        partial_tokens: buffer_mod.DeviceBuffer,
        assistant_hidden: buffer_mod.DeviceBuffer,
        lm_head_weight: buffer_mod.DeviceBuffer,
        centroid_weight: buffer_mod.DeviceBuffer,
        token_ordering: buffer_mod.DeviceBuffer,
        hidden_size: usize,
        vocab_size: usize,
        num_centroids: usize,
        top_k: usize,
        lm_head_dtype: u32,
        centroid_weight_dtype: u32,
    ) driver_mod.Error!bool {
        if (self.gemma4_mtp_centroid_scores_hidden_f32 == null or
            self.gemma4_mtp_centroid_topk_u32 == null or
            self.gemma4_mtp_restricted_lm_head_scores_f32 == null or
            self.gemma4_mtp_reduce_token_scores_f32 == null)
        {
            return false;
        }
        if (hidden_size == 0 or vocab_size == 0 or num_centroids == 0 or top_k == 0) return error.InvalidCudaState;
        if (num_centroids > 4096) return error.InvalidCudaState;
        if (top_k > 128) return error.InvalidCudaState;
        if (vocab_size % num_centroids != 0) return error.InvalidCudaState;
        const cluster_size = vocab_size / num_centroids;
        const candidate_count = try checkedTensorElements(top_k, cluster_size);
        if (candidate_count == 0) return error.InvalidCudaState;
        try checkRawBytes(dst, @sizeOf(u32));
        try checkBytes(centroid_scores, num_centroids);
        try checkRawBytes(top_centroids, try checkedTensorElements(top_k, @sizeOf(u32)));
        try checkBytes(partial_values, candidate_count);
        try checkRawBytes(partial_tokens, try checkedTensorElements(candidate_count, @sizeOf(u32)));
        try checkBytes(assistant_hidden, hidden_size);
        try checkBytes(token_ordering, vocab_size);
        try checkTypedTailWeightBytes(lm_head_weight, try checkedTensorElements(vocab_size, hidden_size), lm_head_dtype);
        try checkTypedTailWeightBytes(centroid_weight, try checkedTensorElements(num_centroids, hidden_size), centroid_weight_dtype);

        var centroid_scores_ptr = centroid_scores.ptr;
        var assistant_hidden_ptr = assistant_hidden.ptr;
        var centroid_weight_ptr = centroid_weight.ptr;
        var hidden_size_u32 = try toU32(hidden_size);
        var num_centroids_u32 = try toU32(num_centroids);
        var centroid_weight_dtype_u32 = centroid_weight_dtype;
        var centroid_params = [_]?*anyopaque{
            @ptrCast(&centroid_scores_ptr),
            @ptrCast(&assistant_hidden_ptr),
            @ptrCast(&centroid_weight_ptr),
            @ptrCast(&hidden_size_u32),
            @ptrCast(&num_centroids_u32),
            @ptrCast(&centroid_weight_dtype_u32),
        };
        try launchBlocks(self.gemma4_mtp_centroid_scores_hidden_f32, ctx, num_centroids, f32_tiled_threads, &centroid_params);

        var top_centroids_ptr = top_centroids.ptr;
        var top_k_u32 = try toU32(top_k);
        var topk_params = [_]?*anyopaque{
            @ptrCast(&top_centroids_ptr),
            @ptrCast(&centroid_scores_ptr),
            @ptrCast(&num_centroids_u32),
            @ptrCast(&top_k_u32),
        };
        try launchBlocks(self.gemma4_mtp_centroid_topk_u32, ctx, 1, f32_tiled_threads, &topk_params);

        var partial_values_ptr = partial_values.ptr;
        var partial_tokens_ptr = partial_tokens.ptr;
        var lm_head_weight_ptr = lm_head_weight.ptr;
        var token_ordering_ptr = token_ordering.ptr;
        var vocab_size_u32 = try toU32(vocab_size);
        var lm_head_dtype_u32 = lm_head_dtype;
        var score_params = [_]?*anyopaque{
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_tokens_ptr),
            @ptrCast(&assistant_hidden_ptr),
            @ptrCast(&lm_head_weight_ptr),
            @ptrCast(&token_ordering_ptr),
            @ptrCast(&top_centroids_ptr),
            @ptrCast(&hidden_size_u32),
            @ptrCast(&vocab_size_u32),
            @ptrCast(&num_centroids_u32),
            @ptrCast(&top_k_u32),
            @ptrCast(&lm_head_dtype_u32),
        };
        try launchBlocks(self.gemma4_mtp_restricted_lm_head_scores_f32, ctx, candidate_count, f32_tiled_threads, &score_params);

        var dst_ptr = dst.ptr;
        var candidate_count_u32 = try toU32(candidate_count);
        var reduce_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_values_ptr),
            @ptrCast(&partial_tokens_ptr),
            @ptrCast(&candidate_count_u32),
        };
        try launchBlocks(self.gemma4_mtp_reduce_token_scores_f32, ctx, 1, f32_tiled_threads, &reduce_params);
        return true;
    }

    pub fn launchGemma4MtpMaskedArgmaxF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        logits: buffer_mod.DeviceBuffer,
        centroid_logits: buffer_mod.DeviceBuffer,
        token_ordering: buffer_mod.DeviceBuffer,
        vocab_size: usize,
        num_centroids: usize,
        top_k: usize,
        use_inverse_ordering: bool,
    ) driver_mod.Error!bool {
        if (self.gemma4_mtp_masked_argmax_f32 == null) return false;
        if (vocab_size == 0 or num_centroids == 0 or top_k == 0) return error.InvalidCudaState;
        try checkRawBytes(dst, @sizeOf(u32));
        try checkBytes(logits, vocab_size);
        try checkBytes(centroid_logits, num_centroids);
        try checkBytes(token_ordering, vocab_size);

        var dst_ptr = dst.ptr;
        var logits_ptr = logits.ptr;
        var centroid_logits_ptr = centroid_logits.ptr;
        var token_ordering_ptr = token_ordering.ptr;
        var vocab_size_u32 = try toU32(vocab_size);
        var num_centroids_u32 = try toU32(num_centroids);
        var top_k_u32 = try toU32(top_k);
        var inverse_u32: u32 = if (use_inverse_ordering) 1 else 0;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&logits_ptr),
            @ptrCast(&centroid_logits_ptr),
            @ptrCast(&token_ordering_ptr),
            @ptrCast(&vocab_size_u32),
            @ptrCast(&num_centroids_u32),
            @ptrCast(&top_k_u32),
            @ptrCast(&inverse_u32),
        };
        try launchBlocks(self.gemma4_mtp_masked_argmax_f32, ctx, 1, f32_tiled_threads, &params);
        return true;
    }

    pub fn launchGemma4MtpVerifyCommitU32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        result: buffer_mod.DeviceBuffer,
        target_choices: buffer_mod.DeviceBuffer,
        draft_tokens: buffer_mod.DeviceBuffer,
        eos_token_ids: buffer_mod.DeviceBuffer,
        draft_count: usize,
        eos_count: usize,
        accept_bonus: bool,
    ) driver_mod.Error!bool {
        if (self.gemma4_mtp_verify_commit_u32 == null) return false;
        if (draft_count == 0) return error.InvalidCudaState;
        try checkRawBytes(result, 12 * @sizeOf(u32));
        try checkRawBytes(target_choices, try checkedTensorElements(draft_count + 1, @sizeOf(u32)));
        try checkRawBytes(draft_tokens, try checkedTensorElements(draft_count, @sizeOf(i64)));
        try checkRawBytes(eos_token_ids, try checkedTensorElements(eos_count, @sizeOf(i32)));

        var result_ptr = result.ptr;
        var target_choices_ptr = target_choices.ptr;
        var draft_tokens_ptr = draft_tokens.ptr;
        var eos_token_ids_ptr = eos_token_ids.ptr;
        var draft_count_u32 = try toU32(draft_count);
        var eos_count_u32 = try toU32(eos_count);
        var accept_bonus_u32: u32 = if (accept_bonus) 1 else 0;
        var params = [_]?*anyopaque{
            @ptrCast(&result_ptr),
            @ptrCast(&target_choices_ptr),
            @ptrCast(&draft_tokens_ptr),
            @ptrCast(&eos_token_ids_ptr),
            @ptrCast(&draft_count_u32),
            @ptrCast(&eos_count_u32),
            @ptrCast(&accept_bonus_u32),
        };
        try launchBlocks(self.gemma4_mtp_verify_commit_u32, ctx, 1, 1, &params);
        return true;
    }

    pub fn launchLinearBiasF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(weight, try checkedTensorElements(out_dim, in_dim));
        try checkBytes(bias, out_dim);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var bias_ptr = bias.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch1d(self.linear_bias_f32, ctx, out_count, &params);
    }

    pub fn launchAddBiasRowsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(bias, out_dim);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var bias_ptr = bias.ptr;
        var rows_u32 = try toU32(rows);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch1d(self.add_bias_rows_f32, ctx, out_count, &params);
    }

    pub fn launchLinearBiasTile4Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearBiasF32Tile4Rows2Common(ctx, self.linear_bias_f32_tile4_r2, dst, input, weight, bias, rows, in_dim, out_dim);
    }

    pub fn launchLinearBiasReluTile4Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearBiasF32Tile4Rows2Common(ctx, self.linear_bias_relu_f32_tile4_r2, dst, input, weight, bias, rows, in_dim, out_dim);
    }

    pub fn launchLinearBiasGeluTile4Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearBiasF32Tile4Rows2Common(ctx, self.linear_bias_gelu_f32_tile4_r2, dst, input, weight, bias, rows, in_dim, out_dim);
    }

    pub fn launchLinearBiasAddTile4Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(weight, try checkedTensorElements(out_dim, in_dim));
        try checkBytes(bias, out_dim);
        try checkBytes(residual, out_count);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var bias_ptr = bias.ptr;
        var residual_ptr = residual.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(self.linear_bias_add_f32_tile4_r2, ctx, (out_dim + f32_col_tile - 1) / f32_col_tile, (rows + f32_row_tile - 1) / f32_row_tile, f32_tiled_threads, &params);
    }

    pub fn launchLinearPairBiasTile4Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        bias_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        bias_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(weight_a, try checkedTensorElements(out_dim, in_dim));
        try checkBytes(weight_b, try checkedTensorElements(out_dim, in_dim));
        try checkBytes(bias_a, out_dim);
        try checkBytes(bias_b, out_dim);
        if (out_count == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var bias_a_ptr = bias_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var bias_b_ptr = bias_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&bias_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&bias_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(self.linear_pair_bias_f32_tile4_r2, ctx, (out_dim + f32_col_tile - 1) / f32_col_tile, (rows + f32_row_tile - 1) / f32_row_tile, f32_tiled_threads, &params);
    }

    pub fn launchLinearTripleBiasTile4Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        dst_c: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        bias_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        bias_b: buffer_mod.DeviceBuffer,
        weight_c: buffer_mod.DeviceBuffer,
        bias_c: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(dst_c, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(weight_a, try checkedTensorElements(out_dim, in_dim));
        try checkBytes(weight_b, try checkedTensorElements(out_dim, in_dim));
        try checkBytes(weight_c, try checkedTensorElements(out_dim, in_dim));
        try checkBytes(bias_a, out_dim);
        try checkBytes(bias_b, out_dim);
        try checkBytes(bias_c, out_dim);
        if (out_count == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var dst_c_ptr = dst_c.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var bias_a_ptr = bias_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var bias_b_ptr = bias_b.ptr;
        var weight_c_ptr = weight_c.ptr;
        var bias_c_ptr = bias_c.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&dst_c_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&bias_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&bias_b_ptr),
            @ptrCast(&weight_c_ptr),
            @ptrCast(&bias_c_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(self.linear_triple_bias_f32_tile4_r2, ctx, (out_dim + f32_col_tile - 1) / f32_col_tile, (rows + f32_row_tile - 1) / f32_row_tile, f32_tiled_threads, &params);
    }

    fn launchLinearBiasF32Tile4Rows2Common(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        function: driver_mod.CUfunction,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        _ = self;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(weight, try checkedTensorElements(out_dim, in_dim));
        try checkBytes(bias, out_dim);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var bias_ptr = bias.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + f32_col_tile - 1) / f32_col_tile, (rows + f32_row_tile - 1) / f32_row_tile, f32_tiled_threads, &params);
    }

    pub fn launchRmsNormF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        total_rows: usize,
        dim: usize,
        eps: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(total_rows, dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkBytes(weight, dim);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var rows_u32 = try toU32(total_rows);
        var dim_u32 = try toU32(dim);
        var eps_value = eps;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&eps_value),
        };
        try launchRows(self.rms_norm_f32, ctx, total_rows, &params);
    }

    pub fn launchRmsNormF32Bf16(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        dst_bf16: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        total_rows: usize,
        dim: usize,
        eps: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(total_rows, dim);
        try checkBytes(dst, count);
        try checkRawBytes(dst_bf16, try checkedTensorElements(count, @sizeOf(u16)));
        try checkBytes(input, count);
        try checkBytes(weight, dim);
        if (count == 0) return;

        const function = self.rms_norm_f32_bf16 orelse return error.CudaKernelUnavailable;
        var dst_ptr = dst.ptr;
        var dst_bf16_ptr = dst_bf16.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var rows_u32 = try toU32(total_rows);
        var dim_u32 = try toU32(dim);
        var eps_value = eps;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&dst_bf16_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&eps_value),
        };
        try launchRows(function, ctx, total_rows, &params);
    }

    pub fn launchRmsNormAddF32Bf16(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        dst_bf16: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        total_rows: usize,
        dim: usize,
        eps: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(total_rows, dim);
        try checkBytes(dst, count);
        try checkRawBytes(dst_bf16, try checkedTensorElements(count, @sizeOf(u16)));
        try checkBytes(input, count);
        try checkBytes(weight, dim);
        try checkBytes(residual, count);
        if (count == 0) return;

        const function = self.rms_norm_add_f32_bf16 orelse return error.CudaKernelUnavailable;
        var dst_ptr = dst.ptr;
        var dst_bf16_ptr = dst_bf16.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var residual_ptr = residual.ptr;
        var rows_u32 = try toU32(total_rows);
        var dim_u32 = try toU32(dim);
        var eps_value = eps;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&dst_bf16_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&eps_value),
        };
        try launchRows(function, ctx, total_rows, &params);
    }

    pub fn launchRmsNormAddF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        total_rows: usize,
        dim: usize,
        eps: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(total_rows, dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkBytes(weight, dim);
        try checkBytes(residual, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var residual_ptr = residual.ptr;
        var rows_u32 = try toU32(total_rows);
        var dim_u32 = try toU32(dim);
        var eps_value = eps;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&eps_value),
        };
        try launchRows(self.rms_norm_add_f32, ctx, total_rows, &params);
    }

    pub fn launchRmsNormAddMulScalarF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        scalar: buffer_mod.DeviceBuffer,
        total_rows: usize,
        dim: usize,
        eps: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(total_rows, dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkBytes(weight, dim);
        try checkBytes(residual, count);
        try checkBytes(scalar, 1);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var residual_ptr = residual.ptr;
        var scalar_ptr = scalar.ptr;
        var rows_u32 = try toU32(total_rows);
        var dim_u32 = try toU32(dim);
        var eps_value = eps;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&scalar_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&eps_value),
        };
        try launchRows(self.rms_norm_add_mul_scalar_f32, ctx, total_rows, &params);
    }

    pub fn launchRmsNormAddOutputScaleF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        scalar: buffer_mod.DeviceBuffer,
        total_rows: usize,
        dim: usize,
        eps: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(total_rows, dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkBytes(weight, dim);
        try checkBytes(residual, count);
        try checkBytes(scalar, 1);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var residual_ptr = residual.ptr;
        var scalar_ptr = scalar.ptr;
        const function = self.rms_norm_add_output_scale_f32 orelse return error.CudaKernelUnavailable;
        var rows_u32 = try toU32(total_rows);
        var dim_u32 = try toU32(dim);
        var eps_value = eps;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&scalar_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&eps_value),
        };
        try launchRows(function, ctx, total_rows, &params);
    }

    pub fn launchRmsNormBareF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        total_rows: usize,
        dim: usize,
        eps: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(total_rows, dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var rows_u32 = try toU32(total_rows);
        var dim_u32 = try toU32(dim);
        var eps_value = eps;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&eps_value),
        };
        try launchRows(self.rms_norm_bare_f32, ctx, total_rows, &params);
    }

    pub fn launchElementwiseF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        a: buffer_mod.DeviceBuffer,
        b: buffer_mod.DeviceBuffer,
        count: usize,
        op: ElementwiseOp,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        try checkBytes(a, count);
        if (!op.isUnary()) try checkBytes(b, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var a_ptr = a.ptr;
        var b_ptr = b.ptr;
        var count_u32 = try toU32(count);
        var op_u32: u32 = @intFromEnum(op);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&a_ptr),
            @ptrCast(&b_ptr),
            @ptrCast(&count_u32),
            @ptrCast(&op_u32),
        };
        try launch1d(self.elementwise_f32, ctx, count, &params);
    }

    pub fn launchLayerNormF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        gamma: buffer_mod.DeviceBuffer,
        beta: buffer_mod.DeviceBuffer,
        rows: usize,
        dim: usize,
        eps: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(rows, dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkBytes(gamma, dim);
        try checkBytes(beta, dim);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var gamma_ptr = gamma.ptr;
        var beta_ptr = beta.ptr;
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var eps_value = eps;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&gamma_ptr),
            @ptrCast(&beta_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&eps_value),
        };
        try launchBlocks(self.layer_norm_f32, ctx, rows, f32_tiled_threads, &params);
    }

    pub fn launchAddLayerNormF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        a: buffer_mod.DeviceBuffer,
        b: buffer_mod.DeviceBuffer,
        gamma: buffer_mod.DeviceBuffer,
        beta: buffer_mod.DeviceBuffer,
        rows: usize,
        dim: usize,
        eps: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(rows, dim);
        try checkBytes(dst, count);
        try checkBytes(a, count);
        try checkBytes(b, count);
        try checkBytes(gamma, dim);
        try checkBytes(beta, dim);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var a_ptr = a.ptr;
        var b_ptr = b.ptr;
        var gamma_ptr = gamma.ptr;
        var beta_ptr = beta.ptr;
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var eps_value = eps;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&a_ptr),
            @ptrCast(&b_ptr),
            @ptrCast(&gamma_ptr),
            @ptrCast(&beta_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&eps_value),
        };
        try launchBlocks(self.add_layer_norm_f32, ctx, rows, f32_tiled_threads, &params);
    }

    pub fn launchEmbeddingLookupF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(weight, dim * @sizeOf(f32));
        try checkRawBytes(ids, total * @sizeOf(i64));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.embedding_lookup_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupBf16WeightF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        const function = self.embedding_lookup_bf16_weight_f32 orelse return error.CudaKernelUnavailable;
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(weight, dim * @sizeOf(u16));
        try checkRawBytes(ids, total * @sizeOf(i64));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(function, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupI32F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(weight, dim * @sizeOf(f32));
        try checkRawBytes(ids, total * @sizeOf(i32));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.embedding_lookup_i32_f32, ctx, count, &params);
    }

    pub fn launchTakeRowsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        row_ids: buffer_mod.DeviceBuffer,
        source_rows: usize,
        rows: usize,
        dim: usize,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(rows, dim);
        try checkBytes(dst, count);
        try checkBytes(input, try checkedTensorElements(source_rows, dim));
        try checkRawBytes(row_ids, rows * @sizeOf(u32));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var row_ids_ptr = row_ids.ptr;
        var source_rows_u32 = try toU32(source_rows);
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&row_ids_ptr),
            @ptrCast(&source_rows_u32),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
        };
        try launch1d(self.take_rows_f32, ctx, count, &params);
    }

    pub fn launchGlinerGatherConcatReluF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        start: buffer_mod.DeviceBuffer,
        end: buffer_mod.DeviceBuffer,
        start_rows: buffer_mod.DeviceBuffer,
        end_rows: buffer_mod.DeviceBuffer,
        source_rows: usize,
        rows: usize,
        dim: usize,
    ) driver_mod.Error!void {
        const function = self.gliner_gather_concat_relu_f32 orelse return error.CudaSymbolMissing;
        const out_dim = try checkedTensorElements(dim, 2);
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(start, try checkedTensorElements(source_rows, dim));
        try checkBytes(end, try checkedTensorElements(source_rows, dim));
        try checkRawBytes(start_rows, rows * @sizeOf(u32));
        try checkRawBytes(end_rows, rows * @sizeOf(u32));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var start_ptr = start.ptr;
        var end_ptr = end.ptr;
        var start_rows_ptr = start_rows.ptr;
        var end_rows_ptr = end_rows.ptr;
        var source_rows_u32 = try toU32(source_rows);
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&start_ptr),
            @ptrCast(&end_ptr),
            @ptrCast(&start_rows_ptr),
            @ptrCast(&end_rows_ptr),
            @ptrCast(&source_rows_u32),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
        };
        try launch1d(function, ctx, out_count, &params);
    }

    pub fn launchSliceLastDimF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        rows: usize,
        cols: usize,
        start: usize,
        out_cols: usize,
    ) driver_mod.Error!void {
        if (start > cols or out_cols > cols - start) return error.InvalidCudaState;
        const count = try checkedTensorElements(rows, out_cols);
        try checkBytes(dst, count);
        try checkBytes(input, try checkedTensorElements(rows, cols));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var rows_u32 = try toU32(rows);
        var cols_u32 = try toU32(cols);
        var start_u32 = try toU32(start);
        var out_cols_u32 = try toU32(out_cols);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&cols_u32),
            @ptrCast(&start_u32),
            @ptrCast(&out_cols_u32),
        };
        try launch1d(self.slice_last_dim_f32, ctx, count, &params);
    }

    pub fn launchGlinerWordEmbeddingsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        hidden: buffer_mod.DeviceBuffer,
        words_mask: buffer_mod.DeviceBuffer,
        batch: usize,
        seq_len: usize,
        hidden_size: usize,
        num_words: usize,
    ) driver_mod.Error!void {
        const out_rows = try checkedTensorElements(batch, num_words);
        const out_count = try checkedTensorElements(out_rows, hidden_size);
        try checkBytes(dst, out_count);
        try checkBytes(hidden, try checkedTensorElements(try checkedTensorElements(batch, seq_len), hidden_size));
        try checkRawBytes(words_mask, try checkedTensorElements(batch, seq_len) * @sizeOf(i64));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var hidden_ptr = hidden.ptr;
        var words_mask_ptr = words_mask.ptr;
        var batch_u32 = try toU32(batch);
        var seq_len_u32 = try toU32(seq_len);
        var hidden_size_u32 = try toU32(hidden_size);
        var num_words_u32 = try toU32(num_words);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&hidden_ptr),
            @ptrCast(&words_mask_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&hidden_size_u32),
            @ptrCast(&num_words_u32),
        };
        try launch1d(self.gliner_word_embeddings_f32, ctx, out_count, &params);
    }

    pub fn launchRepeatFirstRowF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        src: buffer_mod.DeviceBuffer,
        rows: usize,
        dim: usize,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(rows, dim);
        try checkBytes(dst, count);
        try checkBytes(src, dim);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var src_ptr = src.ptr;
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&src_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
        };
        try launch1d(self.repeat_first_row_f32, ctx, count, &params);
    }

    pub fn launchGlinerGruCombineF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        label_embeddings: buffer_mod.DeviceBuffer,
        gi: buffer_mod.DeviceBuffer,
        gh: buffer_mod.DeviceBuffer,
        rows: usize,
        dim: usize,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(rows, dim);
        const gate_dim = try checkedTensorElements(dim, 3);
        try checkBytes(dst, count);
        try checkBytes(label_embeddings, count);
        try checkBytes(gi, try checkedTensorElements(rows, gate_dim));
        try checkBytes(gh, try checkedTensorElements(rows, gate_dim));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var label_ptr = label_embeddings.ptr;
        var gi_ptr = gi.ptr;
        var gh_ptr = gh.ptr;
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&label_ptr),
            @ptrCast(&gi_ptr),
            @ptrCast(&gh_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
        };
        try launch1d(self.gliner_gru_combine_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupQ4KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(ids, try checkedTensorElements(total, @sizeOf(i64)));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight_raw.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.embedding_lookup_q4_k_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupQ6KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(ids, try checkedTensorElements(total, @sizeOf(i64)));
        const row_blocks = dim / q6_k_values_per_block;
        try checkRawBytes(weight_raw, try checkedTensorElements(row_blocks, q6_k_block_bytes));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight_raw.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.embedding_lookup_q6_k_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupQ4_0F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(ids, try checkedTensorElements(total, @sizeOf(i64)));
        const row_blocks = dim / q4_0_values_per_block;
        try checkRawBytes(weight_raw, try checkedTensorElements(row_blocks, q4_0_block_bytes));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight_raw.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.embedding_lookup_q4_0_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupQ8_0F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(ids, try checkedTensorElements(total, @sizeOf(i64)));
        const row_blocks = dim / q8_0_values_per_block;
        try checkRawBytes(weight_raw, try checkedTensorElements(row_blocks, q8_0_block_bytes));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight_raw.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.embedding_lookup_q8_0_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupI32Q4KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(ids, try checkedTensorElements(total, @sizeOf(i32)));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight_raw.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.embedding_lookup_i32_q4_k_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupI32Q6KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(ids, try checkedTensorElements(total, @sizeOf(i32)));
        const row_blocks = dim / q6_k_values_per_block;
        try checkRawBytes(weight_raw, try checkedTensorElements(row_blocks, q6_k_block_bytes));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight_raw.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.embedding_lookup_i32_q6_k_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingAddWeightedI32Q6KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        rhs: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        lhs_scale: f32,
        rhs_scale: f32,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkBytes(rhs, count);
        try checkRawBytes(ids, try checkedTensorElements(total, @sizeOf(i32)));
        const row_blocks = dim / q6_k_values_per_block;
        try checkRawBytes(weight_raw, try checkedTensorElements(row_blocks, q6_k_block_bytes));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight_raw.ptr;
        var ids_ptr = ids.ptr;
        var rhs_ptr = rhs.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var lhs_scale_value = lhs_scale;
        var rhs_scale_value = rhs_scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&rhs_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&lhs_scale_value),
            @ptrCast(&rhs_scale_value),
        };
        try launch1d(self.embedding_add_weighted_i32_q6_k_f32, ctx, count, &params);
    }

    pub fn launchRmsNormAddWeightedEmbeddingI32Q6KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        norm_weight: buffer_mod.DeviceBuffer,
        embedding_weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        num_groups: usize,
        group_dim: usize,
        eps: f32,
        lhs_scale: f32,
        rhs_scale: f32,
    ) driver_mod.Error!void {
        const function = self.rms_norm_add_weighted_embedding_i32_q6_k_f32 orelse return error.CudaKernelUnavailable;
        if (group_dim == 0 or num_groups == 0) return error.InvalidCudaState;
        const embedding_dim = try checkedTensorElements(num_groups, group_dim);
        if (embedding_dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const count = try checkedTensorElements(total, embedding_dim);
        const rows = try checkedTensorElements(total, num_groups);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkBytes(norm_weight, group_dim);
        try checkRawBytes(ids, try checkedTensorElements(total, @sizeOf(i32)));
        const row_blocks = embedding_dim / q6_k_values_per_block;
        try checkRawBytes(embedding_weight_raw, try checkedTensorElements(row_blocks, q6_k_block_bytes));
        if (count == 0 or rows == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var norm_weight_ptr = norm_weight.ptr;
        var embedding_weight_ptr = embedding_weight_raw.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var num_groups_u32 = try toU32(num_groups);
        var group_dim_u32 = try toU32(group_dim);
        var embedding_dim_u32 = try toU32(embedding_dim);
        var eps_value = eps;
        var lhs_scale_value = lhs_scale;
        var rhs_scale_value = rhs_scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&norm_weight_ptr),
            @ptrCast(&embedding_weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&num_groups_u32),
            @ptrCast(&group_dim_u32),
            @ptrCast(&embedding_dim_u32),
            @ptrCast(&eps_value),
            @ptrCast(&lhs_scale_value),
            @ptrCast(&rhs_scale_value),
        };
        try launchBlocks(function, ctx, rows, 256, &params);
    }

    pub fn launchEmbeddingLookupI32Q4_0F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(ids, try checkedTensorElements(total, @sizeOf(i32)));
        const row_blocks = dim / q4_0_values_per_block;
        try checkRawBytes(weight_raw, try checkedTensorElements(row_blocks, q4_0_block_bytes));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight_raw.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.embedding_lookup_i32_q4_0_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupI32Q8_0F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
        scale: f32,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
        const count = try checkedTensorElements(total, dim);
        try checkBytes(dst, count);
        try checkRawBytes(ids, try checkedTensorElements(total, @sizeOf(i32)));
        const row_blocks = dim / q8_0_values_per_block;
        try checkRawBytes(weight_raw, try checkedTensorElements(row_blocks, q8_0_block_bytes));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var weight_ptr = weight_raw.ptr;
        var ids_ptr = ids.ptr;
        var total_u32 = try toU32(total);
        var dim_u32 = try toU32(dim);
        var scale_value = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&scale_value),
        };
        try launch1d(self.embedding_lookup_i32_q8_0_f32, ctx, count, &params);
    }

    pub fn launchConcatLastDimF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        a: buffer_mod.DeviceBuffer,
        b: buffer_mod.DeviceBuffer,
        total: usize,
        dim_a: usize,
        dim_b: usize,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(total, dim_a + dim_b);
        try checkBytes(dst, count);
        try checkBytes(a, try checkedTensorElements(total, dim_a));
        try checkBytes(b, try checkedTensorElements(total, dim_b));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var a_ptr = a.ptr;
        var b_ptr = b.ptr;
        var total_u32 = try toU32(total);
        var dim_a_u32 = try toU32(dim_a);
        var dim_b_u32 = try toU32(dim_b);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&a_ptr),
            @ptrCast(&b_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_a_u32),
            @ptrCast(&dim_b_u32),
        };
        try launch1d(self.concat_lastdim_f32, ctx, count, &params);
    }

    pub fn launchConv2dF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        batch: usize,
        in_channels: usize,
        out_channels: usize,
        height: usize,
        width: usize,
        kernel_h: usize,
        kernel_w: usize,
        stride_h: usize,
        stride_w: usize,
        padding_h: usize,
        padding_w: usize,
        groups: usize,
        out_h: usize,
        out_w: usize,
    ) driver_mod.Error!void {
        const out_count = try checkedTensorElements(try checkedTensorElements(batch, out_channels), try checkedTensorElements(out_h, out_w));
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(try checkedTensorElements(batch, in_channels), try checkedTensorElements(height, width)));
        try checkBytes(weight, try checkedTensorElements(try checkedTensorElements(out_channels, in_channels / groups), try checkedTensorElements(kernel_h, kernel_w)));
        try checkBytes(bias, out_channels);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var bias_ptr = bias.ptr;
        var batch_u32 = try toU32(batch);
        var in_channels_u32 = try toU32(in_channels);
        var out_channels_u32 = try toU32(out_channels);
        var height_u32 = try toU32(height);
        var width_u32 = try toU32(width);
        var kernel_h_u32 = try toU32(kernel_h);
        var kernel_w_u32 = try toU32(kernel_w);
        var stride_h_u32 = try toU32(stride_h);
        var stride_w_u32 = try toU32(stride_w);
        var padding_h_u32 = try toU32(padding_h);
        var padding_w_u32 = try toU32(padding_w);
        var groups_u32 = try toU32(groups);
        var out_h_u32 = try toU32(out_h);
        var out_w_u32 = try toU32(out_w);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&in_channels_u32),
            @ptrCast(&out_channels_u32),
            @ptrCast(&height_u32),
            @ptrCast(&width_u32),
            @ptrCast(&kernel_h_u32),
            @ptrCast(&kernel_w_u32),
            @ptrCast(&stride_h_u32),
            @ptrCast(&stride_w_u32),
            @ptrCast(&padding_h_u32),
            @ptrCast(&padding_w_u32),
            @ptrCast(&groups_u32),
            @ptrCast(&out_h_u32),
            @ptrCast(&out_w_u32),
        };
        try launch1d(self.conv2d_f32, ctx, out_count, &params);
    }

    pub fn launchAttentionF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        q: buffer_mod.DeviceBuffer,
        k: buffer_mod.DeviceBuffer,
        v: buffer_mod.DeviceBuffer,
        mask: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        head_dim: usize,
        causal: bool,
        has_mask: bool,
        bias_mode: u32,
        head_major: bool,
    ) driver_mod.Error!void {
        const hidden = try checkedTensorElements(num_heads, head_dim);
        const count = try checkedTensorElements(try checkedTensorElements(batch, seq_len), hidden);
        try checkBytes(dst, count);
        try checkBytes(q, count);
        try checkBytes(k, count);
        try checkBytes(v, count);
        if (has_mask) try checkRawBytes(mask, try checkedTensorElements(batch, seq_len) * @sizeOf(i64));
        if (bias_mode != 0) try checkBytes(bias, try checkedTensorElements(if (bias_mode == 2) batch * num_heads else num_heads, try checkedTensorElements(seq_len, seq_len)));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var q_ptr = q.ptr;
        var k_ptr = k.ptr;
        var v_ptr = v.ptr;
        var mask_ptr = mask.ptr;
        var bias_ptr = bias.ptr;
        var batch_u32 = try toU32(batch);
        var seq_len_u32 = try toU32(seq_len);
        var num_heads_u32 = try toU32(num_heads);
        var head_dim_u32 = try toU32(head_dim);
        var causal_u32: u32 = if (causal) 1 else 0;
        var has_mask_u32: u32 = if (has_mask) 1 else 0;
        var bias_mode_u32 = bias_mode;
        var head_major_u32: u32 = if (head_major) 1 else 0;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&q_ptr),
            @ptrCast(&k_ptr),
            @ptrCast(&v_ptr),
            @ptrCast(&mask_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&num_heads_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&causal_u32),
            @ptrCast(&has_mask_u32),
            @ptrCast(&bias_mode_u32),
            @ptrCast(&head_major_u32),
        };
        if (seq_len <= 512 and head_dim <= 128) {
            try launchBlocks(self.attention_f32_block, ctx, try checkedTensorElements(try checkedTensorElements(batch, seq_len), num_heads), 128, &params);
        } else {
            try launch1d(self.attention_f32, ctx, count, &params);
        }
    }

    pub fn launchCrossAttentionF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        q: buffer_mod.DeviceBuffer,
        k: buffer_mod.DeviceBuffer,
        v: buffer_mod.DeviceBuffer,
        mask: buffer_mod.DeviceBuffer,
        batch: usize,
        dec_seq: usize,
        enc_seq: usize,
        num_heads: usize,
        head_dim: usize,
    ) driver_mod.Error!void {
        if (self.cross_attention_f32 == null) return error.CudaKernelUnavailable;
        const hidden = try checkedTensorElements(num_heads, head_dim);
        const q_count = try checkedTensorElements(try checkedTensorElements(batch, dec_seq), hidden);
        const kv_count = try checkedTensorElements(try checkedTensorElements(batch, enc_seq), hidden);
        try checkBytes(dst, q_count);
        try checkBytes(q, q_count);
        try checkBytes(k, kv_count);
        try checkBytes(v, kv_count);
        if (mask.ptr != 0) try checkRawBytes(mask, try checkedTensorElements(batch, enc_seq) * @sizeOf(i64));
        if (q_count == 0) return;

        if (dec_seq == 1 and enc_seq != 0 and self.cross_attention_q1_f32 != null) {
            const threads: usize = 256;
            const shared_bytes = try checkedTensorElements(enc_seq + threads, @sizeOf(f32));
            if (shared_bytes <= 48 * 1024) {
                var dst_ptr = dst.ptr;
                var q_ptr = q.ptr;
                var k_ptr = k.ptr;
                var v_ptr = v.ptr;
                var mask_ptr = mask.ptr;
                var batch_u32 = try toU32(batch);
                var enc_seq_u32 = try toU32(enc_seq);
                var num_heads_u32 = try toU32(num_heads);
                var head_dim_u32 = try toU32(head_dim);
                var params = [_]?*anyopaque{
                    @ptrCast(&dst_ptr),
                    @ptrCast(&q_ptr),
                    @ptrCast(&k_ptr),
                    @ptrCast(&v_ptr),
                    @ptrCast(&mask_ptr),
                    @ptrCast(&batch_u32),
                    @ptrCast(&enc_seq_u32),
                    @ptrCast(&num_heads_u32),
                    @ptrCast(&head_dim_u32),
                };
                try launchBlocksShared(self.cross_attention_q1_f32, ctx, try checkedTensorElements(batch, num_heads), threads, shared_bytes, &params);
                return;
            }
        }

        var dst_ptr = dst.ptr;
        var q_ptr = q.ptr;
        var k_ptr = k.ptr;
        var v_ptr = v.ptr;
        var mask_ptr = mask.ptr;
        var batch_u32 = try toU32(batch);
        var dec_seq_u32 = try toU32(dec_seq);
        var enc_seq_u32 = try toU32(enc_seq);
        var num_heads_u32 = try toU32(num_heads);
        var head_dim_u32 = try toU32(head_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&q_ptr),
            @ptrCast(&k_ptr),
            @ptrCast(&v_ptr),
            @ptrCast(&mask_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&dec_seq_u32),
            @ptrCast(&enc_seq_u32),
            @ptrCast(&num_heads_u32),
            @ptrCast(&head_dim_u32),
        };
        try launch1d(self.cross_attention_f32, ctx, q_count, &params);
    }

    pub fn launchTokenToNchwF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        src: buffer_mod.DeviceBuffer,
        batch: usize,
        channels: usize,
        height: usize,
        width: usize,
    ) driver_mod.Error!void {
        if (self.token_to_nchw_f32 == null) return error.CudaKernelUnavailable;
        const count = try checkedTensorElements(try checkedTensorElements(batch, channels), try checkedTensorElements(height, width));
        try checkBytes(dst, count);
        try checkBytes(src, count);
        if (count == 0) return;
        var dst_ptr = dst.ptr;
        var src_ptr = src.ptr;
        var batch_u32 = try toU32(batch);
        var channels_u32 = try toU32(channels);
        var height_u32 = try toU32(height);
        var width_u32 = try toU32(width);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&src_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&channels_u32),
            @ptrCast(&height_u32),
            @ptrCast(&width_u32),
        };
        try launch1d(self.token_to_nchw_f32, ctx, count, &params);
    }

    pub fn launchNchwToTokenF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        src: buffer_mod.DeviceBuffer,
        batch: usize,
        channels: usize,
        height: usize,
        width: usize,
    ) driver_mod.Error!void {
        if (self.nchw_to_token_f32 == null) return error.CudaKernelUnavailable;
        const count = try checkedTensorElements(try checkedTensorElements(batch, channels), try checkedTensorElements(height, width));
        try checkBytes(dst, count);
        try checkBytes(src, count);
        if (count == 0) return;
        var dst_ptr = dst.ptr;
        var src_ptr = src.ptr;
        var batch_u32 = try toU32(batch);
        var channels_u32 = try toU32(channels);
        var height_u32 = try toU32(height);
        var width_u32 = try toU32(width);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&src_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&channels_u32),
            @ptrCast(&height_u32),
            @ptrCast(&width_u32),
        };
        try launch1d(self.nchw_to_token_f32, ctx, count, &params);
    }

    pub fn launchPackWindowsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        src: buffer_mod.DeviceBuffer,
        batch: usize,
        height: usize,
        width: usize,
        dim: usize,
        window_size: usize,
        padded_h: usize,
        padded_w: usize,
    ) driver_mod.Error!void {
        if (self.pack_windows_f32 == null) return error.CudaKernelUnavailable;
        const window_count = try checkedTensorElements(try checkedTensorElements(batch, padded_h / window_size), padded_w / window_size);
        const count = try checkedTensorElements(try checkedTensorElements(window_count, window_size * window_size), dim);
        try checkBytes(dst, count);
        try checkBytes(src, try checkedTensorElements(try checkedTensorElements(batch, height * width), dim));
        if (count == 0) return;
        var dst_ptr = dst.ptr;
        var src_ptr = src.ptr;
        var batch_u32 = try toU32(batch);
        var height_u32 = try toU32(height);
        var width_u32 = try toU32(width);
        var dim_u32 = try toU32(dim);
        var window_size_u32 = try toU32(window_size);
        var padded_h_u32 = try toU32(padded_h);
        var padded_w_u32 = try toU32(padded_w);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&src_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&height_u32),
            @ptrCast(&width_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&window_size_u32),
            @ptrCast(&padded_h_u32),
            @ptrCast(&padded_w_u32),
        };
        try launch1d(self.pack_windows_f32, ctx, count, &params);
    }

    pub fn launchUnpadWindowsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        src: buffer_mod.DeviceBuffer,
        batch: usize,
        height: usize,
        width: usize,
        dim: usize,
        window_size: usize,
        padded_h: usize,
        padded_w: usize,
    ) driver_mod.Error!void {
        if (self.unpad_windows_f32 == null) return error.CudaKernelUnavailable;
        const count = try checkedTensorElements(try checkedTensorElements(batch, height * width), dim);
        const window_count = try checkedTensorElements(try checkedTensorElements(batch, padded_h / window_size), padded_w / window_size);
        const src_count = try checkedTensorElements(try checkedTensorElements(window_count, window_size * window_size), dim);
        try checkBytes(dst, count);
        try checkBytes(src, src_count);
        if (count == 0) return;
        var dst_ptr = dst.ptr;
        var src_ptr = src.ptr;
        var batch_u32 = try toU32(batch);
        var height_u32 = try toU32(height);
        var width_u32 = try toU32(width);
        var dim_u32 = try toU32(dim);
        var window_size_u32 = try toU32(window_size);
        var padded_h_u32 = try toU32(padded_h);
        var padded_w_u32 = try toU32(padded_w);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&src_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&height_u32),
            @ptrCast(&width_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&window_size_u32),
            @ptrCast(&padded_h_u32),
            @ptrCast(&padded_w_u32),
        };
        try launch1d(self.unpad_windows_f32, ctx, count, &params);
    }

    pub fn launchChannelScoresSoftmaxF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        scores: buffer_mod.DeviceBuffer,
        qkv: buffer_mod.DeviceBuffer,
        batch: usize,
        seq_len: usize,
        dim: usize,
        groups: usize,
    ) driver_mod.Error!void {
        if (self.channel_scores_softmax_f32 == null) return error.CudaKernelUnavailable;
        if (groups == 0 or dim % groups != 0) return error.InvalidCudaState;
        const channels_per_group = dim / groups;
        if (channels_per_group > 256) return error.InvalidCudaState;
        const qkv_count = try checkedTensorElements(try checkedTensorElements(batch, seq_len), dim * 3);
        const score_count = try checkedTensorElements(try checkedTensorElements(batch, groups), channels_per_group * channels_per_group);
        try checkBytes(qkv, qkv_count);
        try checkBytes(scores, score_count);
        const rows = try checkedTensorElements(try checkedTensorElements(batch, groups), channels_per_group);
        if (rows == 0) return;
        var scores_ptr = scores.ptr;
        var qkv_ptr = qkv.ptr;
        var batch_u32 = try toU32(batch);
        var seq_len_u32 = try toU32(seq_len);
        var dim_u32 = try toU32(dim);
        var groups_u32 = try toU32(groups);
        var params = [_]?*anyopaque{
            @ptrCast(&scores_ptr),
            @ptrCast(&qkv_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&groups_u32),
        };
        try launchBlocks(self.channel_scores_softmax_f32, ctx, rows, 256, &params);
    }

    pub fn launchChannelApplyF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        qkv: buffer_mod.DeviceBuffer,
        scores: buffer_mod.DeviceBuffer,
        batch: usize,
        seq_len: usize,
        dim: usize,
        groups: usize,
    ) driver_mod.Error!void {
        if (self.channel_apply_f32 == null) return error.CudaKernelUnavailable;
        if (groups == 0 or dim % groups != 0) return error.InvalidCudaState;
        const channels_per_group = dim / groups;
        const count = try checkedTensorElements(try checkedTensorElements(batch, seq_len), dim);
        const qkv_count = try checkedTensorElements(count, 3);
        const score_count = try checkedTensorElements(try checkedTensorElements(batch, groups), channels_per_group * channels_per_group);
        try checkBytes(dst, count);
        try checkBytes(qkv, qkv_count);
        try checkBytes(scores, score_count);
        if (count == 0) return;
        var dst_ptr = dst.ptr;
        var qkv_ptr = qkv.ptr;
        var scores_ptr = scores.ptr;
        var batch_u32 = try toU32(batch);
        var seq_len_u32 = try toU32(seq_len);
        var dim_u32 = try toU32(dim);
        var groups_u32 = try toU32(groups);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&qkv_ptr),
            @ptrCast(&scores_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&groups_u32),
        };
        try launch1d(self.channel_apply_f32, ctx, count, &params);
    }

    pub fn launchFlorenceVisionTailSourcesF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        tokens: buffer_mod.DeviceBuffer,
        row_embed: buffer_mod.DeviceBuffer,
        col_embed: buffer_mod.DeviceBuffer,
        temporal_embed: buffer_mod.DeviceBuffer,
        batch: usize,
        height: usize,
        width: usize,
        dim: usize,
        has_temporal: bool,
        row_dtype: u32,
        col_dtype: u32,
        temporal_dtype: u32,
    ) driver_mod.Error!void {
        if (self.florence_vision_tail_sources_f32 == null) return error.CudaKernelUnavailable;
        if (height == 0 or width == 0 or dim == 0) return error.InvalidCudaState;
        const token_count = try checkedTensorElements(height, width);
        const out_seq = std.math.add(usize, token_count, 1) catch return error.InvalidCudaState;
        const src_count = try checkedTensorElements(try checkedTensorElements(batch, token_count), dim);
        const dst_count = try checkedTensorElements(try checkedTensorElements(batch, out_seq), dim);
        try checkBytes(tokens, src_count);
        try checkBytes(dst, dst_count);
        try checkTypedTailWeightBytes(row_embed, try checkedTensorElements(height, dim / 2), row_dtype);
        try checkTypedTailWeightBytes(col_embed, try checkedTensorElements(width, dim - dim / 2), col_dtype);
        if (has_temporal) try checkTypedTailWeightBytes(temporal_embed, dim, temporal_dtype);
        if (dst_count == 0) return;
        var dst_ptr = dst.ptr;
        var tokens_ptr = tokens.ptr;
        var row_ptr = row_embed.ptr;
        var col_ptr = col_embed.ptr;
        var temporal_ptr = temporal_embed.ptr;
        var batch_u32 = try toU32(batch);
        var height_u32 = try toU32(height);
        var width_u32 = try toU32(width);
        var dim_u32 = try toU32(dim);
        var has_temporal_u32: u32 = if (has_temporal) 1 else 0;
        var row_dtype_u32 = row_dtype;
        var col_dtype_u32 = col_dtype;
        var temporal_dtype_u32 = temporal_dtype;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&tokens_ptr),
            @ptrCast(&row_ptr),
            @ptrCast(&col_ptr),
            @ptrCast(&temporal_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&height_u32),
            @ptrCast(&width_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&has_temporal_u32),
            @ptrCast(&row_dtype_u32),
            @ptrCast(&col_dtype_u32),
            @ptrCast(&temporal_dtype_u32),
        };
        try launch1d(self.florence_vision_tail_sources_f32, ctx, dst_count, &params);
    }

    pub fn launchRopeF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        total_chunks: usize,
        head_dim: usize,
        rope_dim: usize,
        theta: f32,
        freq_scale: f32,
        position_offset: usize,
        seq_len: usize,
        chunks_per_position: usize,
        consecutive_pairs: bool,
    ) driver_mod.Error!void {
        const function = self.rope_f32 orelse return error.CudaKernelUnavailable;
        const count = try checkedTensorElements(total_chunks, head_dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var total_chunks_u32 = try toU32(total_chunks);
        var head_dim_u32 = try toU32(head_dim);
        var rope_dim_u32 = try toU32(rope_dim);
        var theta_f32 = theta;
        var freq_scale_f32 = freq_scale;
        var position_offset_u32 = try toU32(position_offset);
        var seq_len_u32 = try toU32(seq_len);
        var chunks_per_position_u32 = try toU32(chunks_per_position);
        var consecutive_pairs_u32: u32 = if (consecutive_pairs) 1 else 0;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&total_chunks_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&rope_dim_u32),
            @ptrCast(&theta_f32),
            @ptrCast(&freq_scale_f32),
            @ptrCast(&position_offset_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&chunks_per_position_u32),
            @ptrCast(&consecutive_pairs_u32),
        };
        if (captureParamTraceIndex(ctx)) |trace_index| {
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel=rope dst=0x{x} input=0x{x} total_chunks={d} head_dim={d} rope_dim={d} theta={d:.6} freq_scale={d:.6} position_offset={d} seq_len={d} chunks_per_position={d} consecutive_pairs={d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                dst_ptr,
                input_ptr,
                total_chunks_u32,
                head_dim_u32,
                rope_dim_u32,
                theta_f32,
                freq_scale_f32,
                position_offset_u32,
                seq_len_u32,
                chunks_per_position_u32,
                consecutive_pairs_u32,
            });
        }
        try launch1d(function, ctx, count, &params);
    }

    pub fn launchRopeDecodeScalarsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        decode_scalars: buffer_mod.DeviceBuffer,
        total_chunks: usize,
        head_dim: usize,
        rope_dim: usize,
        theta: f32,
        freq_scale: f32,
        position_offset: usize,
        seq_len: usize,
        chunks_per_position: usize,
        consecutive_pairs: bool,
    ) driver_mod.Error!void {
        const function = self.rope_decode_scalars_f32 orelse return error.CudaKernelUnavailable;
        const count = try checkedTensorElements(total_chunks, head_dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkRawBytes(decode_scalars, 5 * @sizeOf(u32));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var total_chunks_u32 = try toU32(total_chunks);
        var head_dim_u32 = try toU32(head_dim);
        var rope_dim_u32 = try toU32(rope_dim);
        var theta_f32 = theta;
        var freq_scale_f32 = freq_scale;
        var position_offset_u32 = try toU32(position_offset);
        var seq_len_u32 = try toU32(seq_len);
        var chunks_per_position_u32 = try toU32(chunks_per_position);
        var consecutive_pairs_u32: u32 = if (consecutive_pairs) 1 else 0;
        var decode_scalars_ptr = decode_scalars.ptr;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&total_chunks_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&rope_dim_u32),
            @ptrCast(&theta_f32),
            @ptrCast(&freq_scale_f32),
            @ptrCast(&position_offset_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&chunks_per_position_u32),
            @ptrCast(&consecutive_pairs_u32),
            @ptrCast(&decode_scalars_ptr),
        };
        if (captureParamTraceIndex(ctx)) |trace_index| {
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel=rope_decode_scalars dst=0x{x} input=0x{x} scalars=0x{x} fallback_position_offset={d} seq_len={d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                dst_ptr,
                input_ptr,
                decode_scalars_ptr,
                position_offset_u32,
                seq_len_u32,
            });
        }
        try launch1d(function, ctx, count, &params);
    }

    pub fn supportsDecodeScalarsAdvance(self: *const KernelModule) bool {
        return self.decode_scalars_advance != null;
    }

    pub fn launchDecodeScalarsAdvance(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        decode_scalars: buffer_mod.DeviceBuffer,
        position_delta: u32,
        query_position_delta: u32,
        kv_seq_delta: u32,
        total_sequence_delta: u32,
        kv_position_delta: u32,
    ) driver_mod.Error!void {
        const function = self.decode_scalars_advance orelse return error.CudaKernelUnavailable;
        try checkRawBytes(decode_scalars, 5 * @sizeOf(u32));
        var decode_scalars_ptr = decode_scalars.ptr;
        var position_delta_arg = position_delta;
        var query_position_delta_arg = query_position_delta;
        var kv_seq_delta_arg = kv_seq_delta;
        var total_sequence_delta_arg = total_sequence_delta;
        var kv_position_delta_arg = kv_position_delta;
        var params = [_]?*anyopaque{
            @ptrCast(&decode_scalars_ptr),
            @ptrCast(&position_delta_arg),
            @ptrCast(&query_position_delta_arg),
            @ptrCast(&kv_seq_delta_arg),
            @ptrCast(&total_sequence_delta_arg),
            @ptrCast(&kv_position_delta_arg),
        };
        if (captureParamTraceIndex(ctx)) |trace_index| {
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel=decode_scalars_advance scalars=0x{x} deltas={d},{d},{d},{d},{d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                decode_scalars_ptr,
                position_delta_arg,
                query_position_delta_arg,
                kv_seq_delta_arg,
                total_sequence_delta_arg,
                kv_position_delta_arg,
            });
        }
        try launch1d(function, ctx, 1, &params);
    }

    pub fn launchRopeScaledF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        total_chunks: usize,
        head_dim: usize,
        rope_dim: usize,
        theta: f32,
        freq_scale: f32,
        position_offset: usize,
        seq_len: usize,
        chunks_per_position: usize,
        consecutive_pairs: bool,
        scale: f32,
    ) driver_mod.Error!void {
        const function = self.rope_scaled_f32 orelse return error.CudaKernelUnavailable;
        const count = try checkedTensorElements(total_chunks, head_dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var total_chunks_u32 = try toU32(total_chunks);
        var head_dim_u32 = try toU32(head_dim);
        var rope_dim_u32 = try toU32(rope_dim);
        var theta_f32 = theta;
        var freq_scale_f32 = freq_scale;
        var position_offset_u32 = try toU32(position_offset);
        var seq_len_u32 = try toU32(seq_len);
        var chunks_per_position_u32 = try toU32(chunks_per_position);
        var consecutive_pairs_u32: u32 = if (consecutive_pairs) 1 else 0;
        var scale_f32 = scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&total_chunks_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&rope_dim_u32),
            @ptrCast(&theta_f32),
            @ptrCast(&freq_scale_f32),
            @ptrCast(&position_offset_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&chunks_per_position_u32),
            @ptrCast(&consecutive_pairs_u32),
            @ptrCast(&scale_f32),
        };
        try launch1d(function, ctx, count, &params);
    }

    pub fn launchRopeScaledDecodeScalarsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        decode_scalars: buffer_mod.DeviceBuffer,
        total_chunks: usize,
        head_dim: usize,
        rope_dim: usize,
        theta: f32,
        freq_scale: f32,
        position_offset: usize,
        seq_len: usize,
        chunks_per_position: usize,
        consecutive_pairs: bool,
        scale: f32,
    ) driver_mod.Error!void {
        const function = self.rope_scaled_decode_scalars_f32 orelse return error.CudaKernelUnavailable;
        const count = try checkedTensorElements(total_chunks, head_dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkRawBytes(decode_scalars, 5 * @sizeOf(u32));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var total_chunks_u32 = try toU32(total_chunks);
        var head_dim_u32 = try toU32(head_dim);
        var rope_dim_u32 = try toU32(rope_dim);
        var theta_f32 = theta;
        var freq_scale_f32 = freq_scale;
        var position_offset_u32 = try toU32(position_offset);
        var seq_len_u32 = try toU32(seq_len);
        var chunks_per_position_u32 = try toU32(chunks_per_position);
        var consecutive_pairs_u32: u32 = if (consecutive_pairs) 1 else 0;
        var scale_f32 = scale;
        var decode_scalars_ptr = decode_scalars.ptr;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&total_chunks_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&rope_dim_u32),
            @ptrCast(&theta_f32),
            @ptrCast(&freq_scale_f32),
            @ptrCast(&position_offset_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&chunks_per_position_u32),
            @ptrCast(&consecutive_pairs_u32),
            @ptrCast(&scale_f32),
            @ptrCast(&decode_scalars_ptr),
        };
        if (captureParamTraceIndex(ctx)) |trace_index| {
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel=rope_scaled_decode_scalars dst=0x{x} input=0x{x} scalars=0x{x} fallback_position_offset={d} seq_len={d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                dst_ptr,
                input_ptr,
                decode_scalars_ptr,
                position_offset_u32,
                seq_len_u32,
            });
        }
        try launch1d(function, ctx, count, &params);
    }

    pub fn launchRopePerItemF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        query_lengths: buffer_mod.DeviceBuffer,
        position_offsets: buffer_mod.DeviceBuffer,
        batch: usize,
        max_seq_len: usize,
        num_heads: usize,
        head_dim: usize,
        rope_dim: usize,
        theta: f32,
        freq_scale: f32,
        consecutive_pairs: bool,
    ) driver_mod.Error!void {
        const function = self.rope_per_item_f32 orelse return error.CudaKernelUnavailable;
        const total_chunks = try checkedTensorElements(try checkedTensorElements(batch, max_seq_len), num_heads);
        const count = try checkedTensorElements(total_chunks, head_dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkRawBytes(query_lengths, batch * @sizeOf(u32));
        try checkRawBytes(position_offsets, batch * @sizeOf(u32));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var query_lengths_ptr = query_lengths.ptr;
        var position_offsets_ptr = position_offsets.ptr;
        var batch_u32 = try toU32(batch);
        var max_seq_len_u32 = try toU32(max_seq_len);
        var num_heads_u32 = try toU32(num_heads);
        var head_dim_u32 = try toU32(head_dim);
        var rope_dim_u32 = try toU32(rope_dim);
        var theta_f32 = theta;
        var freq_scale_f32 = freq_scale;
        var consecutive_pairs_u32: u32 = if (consecutive_pairs) 1 else 0;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&query_lengths_ptr),
            @ptrCast(&position_offsets_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&max_seq_len_u32),
            @ptrCast(&num_heads_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&rope_dim_u32),
            @ptrCast(&theta_f32),
            @ptrCast(&freq_scale_f32),
            @ptrCast(&consecutive_pairs_u32),
        };
        try launch1d(function, ctx, count, &params);
    }

    pub fn launchRmsNormHeadsRopeF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        rows: usize,
        total_dim: usize,
        head_dim: usize,
        rope_dim: usize,
        eps: f32,
        theta: f32,
        freq_scale: f32,
        position_offset: usize,
        seq_len: usize,
        consecutive_pairs: bool,
        output_scale: f32,
    ) driver_mod.Error!void {
        const function = self.rms_norm_heads_rope_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or total_dim == 0 or head_dim == 0 or total_dim % head_dim != 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0 or seq_len == 0) return error.InvalidCudaState;
        const total = try checkedTensorElements(rows, total_dim);
        const total_chunks = total / head_dim;
        const chunks_per_position = total_dim / head_dim;
        try checkBytes(dst, total);
        try checkBytes(input, total);
        try checkBytes(weight, head_dim);

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var total_chunks_u32 = try toU32(total_chunks);
        var head_dim_u32 = try toU32(head_dim);
        var rope_dim_u32 = try toU32(rope_dim);
        var eps_value = eps;
        var theta_value = theta;
        var freq_scale_value = freq_scale;
        var position_offset_u32 = try toU32(position_offset);
        var seq_len_u32 = try toU32(seq_len);
        var chunks_per_position_u32 = try toU32(chunks_per_position);
        var consecutive_pairs_u32: u32 = if (consecutive_pairs) 1 else 0;
        var output_scale_value = output_scale;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&total_chunks_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&rope_dim_u32),
            @ptrCast(&eps_value),
            @ptrCast(&theta_value),
            @ptrCast(&freq_scale_value),
            @ptrCast(&position_offset_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&chunks_per_position_u32),
            @ptrCast(&consecutive_pairs_u32),
            @ptrCast(&output_scale_value),
        };
        if (captureParamTraceIndex(ctx)) |trace_index| {
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel=rms_norm_heads_rope dst=0x{x} input=0x{x} weight=0x{x} total_chunks={d} head_dim={d} rope_dim={d} eps={d:.8} theta={d:.6} freq_scale={d:.6} position_offset={d} seq_len={d} chunks_per_position={d} consecutive_pairs={d} output_scale={d:.6}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                dst_ptr,
                input_ptr,
                weight_ptr,
                total_chunks_u32,
                head_dim_u32,
                rope_dim_u32,
                eps_value,
                theta_value,
                freq_scale_value,
                position_offset_u32,
                seq_len_u32,
                chunks_per_position_u32,
                consecutive_pairs_u32,
                output_scale_value,
            });
        }
        try launchBlocks(function, ctx, total_chunks, f32_tiled_threads, &params);
    }

    pub fn launchRmsNormHeadsRopeDecodeScalarsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight: buffer_mod.DeviceBuffer,
        decode_scalars: buffer_mod.DeviceBuffer,
        rows: usize,
        total_dim: usize,
        head_dim: usize,
        rope_dim: usize,
        eps: f32,
        theta: f32,
        freq_scale: f32,
        position_offset: usize,
        seq_len: usize,
        consecutive_pairs: bool,
        output_scale: f32,
    ) driver_mod.Error!void {
        const function = self.rms_norm_heads_rope_decode_scalars_f32 orelse return error.CudaKernelUnavailable;
        if (rows == 0 or total_dim == 0 or head_dim == 0 or total_dim % head_dim != 0 or rope_dim == 0 or rope_dim > head_dim or rope_dim % 2 != 0 or seq_len == 0) return error.InvalidCudaState;
        try checkRawBytes(decode_scalars, 5 * @sizeOf(u32));
        const total = try checkedTensorElements(rows, total_dim);
        const total_chunks = total / head_dim;
        const chunks_per_position = total_dim / head_dim;
        try checkBytes(dst, total);
        try checkBytes(input, total);
        try checkBytes(weight, head_dim);

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight.ptr;
        var total_chunks_u32 = try toU32(total_chunks);
        var head_dim_u32 = try toU32(head_dim);
        var rope_dim_u32 = try toU32(rope_dim);
        var eps_value = eps;
        var theta_value = theta;
        var freq_scale_value = freq_scale;
        var position_offset_u32 = try toU32(position_offset);
        var seq_len_u32 = try toU32(seq_len);
        var chunks_per_position_u32 = try toU32(chunks_per_position);
        var consecutive_pairs_u32: u32 = if (consecutive_pairs) 1 else 0;
        var output_scale_value = output_scale;
        var decode_scalars_ptr = decode_scalars.ptr;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&total_chunks_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&rope_dim_u32),
            @ptrCast(&eps_value),
            @ptrCast(&theta_value),
            @ptrCast(&freq_scale_value),
            @ptrCast(&position_offset_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&chunks_per_position_u32),
            @ptrCast(&consecutive_pairs_u32),
            @ptrCast(&output_scale_value),
            @ptrCast(&decode_scalars_ptr),
        };
        if (captureParamTraceIndex(ctx)) |trace_index| {
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel=rms_norm_heads_rope_decode_scalars dst=0x{x} input=0x{x} weight=0x{x} scalars=0x{x} fallback_position_offset={d} seq_len={d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                dst_ptr,
                input_ptr,
                weight_ptr,
                decode_scalars_ptr,
                position_offset_u32,
                seq_len_u32,
            });
        }
        try launchBlocks(function, ctx, total_chunks, f32_tiled_threads, &params);
    }

    pub fn launchGqaAttentionF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        q: buffer_mod.DeviceBuffer,
        k: buffer_mod.DeviceBuffer,
        v: buffer_mod.DeviceBuffer,
        attn_or_mask: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        batch: usize,
        q_seq_len: usize,
        kv_seq_len: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        query_position_offset: usize,
        kv_position_offset: usize,
        sliding_window: usize,
        total_sequence_len: usize,
        mask_len: usize,
        bias_mode: u32,
    ) driver_mod.Error!GqaAttentionLaunchKind {
        const scalar_function = self.gqa_attention_f32 orelse return error.CudaKernelUnavailable;
        const q_hidden = try checkedTensorElements(num_heads, head_dim);
        const kv_hidden = try checkedTensorElements(num_kv_heads, head_dim);
        const q_count = try checkedTensorElements(try checkedTensorElements(batch, q_seq_len), q_hidden);
        const kv_count = try checkedTensorElements(try checkedTensorElements(batch, kv_seq_len), kv_hidden);
        try checkBytes(dst, q_count);
        try checkBytes(q, q_count);
        try checkBytes(k, kv_count);
        try checkBytes(v, kv_count);
        if (mask_len != 0) try checkRawBytes(attn_or_mask, mask_len);
        if (bias_mode != 0) try checkBytes(bias, try checkedTensorElements(if (bias_mode == 2) batch * num_heads else num_heads, try checkedTensorElements(q_seq_len, kv_seq_len)));
        if (q_count == 0) return .none;

        var dst_ptr = dst.ptr;
        var q_ptr = q.ptr;
        var k_ptr = k.ptr;
        var v_ptr = v.ptr;
        var mask_ptr = attn_or_mask.ptr;
        var bias_ptr = bias.ptr;
        var batch_u32 = try toU32(batch);
        var q_seq_len_u32 = try toU32(q_seq_len);
        var kv_seq_len_u32 = try toU32(kv_seq_len);
        var num_heads_u32 = try toU32(num_heads);
        var num_kv_heads_u32 = try toU32(num_kv_heads);
        var head_dim_u32 = try toU32(head_dim);
        var query_position_offset_u32 = try toU32(query_position_offset);
        var kv_position_offset_u32 = try toU32(kv_position_offset);
        var sliding_window_u32 = try toU32(sliding_window);
        var total_sequence_len_u32 = try toU32(total_sequence_len);
        var mask_len_u32 = try toU32(mask_len);
        var bias_mode_u32 = bias_mode;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&q_ptr),
            @ptrCast(&k_ptr),
            @ptrCast(&v_ptr),
            @ptrCast(&mask_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&q_seq_len_u32),
            @ptrCast(&kv_seq_len_u32),
            @ptrCast(&num_heads_u32),
            @ptrCast(&num_kv_heads_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&query_position_offset_u32),
            @ptrCast(&kv_position_offset_u32),
            @ptrCast(&sliding_window_u32),
            @ptrCast(&total_sequence_len_u32),
            @ptrCast(&mask_len_u32),
            @ptrCast(&bias_mode_u32),
        };
        if (captureParamTraceIndex(ctx)) |trace_index| {
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel=gqa_attention dst=0x{x} q=0x{x} k=0x{x} v=0x{x} mask=0x{x} bias=0x{x} batch={d} q_seq_len={d} kv_seq_len={d} num_heads={d} num_kv_heads={d} head_dim={d} query_position_offset={d} kv_position_offset={d} sliding_window={d} total_sequence_len={d} mask_len={d} bias_mode={d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                dst_ptr,
                q_ptr,
                k_ptr,
                v_ptr,
                mask_ptr,
                bias_ptr,
                batch_u32,
                q_seq_len_u32,
                kv_seq_len_u32,
                num_heads_u32,
                num_kv_heads_u32,
                head_dim_u32,
                query_position_offset_u32,
                kv_position_offset_u32,
                sliding_window_u32,
                total_sequence_len_u32,
                mask_len_u32,
                bias_mode_u32,
            });
        }
        if (self.gqa_attention_prefill_fast_f32) |prefill_function| {
            if (fastGqaPrefillEligible(batch, q_seq_len, num_heads, num_kv_heads, head_dim, mask_len, bias_mode)) {
                const block: c_uint = if (head_dim <= 256) 256 else 512;
                const grid = try toU32(try checkedTensorElements(try checkedTensorElements(batch, q_seq_len), num_heads));
                try ctx.makeCurrent();
                try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
                    prefill_function,
                    grid,
                    1,
                    1,
                    block,
                    1,
                    1,
                    0,
                    ctx.stream,
                    &params,
                    null,
                ));
                ctx.noteKernelLaunch();
                return .prefill_fast;
            }
        }
        if (self.gqa_attention_decode_f32) |decode_function| {
            if (head_dim <= 512) {
                const block: c_uint = if (head_dim <= 256) 256 else 512;
                const grid = try toU32(try checkedTensorElements(try checkedTensorElements(batch, q_seq_len), num_heads));
                try ctx.makeCurrent();
                try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
                    decode_function,
                    grid,
                    1,
                    1,
                    block,
                    1,
                    1,
                    0,
                    ctx.stream,
                    &params,
                    null,
                ));
                ctx.noteKernelLaunch();
                return .decode;
            }
        }

        try launch1d(scalar_function, ctx, q_count, &params);
        return .scalar;
    }

    pub fn launchGqaAttentionDecodeScalarsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        q: buffer_mod.DeviceBuffer,
        k: buffer_mod.DeviceBuffer,
        v: buffer_mod.DeviceBuffer,
        attn_or_mask: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        decode_scalars: buffer_mod.DeviceBuffer,
        batch: usize,
        q_seq_len: usize,
        kv_seq_len: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        query_position_offset: usize,
        kv_position_offset: usize,
        sliding_window: usize,
        total_sequence_len: usize,
        mask_len: usize,
        bias_mode: u32,
    ) driver_mod.Error!GqaAttentionLaunchKind {
        const fallback_function = self.gqa_attention_decode_scalars_f32 orelse return error.CudaKernelUnavailable;
        if (head_dim > 512) return error.CudaKernelUnavailable;
        const q_hidden = try checkedTensorElements(num_heads, head_dim);
        const kv_hidden = try checkedTensorElements(num_kv_heads, head_dim);
        const q_count = try checkedTensorElements(try checkedTensorElements(batch, q_seq_len), q_hidden);
        const kv_count = try checkedTensorElements(try checkedTensorElements(batch, kv_seq_len), kv_hidden);
        try checkBytes(dst, q_count);
        try checkBytes(q, q_count);
        try checkBytes(k, kv_count);
        try checkBytes(v, kv_count);
        try checkRawBytes(decode_scalars, 5 * @sizeOf(u32));
        if (mask_len != 0) try checkRawBytes(attn_or_mask, mask_len);
        if (bias_mode != 0) try checkBytes(bias, try checkedTensorElements(if (bias_mode == 2) batch * num_heads else num_heads, try checkedTensorElements(q_seq_len, kv_seq_len)));
        if (q_count == 0) return .none;

        var dst_ptr = dst.ptr;
        var q_ptr = q.ptr;
        var k_ptr = k.ptr;
        var v_ptr = v.ptr;
        var mask_ptr = attn_or_mask.ptr;
        var bias_ptr = bias.ptr;
        var decode_scalars_ptr = decode_scalars.ptr;
        var batch_u32 = try toU32(batch);
        var q_seq_len_u32 = try toU32(q_seq_len);
        var kv_seq_len_u32 = try toU32(kv_seq_len);
        var num_heads_u32 = try toU32(num_heads);
        var num_kv_heads_u32 = try toU32(num_kv_heads);
        var head_dim_u32 = try toU32(head_dim);
        var query_position_offset_u32 = try toU32(query_position_offset);
        var kv_position_offset_u32 = try toU32(kv_position_offset);
        var sliding_window_u32 = try toU32(sliding_window);
        var total_sequence_len_u32 = try toU32(total_sequence_len);
        var mask_len_u32 = try toU32(mask_len);
        var bias_mode_u32 = bias_mode;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&q_ptr),
            @ptrCast(&k_ptr),
            @ptrCast(&v_ptr),
            @ptrCast(&mask_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&q_seq_len_u32),
            @ptrCast(&kv_seq_len_u32),
            @ptrCast(&num_heads_u32),
            @ptrCast(&num_kv_heads_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&query_position_offset_u32),
            @ptrCast(&kv_position_offset_u32),
            @ptrCast(&sliding_window_u32),
            @ptrCast(&total_sequence_len_u32),
            @ptrCast(&mask_len_u32),
            @ptrCast(&bias_mode_u32),
            @ptrCast(&decode_scalars_ptr),
        };
        var function = fallback_function;
        var launch_kind: GqaAttentionLaunchKind = .decode;
        if (fastGqaDecodeEligible(batch, q_seq_len, num_heads, num_kv_heads, head_dim, mask_len, bias_mode)) {
            launch_kind = .decode_fast_fallback;
            if (!fastGqaDecodeDisabled()) {
                if (self.gqa_attention_decode_scalars_fast_f32) |fast_function| {
                    function = fast_function;
                    launch_kind = .decode_fast;
                }
            }
        }
        if (captureParamTraceIndex(ctx)) |trace_index| {
            const kernel_name = if (launch_kind == .decode_fast) "gqa_attention_decode_scalars_fast" else "gqa_attention_decode_scalars";
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel={s} dst=0x{x} q=0x{x} k=0x{x} v=0x{x} scalars=0x{x} fallback_query_position_offset={d} fallback_kv_seq_len={d} fallback_total_sequence_len={d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                kernel_name,
                dst_ptr,
                q_ptr,
                k_ptr,
                v_ptr,
                decode_scalars_ptr,
                query_position_offset_u32,
                kv_seq_len_u32,
                total_sequence_len_u32,
            });
        }
        const block: c_uint = if (head_dim <= 256) 256 else 512;
        const grid = try toU32(try checkedTensorElements(try checkedTensorElements(batch, q_seq_len), num_heads));
        try ctx.makeCurrent();
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
            function,
            grid,
            1,
            1,
            block,
            1,
            1,
            0,
            ctx.stream,
            &params,
            null,
        ));
        ctx.noteKernelLaunch();
        return launch_kind;
    }

    pub fn launchKvWriteSuffixDecodeScalarsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        k_dst: buffer_mod.DeviceBuffer,
        v_dst: buffer_mod.DeviceBuffer,
        k_src: buffer_mod.DeviceBuffer,
        v_src: buffer_mod.DeviceBuffer,
        decode_scalars: buffer_mod.DeviceBuffer,
        suffix_token_count: usize,
        row_width: usize,
        fallback_total_token_count: usize,
    ) driver_mod.Error!void {
        const function = self.kv_write_suffix_decode_scalars_f32 orelse return error.CudaKernelUnavailable;
        try checkRawBytes(decode_scalars, 5 * @sizeOf(u32));
        const total = try checkedTensorElements(suffix_token_count, row_width);
        try checkBytes(k_src, total);
        try checkBytes(v_src, total);
        try checkBytes(k_dst, try checkedTensorElements(fallback_total_token_count, row_width));
        try checkBytes(v_dst, try checkedTensorElements(fallback_total_token_count, row_width));

        var k_dst_ptr = k_dst.ptr;
        var v_dst_ptr = v_dst.ptr;
        var k_src_ptr = k_src.ptr;
        var v_src_ptr = v_src.ptr;
        var decode_scalars_ptr = decode_scalars.ptr;
        var suffix_token_count_u32 = try toU32(suffix_token_count);
        var row_width_u32 = try toU32(row_width);
        var fallback_total_token_count_u32 = try toU32(fallback_total_token_count);
        var params = [_]?*anyopaque{
            @ptrCast(&k_dst_ptr),
            @ptrCast(&v_dst_ptr),
            @ptrCast(&k_src_ptr),
            @ptrCast(&v_src_ptr),
            @ptrCast(&decode_scalars_ptr),
            @ptrCast(&suffix_token_count_u32),
            @ptrCast(&row_width_u32),
            @ptrCast(&fallback_total_token_count_u32),
        };
        if (captureParamTraceIndex(ctx)) |trace_index| {
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel=kv_write_suffix_decode_scalars k_dst=0x{x} v_dst=0x{x} k_src=0x{x} v_src=0x{x} scalars=0x{x} suffix={d} row_width={d} fallback_total_token_count={d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                k_dst_ptr,
                v_dst_ptr,
                k_src_ptr,
                v_src_ptr,
                decode_scalars_ptr,
                suffix_token_count_u32,
                row_width_u32,
                fallback_total_token_count_u32,
            });
        }
        try launch1d(function, ctx, total, &params);
    }

    pub fn launchKvWriteSuffixTurboquantF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        k_dst: buffer_mod.DeviceBuffer,
        v_dst: buffer_mod.DeviceBuffer,
        block_table: buffer_mod.DeviceBuffer,
        k_src: buffer_mod.DeviceBuffer,
        v_src: buffer_mod.DeviceBuffer,
        decode_scalars: buffer_mod.DeviceBuffer,
        suffix_token_count: usize,
        row_width: usize,
        num_kv_heads: usize,
        head_dim: usize,
        key_row_bytes: usize,
        base_key_row_bytes: usize,
        value_row_bytes: usize,
        fallback_total_token_count: usize,
        block_count: usize,
        page_size_tokens: usize,
        format: u32,
        value_format: u32,
        physical_token_capacity: usize,
    ) driver_mod.Error!void {
        const function = self.kv_write_suffix_turboquant_f32 orelse return error.CudaKernelUnavailable;
        if (suffix_token_count == 0 or row_width == 0 or key_row_bytes == 0 or base_key_row_bytes == 0 or value_row_bytes == 0) return error.InvalidCudaState;
        if (base_key_row_bytes > key_row_bytes) return error.InvalidCudaState;
        const suffix_values = try checkedTensorElements(suffix_token_count, row_width);
        try checkBytes(k_src, suffix_values);
        try checkBytes(v_src, suffix_values);
        try checkRawBytes(k_dst, try checkedTensorElements(physical_token_capacity, key_row_bytes));
        try checkRawBytes(v_dst, try checkedTensorElements(physical_token_capacity, value_row_bytes));
        if (block_count != 0) try checkRawBytes(block_table, try checkedTensorElements(block_count, @sizeOf(u32)));
        if (decode_scalars.ptr != 0) try checkRawBytes(decode_scalars, 5 * @sizeOf(u32));

        var k_dst_ptr = k_dst.ptr;
        var v_dst_ptr = v_dst.ptr;
        var block_table_ptr = block_table.ptr;
        var k_src_ptr = k_src.ptr;
        var v_src_ptr = v_src.ptr;
        var decode_scalars_ptr = decode_scalars.ptr;
        var suffix_token_count_u32 = try toU32(suffix_token_count);
        var row_width_u32 = try toU32(row_width);
        var num_kv_heads_u32 = try toU32(num_kv_heads);
        var head_dim_u32 = try toU32(head_dim);
        var key_row_bytes_u32 = try toU32(key_row_bytes);
        var base_key_row_bytes_u32 = try toU32(base_key_row_bytes);
        var value_row_bytes_u32 = try toU32(value_row_bytes);
        var fallback_total_token_count_u32 = try toU32(fallback_total_token_count);
        var block_count_u32 = try toU32(block_count);
        var page_size_tokens_u32 = try toU32(page_size_tokens);
        var format_u32 = format;
        var value_format_u32 = value_format;
        var physical_token_capacity_u32 = try toU32(physical_token_capacity);
        var params = [_]?*anyopaque{
            @ptrCast(&k_dst_ptr),
            @ptrCast(&v_dst_ptr),
            @ptrCast(&block_table_ptr),
            @ptrCast(&k_src_ptr),
            @ptrCast(&v_src_ptr),
            @ptrCast(&decode_scalars_ptr),
            @ptrCast(&suffix_token_count_u32),
            @ptrCast(&row_width_u32),
            @ptrCast(&num_kv_heads_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&key_row_bytes_u32),
            @ptrCast(&base_key_row_bytes_u32),
            @ptrCast(&value_row_bytes_u32),
            @ptrCast(&fallback_total_token_count_u32),
            @ptrCast(&block_count_u32),
            @ptrCast(&page_size_tokens_u32),
            @ptrCast(&format_u32),
            @ptrCast(&value_format_u32),
            @ptrCast(&physical_token_capacity_u32),
        };
        if (captureParamTraceIndex(ctx)) |trace_index| {
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel=kv_write_suffix_turboquant k_dst=0x{x} v_dst=0x{x} block_table=0x{x} k_src=0x{x} v_src=0x{x} scalars=0x{x} suffix={d} row_width={d} key_row_bytes={d} base_key_row_bytes={d} value_row_bytes={d} fallback_total_token_count={d} block_count={d} page_size={d} format={d} value_format={d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                k_dst_ptr,
                v_dst_ptr,
                block_table_ptr,
                k_src_ptr,
                v_src_ptr,
                decode_scalars_ptr,
                suffix_token_count_u32,
                row_width_u32,
                key_row_bytes_u32,
                base_key_row_bytes_u32,
                value_row_bytes_u32,
                fallback_total_token_count_u32,
                block_count_u32,
                page_size_tokens_u32,
                format_u32,
                value_format_u32,
            });
        }
        const work_items = @max(try checkedTensorElements(suffix_token_count, key_row_bytes), try checkedTensorElements(suffix_token_count, value_row_bytes));
        try launch1d(function, ctx, work_items, &params);
    }

    pub fn launchGqaAttentionDecodeTurboquantF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        q: buffer_mod.DeviceBuffer,
        k: buffer_mod.DeviceBuffer,
        v: buffer_mod.DeviceBuffer,
        block_table: buffer_mod.DeviceBuffer,
        attn_or_mask: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        batch: usize,
        q_seq_len: usize,
        kv_seq_len: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        query_position_offset: usize,
        kv_position_offset: usize,
        sliding_window: usize,
        total_sequence_len: usize,
        mask_len: usize,
        bias_mode: u32,
        key_row_bytes: usize,
        base_key_row_bytes: usize,
        value_row_bytes: usize,
        block_count: usize,
        page_size_tokens: usize,
        format: u32,
        value_format: u32,
        physical_token_capacity: usize,
        decode_scalars: buffer_mod.DeviceBuffer,
    ) driver_mod.Error!GqaAttentionLaunchKind {
        const fallback_function = self.gqa_attention_decode_turboquant_f32 orelse return error.CudaKernelUnavailable;
        if (head_dim > 512 or key_row_bytes == 0 or base_key_row_bytes == 0 or value_row_bytes == 0 or base_key_row_bytes > key_row_bytes) return error.CudaKernelUnavailable;
        const q_hidden = try checkedTensorElements(num_heads, head_dim);
        const q_count = try checkedTensorElements(try checkedTensorElements(batch, q_seq_len), q_hidden);
        try checkBytes(dst, q_count);
        try checkBytes(q, q_count);
        try checkRawBytes(k, try checkedTensorElements(physical_token_capacity, key_row_bytes));
        try checkRawBytes(v, try checkedTensorElements(physical_token_capacity, value_row_bytes));
        if (block_count != 0) try checkRawBytes(block_table, try checkedTensorElements(block_count, @sizeOf(u32)));
        if (mask_len != 0) try checkRawBytes(attn_or_mask, mask_len);
        if (bias_mode != 0) try checkBytes(bias, try checkedTensorElements(if (bias_mode == 2) batch * num_heads else num_heads, try checkedTensorElements(q_seq_len, kv_seq_len)));
        if (decode_scalars.ptr != 0) try checkRawBytes(decode_scalars, 5 * @sizeOf(u32));
        if (q_count == 0) return .none;

        var dst_ptr = dst.ptr;
        var q_ptr = q.ptr;
        var k_ptr = k.ptr;
        var v_ptr = v.ptr;
        var block_table_ptr = block_table.ptr;
        var mask_ptr = attn_or_mask.ptr;
        var bias_ptr = bias.ptr;
        var batch_u32 = try toU32(batch);
        var q_seq_len_u32 = try toU32(q_seq_len);
        var kv_seq_len_u32 = try toU32(kv_seq_len);
        var num_heads_u32 = try toU32(num_heads);
        var num_kv_heads_u32 = try toU32(num_kv_heads);
        var head_dim_u32 = try toU32(head_dim);
        var query_position_offset_u32 = try toU32(query_position_offset);
        var kv_position_offset_u32 = try toU32(kv_position_offset);
        var sliding_window_u32 = try toU32(sliding_window);
        var total_sequence_len_u32 = try toU32(total_sequence_len);
        var mask_len_u32 = try toU32(mask_len);
        var bias_mode_u32 = bias_mode;
        var key_row_bytes_u32 = try toU32(key_row_bytes);
        var base_key_row_bytes_u32 = try toU32(base_key_row_bytes);
        var value_row_bytes_u32 = try toU32(value_row_bytes);
        var block_count_u32 = try toU32(block_count);
        var page_size_tokens_u32 = try toU32(page_size_tokens);
        var format_u32 = format;
        var value_format_u32 = value_format;
        var physical_token_capacity_u32 = try toU32(physical_token_capacity);
        var decode_scalars_ptr = decode_scalars.ptr;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&q_ptr),
            @ptrCast(&k_ptr),
            @ptrCast(&v_ptr),
            @ptrCast(&block_table_ptr),
            @ptrCast(&mask_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&q_seq_len_u32),
            @ptrCast(&kv_seq_len_u32),
            @ptrCast(&num_heads_u32),
            @ptrCast(&num_kv_heads_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&query_position_offset_u32),
            @ptrCast(&kv_position_offset_u32),
            @ptrCast(&sliding_window_u32),
            @ptrCast(&total_sequence_len_u32),
            @ptrCast(&mask_len_u32),
            @ptrCast(&bias_mode_u32),
            @ptrCast(&key_row_bytes_u32),
            @ptrCast(&base_key_row_bytes_u32),
            @ptrCast(&value_row_bytes_u32),
            @ptrCast(&block_count_u32),
            @ptrCast(&page_size_tokens_u32),
            @ptrCast(&format_u32),
            @ptrCast(&value_format_u32),
            @ptrCast(&physical_token_capacity_u32),
            @ptrCast(&decode_scalars_ptr),
        };
        var function = fallback_function;
        var launch_kind: GqaAttentionLaunchKind = .decode;
        if ((format == 0 or format == 2) and
            base_key_row_bytes == key_row_bytes and
            decode_scalars.ptr == 0 and
            (fastGqaPrefillEnabled() or tiledGqaPrefillEnabled() or mmaGqaPrefillEnabled()) and
            gqaPrefillShapeEligible(batch, q_seq_len, num_heads, num_kv_heads, head_dim, mask_len, bias_mode))
        {
            if (fastGqaPrefillEnabled()) {
                if (self.gqa_attention_prefill_turboquant_fast_f32) |prefill_function| {
                    function = prefill_function;
                    launch_kind = .prefill_fast;
                }
            }
            if (tiledGqaPrefillEnabled()) {
                if (self.gqa_attention_prefill_turboquant_tiled_f32) |tiled_function| {
                    function = tiled_function;
                    launch_kind = .prefill_tiled;
                }
            }
            // Tensor-core prefill. The >48KB dynamic-smem opt-in needs
            // cuFuncSetAttribute. head_dim <= 256 runs everywhere; the
            // head_dim-512 global layers and the TILE_M=32 variant need the
            // larger sm80+ shared-memory budget (89-97KB per block).
            if (mmaGqaPrefillEnabled() and ctx.driver.fns.cuFuncSetAttribute != null) {
                const sm80_plus = ctx.info.compute_major >= 8;
                const mma_hd_ok = head_dim <= 256 or (head_dim <= 512 and sm80_plus);
                if (head_dim <= 256 and sm80_plus and mmaM32GqaPrefillEnabled()) {
                    if (self.gqa_attention_prefill_turboquant_mma_m32_f32) |mma_m32_function| {
                        function = mma_m32_function;
                        launch_kind = .prefill_mma_m32;
                    }
                }
                if (launch_kind != .prefill_mma_m32 and mma_hd_ok) {
                    if (self.gqa_attention_prefill_turboquant_mma_f32) |mma_function| {
                        function = mma_function;
                        launch_kind = .prefill_mma;
                    }
                }
            }
        } else if ((format == 0 or format == 2) and
            base_key_row_bytes == key_row_bytes and
            fastGqaDecodeEligible(batch, q_seq_len, num_heads, num_kv_heads, head_dim, mask_len, bias_mode))
        {
            launch_kind = .decode_fast_fallback;
            if (!fastGqaDecodeDisabled()) {
                if (self.gqa_attention_decode_turboquant_fast_f32) |fast_function| {
                    function = fast_function;
                    launch_kind = .decode_fast;
                }
            }
        }
        if (captureParamTraceIndex(ctx)) |trace_index| {
            const kernel_name = switch (launch_kind) {
                .decode_fast => "gqa_attention_decode_turboquant_fast",
                .prefill_fast => "gqa_attention_prefill_turboquant_fast",
                .prefill_tiled => "gqa_attention_prefill_turboquant_tiled",
                .prefill_mma => "gqa_attention_prefill_turboquant_mma",
                .prefill_mma_m32 => "gqa_attention_prefill_turboquant_mma_m32",
                else => "gqa_attention_decode_turboquant",
            };
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel={s} dst=0x{x} q=0x{x} k=0x{x} v=0x{x} block_table=0x{x} mask=0x{x} bias=0x{x} scalars=0x{x} batch={d} q_seq_len={d} kv_seq_len={d} num_heads={d} num_kv_heads={d} head_dim={d} key_row_bytes={d} base_key_row_bytes={d} value_row_bytes={d} block_count={d} page_size={d} format={d} value_format={d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
                kernel_name,
                dst_ptr,
                q_ptr,
                k_ptr,
                v_ptr,
                block_table_ptr,
                mask_ptr,
                bias_ptr,
                decode_scalars_ptr,
                batch_u32,
                q_seq_len_u32,
                kv_seq_len_u32,
                num_heads_u32,
                num_kv_heads_u32,
                head_dim_u32,
                key_row_bytes_u32,
                base_key_row_bytes_u32,
                value_row_bytes_u32,
                block_count_u32,
                page_size_tokens_u32,
                format_u32,
                value_format_u32,
            });
        }
        var block: c_uint = if (head_dim <= 256) 256 else 512;
        var grid_x = try toU32(try checkedTensorElements(try checkedTensorElements(batch, q_seq_len), num_heads));
        var grid_y: c_uint = 1;
        var shared_bytes: c_uint = 0;
        if (launch_kind == .prefill_tiled) {
            // Tiled prefill: grid = (num_heads, ceil(q_seq_len / TILE_M)), TILE_M = 16.
            // Dynamic smem holds one BF16 K/V tile: tile_n * (head_dim + 2) * 2 bytes.
            block = 256;
            grid_x = try toU32(num_heads);
            grid_y = try toU32((q_seq_len + 15) / 16);
            const tile_n: usize = if (head_dim <= 256) 32 else 16;
            shared_bytes = try toU32(tile_n * (head_dim + 2) * @sizeOf(u16));
        } else if (launch_kind == .prefill_mma) {
            // MMA prefill: same grid shape as tiled. Dynamic smem holds the
            // f16 Q tile (16*hd), one tile_n-key f16 K/V tile (tile_n*(hd+8)),
            // the f32 score tile (16*72), the f16 P tile (16*72), and the f32
            // output tile (16*hd). tile_n is 64 for head_dim <= 256 (224*hd +
            // 7936 bytes) and 32 for the 512-dim global layers (160*hd + 7424).
            block = 256;
            grid_x = try toU32(num_heads);
            grid_y = try toU32((q_seq_len + 15) / 16);
            shared_bytes = if (head_dim <= 256)
                try toU32(224 * head_dim + 7936)
            else
                try toU32(160 * head_dim + 7424);
        } else if (launch_kind == .prefill_mma_m32) {
            // TILE_M=32 MMA prefill: 32 query rows per block, 64-key tiles.
            // Dynamic smem: 320*hd + 14848 bytes (96768 at head_dim 256).
            block = 256;
            grid_x = try toU32(num_heads);
            grid_y = try toU32((q_seq_len + 31) / 32);
            shared_bytes = try toU32(320 * head_dim + 14848);
        }
        try ctx.makeCurrent();
        if (launch_kind == .prefill_mma and !self.gqa_prefill_mma_smem_opted_in) {
            // CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES = 8. The
            // attribute must cover this function's largest launch: 89344 for
            // the head_dim-512 config on sm80+, 65280 (head_dim 256, within
            // Turing's 64KB opt-in cap) otherwise.
            const set_attr = ctx.driver.fns.cuFuncSetAttribute.?;
            const max_shared: c_int = if (ctx.info.compute_major >= 8) 89344 else 65280;
            try ctx.driver.check(set_attr(function, 8, max_shared));
            self.gqa_prefill_mma_smem_opted_in = true;
        }
        if (launch_kind == .prefill_mma_m32 and !self.gqa_prefill_mma_m32_smem_opted_in) {
            const set_attr = ctx.driver.fns.cuFuncSetAttribute.?;
            try ctx.driver.check(set_attr(function, 8, 96768));
            self.gqa_prefill_mma_m32_smem_opted_in = true;
        }
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
            function,
            grid_x,
            grid_y,
            1,
            block,
            1,
            1,
            shared_bytes,
            ctx.stream,
            &params,
            null,
        ));
        ctx.noteKernelLaunch();
        return launch_kind;
    }

    pub fn launchGqaAttentionDecodeTurboquantSplitF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        partial_acc: buffer_mod.DeviceBuffer,
        partial_meta: buffer_mod.DeviceBuffer,
        q: buffer_mod.DeviceBuffer,
        k: buffer_mod.DeviceBuffer,
        v: buffer_mod.DeviceBuffer,
        block_table: buffer_mod.DeviceBuffer,
        q_count: usize,
        kv_seq_len: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        query_position_offset: usize,
        kv_position_offset: usize,
        sliding_window: usize,
        key_row_bytes: usize,
        value_row_bytes: usize,
        block_count: usize,
        page_size_tokens: usize,
        format: u32,
        value_format: u32,
        physical_token_capacity: usize,
        decode_scalars: buffer_mod.DeviceBuffer,
        chunk_size: usize,
        chunk_count: usize,
    ) driver_mod.Error!bool {
        const stage1 = if (format == 0 and value_format == 1 and block_count == 0)
            (self.gqa_attention_decode_turboquant_split_stage1_polar4_int8_identity_f32 orelse self.gqa_attention_decode_turboquant_split_stage1_f32 orelse return false)
        else
            (self.gqa_attention_decode_turboquant_split_stage1_f32 orelse return false);
        const stage2 = self.gqa_attention_decode_turboquant_split_stage2_f32 orelse return false;
        if ((format != 0 and format != 2) or chunk_size == 0 or chunk_count <= 1 or chunk_count > 128) return false;
        if (!fastGqaDecodeEligible(1, 1, num_heads, num_kv_heads, head_dim, 0, 0)) return false;
        if (key_row_bytes == 0 or value_row_bytes == 0) return false;

        try checkBytes(dst, q_count);
        try checkBytes(q, q_count);
        try checkRawBytes(k, try checkedTensorElements(physical_token_capacity, key_row_bytes));
        try checkRawBytes(v, try checkedTensorElements(physical_token_capacity, value_row_bytes));
        if (block_count != 0) try checkRawBytes(block_table, try checkedTensorElements(block_count, @sizeOf(u32)));
        if (decode_scalars.ptr != 0) try checkRawBytes(decode_scalars, 5 * @sizeOf(u32));
        try checkBytes(partial_acc, try checkedTensorElements(try checkedTensorElements(num_heads, chunk_count), head_dim));
        try checkBytes(partial_meta, try checkedTensorElements(try checkedTensorElements(num_heads, chunk_count), @as(usize, 2)));

        var partial_acc_ptr = partial_acc.ptr;
        var partial_meta_ptr = partial_meta.ptr;
        var dst_ptr = dst.ptr;
        var q_ptr = q.ptr;
        var k_ptr = k.ptr;
        var v_ptr = v.ptr;
        var block_table_ptr = block_table.ptr;
        var kv_seq_len_u32 = try toU32(kv_seq_len);
        var num_heads_u32 = try toU32(num_heads);
        var num_kv_heads_u32 = try toU32(num_kv_heads);
        var head_dim_u32 = try toU32(head_dim);
        var query_position_offset_u32 = try toU32(query_position_offset);
        var kv_position_offset_u32 = try toU32(kv_position_offset);
        var sliding_window_u32 = try toU32(sliding_window);
        var key_row_bytes_u32 = try toU32(key_row_bytes);
        var value_row_bytes_u32 = try toU32(value_row_bytes);
        var block_count_u32 = try toU32(block_count);
        var page_size_tokens_u32 = try toU32(page_size_tokens);
        var format_u32 = format;
        var value_format_u32 = value_format;
        var physical_token_capacity_u32 = try toU32(physical_token_capacity);
        var decode_scalars_ptr = decode_scalars.ptr;
        var chunk_size_u32 = try toU32(chunk_size);
        var chunk_count_u32 = try toU32(chunk_count);

        var stage1_params = [_]?*anyopaque{
            @ptrCast(&partial_acc_ptr),
            @ptrCast(&partial_meta_ptr),
            @ptrCast(&q_ptr),
            @ptrCast(&k_ptr),
            @ptrCast(&v_ptr),
            @ptrCast(&block_table_ptr),
            @ptrCast(&kv_seq_len_u32),
            @ptrCast(&num_heads_u32),
            @ptrCast(&num_kv_heads_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&query_position_offset_u32),
            @ptrCast(&kv_position_offset_u32),
            @ptrCast(&sliding_window_u32),
            @ptrCast(&key_row_bytes_u32),
            @ptrCast(&value_row_bytes_u32),
            @ptrCast(&block_count_u32),
            @ptrCast(&page_size_tokens_u32),
            @ptrCast(&format_u32),
            @ptrCast(&value_format_u32),
            @ptrCast(&physical_token_capacity_u32),
            @ptrCast(&decode_scalars_ptr),
            @ptrCast(&chunk_size_u32),
            @ptrCast(&chunk_count_u32),
        };
        try launch2d(stage1, ctx, num_heads, chunk_count, if (head_dim <= 256) 256 else 512, &stage1_params);

        var stage2_params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&partial_acc_ptr),
            @ptrCast(&partial_meta_ptr),
            @ptrCast(&num_heads_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&kv_seq_len_u32),
            @ptrCast(&decode_scalars_ptr),
            @ptrCast(&chunk_size_u32),
            @ptrCast(&chunk_count_u32),
        };
        try launchBlocks(stage2, ctx, num_heads, if (head_dim <= 256) 256 else 512, &stage2_params);
        return true;
    }

    pub fn launchDebertaAttentionF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        q: buffer_mod.DeviceBuffer,
        k: buffer_mod.DeviceBuffer,
        v: buffer_mod.DeviceBuffer,
        q_r: buffer_mod.DeviceBuffer,
        k_r: buffer_mod.DeviceBuffer,
        mask: buffer_mod.DeviceBuffer,
        batch: usize,
        seq_len: usize,
        num_heads: usize,
        head_dim: usize,
    ) driver_mod.Error!void {
        const hidden = try checkedTensorElements(num_heads, head_dim);
        const count = try checkedTensorElements(try checkedTensorElements(batch, seq_len), hidden);
        const rel_count = try checkedTensorElements(2 * seq_len - 1, hidden);
        try checkBytes(dst, count);
        try checkBytes(q, count);
        try checkBytes(k, count);
        try checkBytes(v, count);
        try checkBytes(q_r, rel_count);
        try checkBytes(k_r, rel_count);
        try checkRawBytes(mask, try checkedTensorElements(batch, seq_len) * @sizeOf(i64));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var q_ptr = q.ptr;
        var k_ptr = k.ptr;
        var v_ptr = v.ptr;
        var q_r_ptr = q_r.ptr;
        var k_r_ptr = k_r.ptr;
        var mask_ptr = mask.ptr;
        var batch_u32 = try toU32(batch);
        var seq_len_u32 = try toU32(seq_len);
        var num_heads_u32 = try toU32(num_heads);
        var head_dim_u32 = try toU32(head_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&q_ptr),
            @ptrCast(&k_ptr),
            @ptrCast(&v_ptr),
            @ptrCast(&q_r_ptr),
            @ptrCast(&k_r_ptr),
            @ptrCast(&mask_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&num_heads_u32),
            @ptrCast(&head_dim_u32),
        };
        try launch1d(self.deberta_attention_f32, ctx, count, &params);
    }

    pub fn launchSplitLastDim3F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        first: buffer_mod.DeviceBuffer,
        second: buffer_mod.DeviceBuffer,
        third: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        rows: usize,
        dim: usize,
    ) driver_mod.Error!void {
        const total = try checkedTensorElements(rows, dim);
        try checkBytes(first, total);
        try checkBytes(second, total);
        try checkBytes(third, total);
        try checkBytes(input, try checkedTensorElements(total, 3));
        if (total == 0) return;

        var first_ptr = first.ptr;
        var second_ptr = second.ptr;
        var third_ptr = third.ptr;
        var input_ptr = input.ptr;
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var params = [_]?*anyopaque{
            @ptrCast(&first_ptr),
            @ptrCast(&second_ptr),
            @ptrCast(&third_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
        };
        try launch1d(self.split_last_dim3_f32, ctx, total, &params);
    }

    pub fn launchLinearQ8_0F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q8_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch1d(self.linear_q8_0_f32, ctx, out_count, &params);
    }

    pub fn launchLinearQ8_0Tile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q8_0_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q8_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q8_0_col_tile - 1) / q8_0_col_tile, rows, q8_0_tiled_threads, &params);
    }

    pub fn launchLinearQ8_0GatedDownTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        up: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q8_0_gated_down_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_0_values_per_block;
        const in_count = try checkedTensorElements(rows, in_dim);
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(gate, in_count);
        try checkBytes(up, in_count);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q8_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var gate_ptr = gate.ptr;
        var up_ptr = up.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&up_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launch2d(function, ctx, (out_dim + q8_0_col_tile - 1) / q8_0_col_tile, rows, q8_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0GatedDownTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        up: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_gated_down_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const in_count = try checkedTensorElements(rows, in_dim);
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(gate, in_count);
        try checkBytes(up, in_count);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var gate_ptr = gate.ptr;
        var up_ptr = up.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&up_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, rows, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0GatedDownTile4W4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        up: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_gated_down_f32_tile4_w4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const in_count = try checkedTensorElements(rows, in_dim);
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(gate, in_count);
        try checkBytes(up, in_count);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var gate_ptr = gate.ptr;
        var up_ptr = up.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&up_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, rows, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0GatedDownTile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        up: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_gated_down_f32_tile8 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const in_count = try checkedTensorElements(rows, in_dim);
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(gate, in_count);
        try checkBytes(up, in_count);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var gate_ptr = gate.ptr;
        var up_ptr = up.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&up_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8, rows, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0GatedDownTile16F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        up: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_gated_down_f32_tile16 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const in_count = try checkedTensorElements(rows, in_dim);
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(gate, in_count);
        try checkBytes(up, in_count);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var gate_ptr = gate.ptr;
        var up_ptr = up.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&up_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile16 - 1) / q4_0_col_tile16, rows, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ8_0VariantF32(
        self: *KernelModule,
        variant: QMatmulVariant,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const fn_cols_rows = self.q8VariantFunction(variant, .none) orelse return error.CudaSymbolMissing;
        try self.launchLinearQ8_0RowsColsCommon(ctx, fn_cols_rows.function, dst, input, weight_raw, .{}, .{}, rows, in_dim, out_dim, .none, fn_cols_rows.cols, fn_cols_rows.rows);
    }

    pub fn launchLinearQ8_0BiasVariantF32(
        self: *KernelModule,
        variant: QMatmulVariant,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const fn_cols_rows = self.q8VariantFunction(variant, .bias) orelse return error.CudaSymbolMissing;
        try self.launchLinearQ8_0RowsColsCommon(ctx, fn_cols_rows.function, dst, input, weight_raw, bias, .{}, rows, in_dim, out_dim, .bias, fn_cols_rows.cols, fn_cols_rows.rows);
    }

    pub fn launchLinearQ8_0BiasGeluVariantF32(
        self: *KernelModule,
        variant: QMatmulVariant,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const fn_cols_rows = self.q8VariantFunction(variant, .bias_gelu) orelse return error.CudaSymbolMissing;
        try self.launchLinearQ8_0RowsColsCommon(ctx, fn_cols_rows.function, dst, input, weight_raw, bias, .{}, rows, in_dim, out_dim, .bias_gelu, fn_cols_rows.cols, fn_cols_rows.rows);
    }

    pub fn launchLinearQ8_0BiasAddVariantF32(
        self: *KernelModule,
        variant: QMatmulVariant,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const fn_cols_rows = self.q8VariantFunction(variant, .bias_add) orelse return error.CudaSymbolMissing;
        try self.launchLinearQ8_0RowsColsCommon(ctx, fn_cols_rows.function, dst, input, weight_raw, bias, residual, rows, in_dim, out_dim, .bias_add, fn_cols_rows.cols, fn_cols_rows.rows);
    }

    const Q8VariantFunction = struct {
        function: driver_mod.CUfunction,
        rows: usize,
        cols: usize,
    };

    const Q8Mode = enum {
        none,
        bias,
        bias_gelu,
        bias_add,
    };

    fn q8VariantFunction(self: *const KernelModule, variant: QMatmulVariant, mode: Q8Mode) ?Q8VariantFunction {
        return switch (variant) {
            .legacy => null,
            .tc_hmma => null,
            .fast_r2c4 => .{
                .function = switch (mode) {
                    .none => self.linear_q8_0_f32_tile4_r2 orelse return null,
                    .bias => self.linear_q8_0_bias_f32_tile4_r2 orelse return null,
                    .bias_gelu => self.linear_q8_0_bias_gelu_f32_tile4_r2 orelse return null,
                    .bias_add => self.linear_q8_0_bias_add_f32_tile4_r2 orelse return null,
                },
                .rows = 2,
                .cols = 4,
            },
            .fast_r2c8 => .{
                .function = switch (mode) {
                    .none => self.linear_q8_0_f32_fast_r2c8 orelse return null,
                    .bias => self.linear_q8_0_bias_f32_fast_r2c8 orelse return null,
                    .bias_gelu => self.linear_q8_0_bias_gelu_f32_fast_r2c8 orelse return null,
                    .bias_add => self.linear_q8_0_bias_add_f32_fast_r2c8 orelse return null,
                },
                .rows = 2,
                .cols = 8,
            },
            .fast_r4c4 => .{
                .function = switch (mode) {
                    .none => self.linear_q8_0_f32_fast_r4c4 orelse return null,
                    .bias => self.linear_q8_0_bias_f32_fast_r4c4 orelse return null,
                    .bias_gelu => self.linear_q8_0_bias_gelu_f32_fast_r4c4 orelse return null,
                    .bias_add => self.linear_q8_0_bias_add_f32_fast_r4c4 orelse return null,
                },
                .rows = 4,
                .cols = 4,
            },
        };
    }

    fn launchLinearQ8_0RowsColsCommon(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        function: driver_mod.CUfunction,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        mode: Q8Mode,
        rows_per_block: usize,
        cols_per_block: usize,
    ) driver_mod.Error!void {
        _ = self;
        if (in_dim == 0 or in_dim % q8_0_tiled_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q8_0_block_bytes));
        if (mode == .bias or mode == .bias_gelu or mode == .bias_add) try checkBytes(bias, out_dim);
        if (mode == .bias_add) try checkBytes(residual, out_count);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var bias_ptr = bias.ptr;
        var residual_ptr = residual.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = if (mode == .bias_add) [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        } else if (mode == .bias or mode == .bias_gelu) [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            null,
        } else [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            null,
            null,
        };
        try launch2d(function, ctx, (out_dim + cols_per_block - 1) / cols_per_block, (rows + rows_per_block - 1) / rows_per_block, q8_0_tiled_threads, &params);
    }

    pub fn launchLinearQ8_0TcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q8_0_f32_tc_hmma orelse return error.CudaSymbolMissing;
        try self.launchLinearQ8_0TcHmmaCommon(ctx, function, dst, input, weight_packed, .{}, .{}, rows, in_dim, out_dim, .none);
    }

    pub fn launchLinearQ8_0BiasTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q8_0_bias_f32_tc_hmma orelse return error.CudaSymbolMissing;
        try self.launchLinearQ8_0TcHmmaCommon(ctx, function, dst, input, weight_packed, bias, .{}, rows, in_dim, out_dim, .bias);
    }

    pub fn launchLinearQ8_0BiasGeluTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q8_0_bias_gelu_f32_tc_hmma orelse return error.CudaSymbolMissing;
        try self.launchLinearQ8_0TcHmmaCommon(ctx, function, dst, input, weight_packed, bias, .{}, rows, in_dim, out_dim, .bias_gelu);
    }

    pub fn launchLinearQ8_0BiasAddTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q8_0_bias_add_f32_tc_hmma orelse return error.CudaSymbolMissing;
        try self.launchLinearQ8_0TcHmmaCommon(ctx, function, dst, input, weight_packed, bias, residual, rows, in_dim, out_dim, .bias_add);
    }

    fn launchLinearQ8_0TcHmmaCommon(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        function: driver_mod.CUfunction,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        mode: Q8Mode,
    ) driver_mod.Error!void {
        _ = self;
        if (in_dim == 0 or in_dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_packed, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q8_0_tc_block_bytes));
        if (mode == .bias or mode == .bias_gelu or mode == .bias_add) try checkBytes(bias, out_dim);
        if (mode == .bias_add) try checkBytes(residual, out_count);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_packed.ptr;
        var bias_ptr = bias.ptr;
        var residual_ptr = residual.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = if (mode == .bias_add) [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        } else if (mode == .bias or mode == .bias_gelu) [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            null,
        } else [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            null,
            null,
        };
        try launch2d(function, ctx, (out_dim + q_tc_hmma_cols - 1) / q_tc_hmma_cols, (rows + q_tc_hmma_rows - 1) / q_tc_hmma_rows, q_tc_hmma_threads, &params);
    }

    pub fn launchLinearQ4_0F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch1d(self.linear_q4_0_f32, ctx, out_count, &params);
    }

    pub fn launchLinearQ4_0Tile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, rows, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0Tile4W4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_f32_tile4_w4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, rows, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchQuantizeF32Q8_1Rows(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q8_1: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
    ) driver_mod.Error!void {
        const function = self.quantize_f32_q8_1_rows orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q8_1_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_1_values_per_block;
        const total_blocks = try checkedTensorElements(rows, row_blocks);
        try checkRawBytes(dst_q8_1, try checkedTensorElements(total_blocks, q8_1_block_bytes));
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        if (total_blocks == 0) return;

        var dst_ptr = dst_q8_1.ptr;
        var input_ptr = input.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
        };
        try launchBlocks(function, ctx, (total_blocks + q8_1_quantize_warps - 1) / q8_1_quantize_warps, q8_1_quantize_threads, &params);
    }

    pub fn launchQuantizeGatedF32Q8_1Rows(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q8_1: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        up: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.quantize_gated_f32_q8_1_rows orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q8_1_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_1_values_per_block;
        const total_blocks = try checkedTensorElements(rows, row_blocks);
        const in_count = try checkedTensorElements(rows, in_dim);
        try checkRawBytes(dst_q8_1, try checkedTensorElements(total_blocks, q8_1_block_bytes));
        try checkBytes(gate, in_count);
        try checkBytes(up, in_count);
        if (total_blocks == 0) return;

        var dst_ptr = dst_q8_1.ptr;
        var gate_ptr = gate.ptr;
        var up_ptr = up.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&up_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launchBlocks(function, ctx, (total_blocks + q8_1_quantize_warps - 1) / q8_1_quantize_warps, q8_1_quantize_threads, &params);
    }

    pub fn launchLinearQ4_0Q8_1Tile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_ptr = weight_raw.ptr;
        if (rows == 1 and out_dim == 2560) {
            if (in_dim == 2048) {
                if (self.linear_q4_0_q8_1_f32_tile4_e4b_attn_2048) |function| {
                    var params = [_]?*anyopaque{
                        @ptrCast(&dst_ptr),
                        @ptrCast(&input_ptr),
                        @ptrCast(&weight_ptr),
                    };
                    try launchBlocks(function, ctx, out_dim / q4_0_col_tile, q4_0_tiled_threads_w4, &params);
                    return;
                }
            } else if (in_dim == 4096) {
                if (self.linear_q4_0_q8_1_f32_tile4_e4b_attn_4096) |function| {
                    var params = [_]?*anyopaque{
                        @ptrCast(&dst_ptr),
                        @ptrCast(&input_ptr),
                        @ptrCast(&weight_ptr),
                    };
                    try launchBlocks(function, ctx, out_dim / q4_0_col_tile, q4_0_tiled_threads_w4, &params);
                    return;
                }
            }
        }

        const function = self.linear_q4_0_q8_1_f32_tile4 orelse return error.CudaKernelUnavailable;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, rows, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0Q8_1Tile4W8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_ptr = weight_raw.ptr;
        if (rows > 1) {
            if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_ROWS8_C4", false)) {
                if (self.linear_q4_0_q8_1_f32_tile4_w8_rows8_c4) |function| {
                    var rows_u32 = try toU32(rows);
                    var in_dim_u32 = try toU32(in_dim);
                    var out_dim_u32 = try toU32(out_dim);
                    var params = [_]?*anyopaque{
                        @ptrCast(&dst_ptr),
                        @ptrCast(&input_ptr),
                        @ptrCast(&weight_ptr),
                        @ptrCast(&rows_u32),
                        @ptrCast(&in_dim_u32),
                        @ptrCast(&out_dim_u32),
                    };
                    try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, (rows + 7) / 8, q4_0_tiled_threads, &params);
                    return;
                }
            }
            if (self.linear_q4_0_q8_1_f32_tile4_w8_rows4) |function| {
                var rows_u32 = try toU32(rows);
                var in_dim_u32 = try toU32(in_dim);
                var out_dim_u32 = try toU32(out_dim);
                var params = [_]?*anyopaque{
                    @ptrCast(&dst_ptr),
                    @ptrCast(&input_ptr),
                    @ptrCast(&weight_ptr),
                    @ptrCast(&rows_u32),
                    @ptrCast(&in_dim_u32),
                    @ptrCast(&out_dim_u32),
                };
                try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, (rows + 3) / 4, q4_0_tiled_threads, &params);
                return;
            }
            if (self.linear_q4_0_q8_1_f32_tile4_w8_rows2) |function| {
                var rows_u32 = try toU32(rows);
                var in_dim_u32 = try toU32(in_dim);
                var out_dim_u32 = try toU32(out_dim);
                var params = [_]?*anyopaque{
                    @ptrCast(&dst_ptr),
                    @ptrCast(&input_ptr),
                    @ptrCast(&weight_ptr),
                    @ptrCast(&rows_u32),
                    @ptrCast(&in_dim_u32),
                    @ptrCast(&out_dim_u32),
                };
                try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, (rows + 1) / 2, q4_0_tiled_threads, &params);
                return;
            }
        }
        if (in_dim == 10240 and out_dim == 2560) {
            if (rows == 1 and platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_LINEAR_Q8_1_TILE4_W10_E4B_DOWN", false)) {
                if (self.linear_q4_0_q8_1_f32_tile4_w10_e4b_down) |function| {
                    var params = [_]?*anyopaque{
                        @ptrCast(&dst_ptr),
                        @ptrCast(&input_ptr),
                        @ptrCast(&weight_ptr),
                    };
                    try launchBlocks(function, ctx, out_dim / q4_0_col_tile, q4_0_tiled_threads_w10, &params);
                    return;
                }
            }
            if (rows > 1) {
                if (self.linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows) |function| {
                    var params = [_]?*anyopaque{
                        @ptrCast(&dst_ptr),
                        @ptrCast(&input_ptr),
                        @ptrCast(&weight_ptr),
                    };
                    try launch2d(function, ctx, out_dim / q4_0_col_tile, rows, q4_0_tiled_threads, &params);
                    return;
                }
            } else if (self.linear_q4_0_q8_1_f32_tile4_w8_e4b_down) |function| {
                var params = [_]?*anyopaque{
                    @ptrCast(&dst_ptr),
                    @ptrCast(&input_ptr),
                    @ptrCast(&weight_ptr),
                };
                try launchBlocks(function, ctx, out_dim / q4_0_col_tile, q4_0_tiled_threads, &params);
                return;
            }
        }

        const function = self.linear_q4_0_q8_1_f32_tile4_w8 orelse return error.CudaKernelUnavailable;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, rows, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0Q8_1Tile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_q8_1_f32_tile8 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8, rows, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0Tile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_f32_tile8 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8, rows, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0ActivationSliceLastDimTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        source: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        source_cols: usize,
        source_start: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_activation_slice_last_dim_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        if (source_start > source_cols or out_dim > source_cols - source_start) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        try checkBytes(source, try checkedTensorElements(rows, source_cols));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var source_ptr = source.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var source_cols_u32 = try toU32(source_cols);
        var source_start_u32 = try toU32(source_start);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&source_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&source_cols_u32),
            @ptrCast(&source_start_u32),
            @ptrCast(&activation_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, rows, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0ActivationSliceLastDimTile4W4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        source: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        source_cols: usize,
        source_start: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_activation_slice_last_dim_f32_tile4_w4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        if (source_start > source_cols or out_dim > source_cols - source_start) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        try checkBytes(source, try checkedTensorElements(rows, source_cols));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var source_ptr = source.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var source_cols_u32 = try toU32(source_cols);
        var source_start_u32 = try toU32(source_start);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&source_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&source_cols_u32),
            @ptrCast(&source_start_u32),
            @ptrCast(&activation_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, rows, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0ActivationSliceLastDimQ8_1Tile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        source: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        source_cols: usize,
        source_start: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        if (source_start > source_cols or out_dim > source_cols - source_start) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes));
        try checkBytes(source, try checkedTensorElements(rows, source_cols));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_ptr = weight_raw.ptr;
        var source_ptr = source.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var source_cols_u32 = try toU32(source_cols);
        var source_start_u32 = try toU32(source_start);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&source_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&source_cols_u32),
            @ptrCast(&source_start_u32),
            @ptrCast(&activation_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_0_col_tile - 1) / q4_0_col_tile, rows, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch1d(self.linear_q4_k_f32, ctx, out_count, &params);
    }

    pub fn launchLinearQ4KTiledF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(self.linear_q4_k_f32_tiled, ctx, out_count, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearQ4KTile4Common(ctx, self.linear_q4_k_f32_tile4, dst, input, weight_raw, .{}, .{}, rows, in_dim, out_dim, .none);
    }

    pub fn launchLinearQ6KTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q6_k_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q6_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q6_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_k_col_tile - 1) / q4_k_col_tile, rows, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KGatedDownTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        up: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_gated_down_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const in_count = try checkedTensorElements(rows, in_dim);
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(gate, in_count);
        try checkBytes(up, in_count);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var gate_ptr = gate.ptr;
        var up_ptr = up.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&up_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_k_col_tile - 1) / q4_k_col_tile, rows, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ6KGatedDownTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        gate: buffer_mod.DeviceBuffer,
        up: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q6_k_gated_down_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q6_k_values_per_block;
        const in_count = try checkedTensorElements(rows, in_dim);
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(gate, in_count);
        try checkBytes(up, in_count);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q6_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var gate_ptr = gate.ptr;
        var up_ptr = up.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&gate_ptr),
            @ptrCast(&up_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_k_col_tile - 1) / q4_k_col_tile, rows, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KTile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearQ4KTileNCommon(ctx, self.linear_q4_k_f32_tile8, dst, input, weight_raw, rows, in_dim, out_dim, 8);
    }

    pub fn launchLinearQ4KBiasF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(bias, out_dim);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var bias_ptr = bias.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch1d(self.linear_q4_k_bias_f32, ctx, out_count, &params);
    }

    pub fn launchLinearQ4KBiasTiledF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(bias, out_dim);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var bias_ptr = bias.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(self.linear_q4_k_bias_f32_tiled, ctx, out_count, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KBiasTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearQ4KTile4Common(ctx, self.linear_q4_k_bias_f32_tile4, dst, input, weight_raw, bias, .{}, rows, in_dim, out_dim, .bias);
    }

    pub fn launchLinearQ4KBiasTile4Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearQ4KTile4Rows2Common(ctx, self.linear_q4_k_bias_f32_tile4_r2, dst, input, weight_raw, bias, rows, in_dim, out_dim);
    }

    pub fn launchLinearQ4KBiasGeluTile4Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_bias_gelu_f32_tile4_r2 orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KTile4Rows2Common(ctx, function, dst, input, weight_raw, bias, rows, in_dim, out_dim);
    }

    pub fn launchLinearQ4KBiasAddTile4Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_bias_add_f32_tile4_r2 orelse return error.CudaSymbolMissing;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        try checkBytes(bias, out_dim);
        try checkBytes(residual, out_count);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var bias_ptr = bias.ptr;
        var residual_ptr = residual.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_k_col_tile - 1) / q4_k_col_tile, (rows + q4_k_row_tile - 1) / q4_k_row_tile, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KBiasVariantF32(
        self: *KernelModule,
        variant: QMatmulVariant,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const fn_cols_rows = self.q4VariantFunction(variant, .bias) orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KRowsColsCommon(ctx, fn_cols_rows.function, dst, input, weight_raw, bias, .{}, rows, in_dim, out_dim, .bias, fn_cols_rows.rows, fn_cols_rows.cols);
    }

    pub fn launchLinearQ4KBiasGeluVariantF32(
        self: *KernelModule,
        variant: QMatmulVariant,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const fn_cols_rows = self.q4VariantFunction(variant, .bias_gelu) orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KRowsColsCommon(ctx, fn_cols_rows.function, dst, input, weight_raw, bias, .{}, rows, in_dim, out_dim, .bias_gelu, fn_cols_rows.rows, fn_cols_rows.cols);
    }

    pub fn launchLinearQ4KBiasAddVariantF32(
        self: *KernelModule,
        variant: QMatmulVariant,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const fn_cols_rows = self.q4VariantFunction(variant, .bias_add) orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KRowsColsCommon(ctx, fn_cols_rows.function, dst, input, weight_raw, bias, residual, rows, in_dim, out_dim, .bias_add, fn_cols_rows.rows, fn_cols_rows.cols);
    }

    const Q4VariantFunction = struct {
        function: driver_mod.CUfunction,
        rows: usize,
        cols: usize,
    };

    const Q4Mode = enum {
        bias,
        bias_gelu,
        bias_add,
    };

    fn q4VariantFunction(self: *const KernelModule, variant: QMatmulVariant, mode: Q4Mode) ?Q4VariantFunction {
        return switch (variant) {
            .legacy => null,
            .tc_hmma => null,
            .fast_r2c4 => .{
                .function = switch (mode) {
                    .bias => self.linear_q4_k_bias_f32_tile4_r2 orelse return null,
                    .bias_gelu => self.linear_q4_k_bias_gelu_f32_tile4_r2 orelse return null,
                    .bias_add => self.linear_q4_k_bias_add_f32_tile4_r2 orelse return null,
                },
                .rows = 2,
                .cols = 4,
            },
            .fast_r2c8 => .{
                .function = switch (mode) {
                    .bias => self.linear_q4_k_bias_f32_fast_r2c8 orelse return null,
                    .bias_gelu => self.linear_q4_k_bias_gelu_f32_fast_r2c8 orelse return null,
                    .bias_add => self.linear_q4_k_bias_add_f32_fast_r2c8 orelse return null,
                },
                .rows = 2,
                .cols = 8,
            },
            .fast_r4c4 => .{
                .function = switch (mode) {
                    .bias => self.linear_q4_k_bias_f32_fast_r4c4 orelse return null,
                    .bias_gelu => self.linear_q4_k_bias_gelu_f32_fast_r4c4 orelse return null,
                    .bias_add => self.linear_q4_k_bias_add_f32_fast_r4c4 orelse return null,
                },
                .rows = 4,
                .cols = 4,
            },
        };
    }

    fn launchLinearQ4KRowsColsCommon(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        function: driver_mod.CUfunction,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        mode: Q4Mode,
        rows_per_block: usize,
        cols_per_block: usize,
    ) driver_mod.Error!void {
        _ = self;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(bias, out_dim);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (mode == .bias_add) try checkBytes(residual, out_count);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var bias_ptr = bias.ptr;
        var residual_ptr = residual.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = if (mode == .bias_add) [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        } else [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            null,
        };
        try launch2d(function, ctx, (out_dim + cols_per_block - 1) / cols_per_block, (rows + rows_per_block - 1) / rows_per_block, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_f32_tc_hmma orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KTcHmmaCommon(ctx, function, dst, input, weight_packed, .{}, .{}, rows, in_dim, out_dim, .none);
    }

    pub fn launchLinearQ4KBiasTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_bias_f32_tc_hmma orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KTcHmmaCommon(ctx, function, dst, input, weight_packed, bias, .{}, rows, in_dim, out_dim, .bias);
    }

    pub fn launchLinearQ4KBiasGeluTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_bias_gelu_f32_tc_hmma orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KTcHmmaCommon(ctx, function, dst, input, weight_packed, bias, .{}, rows, in_dim, out_dim, .bias_gelu);
    }

    pub fn launchLinearQ4KBiasAddTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_bias_add_f32_tc_hmma orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KTcHmmaCommon(ctx, function, dst, input, weight_packed, bias, residual, rows, in_dim, out_dim, .bias_add);
    }

    pub fn launchLinearQ4KBiasQuickGeluTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_bias_quick_gelu_f32_tc_hmma orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KTcHmmaCommon(ctx, function, dst, input, weight_packed, bias, .{}, rows, in_dim, out_dim, .bias_quick_gelu);
    }

    pub fn launchLinearQ4KBiasReluTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_bias_relu_f32_tc_hmma orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KTcHmmaCommon(ctx, function, dst, input, weight_packed, bias, .{}, rows, in_dim, out_dim, .bias_relu);
    }

    const Q4TcMode = enum {
        none,
        bias,
        bias_gelu,
        bias_add,
        bias_quick_gelu,
        bias_relu,

        fn hasBias(self: Q4TcMode) bool {
            return switch (self) {
                .bias, .bias_gelu, .bias_add, .bias_quick_gelu, .bias_relu => true,
                .none => false,
            };
        }
    };

    fn launchLinearQ4KTcHmmaCommon(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        function: driver_mod.CUfunction,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_packed: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        mode: Q4TcMode,
    ) driver_mod.Error!void {
        _ = self;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_packed, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_tc_block_bytes));
        if (mode.hasBias()) try checkBytes(bias, out_dim);
        if (mode == .bias_add) try checkBytes(residual, out_count);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_packed.ptr;
        var bias_ptr = bias.ptr;
        var residual_ptr = residual.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = if (mode == .bias_add) [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        } else if (mode.hasBias()) [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            null,
        } else [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            null,
            null,
        };
        try launch2d(function, ctx, (out_dim + q_tc_hmma_cols - 1) / q_tc_hmma_cols, (rows + q_tc_hmma_rows - 1) / q_tc_hmma_rows, q_tc_hmma_threads, &params);
    }

    pub fn launchLinearQ4KTripleBiasTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        dst_c: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a_packed: buffer_mod.DeviceBuffer,
        bias_a: buffer_mod.DeviceBuffer,
        weight_b_packed: buffer_mod.DeviceBuffer,
        bias_b: buffer_mod.DeviceBuffer,
        weight_c_packed: buffer_mod.DeviceBuffer,
        bias_c: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_triple_bias_f32_tc_hmma orelse return error.CudaSymbolMissing;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_tc_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(dst_c, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a_packed, weight_bytes);
        try checkRawBytes(weight_b_packed, weight_bytes);
        try checkRawBytes(weight_c_packed, weight_bytes);
        try checkBytes(bias_a, out_dim);
        try checkBytes(bias_b, out_dim);
        try checkBytes(bias_c, out_dim);
        if (out_count == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var dst_c_ptr = dst_c.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a_packed.ptr;
        var bias_a_ptr = bias_a.ptr;
        var weight_b_ptr = weight_b_packed.ptr;
        var bias_b_ptr = bias_b.ptr;
        var weight_c_ptr = weight_c_packed.ptr;
        var bias_c_ptr = bias_c.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&dst_c_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&bias_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&bias_b_ptr),
            @ptrCast(&weight_c_ptr),
            @ptrCast(&bias_c_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch3d(
            function,
            ctx,
            (out_dim + q_tc_hmma_cols - 1) / q_tc_hmma_cols,
            (rows + q_tc_hmma_rows - 1) / q_tc_hmma_rows,
            3,
            q_tc_hmma_threads,
            &params,
        );
    }

    pub fn launchLinearQ4KPairBiasTcHmmaF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a_packed: buffer_mod.DeviceBuffer,
        bias_a: buffer_mod.DeviceBuffer,
        weight_b_packed: buffer_mod.DeviceBuffer,
        bias_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_pair_bias_f32_tc_hmma orelse return error.CudaSymbolMissing;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_tc_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a_packed, weight_bytes);
        try checkRawBytes(weight_b_packed, weight_bytes);
        try checkBytes(bias_a, out_dim);
        try checkBytes(bias_b, out_dim);
        if (out_count == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a_packed.ptr;
        var bias_a_ptr = bias_a.ptr;
        var weight_b_ptr = weight_b_packed.ptr;
        var bias_b_ptr = bias_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&bias_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&bias_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch3d(
            function,
            ctx,
            (out_dim + q_tc_hmma_cols - 1) / q_tc_hmma_cols,
            (rows + q_tc_hmma_rows - 1) / q_tc_hmma_rows,
            2,
            q_tc_hmma_threads,
            &params,
        );
    }

    pub fn launchLinearQ4KBiasQuickGeluTiledF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(bias, out_dim);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var bias_ptr = bias.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(self.linear_q4_k_bias_quick_gelu_f32_tiled, ctx, out_count, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KBiasQuickGeluTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearQ4KTile4Common(ctx, self.linear_q4_k_bias_quick_gelu_f32_tile4, dst, input, weight_raw, bias, .{}, rows, in_dim, out_dim, .bias);
    }

    pub fn launchLinearQ4KBiasReluTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearQ4KTile4Common(ctx, self.linear_q4_k_bias_relu_f32_tile4, dst, input, weight_raw, bias, .{}, rows, in_dim, out_dim, .bias);
    }

    pub fn launchLinearQ4KBiasReluTile4Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearQ4KTile4Rows2Common(ctx, self.linear_q4_k_bias_relu_f32_tile4_r2, dst, input, weight_raw, bias, rows, in_dim, out_dim);
    }

    pub fn launchLinearQ4KBiasAddTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        try self.launchLinearQ4KTile4Common(ctx, self.linear_q4_k_bias_add_f32_tile4, dst, input, weight_raw, bias, residual, rows, in_dim, out_dim, .bias_residual);
    }

    const Tile4Mode = enum { none, bias, bias_residual };

    fn launchLinearQ4KTile4Common(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        function: driver_mod.CUfunction,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        residual: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        mode: Tile4Mode,
    ) driver_mod.Error!void {
        _ = self;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (mode == .bias or mode == .bias_residual) try checkBytes(bias, out_dim);
        if (mode == .bias_residual) try checkBytes(residual, out_count);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var bias_ptr = bias.ptr;
        var residual_ptr = residual.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = if (mode == .bias_residual) [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&residual_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        } else if (mode == .bias) [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            null,
        } else [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            null,
            null,
        };
        try launch2d(function, ctx, (out_dim + q4_k_col_tile - 1) / q4_k_col_tile, rows, q4_k_tiled_threads, &params);
    }

    fn launchLinearQ4KTileNCommon(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        function: driver_mod.CUfunction,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        col_tile: usize,
    ) driver_mod.Error!void {
        _ = self;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        if (col_tile == 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + col_tile - 1) / col_tile, rows, q4_k_tiled_threads, &params);
    }

    fn launchLinearQ4KTile4Rows2Common(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        function: driver_mod.CUfunction,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        _ = self;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(bias, out_dim);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var bias_ptr = bias.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_k_col_tile - 1) / q4_k_col_tile, (rows + q4_k_row_tile - 1) / q4_k_row_tile, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KTripleBiasF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        dst_c: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        bias_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        bias_b: buffer_mod.DeviceBuffer,
        weight_c: buffer_mod.DeviceBuffer,
        bias_c: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(dst_c, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        try checkRawBytes(weight_c, weight_bytes);
        try checkBytes(bias_a, out_dim);
        try checkBytes(bias_b, out_dim);
        try checkBytes(bias_c, out_dim);
        if (out_count == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var dst_c_ptr = dst_c.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var bias_a_ptr = bias_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var bias_b_ptr = bias_b.ptr;
        var weight_c_ptr = weight_c.ptr;
        var bias_c_ptr = bias_c.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&dst_c_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&bias_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&bias_b_ptr),
            @ptrCast(&weight_c_ptr),
            @ptrCast(&bias_c_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch1d(self.linear_q4_k_triple_bias_f32, ctx, try checkedTensorElements(out_count, 3), &params);
    }

    pub fn launchLinearQ4KTripleBiasTiledF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        dst_c: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        bias_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        bias_b: buffer_mod.DeviceBuffer,
        weight_c: buffer_mod.DeviceBuffer,
        bias_c: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(dst_c, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        try checkRawBytes(weight_c, weight_bytes);
        try checkBytes(bias_a, out_dim);
        try checkBytes(bias_b, out_dim);
        try checkBytes(bias_c, out_dim);
        if (out_count == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var dst_c_ptr = dst_c.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var bias_a_ptr = bias_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var bias_b_ptr = bias_b.ptr;
        var weight_c_ptr = weight_c.ptr;
        var bias_c_ptr = bias_c.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&dst_c_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&bias_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&bias_b_ptr),
            @ptrCast(&weight_c_ptr),
            @ptrCast(&bias_c_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(self.linear_q4_k_triple_bias_f32_tiled, ctx, try checkedTensorElements(out_count, 3), q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KPairBiasTiledF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        bias_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        bias_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        try checkBytes(bias_a, out_dim);
        try checkBytes(bias_b, out_dim);
        if (out_count == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var bias_a_ptr = bias_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var bias_b_ptr = bias_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&bias_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&bias_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(self.linear_q4_k_pair_bias_f32_tiled, ctx, try checkedTensorElements(out_count, 2), q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ8_0PairNoBiasTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q8_0_pair_nobias_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q8_0_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        const col_tiles = (out_dim + q8_0_col_tile - 1) / q8_0_col_tile;
        const tiles_per_row = try checkedTensorElements(col_tiles, 2);
        const total_tiles = try checkedTensorElements(rows, tiles_per_row);
        if (total_tiles == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q8_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0PairNoBiasTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_pair_nobias_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        const col_tiles = (out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const tiles_per_row = try checkedTensorElements(col_tiles, 2);
        const total_tiles = try checkedTensorElements(rows, tiles_per_row);
        if (total_tiles == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0PairNoBiasTile4W4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_pair_nobias_f32_tile4_w4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        const col_tiles = (out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const tiles_per_row = try checkedTensorElements(col_tiles, 2);
        const total_tiles = try checkedTensorElements(rows, tiles_per_row);
        if (total_tiles == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0PairNoBiasQ8_1Tile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_pair_nobias_q8_1_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        const col_tiles = (out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const tiles_per_row = try checkedTensorElements(col_tiles, 2);
        const total_tiles = try checkedTensorElements(rows, tiles_per_row);
        if (total_tiles == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_a_ptr = weight_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0PairNoBiasQ8_1Tile4W8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_pair_nobias_q8_1_f32_tile4_w8 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        const col_tiles = (out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const tiles_per_row = try checkedTensorElements(col_tiles, 2);
        const total_tiles = try checkedTensorElements(rows, tiles_per_row);
        if (total_tiles == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_a_ptr = weight_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0PairNoBiasQ8_1Tile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_pair_nobias_q8_1_f32_tile8 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        const col_tiles = (out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8;
        const tiles_per_row = try checkedTensorElements(col_tiles, 2);
        const total_tiles = try checkedTensorElements(rows, tiles_per_row);
        if (total_tiles == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_a_ptr = weight_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0PairActivationTile4W4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_gate: buffer_mod.DeviceBuffer,
        weight_up: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_pair_activation_f32_tile4_w4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_gate, weight_bytes);
        try checkRawBytes(weight_up, weight_bytes);
        const col_tiles = (out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const tiles_per_row = try checkedTensorElements(col_tiles, 2);
        const total_tiles = try checkedTensorElements(rows, tiles_per_row);
        if (total_tiles == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_gate_ptr = weight_gate.ptr;
        var weight_up_ptr = weight_up.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_gate_ptr),
            @ptrCast(&weight_up_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0PairActivationQ8_1Tile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_gate: buffer_mod.DeviceBuffer,
        weight_up: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_pair_activation_q8_1_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst, out_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_gate, weight_bytes);
        try checkRawBytes(weight_up, weight_bytes);
        const col_tiles = (out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const total_tiles = try checkedTensorElements(rows, col_tiles);
        if (total_tiles == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_gate_ptr = weight_gate.ptr;
        var weight_up_ptr = weight_up.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_gate_ptr),
            @ptrCast(&weight_up_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0PairActivationQ8_1Tile4W8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_gate: buffer_mod.DeviceBuffer,
        weight_up: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst, out_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_gate, weight_bytes);
        try checkRawBytes(weight_up, weight_bytes);
        const col_tiles = (out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const total_tiles = try checkedTensorElements(rows, col_tiles);
        if (total_tiles == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_gate_ptr = weight_gate.ptr;
        var weight_up_ptr = weight_up.ptr;
        var activation_u32: u32 = activation;
        if (rows == 1 and in_dim == 2560 and out_dim == 10240) {
            if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_TILE4_W5_E4B_FFN", true)) {
                if (self.linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn) |function| {
                    var params = [_]?*anyopaque{
                        @ptrCast(&dst_ptr),
                        @ptrCast(&input_ptr),
                        @ptrCast(&weight_gate_ptr),
                        @ptrCast(&weight_up_ptr),
                        @ptrCast(&activation_u32),
                    };
                    try launchBlocks(function, ctx, out_dim / q4_0_col_tile, q4_0_tiled_threads_w5, &params);
                    return;
                }
            }
            if (self.linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn) |function| {
                var params = [_]?*anyopaque{
                    @ptrCast(&dst_ptr),
                    @ptrCast(&input_ptr),
                    @ptrCast(&weight_gate_ptr),
                    @ptrCast(&weight_up_ptr),
                    @ptrCast(&activation_u32),
                };
                try launchBlocks(function, ctx, out_dim / q4_0_col_tile, q4_0_tiled_threads, &params);
                return;
            }
        }

        const function = self.linear_q4_0_pair_activation_q8_1_f32_tile4_w8 orelse return error.CudaKernelUnavailable;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_gate_ptr),
            @ptrCast(&weight_up_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads, &params);
    }

    // E4B FFN-specific: the CUDA body bakes in 2560 input columns and
    // 10240 activated columns to keep row-aware Q8_1 prefill fast.
    pub fn launchLinearQ4_0PairActivationQ8_1Tile32W5E4BFfnQ8_1(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q8_1: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_gate: buffer_mod.DeviceBuffer,
        weight_up: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        if (rows == 0 or in_dim != 2560 or out_dim != 10240) return error.InvalidCudaState;
        const input_row_blocks = in_dim / q8_1_values_per_block;
        const out_row_blocks = out_dim / q8_1_values_per_block;
        const total_out_blocks = try checkedTensorElements(rows, out_row_blocks);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, input_row_blocks), q4_0_block_bytes);
        try checkRawBytes(dst_q8_1, try checkedTensorElements(total_out_blocks, q8_1_block_bytes));
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, input_row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_gate, weight_bytes);
        try checkRawBytes(weight_up, weight_bytes);

        var dst_ptr = dst_q8_1.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_gate_ptr = weight_gate.ptr;
        var weight_up_ptr = weight_up.ptr;
        var activation_u32: u32 = activation;
        if (rows > 1) {
            if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS16_C1", false)) {
                if (self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows16_c1) |function| {
                    var rows_u32 = try toU32(rows);
                    var params = [_]?*anyopaque{
                        @ptrCast(&dst_ptr),
                        @ptrCast(&input_ptr),
                        @ptrCast(&weight_gate_ptr),
                        @ptrCast(&weight_up_ptr),
                        @ptrCast(&rows_u32),
                        @ptrCast(&activation_u32),
                    };
                    try launchBlocks(function, ctx, try checkedTensorElements((rows + 15) / 16, out_row_blocks), q4_0_pair_activation_q8_1_tile32_w5_rows16_threads, &params);
                    return;
                }
            }
            if (platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q4_0_PAIR_ACTIVATION_Q8_1_ROWS8_C2", false)) {
                if (self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows8_c2) |function| {
                    var rows_u32 = try toU32(rows);
                    var params = [_]?*anyopaque{
                        @ptrCast(&dst_ptr),
                        @ptrCast(&input_ptr),
                        @ptrCast(&weight_gate_ptr),
                        @ptrCast(&weight_up_ptr),
                        @ptrCast(&rows_u32),
                        @ptrCast(&activation_u32),
                    };
                    try launchBlocks(function, ctx, try checkedTensorElements((rows + 7) / 8, out_row_blocks), q4_0_pair_activation_q8_1_tile32_w5_threads, &params);
                    return;
                }
            }
            if (self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows4) |function| {
                var rows_u32 = try toU32(rows);
                var params = [_]?*anyopaque{
                    @ptrCast(&dst_ptr),
                    @ptrCast(&input_ptr),
                    @ptrCast(&weight_gate_ptr),
                    @ptrCast(&weight_up_ptr),
                    @ptrCast(&rows_u32),
                    @ptrCast(&activation_u32),
                };
                try launchBlocks(function, ctx, try checkedTensorElements((rows + 3) / 4, out_row_blocks), q4_0_pair_activation_q8_1_tile32_w5_threads, &params);
                return;
            }
            if (self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn_rows2) |function| {
                var rows_u32 = try toU32(rows);
                var params = [_]?*anyopaque{
                    @ptrCast(&dst_ptr),
                    @ptrCast(&input_ptr),
                    @ptrCast(&weight_gate_ptr),
                    @ptrCast(&weight_up_ptr),
                    @ptrCast(&rows_u32),
                    @ptrCast(&activation_u32),
                };
                try launchBlocks(function, ctx, try checkedTensorElements((rows + 1) / 2, out_row_blocks), q4_0_pair_activation_q8_1_tile32_w5_threads, &params);
                return;
            }
        }
        const function = self.linear_q4_0_pair_activation_q8_1_q8_1_tile32_w5_e4b_ffn orelse return error.CudaKernelUnavailable;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_gate_ptr),
            @ptrCast(&weight_up_ptr),
            @ptrCast(&activation_u32),
        };
        try launchBlocks(function, ctx, total_out_blocks, q4_0_pair_activation_q8_1_tile32_w5_threads, &params);
    }

    pub fn launchLinearQ4_0PairActivationQ8_1Tile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_gate: buffer_mod.DeviceBuffer,
        weight_up: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        activation: u8,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_pair_activation_q8_1_f32_tile8 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst, out_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_gate, weight_bytes);
        try checkRawBytes(weight_up, weight_bytes);
        const col_tiles = (out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8;
        const total_tiles = try checkedTensorElements(rows, col_tiles);
        if (total_tiles == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_gate_ptr = weight_gate.ptr;
        var weight_up_ptr = weight_up.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var activation_u32: u32 = activation;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_gate_ptr),
            @ptrCast(&weight_up_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
            @ptrCast(&activation_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0PairNoBiasTile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_pair_nobias_f32_tile8 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        const col_tiles = (out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8;
        const tiles_per_row = try checkedTensorElements(col_tiles, 2);
        const total_tiles = try checkedTensorElements(rows, tiles_per_row);
        if (total_tiles == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4KPairNoBiasTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_pair_nobias_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        const col_tiles = (out_dim + q4_k_col_tile - 1) / q4_k_col_tile;
        const tiles_per_row = try checkedTensorElements(col_tiles, 2);
        const total_tiles = try checkedTensorElements(rows, tiles_per_row);
        if (total_tiles == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KQkvNoBiasTiledF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_qkv_nobias_f32_tiled orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        const q_weight_bytes = try checkedTensorElements(try checkedTensorElements(q_out_dim, row_blocks), q4_k_block_bytes);
        const kv_weight_bytes = try checkedTensorElements(try checkedTensorElements(kv_out_dim, row_blocks), q4_k_block_bytes);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_q, q_weight_bytes);
        try checkRawBytes(weight_k, kv_weight_bytes);
        try checkRawBytes(weight_v, kv_weight_bytes);
        const total = try checkedTensorElements(q_count + kv_count, 1);
        const total_with_v = try checkedTensorElements(total + kv_count, 1);
        if (total_with_v == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_with_v, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearF32QkvNoBiasTiled(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_f32_qkv_nobias_tiled orelse return error.CudaKernelUnavailable;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(weight_q, try checkedTensorElements(q_out_dim, in_dim));
        try checkBytes(weight_k, try checkedTensorElements(kv_out_dim, in_dim));
        try checkBytes(weight_v, try checkedTensorElements(kv_out_dim, in_dim));
        const total = try checkedTensorElements(q_count + kv_count, 1);
        const total_with_v = try checkedTensorElements(total + kv_count, 1);
        if (total_with_v == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_with_v, f32_tiled_threads, &params);
    }

    pub fn launchLinearQ8_0QkvNoBiasTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q8_0_qkv_nobias_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q8_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q8_0_values_per_block;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        const q_weight_bytes = try checkedTensorElements(try checkedTensorElements(q_out_dim, row_blocks), q8_0_block_bytes);
        const kv_weight_bytes = try checkedTensorElements(try checkedTensorElements(kv_out_dim, row_blocks), q8_0_block_bytes);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_q, q_weight_bytes);
        try checkRawBytes(weight_k, kv_weight_bytes);
        try checkRawBytes(weight_v, kv_weight_bytes);
        const q_tiles = (q_out_dim + q8_0_col_tile - 1) / q8_0_col_tile;
        const kv_tiles = (kv_out_dim + q8_0_col_tile - 1) / q8_0_col_tile;
        const tiles_per_row = try checkedTensorElements(q_tiles + kv_tiles, 1);
        const tiles_with_v = try checkedTensorElements(tiles_per_row + kv_tiles, 1);
        const total_tiles = try checkedTensorElements(rows, tiles_with_v);
        if (total_tiles == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q8_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0QkvNoBiasTile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_qkv_nobias_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        const q_weight_bytes = try checkedTensorElements(try checkedTensorElements(q_out_dim, row_blocks), q4_0_block_bytes);
        const kv_weight_bytes = try checkedTensorElements(try checkedTensorElements(kv_out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_q, q_weight_bytes);
        try checkRawBytes(weight_k, kv_weight_bytes);
        try checkRawBytes(weight_v, kv_weight_bytes);
        const q_tiles = (q_out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const kv_tiles = (kv_out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const tiles_per_row = try checkedTensorElements(q_tiles + kv_tiles, 1);
        const tiles_with_v = try checkedTensorElements(tiles_per_row + kv_tiles, 1);
        const total_tiles = try checkedTensorElements(rows, tiles_with_v);
        if (total_tiles == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0QkvNoBiasTile4W4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_qkv_nobias_f32_tile4_w4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        const q_weight_bytes = try checkedTensorElements(try checkedTensorElements(q_out_dim, row_blocks), q4_0_block_bytes);
        const kv_weight_bytes = try checkedTensorElements(try checkedTensorElements(kv_out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_q, q_weight_bytes);
        try checkRawBytes(weight_k, kv_weight_bytes);
        try checkRawBytes(weight_v, kv_weight_bytes);
        const q_tiles = (q_out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const kv_tiles = (kv_out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const tiles_per_row = try checkedTensorElements(q_tiles + kv_tiles, 1);
        const tiles_with_v = try checkedTensorElements(tiles_per_row + kv_tiles, 1);
        const total_tiles = try checkedTensorElements(rows, tiles_with_v);
        if (total_tiles == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0QkvNoBiasQ8_1Tile4F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_qkv_nobias_q8_1_f32_tile4 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        const q_weight_bytes = try checkedTensorElements(try checkedTensorElements(q_out_dim, row_blocks), q4_0_block_bytes);
        const kv_weight_bytes = try checkedTensorElements(try checkedTensorElements(kv_out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_q, q_weight_bytes);
        try checkRawBytes(weight_k, kv_weight_bytes);
        try checkRawBytes(weight_v, kv_weight_bytes);
        const q_tiles = (q_out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const kv_tiles = (kv_out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
        const tiles_per_row = try checkedTensorElements(q_tiles + kv_tiles, 1);
        const tiles_with_v = try checkedTensorElements(tiles_per_row + kv_tiles, 1);
        const total_tiles = try checkedTensorElements(rows, tiles_with_v);
        if (total_tiles == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0QkvNoBiasQ8_1Tile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        const q_weight_bytes = try checkedTensorElements(try checkedTensorElements(q_out_dim, row_blocks), q4_0_block_bytes);
        const kv_weight_bytes = try checkedTensorElements(try checkedTensorElements(kv_out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_q, q_weight_bytes);
        try checkRawBytes(weight_k, kv_weight_bytes);
        try checkRawBytes(weight_v, kv_weight_bytes);
        const q_tiles = (q_out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8;
        const kv_tiles = (kv_out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8;
        const tiles_per_row = try checkedTensorElements(q_tiles + kv_tiles, 1);
        const tiles_with_v = try checkedTensorElements(tiles_per_row + kv_tiles, 1);
        const total_tiles = try checkedTensorElements(rows, tiles_with_v);
        if (total_tiles == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        if (rows > 1) {
            if (self.linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4) |function| {
                var params = [_]?*anyopaque{
                    @ptrCast(&dst_q_ptr),
                    @ptrCast(&dst_k_ptr),
                    @ptrCast(&dst_v_ptr),
                    @ptrCast(&input_ptr),
                    @ptrCast(&weight_q_ptr),
                    @ptrCast(&weight_k_ptr),
                    @ptrCast(&weight_v_ptr),
                    @ptrCast(&rows_u32),
                    @ptrCast(&in_dim_u32),
                    @ptrCast(&q_out_dim_u32),
                    @ptrCast(&kv_out_dim_u32),
                };
                try launchBlocks(function, ctx, try checkedTensorElements((rows + 3) / 4, tiles_with_v), q4_0_tiled_threads_w4, &params);
                return;
            }
        }
        const function = self.linear_q4_0_qkv_nobias_q8_1_f32_tile8 orelse return error.CudaKernelUnavailable;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads_w4, &params);
    }

    pub fn launchLinearQ4_0QkvNoBiasQ8_1Tile8W8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input_q8_1: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0 or q4_0_values_per_block != q8_1_values_per_block) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        const q_weight_bytes = try checkedTensorElements(try checkedTensorElements(q_out_dim, row_blocks), q4_0_block_bytes);
        const kv_weight_bytes = try checkedTensorElements(try checkedTensorElements(kv_out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkRawBytes(input_q8_1, try checkedTensorElements(try checkedTensorElements(rows, row_blocks), q8_1_block_bytes));
        try checkRawBytes(weight_q, q_weight_bytes);
        try checkRawBytes(weight_k, kv_weight_bytes);
        try checkRawBytes(weight_v, kv_weight_bytes);
        const q_tiles = (q_out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8;
        const kv_tiles = (kv_out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8;
        const tiles_per_row = try checkedTensorElements(q_tiles + kv_tiles, 1);
        const tiles_with_v = try checkedTensorElements(tiles_per_row + kv_tiles, 1);
        const total_tiles = try checkedTensorElements(rows, tiles_with_v);
        if (total_tiles == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input_q8_1.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearQ4_0QkvNoBiasTile8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_0_qkv_nobias_f32_tile8 orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_0_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_0_values_per_block;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        const q_weight_bytes = try checkedTensorElements(try checkedTensorElements(q_out_dim, row_blocks), q4_0_block_bytes);
        const kv_weight_bytes = try checkedTensorElements(try checkedTensorElements(kv_out_dim, row_blocks), q4_0_block_bytes);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_q, q_weight_bytes);
        try checkRawBytes(weight_k, kv_weight_bytes);
        try checkRawBytes(weight_v, kv_weight_bytes);
        const q_tiles = (q_out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8;
        const kv_tiles = (kv_out_dim + q4_0_col_tile8 - 1) / q4_0_col_tile8;
        const tiles_per_row = try checkedTensorElements(q_tiles + kv_tiles, 1);
        const tiles_with_v = try checkedTensorElements(tiles_per_row + kv_tiles, 1);
        const total_tiles = try checkedTensorElements(rows, tiles_with_v);
        if (total_tiles == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_tiles, q4_0_tiled_threads, &params);
    }

    pub fn launchLinearBf16WeightF32QkvNoBiasTiled(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_bf16_weight_f32_qkv_nobias_tiled orelse return error.CudaKernelUnavailable;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_q, try checkedTensorElements(try checkedTensorElements(q_out_dim, in_dim), @sizeOf(u16)));
        try checkRawBytes(weight_k, try checkedTensorElements(try checkedTensorElements(kv_out_dim, in_dim), @sizeOf(u16)));
        try checkRawBytes(weight_v, try checkedTensorElements(try checkedTensorElements(kv_out_dim, in_dim), @sizeOf(u16)));
        const total = try checkedTensorElements(q_count + kv_count, 1);
        const total_with_v = try checkedTensorElements(total + kv_count, 1);
        if (total_with_v == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_with_v, f32_tiled_threads, &params);
    }

    pub fn launchLinearQ4KQ4KF32QkvNoBiasTiled(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_q: buffer_mod.DeviceBuffer,
        dst_k: buffer_mod.DeviceBuffer,
        dst_v: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_q: buffer_mod.DeviceBuffer,
        weight_k: buffer_mod.DeviceBuffer,
        weight_v: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        q_out_dim: usize,
        kv_out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_q4_k_f32_qkv_nobias_tiled orelse return error.CudaKernelUnavailable;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const q_count = try checkedTensorElements(rows, q_out_dim);
        const kv_count = try checkedTensorElements(rows, kv_out_dim);
        const q_weight_bytes = try checkedTensorElements(try checkedTensorElements(q_out_dim, row_blocks), q4_k_block_bytes);
        const kv_weight_bytes = try checkedTensorElements(try checkedTensorElements(kv_out_dim, row_blocks), q4_k_block_bytes);
        try checkBytes(dst_q, q_count);
        try checkBytes(dst_k, kv_count);
        try checkBytes(dst_v, kv_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_q, q_weight_bytes);
        try checkRawBytes(weight_k, kv_weight_bytes);
        try checkBytes(weight_v, try checkedTensorElements(kv_out_dim, in_dim));
        const total = try checkedTensorElements(q_count + kv_count, 1);
        const total_with_v = try checkedTensorElements(total + kv_count, 1);
        if (total_with_v == 0) return;

        var dst_q_ptr = dst_q.ptr;
        var dst_k_ptr = dst_k.ptr;
        var dst_v_ptr = dst_v.ptr;
        var input_ptr = input.ptr;
        var weight_q_ptr = weight_q.ptr;
        var weight_k_ptr = weight_k.ptr;
        var weight_v_ptr = weight_v.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var q_out_dim_u32 = try toU32(q_out_dim);
        var kv_out_dim_u32 = try toU32(kv_out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_q_ptr),
            @ptrCast(&dst_k_ptr),
            @ptrCast(&dst_v_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_q_ptr),
            @ptrCast(&weight_k_ptr),
            @ptrCast(&weight_v_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&q_out_dim_u32),
            @ptrCast(&kv_out_dim_u32),
        };
        try launchBlocks(function, ctx, total_with_v, q4_k_tiled_threads, &params);
    }
    pub fn launchLinearQ4KSpanBiasTile8Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_span_bias_f32_tile8_r2 orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KSpanRowsColsCommon(ctx, function, dst, input, weight_raw, bias, rows, in_dim, out_dim, 8, 2);
    }

    pub fn launchLinearQ4KSpanBiasReluTile8Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_span_bias_relu_f32_tile8_r2 orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KSpanRowsColsCommon(ctx, function, dst, input, weight_raw, bias, rows, in_dim, out_dim, 8, 2);
    }

    pub fn launchLinearQ4KSpanBiasTile4Rows8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_span_bias_f32_tile4_r8 orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KSpanRowsColsCommon(ctx, function, dst, input, weight_raw, bias, rows, in_dim, out_dim, 4, 8);
    }

    pub fn launchLinearQ4KSpanBiasReluTile4Rows8F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_span_bias_relu_f32_tile4_r8 orelse return error.CudaSymbolMissing;
        try self.launchLinearQ4KSpanRowsColsCommon(ctx, function, dst, input, weight_raw, bias, rows, in_dim, out_dim, 4, 8);
    }

    fn launchLinearQ4KSpanRowsColsCommon(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        function: driver_mod.CUfunction,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
        cols_per_block: usize,
        rows_per_block: usize,
    ) driver_mod.Error!void {
        _ = self;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(bias, out_dim);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes));
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var weight_ptr = weight_raw.ptr;
        var bias_ptr = bias.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + cols_per_block - 1) / cols_per_block, (rows + rows_per_block - 1) / rows_per_block, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KSpanPairBiasTile8Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        bias_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        bias_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_span_pair_bias_f32_tile8_r2 orelse return error.CudaSymbolMissing;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        try checkBytes(bias_a, out_dim);
        try checkBytes(bias_b, out_dim);
        if (out_count == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_ptr = input.ptr;
        var weight_a_ptr = weight_a.ptr;
        var bias_a_ptr = bias_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var bias_b_ptr = bias_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&bias_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&bias_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_k_span_col_tile - 1) / q4_k_span_col_tile, (rows + q4_k_span_row_tile - 1) / q4_k_span_row_tile, q4_k_tiled_threads, &params);
    }

    pub fn launchLinearQ4KSpanPairBiasReluTile8Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        bias_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        bias_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_span_pair_bias_relu_f32_tile8_r2 orelse return error.CudaSymbolMissing;
        try launchLinearQ4KSpanPairSharedInput(ctx, function, dst_a, dst_b, input, weight_a, bias_a, weight_b, bias_b, rows, in_dim, out_dim);
    }

    pub fn launchLinearQ4KSpanPair2BiasTile8Rows2F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst_a: buffer_mod.DeviceBuffer,
        dst_b: buffer_mod.DeviceBuffer,
        input_a: buffer_mod.DeviceBuffer,
        input_b: buffer_mod.DeviceBuffer,
        weight_a: buffer_mod.DeviceBuffer,
        bias_a: buffer_mod.DeviceBuffer,
        weight_b: buffer_mod.DeviceBuffer,
        bias_b: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        const function = self.linear_q4_k_span_pair2_bias_f32_tile8_r2 orelse return error.CudaSymbolMissing;
        if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q4_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        const input_count = try checkedTensorElements(rows, in_dim);
        const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes);
        try checkBytes(dst_a, out_count);
        try checkBytes(dst_b, out_count);
        try checkBytes(input_a, input_count);
        try checkBytes(input_b, input_count);
        try checkRawBytes(weight_a, weight_bytes);
        try checkRawBytes(weight_b, weight_bytes);
        try checkBytes(bias_a, out_dim);
        try checkBytes(bias_b, out_dim);
        if (out_count == 0) return;

        var dst_a_ptr = dst_a.ptr;
        var dst_b_ptr = dst_b.ptr;
        var input_a_ptr = input_a.ptr;
        var input_b_ptr = input_b.ptr;
        var weight_a_ptr = weight_a.ptr;
        var bias_a_ptr = bias_a.ptr;
        var weight_b_ptr = weight_b.ptr;
        var bias_b_ptr = bias_b.ptr;
        var rows_u32 = try toU32(rows);
        var in_dim_u32 = try toU32(in_dim);
        var out_dim_u32 = try toU32(out_dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_a_ptr),
            @ptrCast(&dst_b_ptr),
            @ptrCast(&input_a_ptr),
            @ptrCast(&input_b_ptr),
            @ptrCast(&weight_a_ptr),
            @ptrCast(&bias_a_ptr),
            @ptrCast(&weight_b_ptr),
            @ptrCast(&bias_b_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&in_dim_u32),
            @ptrCast(&out_dim_u32),
        };
        try launch2d(function, ctx, (out_dim + q4_k_span_col_tile - 1) / q4_k_span_col_tile, (rows + q4_k_span_row_tile - 1) / q4_k_span_row_tile, q4_k_tiled_threads, &params);
    }
};

fn launchLinearQ4KSpanPairSharedInput(
    ctx: *context_mod.CudaContext,
    function: driver_mod.CUfunction,
    dst_a: buffer_mod.DeviceBuffer,
    dst_b: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight_a: buffer_mod.DeviceBuffer,
    bias_a: buffer_mod.DeviceBuffer,
    weight_b: buffer_mod.DeviceBuffer,
    bias_b: buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) driver_mod.Error!void {
    if (in_dim == 0 or in_dim % q4_k_values_per_block != 0) return error.InvalidCudaState;
    const row_blocks = in_dim / q4_k_values_per_block;
    const out_count = try checkedTensorElements(rows, out_dim);
    const weight_bytes = try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q4_k_block_bytes);
    try checkBytes(dst_a, out_count);
    try checkBytes(dst_b, out_count);
    try checkBytes(input, try checkedTensorElements(rows, in_dim));
    try checkRawBytes(weight_a, weight_bytes);
    try checkRawBytes(weight_b, weight_bytes);
    try checkBytes(bias_a, out_dim);
    try checkBytes(bias_b, out_dim);
    if (out_count == 0) return;

    var dst_a_ptr = dst_a.ptr;
    var dst_b_ptr = dst_b.ptr;
    var input_ptr = input.ptr;
    var weight_a_ptr = weight_a.ptr;
    var bias_a_ptr = bias_a.ptr;
    var weight_b_ptr = weight_b.ptr;
    var bias_b_ptr = bias_b.ptr;
    var rows_u32 = try toU32(rows);
    var in_dim_u32 = try toU32(in_dim);
    var out_dim_u32 = try toU32(out_dim);
    var params = [_]?*anyopaque{
        @ptrCast(&dst_a_ptr),
        @ptrCast(&dst_b_ptr),
        @ptrCast(&input_ptr),
        @ptrCast(&weight_a_ptr),
        @ptrCast(&bias_a_ptr),
        @ptrCast(&weight_b_ptr),
        @ptrCast(&bias_b_ptr),
        @ptrCast(&rows_u32),
        @ptrCast(&in_dim_u32),
        @ptrCast(&out_dim_u32),
    };
    try launch2d(function, ctx, (out_dim + q4_k_span_col_tile - 1) / q4_k_span_col_tile, (rows + q4_k_span_row_tile - 1) / q4_k_span_row_tile, q4_k_tiled_threads, &params);
}

pub const ElementwiseOp = enum(u32) {
    add = 0,
    multiply = 1,
    silu = 2,
    gelu = 3,
    relu = 4,
    quick_gelu = 5,
    sigmoid = 6,
    tanh = 7,

    fn isUnary(self: ElementwiseOp) bool {
        return switch (self) {
            .add, .multiply => false,
            .silu, .gelu, .relu, .quick_gelu, .sigmoid, .tanh => true,
        };
    }
};

const q8_0_values_per_block: usize = 32;
const q8_0_block_bytes: usize = 34;

const q8_0_tc_block_bytes: usize = 34;
const q8_0_tiled_values_per_block: usize = 256;
const q8_0_tiled_threads: usize = 256;
const q8_0_col_tile: usize = 4;

const q8_0_row_tile: usize = 2;
const q8_1_values_per_block: usize = 32;
const q8_1_block_bytes: usize = 36;
const q8_1_quantize_threads: usize = 128;
const q8_1_quantize_warps: usize = q8_1_quantize_threads / 32;
const q4_0_values_per_block: usize = 32;
const q4_0_block_bytes: usize = 18;
const q4_0_tiled_threads: usize = 256;
const q4_0_tiled_threads_w4: usize = 128;
const q4_0_tiled_threads_w5: usize = 160;
const q4_0_tiled_threads_w10: usize = 320;
const q4_0_pair_activation_q8_1_tile32_w5_threads: usize = 640;
const q4_0_pair_activation_q8_1_tile32_w5_rows16_threads: usize = 512;
const q4_0_col_tile: usize = 4;
const q4_0_col_tile8: usize = 8;
const q4_0_col_tile16: usize = 16;
const q4_k_values_per_block: usize = 256;
const q4_k_block_bytes: usize = 144;
const q6_k_values_per_block: usize = 256;
const q6_k_block_bytes: usize = 210;
const q6_k_col_tile8: usize = 8;
const q6_k_col_tile16: usize = 16;
const q6_k_col_tile32: usize = 32;

const q4_k_tc_block_bytes: usize = 148;
const q4_k_tiled_threads: usize = 256;
const q4_k_tiled_threads_w4: usize = 128;
const q4_k_col_tile: usize = 4;
const q4_k_row_tile: usize = 2;

fn q6KQ8_1Tile8Stage1Threads(row_blocks: usize) usize {
    if (!platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_Q6_K_LM_HEAD_Q8_1_TILE8_EXACT_THREADS", false)) {
        return q4_k_tiled_threads;
    }
    const task_count = row_blocks * 16;
    if (task_count == 0 or task_count > q4_k_tiled_threads) return q4_k_tiled_threads;
    const rounded = ((task_count + 31) / 32) * 32;
    if (rounded == 0 or rounded > q4_k_tiled_threads) return q4_k_tiled_threads;
    return rounded;
}

const q4_k_span_col_tile: usize = 8;
const q4_k_span_row_tile: usize = 2;
const q4_k_span_gather_row_tile: usize = 8;
const f32_tiled_threads: usize = 256;
const f32_col_tile: usize = 4;
const f32_row_tile: usize = 2;

const q_tc_hmma_threads: usize = 256;
const q_tc_hmma_rows: usize = 64;
const q_tc_hmma_cols: usize = 32;

fn checkedTensorElements(a: usize, b: usize) driver_mod.Error!usize {
    return std.math.mul(usize, a, b) catch error.InvalidCudaState;
}

fn toU32(value: usize) driver_mod.Error!u32 {
    if (value > std.math.maxInt(u32)) return error.InvalidCudaState;
    return @intCast(value);
}

fn checkBytes(buffer: buffer_mod.DeviceBuffer, f32_count: usize) driver_mod.Error!void {
    const bytes = std.math.mul(usize, f32_count, @sizeOf(f32)) catch return error.InvalidCudaState;
    try checkRawBytes(buffer, bytes);
}

fn checkTypedTailWeightBytes(buffer: buffer_mod.DeviceBuffer, elem_count: usize, dtype: u32) driver_mod.Error!void {
    const elem_bytes: usize = switch (dtype) {
        0 => @sizeOf(f32),
        1, 2 => @sizeOf(u16),
        else => return error.InvalidCudaState,
    };
    const bytes = std.math.mul(usize, elem_count, elem_bytes) catch return error.InvalidCudaState;
    try checkRawBytes(buffer, bytes);
}

fn checkRawBytes(buffer: buffer_mod.DeviceBuffer, bytes: usize) driver_mod.Error!void {
    if (bytes > buffer.len) return error.InvalidCudaState;
}

fn launch1d(function: driver_mod.CUfunction, ctx: *context_mod.CudaContext, count: usize, params: [*]?*anyopaque) driver_mod.Error!void {
    const block: c_uint = 256;
    const grid: c_uint = try toU32((count + block - 1) / block);
    try launchRaw(function, ctx, grid, block, params);
}

fn launchBlocks(function: driver_mod.CUfunction, ctx: *context_mod.CudaContext, blocks: usize, threads: usize, params: [*]?*anyopaque) driver_mod.Error!void {
    const grid: c_uint = try toU32(blocks);
    const block: c_uint = try toU32(threads);
    try launchRaw(function, ctx, grid, block, params);
}

fn launchBlocksShared(function: driver_mod.CUfunction, ctx: *context_mod.CudaContext, blocks: usize, threads: usize, shared_bytes: usize, params: [*]?*anyopaque) driver_mod.Error!void {
    const grid: c_uint = try toU32(blocks);
    const block: c_uint = try toU32(threads);
    const shared: c_uint = try toU32(shared_bytes);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid,
        1,
        1,
        block,
        1,
        1,
        shared,
        ctx.stream,
        params,
        null,
    ));
    ctx.noteKernelLaunch();
}

fn launch2d(function: driver_mod.CUfunction, ctx: *context_mod.CudaContext, grid_x: usize, grid_y: usize, threads: usize, params: [*]?*anyopaque) driver_mod.Error!void {
    const gx: c_uint = try toU32(grid_x);
    const gy: c_uint = try toU32(grid_y);
    const block: c_uint = try toU32(threads);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        gx,
        gy,
        1,
        block,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
    ctx.noteKernelLaunch();
}

fn launch3d(function: driver_mod.CUfunction, ctx: *context_mod.CudaContext, grid_x: usize, grid_y: usize, grid_z: usize, threads: usize, params: [*]?*anyopaque) driver_mod.Error!void {
    const gx: c_uint = try toU32(grid_x);
    const gy: c_uint = try toU32(grid_y);
    const gz: c_uint = try toU32(grid_z);
    const block: c_uint = try toU32(threads);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        gx,
        gy,
        gz,
        block,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
    ctx.noteKernelLaunch();
}

fn launchRaw(function: driver_mod.CUfunction, ctx: *context_mod.CudaContext, grid: c_uint, block: c_uint, params: [*]?*anyopaque) driver_mod.Error!void {
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid,
        1,
        1,
        block,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
    ctx.noteKernelLaunch();
}

fn launchRows(function: driver_mod.CUfunction, ctx: *context_mod.CudaContext, rows: usize, params: [*]?*anyopaque) driver_mod.Error!void {
    const grid: c_uint = try toU32(rows);
    const block: c_uint = try toU32(f32_tiled_threads);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid,
        1,
        1,
        block,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
    ctx.noteKernelLaunch();
}

fn loadOptionalFunction(ctx: *context_mod.CudaContext, module: driver_mod.CUmodule, name: [*:0]const u8) driver_mod.CUfunction {
    var function: driver_mod.CUfunction = null;
    const result = ctx.driver.fns.cuModuleGetFunction(&function, module, name);
    if (result != driver_mod.CUDA_SUCCESS) return null;
    return function;
}

fn loadModuleWithJitLog(ctx: *context_mod.CudaContext, module: *driver_mod.CUmodule) driver_mod.Error!void {
    var info_log: [4096]u8 = .{0} ** 4096;
    var error_log: [4096]u8 = .{0} ** 4096;
    var options = [_]driver_mod.CUjit_option{
        driver_mod.CU_JIT_INFO_LOG_BUFFER,
        driver_mod.CU_JIT_INFO_LOG_BUFFER_SIZE_BYTES,
        driver_mod.CU_JIT_ERROR_LOG_BUFFER,
        driver_mod.CU_JIT_ERROR_LOG_BUFFER_SIZE_BYTES,
    };
    var values = [_]?*anyopaque{
        @ptrCast(info_log[0..].ptr),
        @ptrFromInt(info_log.len),
        @ptrCast(error_log[0..].ptr),
        @ptrFromInt(error_log.len),
    };
    const result = if (cuda_artifact.uses_jit)
        ctx.driver.fns.cuModuleLoadDataEx(module, cuda_artifact.image.ptr, options.len, &options, &values)
    else
        ctx.driver.fns.cuModuleLoadDataEx(module, cuda_artifact.image.ptr, 0, null, null);
    if (result == driver_mod.CUDA_SUCCESS) return;

    std.debug.print(
        "cuda {s}: module load failed: {s}: {s}\n",
        .{ cuda_artifact.format, ctx.driver.errorName(result), ctx.driver.errorString(result) },
    );
    const error_message = trimCudaLog(&error_log);
    if (error_message.len > 0) std.debug.print("cuda jit error log:\n{s}\n", .{error_message});
    const info_message = trimCudaLog(&info_log);
    if (info_message.len > 0) std.debug.print("cuda jit info log:\n{s}\n", .{info_message});
    return error.CudaDriverError;
}

fn trimCudaLog(buf: []const u8) []const u8 {
    const end = std.mem.indexOfScalar(u8, buf, 0) orelse buf.len;
    return std.mem.trim(u8, buf[0..end], " \t\r\n");
}

pub fn smokeFill(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    const count: usize = 16;
    var buf = try buffer_mod.DeviceBuffer.alloc(&ctx, count * @sizeOf(f32));
    defer buf.free(&ctx);
    try module.launchFillF32(&ctx, buf, count, 3.5);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, count);
    defer allocator.free(out);
    try buf.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    for (out) |v| {
        if (@abs(v - 3.5) > 0.00001) return error.CudaSmokeMismatch;
    }
}

pub fn smokeGraphCapture(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    const count: usize = 16;
    var buf = try buffer_mod.DeviceBuffer.alloc(&ctx, count * @sizeOf(f32));
    defer buf.free(&ctx);
    try module.launchFillF32(&ctx, buf, count, 0.0);
    try ctx.synchronize();

    try ctx.beginStreamCapture(driver_mod.CU_STREAM_CAPTURE_MODE_RELAXED);
    try module.launchFillF32(&ctx, buf, count, 4.25);
    const graph = try ctx.endStreamCapture();
    defer ctx.destroyGraph(graph);
    const exec = try ctx.instantiateGraph(graph);
    defer ctx.destroyGraphExec(exec);

    try ctx.launchGraph(exec);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, count);
    defer allocator.free(out);
    try buf.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    for (out) |v| {
        if (@abs(v - 4.25) > 0.00001) return error.CudaSmokeMismatch;
    }
}

pub fn smokeDenseF32(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    try smokeLinearF32(allocator, &ctx, &module);
    try smokeRmsNormF32(allocator, &ctx, &module);
    try smokeRmsNormBareF32(allocator, &ctx, &module);
    try smokeElementwiseF32(allocator, &ctx, &module);
    try smokeLayerNormF32(allocator, &ctx, &module);
    try smokeEmbeddingConcatConvF32(allocator, &ctx, &module);
    try smokeAttentionF32(allocator, &ctx, &module);
}

pub fn smokeGemma4Primitives(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    try module.requireGemma4DecoderPrimitives();
    try smokeBf16WeightPrimitives(allocator, &ctx, &module);
    try smokeAddMulScalarF32(allocator, &ctx, &module);
    try smokeRmsNormAddMulScalarF32(allocator, &ctx, &module);
    try smokeRmsNormAddF32(allocator, &ctx, &module);
    try smokeRopeF32(allocator, &ctx, &module);
    try smokeRmsNormHeadsRopeF32(allocator, &ctx, &module);
    try smokeRopePerItemF32(allocator, &ctx, &module);
    try smokeGqaAttentionF32(allocator, &ctx, &module);
    try smokeArgmaxLastRowSuppressF32(&ctx, &module);
    try smokeGemma4MtpMaskedArgmaxF32(&ctx, &module);
    try smokeGemma4MtpVerifyCommitU32(&ctx, &module);
}

pub fn smokeFlorence2Primitives(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    try module.requireFlorence2Primitives();
    try smokeFlorence2CrossAttentionF32(allocator, &ctx, &module);
    try smokeFlorence2LayoutF32(allocator, &ctx, &module);
    try smokeFlorence2WindowsF32(allocator, &ctx, &module);
    try smokeFlorence2ChannelAttentionF32(allocator, &ctx, &module);
    try smokeFlorence2VisionTailSourcesF32(allocator, &ctx, &module);
    try smokeFlorence2TripleQ4KTcHmmaF32(allocator, &ctx, &module);
}

pub fn smokeTurboquantKv(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    try smokeTurboquantKvFormat(allocator, &ctx, &module, 0);
    try smokeTurboquantKvFormat(allocator, &ctx, &module, 1);
    try smokeTurboquantKvFormat(allocator, &ctx, &module, 2);
}

fn smokeTurboquantKvFormat(
    allocator: std.mem.Allocator,
    ctx: *context_mod.CudaContext,
    module: *KernelModule,
    format: u32,
) !void {
    const num_kv_heads: usize = 1;
    const num_heads: usize = 1;
    const head_dim: usize = 64;
    const token_count: usize = 2;
    const row_width = num_kv_heads * head_dim;
    const base_key_row_bytes = switch (format) {
        0 => turboquant.polar4KeyBytes(num_kv_heads, head_dim),
        1 => turboquant.turbo3KeyBytes(num_kv_heads, head_dim),
        2 => kv_pool_mod.KvDType.f16.bytesForKeyRow(@intCast(num_kv_heads), @intCast(head_dim)),
        else => return error.CudaSmokeMismatch,
    };
    const key_row_bytes = switch (format) {
        0 => base_key_row_bytes,
        1 => base_key_row_bytes + turboquant.turbo3ResidualBytes(num_kv_heads, head_dim),
        2 => base_key_row_bytes,
        else => unreachable,
    };
    const value_format: u32 = if (format == 2) 2 else 1;
    const value_row_bytes = if (value_format == 2)
        kv_pool_mod.KvDType.f16.bytesForValueRow(@intCast(num_kv_heads), @intCast(head_dim))
    else
        kv_pool_mod.KvDType.int8.bytesForValueRow(@intCast(num_kv_heads), @intCast(head_dim));
    const page_size_tokens: usize = 1;
    const block_table_host = [_]u32{ 1, 3 };
    const physical_token_capacity: usize = 4;
    if (key_row_bytes == 0 or base_key_row_bytes == 0) return error.CudaSmokeMismatch;

    var q_host: [head_dim]f32 = undefined;
    var k_host: [token_count * row_width]f32 = undefined;
    var v_host: [token_count * row_width]f32 = undefined;
    for (0..head_dim) |d| {
        q_host[d] = (@as(f32, @floatFromInt(@as(i32, @intCast(d % 11)) - 5)) / 9.0);
        k_host[d] = (@as(f32, @floatFromInt(@as(i32, @intCast(d % 13)) - 6)) / 8.0);
        k_host[row_width + d] = (@as(f32, @floatFromInt(@as(i32, @intCast((d * 3) % 17)) - 8)) / 10.0);
        v_host[d] = @as(f32, @floatFromInt(@as(i32, @intCast(d % 7)) - 3)) / 5.0;
        v_host[row_width + d] = @as(f32, @floatFromInt(@as(i32, @intCast((d * 5) % 9)) - 4)) / 6.0;
    }

    var k_src = try buffer_mod.DeviceBuffer.alloc(ctx, k_host.len * @sizeOf(f32));
    defer k_src.free(ctx);
    var v_src = try buffer_mod.DeviceBuffer.alloc(ctx, v_host.len * @sizeOf(f32));
    defer v_src.free(ctx);
    var block_table = try buffer_mod.DeviceBuffer.alloc(ctx, block_table_host.len * @sizeOf(u32));
    defer block_table.free(ctx);
    var k_dst = try buffer_mod.DeviceBuffer.alloc(ctx, physical_token_capacity * key_row_bytes);
    defer k_dst.free(ctx);
    var v_dst = try buffer_mod.DeviceBuffer.alloc(ctx, physical_token_capacity * value_row_bytes);
    defer v_dst.free(ctx);
    var q = try buffer_mod.DeviceBuffer.alloc(ctx, q_host.len * @sizeOf(f32));
    defer q.free(ctx);
    var out = try buffer_mod.DeviceBuffer.alloc(ctx, q_host.len * @sizeOf(f32));
    defer out.free(ctx);

    try k_src.copyFromHost(ctx, std.mem.sliceAsBytes(&k_host));
    try v_src.copyFromHost(ctx, std.mem.sliceAsBytes(&v_host));
    try block_table.copyFromHost(ctx, std.mem.sliceAsBytes(&block_table_host));
    try q.copyFromHost(ctx, std.mem.sliceAsBytes(&q_host));
    try module.launchKvWriteSuffixTurboquantF32(
        ctx,
        k_dst,
        v_dst,
        block_table,
        k_src,
        v_src,
        .{},
        token_count,
        row_width,
        num_kv_heads,
        head_dim,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        token_count,
        block_table_host.len,
        page_size_tokens,
        format,
        value_format,
        physical_token_capacity,
    );
    try ctx.synchronize();

    const encoded_actual = try allocator.alloc(u8, physical_token_capacity * key_row_bytes);
    defer allocator.free(encoded_actual);
    try k_dst.copyToHost(ctx, encoded_actual);
    try ctx.synchronize();
    const encoded_expected = try allocator.alloc(u8, token_count * key_row_bytes);
    defer allocator.free(encoded_expected);
    const value_actual = try allocator.alloc(u8, physical_token_capacity * value_row_bytes);
    defer allocator.free(value_actual);
    try v_dst.copyToHost(ctx, value_actual);
    try ctx.synchronize();
    const value_expected = try allocator.alloc(u8, token_count * value_row_bytes);
    defer allocator.free(value_expected);
    const key_dequant = try allocator.alloc(f32, token_count * row_width);
    defer allocator.free(key_dequant);
    const value_dequant = try allocator.alloc(f32, token_count * row_width);
    defer allocator.free(value_dequant);
    for (0..token_count) |token| {
        const src_row = k_host[token * row_width ..][0..row_width];
        const dst_row = encoded_expected[token * key_row_bytes ..][0..key_row_bytes];
        switch (format) {
            0 => try turboquant.encodePolar4Key(src_row, dst_row, num_kv_heads, head_dim),
            1 => {
                try turboquant.encodeTurbo3Key(src_row, dst_row[0..base_key_row_bytes], num_kv_heads, head_dim);
                try turboquant.encodeTurbo3ResidualSketch(src_row, dst_row[0..base_key_row_bytes], dst_row[base_key_row_bytes..], num_kv_heads, head_dim);
            },
            2 => {
                for (src_row, 0..) |value, i| {
                    const half_bits: u16 = @bitCast(@as(f16, @floatCast(value)));
                    dst_row[i * 2] = @intCast(half_bits & 0xff);
                    dst_row[i * 2 + 1] = @intCast(half_bits >> 8);
                    key_dequant[token * row_width + i] = @floatCast(@as(f16, @bitCast(half_bits)));
                }
            },
            else => unreachable,
        }
        const value_src = v_host[token * row_width ..][0..row_width];
        const value_dst = value_expected[token * value_row_bytes ..][0..value_row_bytes];
        if (value_format == 2) {
            for (value_src, 0..) |value, i| {
                const half_bits: u16 = @bitCast(@as(f16, @floatCast(value)));
                value_dst[i * 2] = @intCast(half_bits & 0xff);
                value_dst[i * 2 + 1] = @intCast(half_bits >> 8);
                value_dequant[token * row_width + i] = @floatCast(@as(f16, @bitCast(half_bits)));
            }
        } else {
            kv_pool_mod.quantizeF32ToInt8PerHead(value_src, value_dst, @intCast(num_kv_heads), @intCast(head_dim));
            kv_pool_mod.dequantizeInt8PerHeadToF32(value_dst, value_dequant[token * row_width ..][0..row_width], @intCast(num_kv_heads), @intCast(head_dim));
        }
    }
    for (0..token_count) |token| {
        const physical = @as(usize, block_table_host[token]) * page_size_tokens;
        const key_actual_row = encoded_actual[physical * key_row_bytes ..][0..key_row_bytes];
        const key_expected_row = encoded_expected[token * key_row_bytes ..][0..key_row_bytes];
        if (!std.mem.eql(u8, key_actual_row, key_expected_row)) return error.CudaSmokeMismatch;
        const value_actual_row = value_actual[physical * value_row_bytes ..][0..value_row_bytes];
        const value_expected_row = value_expected[token * value_row_bytes ..][0..value_row_bytes];
        if (!std.mem.eql(u8, value_actual_row, value_expected_row)) return error.CudaSmokeMismatch;
    }

    _ = try module.launchGqaAttentionDecodeTurboquantF32(
        ctx,
        out,
        q,
        k_dst,
        v_dst,
        block_table,
        .{},
        .{},
        1,
        1,
        token_count,
        num_heads,
        num_kv_heads,
        head_dim,
        1,
        0,
        0,
        token_count,
        0,
        0,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_table_host.len,
        page_size_tokens,
        format,
        value_format,
        physical_token_capacity,
        .{},
    );
    try ctx.synchronize();
    const out_host = try allocator.alloc(f32, q_host.len);
    defer allocator.free(out_host);
    try out.copyToHost(ctx, std.mem.sliceAsBytes(out_host));
    try ctx.synchronize();

    var scores: [token_count]f32 = undefined;
    for (0..token_count) |token| {
        const encoded_row = encoded_expected[token * key_row_bytes ..][0..key_row_bytes];
        const raw_score = switch (format) {
            0 => try turboquant.dotPolar4KeyFast(&q_host, encoded_row, num_kv_heads, head_dim, 0),
            1 => blk: {
                const base = encoded_row[0..base_key_row_bytes];
                const residual = encoded_row[base_key_row_bytes..];
                var projected_query: [turboquant.turbo3_residual_bits_per_head]f32 = undefined;
                try turboquant.projectTurbo3ResidualQuery(&q_host, &projected_query, head_dim, 0);
                const residual_score = try turboquant.dotTurbo3ProjectedResidualSketch(&projected_query, residual, num_kv_heads, head_dim, 0);
                break :blk try turboquant.dotTurbo3KeyFast(&q_host, base, num_kv_heads, head_dim, 0) +
                    turboquant.turbo3_residual_default_scale * residual_score;
            },
            2 => blk: {
                const key_row = key_dequant[token * row_width ..][0..row_width];
                var sum: f32 = 0.0;
                for (0..head_dim) |d| sum += q_host[d] * key_row[d];
                break :blk sum;
            },
            else => unreachable,
        };
        scores[token] = raw_score / @sqrt(@as(f32, @floatFromInt(head_dim)));
    }
    const max_score = @max(scores[0], scores[1]);
    const e0 = @exp(scores[0] - max_score);
    const e1 = @exp(scores[1] - max_score);
    const denom = e0 + e1;
    for (0..head_dim) |d| {
        const expected = (e0 * value_dequant[d] + e1 * value_dequant[row_width + d]) / denom;
        if (@abs(out_host[d] - expected) > 0.0005) return error.CudaSmokeMismatch;
    }
}

fn smokeBf16WeightPrimitives(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const in_dim: usize = 3;
    const out_dim: usize = 2;
    const input_data = [_]f32{ 1.0, 2.0, 3.0, -1.0, 0.5, 4.0 };
    const weight_data = [_]u16{
        bf16Bits(1.0), bf16Bits(0.0), bf16Bits(-1.0),
        bf16Bits(0.5), bf16Bits(2.0), bf16Bits(1.0),
    };
    const expected_linear = [_]f32{ -2.0, 7.5, -5.0, 4.5 };

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try buffer_mod.DeviceBuffer.alloc(ctx, weight_data.len * @sizeOf(u16));
    defer weight.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, rows * out_dim * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(ctx, std.mem.sliceAsBytes(&weight_data));
    try module.launchLinearBf16WeightF32Tiled(ctx, output, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_linear, 0.0001);

    const vocab: usize = 3;
    const embed_dim: usize = 2;
    _ = vocab;
    const embed_weight_data = [_]u16{
        bf16Bits(1.0), bf16Bits(2.0),
        bf16Bits(3.0), bf16Bits(4.0),
        bf16Bits(5.0), bf16Bits(6.0),
    };
    const ids_data = [_]i64{ 2, 0 };
    const expected_embed = [_]f32{ 5.0, 6.0, 1.0, 2.0 };
    var embed_weight = try buffer_mod.DeviceBuffer.alloc(ctx, embed_weight_data.len * @sizeOf(u16));
    defer embed_weight.free(ctx);
    var ids = try buffer_mod.DeviceBuffer.alloc(ctx, ids_data.len * @sizeOf(i64));
    defer ids.free(ctx);
    var embed_output = try buffer_mod.DeviceBuffer.alloc(ctx, ids_data.len * embed_dim * @sizeOf(f32));
    defer embed_output.free(ctx);
    try embed_weight.copyFromHost(ctx, std.mem.sliceAsBytes(&embed_weight_data));
    try ids.copyFromHost(ctx, std.mem.sliceAsBytes(&ids_data));
    try module.launchEmbeddingLookupBf16WeightF32(ctx, embed_output, embed_weight, ids, ids_data.len, embed_dim, 1.0);
    try ctx.synchronize();

    const embed_out = try allocator.alloc(f32, ids_data.len * embed_dim);
    defer allocator.free(embed_out);
    try embed_output.copyToHost(ctx, std.mem.sliceAsBytes(embed_out));
    try ctx.synchronize();
    try expectApproxSlice(embed_out, &expected_embed, 0.0001);

    const q_out_dim: usize = 2;
    const kv_out_dim: usize = 1;
    const k_weight_data = [_]u16{ bf16Bits(1.0), bf16Bits(1.0), bf16Bits(1.0) };
    const v_weight_data = [_]u16{ bf16Bits(-1.0), bf16Bits(0.0), bf16Bits(1.0) };
    const expected_q = [_]f32{ -2.0, 7.5, -5.0, 4.5 };
    const expected_k = [_]f32{ 6.0, 3.5 };
    const expected_v = [_]f32{ 2.0, 5.0 };
    var k_weight = try buffer_mod.DeviceBuffer.alloc(ctx, k_weight_data.len * @sizeOf(u16));
    defer k_weight.free(ctx);
    var v_weight = try buffer_mod.DeviceBuffer.alloc(ctx, v_weight_data.len * @sizeOf(u16));
    defer v_weight.free(ctx);
    var q_output = try buffer_mod.DeviceBuffer.alloc(ctx, rows * q_out_dim * @sizeOf(f32));
    defer q_output.free(ctx);
    var k_output = try buffer_mod.DeviceBuffer.alloc(ctx, rows * kv_out_dim * @sizeOf(f32));
    defer k_output.free(ctx);
    var v_output = try buffer_mod.DeviceBuffer.alloc(ctx, rows * kv_out_dim * @sizeOf(f32));
    defer v_output.free(ctx);
    try k_weight.copyFromHost(ctx, std.mem.sliceAsBytes(&k_weight_data));
    try v_weight.copyFromHost(ctx, std.mem.sliceAsBytes(&v_weight_data));
    try module.launchLinearBf16WeightF32QkvNoBiasTiled(ctx, q_output, k_output, v_output, input, weight, k_weight, v_weight, rows, in_dim, q_out_dim, kv_out_dim);
    try ctx.synchronize();

    const q_out = try allocator.alloc(f32, rows * q_out_dim);
    defer allocator.free(q_out);
    const k_out = try allocator.alloc(f32, rows * kv_out_dim);
    defer allocator.free(k_out);
    const v_out = try allocator.alloc(f32, rows * kv_out_dim);
    defer allocator.free(v_out);
    try q_output.copyToHost(ctx, std.mem.sliceAsBytes(q_out));
    try k_output.copyToHost(ctx, std.mem.sliceAsBytes(k_out));
    try v_output.copyToHost(ctx, std.mem.sliceAsBytes(v_out));
    try ctx.synchronize();
    try expectApproxSlice(q_out, &expected_q, 0.0001);
    try expectApproxSlice(k_out, &expected_k, 0.0001);
    try expectApproxSlice(v_out, &expected_v, 0.0001);
}

fn smokeAddMulScalarF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const a_data = [_]f32{ 1.0, -2.0, 3.5, 0.25 };
    const b_data = [_]f32{ 0.5, 4.0, -1.5, 1.75 };
    const scale_data = [_]f32{0.25};
    const expected = [_]f32{ 0.75, 3.5, -0.625, 1.8125 };

    var a = try buffer_mod.DeviceBuffer.alloc(ctx, a_data.len * @sizeOf(f32));
    defer a.free(ctx);
    var b = try buffer_mod.DeviceBuffer.alloc(ctx, b_data.len * @sizeOf(f32));
    defer b.free(ctx);
    var scale = try buffer_mod.DeviceBuffer.alloc(ctx, scale_data.len * @sizeOf(f32));
    defer scale.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, a_data.len * @sizeOf(f32));
    defer output.free(ctx);

    try a.copyFromHost(ctx, std.mem.sliceAsBytes(&a_data));
    try b.copyFromHost(ctx, std.mem.sliceAsBytes(&b_data));
    try scale.copyFromHost(ctx, std.mem.sliceAsBytes(&scale_data));
    try module.launchAddMulScalarF32(ctx, output, a, b, scale, a_data.len);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, a_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.0001);
}

fn smokeRmsNormAddMulScalarF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const dim: usize = 3;
    const eps: f32 = 1.0e-5;
    const input_data = [_]f32{ 1.0, 2.0, -1.0, -2.0, 0.5, 4.0 };
    const weight_data = [_]f32{ 1.0, -0.5, 2.0 };
    const residual_data = [_]f32{ 0.25, 0.5, -0.75, 1.0, -1.0, 0.0 };
    const scale_data = [_]f32{0.5};
    var expected: [rows * dim]f32 = undefined;
    for (0..rows) |row| {
        const src = input_data[row * dim ..][0..dim];
        var sumsq: f32 = 0.0;
        for (src) |value| sumsq += value * value;
        const norm_scale = 1.0 / std.math.sqrt(sumsq / @as(f32, @floatFromInt(dim)) + eps);
        for (0..dim) |col| {
            const idx = row * dim + col;
            const normed = input_data[idx] * norm_scale * weight_data[col];
            expected[idx] = normed * scale_data[0] + residual_data[idx];
        }
    }

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try buffer_mod.DeviceBuffer.alloc(ctx, weight_data.len * @sizeOf(f32));
    defer weight.free(ctx);
    var residual = try buffer_mod.DeviceBuffer.alloc(ctx, residual_data.len * @sizeOf(f32));
    defer residual.free(ctx);
    var scale = try buffer_mod.DeviceBuffer.alloc(ctx, scale_data.len * @sizeOf(f32));
    defer scale.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(ctx, std.mem.sliceAsBytes(&weight_data));
    try residual.copyFromHost(ctx, std.mem.sliceAsBytes(&residual_data));
    try scale.copyFromHost(ctx, std.mem.sliceAsBytes(&scale_data));
    try module.launchRmsNormAddMulScalarF32(ctx, output, input, weight, residual, scale, rows, dim, eps);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, input_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.0001);
}

fn smokeRmsNormAddF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const dim: usize = 3;
    const eps: f32 = 1.0e-5;
    const input_data = [_]f32{ 1.0, 2.0, -1.0, -2.0, 0.5, 4.0 };
    const weight_data = [_]f32{ 1.0, -0.5, 2.0 };
    const residual_data = [_]f32{ 0.25, 0.5, -0.75, 1.0, -1.0, 0.0 };
    var expected: [rows * dim]f32 = undefined;
    for (0..rows) |row| {
        const src = input_data[row * dim ..][0..dim];
        var sumsq: f32 = 0.0;
        for (src) |value| sumsq += value * value;
        const norm_scale = 1.0 / std.math.sqrt(sumsq / @as(f32, @floatFromInt(dim)) + eps);
        for (0..dim) |col| {
            const idx = row * dim + col;
            expected[idx] = input_data[idx] * norm_scale * weight_data[col] + residual_data[idx];
        }
    }

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try buffer_mod.DeviceBuffer.alloc(ctx, weight_data.len * @sizeOf(f32));
    defer weight.free(ctx);
    var residual = try buffer_mod.DeviceBuffer.alloc(ctx, residual_data.len * @sizeOf(f32));
    defer residual.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(ctx, std.mem.sliceAsBytes(&weight_data));
    try residual.copyFromHost(ctx, std.mem.sliceAsBytes(&residual_data));
    try module.launchRmsNormAddF32(ctx, output, input, weight, residual, rows, dim, eps);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, input_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.0001);
}

fn smokeRmsNormHeadsRopeF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const total_dim: usize = 4;
    const head_dim: usize = 2;
    const rope_dim: usize = 2;
    const eps: f32 = 1.0e-5;
    const theta: f32 = 10000.0;
    const input_data = [_]f32{ 1.0, 2.0, -1.0, 0.5, 0.25, -0.75, 3.0, -2.0 };
    const weight_data = [_]f32{ 1.0, 0.5 };
    const output_scale: f32 = 1.25;
    var normed: [rows * total_dim]f32 = undefined;
    for (0..(rows * total_dim / head_dim)) |chunk| {
        const src = input_data[chunk * head_dim ..][0..head_dim];
        var sumsq: f32 = 0.0;
        for (src) |value| sumsq += value * value;
        const norm_scale = 1.0 / std.math.sqrt(sumsq / @as(f32, @floatFromInt(head_dim)) + eps);
        for (0..head_dim) |col| normed[chunk * head_dim + col] = src[col] * norm_scale * weight_data[col];
    }
    var expected: [rows * total_dim]f32 = undefined;
    for (0..(rows * total_dim / head_dim)) |chunk| {
        const token_pos = (chunk / (total_dim / head_dim)) % rows;
        expectedRopePair(normed[chunk * head_dim ..][0..head_dim], expected[chunk * head_dim ..][0..head_dim], @floatFromInt(token_pos), theta, 1.0);
        for (expected[chunk * head_dim ..][0..head_dim]) |*value| value.* *= output_scale;
    }

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try buffer_mod.DeviceBuffer.alloc(ctx, weight_data.len * @sizeOf(f32));
    defer weight.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(ctx, std.mem.sliceAsBytes(&weight_data));
    try module.launchRmsNormHeadsRopeF32(ctx, output, input, weight, rows, total_dim, head_dim, rope_dim, eps, theta, 1.0, 0, rows, false, output_scale);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, input_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.0001);

    {
        const partial_rows: usize = 1;
        const partial_total_dim: usize = 8;
        const partial_head_dim: usize = 8;
        const partial_rope_dim: usize = 4;
        const partial_input = [_]f32{ 1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0, 40.0 };
        const partial_weight = [_]f32{ 1.0, 0.5, 1.25, -0.75, 0.25, 1.5, -0.5, 2.0 };
        var partial_expected: [partial_input.len]f32 = undefined;
        var sumsq: f32 = 0.0;
        for (partial_input) |value| sumsq += value * value;
        const partial_norm_scale = 1.0 / std.math.sqrt(sumsq / @as(f32, @floatFromInt(partial_head_dim)) + eps);
        for (partial_input, 0..) |value, i| {
            partial_expected[i] = value * partial_norm_scale * partial_weight[i];
        }
        applySplitHalfRopeExpected(partial_expected[0..], 1, partial_head_dim, partial_rope_dim, theta, 1.0);
        for (&partial_expected) |*value| value.* *= output_scale;

        var partial_in = try buffer_mod.DeviceBuffer.alloc(ctx, partial_input.len * @sizeOf(f32));
        defer partial_in.free(ctx);
        var partial_weight_device = try buffer_mod.DeviceBuffer.alloc(ctx, partial_weight.len * @sizeOf(f32));
        defer partial_weight_device.free(ctx);
        var partial_out = try buffer_mod.DeviceBuffer.alloc(ctx, partial_input.len * @sizeOf(f32));
        defer partial_out.free(ctx);

        try partial_in.copyFromHost(ctx, std.mem.sliceAsBytes(&partial_input));
        try partial_weight_device.copyFromHost(ctx, std.mem.sliceAsBytes(&partial_weight));
        try module.launchRmsNormHeadsRopeF32(ctx, partial_out, partial_in, partial_weight_device, partial_rows, partial_total_dim, partial_head_dim, partial_rope_dim, eps, theta, 1.0, 1, 1, false, output_scale);
        try ctx.synchronize();

        const partial_host = try allocator.alloc(f32, partial_input.len);
        defer allocator.free(partial_host);
        try partial_out.copyToHost(ctx, std.mem.sliceAsBytes(partial_host));
        try ctx.synchronize();
        try expectApproxSlice(partial_host, &partial_expected, 0.0001);
    }
}

fn smokeGemma4MtpMaskedArgmaxF32(ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const logits = [_]f32{ 0.1, 0.2, 9.0, 0.4, 0.5, 0.6, 8.0, 0.7 };
    const centroid_logits = [_]f32{ 0.25, 1.0 };
    const token_ordering = [_]f32{ 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0 };

    var logits_device = try buffer_mod.DeviceBuffer.alloc(ctx, logits.len * @sizeOf(f32));
    defer logits_device.free(ctx);
    var centroid_device = try buffer_mod.DeviceBuffer.alloc(ctx, centroid_logits.len * @sizeOf(f32));
    defer centroid_device.free(ctx);
    var ordering_device = try buffer_mod.DeviceBuffer.alloc(ctx, token_ordering.len * @sizeOf(f32));
    defer ordering_device.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, @sizeOf(u32));
    defer output.free(ctx);

    try logits_device.copyFromHost(ctx, std.mem.sliceAsBytes(&logits));
    try centroid_device.copyFromHost(ctx, std.mem.sliceAsBytes(&centroid_logits));
    try ordering_device.copyFromHost(ctx, std.mem.sliceAsBytes(&token_ordering));
    const launched = try module.launchGemma4MtpMaskedArgmaxF32(ctx, output, logits_device, centroid_device, ordering_device, logits.len, centroid_logits.len, 1, false);
    if (!launched) return error.CudaKernelUnavailable;
    try ctx.synchronize();

    var actual: u32 = 0;
    try output.copyToHost(ctx, std.mem.asBytes(&actual));
    try ctx.synchronize();
    if (actual != 6) return error.CudaSmokeMismatch;
}

fn smokeGemma4MtpVerifyCommitU32(ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const target_choices = [_]u32{ 10, 42, 99 };
    const draft_tokens = [_]i64{ 10, 11 };
    const eos_token_ids = [_]i32{99};
    var choices_device = try buffer_mod.DeviceBuffer.alloc(ctx, target_choices.len * @sizeOf(u32));
    defer choices_device.free(ctx);
    var draft_device = try buffer_mod.DeviceBuffer.alloc(ctx, draft_tokens.len * @sizeOf(i64));
    defer draft_device.free(ctx);
    var eos_device = try buffer_mod.DeviceBuffer.alloc(ctx, eos_token_ids.len * @sizeOf(i32));
    defer eos_device.free(ctx);
    var result_device = try buffer_mod.DeviceBuffer.alloc(ctx, 12 * @sizeOf(u32));
    defer result_device.free(ctx);

    try choices_device.copyFromHost(ctx, std.mem.sliceAsBytes(&target_choices));
    try draft_device.copyFromHost(ctx, std.mem.sliceAsBytes(&draft_tokens));
    try eos_device.copyFromHost(ctx, std.mem.sliceAsBytes(&eos_token_ids));
    const launched = try module.launchGemma4MtpVerifyCommitU32(ctx, result_device, choices_device, draft_device, eos_device, draft_tokens.len, eos_token_ids.len, true);
    if (!launched) return error.CudaKernelUnavailable;
    try ctx.synchronize();

    var actual: [12]u32 = undefined;
    try result_device.copyToHost(ctx, std.mem.sliceAsBytes(&actual));
    try ctx.synchronize();
    const expected = [_]u32{ 1, 2, 1, 0, 0, 0, 1, 0, 0xffffffff, 42, 0, 0 };
    if (!std.mem.eql(u32, &actual, &expected)) return error.CudaSmokeMismatch;

    const bonus_choices = [_]u32{ 10, 11, 99 };
    try choices_device.copyFromHost(ctx, std.mem.sliceAsBytes(&bonus_choices));
    const launched_bonus = try module.launchGemma4MtpVerifyCommitU32(ctx, result_device, choices_device, draft_device, eos_device, draft_tokens.len, eos_token_ids.len, true);
    if (!launched_bonus) return error.CudaKernelUnavailable;
    try ctx.synchronize();
    try result_device.copyToHost(ctx, std.mem.sliceAsBytes(&actual));
    try ctx.synchronize();
    const expected_bonus = [_]u32{ 2, 3, 0, 1, 0, 1, 1, 0, 0xffffffff, 0, 99, 0 };
    if (!std.mem.eql(u32, &actual, &expected_bonus)) return error.CudaSmokeMismatch;
}

fn smokeArgmaxLastRowSuppressF32(ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const dim: usize = 5;
    const logits = [_]f32{
        0.0, 1.0, 2.0, 3.0, 4.0,
        1.0, 9.0, 8.0, 9.0, 0.0,
    };
    const suppress_token_ids = [_]i32{ 1, -1 };

    var logits_device = try buffer_mod.DeviceBuffer.alloc(ctx, logits.len * @sizeOf(f32));
    defer logits_device.free(ctx);
    var suppress_device = try buffer_mod.DeviceBuffer.alloc(ctx, suppress_token_ids.len * @sizeOf(i32));
    defer suppress_device.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, @sizeOf(u32));
    defer output.free(ctx);

    try logits_device.copyFromHost(ctx, std.mem.sliceAsBytes(&logits));
    try suppress_device.copyFromHost(ctx, std.mem.sliceAsBytes(&suppress_token_ids));
    try module.launchArgmaxLastRowSuppressF32(ctx, output, logits_device, suppress_device, rows, dim, suppress_token_ids.len);
    try ctx.synchronize();

    var actual: u32 = 0;
    try output.copyToHost(ctx, std.mem.asBytes(&actual));
    try ctx.synchronize();
    if (actual != 3) return error.CudaSmokeMismatch;
}

fn smokeLinearF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const in_dim: usize = 3;
    const out_dim: usize = 2;
    const input_data = [_]f32{ 1.0, 2.0, 3.0, -1.0, 0.5, 4.0 };
    const weight_data = [_]f32{ 1.0, 0.0, -1.0, 0.5, 2.0, 1.0 };
    const bias_data = [_]f32{ 0.25, -1.0 };
    const expected_no_bias = [_]f32{ -2.0, 7.5, -5.0, 4.5 };
    const expected_bias = [_]f32{ -1.75, 6.5, -4.75, 3.5 };

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try buffer_mod.DeviceBuffer.alloc(ctx, weight_data.len * @sizeOf(f32));
    defer weight.free(ctx);
    var bias = try buffer_mod.DeviceBuffer.alloc(ctx, bias_data.len * @sizeOf(f32));
    defer bias.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, rows * out_dim * @sizeOf(f32));
    defer output.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(ctx, std.mem.sliceAsBytes(&weight_data));
    try bias.copyFromHost(ctx, std.mem.sliceAsBytes(&bias_data));

    try module.launchLinearF32(ctx, output, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    const out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_no_bias, 0.0001);

    try module.launchLinearBiasF32(ctx, output, input, weight, bias, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 0.0001);
}

fn smokeRmsNormF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const dim: usize = 3;
    const input_data = [_]f32{ 1.0, 2.0, 3.0, -2.0, 0.0, 4.0 };
    const weight_data = [_]f32{ 1.0, 2.0, -1.0 };
    const eps: f32 = 1.0e-5;
    var expected: [rows * dim]f32 = undefined;
    for (0..rows) |row| {
        var sumsq: f32 = 0.0;
        for (0..dim) |col| {
            const x = input_data[row * dim + col];
            sumsq += x * x;
        }
        const scale = 1.0 / std.math.sqrt(sumsq / @as(f32, @floatFromInt(dim)) + eps);
        for (0..dim) |col| {
            expected[row * dim + col] = input_data[row * dim + col] * scale * weight_data[col];
        }
    }

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try buffer_mod.DeviceBuffer.alloc(ctx, weight_data.len * @sizeOf(f32));
    defer weight.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer output.free(ctx);
    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(ctx, std.mem.sliceAsBytes(&weight_data));
    try module.launchRmsNormF32(ctx, output, input, weight, rows, dim, eps);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, input_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.003);
}

fn smokeRmsNormBareF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const dim: usize = 4;
    const input_data = [_]f32{ 1.0, 2.0, -1.0, 0.5, -2.0, 0.0, 4.0, 1.0 };
    const eps: f32 = 1.0e-5;
    var expected: [rows * dim]f32 = undefined;
    for (0..rows) |row| {
        var sumsq: f32 = 0.0;
        for (0..dim) |col| {
            const x = input_data[row * dim + col];
            sumsq += x * x;
        }
        const scale = 1.0 / std.math.sqrt(sumsq / @as(f32, @floatFromInt(dim)) + eps);
        for (0..dim) |col| {
            expected[row * dim + col] = input_data[row * dim + col] * scale;
        }
    }

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer output.free(ctx);
    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try module.launchRmsNormBareF32(ctx, output, input, rows, dim, eps);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, input_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.003);
}

fn smokeElementwiseF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const a_data = [_]f32{ 1.0, -2.0, 0.0, 4.0 };
    const b_data = [_]f32{ 3.0, 5.0, -1.0, 0.5 };
    const expected_add = [_]f32{ 4.0, 3.0, -1.0, 4.5 };
    const expected_mul = [_]f32{ 3.0, -10.0, -0.0, 2.0 };
    var expected_silu: [a_data.len]f32 = undefined;
    for (a_data, 0..) |x, i| expected_silu[i] = x / (1.0 + std.math.exp(-x));

    var a = try buffer_mod.DeviceBuffer.alloc(ctx, a_data.len * @sizeOf(f32));
    defer a.free(ctx);
    var b = try buffer_mod.DeviceBuffer.alloc(ctx, b_data.len * @sizeOf(f32));
    defer b.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, a_data.len * @sizeOf(f32));
    defer output.free(ctx);
    try a.copyFromHost(ctx, std.mem.sliceAsBytes(&a_data));
    try b.copyFromHost(ctx, std.mem.sliceAsBytes(&b_data));

    const out = try allocator.alloc(f32, a_data.len);
    defer allocator.free(out);

    try module.launchElementwiseF32(ctx, output, a, b, a_data.len, .add);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_add, 0.0001);

    try module.launchElementwiseF32(ctx, output, a, b, a_data.len, .multiply);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_mul, 0.0001);

    try module.launchElementwiseF32(ctx, output, a, .{}, a_data.len, .silu);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_silu, 0.01);
}

fn smokeLayerNormF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const dim: usize = 3;
    const input_data = [_]f32{ 1.0, 2.0, 3.0, -2.0, 0.0, 4.0 };
    const gamma_data = [_]f32{ 1.0, 2.0, -1.0 };
    const beta_data = [_]f32{ 0.5, -0.25, 1.0 };
    const eps: f32 = 1.0e-5;
    var expected: [rows * dim]f32 = undefined;
    for (0..rows) |row| {
        const base = row * dim;
        var mean: f32 = 0.0;
        for (0..dim) |i| mean += input_data[base + i];
        mean /= @floatFromInt(dim);
        var var_sum: f32 = 0.0;
        for (0..dim) |i| {
            const d = input_data[base + i] - mean;
            var_sum += d * d;
        }
        const inv = 1.0 / std.math.sqrt(var_sum / @as(f32, @floatFromInt(dim)) + eps);
        for (0..dim) |i| expected[base + i] = (input_data[base + i] - mean) * inv * gamma_data[i] + beta_data[i];
    }

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var gamma = try buffer_mod.DeviceBuffer.alloc(ctx, gamma_data.len * @sizeOf(f32));
    defer gamma.free(ctx);
    var beta = try buffer_mod.DeviceBuffer.alloc(ctx, beta_data.len * @sizeOf(f32));
    defer beta.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer output.free(ctx);
    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try gamma.copyFromHost(ctx, std.mem.sliceAsBytes(&gamma_data));
    try beta.copyFromHost(ctx, std.mem.sliceAsBytes(&beta_data));
    try module.launchLayerNormF32(ctx, output, input, gamma, beta, rows, dim, eps);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, input_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.003);
}

fn smokeEmbeddingConcatConvF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const weight_data = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const ids_data = [_]i64{ 1, 0, 3 };
    const expected_embed = [_]f32{ 3, 4, 1, 2, 7, 8 };
    var weight = try buffer_mod.DeviceBuffer.alloc(ctx, weight_data.len * @sizeOf(f32));
    defer weight.free(ctx);
    var ids = try buffer_mod.DeviceBuffer.alloc(ctx, ids_data.len * @sizeOf(i64));
    defer ids.free(ctx);
    var embed = try buffer_mod.DeviceBuffer.alloc(ctx, expected_embed.len * @sizeOf(f32));
    defer embed.free(ctx);
    try weight.copyFromHost(ctx, std.mem.sliceAsBytes(&weight_data));
    try ids.copyFromHost(ctx, std.mem.sliceAsBytes(&ids_data));
    try module.launchEmbeddingLookupF32(ctx, embed, weight, ids, ids_data.len, 2, 1.0);
    try ctx.synchronize();
    const embed_out = try allocator.alloc(f32, expected_embed.len);
    defer allocator.free(embed_out);
    try embed.copyToHost(ctx, std.mem.sliceAsBytes(embed_out));
    try ctx.synchronize();
    try expectApproxSlice(embed_out, &expected_embed, 0.0001);

    const ids_i32_data = [_]i32{ 1, 0, 3 };
    var ids_i32 = try buffer_mod.DeviceBuffer.alloc(ctx, ids_i32_data.len * @sizeOf(i32));
    defer ids_i32.free(ctx);
    var embed_i32 = try buffer_mod.DeviceBuffer.alloc(ctx, expected_embed.len * @sizeOf(f32));
    defer embed_i32.free(ctx);
    try ids_i32.copyFromHost(ctx, std.mem.sliceAsBytes(&ids_i32_data));
    try module.launchEmbeddingLookupI32F32(ctx, embed_i32, weight, ids_i32, ids_i32_data.len, 2, 1.0);
    try ctx.synchronize();
    const embed_i32_out = try allocator.alloc(f32, expected_embed.len);
    defer allocator.free(embed_i32_out);
    try embed_i32.copyToHost(ctx, std.mem.sliceAsBytes(embed_i32_out));
    try ctx.synchronize();
    try expectApproxSlice(embed_i32_out, &expected_embed, 0.0001);

    const a_data = [_]f32{ 1, 2, 3, 4 };
    const b_data = [_]f32{ 10, 11, 12, 13, 14, 15 };
    const expected_concat = [_]f32{ 1, 2, 10, 11, 12, 3, 4, 13, 14, 15 };
    var a = try buffer_mod.DeviceBuffer.alloc(ctx, a_data.len * @sizeOf(f32));
    defer a.free(ctx);
    var b = try buffer_mod.DeviceBuffer.alloc(ctx, b_data.len * @sizeOf(f32));
    defer b.free(ctx);
    var concat = try buffer_mod.DeviceBuffer.alloc(ctx, expected_concat.len * @sizeOf(f32));
    defer concat.free(ctx);
    try a.copyFromHost(ctx, std.mem.sliceAsBytes(&a_data));
    try b.copyFromHost(ctx, std.mem.sliceAsBytes(&b_data));
    try module.launchConcatLastDimF32(ctx, concat, a, b, 2, 2, 3);
    try ctx.synchronize();
    const concat_out = try allocator.alloc(f32, expected_concat.len);
    defer allocator.free(concat_out);
    try concat.copyToHost(ctx, std.mem.sliceAsBytes(concat_out));
    try ctx.synchronize();
    try expectApproxSlice(concat_out, &expected_concat, 0.0001);

    const input_data = [_]f32{ 1, 2, 3, 4 };
    const conv_weight_data = [_]f32{ 1, 0, 0, 1 };
    const conv_bias_data = [_]f32{0.5};
    const expected_conv = [_]f32{5.5};
    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var conv_weight = try buffer_mod.DeviceBuffer.alloc(ctx, conv_weight_data.len * @sizeOf(f32));
    defer conv_weight.free(ctx);
    var conv_bias = try buffer_mod.DeviceBuffer.alloc(ctx, conv_bias_data.len * @sizeOf(f32));
    defer conv_bias.free(ctx);
    var conv = try buffer_mod.DeviceBuffer.alloc(ctx, expected_conv.len * @sizeOf(f32));
    defer conv.free(ctx);
    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try conv_weight.copyFromHost(ctx, std.mem.sliceAsBytes(&conv_weight_data));
    try conv_bias.copyFromHost(ctx, std.mem.sliceAsBytes(&conv_bias_data));
    try module.launchConv2dF32(ctx, conv, input, conv_weight, conv_bias, 1, 1, 1, 2, 2, 2, 2, 1, 1, 0, 0, 1, 1, 1);
    try ctx.synchronize();
    const conv_out = try allocator.alloc(f32, expected_conv.len);
    defer allocator.free(conv_out);
    try conv.copyToHost(ctx, std.mem.sliceAsBytes(conv_out));
    try ctx.synchronize();
    try expectApproxSlice(conv_out, &expected_conv, 0.0001);
}

fn smokeAttentionF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const batch: usize = 1;
    const seq: usize = 2;
    const heads: usize = 1;
    const dim: usize = 2;
    const q_token_major = [_]f32{ 1, 0, 0, 1 };
    const k_token_major = [_]f32{ 1, 0, 0, 1 };
    const v_token_major = [_]f32{ 10, 0, 0, 20 };
    const expected_causal = [_]f32{ 10, 0, 3.302384, 13.395232 };
    var q = try buffer_mod.DeviceBuffer.alloc(ctx, q_token_major.len * @sizeOf(f32));
    defer q.free(ctx);
    var k = try buffer_mod.DeviceBuffer.alloc(ctx, k_token_major.len * @sizeOf(f32));
    defer k.free(ctx);
    var v = try buffer_mod.DeviceBuffer.alloc(ctx, v_token_major.len * @sizeOf(f32));
    defer v.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, q_token_major.len * @sizeOf(f32));
    defer output.free(ctx);
    try q.copyFromHost(ctx, std.mem.sliceAsBytes(&q_token_major));
    try k.copyFromHost(ctx, std.mem.sliceAsBytes(&k_token_major));
    try v.copyFromHost(ctx, std.mem.sliceAsBytes(&v_token_major));
    try module.launchAttentionF32(ctx, output, q, k, v, .{}, .{}, batch, seq, heads, dim, true, false, 0, false);
    try ctx.synchronize();
    const out = try allocator.alloc(f32, q_token_major.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_causal, 0.001);

    const mask_data = [_]i64{ 1, 0 };
    const expected_sdpa = [_]f32{ 10, 0, 10, 0 };
    var mask = try buffer_mod.DeviceBuffer.alloc(ctx, mask_data.len * @sizeOf(i64));
    defer mask.free(ctx);
    try mask.copyFromHost(ctx, std.mem.sliceAsBytes(&mask_data));
    try module.launchAttentionF32(ctx, output, q, k, v, mask, .{}, batch, seq, heads, dim, false, true, 0, true);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_sdpa, 0.001);
}

fn smokeFlorence2CrossAttentionF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const batch: usize = 1;
    const dec_seq: usize = 2;
    const enc_seq: usize = 2;
    const heads: usize = 1;
    const head_dim: usize = 2;
    const q_data = [_]f32{ 1, 0, 0, 1 };
    const k_data = [_]f32{ 1, 0, 0, 1 };
    const v_data = [_]f32{ 10, 0, 0, 20 };
    const expected = [_]f32{ 6.697615, 6.604769, 3.302384, 13.395231 };

    var q = try buffer_mod.DeviceBuffer.alloc(ctx, q_data.len * @sizeOf(f32));
    defer q.free(ctx);
    var k = try buffer_mod.DeviceBuffer.alloc(ctx, k_data.len * @sizeOf(f32));
    defer k.free(ctx);
    var v = try buffer_mod.DeviceBuffer.alloc(ctx, v_data.len * @sizeOf(f32));
    defer v.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, expected.len * @sizeOf(f32));
    defer output.free(ctx);
    try q.copyFromHost(ctx, std.mem.sliceAsBytes(&q_data));
    try k.copyFromHost(ctx, std.mem.sliceAsBytes(&k_data));
    try v.copyFromHost(ctx, std.mem.sliceAsBytes(&v_data));
    try module.launchCrossAttentionF32(ctx, output, q, k, v, .{}, batch, dec_seq, enc_seq, heads, head_dim);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, expected.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.001);

    try module.launchCrossAttentionF32(ctx, output, q, k, v, .{}, batch, 1, enc_seq, heads, head_dim);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out[0..head_dim], expected[0..head_dim], 0.001);

    const mask_data = [_]i64{ 1, 0 };
    const expected_masked = [_]f32{ 10, 0, 10, 0 };
    var mask = try buffer_mod.DeviceBuffer.alloc(ctx, mask_data.len * @sizeOf(i64));
    defer mask.free(ctx);
    try mask.copyFromHost(ctx, std.mem.sliceAsBytes(&mask_data));
    try module.launchCrossAttentionF32(ctx, output, q, k, v, mask, batch, dec_seq, enc_seq, heads, head_dim);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_masked, 0.001);

    try module.launchCrossAttentionF32(ctx, output, q, k, v, mask, batch, 1, enc_seq, heads, head_dim);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out[0..head_dim], expected_masked[0..head_dim], 0.001);
}

fn smokeFlorence2LayoutF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const batch: usize = 1;
    const channels: usize = 2;
    const height: usize = 2;
    const width: usize = 2;
    const token_data = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const expected_nchw = [_]f32{ 1, 3, 5, 7, 2, 4, 6, 8 };

    var token = try buffer_mod.DeviceBuffer.alloc(ctx, token_data.len * @sizeOf(f32));
    defer token.free(ctx);
    var nchw = try buffer_mod.DeviceBuffer.alloc(ctx, token_data.len * @sizeOf(f32));
    defer nchw.free(ctx);
    var roundtrip = try buffer_mod.DeviceBuffer.alloc(ctx, token_data.len * @sizeOf(f32));
    defer roundtrip.free(ctx);
    try token.copyFromHost(ctx, std.mem.sliceAsBytes(&token_data));
    try module.launchTokenToNchwF32(ctx, nchw, token, batch, channels, height, width);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, token_data.len);
    defer allocator.free(out);
    try nchw.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_nchw, 0.0001);

    try module.launchNchwToTokenF32(ctx, roundtrip, nchw, batch, channels, height, width);
    try ctx.synchronize();
    try roundtrip.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &token_data, 0.0001);
}

fn smokeFlorence2WindowsF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const batch: usize = 1;
    const height: usize = 2;
    const width: usize = 3;
    const dim: usize = 1;
    const window_size: usize = 2;
    const padded_h: usize = 2;
    const padded_w: usize = 4;
    const token_data = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const expected_packed = [_]f32{ 1, 2, 4, 5, 3, 0, 6, 0 };

    var token = try buffer_mod.DeviceBuffer.alloc(ctx, token_data.len * @sizeOf(f32));
    defer token.free(ctx);
    var packed_windows = try buffer_mod.DeviceBuffer.alloc(ctx, expected_packed.len * @sizeOf(f32));
    defer packed_windows.free(ctx);
    var unpacked = try buffer_mod.DeviceBuffer.alloc(ctx, token_data.len * @sizeOf(f32));
    defer unpacked.free(ctx);
    try token.copyFromHost(ctx, std.mem.sliceAsBytes(&token_data));
    try module.launchPackWindowsF32(ctx, packed_windows, token, batch, height, width, dim, window_size, padded_h, padded_w);
    try ctx.synchronize();

    const packed_out = try allocator.alloc(f32, expected_packed.len);
    defer allocator.free(packed_out);
    try packed_windows.copyToHost(ctx, std.mem.sliceAsBytes(packed_out));
    try ctx.synchronize();
    try expectApproxSlice(packed_out, &expected_packed, 0.0001);

    const unpacked_out = try allocator.alloc(f32, token_data.len);
    defer allocator.free(unpacked_out);
    try module.launchUnpadWindowsF32(ctx, unpacked, packed_windows, batch, height, width, dim, window_size, padded_h, padded_w);
    try ctx.synchronize();
    try unpacked.copyToHost(ctx, std.mem.sliceAsBytes(unpacked_out));
    try ctx.synchronize();
    try expectApproxSlice(unpacked_out, &token_data, 0.0001);
}

fn smokeFlorence2ChannelAttentionF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const batch: usize = 1;
    const seq_len: usize = 2;
    const dim: usize = 2;
    const groups: usize = 1;
    const qkv_data = [_]f32{
        1, 0, 1, 0, 10, 20,
        0, 1, 0, 1, 30, 40,
    };
    const expected_scores = [_]f32{ 0.669762, 0.330238, 0.330238, 0.669762 };
    const expected_out = [_]f32{ 13.30238, 16.69762, 33.30238, 36.69762 };

    var qkv = try buffer_mod.DeviceBuffer.alloc(ctx, qkv_data.len * @sizeOf(f32));
    defer qkv.free(ctx);
    var scores = try buffer_mod.DeviceBuffer.alloc(ctx, expected_scores.len * @sizeOf(f32));
    defer scores.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, expected_out.len * @sizeOf(f32));
    defer output.free(ctx);
    try qkv.copyFromHost(ctx, std.mem.sliceAsBytes(&qkv_data));
    try module.launchChannelScoresSoftmaxF32(ctx, scores, qkv, batch, seq_len, dim, groups);
    try ctx.synchronize();

    const score_out = try allocator.alloc(f32, expected_scores.len);
    defer allocator.free(score_out);
    try scores.copyToHost(ctx, std.mem.sliceAsBytes(score_out));
    try ctx.synchronize();
    try expectApproxSlice(score_out, &expected_scores, 0.001);

    const out = try allocator.alloc(f32, expected_out.len);
    defer allocator.free(out);
    try module.launchChannelApplyF32(ctx, output, qkv, scores, batch, seq_len, dim, groups);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_out, 0.001);
}

fn smokeFlorence2VisionTailSourcesF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const batch: usize = 1;
    const height: usize = 2;
    const width: usize = 2;
    const dim: usize = 4;
    const tokens_data = [_]f32{
        1,  2,  3,  4,
        5,  6,  7,  8,
        9,  10, 11, 12,
        13, 14, 15, 16,
    };
    const row_data = [_]f32{
        100, 200,
        300, 400,
    };
    const col_data = [_]f32{
        10, 20,
        30, 40,
    };
    const temporal_data = [_]f32{ 1, 2, 3, 4 };
    const expected = [_]f32{
        28, 40, 212, 314,
        12, 24, 106, 208,
        36, 48, 110, 212,
        20, 32, 314, 416,
        44, 56, 318, 420,
    };

    var tokens = try buffer_mod.DeviceBuffer.alloc(ctx, tokens_data.len * @sizeOf(f32));
    defer tokens.free(ctx);
    var row = try buffer_mod.DeviceBuffer.alloc(ctx, row_data.len * @sizeOf(f32));
    defer row.free(ctx);
    var col = try buffer_mod.DeviceBuffer.alloc(ctx, col_data.len * @sizeOf(f32));
    defer col.free(ctx);
    var temporal = try buffer_mod.DeviceBuffer.alloc(ctx, temporal_data.len * @sizeOf(f32));
    defer temporal.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, expected.len * @sizeOf(f32));
    defer output.free(ctx);
    try tokens.copyFromHost(ctx, std.mem.sliceAsBytes(&tokens_data));
    try row.copyFromHost(ctx, std.mem.sliceAsBytes(&row_data));
    try col.copyFromHost(ctx, std.mem.sliceAsBytes(&col_data));
    try temporal.copyFromHost(ctx, std.mem.sliceAsBytes(&temporal_data));

    try module.launchFlorenceVisionTailSourcesF32(ctx, output, tokens, row, col, temporal, batch, height, width, dim, true, 0, 0, 0);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, expected.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.0001);
}

fn smokeRopeF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const head_dim: usize = 4;
    const rope_dim: usize = 4;
    const theta: f32 = 10000.0;
    const freq_scale: f32 = 1.0;
    const input_data = [_]f32{
        1, 0, 0, 1,
        1, 0, 0, 1,
    };
    var expected: [input_data.len]f32 = undefined;
    fillRopeSmokeExpected(expected[0..4], 0, theta, freq_scale);
    fillRopeSmokeExpected(expected[4..8], 1, theta, freq_scale);

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer output.free(ctx);
    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try module.launchRopeF32(ctx, output, input, 2, head_dim, rope_dim, theta, freq_scale, 0, 2, 1, false);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, input_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.0001);

    {
        const partial_head_dim: usize = 8;
        const partial_rope_dim: usize = 4;
        const partial_input = [_]f32{ 1.0, 2.0, 3.0, 4.0, 10.0, 20.0, 30.0, 40.0 };
        var partial_expected = partial_input;
        applySplitHalfRopeExpected(partial_expected[0..], 1, partial_head_dim, partial_rope_dim, theta, freq_scale);

        var partial_in = try buffer_mod.DeviceBuffer.alloc(ctx, partial_input.len * @sizeOf(f32));
        defer partial_in.free(ctx);
        var partial_out = try buffer_mod.DeviceBuffer.alloc(ctx, partial_input.len * @sizeOf(f32));
        defer partial_out.free(ctx);
        try partial_in.copyFromHost(ctx, std.mem.sliceAsBytes(&partial_input));
        try module.launchRopeF32(ctx, partial_out, partial_in, 1, partial_head_dim, partial_rope_dim, theta, freq_scale, 1, 1, 1, false);
        try ctx.synchronize();

        const partial_host = try allocator.alloc(f32, partial_input.len);
        defer allocator.free(partial_host);
        try partial_out.copyToHost(ctx, std.mem.sliceAsBytes(partial_host));
        try ctx.synchronize();
        try expectApproxSlice(partial_host, &partial_expected, 0.0001);
    }
}

fn smokeRopePerItemF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const batch: usize = 2;
    const max_seq_len: usize = 2;
    const num_heads: usize = 1;
    const head_dim: usize = 4;
    const rope_dim: usize = 4;
    const theta: f32 = 10000.0;
    const freq_scale: f32 = 1.0;
    const input_data = [_]f32{
        1, 0, 0, 1,
        1, 0, 0, 1,
        1, 0, 0, 1,
        1, 0, 0, 1,
    };
    const query_lengths = [_]u32{ 1, 2 };
    const position_offsets = [_]u32{ 2, 5 };
    var expected: [input_data.len]f32 = undefined;
    fillRopeSmokeExpected(expected[0..4], 2, theta, freq_scale);
    fillRopeSmokeExpected(expected[4..8], 0, theta, freq_scale);
    fillRopeSmokeExpected(expected[8..12], 5, theta, freq_scale);
    fillRopeSmokeExpected(expected[12..16], 6, theta, freq_scale);

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var lengths = try buffer_mod.DeviceBuffer.alloc(ctx, query_lengths.len * @sizeOf(u32));
    defer lengths.free(ctx);
    var offsets = try buffer_mod.DeviceBuffer.alloc(ctx, position_offsets.len * @sizeOf(u32));
    defer offsets.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer output.free(ctx);
    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try lengths.copyFromHost(ctx, std.mem.sliceAsBytes(&query_lengths));
    try offsets.copyFromHost(ctx, std.mem.sliceAsBytes(&position_offsets));
    try module.launchRopePerItemF32(ctx, output, input, lengths, offsets, batch, max_seq_len, num_heads, head_dim, rope_dim, theta, freq_scale, false);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, input_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.0001);
}

fn smokeGqaAttentionF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const batch: usize = 1;
    const q_seq_len: usize = 2;
    const kv_seq_len: usize = 2;
    const num_heads: usize = 2;
    const num_kv_heads: usize = 1;
    const head_dim: usize = 1;
    const q_data = [_]f32{ 1, 1, 1, 1 };
    const k_data = [_]f32{ 0, 0 };
    const v_data = [_]f32{ 10, 20 };
    const expected = [_]f32{ 10, 10, 15, 15 };

    var q = try buffer_mod.DeviceBuffer.alloc(ctx, q_data.len * @sizeOf(f32));
    defer q.free(ctx);
    var k = try buffer_mod.DeviceBuffer.alloc(ctx, k_data.len * @sizeOf(f32));
    defer k.free(ctx);
    var v = try buffer_mod.DeviceBuffer.alloc(ctx, v_data.len * @sizeOf(f32));
    defer v.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, q_data.len * @sizeOf(f32));
    defer output.free(ctx);
    try q.copyFromHost(ctx, std.mem.sliceAsBytes(&q_data));
    try k.copyFromHost(ctx, std.mem.sliceAsBytes(&k_data));
    try v.copyFromHost(ctx, std.mem.sliceAsBytes(&v_data));
    _ = try module.launchGqaAttentionF32(ctx, output, q, k, v, .{}, .{}, batch, q_seq_len, kv_seq_len, num_heads, num_kv_heads, head_dim, 0, 0, 0, kv_seq_len, 0, 0);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, q_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.0001);
}

pub fn smokeQ8_0(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    const rows: usize = 2;
    const in_dim: usize = 32;
    const out_dim: usize = 3;
    const input_data = [_]f32{
        1,   2,   3,   4,   5,   6,   7,   8,
        9,   10,  11,  12,  13,  14,  15,  16,
        17,  18,  19,  20,  21,  22,  23,  24,
        25,  26,  27,  28,  29,  30,  31,  32,
        -1,  -2,  -3,  -4,  -5,  -6,  -7,  -8,
        -9,  -10, -11, -12, -13, -14, -15, -16,
        -17, -18, -19, -20, -21, -22, -23, -24,
        -25, -26, -27, -28, -29, -30, -31, -32,
    };
    var weight_raw = [_]u8{0} ** (out_dim * q8_0_block_bytes);
    writeQ8_0SmokeRow(weight_raw[0..34], 1.0, 1);
    writeQ8_0SmokeRow(weight_raw[34..68], 0.5, 2);
    writeQ8_0SmokeRow(weight_raw[68..102], 2.0, -1);

    var input = try buffer_mod.DeviceBuffer.alloc(&ctx, input_data.len * @sizeOf(f32));
    defer input.free(&ctx);
    var weight = try buffer_mod.DeviceBuffer.alloc(&ctx, weight_raw.len);
    defer weight.free(&ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer output.free(&ctx);
    try input.copyFromHost(&ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(&ctx, &weight_raw);
    try module.launchLinearQ8_0F32(&ctx, output, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(out);
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();

    const expected = [_]f32{ 528, 528, -1056, -528, -528, 1056 };
    for (expected, 0..) |want, i| {
        if (@abs(out[i] - want) > 0.01) return error.CudaSmokeMismatch;
    }

    const ids_i64 = [_]i64{ 2, 0 };
    const ids_i32 = [_]i32{ 2, 0 };
    var ids64 = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_i64.len * @sizeOf(i64));
    defer ids64.free(&ctx);
    var ids32 = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_i32.len * @sizeOf(i32));
    defer ids32.free(&ctx);
    var embed_out = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_i64.len * in_dim * @sizeOf(f32));
    defer embed_out.free(&ctx);
    try ids64.copyFromHost(&ctx, std.mem.sliceAsBytes(&ids_i64));
    try ids32.copyFromHost(&ctx, std.mem.sliceAsBytes(&ids_i32));

    const embed_expected = try allocator.alloc(f32, ids_i64.len * in_dim);
    defer allocator.free(embed_expected);
    for (0..in_dim) |i| {
        embed_expected[i] = -2.0;
        embed_expected[in_dim + i] = 1.0;
    }
    const embed_actual = try allocator.alloc(f32, ids_i64.len * in_dim);
    defer allocator.free(embed_actual);

    try module.launchEmbeddingLookupQ8_0F32(&ctx, embed_out, weight, ids64, ids_i64.len, in_dim, 1.0);
    try ctx.synchronize();
    try embed_out.copyToHost(&ctx, std.mem.sliceAsBytes(embed_actual));
    try ctx.synchronize();
    try expectApproxSlice(embed_actual, embed_expected, 0.0001);

    try module.launchEmbeddingLookupI32Q8_0F32(&ctx, embed_out, weight, ids32, ids_i32.len, in_dim, 1.0);
    try ctx.synchronize();
    try embed_out.copyToHost(&ctx, std.mem.sliceAsBytes(embed_actual));
    try ctx.synchronize();
    try expectApproxSlice(embed_actual, embed_expected, 0.0001);
}

pub fn smokeQ4_0(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    const rows: usize = 2;
    const in_dim: usize = 32;
    const out_dim: usize = 4;
    const input_data = [_]f32{
        1,   2,   3,   4,   5,   6,   7,   8,
        9,   10,  11,  12,  13,  14,  15,  16,
        17,  18,  19,  20,  21,  22,  23,  24,
        25,  26,  27,  28,  29,  30,  31,  32,
        -1,  -2,  -3,  -4,  -5,  -6,  -7,  -8,
        -9,  -10, -11, -12, -13, -14, -15, -16,
        -17, -18, -19, -20, -21, -22, -23, -24,
        -25, -26, -27, -28, -29, -30, -31, -32,
    };
    var weight_raw = [_]u8{0} ** (out_dim * q4_0_block_bytes);
    writeQ4_0SmokeRow(weight_raw[0..18], 1.0, 1);
    writeQ4_0SmokeRow(weight_raw[18..36], 0.5, 2);
    writeQ4_0SmokeRow(weight_raw[36..54], 2.0, -1);
    var patterned_row = [_]i4{0} ** q4_0_values_per_block;
    patterned_row[0] = 1;
    patterned_row[16] = -1;
    writeQ4_0SmokeRowValues(weight_raw[54..72], 1.0, &patterned_row);

    var input = try buffer_mod.DeviceBuffer.alloc(&ctx, input_data.len * @sizeOf(f32));
    defer input.free(&ctx);
    var weight = try buffer_mod.DeviceBuffer.alloc(&ctx, weight_raw.len);
    defer weight.free(&ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer output.free(&ctx);
    try input.copyFromHost(&ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(&ctx, &weight_raw);
    try module.launchLinearQ4_0F32(&ctx, output, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(out);
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();

    const expected = [_]f32{ 528, 528, -1056, -16, -528, -528, 1056, 16 };
    for (expected, 0..) |want, i| {
        if (@abs(out[i] - want) > 0.01) return error.CudaSmokeMismatch;
    }

    try module.launchLinearQ4_0Tile4F32(&ctx, output, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);

    try module.launchLinearQ4_0Tile8F32(&ctx, output, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);

    var input_q8_1 = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * (in_dim / q8_1_values_per_block) * q8_1_block_bytes);
    defer input_q8_1.free(&ctx);
    try module.launchQuantizeF32Q8_1Rows(&ctx, input_q8_1, input, rows, in_dim);
    try ctx.synchronize();

    try module.launchLinearQ4_0Q8_1Tile4F32(&ctx, output, input_q8_1, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 4.0);

    try module.launchLinearQ4_0Q8_1Tile8F32(&ctx, output, input_q8_1, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 4.0);

    const large_rows: usize = 1;
    const large_in_dim: usize = 512;
    const large_out_dim: usize = 8;
    const large_row_blocks = large_in_dim / q4_0_values_per_block;
    var large_input_data: [large_rows * large_in_dim]f32 = undefined;
    for (&large_input_data, 0..) |*value, i| {
        const signed: i32 = @intCast((i * 17 + 11) % 97);
        const centered: f32 = @as(f32, @floatFromInt(signed - 48)) / 19.0;
        const ripple: f32 = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7) % 9)) - 4)) / 43.0;
        value.* = centered + ripple;
    }
    var large_weight_raw = [_]u8{0} ** (large_out_dim * large_row_blocks * q4_0_block_bytes);
    for (0..large_out_dim) |col| {
        for (0..large_row_blocks) |block| {
            var values = [_]i4{0} ** q4_0_values_per_block;
            for (&values, 0..) |*value, lane_index| {
                const raw: i32 = @intCast((col * 31 + block * 13 + lane_index * 5) % 15);
                value.* = @intCast(raw - 7);
            }
            const scale = 0.0175 + @as(f32, @floatFromInt((col * 3 + block) % 11)) * 0.00425;
            const offset = (col * large_row_blocks + block) * q4_0_block_bytes;
            writeQ4_0SmokeRowValues(large_weight_raw[offset .. offset + q4_0_block_bytes], scale, &values);
        }
    }

    var large_input = try buffer_mod.DeviceBuffer.alloc(&ctx, large_input_data.len * @sizeOf(f32));
    defer large_input.free(&ctx);
    var large_weight = try buffer_mod.DeviceBuffer.alloc(&ctx, large_weight_raw.len);
    defer large_weight.free(&ctx);
    var large_exact = try buffer_mod.DeviceBuffer.alloc(&ctx, large_rows * large_out_dim * @sizeOf(f32));
    defer large_exact.free(&ctx);
    var large_q8 = try buffer_mod.DeviceBuffer.alloc(&ctx, large_rows * large_out_dim * @sizeOf(f32));
    defer large_q8.free(&ctx);
    var large_input_q8_1 = try buffer_mod.DeviceBuffer.alloc(&ctx, large_rows * large_row_blocks * q8_1_block_bytes);
    defer large_input_q8_1.free(&ctx);
    try large_input.copyFromHost(&ctx, std.mem.sliceAsBytes(&large_input_data));
    try large_weight.copyFromHost(&ctx, &large_weight_raw);
    try module.launchLinearQ4_0Tile4F32(&ctx, large_exact, large_input, large_weight, large_rows, large_in_dim, large_out_dim);
    try module.launchQuantizeF32Q8_1Rows(&ctx, large_input_q8_1, large_input, large_rows, large_in_dim);
    try module.launchLinearQ4_0Q8_1Tile4F32(&ctx, large_q8, large_input_q8_1, large_weight, large_rows, large_in_dim, large_out_dim);
    try ctx.synchronize();
    const large_exact_host = try allocator.alloc(f32, large_rows * large_out_dim);
    defer allocator.free(large_exact_host);
    const large_q8_host = try allocator.alloc(f32, large_rows * large_out_dim);
    defer allocator.free(large_q8_host);
    try large_exact.copyToHost(&ctx, std.mem.sliceAsBytes(large_exact_host));
    try large_q8.copyToHost(&ctx, std.mem.sliceAsBytes(large_q8_host));
    try ctx.synchronize();
    try expectApproxSlice(large_q8_host, large_exact_host, 0.35);

    try module.launchLinearQ4_0Q8_1Tile8F32(&ctx, large_q8, large_input_q8_1, large_weight, large_rows, large_in_dim, large_out_dim);
    try ctx.synchronize();
    try large_q8.copyToHost(&ctx, std.mem.sliceAsBytes(large_q8_host));
    try ctx.synchronize();
    try expectApproxSlice(large_q8_host, large_exact_host, 0.35);

    var pair_a = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer pair_a.free(&ctx);
    var pair_b = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer pair_b.free(&ctx);
    try module.launchLinearQ4_0PairNoBiasTile4F32(&ctx, pair_a, pair_b, input, weight, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try pair_a.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);
    try pair_b.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);

    try module.launchLinearQ4_0PairNoBiasTile4W4F32(&ctx, pair_a, pair_b, input, weight, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try pair_a.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);
    try pair_b.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);

    try module.launchLinearQ4_0PairNoBiasTile8F32(&ctx, pair_a, pair_b, input, weight, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try pair_a.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);
    try pair_b.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);

    try module.launchLinearQ4_0PairNoBiasQ8_1Tile4F32(&ctx, pair_a, pair_b, input_q8_1, weight, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try pair_a.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 4.0);
    try pair_b.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 4.0);

    try module.launchLinearQ4_0PairNoBiasQ8_1Tile8F32(&ctx, pair_a, pair_b, input_q8_1, weight, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try pair_a.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 4.0);
    try pair_b.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 4.0);

    var q_out = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer q_out.free(&ctx);
    var k_out = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer k_out.free(&ctx);
    var v_out = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer v_out.free(&ctx);
    try module.launchLinearQ4_0QkvNoBiasTile4F32(&ctx, q_out, k_out, v_out, input, weight, weight, weight, rows, in_dim, out_dim, out_dim);
    try ctx.synchronize();
    try q_out.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);
    try k_out.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);
    try v_out.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);

    try module.launchLinearQ4_0QkvNoBiasTile8F32(&ctx, q_out, k_out, v_out, input, weight, weight, weight, rows, in_dim, out_dim, out_dim);
    try ctx.synchronize();
    try q_out.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);
    try k_out.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);
    try v_out.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);

    try module.launchLinearQ4_0QkvNoBiasQ8_1Tile4F32(&ctx, q_out, k_out, v_out, input_q8_1, weight, weight, weight, rows, in_dim, out_dim, out_dim);
    try ctx.synchronize();
    try q_out.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 4.0);
    try k_out.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 4.0);
    try v_out.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 4.0);

    const gate_data = [_]f32{1.0} ** (rows * in_dim);
    var gate = try buffer_mod.DeviceBuffer.alloc(&ctx, gate_data.len * @sizeOf(f32));
    defer gate.free(&ctx);
    try gate.copyFromHost(&ctx, std.mem.sliceAsBytes(&gate_data));
    try module.launchLinearQ4_0GatedDownTile4F32(&ctx, output, gate, input, weight, rows, in_dim, out_dim, 3);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);

    try module.launchLinearQ4_0GatedDownTile8F32(&ctx, output, gate, input, weight, rows, in_dim, out_dim, 3);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);

    try module.launchLinearQ4_0GatedDownTile16F32(&ctx, output, gate, input, weight, rows, in_dim, out_dim, 3);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 0.01);

    var gated_q8_1 = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * (in_dim / q8_1_values_per_block) * q8_1_block_bytes);
    defer gated_q8_1.free(&ctx);
    try module.launchQuantizeGatedF32Q8_1Rows(&ctx, gated_q8_1, gate, input, rows, in_dim, 3);
    try ctx.synchronize();
    try module.launchLinearQ4_0Q8_1Tile4F32(&ctx, output, gated_q8_1, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected[0..], 4.0);

    const col_tiles = (out_dim + q4_0_col_tile - 1) / q4_0_col_tile;
    var argmax_output = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * @sizeOf(u32));
    defer argmax_output.free(&ctx);
    var partial_values = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * col_tiles * @sizeOf(f32));
    defer partial_values.free(&ctx);
    var partial_indices = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * col_tiles * @sizeOf(u32));
    defer partial_indices.free(&ctx);
    const empty_suppress: buffer_mod.DeviceBuffer = .{};
    try module.launchLinearQ4_0ArgmaxRowsTile4F32(
        &ctx,
        argmax_output,
        partial_values,
        partial_indices,
        input,
        weight,
        empty_suppress,
        rows,
        in_dim,
        out_dim,
        0,
    );
    try ctx.synchronize();
    const argmax_actual = try allocator.alloc(u32, rows);
    defer allocator.free(argmax_actual);
    try argmax_output.copyToHost(&ctx, std.mem.sliceAsBytes(argmax_actual));
    try ctx.synchronize();
    const argmax_expected = [_]u32{ 0, 2 };
    for (argmax_expected, 0..) |want, i| {
        if (argmax_actual[i] != want) return error.CudaSmokeMismatch;
    }
}

pub fn smokeQ4_0E4BPairActivationQ8_1Rows(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    const rows: usize = 8;
    const in_dim: usize = 2560;
    const out_dim: usize = 10240;
    const input_row_blocks = in_dim / q8_1_values_per_block;
    const out_row_blocks = out_dim / q8_1_values_per_block;
    const weight_bytes = out_dim * input_row_blocks * q4_0_block_bytes;

    const input_data = try allocator.alloc(f32, rows * in_dim);
    defer allocator.free(input_data);
    for (0..in_dim) |col| {
        const signed: i32 = @intCast((col * 17 + 11) % 97);
        const centered: f32 = @as(f32, @floatFromInt(signed - 48)) / 19.0;
        const ripple: f32 = @as(f32, @floatFromInt(@as(i32, @intCast((col * 7) % 9)) - 4)) / 43.0;
        input_data[col] = centered + ripple;
    }
    for (1..rows) |row| {
        @memcpy(input_data[row * in_dim .. (row + 1) * in_dim], input_data[0..in_dim]);
    }

    const gate_weight_raw = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(gate_weight_raw);
    const up_weight_raw = try allocator.alloc(u8, weight_bytes);
    defer allocator.free(up_weight_raw);
    for (0..out_dim) |col| {
        for (0..input_row_blocks) |block| {
            var gate_values = [_]i4{0} ** q4_0_values_per_block;
            var up_values = [_]i4{0} ** q4_0_values_per_block;
            for (0..q4_0_values_per_block) |lane| {
                const gate_raw: i32 = @intCast((col * 31 + block * 13 + lane * 5) % 15);
                const up_raw: i32 = @intCast((col * 19 + block * 23 + lane * 7 + 3) % 15);
                gate_values[lane] = @intCast(gate_raw - 7);
                up_values[lane] = @intCast(up_raw - 7);
            }
            const gate_scale = 0.0175 + @as(f32, @floatFromInt((col * 3 + block) % 11)) * 0.00425;
            const up_scale = 0.015 + @as(f32, @floatFromInt((col * 5 + block * 2) % 13)) * 0.00375;
            const offset = (col * input_row_blocks + block) * q4_0_block_bytes;
            writeQ4_0SmokeRowValues(gate_weight_raw[offset .. offset + q4_0_block_bytes], gate_scale, &gate_values);
            writeQ4_0SmokeRowValues(up_weight_raw[offset .. offset + q4_0_block_bytes], up_scale, &up_values);
        }
    }

    var input = try buffer_mod.DeviceBuffer.alloc(&ctx, input_data.len * @sizeOf(f32));
    defer input.free(&ctx);
    var input_q8_1 = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * input_row_blocks * q8_1_block_bytes);
    defer input_q8_1.free(&ctx);
    var gate_weight = try buffer_mod.DeviceBuffer.alloc(&ctx, gate_weight_raw.len);
    defer gate_weight.free(&ctx);
    var up_weight = try buffer_mod.DeviceBuffer.alloc(&ctx, up_weight_raw.len);
    defer up_weight.free(&ctx);
    var output_q8_1 = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_row_blocks * q8_1_block_bytes);
    defer output_q8_1.free(&ctx);

    try input.copyFromHost(&ctx, std.mem.sliceAsBytes(input_data));
    try gate_weight.copyFromHost(&ctx, gate_weight_raw);
    try up_weight.copyFromHost(&ctx, up_weight_raw);
    try module.launchQuantizeF32Q8_1Rows(&ctx, input_q8_1, input, rows, in_dim);
    try module.launchLinearQ4_0PairActivationQ8_1Tile32W5E4BFfnQ8_1(
        &ctx,
        output_q8_1,
        input_q8_1,
        gate_weight,
        up_weight,
        rows,
        in_dim,
        out_dim,
        @intFromEnum(@import("../../graph/backend_contracts.zig").DecoderRuntimeActivationKind.gelu),
    );
    try ctx.synchronize();

    const output_host = try allocator.alloc(u8, rows * out_row_blocks * q8_1_block_bytes);
    defer allocator.free(output_host);
    try output_q8_1.copyToHost(&ctx, output_host);
    try ctx.synchronize();
    const row_bytes = out_row_blocks * q8_1_block_bytes;
    for (1..rows) |row| {
        if (!std.mem.eql(u8, output_host[0..row_bytes], output_host[row * row_bytes .. (row + 1) * row_bytes])) {
            return error.CudaSmokeMismatch;
        }
    }
}

pub fn smokeQ4_K(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    const rows: usize = 2;
    const in_dim: usize = 256;
    const out_dim: usize = 2;
    var input_data: [rows * in_dim]f32 = undefined;
    for (0..in_dim) |i| {
        input_data[i] = @floatFromInt(i + 1);
        input_data[in_dim + i] = -@as(f32, @floatFromInt(i + 1));
    }
    var weight_raw = [_]u8{0} ** (out_dim * q4_k_block_bytes);
    writeQ4_KSmokeRow(weight_raw[0..144], 1.0, 1);
    writeQ4_KSmokeRow(weight_raw[144..288], 0.5, 2);
    const bias_data = [_]f32{ 0.25, -1.0 };

    var input = try buffer_mod.DeviceBuffer.alloc(&ctx, input_data.len * @sizeOf(f32));
    defer input.free(&ctx);
    var weight = try buffer_mod.DeviceBuffer.alloc(&ctx, weight_raw.len);
    defer weight.free(&ctx);
    var bias = try buffer_mod.DeviceBuffer.alloc(&ctx, bias_data.len * @sizeOf(f32));
    defer bias.free(&ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer output.free(&ctx);
    try input.copyFromHost(&ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(&ctx, &weight_raw);
    try bias.copyFromHost(&ctx, std.mem.sliceAsBytes(&bias_data));

    try module.launchLinearQ4KF32(&ctx, output, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    const out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(out);
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    const expected = [_]f32{ 32896, 32896, -32896, -32896 };
    try expectApproxSlice(out, &expected, 0.1);

    try module.launchLinearQ4KTiledF32(&ctx, output, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.1);

    try module.launchLinearQ4KBiasF32(&ctx, output, input, weight, bias, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    const expected_bias = [_]f32{ 32896.25, 32895, -32895.75, -32897 };
    try expectApproxSlice(out, &expected_bias, 0.1);

    try module.launchLinearQ4KBiasTiledF32(&ctx, output, input, weight, bias, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 0.1);

    try module.launchLinearQ4KBiasQuickGeluTiledF32(&ctx, output, input, weight, bias, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    var expected_quick_gelu: [rows * out_dim]f32 = undefined;
    for (&expected_quick_gelu, 0..) |*value, i| {
        const x = expected_bias[i];
        value.* = x / (1.0 + std.math.exp(-1.702 * x));
    }
    try expectApproxSlice(out, &expected_quick_gelu, 0.1);

    const ids_data = [_]i64{ 1, 0 };
    var ids = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_data.len * @sizeOf(i64));
    defer ids.free(&ctx);
    var embed_out = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_data.len * in_dim * @sizeOf(f32));
    defer embed_out.free(&ctx);
    try ids.copyFromHost(&ctx, std.mem.sliceAsBytes(&ids_data));
    try module.launchEmbeddingLookupQ4KF32(&ctx, embed_out, weight, ids, ids_data.len, in_dim, 1.0);
    try ctx.synchronize();
    const embed = try allocator.alloc(f32, ids_data.len * in_dim);
    defer allocator.free(embed);
    try embed_out.copyToHost(&ctx, std.mem.sliceAsBytes(embed));
    try ctx.synchronize();
    for (embed) |value| {
        if (@abs(value - 1.0) > 0.001) return error.CudaSmokeMismatch;
    }

    var output_b = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer output_b.free(&ctx);
    var output_c = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * out_dim * @sizeOf(f32));
    defer output_c.free(&ctx);
    try module.launchLinearQ4KTripleBiasF32(&ctx, output, output_b, output_c, input, weight, bias, weight, bias, weight, bias, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 0.1);
    try output_b.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 0.1);
    try output_c.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 0.1);

    try module.launchLinearQ4KTripleBiasTiledF32(&ctx, output, output_b, output_c, input, weight, bias, weight, bias, weight, bias, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 0.1);
    try output_b.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 0.1);
    try output_c.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 0.1);
}

pub fn smokeQ6_K(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    const rows: usize = 2;
    const dim: usize = q6_k_values_per_block;
    var weight_raw = [_]u8{0} ** (rows * q6_k_block_bytes);
    writeQ6_KSmokeRow(weight_raw[0..q6_k_block_bytes], 1.0, -3);
    writeQ6_KSmokeRow(weight_raw[q6_k_block_bytes .. 2 * q6_k_block_bytes], 0.5, 4);

    var weight = try buffer_mod.DeviceBuffer.alloc(&ctx, weight_raw.len);
    defer weight.free(&ctx);
    try weight.copyFromHost(&ctx, &weight_raw);

    const ids_i64 = [_]i64{ 1, 0 };
    const ids_i32 = [_]i32{ 1, 0 };
    var ids64 = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_i64.len * @sizeOf(i64));
    defer ids64.free(&ctx);
    var ids32 = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_i32.len * @sizeOf(i32));
    defer ids32.free(&ctx);
    try ids64.copyFromHost(&ctx, std.mem.sliceAsBytes(&ids_i64));
    try ids32.copyFromHost(&ctx, std.mem.sliceAsBytes(&ids_i32));

    var embed_out = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_i64.len * dim * @sizeOf(f32));
    defer embed_out.free(&ctx);

    const expected = try allocator.alloc(f32, ids_i64.len * dim);
    defer allocator.free(expected);
    for (0..dim) |i| {
        expected[i] = 2.0;
        expected[dim + i] = -3.0;
    }
    const actual = try allocator.alloc(f32, ids_i64.len * dim);
    defer allocator.free(actual);

    try module.launchEmbeddingLookupQ6KF32(&ctx, embed_out, weight, ids64, ids_i64.len, dim, 1.0);
    try ctx.synchronize();
    try embed_out.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
    try ctx.synchronize();
    try expectApproxSlice(actual, expected, 0.0001);

    try module.launchEmbeddingLookupI32Q6KF32(&ctx, embed_out, weight, ids32, ids_i32.len, dim, 1.0);
    try ctx.synchronize();
    try embed_out.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
    try ctx.synchronize();
    try expectApproxSlice(actual, expected, 0.0001);

    const gate_data = [_]f32{1.0} ** q6_k_values_per_block;
    const up_data = [_]f32{1.0} ** q6_k_values_per_block;
    var gate = try buffer_mod.DeviceBuffer.alloc(&ctx, gate_data.len * @sizeOf(f32));
    defer gate.free(&ctx);
    var up = try buffer_mod.DeviceBuffer.alloc(&ctx, up_data.len * @sizeOf(f32));
    defer up.free(&ctx);
    var down_out = try buffer_mod.DeviceBuffer.alloc(&ctx, rows * @sizeOf(f32));
    defer down_out.free(&ctx);
    try gate.copyFromHost(&ctx, std.mem.sliceAsBytes(&gate_data));
    try up.copyFromHost(&ctx, std.mem.sliceAsBytes(&up_data));
    const down_expected = [_]f32{ -768.0, 512.0 };

    try module.launchLinearQ6KTile4F32(&ctx, down_out, gate, weight, 1, q6_k_values_per_block, rows);
    try ctx.synchronize();
    var down_actual = [_]f32{0} ** rows;
    try down_out.copyToHost(&ctx, std.mem.sliceAsBytes(&down_actual));
    try ctx.synchronize();
    try expectApproxSlice(&down_actual, &down_expected, 0.01);

    @memset(&down_actual, 0);
    try module.launchLinearQ6KGatedDownTile4F32(&ctx, down_out, gate, up, weight, 1, q6_k_values_per_block, rows, @intFromEnum(@import("../../graph/backend_contracts.zig").DecoderRuntimeActivationKind.relu));
    try ctx.synchronize();
    try down_out.copyToHost(&ctx, std.mem.sliceAsBytes(&down_actual));
    try ctx.synchronize();
    try expectApproxSlice(&down_actual, &down_expected, 0.01);
}

fn smokeFlorence2TripleQ4KTcHmmaF32(
    allocator: std.mem.Allocator,
    ctx: *context_mod.CudaContext,
    module: *KernelModule,
) !void {
    const rows: usize = 2;
    const in_dim: usize = 256;
    const out_dim: usize = 2;
    var input_data: [rows * in_dim]f32 = undefined;
    for (0..in_dim) |i| {
        input_data[i] = @floatFromInt(i + 1);
        input_data[in_dim + i] = -@as(f32, @floatFromInt(i + 1));
    }
    var weight_raw = [_]u8{0} ** (out_dim * q4_k_block_bytes);
    writeQ4_KSmokeRow(weight_raw[0..144], 1.0, 1);
    writeQ4_KSmokeRow(weight_raw[144..288], 0.5, 2);
    var weight_tc_raw = [_]u8{0} ** (out_dim * q4_k_tc_block_bytes);
    writeQ4_KSmokeTensorCore(&weight_tc_raw, &weight_raw, in_dim, out_dim);
    var weight_b_raw = [_]u8{0} ** (out_dim * q4_k_block_bytes);
    writeQ4_KSmokeRow(weight_b_raw[0..144], 0.25, 3);
    writeQ4_KSmokeRow(weight_b_raw[144..288], 2.0, 1);
    var weight_b_tc_raw = [_]u8{0} ** (out_dim * q4_k_tc_block_bytes);
    writeQ4_KSmokeTensorCore(&weight_b_tc_raw, &weight_b_raw, in_dim, out_dim);
    const bias_data = [_]f32{ 0.25, -1.0 };
    const bias_b_data = [_]f32{ 1.5, -2.25 };

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var weight = try buffer_mod.DeviceBuffer.alloc(ctx, weight_tc_raw.len);
    defer weight.free(ctx);
    var weight_b = try buffer_mod.DeviceBuffer.alloc(ctx, weight_b_tc_raw.len);
    defer weight_b.free(ctx);
    var bias = try buffer_mod.DeviceBuffer.alloc(ctx, bias_data.len * @sizeOf(f32));
    defer bias.free(ctx);
    var bias_b = try buffer_mod.DeviceBuffer.alloc(ctx, bias_b_data.len * @sizeOf(f32));
    defer bias_b.free(ctx);
    var output_a = try buffer_mod.DeviceBuffer.alloc(ctx, rows * out_dim * @sizeOf(f32));
    defer output_a.free(ctx);
    var output_b = try buffer_mod.DeviceBuffer.alloc(ctx, rows * out_dim * @sizeOf(f32));
    defer output_b.free(ctx);
    var output_c = try buffer_mod.DeviceBuffer.alloc(ctx, rows * out_dim * @sizeOf(f32));
    defer output_c.free(ctx);

    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try weight.copyFromHost(ctx, &weight_tc_raw);
    try weight_b.copyFromHost(ctx, &weight_b_tc_raw);
    try bias.copyFromHost(ctx, std.mem.sliceAsBytes(&bias_data));
    try bias_b.copyFromHost(ctx, std.mem.sliceAsBytes(&bias_b_data));
    try module.launchLinearQ4KTripleBiasTcHmmaF32(ctx, output_a, output_b, output_c, input, weight, bias, weight, bias, weight, bias, rows, in_dim, out_dim);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(out);
    const expected_bias = [_]f32{ 32896.25, 32895, -32895.75, -32897 };
    try output_a.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 4.0);
    try output_b.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 4.0);
    try output_c.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_bias, 4.0);

    const expected_a = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(expected_a);
    const expected_b = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(expected_b);
    try module.launchLinearQ4KTripleBiasTcHmmaF32(ctx, output_a, output_b, output_c, input, weight, bias, weight_b, bias_b, weight, bias, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output_a.copyToHost(ctx, std.mem.sliceAsBytes(expected_a));
    try output_b.copyToHost(ctx, std.mem.sliceAsBytes(expected_b));
    try ctx.synchronize();

    try module.launchLinearQ4KPairBiasTcHmmaF32(ctx, output_a, output_b, input, weight, bias, weight_b, bias_b, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output_a.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected_a, 0.1);
    try output_b.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, expected_b, 0.1);
}

fn writeQ4_KSmokeTensorCore(dst: []u8, raw: []const u8, in_dim: usize, out_dim: usize) void {
    std.debug.assert(in_dim != 0 and in_dim % q4_k_values_per_block == 0);
    const row_blocks = in_dim / q4_k_values_per_block;
    const block_count = out_dim * row_blocks;
    std.debug.assert(raw.len == block_count * q4_k_block_bytes);
    std.debug.assert(dst.len == block_count * q4_k_tc_block_bytes);

    const meta_bytes = block_count * 20;
    for (0..block_count) |block| {
        const src = raw[block * q4_k_block_bytes ..][0..q4_k_block_bytes];
        const meta = dst[block * 20 ..][0..20];
        @memcpy(meta[0..4], src[0..4]);
        const scales = src[4..16];
        for (0..8) |sub| {
            meta[4 + sub] = q4KSmokeScale(scales, sub);
            meta[12 + sub] = q4KSmokeMin(scales, sub);
        }
        @memcpy(dst[meta_bytes + block * 128 ..][0..128], src[16..144]);
    }
}

fn q4KSmokeScale(scales: []const u8, sub: usize) u8 {
    std.debug.assert(scales.len >= 12);
    if (sub < 4) return scales[sub] & 63;
    return (scales[sub + 4] & 0x0f) | ((scales[sub - 4] >> 6) << 4);
}

fn q4KSmokeMin(scales: []const u8, sub: usize) u8 {
    std.debug.assert(scales.len >= 12);
    if (sub < 4) return scales[sub + 4] & 63;
    return (scales[sub + 4] >> 4) | ((scales[sub] >> 6) << 4);
}

fn writeQ8_0SmokeRow(dst: []u8, scale: f32, value: i8) void {
    std.debug.assert(dst.len == q8_0_block_bytes);
    const scale_bits: u16 = @bitCast(@as(f16, @floatCast(scale)));
    dst[0] = @truncate(scale_bits);
    dst[1] = @truncate(scale_bits >> 8);
    for (0..q8_0_values_per_block) |i| dst[2 + i] = @bitCast(value);
}

fn writeQ4_0SmokeRow(dst: []u8, scale: f32, value: i4) void {
    std.debug.assert(dst.len == q4_0_block_bytes);
    const scale_bits: u16 = @bitCast(@as(f16, @floatCast(scale)));
    dst[0] = @truncate(scale_bits);
    dst[1] = @truncate(scale_bits >> 8);
    const nibble: u8 = @intCast(@as(i16, value) + 8);
    for (0..q4_0_values_per_block / 2) |i| dst[2 + i] = nibble | (nibble << 4);
}

fn writeQ4_0SmokeRowValues(dst: []u8, scale: f32, values: *const [q4_0_values_per_block]i4) void {
    std.debug.assert(dst.len == q4_0_block_bytes);
    const scale_bits: u16 = @bitCast(@as(f16, @floatCast(scale)));
    dst[0] = @truncate(scale_bits);
    dst[1] = @truncate(scale_bits >> 8);
    for (0..q4_0_values_per_block / 2) |i| {
        const lo: u8 = @intCast(@as(i16, values[i]) + 8);
        const hi: u8 = @intCast(@as(i16, values[16 + i]) + 8);
        dst[2 + i] = lo | (hi << 4);
    }
}

fn writeQ4_KSmokeRow(dst: []u8, scale: f32, value: u4) void {
    std.debug.assert(dst.len == q4_k_block_bytes);
    @memset(dst, 0);
    const scale_bits: u16 = @bitCast(@as(f16, @floatCast(scale)));
    dst[0] = @truncate(scale_bits);
    dst[1] = @truncate(scale_bits >> 8);
    dst[4] = 1;
    dst[5] = 1;
    dst[6] = 1;
    dst[7] = 1;
    dst[12] = 1;
    dst[13] = 1;
    dst[14] = 1;
    dst[15] = 1;
    const packed_byte = @as(u8, value) | (@as(u8, value) << 4);
    for (0..128) |i| dst[16 + i] = packed_byte;
}

fn writeQ6_KSmokeRow(dst: []u8, scale: f32, value: i8) void {
    std.debug.assert(dst.len == q6_k_block_bytes);
    std.debug.assert(value >= -32 and value <= 31);
    @memset(dst, 0);
    for (0..16) |sub| dst[192 + sub] = @bitCast(@as(i8, 1));
    const scale_bits: u16 = @bitCast(@as(f16, @floatCast(scale)));
    dst[208] = @truncate(scale_bits);
    dst[209] = @truncate(scale_bits >> 8);

    const q: u8 = @intCast(@as(i16, value) + 32);
    const low4 = q & 0x0f;
    const high2 = (q >> 4) & 0x03;
    for (0..16) |sub| {
        const half = sub / 8;
        const group = (sub % 8) / 2;
        const l_base = (sub % 2) * 16;
        const ql_off = half * 64 + (group & 1) * 32;
        const qh_off = half * 32;
        const qh_shift: u3 = @intCast(group * 2);
        const nibble_shift: u3 = @intCast((group / 2) * 4);
        for (0..16) |i| {
            const l = l_base + i;
            dst[ql_off + l] |= low4 << nibble_shift;
            dst[128 + qh_off + l] |= high2 << qh_shift;
        }
    }
}

fn fillRopeSmokeExpected(dst: []f32, position: usize, theta: f32, freq_scale: f32) void {
    std.debug.assert(dst.len == 4);
    const pos: f32 = @floatFromInt(position);
    const angle0 = pos * freq_scale;
    const angle1 = angle0 / std.math.sqrt(theta);
    dst[0] = std.math.cos(angle0);
    dst[1] = -std.math.sin(angle1);
    dst[2] = std.math.sin(angle0);
    dst[3] = std.math.cos(angle1);
}

fn expectedRopePair(input: []const f32, dst: []f32, position: f32, theta: f32, freq_scale: f32) void {
    std.debug.assert(input.len == 2);
    std.debug.assert(dst.len == 2);
    const angle = position * freq_scale / std.math.pow(f32, theta, 0.0);
    const s = std.math.sin(angle);
    const c = std.math.cos(angle);
    dst[0] = input[0] * c - input[1] * s;
    dst[1] = input[0] * s + input[1] * c;
}

fn applySplitHalfRopeExpected(row: []f32, position: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32) void {
    std.debug.assert(row.len == head_dim);
    std.debug.assert(head_dim % 2 == 0);
    std.debug.assert(rope_dim % 2 == 0);
    std.debug.assert(rope_dim <= head_dim);
    const active_pairs = rope_dim / 2;
    const head_half = head_dim / 2;
    const pos: f32 = @floatFromInt(position);
    for (0..active_pairs) |j| {
        const idx0 = j;
        const idx1 = j + head_half;
        const angle = pos * freq_scale / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * j)) / @as(f32, @floatFromInt(rope_dim)));
        const s = std.math.sin(angle);
        const c = std.math.cos(angle);
        const x0 = row[idx0];
        const x1 = row[idx1];
        row[idx0] = x0 * c - x1 * s;
        row[idx1] = x0 * s + x1 * c;
    }
}

fn bf16Bits(value: f32) u16 {
    return @intCast(@as(u32, @bitCast(value)) >> 16);
}

fn expectApproxSlice(actual: []const f32, expected: []const f32, tolerance: f32) !void {
    if (actual.len != expected.len) return error.CudaSmokeMismatch;
    for (expected, 0..) |want, i| {
        if (@abs(actual[i] - want) > tolerance) {
            std.debug.print(
                "cuda_smoke_mismatch: index={d} actual={d:.6} expected={d:.6} tolerance={d:.6}\n",
                .{ i, actual[i], want, tolerance },
            );
            return error.CudaSmokeMismatch;
        }
    }
}

test "cuda kernel launch helper bounds" {
    try std.testing.expectEqual(@as(u32, 0), try toU32(0));
    try std.testing.expectEqual(std.math.maxInt(u32), try toU32(std.math.maxInt(u32)));
    try std.testing.expectError(error.InvalidCudaState, toU32(@as(usize, std.math.maxInt(u32)) + 1));
    try std.testing.expectEqual(@as(usize, 12), try checkedTensorElements(3, 4));
}

test "cuda q8_0 smoke row writer uses gguf block layout" {
    var raw = [_]u8{0} ** q8_0_block_bytes;
    writeQ8_0SmokeRow(&raw, 1.0, -3);
    try std.testing.expectEqual(@as(u8, 0x00), raw[0]);
    try std.testing.expectEqual(@as(u8, 0x3c), raw[1]);
    for (raw[2..]) |byte| try std.testing.expectEqual(@as(u8, @bitCast(@as(i8, -3))), byte);
}

test "cuda q4_0 smoke row writer uses gguf block layout" {
    var raw = [_]u8{0} ** q4_0_block_bytes;
    writeQ4_0SmokeRow(&raw, 1.0, -3);
    try std.testing.expectEqual(@as(u8, 0x00), raw[0]);
    try std.testing.expectEqual(@as(u8, 0x3c), raw[1]);
    for (raw[2..]) |byte| try std.testing.expectEqual(@as(u8, 0x55), byte);
}
