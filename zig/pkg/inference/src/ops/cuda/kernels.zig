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

fn captureParamTraceIndex(ctx: *context_mod.CudaContext) ?usize {
    if (!ctx.debug_graph_capture_active) return null;
    if (!platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_PARAM_TRACE", false)) return null;
    const limit = platform.env.getenvUsize("ANTFLY_INFERENCE_CUDA_CAPTURE_PARAM_TRACE_LIMIT") orelse 256;
    if (ctx.debug_graph_capture_param_trace_count >= limit) return null;
    const index = ctx.debug_graph_capture_param_trace_count;
    ctx.debug_graph_capture_param_trace_count += 1;
    return index;
}

pub const GqaAttentionLaunchKind = enum {
    none,
    decode,
    scalar,
};

pub const KernelModule = struct {
    module: driver_mod.CUmodule = null,
    fill_f32: driver_mod.CUfunction = null,
    f32_to_bf16: driver_mod.CUfunction = null,
    scale_f32: driver_mod.CUfunction = null,
    add_scalar_f32: driver_mod.CUfunction = null,
    binary_scalar_f32: driver_mod.CUfunction = null,
    add_mul_scalar_f32: driver_mod.CUfunction = null,
    linear_f32: driver_mod.CUfunction = null,
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
    linear_q4_k_argmax_rows_stage1_tile4: driver_mod.CUfunction = null,
    argmax_reduce_rows_pairs_f32: driver_mod.CUfunction = null,
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
    rms_norm_add_f32: driver_mod.CUfunction = null,
    rms_norm_add_mul_scalar_f32: driver_mod.CUfunction = null,
    rms_norm_add_output_scale_f32: driver_mod.CUfunction = null,
    rms_norm_bare_f32: driver_mod.CUfunction = null,
    layer_norm_f32: driver_mod.CUfunction = null,
    add_layer_norm_f32: driver_mod.CUfunction = null,
    elementwise_f32: driver_mod.CUfunction = null,
    silu_multiply_f32: driver_mod.CUfunction = null,
    activation_multiply_f32: driver_mod.CUfunction = null,
    embedding_lookup_f32: driver_mod.CUfunction = null,
    embedding_lookup_bf16_weight_f32: driver_mod.CUfunction = null,
    embedding_lookup_i32_f32: driver_mod.CUfunction = null,
    take_rows_f32: driver_mod.CUfunction = null,
    gliner_word_embeddings_f32: driver_mod.CUfunction = null,
    repeat_first_row_f32: driver_mod.CUfunction = null,
    gliner_gru_combine_f32: driver_mod.CUfunction = null,
    concat_lastdim_f32: driver_mod.CUfunction = null,
    conv2d_f32: driver_mod.CUfunction = null,
    attention_f32: driver_mod.CUfunction = null,
    attention_f32_block: driver_mod.CUfunction = null,
    rope_f32: driver_mod.CUfunction = null,
    rope_scaled_f32: driver_mod.CUfunction = null,
    rope_per_item_f32: driver_mod.CUfunction = null,
    rms_norm_heads_rope_f32: driver_mod.CUfunction = null,
    rms_norm_heads_rope_decode_scalars_f32: driver_mod.CUfunction = null,
    gqa_attention_f32: driver_mod.CUfunction = null,
    gqa_attention_decode_f32: driver_mod.CUfunction = null,
    gqa_attention_decode_scalars_f32: driver_mod.CUfunction = null,
    kv_write_suffix_decode_scalars_f32: driver_mod.CUfunction = null,
    deberta_attention_f32: driver_mod.CUfunction = null,
    split_last_dim3_f32: driver_mod.CUfunction = null,
    linear_q8_0_f32: driver_mod.CUfunction = null,
    linear_q8_0_f32_tile4: driver_mod.CUfunction = null,
    linear_q8_0_gated_down_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_0_f32: driver_mod.CUfunction = null,
    linear_q4_k_f32: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32: driver_mod.CUfunction = null,
    linear_q4_k_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_gated_down_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_relu_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_relu_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q4_k_bias_add_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_f32_tile8: driver_mod.CUfunction = null,
    linear_q4_k_triple_bias_f32: driver_mod.CUfunction = null,
    linear_q4_k_triple_bias_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_pair_bias_f32_tiled: driver_mod.CUfunction = null,
    linear_q8_0_pair_nobias_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_pair_nobias_f32_tile4: driver_mod.CUfunction = null,
    linear_f32_qkv_nobias_tiled: driver_mod.CUfunction = null,
    linear_bf16_weight_f32_qkv_nobias_tiled: driver_mod.CUfunction = null,
    linear_q8_0_qkv_nobias_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_qkv_nobias_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_q4_k_f32_qkv_nobias_tiled: driver_mod.CUfunction = null,
    embedding_lookup_q8_0_f32: driver_mod.CUfunction = null,
    embedding_lookup_i32_q8_0_f32: driver_mod.CUfunction = null,
    embedding_lookup_q4_0_f32: driver_mod.CUfunction = null,
    embedding_lookup_i32_q4_0_f32: driver_mod.CUfunction = null,
    embedding_lookup_q4_k_f32: driver_mod.CUfunction = null,
    embedding_lookup_i32_q4_k_f32: driver_mod.CUfunction = null,
    slice_last_dim_f32: driver_mod.CUfunction = null,

    pub fn load(ctx: *context_mod.CudaContext) driver_mod.Error!KernelModule {
        try ctx.makeCurrent();
        var module: driver_mod.CUmodule = null;
        try loadModuleWithJitLog(ctx, &module);
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);

        var fill_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&fill_f32, module, "termite_fill_f32"));
        const f32_to_bf16 = loadOptionalFunction(ctx, module, "termite_f32_to_bf16");
        var scale_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&scale_f32, module, "termite_scale_f32"));
        var add_scalar_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&add_scalar_f32, module, "termite_add_scalar_f32"));
        var binary_scalar_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&binary_scalar_f32, module, "termite_binary_scalar_f32"));
        var add_mul_scalar_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&add_mul_scalar_f32, module, "termite_add_mul_scalar_f32"));
        var linear_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_f32, module, "termite_linear_f32"));
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
        const linear_q4_k_argmax_rows_stage1_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_argmax_rows_stage1_tile4");
        const argmax_reduce_rows_pairs_f32 = loadOptionalFunction(ctx, module, "termite_argmax_reduce_rows_pairs_f32");
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
        var embedding_lookup_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_f32, module, "termite_embedding_lookup_f32"));
        const embedding_lookup_bf16_weight_f32 = loadOptionalFunction(ctx, module, "termite_embedding_lookup_bf16_weight_f32");
        var embedding_lookup_i32_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_i32_f32, module, "termite_embedding_lookup_i32_f32"));
        var take_rows_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&take_rows_f32, module, "termite_take_rows_f32"));
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
        const rope_f32 = loadOptionalFunction(ctx, module, "termite_rope_f32");
        const rope_scaled_f32 = loadOptionalFunction(ctx, module, "termite_rope_scaled_f32");
        const rope_per_item_f32 = loadOptionalFunction(ctx, module, "termite_rope_per_item_f32");
        const rms_norm_heads_rope_f32 = loadOptionalFunction(ctx, module, "termite_rms_norm_heads_rope_f32");
        const rms_norm_heads_rope_decode_scalars_f32 = loadOptionalFunction(ctx, module, "termite_rms_norm_heads_rope_decode_scalars_f32");
        const gqa_attention_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_f32");
        const gqa_attention_decode_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_decode_f32");
        const gqa_attention_decode_scalars_f32 = loadOptionalFunction(ctx, module, "termite_gqa_attention_decode_scalars_f32");
        const kv_write_suffix_decode_scalars_f32 = loadOptionalFunction(ctx, module, "termite_kv_write_suffix_decode_scalars_f32");
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
        var linear_q4_0_f32: driver_mod.CUfunction = null;
        const q4_result = ctx.driver.fns.cuModuleGetFunction(&linear_q4_0_f32, module, "termite_linear_q4_0_f32");
        if (q4_result != driver_mod.CUDA_SUCCESS) {
            linear_q4_0_f32 = null;
        } else {
            try ctx.driver.check(q4_result);
        }
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
        const linear_q4_k_gated_down_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_gated_down_f32_tile4");
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
        var linear_q4_k_f32_tile8: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_f32_tile8, module, "termite_linear_q4_k_f32_tile8"));
        var linear_q4_k_triple_bias_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_triple_bias_f32, module, "termite_linear_q4_k_triple_bias_f32"));
        var linear_q4_k_triple_bias_f32_tiled: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_triple_bias_f32_tiled, module, "termite_linear_q4_k_triple_bias_f32_tiled"));
        var linear_q4_k_pair_bias_f32_tiled: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_pair_bias_f32_tiled, module, "termite_linear_q4_k_pair_bias_f32_tiled"));
        const linear_q8_0_pair_nobias_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_pair_nobias_f32_tile4");
        const linear_q4_k_pair_nobias_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q4_k_pair_nobias_f32_tile4");
        const linear_f32_qkv_nobias_tiled = loadOptionalFunction(ctx, module, "termite_linear_f32_qkv_nobias_tiled");
        const linear_bf16_weight_f32_qkv_nobias_tiled = loadOptionalFunction(ctx, module, "termite_linear_bf16_weight_f32_qkv_nobias_tiled");
        const linear_q8_0_qkv_nobias_f32_tile4 = loadOptionalFunction(ctx, module, "termite_linear_q8_0_qkv_nobias_f32_tile4");
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
        var slice_last_dim_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&slice_last_dim_f32, module, "termite_slice_last_dim_f32"));

        return .{
            .module = module,
            .fill_f32 = fill_f32,
            .f32_to_bf16 = f32_to_bf16,
            .scale_f32 = scale_f32,
            .add_scalar_f32 = add_scalar_f32,
            .binary_scalar_f32 = binary_scalar_f32,
            .add_mul_scalar_f32 = add_mul_scalar_f32,
            .linear_f32 = linear_f32,
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
            .linear_q4_k_argmax_rows_stage1_tile4 = linear_q4_k_argmax_rows_stage1_tile4,
            .argmax_reduce_rows_pairs_f32 = argmax_reduce_rows_pairs_f32,
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
            .rms_norm_add_f32 = rms_norm_add_f32,
            .rms_norm_add_mul_scalar_f32 = rms_norm_add_mul_scalar_f32,
            .rms_norm_add_output_scale_f32 = rms_norm_add_output_scale_f32,
            .rms_norm_bare_f32 = rms_norm_bare_f32,
            .layer_norm_f32 = layer_norm_f32,
            .add_layer_norm_f32 = add_layer_norm_f32,
            .elementwise_f32 = elementwise_f32,
            .silu_multiply_f32 = silu_multiply_f32,
            .activation_multiply_f32 = activation_multiply_f32,
            .embedding_lookup_f32 = embedding_lookup_f32,
            .embedding_lookup_bf16_weight_f32 = embedding_lookup_bf16_weight_f32,
            .embedding_lookup_i32_f32 = embedding_lookup_i32_f32,
            .take_rows_f32 = take_rows_f32,
            .gliner_word_embeddings_f32 = gliner_word_embeddings_f32,
            .repeat_first_row_f32 = repeat_first_row_f32,
            .gliner_gru_combine_f32 = gliner_gru_combine_f32,
            .concat_lastdim_f32 = concat_lastdim_f32,
            .conv2d_f32 = conv2d_f32,
            .attention_f32 = attention_f32,
            .attention_f32_block = attention_f32_block,
            .rope_f32 = rope_f32,
            .rope_scaled_f32 = rope_scaled_f32,
            .rope_per_item_f32 = rope_per_item_f32,
            .rms_norm_heads_rope_f32 = rms_norm_heads_rope_f32,
            .rms_norm_heads_rope_decode_scalars_f32 = rms_norm_heads_rope_decode_scalars_f32,
            .gqa_attention_f32 = gqa_attention_f32,
            .gqa_attention_decode_f32 = gqa_attention_decode_f32,
            .gqa_attention_decode_scalars_f32 = gqa_attention_decode_scalars_f32,
            .kv_write_suffix_decode_scalars_f32 = kv_write_suffix_decode_scalars_f32,
            .deberta_attention_f32 = deberta_attention_f32,
            .split_last_dim3_f32 = split_last_dim3_f32,
            .linear_q8_0_f32 = linear_q8_0_f32,
            .linear_q8_0_f32_tile4 = linear_q8_0_f32_tile4,
            .linear_q8_0_gated_down_f32_tile4 = linear_q8_0_gated_down_f32_tile4,
            .linear_q4_0_f32 = linear_q4_0_f32,
            .linear_q4_k_f32 = linear_q4_k_f32,
            .linear_q4_k_bias_f32 = linear_q4_k_bias_f32,
            .linear_q4_k_f32_tiled = linear_q4_k_f32_tiled,
            .linear_q4_k_bias_f32_tiled = linear_q4_k_bias_f32_tiled,
            .linear_q4_k_bias_quick_gelu_f32_tiled = linear_q4_k_bias_quick_gelu_f32_tiled,
            .linear_q4_k_f32_tile4 = linear_q4_k_f32_tile4,
            .linear_q4_k_gated_down_f32_tile4 = linear_q4_k_gated_down_f32_tile4,
            .linear_q4_k_bias_f32_tile4 = linear_q4_k_bias_f32_tile4,
            .linear_q4_k_bias_f32_tile4_r2 = linear_q4_k_bias_f32_tile4_r2,
            .linear_q4_k_bias_quick_gelu_f32_tile4 = linear_q4_k_bias_quick_gelu_f32_tile4,
            .linear_q4_k_bias_relu_f32_tile4 = linear_q4_k_bias_relu_f32_tile4,
            .linear_q4_k_bias_relu_f32_tile4_r2 = linear_q4_k_bias_relu_f32_tile4_r2,
            .linear_q4_k_bias_add_f32_tile4 = linear_q4_k_bias_add_f32_tile4,
            .linear_q4_k_f32_tile8 = linear_q4_k_f32_tile8,
            .linear_q4_k_triple_bias_f32 = linear_q4_k_triple_bias_f32,
            .linear_q4_k_triple_bias_f32_tiled = linear_q4_k_triple_bias_f32_tiled,
            .linear_q4_k_pair_bias_f32_tiled = linear_q4_k_pair_bias_f32_tiled,
            .linear_q8_0_pair_nobias_f32_tile4 = linear_q8_0_pair_nobias_f32_tile4,
            .linear_q4_k_pair_nobias_f32_tile4 = linear_q4_k_pair_nobias_f32_tile4,
            .linear_f32_qkv_nobias_tiled = linear_f32_qkv_nobias_tiled,
            .linear_bf16_weight_f32_qkv_nobias_tiled = linear_bf16_weight_f32_qkv_nobias_tiled,
            .linear_q8_0_qkv_nobias_f32_tile4 = linear_q8_0_qkv_nobias_f32_tile4,
            .linear_q4_k_qkv_nobias_f32_tiled = linear_q4_k_qkv_nobias_f32_tiled,
            .linear_q4_k_q4_k_f32_qkv_nobias_tiled = linear_q4_k_q4_k_f32_qkv_nobias_tiled,
            .embedding_lookup_q8_0_f32 = embedding_lookup_q8_0_f32,
            .embedding_lookup_i32_q8_0_f32 = embedding_lookup_i32_q8_0_f32,
            .embedding_lookup_q4_0_f32 = embedding_lookup_q4_0_f32,
            .embedding_lookup_i32_q4_0_f32 = embedding_lookup_i32_q4_0_f32,
            .embedding_lookup_q4_k_f32 = embedding_lookup_q4_k_f32,
            .embedding_lookup_i32_q4_k_f32 = embedding_lookup_i32_q4_k_f32,
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
            self.scale_f32 = null;
            self.add_scalar_f32 = null;
            self.binary_scalar_f32 = null;
            self.add_mul_scalar_f32 = null;
            self.linear_f32 = null;
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
            self.linear_q4_k_argmax_rows_stage1_tile4 = null;
            self.argmax_reduce_rows_pairs_f32 = null;
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
            self.rms_norm_add_mul_scalar_f32 = null;
            self.rms_norm_add_output_scale_f32 = null;
            self.rms_norm_bare_f32 = null;
            self.layer_norm_f32 = null;
            self.add_layer_norm_f32 = null;
            self.elementwise_f32 = null;
            self.silu_multiply_f32 = null;
            self.activation_multiply_f32 = null;
            self.embedding_lookup_f32 = null;
            self.embedding_lookup_bf16_weight_f32 = null;
            self.embedding_lookup_i32_f32 = null;
            self.take_rows_f32 = null;
            self.gliner_word_embeddings_f32 = null;
            self.repeat_first_row_f32 = null;
            self.gliner_gru_combine_f32 = null;
            self.concat_lastdim_f32 = null;
            self.conv2d_f32 = null;
            self.attention_f32 = null;
            self.attention_f32_block = null;
            self.rope_f32 = null;
            self.rope_scaled_f32 = null;
            self.rope_per_item_f32 = null;
            self.rms_norm_heads_rope_f32 = null;
            self.rms_norm_heads_rope_decode_scalars_f32 = null;
            self.gqa_attention_f32 = null;
            self.gqa_attention_decode_f32 = null;
            self.gqa_attention_decode_scalars_f32 = null;
            self.kv_write_suffix_decode_scalars_f32 = null;
            self.deberta_attention_f32 = null;
            self.split_last_dim3_f32 = null;
            self.linear_q8_0_f32 = null;
            self.linear_q8_0_f32_tile4 = null;
            self.linear_q8_0_gated_down_f32_tile4 = null;
            self.linear_q4_0_f32 = null;
            self.linear_q4_k_f32 = null;
            self.linear_q4_k_bias_f32 = null;
            self.linear_q4_k_f32_tiled = null;
            self.linear_q4_k_bias_f32_tiled = null;
            self.linear_q4_k_bias_quick_gelu_f32_tiled = null;
            self.linear_q4_k_f32_tile4 = null;
            self.linear_q4_k_gated_down_f32_tile4 = null;
            self.linear_q4_k_bias_f32_tile4 = null;
            self.linear_q4_k_bias_f32_tile4_r2 = null;
            self.linear_q4_k_bias_quick_gelu_f32_tile4 = null;
            self.linear_q4_k_bias_relu_f32_tile4 = null;
            self.linear_q4_k_bias_relu_f32_tile4_r2 = null;
            self.linear_q4_k_bias_add_f32_tile4 = null;
            self.linear_q4_k_f32_tile8 = null;
            self.linear_q4_k_triple_bias_f32 = null;
            self.linear_q4_k_triple_bias_f32_tiled = null;
            self.linear_q4_k_pair_bias_f32_tiled = null;
            self.linear_q8_0_pair_nobias_f32_tile4 = null;
            self.linear_q4_k_pair_nobias_f32_tile4 = null;
            self.linear_f32_qkv_nobias_tiled = null;
            self.linear_bf16_weight_f32_qkv_nobias_tiled = null;
            self.linear_q8_0_qkv_nobias_f32_tile4 = null;
            self.linear_q4_k_qkv_nobias_f32_tiled = null;
            self.linear_q4_k_q4_k_f32_qkv_nobias_tiled = null;
            self.embedding_lookup_q8_0_f32 = null;
            self.embedding_lookup_i32_q8_0_f32 = null;
            self.embedding_lookup_q4_0_f32 = null;
            self.embedding_lookup_i32_q4_0_f32 = null;
            self.embedding_lookup_q4_k_f32 = null;
            self.embedding_lookup_i32_q4_k_f32 = null;
            self.slice_last_dim_f32 = null;
        }
    }

    pub fn hasGemma4DecoderPrimitives(self: *const KernelModule) bool {
        return self.rope_f32 != null and
            self.rope_per_item_f32 != null and
            self.gqa_attention_f32 != null and
            self.add_mul_scalar_f32 != null and
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
            self.slice_last_dim_f32 != null;
    }

    pub fn requireGemma4DecoderPrimitives(self: *const KernelModule) driver_mod.Error!void {
        if (!self.hasGemma4DecoderPrimitives()) return error.CudaKernelUnavailable;
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

        const function = self.rms_norm_add_output_scale_f32 orelse return error.CudaKernelUnavailable;
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
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
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
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
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
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
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
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
        };
        try launch1d(self.embedding_lookup_q4_k_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupQ4_0F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
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
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
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
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
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
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
        };
        try launch1d(self.embedding_lookup_i32_q4_k_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupI32Q4_0F32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
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
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
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
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&weight_ptr),
            @ptrCast(&ids_ptr),
            @ptrCast(&total_u32),
            @ptrCast(&dim_u32),
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
        try checkRawBytes(decode_scalars, 4 * @sizeOf(u32));
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
        const function = self.gqa_attention_decode_scalars_f32 orelse return error.CudaKernelUnavailable;
        if (head_dim > 512) return error.CudaKernelUnavailable;
        const q_hidden = try checkedTensorElements(num_heads, head_dim);
        const kv_hidden = try checkedTensorElements(num_kv_heads, head_dim);
        const q_count = try checkedTensorElements(try checkedTensorElements(batch, q_seq_len), q_hidden);
        const kv_count = try checkedTensorElements(try checkedTensorElements(batch, kv_seq_len), kv_hidden);
        try checkBytes(dst, q_count);
        try checkBytes(q, q_count);
        try checkBytes(k, kv_count);
        try checkBytes(v, kv_count);
        try checkRawBytes(decode_scalars, 4 * @sizeOf(u32));
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
        if (captureParamTraceIndex(ctx)) |trace_index| {
            std.log.info("cuda_capture_param_trace: capture={d} index={d} kernel=gqa_attention_decode_scalars dst=0x{x} q=0x{x} k=0x{x} v=0x{x} scalars=0x{x} fallback_query_position_offset={d} fallback_kv_seq_len={d} fallback_total_sequence_len={d}", .{
                ctx.debug_graph_capture_id,
                trace_index,
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
        return .decode;
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
        try checkRawBytes(decode_scalars, 4 * @sizeOf(u32));
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
};

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
const q8_0_tiled_threads: usize = 256;
const q8_0_col_tile: usize = 4;
const q4_0_values_per_block: usize = 32;
const q4_0_block_bytes: usize = 18;
const q4_0_col_tile: usize = 4;
const q4_k_values_per_block: usize = 256;
const q4_k_block_bytes: usize = 144;
const q4_k_tiled_threads: usize = 256;
const q4_k_col_tile: usize = 4;
const q4_k_row_tile: usize = 2;
const f32_tiled_threads: usize = 256;
const f32_col_tile: usize = 4;
const f32_row_tile: usize = 2;

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
    try module.launchEmbeddingLookupBf16WeightF32(ctx, embed_output, embed_weight, ids, ids_data.len, embed_dim);
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
    try module.launchEmbeddingLookupF32(ctx, embed, weight, ids, ids_data.len, 2);
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
    try module.launchEmbeddingLookupI32F32(ctx, embed_i32, weight, ids_i32, ids_i32_data.len, 2);
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

    try module.launchEmbeddingLookupQ8_0F32(&ctx, embed_out, weight, ids64, ids_i64.len, in_dim);
    try ctx.synchronize();
    try embed_out.copyToHost(&ctx, std.mem.sliceAsBytes(embed_actual));
    try ctx.synchronize();
    try expectApproxSlice(embed_actual, embed_expected, 0.0001);

    try module.launchEmbeddingLookupI32Q8_0F32(&ctx, embed_out, weight, ids32, ids_i32.len, in_dim);
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
    try module.launchEmbeddingLookupQ4KF32(&ctx, embed_out, weight, ids, ids_data.len, in_dim);
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
        if (@abs(actual[i] - want) > tolerance) return error.CudaSmokeMismatch;
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
