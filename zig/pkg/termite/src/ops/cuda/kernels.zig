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

const ptx_sm75 = @embedFile("artifacts/termite_cuda_kernels.ptx");
const ptx_sm75_z = ptx_sm75 ++ "\x00";
const ptx_sm61 = @embedFile("artifacts/termite_cuda_kernels_sm61.ptx");
const ptx_sm61_z = ptx_sm61 ++ "\x00";

pub fn embeddedPtxVersion() ?[]const u8 {
    return ptxDirectiveValue(ptx_sm75, ".version ");
}

pub fn embeddedPtxTarget() ?[]const u8 {
    return ptxDirectiveValue(ptx_sm75, ".target ");
}

pub fn embeddedPtxHasSymbol(symbol: []const u8) bool {
    return std.mem.indexOf(u8, ptx_sm75, symbol) != null and std.mem.indexOf(u8, ptx_sm61, symbol) != null;
}

fn ptxDirectiveValue(ptx: []const u8, prefix: []const u8) ?[]const u8 {
    const start = (std.mem.indexOf(u8, ptx, prefix) orelse return null) + prefix.len;
    const end = start + (std.mem.indexOfScalar(u8, ptx[start..], '\n') orelse return null);
    return std.mem.trim(u8, ptx[start..end], " \t\r");
}

fn ptxForDevice(ctx: *const context_mod.CudaContext) [:0]const u8 {
    if (ctx.info.compute_major < 7) return ptx_sm61_z;
    return ptx_sm75_z;
}

pub const KernelModule = struct {
    module: driver_mod.CUmodule = null,
    fill_f32: driver_mod.CUfunction = null,
    linear_f32: driver_mod.CUfunction = null,
    linear_bias_f32: driver_mod.CUfunction = null,
    add_bias_rows_f32: driver_mod.CUfunction = null,
    linear_bias_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_bias_relu_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_bias_gelu_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_bias_add_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_pair_bias_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_triple_bias_f32_tile4_r2: driver_mod.CUfunction = null,
    rms_norm_f32: driver_mod.CUfunction = null,
    layer_norm_f32: driver_mod.CUfunction = null,
    add_layer_norm_f32: driver_mod.CUfunction = null,
    elementwise_f32: driver_mod.CUfunction = null,
    softmax_lastdim_f32: driver_mod.CUfunction = null,
    reduce_lastdim_f32: driver_mod.CUfunction = null,
    broadcast_in_dim_f32: driver_mod.CUfunction = null,
    embedding_lookup_f32: driver_mod.CUfunction = null,
    take_rows_f32: driver_mod.CUfunction = null,
    scatter_add_rows_f32: driver_mod.CUfunction = null,
    transpose2d_f32: driver_mod.CUfunction = null,
    transpose_nd_f32: driver_mod.CUfunction = null,
    rope_f32: driver_mod.CUfunction = null,
    batched_matmul_f32: driver_mod.CUfunction = null,
    argmax_lastdim_f32: driver_mod.CUfunction = null,
    gliner_word_embeddings_f32: driver_mod.CUfunction = null,
    repeat_first_row_f32: driver_mod.CUfunction = null,
    gliner_gru_combine_f32: driver_mod.CUfunction = null,
    concat_lastdim_f32: driver_mod.CUfunction = null,
    conv2d_f32: driver_mod.CUfunction = null,
    attention_f32: driver_mod.CUfunction = null,
    attention_f32_block: driver_mod.CUfunction = null,
    gqa_attention_f32: driver_mod.CUfunction = null,
    deberta_attention_f32: driver_mod.CUfunction = null,
    split_last_dim3_f32: driver_mod.CUfunction = null,
    linear_q8_0_f32: driver_mod.CUfunction = null,
    linear_q4_0_f32: driver_mod.CUfunction = null,
    linear_q4_k_f32: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32: driver_mod.CUfunction = null,
    linear_q4_k_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q4_k_bias_quick_gelu_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_relu_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_bias_relu_f32_tile4_r2: driver_mod.CUfunction = null,
    linear_q4_k_bias_add_f32_tile4: driver_mod.CUfunction = null,
    linear_q4_k_triple_bias_f32: driver_mod.CUfunction = null,
    linear_q4_k_triple_bias_f32_tiled: driver_mod.CUfunction = null,
    linear_q4_k_pair_bias_f32_tiled: driver_mod.CUfunction = null,
    linear_q5_k_f32: driver_mod.CUfunction = null,
    linear_q5_k_bias_f32: driver_mod.CUfunction = null,
    linear_q6_k_f32: driver_mod.CUfunction = null,
    linear_q6_k_bias_f32: driver_mod.CUfunction = null,
    embedding_lookup_q4_k_f32: driver_mod.CUfunction = null,
    embedding_lookup_q5_k_f32: driver_mod.CUfunction = null,
    embedding_lookup_q6_k_f32: driver_mod.CUfunction = null,

    pub fn load(ctx: *context_mod.CudaContext) driver_mod.Error!KernelModule {
        try ctx.makeCurrent();
        var module: driver_mod.CUmodule = null;
        try loadModuleWithJitLog(ctx, &module);
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);

        var fill_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&fill_f32, module, "termite_fill_f32"));
        var linear_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_f32, module, "termite_linear_f32"));
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
        var layer_norm_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&layer_norm_f32, module, "termite_layer_norm_f32"));
        var add_layer_norm_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&add_layer_norm_f32, module, "termite_add_layer_norm_f32"));
        var elementwise_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&elementwise_f32, module, "termite_elementwise_f32"));
        var softmax_lastdim_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&softmax_lastdim_f32, module, "termite_softmax_lastdim_f32"));
        var reduce_lastdim_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&reduce_lastdim_f32, module, "termite_reduce_lastdim_f32"));
        var broadcast_in_dim_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&broadcast_in_dim_f32, module, "termite_broadcast_in_dim_f32"));
        var embedding_lookup_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_f32, module, "termite_embedding_lookup_f32"));
        var take_rows_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&take_rows_f32, module, "termite_take_rows_f32"));
        var scatter_add_rows_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&scatter_add_rows_f32, module, "termite_scatter_add_rows_f32"));
        var transpose2d_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&transpose2d_f32, module, "termite_transpose2d_f32"));
        var transpose_nd_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&transpose_nd_f32, module, "termite_transpose_nd_f32"));
        var rope_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&rope_f32, module, "termite_rope_f32"));
        var batched_matmul_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&batched_matmul_f32, module, "termite_batched_matmul_f32"));
        var argmax_lastdim_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&argmax_lastdim_f32, module, "termite_argmax_lastdim_f32"));
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
        var gqa_attention_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&gqa_attention_f32, module, "termite_gqa_attention_f32"));
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
        var linear_q4_k_triple_bias_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_triple_bias_f32, module, "termite_linear_q4_k_triple_bias_f32"));
        var linear_q4_k_triple_bias_f32_tiled: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_triple_bias_f32_tiled, module, "termite_linear_q4_k_triple_bias_f32_tiled"));
        var linear_q4_k_pair_bias_f32_tiled: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q4_k_pair_bias_f32_tiled, module, "termite_linear_q4_k_pair_bias_f32_tiled"));
        var linear_q5_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q5_k_f32, module, "termite_linear_q5_k_f32"));
        var linear_q5_k_bias_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q5_k_bias_f32, module, "termite_linear_q5_k_bias_f32"));
        var linear_q6_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q6_k_f32, module, "termite_linear_q6_k_f32"));
        var linear_q6_k_bias_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&linear_q6_k_bias_f32, module, "termite_linear_q6_k_bias_f32"));
        var embedding_lookup_q4_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_q4_k_f32, module, "termite_embedding_lookup_q4_k_f32"));
        var embedding_lookup_q5_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_q5_k_f32, module, "termite_embedding_lookup_q5_k_f32"));
        var embedding_lookup_q6_k_f32: driver_mod.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&embedding_lookup_q6_k_f32, module, "termite_embedding_lookup_q6_k_f32"));

        return .{
            .module = module,
            .fill_f32 = fill_f32,
            .linear_f32 = linear_f32,
            .linear_bias_f32 = linear_bias_f32,
            .add_bias_rows_f32 = add_bias_rows_f32,
            .linear_bias_f32_tile4_r2 = linear_bias_f32_tile4_r2,
            .linear_bias_relu_f32_tile4_r2 = linear_bias_relu_f32_tile4_r2,
            .linear_bias_gelu_f32_tile4_r2 = linear_bias_gelu_f32_tile4_r2,
            .linear_bias_add_f32_tile4_r2 = linear_bias_add_f32_tile4_r2,
            .linear_pair_bias_f32_tile4_r2 = linear_pair_bias_f32_tile4_r2,
            .linear_triple_bias_f32_tile4_r2 = linear_triple_bias_f32_tile4_r2,
            .rms_norm_f32 = rms_norm_f32,
            .layer_norm_f32 = layer_norm_f32,
            .add_layer_norm_f32 = add_layer_norm_f32,
            .elementwise_f32 = elementwise_f32,
            .softmax_lastdim_f32 = softmax_lastdim_f32,
            .reduce_lastdim_f32 = reduce_lastdim_f32,
            .broadcast_in_dim_f32 = broadcast_in_dim_f32,
            .embedding_lookup_f32 = embedding_lookup_f32,
            .take_rows_f32 = take_rows_f32,
            .scatter_add_rows_f32 = scatter_add_rows_f32,
            .transpose2d_f32 = transpose2d_f32,
            .transpose_nd_f32 = transpose_nd_f32,
            .rope_f32 = rope_f32,
            .batched_matmul_f32 = batched_matmul_f32,
            .argmax_lastdim_f32 = argmax_lastdim_f32,
            .gliner_word_embeddings_f32 = gliner_word_embeddings_f32,
            .repeat_first_row_f32 = repeat_first_row_f32,
            .gliner_gru_combine_f32 = gliner_gru_combine_f32,
            .concat_lastdim_f32 = concat_lastdim_f32,
            .conv2d_f32 = conv2d_f32,
            .attention_f32 = attention_f32,
            .attention_f32_block = attention_f32_block,
            .gqa_attention_f32 = gqa_attention_f32,
            .deberta_attention_f32 = deberta_attention_f32,
            .split_last_dim3_f32 = split_last_dim3_f32,
            .linear_q8_0_f32 = linear_q8_0_f32,
            .linear_q4_0_f32 = linear_q4_0_f32,
            .linear_q4_k_f32 = linear_q4_k_f32,
            .linear_q4_k_bias_f32 = linear_q4_k_bias_f32,
            .linear_q4_k_f32_tiled = linear_q4_k_f32_tiled,
            .linear_q4_k_bias_f32_tiled = linear_q4_k_bias_f32_tiled,
            .linear_q4_k_bias_quick_gelu_f32_tiled = linear_q4_k_bias_quick_gelu_f32_tiled,
            .linear_q4_k_f32_tile4 = linear_q4_k_f32_tile4,
            .linear_q4_k_bias_f32_tile4 = linear_q4_k_bias_f32_tile4,
            .linear_q4_k_bias_f32_tile4_r2 = linear_q4_k_bias_f32_tile4_r2,
            .linear_q4_k_bias_quick_gelu_f32_tile4 = linear_q4_k_bias_quick_gelu_f32_tile4,
            .linear_q4_k_bias_relu_f32_tile4 = linear_q4_k_bias_relu_f32_tile4,
            .linear_q4_k_bias_relu_f32_tile4_r2 = linear_q4_k_bias_relu_f32_tile4_r2,
            .linear_q4_k_bias_add_f32_tile4 = linear_q4_k_bias_add_f32_tile4,
            .linear_q4_k_triple_bias_f32 = linear_q4_k_triple_bias_f32,
            .linear_q4_k_triple_bias_f32_tiled = linear_q4_k_triple_bias_f32_tiled,
            .linear_q4_k_pair_bias_f32_tiled = linear_q4_k_pair_bias_f32_tiled,
            .linear_q5_k_f32 = linear_q5_k_f32,
            .linear_q5_k_bias_f32 = linear_q5_k_bias_f32,
            .linear_q6_k_f32 = linear_q6_k_f32,
            .linear_q6_k_bias_f32 = linear_q6_k_bias_f32,
            .embedding_lookup_q4_k_f32 = embedding_lookup_q4_k_f32,
            .embedding_lookup_q5_k_f32 = embedding_lookup_q5_k_f32,
            .embedding_lookup_q6_k_f32 = embedding_lookup_q6_k_f32,
        };
    }

    pub fn unload(self: *KernelModule, ctx: *context_mod.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.module = null;
            self.fill_f32 = null;
            self.linear_f32 = null;
            self.linear_bias_f32 = null;
            self.add_bias_rows_f32 = null;
            self.linear_bias_f32_tile4_r2 = null;
            self.linear_bias_relu_f32_tile4_r2 = null;
            self.linear_bias_gelu_f32_tile4_r2 = null;
            self.linear_bias_add_f32_tile4_r2 = null;
            self.linear_pair_bias_f32_tile4_r2 = null;
            self.linear_triple_bias_f32_tile4_r2 = null;
            self.rms_norm_f32 = null;
            self.layer_norm_f32 = null;
            self.add_layer_norm_f32 = null;
            self.elementwise_f32 = null;
            self.softmax_lastdim_f32 = null;
            self.reduce_lastdim_f32 = null;
            self.broadcast_in_dim_f32 = null;
            self.embedding_lookup_f32 = null;
            self.take_rows_f32 = null;
            self.scatter_add_rows_f32 = null;
            self.transpose2d_f32 = null;
            self.transpose_nd_f32 = null;
            self.rope_f32 = null;
            self.batched_matmul_f32 = null;
            self.argmax_lastdim_f32 = null;
            self.gliner_word_embeddings_f32 = null;
            self.repeat_first_row_f32 = null;
            self.gliner_gru_combine_f32 = null;
            self.concat_lastdim_f32 = null;
            self.conv2d_f32 = null;
            self.attention_f32 = null;
            self.attention_f32_block = null;
            self.gqa_attention_f32 = null;
            self.deberta_attention_f32 = null;
            self.split_last_dim3_f32 = null;
            self.linear_q8_0_f32 = null;
            self.linear_q4_0_f32 = null;
            self.linear_q4_k_f32 = null;
            self.linear_q4_k_bias_f32 = null;
            self.linear_q4_k_f32_tiled = null;
            self.linear_q4_k_bias_f32_tiled = null;
            self.linear_q4_k_bias_quick_gelu_f32_tiled = null;
            self.linear_q4_k_f32_tile4 = null;
            self.linear_q4_k_bias_f32_tile4 = null;
            self.linear_q4_k_bias_f32_tile4_r2 = null;
            self.linear_q4_k_bias_quick_gelu_f32_tile4 = null;
            self.linear_q4_k_bias_relu_f32_tile4 = null;
            self.linear_q4_k_bias_relu_f32_tile4_r2 = null;
            self.linear_q4_k_bias_add_f32_tile4 = null;
            self.linear_q4_k_triple_bias_f32 = null;
            self.linear_q4_k_triple_bias_f32_tiled = null;
            self.linear_q4_k_pair_bias_f32_tiled = null;
            self.linear_q5_k_f32 = null;
            self.linear_q5_k_bias_f32 = null;
            self.linear_q6_k_f32 = null;
            self.linear_q6_k_bias_f32 = null;
            self.embedding_lookup_q4_k_f32 = null;
            self.embedding_lookup_q5_k_f32 = null;
            self.embedding_lookup_q6_k_f32 = null;
        }
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

    pub fn launchSoftmaxLastDimF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        rows: usize,
        dim: usize,
        log_mode: bool,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(rows, dim);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var log_mode_u32: u32 = if (log_mode) 1 else 0;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&log_mode_u32),
        };
        try launchBlocks(self.softmax_lastdim_f32, ctx, rows, 256, &params);
    }

    pub fn launchReduceLastDimF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        rows: usize,
        dim: usize,
        op: ReduceOp,
    ) driver_mod.Error!void {
        const input_count = try checkedTensorElements(rows, dim);
        try checkBytes(dst, rows);
        try checkBytes(input, input_count);
        if (input_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var op_u32: u32 = @intFromEnum(op);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
            @ptrCast(&op_u32),
        };
        try launchBlocks(self.reduce_lastdim_f32, ctx, rows, 256, &params);
    }

    pub fn launchBroadcastInDimF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        out_count: usize,
        input_count: usize,
        out_rank: usize,
        in_rank: usize,
        target_shape: buffer_mod.DeviceBuffer,
        input_shape: buffer_mod.DeviceBuffer,
        axes: buffer_mod.DeviceBuffer,
    ) driver_mod.Error!void {
        try checkBytes(dst, out_count);
        try checkBytes(input, input_count);
        try checkRawBytes(target_shape, std.math.mul(usize, out_rank, @sizeOf(u32)) catch return error.InvalidCudaState);
        try checkRawBytes(input_shape, std.math.mul(usize, in_rank, @sizeOf(u32)) catch return error.InvalidCudaState);
        try checkRawBytes(axes, std.math.mul(usize, in_rank, @sizeOf(u32)) catch return error.InvalidCudaState);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var out_count_u32 = try toU32(out_count);
        var out_rank_u32 = try toU32(out_rank);
        var in_rank_u32 = try toU32(in_rank);
        var target_shape_ptr = target_shape.ptr;
        var input_shape_ptr = input_shape.ptr;
        var axes_ptr = axes.ptr;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&out_count_u32),
            @ptrCast(&out_rank_u32),
            @ptrCast(&in_rank_u32),
            @ptrCast(&target_shape_ptr),
            @ptrCast(&input_shape_ptr),
            @ptrCast(&axes_ptr),
        };
        try launch1d(self.broadcast_in_dim_f32, ctx, out_count, &params);
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

    pub fn launchScatterAddRowsF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        row_ids: buffer_mod.DeviceBuffer,
        out_rows: usize,
        rows: usize,
        dim: usize,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(rows, dim);
        try checkBytes(dst, try checkedTensorElements(out_rows, dim));
        try checkBytes(input, count);
        try checkRawBytes(row_ids, rows * @sizeOf(u32));
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var row_ids_ptr = row_ids.ptr;
        var out_rows_u32 = try toU32(out_rows);
        var rows_u32 = try toU32(rows);
        var dim_u32 = try toU32(dim);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&row_ids_ptr),
            @ptrCast(&out_rows_u32),
            @ptrCast(&rows_u32),
            @ptrCast(&dim_u32),
        };
        try launch1d(self.scatter_add_rows_f32, ctx, count, &params);
    }

    pub fn launchTranspose2DF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        rows: usize,
        cols: usize,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(rows, cols);
        try checkBytes(dst, count);
        try checkBytes(input, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var rows_u32 = try toU32(rows);
        var cols_u32 = try toU32(cols);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&rows_u32),
            @ptrCast(&cols_u32),
        };
        try launch1d(self.transpose2d_f32, ctx, count, &params);
    }

    pub fn launchTransposeNDF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        count: usize,
        rank: usize,
        input_shape: buffer_mod.DeviceBuffer,
        input_strides: buffer_mod.DeviceBuffer,
        output_strides: buffer_mod.DeviceBuffer,
        perm: buffer_mod.DeviceBuffer,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        try checkBytes(input, count);
        try checkBytes(input_shape, rank);
        try checkBytes(input_strides, rank);
        try checkBytes(output_strides, rank);
        try checkBytes(perm, rank);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var count_u32 = try toU32(count);
        var rank_u32 = try toU32(rank);
        var input_shape_ptr = input_shape.ptr;
        var input_strides_ptr = input_strides.ptr;
        var output_strides_ptr = output_strides.ptr;
        var perm_ptr = perm.ptr;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&count_u32),
            @ptrCast(&rank_u32),
            @ptrCast(&input_shape_ptr),
            @ptrCast(&input_strides_ptr),
            @ptrCast(&output_strides_ptr),
            @ptrCast(&perm_ptr),
        };
        try launch1d(self.transpose_nd_f32, ctx, count, &params);
    }

    pub fn launchArgMaxLastDimF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        rows: usize,
        dim: usize,
    ) driver_mod.Error!void {
        const count = try checkedTensorElements(rows, dim);
        try checkBytes(dst, rows);
        try checkBytes(input, count);
        if (rows == 0 or dim == 0) return;

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
        try launch1d(self.argmax_lastdim_f32, ctx, rows, &params);
    }

    pub fn launchRopeF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        count: usize,
        seq_len: usize,
        head_dim: usize,
        rope_dim: usize,
        theta: f32,
        freq_scale: f32,
        position_offset: usize,
        consecutive_pairs: bool,
    ) driver_mod.Error!void {
        try checkBytes(dst, count);
        try checkBytes(input, count);
        if (count == 0) return;

        var dst_ptr = dst.ptr;
        var input_ptr = input.ptr;
        var count_u32 = try toU32(count);
        var seq_len_u32 = try toU32(seq_len);
        var head_dim_u32 = try toU32(head_dim);
        var rope_dim_u32 = try toU32(rope_dim);
        var theta_value = theta;
        var freq_scale_value = freq_scale;
        var position_offset_u32 = try toU32(position_offset);
        var consecutive_pairs_u32: u32 = if (consecutive_pairs) 1 else 0;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&input_ptr),
            @ptrCast(&count_u32),
            @ptrCast(&seq_len_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&rope_dim_u32),
            @ptrCast(&theta_value),
            @ptrCast(&freq_scale_value),
            @ptrCast(&position_offset_u32),
            @ptrCast(&consecutive_pairs_u32),
        };
        try launch1d(self.rope_f32, ctx, count, &params);
    }

    pub fn launchBatchedMatmulF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        lhs: buffer_mod.DeviceBuffer,
        rhs: buffer_mod.DeviceBuffer,
        batches: usize,
        m: usize,
        k: usize,
        n: usize,
    ) driver_mod.Error!void {
        const out_count = try checkedTensorElements(try checkedTensorElements(batches, m), n);
        const lhs_count = try checkedTensorElements(try checkedTensorElements(batches, m), k);
        const rhs_count = try checkedTensorElements(try checkedTensorElements(batches, k), n);
        try checkBytes(dst, out_count);
        try checkBytes(lhs, lhs_count);
        try checkBytes(rhs, rhs_count);
        if (out_count == 0) return;

        var dst_ptr = dst.ptr;
        var lhs_ptr = lhs.ptr;
        var rhs_ptr = rhs.ptr;
        var batches_u32 = try toU32(batches);
        var m_u32 = try toU32(m);
        var k_u32 = try toU32(k);
        var n_u32 = try toU32(n);
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&lhs_ptr),
            @ptrCast(&rhs_ptr),
            @ptrCast(&batches_u32),
            @ptrCast(&m_u32),
            @ptrCast(&k_u32),
            @ptrCast(&n_u32),
        };
        try launch1d(self.batched_matmul_f32, ctx, out_count, &params);
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

    pub fn launchEmbeddingLookupQ5KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q5_k_values_per_block != 0) return error.InvalidCudaState;
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
        try launch1d(self.embedding_lookup_q5_k_f32, ctx, count, &params);
    }

    pub fn launchEmbeddingLookupQ6KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        ids: buffer_mod.DeviceBuffer,
        total: usize,
        dim: usize,
    ) driver_mod.Error!void {
        if (dim == 0 or dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
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
        try launch1d(self.embedding_lookup_q6_k_f32, ctx, count, &params);
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
        total_sequence_len: usize,
        query_position_offset: usize,
        kv_position_offset: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        sliding_window: usize,
        has_attn_or_mask: bool,
        bias_mode: u32,
    ) driver_mod.Error!void {
        const q_hidden = try checkedTensorElements(num_heads, head_dim);
        const kv_hidden = try checkedTensorElements(num_kv_heads, head_dim);
        const q_count = try checkedTensorElements(try checkedTensorElements(batch, q_seq_len), q_hidden);
        const kv_count = try checkedTensorElements(try checkedTensorElements(batch, kv_seq_len), kv_hidden);
        try checkBytes(dst, q_count);
        try checkBytes(q, q_count);
        try checkBytes(k, kv_count);
        try checkBytes(v, kv_count);
        if (has_attn_or_mask) try checkRawBytes(attn_or_mask, try checkedTensorElements(total_sequence_len, total_sequence_len));
        if (bias_mode != 0) try checkBytes(bias, try checkedTensorElements(if (bias_mode == 2) batch * num_heads else num_heads, try checkedTensorElements(q_seq_len, kv_seq_len)));
        if (q_count == 0) return;

        var dst_ptr = dst.ptr;
        var q_ptr = q.ptr;
        var k_ptr = k.ptr;
        var v_ptr = v.ptr;
        var attn_or_mask_ptr = attn_or_mask.ptr;
        var bias_ptr = bias.ptr;
        var batch_u32 = try toU32(batch);
        var q_seq_len_u32 = try toU32(q_seq_len);
        var kv_seq_len_u32 = try toU32(kv_seq_len);
        var total_sequence_len_u32 = try toU32(total_sequence_len);
        var query_position_offset_u32 = try toU32(query_position_offset);
        var kv_position_offset_u32 = try toU32(kv_position_offset);
        var num_heads_u32 = try toU32(num_heads);
        var num_kv_heads_u32 = try toU32(num_kv_heads);
        var head_dim_u32 = try toU32(head_dim);
        var sliding_window_u32 = try toU32(sliding_window);
        var has_attn_or_mask_u32: u32 = if (has_attn_or_mask) 1 else 0;
        var bias_mode_u32 = bias_mode;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&q_ptr),
            @ptrCast(&k_ptr),
            @ptrCast(&v_ptr),
            @ptrCast(&attn_or_mask_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&q_seq_len_u32),
            @ptrCast(&kv_seq_len_u32),
            @ptrCast(&total_sequence_len_u32),
            @ptrCast(&query_position_offset_u32),
            @ptrCast(&kv_position_offset_u32),
            @ptrCast(&num_heads_u32),
            @ptrCast(&num_kv_heads_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&sliding_window_u32),
            @ptrCast(&has_attn_or_mask_u32),
            @ptrCast(&bias_mode_u32),
        };
        try launch1d(self.gqa_attention_f32, ctx, q_count, &params);
    }

    pub fn launchGqaAttentionF32Graph(
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
        total_sequence_len: usize,
        query_position_offset: usize,
        kv_position_offset: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        sliding_window: usize,
        has_attn_or_mask: bool,
        bias_mode: u32,
    ) driver_mod.Error!void {
        try ctx.makeCurrent();
        const graph_fns = ctx.driver.graphFns() orelse return error.CudaSymbolMissing;
        try ctx.driver.check(graph_fns.cuStreamBeginCapture(ctx.stream, driver_mod.CU_STREAM_CAPTURE_MODE_RELAXED));
        var graph: driver_mod.CUgraph = null;
        self.launchGqaAttentionF32(ctx, dst, q, k, v, attn_or_mask, bias, batch, q_seq_len, kv_seq_len, total_sequence_len, query_position_offset, kv_position_offset, num_heads, num_kv_heads, head_dim, sliding_window, has_attn_or_mask, bias_mode) catch |err| {
            _ = graph_fns.cuStreamEndCapture(ctx.stream, &graph);
            if (graph != null) _ = graph_fns.cuGraphDestroy(graph);
            return err;
        };
        try ctx.driver.check(graph_fns.cuStreamEndCapture(ctx.stream, &graph));
        defer {
            if (graph != null) _ = graph_fns.cuGraphDestroy(graph);
        }

        var graph_exec: driver_mod.CUgraphExec = null;
        try ctx.driver.check(graph_fns.cuGraphInstantiateWithFlags(&graph_exec, graph, 0));
        defer {
            if (graph_exec != null) _ = graph_fns.cuGraphExecDestroy(graph_exec);
        }
        try ctx.driver.check(graph_fns.cuGraphLaunch(graph_exec, ctx.stream));
    }

    pub const GqaAttentionGraph = struct {
        graph: driver_mod.CUgraph = null,
        graph_exec: driver_mod.CUgraphExec = null,
        kernel_node: driver_mod.CUgraphNode = null,

        pub fn deinit(self: *GqaAttentionGraph, ctx: *context_mod.CudaContext) void {
            if (ctx.driver.graphFns()) |graph_fns| {
                if (self.graph_exec != null) {
                    _ = graph_fns.cuGraphExecDestroy(self.graph_exec);
                    self.graph_exec = null;
                }
                if (self.graph != null) {
                    _ = graph_fns.cuGraphDestroy(self.graph);
                    self.graph = null;
                }
            }
            self.graph_exec = null;
            self.graph = null;
            self.kernel_node = null;
        }
    };

    pub fn captureGqaAttentionF32Graph(
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
        total_sequence_len: usize,
        query_position_offset: usize,
        kv_position_offset: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        sliding_window: usize,
        has_attn_or_mask: bool,
        bias_mode: u32,
    ) driver_mod.Error!GqaAttentionGraph {
        try ctx.makeCurrent();
        const graph_fns = ctx.driver.graphFns() orelse return error.CudaSymbolMissing;
        try ctx.driver.check(graph_fns.cuStreamBeginCapture(ctx.stream, driver_mod.CU_STREAM_CAPTURE_MODE_RELAXED));
        var graph: driver_mod.CUgraph = null;
        self.launchGqaAttentionF32(ctx, dst, q, k, v, attn_or_mask, bias, batch, q_seq_len, kv_seq_len, total_sequence_len, query_position_offset, kv_position_offset, num_heads, num_kv_heads, head_dim, sliding_window, has_attn_or_mask, bias_mode) catch |err| {
            _ = graph_fns.cuStreamEndCapture(ctx.stream, &graph);
            if (graph != null) _ = graph_fns.cuGraphDestroy(graph);
            return err;
        };
        try ctx.driver.check(graph_fns.cuStreamEndCapture(ctx.stream, &graph));
        errdefer {
            if (graph != null) _ = graph_fns.cuGraphDestroy(graph);
        }

        var nodes: [8]driver_mod.CUgraphNode = undefined;
        var node_count: usize = nodes.len;
        try ctx.driver.check(graph_fns.cuGraphGetNodes(graph, &nodes, &node_count));
        var kernel_node: driver_mod.CUgraphNode = null;
        for (nodes[0..node_count]) |node| {
            var node_type: driver_mod.CUgraphNodeType = 0;
            try ctx.driver.check(graph_fns.cuGraphNodeGetType(node, &node_type));
            if (node_type == driver_mod.CU_GRAPH_NODE_TYPE_KERNEL) {
                kernel_node = node;
                break;
            }
        }
        if (kernel_node == null) return error.InvalidCudaState;

        var graph_exec: driver_mod.CUgraphExec = null;
        try ctx.driver.check(graph_fns.cuGraphInstantiateWithFlags(&graph_exec, graph, 0));
        errdefer {
            if (graph_exec != null) _ = graph_fns.cuGraphExecDestroy(graph_exec);
        }
        return .{ .graph = graph, .graph_exec = graph_exec, .kernel_node = kernel_node };
    }

    pub fn launchGqaAttentionF32Captured(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        graph: *GqaAttentionGraph,
        dst: buffer_mod.DeviceBuffer,
        q: buffer_mod.DeviceBuffer,
        k: buffer_mod.DeviceBuffer,
        v: buffer_mod.DeviceBuffer,
        attn_or_mask: buffer_mod.DeviceBuffer,
        bias: buffer_mod.DeviceBuffer,
        batch: usize,
        q_seq_len: usize,
        kv_seq_len: usize,
        total_sequence_len: usize,
        query_position_offset: usize,
        kv_position_offset: usize,
        num_heads: usize,
        num_kv_heads: usize,
        head_dim: usize,
        sliding_window: usize,
        has_attn_or_mask: bool,
        bias_mode: u32,
    ) driver_mod.Error!void {
        const q_hidden = try checkedTensorElements(num_heads, head_dim);
        const kv_hidden = try checkedTensorElements(num_kv_heads, head_dim);
        const q_count = try checkedTensorElements(try checkedTensorElements(batch, q_seq_len), q_hidden);
        const kv_count = try checkedTensorElements(try checkedTensorElements(batch, kv_seq_len), kv_hidden);
        try checkBytes(dst, q_count);
        try checkBytes(q, q_count);
        try checkBytes(k, kv_count);
        try checkBytes(v, kv_count);
        if (has_attn_or_mask) try checkRawBytes(attn_or_mask, try checkedTensorElements(total_sequence_len, total_sequence_len));
        if (bias_mode != 0) try checkBytes(bias, try checkedTensorElements(if (bias_mode == 2) batch * num_heads else num_heads, try checkedTensorElements(q_seq_len, kv_seq_len)));
        if (q_count == 0) return;

        var dst_ptr = dst.ptr;
        var q_ptr = q.ptr;
        var k_ptr = k.ptr;
        var v_ptr = v.ptr;
        var attn_or_mask_ptr = attn_or_mask.ptr;
        var bias_ptr = bias.ptr;
        var batch_u32 = try toU32(batch);
        var q_seq_len_u32 = try toU32(q_seq_len);
        var kv_seq_len_u32 = try toU32(kv_seq_len);
        var total_sequence_len_u32 = try toU32(total_sequence_len);
        var query_position_offset_u32 = try toU32(query_position_offset);
        var kv_position_offset_u32 = try toU32(kv_position_offset);
        var num_heads_u32 = try toU32(num_heads);
        var num_kv_heads_u32 = try toU32(num_kv_heads);
        var head_dim_u32 = try toU32(head_dim);
        var sliding_window_u32 = try toU32(sliding_window);
        var has_attn_or_mask_u32: u32 = if (has_attn_or_mask) 1 else 0;
        var bias_mode_u32 = bias_mode;
        var params = [_]?*anyopaque{
            @ptrCast(&dst_ptr),
            @ptrCast(&q_ptr),
            @ptrCast(&k_ptr),
            @ptrCast(&v_ptr),
            @ptrCast(&attn_or_mask_ptr),
            @ptrCast(&bias_ptr),
            @ptrCast(&batch_u32),
            @ptrCast(&q_seq_len_u32),
            @ptrCast(&kv_seq_len_u32),
            @ptrCast(&total_sequence_len_u32),
            @ptrCast(&query_position_offset_u32),
            @ptrCast(&kv_position_offset_u32),
            @ptrCast(&num_heads_u32),
            @ptrCast(&num_kv_heads_u32),
            @ptrCast(&head_dim_u32),
            @ptrCast(&sliding_window_u32),
            @ptrCast(&has_attn_or_mask_u32),
            @ptrCast(&bias_mode_u32),
        };
        const block: c_uint = 256;
        const grid: c_uint = try toU32((q_count + block - 1) / block);
        const node_params = driver_mod.CUDA_KERNEL_NODE_PARAMS{
            .func = self.gqa_attention_f32,
            .gridDimX = grid,
            .gridDimY = 1,
            .gridDimZ = 1,
            .blockDimX = block,
            .blockDimY = 1,
            .blockDimZ = 1,
            .sharedMemBytes = 0,
            .kernelParams = &params,
            .extra = null,
        };
        try ctx.makeCurrent();
        const graph_fns = ctx.driver.graphFns() orelse return error.CudaSymbolMissing;
        try ctx.driver.check(graph_fns.cuGraphExecKernelNodeSetParams(graph.graph_exec, graph.kernel_node, &node_params));
        try ctx.driver.check(graph_fns.cuGraphLaunch(graph.graph_exec, ctx.stream));
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

    pub fn launchLinearQ5KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
        if (in_dim == 0 or in_dim % q5_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q5_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q5_k_block_bytes));
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
        try launch1d(self.linear_q5_k_f32, ctx, out_count, &params);
    }

    pub fn launchLinearQ5KBiasF32(
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
        if (in_dim == 0 or in_dim % q5_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q5_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(bias, out_dim);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q5_k_block_bytes));
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
        try launch1d(self.linear_q5_k_bias_f32, ctx, out_count, &params);
    }

    pub fn launchLinearQ6KF32(
        self: *KernelModule,
        ctx: *context_mod.CudaContext,
        dst: buffer_mod.DeviceBuffer,
        input: buffer_mod.DeviceBuffer,
        weight_raw: buffer_mod.DeviceBuffer,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) driver_mod.Error!void {
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
        try launch1d(self.linear_q6_k_f32, ctx, out_count, &params);
    }

    pub fn launchLinearQ6KBiasF32(
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
        if (in_dim == 0 or in_dim % q6_k_values_per_block != 0) return error.InvalidCudaState;
        const row_blocks = in_dim / q6_k_values_per_block;
        const out_count = try checkedTensorElements(rows, out_dim);
        try checkBytes(dst, out_count);
        try checkBytes(input, try checkedTensorElements(rows, in_dim));
        try checkBytes(bias, out_dim);
        try checkRawBytes(weight_raw, try checkedTensorElements(try checkedTensorElements(out_dim, row_blocks), q6_k_block_bytes));
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
        try launch1d(self.linear_q6_k_bias_f32, ctx, out_count, &params);
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
    less_than = 8,
    divide = 9,
    exp = 10,
    log = 11,
    sqrt = 12,
    rsqrt = 13,
    abs = 14,
    sin = 15,
    cos = 16,
    erf = 17,
    subtract = 18,

    fn isUnary(self: ElementwiseOp) bool {
        return switch (self) {
            .add, .multiply, .less_than, .divide, .subtract => false,
            .silu, .gelu, .relu, .quick_gelu, .sigmoid, .tanh, .exp, .log, .sqrt, .rsqrt, .abs, .sin, .cos, .erf => true,
        };
    }
};

pub const ReduceOp = enum(u32) {
    sum = 0,
    max = 1,
    mean = 2,
};

const q8_0_values_per_block: usize = 32;
const q8_0_block_bytes: usize = 34;
const q4_0_values_per_block: usize = 32;
const q4_0_block_bytes: usize = 18;
const q4_k_values_per_block: usize = 256;
const q4_k_block_bytes: usize = 144;
const q5_k_values_per_block: usize = 256;
const q5_k_block_bytes: usize = 176;
const q6_k_values_per_block: usize = 256;
const q6_k_block_bytes: usize = 210;
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
}

fn launchRows(function: driver_mod.CUfunction, ctx: *context_mod.CudaContext, rows: usize, params: [*]?*anyopaque) driver_mod.Error!void {
    const grid: c_uint = try toU32(rows);
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(
        function,
        grid,
        1,
        1,
        1,
        1,
        1,
        0,
        ctx.stream,
        params,
        null,
    ));
}

fn launchRowsThreads(function: driver_mod.CUfunction, ctx: *context_mod.CudaContext, rows: usize, threads: usize, params: [*]?*anyopaque) driver_mod.Error!void {
    const grid: c_uint = try toU32(rows);
    const block: c_uint = try toU32(threads);
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
    const ptx = ptxForDevice(ctx);
    const result = ctx.driver.fns.cuModuleLoadDataEx(module, ptx.ptr, options.len, &options, &values);
    if (result == driver_mod.CUDA_SUCCESS) return;

    std.debug.print(
        "cuda jit: module load failed: {s}: {s}\n",
        .{ ctx.driver.errorName(result), ctx.driver.errorString(result) },
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

pub fn smokeDenseF32(allocator: std.mem.Allocator) !void {
    var ctx = try context_mod.CudaContext.initDefault();
    defer ctx.deinit();
    var module = try KernelModule.load(&ctx);
    defer module.unload(&ctx);

    try smokeLinearF32(allocator, &ctx, &module);
    try smokeRmsNormF32(allocator, &ctx, &module);
    try smokeElementwiseF32(allocator, &ctx, &module);
    try smokeSoftmaxLastDimF32(allocator, &ctx, &module);
    try smokeReduceLastDimF32(allocator, &ctx, &module);
    try smokeBroadcastInDimF32(allocator, &ctx, &module);
    try smokeLayerNormF32(allocator, &ctx, &module);
    try smokeEmbeddingConcatConvF32(allocator, &ctx, &module);
    try smokeAttentionF32(allocator, &ctx, &module);
    try smokeGqaAttentionF32(allocator, &ctx, &module);
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

fn smokeElementwiseF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const a_data = [_]f32{ 1.0, -2.0, 0.0, 4.0 };
    const b_data = [_]f32{ 3.0, 5.0, -1.0, 0.5 };
    const expected_add = [_]f32{ 4.0, 3.0, -1.0, 4.5 };
    const expected_mul = [_]f32{ 3.0, -10.0, -0.0, 2.0 };
    const expected_lt = [_]f32{ 1.0, 1.0, 0.0, 0.0 };
    const expected_div = [_]f32{ 0.33333334, -0.4, -0.0, 8.0 };
    var expected_silu: [a_data.len]f32 = undefined;
    for (a_data, 0..) |x, i| expected_silu[i] = x / (1.0 + std.math.exp(-x));
    var expected_exp: [a_data.len]f32 = undefined;
    for (a_data, 0..) |x, i| expected_exp[i] = std.math.exp(x);
    var expected_log: [a_data.len]f32 = undefined;
    for (b_data, 0..) |x, i| expected_log[i] = std.math.log(f32, std.math.e, x);
    var expected_sqrt: [b_data.len]f32 = undefined;
    for (b_data, 0..) |x, i| expected_sqrt[i] = std.math.sqrt(x);
    var expected_rsqrt: [b_data.len]f32 = undefined;
    for (b_data, 0..) |x, i| expected_rsqrt[i] = 1.0 / std.math.sqrt(x);
    var expected_abs: [a_data.len]f32 = undefined;
    for (a_data, 0..) |x, i| expected_abs[i] = @abs(x);
    var expected_sin: [a_data.len]f32 = undefined;
    for (a_data, 0..) |x, i| expected_sin[i] = @sin(x);
    var expected_cos: [a_data.len]f32 = undefined;
    for (a_data, 0..) |x, i| expected_cos[i] = @cos(x);
    var expected_erf: [a_data.len]f32 = undefined;
    for (a_data, 0..) |x, i| expected_erf[i] = erfApprox(x);

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

    try module.launchElementwiseF32(ctx, output, a, b, a_data.len, .less_than);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_lt, 0.0001);

    try module.launchElementwiseF32(ctx, output, a, b, a_data.len, .divide);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_div, 0.0001);

    try module.launchElementwiseF32(ctx, output, a, .{}, a_data.len, .silu);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_silu, 0.01);

    try module.launchElementwiseF32(ctx, output, a, .{}, a_data.len, .exp);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_exp, 0.01);

    try module.launchElementwiseF32(ctx, output, b, .{}, b_data.len, .log);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_log, 0.01);

    try module.launchElementwiseF32(ctx, output, b, .{}, b_data.len, .sqrt);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_sqrt, 0.01);

    try module.launchElementwiseF32(ctx, output, b, .{}, b_data.len, .rsqrt);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_rsqrt, 0.01);

    try module.launchElementwiseF32(ctx, output, a, .{}, a_data.len, .abs);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_abs, 0.0001);

    try module.launchElementwiseF32(ctx, output, a, .{}, a_data.len, .sin);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_sin, 0.01);

    try module.launchElementwiseF32(ctx, output, a, .{}, a_data.len, .cos);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_cos, 0.01);

    try module.launchElementwiseF32(ctx, output, a, .{}, a_data.len, .erf);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_erf, 0.02);
}

fn erfApprox(x: f32) f32 {
    const sign: f32 = if (x < 0) -1.0 else 1.0;
    const ax = @abs(x);
    const t = 1.0 / (1.0 + 0.3275911 * ax);
    const y = 1.0 - (((((1.061405429 * t - 1.453152027) * t + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t) * @exp(-ax * ax);
    return sign * y;
}

fn smokeSoftmaxLastDimF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const dim: usize = 4;
    const input_data = [_]f32{ 1.0, 2.0, 3.0, 4.0, -1.0, 0.0, 1.0, 2.0 };
    var expected_softmax: [input_data.len]f32 = undefined;
    var expected_log_softmax: [input_data.len]f32 = undefined;
    for (0..rows) |row_idx| {
        const row = input_data[row_idx * dim ..][0..dim];
        var max_val: f32 = -std.math.inf(f32);
        for (row) |v| max_val = @max(max_val, v);
        var sum: f32 = 0.0;
        for (row) |v| sum += std.math.exp(v - max_val);
        const log_sum = max_val + std.math.log(f32, std.math.e, sum);
        for (row, 0..) |v, col| {
            const idx = row_idx * dim + col;
            expected_log_softmax[idx] = v - log_sum;
            expected_softmax[idx] = std.math.exp(expected_log_softmax[idx]);
        }
    }

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer output.free(ctx);
    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));

    const out = try allocator.alloc(f32, input_data.len);
    defer allocator.free(out);

    try module.launchSoftmaxLastDimF32(ctx, output, input, rows, dim, false);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_softmax, 0.003);

    try module.launchSoftmaxLastDimF32(ctx, output, input, rows, dim, true);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_log_softmax, 0.003);
}

fn smokeReduceLastDimF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const rows: usize = 2;
    const dim: usize = 4;
    const input_data = [_]f32{ 1.0, 2.0, 3.0, 4.0, -1.0, 0.0, 1.0, 2.0 };
    const expected_sum = [_]f32{ 10.0, 2.0 };
    const expected_max = [_]f32{ 4.0, 2.0 };
    const expected_mean = [_]f32{ 2.5, 0.5 };

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, rows * @sizeOf(f32));
    defer output.free(ctx);
    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));

    const out = try allocator.alloc(f32, rows);
    defer allocator.free(out);

    try module.launchReduceLastDimF32(ctx, output, input, rows, dim, .sum);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_sum, 0.0001);

    try module.launchReduceLastDimF32(ctx, output, input, rows, dim, .max);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_max, 0.0001);

    try module.launchReduceLastDimF32(ctx, output, input, rows, dim, .mean);
    try ctx.synchronize();
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected_mean, 0.0001);
}

fn smokeBroadcastInDimF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const input_data = [_]f32{ 1, 2, 3, 4, 5, 6 };
    const target_shape_data = [_]u32{ 2, 2, 3 };
    const input_shape_data = [_]u32{ 2, 3 };
    const axes_data = [_]u32{ 0, 2 };
    const expected = [_]f32{ 1, 2, 3, 1, 2, 3, 4, 5, 6, 4, 5, 6 };

    var input = try buffer_mod.DeviceBuffer.alloc(ctx, input_data.len * @sizeOf(f32));
    defer input.free(ctx);
    var target_shape = try buffer_mod.DeviceBuffer.alloc(ctx, target_shape_data.len * @sizeOf(u32));
    defer target_shape.free(ctx);
    var input_shape = try buffer_mod.DeviceBuffer.alloc(ctx, input_shape_data.len * @sizeOf(u32));
    defer input_shape.free(ctx);
    var axes = try buffer_mod.DeviceBuffer.alloc(ctx, axes_data.len * @sizeOf(u32));
    defer axes.free(ctx);
    var output = try buffer_mod.DeviceBuffer.alloc(ctx, expected.len * @sizeOf(f32));
    defer output.free(ctx);
    try input.copyFromHost(ctx, std.mem.sliceAsBytes(&input_data));
    try target_shape.copyFromHost(ctx, std.mem.sliceAsBytes(&target_shape_data));
    try input_shape.copyFromHost(ctx, std.mem.sliceAsBytes(&input_shape_data));
    try axes.copyFromHost(ctx, std.mem.sliceAsBytes(&axes_data));

    try module.launchBroadcastInDimF32(ctx, output, input, expected.len, input_data.len, target_shape_data.len, input_shape_data.len, target_shape, input_shape, axes);
    try ctx.synchronize();
    const out = try allocator.alloc(f32, expected.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.0001);

    const scalar_data = [_]f32{7.0};
    const scalar_target_shape = [_]u32{ 2, 3 };
    const expected_scalar = [_]f32{ 7, 7, 7, 7, 7, 7 };
    var scalar = try buffer_mod.DeviceBuffer.alloc(ctx, scalar_data.len * @sizeOf(f32));
    defer scalar.free(ctx);
    var scalar_target = try buffer_mod.DeviceBuffer.alloc(ctx, scalar_target_shape.len * @sizeOf(u32));
    defer scalar_target.free(ctx);
    var scalar_output = try buffer_mod.DeviceBuffer.alloc(ctx, expected_scalar.len * @sizeOf(f32));
    defer scalar_output.free(ctx);
    try scalar.copyFromHost(ctx, std.mem.sliceAsBytes(&scalar_data));
    try scalar_target.copyFromHost(ctx, std.mem.sliceAsBytes(&scalar_target_shape));

    try module.launchBroadcastInDimF32(ctx, scalar_output, scalar, expected_scalar.len, scalar_data.len, scalar_target_shape.len, 0, scalar_target, .{}, .{});
    try ctx.synchronize();
    const scalar_out = try allocator.alloc(f32, expected_scalar.len);
    defer allocator.free(scalar_out);
    try scalar_output.copyToHost(ctx, std.mem.sliceAsBytes(scalar_out));
    try ctx.synchronize();
    try expectApproxSlice(scalar_out, &expected_scalar, 0.0001);
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

fn smokeGqaAttentionF32(allocator: std.mem.Allocator, ctx: *context_mod.CudaContext, module: *KernelModule) !void {
    const batch: usize = 1;
    const q_seq: usize = 2;
    const kv_seq: usize = 2;
    const heads: usize = 2;
    const kv_heads: usize = 1;
    const dim: usize = 2;
    const q_data = [_]f32{ 1, 0, 0, 1, 0, 1, 1, 0 };
    const k_data = [_]f32{ 1, 0, 0, 1 };
    const v_data = [_]f32{ 10, 20, 30, 40 };
    const expected = [_]f32{ 10, 20, 10, 20, 23.395232, 33.395233, 16.604767, 26.604767 };

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
    try module.launchGqaAttentionF32(ctx, output, q, k, v, .{}, .{}, batch, q_seq, kv_seq, q_seq, 0, 0, heads, kv_heads, dim, 0, false, 0);
    try ctx.synchronize();

    const out = try allocator.alloc(f32, q_data.len);
    defer allocator.free(out);
    try output.copyToHost(ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    try expectApproxSlice(out, &expected, 0.001);
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
}

pub fn smokeQ4_0(allocator: std.mem.Allocator) !void {
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
    var weight_raw = [_]u8{0} ** (out_dim * q4_0_block_bytes);
    writeQ4_0SmokeRow(weight_raw[0..18], 1.0, 1);
    writeQ4_0SmokeRow(weight_raw[18..36], 0.5, 2);
    writeQ4_0SmokeRow(weight_raw[36..54], 2.0, -1);

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

    const expected = [_]f32{ 528, 528, -1056, -528, -528, 1056 };
    for (expected, 0..) |want, i| {
        if (@abs(out[i] - want) > 0.01) return error.CudaSmokeMismatch;
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

pub fn smokeQ5_K(allocator: std.mem.Allocator) !void {
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
    var weight_raw = [_]u8{0} ** (out_dim * q5_k_block_bytes);
    writeQ5_KSmokeRow(weight_raw[0..176], 1.0, 1);
    writeQ5_KSmokeRow(weight_raw[176..352], 0.5, 2);
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

    try module.launchLinearQ5KF32(&ctx, output, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    const out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(out);
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    const expected = [_]f32{ 32896, 32896, -32896, -32896 };
    try expectApproxSlice(out, &expected, 0.1);

    try module.launchLinearQ5KBiasF32(&ctx, output, input, weight, bias, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    const expected_bias = [_]f32{ 32896.25, 32895, -32895.75, -32897 };
    try expectApproxSlice(out, &expected_bias, 0.1);

    const ids_data = [_]i64{ 1, 0 };
    var ids = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_data.len * @sizeOf(i64));
    defer ids.free(&ctx);
    var embed_out = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_data.len * in_dim * @sizeOf(f32));
    defer embed_out.free(&ctx);
    try ids.copyFromHost(&ctx, std.mem.sliceAsBytes(&ids_data));
    try module.launchEmbeddingLookupQ5KF32(&ctx, embed_out, weight, ids, ids_data.len, in_dim);
    try ctx.synchronize();
    const embed = try allocator.alloc(f32, ids_data.len * in_dim);
    defer allocator.free(embed);
    try embed_out.copyToHost(&ctx, std.mem.sliceAsBytes(embed));
    try ctx.synchronize();
    for (embed) |value| {
        if (@abs(value - 1.0) > 0.001) return error.CudaSmokeMismatch;
    }
}

pub fn smokeQ6_K(allocator: std.mem.Allocator) !void {
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
    var weight_raw = [_]u8{0} ** (out_dim * q6_k_block_bytes);
    writeQ6_KSmokeRow(weight_raw[0..210], 1.0, 1);
    writeQ6_KSmokeRow(weight_raw[210..420], 0.5, 2);
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

    try module.launchLinearQ6KF32(&ctx, output, input, weight, rows, in_dim, out_dim);
    try ctx.synchronize();
    const out = try allocator.alloc(f32, rows * out_dim);
    defer allocator.free(out);
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    const expected = [_]f32{ 32896, 32896, -32896, -32896 };
    try expectApproxSlice(out, &expected, 0.1);

    try module.launchLinearQ6KBiasF32(&ctx, output, input, weight, bias, rows, in_dim, out_dim);
    try ctx.synchronize();
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(out));
    try ctx.synchronize();
    const expected_bias = [_]f32{ 32896.25, 32895, -32895.75, -32897 };
    try expectApproxSlice(out, &expected_bias, 0.1);

    const ids_data = [_]i64{ 1, 0 };
    var ids = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_data.len * @sizeOf(i64));
    defer ids.free(&ctx);
    var embed_out = try buffer_mod.DeviceBuffer.alloc(&ctx, ids_data.len * in_dim * @sizeOf(f32));
    defer embed_out.free(&ctx);
    try ids.copyFromHost(&ctx, std.mem.sliceAsBytes(&ids_data));
    try module.launchEmbeddingLookupQ6KF32(&ctx, embed_out, weight, ids, ids_data.len, in_dim);
    try ctx.synchronize();
    const embed = try allocator.alloc(f32, ids_data.len * in_dim);
    defer allocator.free(embed);
    try embed_out.copyToHost(&ctx, std.mem.sliceAsBytes(embed));
    try ctx.synchronize();
    for (embed) |value| {
        if (@abs(value - 1.0) > 0.001) return error.CudaSmokeMismatch;
    }
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

fn writeQ5_KSmokeRow(dst: []u8, scale: f32, value: u5) void {
    std.debug.assert(dst.len == q5_k_block_bytes);
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
    const low: u8 = @as(u8, value & 0x0f);
    const packed_byte = low | (low << 4);
    for (0..128) |i| dst[48 + i] = packed_byte;
    if ((@as(u8, value) & 0x10) != 0) {
        for (0..32) |i| dst[16 + i] = 0xff;
    }
}

fn writeQ6_KSmokeRow(dst: []u8, scale: f32, value: i6) void {
    std.debug.assert(dst.len == q6_k_block_bytes);
    @memset(dst, 0);
    const scale_bits: u16 = @bitCast(@as(f16, @floatCast(scale)));
    dst[208] = @truncate(scale_bits);
    dst[209] = @truncate(scale_bits >> 8);
    for (0..16) |sub| dst[192 + sub] = @bitCast(@as(i8, 1));

    const q_u: u8 = @intCast(@as(i16, value) + 32);
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
            dst[ql_off + l] |= (q_u & 0x0f) << nibble_shift;
            dst[128 + qh_off + l] |= ((q_u >> 4) & 0x03) << qh_shift;
        }
    }
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

test "embedded cuda ptx artifact contract" {
    try std.testing.expectEqualStrings("9.0", embeddedPtxVersion().?);
    try std.testing.expect(std.mem.startsWith(u8, embeddedPtxTarget().?, "sm_"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_fill_f32"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_linear_q8_0_f32"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_linear_q4_k_f32"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_linear_q5_k_f32"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_embedding_lookup_q5_k_f32"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_linear_q6_k_f32"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_embedding_lookup_q6_k_f32"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_gqa_attention_f32"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_softmax_lastdim_f32"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_reduce_lastdim_f32"));
    try std.testing.expect(embeddedPtxHasSymbol("termite_broadcast_in_dim_f32"));
}

test "cuda elementwise op metadata includes less-than as binary" {
    try std.testing.expect(!ElementwiseOp.less_than.isUnary());
    try std.testing.expectEqual(@as(u32, 8), @intFromEnum(ElementwiseOp.less_than));
    try std.testing.expect(!ElementwiseOp.divide.isUnary());
    try std.testing.expect(ElementwiseOp.exp.isUnary());
    try std.testing.expect(ElementwiseOp.log.isUnary());
    try std.testing.expect(ElementwiseOp.sqrt.isUnary());
    try std.testing.expect(ElementwiseOp.rsqrt.isUnary());
    try std.testing.expectEqual(@as(u32, 12), @intFromEnum(ElementwiseOp.sqrt));
    try std.testing.expectEqual(@as(u32, 13), @intFromEnum(ElementwiseOp.rsqrt));
    try std.testing.expect(ElementwiseOp.abs.isUnary());
    try std.testing.expectEqual(@as(u32, 14), @intFromEnum(ElementwiseOp.abs));
    try std.testing.expect(ElementwiseOp.sin.isUnary());
    try std.testing.expectEqual(@as(u32, 15), @intFromEnum(ElementwiseOp.sin));
    try std.testing.expect(ElementwiseOp.cos.isUnary());
    try std.testing.expectEqual(@as(u32, 16), @intFromEnum(ElementwiseOp.cos));
    try std.testing.expect(ElementwiseOp.erf.isUnary());
    try std.testing.expectEqual(@as(u32, 17), @intFromEnum(ElementwiseOp.erf));
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

test "cuda q5_k smoke row writer uses gguf block layout" {
    var raw = [_]u8{0} ** q5_k_block_bytes;
    writeQ5_KSmokeRow(&raw, 1.0, 17);
    try std.testing.expectEqual(@as(u8, 0x00), raw[0]);
    try std.testing.expectEqual(@as(u8, 0x3c), raw[1]);
    try std.testing.expectEqual(@as(u8, 0x01), raw[4]);
    try std.testing.expectEqual(@as(u8, 0xff), raw[16]);
    for (raw[48..]) |byte| try std.testing.expectEqual(@as(u8, 0x11), byte);
}

test "cuda q6_k smoke row writer uses gguf block layout" {
    var raw = [_]u8{0} ** q6_k_block_bytes;
    writeQ6_KSmokeRow(&raw, 1.0, 1);
    try std.testing.expectEqual(@as(u8, 0x01), raw[192]);
    try std.testing.expectEqual(@as(u8, 0x00), raw[208]);
    try std.testing.expectEqual(@as(u8, 0x3c), raw[209]);
    try std.testing.expectEqual(@as(u8, 0x11), raw[0]);
    try std.testing.expectEqual(@as(u8, 0xaa), raw[128]);
}
