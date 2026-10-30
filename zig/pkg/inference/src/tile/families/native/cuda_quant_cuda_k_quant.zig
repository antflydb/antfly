// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    termite_q4k_tile_rows_cols<{{literal_0}}, {{literal_1}}, {{literal_2}}>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
;

pub const source_0 = program.join(&.{
    \\extern "C" __global__ void termite_linear_q4_k_f32_tile4(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    termite_q4k_tile_cols<4u, 0u>(dst, input, weight, nullptr, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q6_k_f32_tile4(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\#define TERMITE_K_TILE_INPUT(index) input[index]
    \\#define TERMITE_K_TILE_BYTES 210u
    \\#define TERMITE_K_TILE_VALUE termite_q6k_value
    \\#include "../kernels/quant_k_tile4_partials.cuh"
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_gated_down_f32_tile4(ANTFLY_CUDA_QUANT_GATED_DOWN_PARAMS) {
    \\#define TERMITE_K_TILE_INPUT(index) termite_decoder_activation_f32(gate[index], activation) * up[index]
    \\#define TERMITE_K_TILE_BYTES 144u
    \\#define TERMITE_K_TILE_VALUE termite_q4k_value
    \\#include "../kernels/quant_k_tile4_partials.cuh"
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q6_k_gated_down_f32_tile4(ANTFLY_CUDA_QUANT_GATED_DOWN_PARAMS) {
    \\#define TERMITE_K_TILE_INPUT(index) termite_decoder_activation_f32(gate[index], activation) * up[index]
    \\#define TERMITE_K_TILE_BYTES 210u
    \\#define TERMITE_K_TILE_VALUE termite_q6k_value
    \\#include "../kernels/quant_k_tile4_partials.cuh"
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_f32_tile8(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    termite_q4k_tile_cols<8u, 0u>(dst, input, weight, nullptr, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_f32_tile4(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    termite_q4k_tile_cols<4u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_bias_f32_tile4_r2", .literal_0 = "2u", .literal_1 = "4u", .literal_2 = "1u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_bias_gelu_f32_tile4_r2", .literal_0 = "2u", .literal_1 = "4u", .literal_2 = "2u" }),

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_add_f32_tile4_r2(ANTFLY_CUDA_QUANT_LINEAR_RESIDUAL_PARAMS) {
    \\    termite_q4k_tile_rows_cols<2u, 4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_f32_fast_r2c8(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<true, 2u, 8u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_gelu_f32_fast_r2c8(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<true, 2u, 8u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_add_f32_fast_r2c8(ANTFLY_CUDA_QUANT_LINEAR_RESIDUAL_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<true, 2u, 8u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_f32_fast_r4c4(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<true, 4u, 4u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_gelu_f32_fast_r4c4(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<true, 4u, 4u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_add_f32_fast_r4c4(ANTFLY_CUDA_QUANT_LINEAR_RESIDUAL_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<true, 4u, 4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_span_bias_f32_tile8_r2", .literal_0 = "2u", .literal_1 = "8u", .literal_2 = "1u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_span_bias_relu_f32_tile8_r2", .literal_0 = "2u", .literal_1 = "8u", .literal_2 = "4u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_span_bias_f32_tile4_r8", .literal_0 = "8u", .literal_1 = "4u", .literal_2 = "1u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_span_bias_relu_f32_tile4_r8", .literal_0 = "8u", .literal_1 = "4u", .literal_2 = "4u" }),

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_span_pair_bias_f32_tile8_r2(ANTFLY_CUDA_LINEAR_Q4_K_PAIR_BIAS_F32_TILED_PARAMS) {
    \\    termite_q4k_pair_tile_rows_cols<2u, 8u, false, false>(dst_a, dst_b, input, nullptr, weight_a, bias_a, weight_b, bias_b, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_span_pair_bias_relu_f32_tile8_r2(
    \\    float* dst_a,
    \\    float* dst_b,
    \\    const float* input,
    \\    const unsigned char* weight_a,
    \\    const float* bias_a,
    \\    const unsigned char* weight_b,
    \\    const float* bias_b,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    termite_q4k_pair_tile_rows_cols<2u, 8u, true, false>(dst_a, dst_b, input, nullptr, weight_a, bias_a, weight_b, bias_b, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_span_pair2_bias_f32_tile8_r2(
    \\    float* dst_a,
    \\    float* dst_b,
    \\    const float* input_a,
    \\    const float* input_b,
    \\    const unsigned char* weight_a,
    \\    const float* bias_a,
    \\    const unsigned char* weight_b,
    \\    const float* bias_b,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    termite_q4k_pair_tile_rows_cols<2u, 8u, false, true>(dst_a, dst_b, input_a, input_b, weight_a, bias_a, weight_b, bias_b, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_quick_gelu_f32_tile4(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    termite_q4k_tile_cols<4u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_relu_f32_tile4(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    termite_q4k_tile_cols<4u, 4u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_bias_relu_f32_tile4_r2", .literal_0 = "2u", .literal_1 = "4u", .literal_2 = "4u" }),

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_add_f32_tile4(ANTFLY_CUDA_QUANT_LINEAR_RESIDUAL_PARAMS) {
    \\    termite_q4k_tile_cols<4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_f32(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (idx >= total) return;
    \\    unsigned int row = idx / out_dim;
    \\    unsigned int col = idx - row * out_dim;
    \\    unsigned int row_blocks = in_dim / 256u;
    \\    float acc = 0.0f;
    \\    for (unsigned int block = 0; block < row_blocks; ++block) {
    \\        const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
    \\        for (unsigned int i = 0; i < 256u; ++i) {
    \\            acc += input[row * in_dim + block * 256u + i] * termite_q4k_value(bp, i);
    \\        }
    \\    }
    \\    dst[idx] = acc;
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_f32(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (idx >= total) return;
    \\    unsigned int row = idx / out_dim;
    \\    unsigned int col = idx - row * out_dim;
    \\    unsigned int row_blocks = in_dim / 256u;
    \\    float acc = bias[col];
    \\    for (unsigned int block = 0; block < row_blocks; ++block) {
    \\        const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
    \\        for (unsigned int i = 0; i < 256u; ++i) {
    \\            acc += input[row * in_dim + block * 256u + i] * termite_q4k_value(bp, i);
    \\        }
    \\    }
    \\    dst[idx] = acc;
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_f32_tiled(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    unsigned int out_idx = blockIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (out_idx >= total) return;
    \\    unsigned int row = out_idx / out_dim;
    \\    unsigned int col = out_idx - row * out_dim;
    \\#include "../kernels/q4_k_tiled_dot.cuh"
    \\    if (tid == 0u) dst[out_idx] = partial[0];
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_f32_tiled(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    unsigned int out_idx = blockIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (out_idx >= total) return;
    \\    unsigned int row = out_idx / out_dim;
    \\    unsigned int col = out_idx - row * out_dim;
    \\#include "../kernels/q4_k_tiled_dot.cuh"
    \\    if (tid == 0u) dst[out_idx] = partial[0] + bias[col];
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_quick_gelu_f32_tiled(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    unsigned int out_idx = blockIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (out_idx >= total) return;
    \\    unsigned int row = out_idx / out_dim;
    \\    unsigned int col = out_idx - row * out_dim;
    \\#include "../kernels/q4_k_tiled_dot.cuh"
    \\    if (tid == 0u) {
    \\        float x = partial[0] + bias[col];
    \\        dst[out_idx] = x / (1.0f + expf(-1.702f * x));
    \\    }
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_triple_bias_f32(ANTFLY_CUDA_LINEAR_Q4_K_TRIPLE_BIAS_F32_PARAMS) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int plane = rows * out_dim;
    \\    unsigned int total = plane * 3u;
    \\    if (idx >= total) return;
    \\    unsigned int projection = idx / plane;
    \\    unsigned int local = idx - projection * plane;
    \\    unsigned int row = local / out_dim;
    \\    unsigned int col = local - row * out_dim;
    \\    const unsigned char* weight = projection == 0u ? weight_a : (projection == 1u ? weight_b : weight_c);
    \\    const float* bias = projection == 0u ? bias_a : (projection == 1u ? bias_b : bias_c);
    \\    float* dst = projection == 0u ? dst_a : (projection == 1u ? dst_b : dst_c);
    \\    unsigned int row_blocks = in_dim / 256u;
    \\    float acc = bias[col];
    \\    for (unsigned int block = 0; block < row_blocks; ++block) {
    \\        const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
    \\        for (unsigned int i = 0; i < 256u; ++i) {
    \\            acc += input[row * in_dim + block * 256u + i] * termite_q4k_value(bp, i);
    \\        }
    \\    }
    \\    dst[local] = acc;
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_triple_bias_f32_tiled(ANTFLY_CUDA_LINEAR_Q4_K_TRIPLE_BIAS_F32_PARAMS) {
    \\    unsigned int plane = rows * out_dim;
    \\    unsigned int global = blockIdx.x;
    \\    if (global >= plane * 3u) return;
    \\    unsigned int projection = global / plane;
    \\    unsigned int local = global - projection * plane;
    \\    unsigned int row = local / out_dim;
    \\    unsigned int col = local - row * out_dim;
    \\    const unsigned char* weight = projection == 0u ? weight_a : (projection == 1u ? weight_b : weight_c);
    \\    const float* bias = projection == 0u ? bias_a : (projection == 1u ? bias_b : bias_c);
    \\    float* dst = projection == 0u ? dst_a : (projection == 1u ? dst_b : dst_c);
    \\#include "../kernels/q4_k_tiled_dot.cuh"
    \\    if (tid == 0u) dst[local] = partial[0] + bias[col];
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_pair_bias_f32_tiled(ANTFLY_CUDA_LINEAR_Q4_K_PAIR_BIAS_F32_TILED_PARAMS) {
    \\    unsigned int plane = rows * out_dim;
    \\    unsigned int global = blockIdx.x;
    \\    if (global >= plane * 2u) return;
    \\    unsigned int projection = global / plane;
    \\    unsigned int local = global - projection * plane;
    \\    unsigned int row = local / out_dim;
    \\    unsigned int col = local - row * out_dim;
    \\    const unsigned char* weight = projection == 0u ? weight_a : weight_b;
    \\    const float* bias = projection == 0u ? bias_a : bias_b;
    \\    float* dst = projection == 0u ? dst_a : dst_b;
    \\#include "../kernels/q4_k_tiled_dot.cuh"
    \\    if (tid == 0u) dst[local] = partial[0] + bias[col];
    \\}
    \\
});
