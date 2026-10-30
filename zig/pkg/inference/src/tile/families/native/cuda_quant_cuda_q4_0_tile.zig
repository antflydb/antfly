// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_Q8_INPUT_LINEAR_PARAMS) {
    \\    unsigned int col_tile = blockIdx.x * 4u;
    \\    unsigned int row = blockIdx.y;
    \\    if (row >= rows) return;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int lane = tid & 31u;
    \\    unsigned int warp = tid >> 5;
    \\    unsigned int row_blocks = in_dim / 32u;
    \\    __shared__ float warp_partial[4][{{literal_5}}];
    \\    float acc[4];
    \\    bool full_tile = col_tile + 3u < out_dim;
    \\    termite_q4_0_q8_1_cols_partial<4u>(acc, q8_input, weight, row, row_blocks, out_dim, col_tile, full_tile, tid);
    \\    termite_store_q4_0_cols_warp_sum_warps<4u, {{literal_10}}>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
    \\}
    \\
;

const program_1 =
    \\extern "C" __global__ void {{name}}(
    \\    float* dst,
    \\    const unsigned char* q8_input,
    \\    const unsigned char* weight
    \\) {
    \\    const unsigned int cols = 4u;
    \\    const unsigned int row_blocks = {{literal_1}};
    \\    unsigned int col_tile = blockIdx.x * cols;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int lane = tid & 31u;
    \\    unsigned int warp = tid >> 5;
    \\    __shared__ float warp_partial[4][{{literal_5}}];
    \\    float acc[4];
    \\    termite_q4_0_q8_1_cols_partial<4u>(acc, q8_input, weight, 0u, row_blocks, 0u, col_tile, true, tid);
    \\    termite_store_q4_0_cols_warp_sum_warps<4u, {{literal_12}}, true, true>(dst, 0u, 0u, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
    \\}
    \\
;

const program_2 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_LINEAR_Q4_0_ACTIVATION_SLICE_LAST_DIM_F32_TILE4_PARAMS) {
    \\    unsigned int col_tile = blockIdx.x * 4u;
    \\    unsigned int row = blockIdx.y;
    \\    if (row >= rows) return;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int lane = tid & 31u;
    \\    unsigned int warp = tid >> 5;
    \\    __shared__ float warp_partial[4][{{literal_4}}];
    \\    termite_q4_0_activation_slice_partials<{{literal_5}}>(input, weight, in_dim, out_dim, row, col_tile, tid, lane, warp, warp_partial);
    \\    __syncthreads();
    \\    if (tid == 0u) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) {
    \\                float y = 0.0f;
    \\                #pragma unroll
    \\                for (unsigned int w = 0; w < {{literal_10}}; ++w) y += warp_partial[c][w];
    \\                dst[row * out_dim + col] =
    \\                    termite_decoder_activation_f32(y, activation) *
    \\                    source[row * source_cols + source_start + col];
    \\            }
    \\        }
    \\    }
    \\}
    \\
;

const program_3 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_QUANT_GATED_DOWN_PARAMS) {
    \\    termite_q4_0_tile_cols<{{literal_0}}, {{literal_1}}, true>(dst, gate, weight, rows, in_dim, out_dim, up, activation);
    \\}
    \\
;

pub const source_0 = program.join(&.{
    \\extern "C" __global__ void termite_linear_q4_0_f32_tile4(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    termite_q4_0_tile_cols<4u>(dst, input, weight, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_0_f32_tile4_w4(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    termite_q4_0_tile_cols<4u, 4u>(dst, input, weight, rows, in_dim, out_dim);
    \\}
    \\
});

pub const source_1 = program.join(&.{
    program.render(program_0, .{ .name = "termite_linear_q4_0_q8_1_f32_tile4", .literal_5 = "4", .literal_10 = "4u" }),

    \\
    \\
    ,
    program.render(program_1, .{ .name = "termite_linear_q4_0_q8_1_f32_tile4_e4b_attn_2048", .literal_1 = "64u", .literal_5 = "4", .literal_12 = "4u" }),

    \\
    \\
    ,
    program.render(program_1, .{ .name = "termite_linear_q4_0_q8_1_f32_tile4_e4b_attn_4096", .literal_1 = "128u", .literal_5 = "4", .literal_12 = "4u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_0_q8_1_f32_tile4_w8", .literal_5 = "8", .literal_10 = "8u" }),
});

pub const source_2 = program.join(&.{
    \\extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w8_rows4(ANTFLY_CUDA_Q8_INPUT_LINEAR_PARAMS) {
    \\    termite_q4_0_q8_1_rows_impl<4u>(dst, q8_input, weight, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w8_rows8_c4(ANTFLY_CUDA_Q8_INPUT_LINEAR_PARAMS) {
    \\    termite_q4_0_q8_1_rows_impl<8u>(dst, q8_input, weight, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,
    program.render(program_1, .{ .name = "termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down", .literal_1 = "320u", .literal_5 = "8", .literal_12 = "8u" }),
});

pub const source_3 = program.render(program_1, .{ .name = "termite_linear_q4_0_q8_1_f32_tile4_w10_e4b_down", .literal_1 = "320u", .literal_5 = "10", .literal_12 = "10u" });

pub const source_4 =
    \\extern "C" __global__ void termite_linear_q4_0_f32_tile8(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    termite_q4_0_tile_cols<8u>(dst, input, weight, rows, in_dim, out_dim);
    \\}
    \\
;

pub const source_5 = program.join(&.{
    program.render(program_2, .{ .name = "termite_linear_q4_0_activation_slice_last_dim_f32_tile4", .literal_4 = "8", .literal_5 = "8u", .literal_10 = "8u" }),

    \\
    \\
    ,
    program.render(program_2, .{ .name = "termite_linear_q4_0_activation_slice_last_dim_f32_tile4_w4", .literal_4 = "4", .literal_5 = "4u", .literal_10 = "4u" }),
});

pub const source_6 = program.join(&.{
    program.render(program_3, .{ .name = "termite_linear_q4_0_gated_down_f32_tile4", .literal_0 = "4u", .literal_1 = "8u" }),

    \\
    \\
    ,
    program.render(program_3, .{ .name = "termite_linear_q4_0_gated_down_f32_tile4_w4", .literal_0 = "4u", .literal_1 = "4u" }),

    \\
    \\
    ,
    program.render(program_3, .{ .name = "termite_linear_q4_0_gated_down_f32_tile8", .literal_0 = "8u", .literal_1 = "8u" }),

    \\
    \\
    ,
    program.render(program_3, .{ .name = "termite_linear_q4_0_gated_down_f32_tile16", .literal_0 = "16u", .literal_1 = "8u" }),
});
