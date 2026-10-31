// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Qualified native program instances and specialization exports.

const cuda_phases = @import("cuda_phases.zig");

const program = @import("../../native_program.zig");

const program_0 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_QUANT_PAIR_PARAMS) {
    \\    const unsigned int cols = {{literal_0}};
    \\    unsigned int tiles = (out_dim + cols - 1u) / cols;
    \\    unsigned int tiles_per_row = tiles * 2u;
    \\{{pair_projection_tile}}
    \\    termite_q4_0_tile_cols_at<{{literal_5}}, 8u>(dst, input, weight, rows, in_dim, out_dim, row, col_tile);
    \\}
    \\
;

const program_1 =
    \\extern "C" __global__ void {{name}}(
    \\    float* dst,
    \\    const unsigned char* q8_input,
    \\    const unsigned char* weight_gate,
    \\    const unsigned char* weight_up,
    \\    unsigned int activation
    \\) {
    \\    termite_e4b_ffn_pair_impl<{{literal_0}}>(dst, q8_input, weight_gate, weight_up, activation);
    \\}
    \\
;

const program_2 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_QUANT_ARGMAX_PARAMS) {
    \\    termite_k_projection_argmax_impl<true, {{literal_1}}>(partial_values, partial_indices, input, weight, suppress_token_ids, rows, in_dim, out_dim, suppress_count);
    \\}
    \\
;

pub const source_0 = program.render(program_0, .{ .name = "termite_linear_q4_0_pair_nobias_f32_tile4", .literal_0 = "4u", .literal_5 = "4u", .pair_projection_tile = cuda_phases.projection.pair_projection_tile });

pub const source_1 = program.join(&.{
    \\extern "C" __global__ void termite_linear_q4_0_pair_nobias_q8_1_f32_tile4(ANTFLY_CUDA_Q8_INPUT_PAIR_PARAMS) {
    \\    termite_q4_0_pair_q8_1_tile4_impl<4u>(dst_a, dst_b, q8_input, weight_a, weight_b, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_0_pair_nobias_q8_1_f32_tile4_w8(ANTFLY_CUDA_Q8_INPUT_PAIR_PARAMS) {
    \\    termite_q4_0_pair_q8_1_tile4_impl<8u>(dst_a, dst_b, q8_input, weight_a, weight_b, rows, in_dim, out_dim);
    \\}
    \\
});

pub const source_2 = program.join(&.{
    program.render(program_1, .{ .name = "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn", .literal_0 = "8u" }),

    \\
    \\
    ,
    program.render(program_1, .{ .name = "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn", .literal_0 = "5u" }),
});

pub const source_3 = program.render(program_0, .{ .name = "termite_linear_q4_0_pair_nobias_f32_tile8", .literal_0 = "8u", .literal_5 = "8u", .pair_projection_tile = cuda_phases.projection.pair_projection_tile });

pub const source_4 = program.join(&.{
    \\extern "C" __global__ void termite_linear_q4_0_argmax_rows_stage1_tile4(ANTFLY_CUDA_QUANT_ARGMAX_PARAMS) {
    \\    termite_scalar_projection_argmax_impl<false, 4u>(partial_values, partial_indices, input, weight, suppress_token_ids, rows, in_dim, out_dim, suppress_count);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_0_argmax_rows_stage1_tile16(ANTFLY_CUDA_QUANT_ARGMAX_PARAMS) {
    \\    termite_scalar_projection_argmax_impl<false, 16u>(partial_values, partial_indices, input, weight, suppress_token_ids, rows, in_dim, out_dim, suppress_count);
    \\}
    \\
});

pub const source_5 = program.join(&.{
    program.render(program_2, .{ .name = "termite_linear_q6_k_argmax_rows_stage1_tile4", .literal_1 = "4u" }),

    \\
    \\
    ,
    program.render(program_2, .{ .name = "termite_linear_q6_k_argmax_rows_stage1_tile8", .literal_1 = "8u" }),

    \\
    \\
    ,
    program.render(program_2, .{ .name = "termite_linear_q6_k_argmax_rows_stage1_tile16", .literal_1 = "16u" }),
});

pub const source_6 =
    \\extern "C" __global__ void termite_linear_q4_0_qkv_nobias_f32_tile4(ANTFLY_CUDA_QUANT_QKV_PARAMS) {
    \\    termite_q4_0_qkv_projection_impl<4u, 8u>(dst_q, dst_k, dst_v, input, weight_q, weight_k, weight_v, rows, in_dim, q_out_dim, kv_out_dim);
    \\}
    \\
;

pub const source_7 =
    \\extern "C" __global__ void termite_linear_q4_0_qkv_nobias_f32_tile8(ANTFLY_CUDA_QUANT_QKV_PARAMS) {
    \\    termite_q4_0_qkv_projection_impl<8u, 8u>(dst_q, dst_k, dst_v, input, weight_q, weight_k, weight_v, rows, in_dim, q_out_dim, kv_out_dim);
    \\}
    \\
;
