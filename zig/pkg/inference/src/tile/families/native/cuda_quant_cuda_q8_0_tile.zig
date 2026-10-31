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

const program = @import("../../native_program.zig");

const program_0 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<false, {{literal_1}}, {{literal_2}}, {{literal_3}}>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
;

pub const source_0 = program.join(&.{
    \\extern "C" __global__ void termite_linear_q8_0_f32_tile4_r2(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    termite_q8_0_tile_rows_cols<2u, 4u, 0u>(dst, input, weight, nullptr, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q8_0_bias_f32_tile4_r2(ANTFLY_CUDA_QUANT_LINEAR_RESIDUAL_PARAMS) {
    \\    termite_q8_0_tile_rows_cols<2u, 4u, 1u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q8_0_bias_gelu_f32_tile4_r2(ANTFLY_CUDA_QUANT_LINEAR_BIAS_PARAMS) {
    \\    termite_q8_0_tile_rows_cols<2u, 4u, 2u>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q8_0_bias_add_f32_tile4_r2(ANTFLY_CUDA_QUANT_LINEAR_RESIDUAL_PARAMS) {
    \\    termite_q8_0_tile_rows_cols<2u, 4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q8_0_f32_fast_r2c8(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<false, 2u, 8u, 0u>(dst, input, weight, nullptr, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q8_0_bias_f32_fast_r2c8", .literal_1 = "2u", .literal_2 = "8u", .literal_3 = "1u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q8_0_bias_gelu_f32_fast_r2c8", .literal_1 = "2u", .literal_2 = "8u", .literal_3 = "2u" }),

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q8_0_bias_add_f32_fast_r2c8(ANTFLY_CUDA_QUANT_LINEAR_RESIDUAL_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<false, 2u, 8u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q8_0_f32_fast_r4c4(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<false, 4u, 4u, 0u>(dst, input, weight, nullptr, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q8_0_bias_f32_fast_r4c4", .literal_1 = "4u", .literal_2 = "4u", .literal_3 = "1u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q8_0_bias_gelu_f32_fast_r4c4", .literal_1 = "4u", .literal_2 = "4u", .literal_3 = "2u" }),

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q8_0_bias_add_f32_fast_r4c4(ANTFLY_CUDA_QUANT_LINEAR_RESIDUAL_PARAMS) {
    \\    termite_quant_tile_rows_cols_fast<false, 4u, 4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
    \\}
    \\
});

pub const source_1 =
    \\extern "C" __global__ void termite_linear_q8_0_f32_tile4(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    \\    termite_q8_0_tile_cols<4u>(dst, input, weight, rows, in_dim, out_dim);
    \\}
    \\
;
