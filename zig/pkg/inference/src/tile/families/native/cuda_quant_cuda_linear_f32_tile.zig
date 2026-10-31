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
    \\extern "C" __global__ void {{name}}(
    \\    float* dst,
    \\    const float* input,
    \\    const float* weight,
    \\    const float* bias,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    termite_linear_bias_f32_tile_rows_cols<2u, 4u, {{literal_2}}>(dst, input, weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
;

pub const source_0 = program.join(&.{
    program.render(program_0, .{ .name = "termite_linear_bias_f32_tile4_r2", .literal_2 = "0u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_bias_relu_f32_tile4_r2", .literal_2 = "1u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_bias_gelu_f32_tile4_r2", .literal_2 = "2u" }),

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_bias_add_f32_tile4_r2(
    \\    float* dst,
    \\    const float* input,
    \\    const float* weight,
    \\    const float* bias,
    \\    const float* residual,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    termite_linear_bias_f32_tile_rows_cols<2u, 4u, 3u>(dst, input, weight, bias, residual, rows, in_dim, out_dim);
    \\}
    \\
});

pub const source_1 = program.join(&.{
    \\extern "C" __global__ void termite_linear_pair_bias_f32_tile4_r2(ANTFLY_CUDA_LINEAR_PAIR_BIAS_F32_TILE4_R2_PARAMS) {
    \\    float* outputs[] = {dst_a, dst_b};
    \\    const float* weights[] = {weight_a, weight_b};
    \\    const float* biases[] = {bias_a, bias_b};
    \\    termite_linear_grouped_bias_tile4_r2<2>(outputs, input, weights, biases, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_triple_bias_f32_tile4_r2(ANTFLY_CUDA_LINEAR_TRIPLE_BIAS_F32_TILE4_R2_PARAMS) {
    \\    float* outputs[] = {dst_a, dst_b, dst_c};
    \\    const float* weights[] = {weight_a, weight_b, weight_c};
    \\    const float* biases[] = {bias_a, bias_b, bias_c};
    \\    termite_linear_grouped_bias_tile4_r2<3>(outputs, input, weights, biases, rows, in_dim, out_dim);
    \\}
    \\
});
