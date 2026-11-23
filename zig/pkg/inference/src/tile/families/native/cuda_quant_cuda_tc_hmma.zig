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
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_PACKED_LINEAR_BIAS_PARAMS) {
    \\    termite_qtc_hmma_tile<{{literal_0}}, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
;

pub const source_0 = program.join(&.{
    \\extern "C" __global__ void termite_linear_q8_0_f32_tc_hmma(
    \\    float* dst,
    \\    const float* input,
    \\    const unsigned char* packed_weight,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    termite_qtc_hmma_tile<0u, TERMITE_QTC_FMT_Q8_0>(dst, input, packed_weight, nullptr, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q8_0_bias_f32_tc_hmma(ANTFLY_CUDA_PACKED_LINEAR_BIAS_PARAMS) {
    \\    termite_qtc_hmma_tile<1u, TERMITE_QTC_FMT_Q8_0>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q8_0_bias_gelu_f32_tc_hmma(ANTFLY_CUDA_PACKED_LINEAR_BIAS_PARAMS) {
    \\    termite_qtc_hmma_tile<2u, TERMITE_QTC_FMT_Q8_0>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q8_0_bias_add_f32_tc_hmma(
    \\    float* dst,
    \\    const float* input,
    \\    const unsigned char* packed_weight,
    \\    const float* bias,
    \\    const float* residual,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    termite_qtc_hmma_tile<3u, TERMITE_QTC_FMT_Q8_0>(dst, input, packed_weight, bias, residual, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_f32_tc_hmma(
    \\    float* dst,
    \\    const float* input,
    \\    const unsigned char* packed_weight,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    termite_qtc_hmma_tile<0u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, nullptr, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_bias_f32_tc_hmma", .literal_0 = "1u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_bias_gelu_f32_tc_hmma", .literal_0 = "2u" }),

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_bias_add_f32_tc_hmma(
    \\    float* dst,
    \\    const float* input,
    \\    const unsigned char* packed_weight,
    \\    const float* bias,
    \\    const float* residual,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    termite_qtc_hmma_tile<3u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, residual, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_bias_quick_gelu_f32_tc_hmma", .literal_0 = "4u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_linear_q4_k_bias_relu_f32_tc_hmma", .literal_0 = "5u" }),

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_triple_bias_f32_tc_hmma(ANTFLY_CUDA_LINEAR_Q4_K_TRIPLE_BIAS_F32_TC_HMMA_PARAMS) {
    \\    unsigned int projection = blockIdx.z;
    \\    float* dst = projection == 0u ? dst_a : (projection == 1u ? dst_b : dst_c);
    \\    const unsigned char* packed_weight = projection == 0u ? packed_weight_a : (projection == 1u ? packed_weight_b : packed_weight_c);
    \\    const float* bias = projection == 0u ? bias_a : (projection == 1u ? bias_b : bias_c);
    \\    termite_qtc_hmma_tile<1u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_k_pair_bias_f32_tc_hmma(
    \\    float* dst_a,
    \\    float* dst_b,
    \\    const float* input,
    \\    const unsigned char* packed_weight_a,
    \\    const float* bias_a,
    \\    const unsigned char* packed_weight_b,
    \\    const float* bias_b,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    unsigned int projection = blockIdx.z;
    \\    float* dst = projection == 0u ? dst_a : dst_b;
    \\    const unsigned char* packed_weight = projection == 0u ? packed_weight_a : packed_weight_b;
    \\    const float* bias = projection == 0u ? bias_a : bias_b;
    \\    termite_qtc_hmma_tile<1u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_0_f32_tc_hmma(
    \\    float* dst,
    \\    const float* input,
    \\    const unsigned char* packed_weight,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    termite_qtc_hmma_tile<0u, TERMITE_QTC_FMT_Q4_0>(dst, input, packed_weight, nullptr, nullptr, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\
    \\
    ,

    \\
    \\
    ,
});

pub const source_1 =
    \\extern "C" __global__ void termite_linear_q4_0_f32_tc_hmma_bf16(
    \\    float* dst,
    \\    const float* input,
    \\    const unsigned char* packed_weight,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\#if __CUDA_ARCH__ >= 800
    \\    termite_qtc_hmma_tile<0u, TERMITE_QTC_FMT_Q4_0, __nv_bfloat16>(dst, input, packed_weight, nullptr, nullptr, rows, in_dim, out_dim);
    \\#endif
    \\}
    \\
;
