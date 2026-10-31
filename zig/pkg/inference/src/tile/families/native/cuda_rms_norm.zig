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

pub const source_0 =
    \\extern "C" __global__ void termite_rms_norm_f32(ANTFLY_CUDA_RMS_NORM_PARAMS) {
    \\    termite_rms_norm_impl<true, false, false, 0>(dst, nullptr, input, weight, nullptr, nullptr, rows, dim, eps);
    \\}
    \\
    \\extern "C" __global__ void termite_rms_norm_f32_bf16(ANTFLY_CUDA_RMS_NORM_F32_BF16_PARAMS) {
    \\    termite_rms_norm_impl<true, false, true, 0>(dst, dst_bf16, input, weight, nullptr, nullptr, rows, dim, eps);
    \\}
    \\
    \\extern "C" __global__ void termite_rms_norm_add_f32(ANTFLY_CUDA_RMS_NORM_ADD_PARAMS) {
    \\    termite_rms_norm_impl<true, true, false, 0>(dst, nullptr, input, weight, residual, nullptr, rows, dim, eps);
    \\}
    \\
    \\extern "C" __global__ void termite_rms_norm_add_f32_bf16(ANTFLY_CUDA_RMS_NORM_ADD_F32_BF16_PARAMS) {
    \\    termite_rms_norm_impl<true, true, true, 0>(dst, dst_bf16, input, weight, residual, nullptr, rows, dim, eps);
    \\}
    \\
    \\extern "C" __global__ void termite_rms_norm_add_mul_scalar_f32(ANTFLY_CUDA_RMS_NORM_ADD_MUL_SCALAR_PARAMS) {
    \\    termite_rms_norm_impl<true, true, false, 1>(dst, nullptr, input, weight, residual, scalar, rows, dim, eps);
    \\}
    \\
    \\extern "C" __global__ void termite_rms_norm_add_output_scale_f32(ANTFLY_CUDA_RMS_NORM_ADD_MUL_SCALAR_PARAMS) {
    \\    termite_rms_norm_impl<true, true, false, 2>(dst, nullptr, input, weight, residual, scalar, rows, dim, eps);
    \\}
    \\
    \\extern "C" __global__ void termite_rms_norm_bare_f32(ANTFLY_CUDA_RMS_NORM_BARE_PARAMS) {
    \\    termite_rms_norm_impl<false, false, false, 0>(dst, nullptr, input, nullptr, nullptr, nullptr, rows, dim, eps);
    \\}
    \\
;

pub const source_1 =
    \\extern "C" __global__ void termite_gemma4_a4b_rms_norm_triple_f32(ANTFLY_CUDA_GEMMA4_A4B_RMS_NORM_TRIPLE_PARAMS) {
    \\    const unsigned int row = blockIdx.x;
    \\    if (row >= rows) return;
    \\    const unsigned int tid = threadIdx.x;
    \\    const unsigned int base = row * dim;
    \\    __shared__ float partial[256];
    \\    const float scale = antfly_row_rms_tree_inverse(dim, eps, partial,
    \\        [&](unsigned i) { return input[base + i]; });
    \\
    \\    for (unsigned int i = tid; i < dim; i += blockDim.x) {
    \\        const unsigned int idx = base + i;
    \\        const float normalized = input[idx] * scale;
    \\        first[idx] = normalized * first_weight[i];
    \\        second[idx] = normalized * second_weight[i];
    \\        third[idx] = normalized * third_weight[i];
    \\    }
    \\}
    \\
;
