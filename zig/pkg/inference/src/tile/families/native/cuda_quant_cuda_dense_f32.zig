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
    \\extern "C" __global__ void termite_linear_f32(ANTFLY_CUDA_LINEAR_F32_PARAMS) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (idx >= total) return;
    \\    unsigned int row = idx / out_dim;
    \\    unsigned int col = idx - row * out_dim;
    \\    float acc = 0.0f;
    \\    for (unsigned int k = 0; k < in_dim; ++k) {
    \\        acc += input[row * in_dim + k] * weight[col * in_dim + k];
    \\    }
    \\    dst[idx] = acc;
    \\}
    \\
    \\extern "C" __global__ void termite_linear_f32_tiled(ANTFLY_CUDA_LINEAR_F32_PARAMS) {
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (global >= total) return;
    \\    unsigned int row = global / out_dim;
    \\    unsigned int col = global - row * out_dim;
    \\    unsigned int tid = threadIdx.x;
    \\    __shared__ float partial[256];
    \\    float acc = 0.0f;
    \\    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
    \\        acc += input[row * in_dim + i] * weight[col * in_dim + i];
    \\    }
    \\    partial[tid] = acc;
    \\    __syncthreads();
    \\    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
    \\        if (tid < stride) partial[tid] += partial[tid + stride];
    \\        __syncthreads();
    \\    }
    \\    if (tid == 0u) dst[global] = partial[0];
    \\}
    \\
;

pub const source_1 =
    \\extern "C" __global__ void termite_linear_bf16_weight_f32_tiled(ANTFLY_CUDA_LINEAR_BF16_WEIGHT_F32_TILED_PARAMS) {
    \\    termite_linear_u16_weight_f32_tiled_impl<true>(dst, input, weight, rows, in_dim, out_dim);
    \\}
    \\
;

pub const source_2 =
    \\extern "C" __global__ void termite_linear_f16_weight_f32_tiled(ANTFLY_CUDA_LINEAR_BF16_WEIGHT_F32_TILED_PARAMS) {
    \\    termite_linear_u16_weight_f32_tiled_impl<false>(dst, input, weight, rows, in_dim, out_dim);
    \\}
    \\
;

pub const source_3 =
    \\extern "C" __global__ void termite_linear_bias_f32(
    \\    float* dst,
    \\    const float* input,
    \\    const float* weight,
    \\    const float* bias,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim
    \\) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (idx >= total) return;
    \\    unsigned int row = idx / out_dim;
    \\    unsigned int col = idx - row * out_dim;
    \\    float acc = bias[col];
    \\    for (unsigned int k = 0; k < in_dim; ++k) {
    \\        acc += input[row * in_dim + k] * weight[col * in_dim + k];
    \\    }
    \\    dst[idx] = acc;
    \\}
    \\
;
