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
const Dot = @import("antfly_tile").native_projection.Dot;
const dots = .{ .shared_tree = @import("antfly_tile").native_statistics.Tree.native(.{ .integer = "unsigned int", .lane = "tid", .threads = "blockDim.x", .barrier = "__syncthreads();", .planes = &.{.{ .partial = "partial", .value = "acc" }} }), .serial = (Dot{ .integer = "unsigned int", .index = "k", .end = "in_dim", .left = "input[row * in_dim + k]", .right = "weight[col * in_dim + k]" }).native(), .parallel = (Dot{ .integer = "unsigned int", .index = "i", .begin = "tid", .end = "in_dim", .step = "blockDim.x", .left = "input[row * in_dim + i]", .right = "weight[col * in_dim + i]" }).native() };

pub const source_0 = program.render(
    \\extern "C" __global__ void termite_linear_f32(ANTFLY_CUDA_LINEAR_F32_PARAMS) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int total = rows * out_dim;
    \\    if (idx >= total) return;
    \\    unsigned int row = idx / out_dim;
    \\    unsigned int col = idx - row * out_dim;
    \\    float acc = 0.0f;
    \\{{serial}}
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
    \\{{parallel}}
    \\{{shared_tree}}
    \\    if (tid == 0u) dst[global] = partial[0];
    \\}
    \\
, dots);

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

pub const source_3 = program.render(
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
    \\{{serial}}
    \\    dst[idx] = acc;
    \\}
    \\
, dots);
