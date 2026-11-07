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
    \\    const unsigned short* weight,
    \\    const int* ids,
    \\    unsigned int total,
    \\    unsigned int dim,
    \\    float scale
    \\) {
    \\    termite_embedding_dense_impl<{{literal_0}}>(dst, weight, ids, total, dim, scale);
    \\}
    \\
;

pub const source_0 = program.join(&.{
    \\extern "C" __global__ void termite_embedding_lookup_f32(
    \\    float* dst,
    \\    const float* weight,
    \\    const long long* ids,
    \\    unsigned int total,
    \\    unsigned int dim,
    \\    float scale
    \\) {
    \\    termite_embedding_dense_impl<0u>(dst, weight, ids, total, dim, scale);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_embedding_lookup_bf16_weight_f32(ANTFLY_CUDA_EMBEDDING_LOOKUP_BF16_WEIGHT_F32_PARAMS) {
    \\    termite_embedding_dense_impl<1u>(dst, weight, ids, total, dim, scale);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_embedding_lookup_f16_weight_f32(ANTFLY_CUDA_EMBEDDING_LOOKUP_BF16_WEIGHT_F32_PARAMS) {
    \\    termite_embedding_dense_impl<2u>(dst, weight, ids, total, dim, scale);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_embedding_lookup_i32_f32(
    \\    float* dst,
    \\    const float* weight,
    \\    const int* ids,
    \\    unsigned int total,
    \\    unsigned int dim,
    \\    float scale
    \\) {
    \\    termite_embedding_dense_impl<0u>(dst, weight, ids, total, dim, scale);
    \\}
    \\
    ,

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_embedding_lookup_i32_bf16_weight_f32", .literal_0 = "1u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_embedding_lookup_i32_f16_weight_f32", .literal_0 = "2u" }),
});
