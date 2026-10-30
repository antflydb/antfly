// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

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
