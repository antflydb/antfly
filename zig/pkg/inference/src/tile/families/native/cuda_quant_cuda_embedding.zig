// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_EMBEDDING_LOOKUP_Q4_K_F32_PARAMS) {
    \\    termite_embedding_quant_impl<{{literal_0}}>(dst, weight, ids, total, dim, scale);
    \\}
    \\
;

const program_1 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_EMBEDDING_LOOKUP_I32_Q4_K_F32_PARAMS) {
    \\    termite_embedding_quant_impl<{{literal_0}}>(dst, weight, ids, total, dim, scale);
    \\}
    \\
;

pub const source_0 = program.join(&.{
    program.render(program_0, .{ .name = "termite_embedding_lookup_q4_k_f32", .literal_0 = "0u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_embedding_lookup_q6_k_f32", .literal_0 = "1u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_embedding_lookup_q8_0_f32", .literal_0 = "2u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_embedding_lookup_q4_0_f32", .literal_0 = "3u" }),

    \\
    \\
    ,
    program.render(program_1, .{ .name = "termite_embedding_lookup_i32_q4_k_f32", .literal_0 = "0u" }),

    \\
    \\
    ,
    program.render(program_1, .{ .name = "termite_embedding_lookup_i32_q6_k_f32", .literal_0 = "1u" }),

    \\
    \\
    ,

    \\extern "C" __global__ void termite_embedding_add_weighted_i32_q6_k_f32(
    \\    float* dst,
    \\    const unsigned char* weight,
    \\    const int* ids,
    \\    const float* rhs,
    \\    unsigned int total,
    \\    unsigned int dim,
    \\    float lhs_scale,
    \\    float rhs_scale
    \\) {
    \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    \\    unsigned int count = total * dim;
    \\    if (idx >= count) return;
    \\    unsigned int out_row = idx / dim;
    \\    unsigned int col = idx - out_row * dim;
    \\    unsigned long long src_row = (unsigned long long)((unsigned int)ids[out_row]);
    \\    unsigned int row_blocks = dim / 256u;
    \\    unsigned int block = col / 256u;
    \\    unsigned int value_index = col - block * 256u;
    \\    const unsigned char* bp = weight + (src_row * row_blocks + block) * 210ull;
    \\    dst[idx] = termite_q6k_value(bp, value_index) * lhs_scale + rhs[idx] * rhs_scale;
    \\}
    \\
});

pub const source_1 = program.join(&.{
    program.render(program_1, .{ .name = "termite_embedding_lookup_i32_q8_0_f32", .literal_0 = "2u" }),

    \\
    \\
    ,
    program.render(program_1, .{ .name = "termite_embedding_lookup_i32_q4_0_f32", .literal_0 = "3u" }),
});
