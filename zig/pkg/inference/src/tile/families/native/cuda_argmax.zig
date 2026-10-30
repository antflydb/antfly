// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

const program_0 =
    \\extern "C" __global__ void {{name}}(
    \\    unsigned int* dst,
    \\    const float* partial_values,
    \\    const unsigned int* partial_indices,
    \\    unsigned int rows,
    \\    unsigned int col_tiles
    \\) {
    \\    termite_argmax_reduce_pairs_impl<{{literal_0}}>(dst, partial_values, partial_indices, rows, col_tiles);
    \\}
    \\
;

pub const source_0 = program.join(&.{
    \\extern "C" __global__ void termite_argmax_last_row_f32(
    \\    unsigned int* dst,
    \\    const float* input,
    \\    unsigned int rows,
    \\    unsigned int dim
    \\) {
    \\    __shared__ float best_values[256];
    \\    __shared__ unsigned int best_indices[256];
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int row = rows == 0u ? 0u : rows - 1u;
    \\    const float* row_ptr = input + row * dim;
    \\#include "../kernels/scalar_argmax_fold.cuh"
    \\    if (tid == 0u) dst[0] = best_indices[0];
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_argmax_rows_f32(
    \\    unsigned int* dst,
    \\    const float* input,
    \\    unsigned int row_start,
    \\    unsigned int row_count,
    \\    unsigned int dim
    \\) {
    \\    __shared__ float best_values[256];
    \\    __shared__ unsigned int best_indices[256];
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int row_local = blockIdx.x;
    \\    if (row_local >= row_count) return;
    \\    const float* row_ptr = input + (row_start + row_local) * dim;
    \\#include "../kernels/scalar_argmax_fold.cuh"
    \\    if (tid == 0u) dst[row_local] = best_indices[0];
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_argmax_rows_suppress_f32(
    \\    unsigned int* dst,
    \\    const float* input,
    \\    const int* suppress_token_ids,
    \\    unsigned int row_start,
    \\    unsigned int row_count,
    \\    unsigned int dim,
    \\    unsigned int suppress_count
    \\) {
    \\    __shared__ float best_values[256];
    \\    __shared__ unsigned int best_indices[256];
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int row_local = blockIdx.x;
    \\    if (row_local >= row_count) return;
    \\    const float* row_ptr = input + (row_start + row_local) * dim;
    \\#define TERMITE_ARGMAX_SUPPRESS
    \\#include "../kernels/scalar_argmax_fold.cuh"
    \\    if (tid == 0u) dst[row_local] = best_indices[0];
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_argmax_last_row_suppress_f32(
    \\    unsigned int* dst,
    \\    const float* input,
    \\    const int* suppress_token_ids,
    \\    unsigned int rows,
    \\    unsigned int dim,
    \\    unsigned int suppress_count
    \\) {
    \\    __shared__ float best_values[256];
    \\    __shared__ unsigned int best_indices[256];
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int row = rows == 0u ? 0u : rows - 1u;
    \\    const float* row_ptr = input + row * dim;
    \\#define TERMITE_ARGMAX_SUPPRESS
    \\#include "../kernels/scalar_argmax_fold.cuh"
    \\    if (tid == 0u) dst[0] = best_indices[0];
    \\}
    \\
});

pub const source_1 = program.join(&.{
    program.render(program_0, .{ .name = "termite_argmax_reduce_rows_pairs_f32", .literal_0 = "8u" }),

    \\
    \\
    ,
    program.render(program_0, .{ .name = "termite_argmax_reduce_rows_pairs_f32_w16", .literal_0 = "16u" }),

    \\
    \\
    ,

    \\extern "C" __global__ void termite_argmax_reduce_pairs_f32(
    \\    unsigned int* dst,
    \\    const float* partial_values,
    \\    const unsigned int* partial_indices,
    \\    unsigned int count
    \\) {
    \\    __shared__ float warp_best_values[8];
    \\    __shared__ unsigned int warp_best_indices[8];
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int lane = tid & 31u;
    \\    unsigned int warp = tid >> 5;
    \\    float best_value = -3.402823466e+38f;
    \\    unsigned int best_index = 0xffffffffu;
    \\    for (unsigned int i = tid; i < count; i += blockDim.x) {
    \\        unsigned int index = partial_indices[i];
    \\        if (index == 0xffffffffu) continue;
    \\        float value = partial_values[i];
    \\        if (value > best_value || (value == best_value && index < best_index)) {
    \\            best_value = value;
    \\            best_index = index;
    \\        }
    \\    }
    \\    for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
    \\        float other_value = __shfl_down_sync(0xffffffffu, best_value, offset);
    \\        unsigned int other_index = __shfl_down_sync(0xffffffffu, best_index, offset);
    \\        if (other_index != 0xffffffffu &&
    \\            (other_value > best_value || (other_value == best_value && other_index < best_index))) {
    \\            best_value = other_value;
    \\            best_index = other_index;
    \\        }
    \\    }
    \\    if (lane == 0u) {
    \\        warp_best_values[warp] = best_value;
    \\        warp_best_indices[warp] = best_index;
    \\    }
    \\    __syncthreads();
    \\    if (tid == 0u) {
    \\        float final_value = -3.402823466e+38f;
    \\        unsigned int final_index = 0xffffffffu;
    \\        unsigned int warp_count = blockDim.x >> 5;
    \\        #pragma unroll
    \\        for (unsigned int w = 0u; w < 8u; ++w) {
    \\            if (w >= warp_count) continue;
    \\            float other_value = warp_best_values[w];
    \\            unsigned int other_index = warp_best_indices[w];
    \\            if (other_index != 0xffffffffu &&
    \\                (other_value > final_value || (other_value == final_value && other_index < final_index))) {
    \\                final_value = other_value;
    \\                final_index = other_index;
    \\            }
    \\        }
    \\        dst[0] = final_index == 0xffffffffu ? 0u : final_index;
    \\    }
    \\}
    \\
});
