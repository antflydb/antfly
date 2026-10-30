// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_linear_q8_0_pair_nobias_f32_tile4(ANTFLY_CUDA_QUANT_PAIR_PARAMS) {
    \\    const unsigned int cols = 4u;
    \\    unsigned int tiles = (out_dim + cols - 1u) / cols;
    \\    unsigned int tiles_per_row = tiles * 2u;
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int row = global / tiles_per_row;
    \\    if (row >= rows) return;
    \\    unsigned int tile_local = global - row * tiles_per_row;
    \\    unsigned int projection = tile_local / tiles;
    \\    unsigned int col_tile = (tile_local - projection * tiles) * cols;
    \\    const unsigned char* weight = projection == 0u ? weight_a : weight_b;
    \\    float* dst = projection == 0u ? dst_a : dst_b;
    \\#include "../kernels/q8_projection_tile4.cuh"
    \\}
    \\
;

pub const source_1 =
    \\extern "C" __global__ void termite_linear_q4_k_pair_nobias_f32_tile4(ANTFLY_CUDA_QUANT_PAIR_PARAMS) {
    \\    const unsigned int cols = 4u;
    \\    unsigned int tiles = (out_dim + cols - 1u) / cols;
    \\    unsigned int tiles_per_row = tiles * 2u;
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int row = global / tiles_per_row;
    \\    if (row >= rows) return;
    \\    unsigned int tile_local = global - row * tiles_per_row;
    \\    unsigned int projection = tile_local / tiles;
    \\    unsigned int col_tile = (tile_local - projection * tiles) * cols;
    \\    const unsigned char* weight = projection == 0u ? weight_a : weight_b;
    \\    float* dst = projection == 0u ? dst_a : dst_b;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int row_blocks = in_dim / 256u;
    \\    __shared__ float partial[4][256];
    \\    float acc[4];
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;
    \\
    \\    if (tid < 256u) {
    \\        for (unsigned int block = 0; block < row_blocks; ++block) {
    \\            float x = input[row * in_dim + block * 256u + tid];
    \\            #pragma unroll
    \\            for (unsigned int c = 0; c < 4u; ++c) {
    \\                unsigned int col = col_tile + c;
    \\                if (col < out_dim) {
    \\                    const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
    \\                    acc[c] += x * termite_q4k_value(bp, tid);
    \\                }
    \\            }
    \\        }
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
    \\    }
    \\    __syncthreads();
    \\    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
    \\        if (tid < stride) {
    \\            #pragma unroll
    \\            for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] += partial[c][tid + stride];
    \\        }
    \\        __syncthreads();
    \\    }
    \\    if (tid == 0u) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) dst[row * out_dim + col] = partial[c][0];
    \\        }
    \\    }
    \\}
    \\
    \\extern "C" __global__ void termite_linear_q8_0_argmax_stage1_tile4(ANTFLY_CUDA_QUANT_ARGMAX_PARAMS) {
    \\    const unsigned int cols = 4u;
    \\    unsigned int tile = blockIdx.x;
    \\    unsigned int col_tile = tile * cols;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int row = rows == 0u ? 0u : rows - 1u;
    \\    unsigned int row_blocks = in_dim / 32u;
    \\    __shared__ float partial[4][256];
    \\    float acc[4];
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;
    \\
    \\    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
    \\        float x = input[row * in_dim + i];
    \\        unsigned int block = i / 32u;
    \\        unsigned int lane = i - block * 32u;
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) {
    \\                const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
    \\                acc[c] += x * termite_q8_0_value(bp, lane);
    \\            }
    \\        }
    \\    }
    \\#include "../kernels/quant_argmax_publish.cuh"
    \\}
    \\
    \\extern "C" __global__ void termite_linear_q4_k_argmax_stage1_tile4(ANTFLY_CUDA_QUANT_ARGMAX_PARAMS) {
    \\    const unsigned int cols = 4u;
    \\    unsigned int tile = blockIdx.x;
    \\    unsigned int col_tile = tile * cols;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int row = rows == 0u ? 0u : rows - 1u;
    \\    unsigned int row_blocks = in_dim / 256u;
    \\    __shared__ float partial[4][256];
    \\    float acc[4];
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;
    \\
    \\    for (unsigned int block = 0; block < row_blocks; ++block) {
    \\        float x = input[row * in_dim + block * 256u + tid];
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) {
    \\                const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
    \\                acc[c] += x * termite_q4k_value(bp, tid);
    \\            }
    \\        }
    \\    }
    \\#include "../kernels/quant_argmax_publish.cuh"
    \\}
    \\
;

pub const source_2 =
    \\extern "C" __global__ void termite_linear_q8_0_argmax_rows_stage1_tile4(ANTFLY_CUDA_QUANT_ARGMAX_PARAMS) {
    \\    termite_scalar_projection_argmax_impl<true, 4u>(partial_values, partial_indices, input, weight, suppress_token_ids, rows, in_dim, out_dim, suppress_count);
    \\}
    \\
;

pub const source_3 =
    \\extern "C" __global__ void termite_linear_q4_k_argmax_rows_stage1_tile4(ANTFLY_CUDA_QUANT_ARGMAX_PARAMS) {
    \\    termite_k_projection_argmax_impl<false, 4u>(partial_values, partial_indices, input, weight, suppress_token_ids, rows, in_dim, out_dim, suppress_count);
    \\}
    \\
;
