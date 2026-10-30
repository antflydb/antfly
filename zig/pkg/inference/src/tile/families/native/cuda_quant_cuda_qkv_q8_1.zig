// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\extern "C" __global__ void termite_linear_q4_0_qkv_nobias_q8_1_f32_tile4(ANTFLY_CUDA_Q8_INPUT_QKV_PARAMS) {
    \\    termite_packed_qkv_projection_impl<4u>(dst_q, dst_k, dst_v, q8_input, weight_q, weight_k, weight_v, rows, in_dim, q_out_dim, kv_out_dim);
    \\}
    \\
;

pub const source_1 =
    \\extern "C" __global__ void termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_w8(ANTFLY_CUDA_Q8_INPUT_QKV_PARAMS) {
    \\    const unsigned int cols = 8u;
    \\    unsigned int q_tiles = (q_out_dim + cols - 1u) / cols;
    \\    unsigned int kv_tiles = (kv_out_dim + cols - 1u) / cols;
    \\    unsigned int tiles_per_row = q_tiles + kv_tiles * 2u;
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int row = global / tiles_per_row;
    \\    if (row >= rows) return;
    \\    unsigned int tile = global - row * tiles_per_row;
    \\
    \\    const unsigned char* weight;
    \\    float* dst;
    \\    unsigned int out_dim;
    \\    unsigned int col_tile;
    \\    if (tile < q_tiles) {
    \\        weight = weight_q;
    \\        dst = dst_q;
    \\        out_dim = q_out_dim;
    \\        col_tile = tile * cols;
    \\    } else if (tile < q_tiles + kv_tiles) {
    \\        weight = weight_k;
    \\        dst = dst_k;
    \\        out_dim = kv_out_dim;
    \\        col_tile = (tile - q_tiles) * cols;
    \\    } else {
    \\        weight = weight_v;
    \\        dst = dst_v;
    \\        out_dim = kv_out_dim;
    \\        col_tile = (tile - q_tiles - kv_tiles) * cols;
    \\    }
    \\
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int lane = tid & 31u;
    \\    unsigned int warp = tid >> 5;
    \\    unsigned int row_blocks = in_dim / 32u;
    \\    __shared__ float warp_partial[8][8];
    \\    float acc[8];
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;
    \\
    \\    unsigned int iqs = (tid & 1u) * 2u;
    \\    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
    \\        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
    \\#include "../kernels/q4_a4b_q8_acquire.cuh"
    \\
    \\        #pragma unroll
    \\        for (unsigned int c = 0u; c < cols; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) {
    \\                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
    \\                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
    \\            }
    \\        }
    \\    }
    \\    termite_store_q4_0_cols_warp_sum_warps<8u, 8u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
    \\}
    \\
;
