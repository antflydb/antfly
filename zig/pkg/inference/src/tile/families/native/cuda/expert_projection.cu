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

// native-program:entry termite_gemma4_a4b_q4_0_gate_up_rows_q8_1_f32
extern "C" __global__ void termite_gemma4_a4b_q4_0_gate_up_rows_q8_1_f32(ANTFLY_CUDA_GEMMA4_A4B_Q4_0_GATE_UP_ROWS_Q8_1_F32_PARAMS) {
    const unsigned int cols = 4u;
    const unsigned int tiles = (out_dim + cols - 1u) / cols;
    const unsigned int route = blockIdx.x / tiles;
    const unsigned int route_count = rows * top_k;
    if (route >= route_count) return;
    const unsigned int row = route / top_k;
    const unsigned int expert = route_ids[route];
    const unsigned int col_tile = (blockIdx.x - route * tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned char* weight_gate = gate_base + (size_t)expert * gate_expert_stride;
    const unsigned char* weight_up = up_base + (size_t)expert * up_expert_stride;
    __shared__ float gate_partial[4][8];
    __shared__ float up_partial[4][8];
    float gate_acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};
    float up_acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};
    const unsigned int row_blocks = in_dim / 32u;
    const unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + ((size_t)row * row_blocks + block) * 36u;
#include "../kernels/q4_a4b_q8_acquire.cuh"
#pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* gate_bp = weight_gate + ((size_t)col * row_blocks + block) * 18u;
                const unsigned char* up_bp = weight_up + ((size_t)col * row_blocks + block) * 18u;
                gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        const float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 8u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
#pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                float gate_y = 0.0f;
                float up_y = 0.0f;
#pragma unroll
                for (unsigned int w = 0u; w < 8u; ++w) {
                    gate_y += gate_partial[c][w];
                    up_y += up_partial[c][w];
                }
                dst[(size_t)route * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
    }
}
// native-program:entry termite_gemma4_a4b_q4_0_down_rows_q8_1_f32
extern "C" __global__ void termite_gemma4_a4b_q4_0_down_rows_q8_1_f32(ANTFLY_CUDA_GEMMA4_A4B_Q4_0_DOWN_ROWS_Q8_1_F32_PARAMS) {
    const unsigned int cols = 8u;
    const unsigned int col_tile = blockIdx.x * cols;
    const unsigned int route = blockIdx.y;
    const unsigned int route_count = rows * top_k;
    if (route >= route_count) return;
    const unsigned int expert = route_ids[route];
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int row_blocks = in_dim / 32u;
    const unsigned char* weight = down_base + (size_t)expert * down_expert_stride;
    __shared__ float warp_partial[8][4];
    float acc[8] = {0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
    const unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_activated + ((size_t)route * row_blocks + block) * 36u;
#include "../kernels/q4_a4b_q8_acquire.cuh"
#pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + ((size_t)col * row_blocks + block) * 18u;
                acc[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<8u, 4u>(expert_output, route, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}
// native-program:entry termite_gemma4_a4b_q4_0_down_rows_q8_1_f32_compact
extern "C" __global__ void termite_gemma4_a4b_q4_0_down_rows_q8_1_f32_compact(ANTFLY_CUDA_GEMMA4_A4B_Q4_0_DOWN_ROWS_Q8_1_F32_PARAMS) {
    if (rows != 1u || top_k != 8u || in_dim != 704u || out_dim != 2816u || blockDim.x != 64u) return;
    const unsigned int cols = 8u;
    const unsigned int col_tile = blockIdx.x * cols;
    const unsigned int route = blockIdx.y;
    if (route >= 8u) return;
    const unsigned int expert = route_ids[route];
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned char* weight = down_base + (size_t)expert * down_expert_stride;
    __shared__ float warp_partial[8][2];
    float acc[8] = {0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f, 0.0f};
    const unsigned int iqs = (tid & 1u) * 2u;
    const unsigned int block = tid >> 1u;
    if (block < 22u) {
        const unsigned char* q8_bp = q8_activated + ((size_t)route * 22u + block) * 36u;
#include "../kernels/q4_a4b_q8_acquire.cuh"
#pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned char* bp = weight + ((size_t)(col_tile + c) * 22u + block) * 18u;
            acc[c] = termite_q4_0_q8_1_partial_mmvq2(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }
    termite_store_q4_0_cols_warp_sum_warps<8u, 2u>(
        expert_output,
        route,
        2816u,
        col_tile,
        acc,
        &warp_partial[0][0],
        tid,
        lane,
        warp
    );
}
// native-program:entry termite_gemma4_a4b_combine_rows_f32
extern "C" __global__ void termite_gemma4_a4b_combine_rows_f32(
    float* dst,
    const float* expert_output,
    const float* route_weights,
    unsigned int rows,
    unsigned int top_k,
    unsigned int dim
) {
    const size_t index = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    const size_t count = (size_t)rows * dim;
    if (index >= count) return;
    const unsigned int row = (unsigned int)(index / dim);
    const unsigned int col = (unsigned int)(index - (size_t)row * dim);
    float value = 0.0f;
    for (unsigned int slot = 0u; slot < top_k; ++slot) {
        const size_t route = (size_t)row * top_k + slot;
        value = __fadd_rn(value, __fmul_rn(route_weights[route], expert_output[route * dim + col]));
    }
    dst[index] = value;
}
