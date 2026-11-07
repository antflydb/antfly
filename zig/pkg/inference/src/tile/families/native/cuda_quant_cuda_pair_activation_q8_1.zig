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
const cuda_phases = @import("cuda_phases.zig");

pub const source_0 = program.render(pair_activation_program, cuda_phases.projection) ++
    \\extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile4(ANTFLY_CUDA_Q8_INPUT_PAIR_ACTIVATION_PARAMS) {
    \\    termite_q4_0_pair_activation_q8_1_tile4_impl<4u>(dst, q8_input, weight_gate, weight_up, rows, in_dim, out_dim, activation);
    \\}
    \\
    \\extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8(ANTFLY_CUDA_Q8_INPUT_PAIR_ACTIVATION_PARAMS) {
    \\    termite_q4_0_pair_activation_q8_1_tile4_impl<8u>(dst, q8_input, weight_gate, weight_up, rows, in_dim, out_dim, activation);
    \\}
    \\
;

// Static native helper owned alongside its specialization exports.
const pair_activation_program =
    \\template<unsigned int Warps>
    \\__device__ __forceinline__ void termite_q4_0_pair_activation_q8_1_tile4_impl(
    \\    float* dst,
    \\    const unsigned char* q8_input,
    \\    const unsigned char* weight_gate,
    \\    const unsigned char* weight_up,
    \\    unsigned int rows,
    \\    unsigned int in_dim,
    \\    unsigned int out_dim,
    \\    unsigned int activation
    \\) {
    \\    const unsigned int cols = 4u;
    \\    unsigned int tiles = (out_dim + cols - 1u) / cols;
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int row = global / tiles;
    \\    if (row >= rows) return;
    \\    unsigned int col_tile = (global - row * tiles) * cols;
    \\    unsigned int tid = threadIdx.x;
    \\    unsigned int lane = tid & 31u;
    \\    unsigned int warp = tid >> 5;
    \\    unsigned int row_blocks = in_dim / 32u;
    \\    __shared__ float gate_partial[4][Warps];
    \\    __shared__ float up_partial[4][Warps];
    \\    float gate_acc[4];
    \\    float up_acc[4];
    \\    bool full_tile = col_tile + 3u < out_dim;
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < 4u; ++c) {
    \\        gate_acc[c] = 0.0f;
    \\        up_acc[c] = 0.0f;
    \\    }
    \\
    \\    unsigned int iqs = (tid & 1u) * 2u;
    \\    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
    \\        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
    \\#include "../kernels/q4_a4b_q8_acquire.cuh"
    \\
    \\        if (full_tile) {
    \\            #pragma unroll
    \\            for (unsigned int c = 0u; c < 4u; ++c) {
    \\                unsigned int col = col_tile + c;
    \\{{q4q8_pair_dot_u32_inner}}
    \\            }
    \\        } else {
    \\            #pragma unroll
    \\            for (unsigned int c = 0u; c < 4u; ++c) {
    \\                unsigned int col = col_tile + c;
    \\                if (col < out_dim) {
    \\{{q4q8_pair_dot_u32_tail}}
    \\                }
    \\            }
    \\        }
    \\    }
    \\
    \\{{q4q8_pair_warp_tile4}}
    \\    __syncthreads();
    \\    if (tid == 0u) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0; c < 4u; ++c) {
    \\            unsigned int col = col_tile + c;
    \\            if (full_tile || col < out_dim) {
    \\                float gate_y = 0.0f;
    \\                float up_y = 0.0f;
    \\                #pragma unroll
    \\                for (unsigned int w = 0; w < Warps; ++w) {
    \\                    gate_y += gate_partial[c][w];
    \\                    up_y += up_partial[c][w];
    \\                }
    \\                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
    \\            }
    \\        }
    \\    }
    \\}
    \\
    \\
;
