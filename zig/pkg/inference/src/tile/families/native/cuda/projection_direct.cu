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

// native-program:entry termite_primitive_batched_dot_f32
extern "C" __global__ void termite_primitive_batched_dot_f32(ANTFLY_CUDA_PRIMITIVE_BATCHED_DOT_PARAMS) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int output_count = batch_count * m * n;
    if (idx >= output_count) return;

    unsigned int col = idx % n;
    unsigned int row_batch = idx / n;
    unsigned int row = row_batch % m;
    unsigned int batch = row_batch / m;
    unsigned int lhs_base = lhs_contract_last != 0u
        ? (batch * m + row) * k
        : batch * k * m + row;
    float acc = 0.0f;
    if (rhs_contract_last != 0u) {
        unsigned int rhs_base = (batch * n + col) * k;
        for (unsigned int inner = 0; inner < k; ++inner) {
            unsigned int lhs_idx = lhs_contract_last != 0u ? lhs_base + inner : lhs_base + inner * m;
            acc += lhs[lhs_idx] * rhs[rhs_base + inner];
        }
    } else {
        unsigned int rhs_base = batch * k * n + col;
        for (unsigned int inner = 0; inner < k; ++inner) {
            unsigned int lhs_idx = lhs_contract_last != 0u ? lhs_base + inner : lhs_base + inner * m;
            acc += lhs[lhs_idx] * rhs[rhs_base + inner * n];
        }
    }
    output[idx] = acc;
}
// native-program:entry termite_activation_multiply_fused_gate_up_f32
extern "C" __global__ void termite_activation_multiply_fused_gate_up_f32(
    float* dst,
    const float* fused,
    unsigned int rows,
    unsigned int f,
    unsigned int activation
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * f;
    if (i >= count) return;
    unsigned int row = i / f;
    unsigned int col = i - row * f;
    const float* fused_row = fused + (size_t)row * (2u * f);
    dst[i] = termite_decoder_activation_f32(fused_row[col], activation) * fused_row[f + col];
}
// native-program:entry termite_linear_q8_0_f32
extern "C" __global__ void termite_linear_q8_0_f32(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (idx >= total) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    unsigned int row_blocks = in_dim / 32u;
    float acc = 0.0f;
    for (unsigned int block = 0; block < row_blocks; ++block) {
        const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
        unsigned short h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
        float d = termite_half_to_float(h);
        for (unsigned int i = 0; i < 32u; ++i) {
            signed char q = (signed char)bp[2u + i];
            acc += input[row * in_dim + block * 32u + i] * ((float)q * d);
        }
    }
    dst[idx] = acc;
}
// native-program:entry termite_linear_q8_0_gated_down_f32_tile4
extern "C" __global__ void termite_linear_q8_0_gated_down_f32_tile4(ANTFLY_CUDA_QUANT_GATED_DOWN_PARAMS) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float partial[4][256];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        unsigned int input_idx = row * in_dim + i;
        float x = termite_decoder_activation_f32(gate[input_idx], activation) * up[input_idx];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 34u;
                acc[c] += x * termite_q8_0_value(bp, lane);
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) partial[c][tid] = acc[c];
#include "../kernels/quant_tile4_publish.cuh"
}
// native-program:entry termite_linear_q4_0_f32
extern "C" __global__ void termite_linear_q4_0_f32(ANTFLY_CUDA_QUANT_LINEAR_PARAMS) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (idx >= total) return;
    unsigned int row = idx / out_dim;
    unsigned int col = idx - row * out_dim;
    unsigned int row_blocks = in_dim / 32u;
    float acc = 0.0f;
    for (unsigned int block = 0; block < row_blocks; ++block) {
        const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
        for (unsigned int i = 0; i < 32u; ++i) {
            acc += input[row * in_dim + block * 32u + i] * termite_q4_0_value(bp, i);
        }
    }
    dst[idx] = acc;
}
// native-program:entry termite_linear_q4_0_q8_1_f32_tile4_w8_rows2
extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w8_rows2(ANTFLY_CUDA_Q8_INPUT_LINEAR_PARAMS) {
    const unsigned int cols = 4u;
    const unsigned int rows_per_block = 2u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int row0 = blockIdx.y * rows_per_block;
    if (row0 >= rows) return;
    unsigned int row1 = row0 + 1u;
    bool has_row1 = row1 < rows;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[2][4][8];
    float acc0[4];
    float acc1[4];
    bool full_tile = col_tile + 3u < out_dim;
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        acc0[c] = 0.0f;
        acc1[c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp0 = q8_input + (row0 * row_blocks + block) * 36u;
        unsigned short q8_d0_h = (unsigned short)q8_bp0[0] | ((unsigned short)q8_bp0[1] << 8);
        float q8_d0 = termite_half_to_float(q8_d0_h);
        const signed char* q8_values0 = (const signed char*)(q8_bp0 + 4u);
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        int q8_low0_0 = termite_load_i8x4_aligned(q8_values0 + q8_base0);
        int q8_high0_0 = termite_load_i8x4_aligned(q8_values0 + q8_base0 + 16u);
        int q8_low1_0 = termite_load_i8x4_aligned(q8_values0 + q8_base1);
        int q8_high1_0 = termite_load_i8x4_aligned(q8_values0 + q8_base1 + 16u);

        float q8_d1 = 0.0f;
        int q8_low0_1 = 0;
        int q8_high0_1 = 0;
        int q8_low1_1 = 0;
        int q8_high1_1 = 0;
        if (has_row1) {
            const unsigned char* q8_bp1 = q8_input + (row1 * row_blocks + block) * 36u;
            unsigned short q8_d1_h = (unsigned short)q8_bp1[0] | ((unsigned short)q8_bp1[1] << 8);
            q8_d1 = termite_half_to_float(q8_d1_h);
            const signed char* q8_values1 = (const signed char*)(q8_bp1 + 4u);
            q8_low0_1 = termite_load_i8x4_aligned(q8_values1 + q8_base0);
            q8_high0_1 = termite_load_i8x4_aligned(q8_values1 + q8_base0 + 16u);
            q8_low1_1 = termite_load_i8x4_aligned(q8_values1 + q8_base1);
            q8_high1_1 = termite_load_i8x4_aligned(q8_values1 + q8_base1 + 16u);
        }

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc0[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d0, iqs, q8_low0_0, q8_high0_0, q8_low1_0, q8_high1_0);
                if (has_row1) acc1[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d1, iqs, q8_low0_1, q8_high0_1, q8_low1_1, q8_high1_1);
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    acc0[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d0, iqs, q8_low0_0, q8_high0_0, q8_low1_0, q8_high1_0);
                    if (has_row1) acc1[c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d1, iqs, q8_low0_1, q8_high0_1, q8_low1_1, q8_high1_1);
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) {
        float sum0 = termite_warp_reduce_sum(acc0[c]);
        float sum1 = termite_warp_reduce_sum(acc1[c]);
        if (lane == 0u && warp < 8u) {
            warp_partial[0][c][warp] = sum0;
            warp_partial[1][c][warp] = sum1;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < cols; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                float y0 = 0.0f;
                float y1 = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 8u; ++w) {
                    y0 += warp_partial[0][c][w];
                    y1 += warp_partial[1][c][w];
                }
                dst[row0 * out_dim + col] = y0;
                if (has_row1) dst[row1 * out_dim + col] = y1;
            }
        }
    }
}
// native-program:entry termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows
extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile4_w8_e4b_down_rows(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight
) {
    const unsigned int cols = 4u;
    const unsigned int row_blocks = 320u;
    const unsigned int out_dim = 2560u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int row = blockIdx.y;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    __shared__ float warp_partial[4][8];
    float acc[4];
    termite_q4_0_q8_1_cols_partial<4u>(acc, q8_input, weight, row, row_blocks, 0u, col_tile, true, tid);
    termite_store_q4_0_cols_warp_sum_warps<4u, 8u, true, true>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}
// native-program:entry termite_linear_q4_0_q8_1_f32_tile8
extern "C" __global__ void termite_linear_q4_0_q8_1_f32_tile8(ANTFLY_CUDA_Q8_INPUT_LINEAR_PARAMS) {
    const unsigned int cols = 8u;
    unsigned int col_tile = blockIdx.x * cols;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[8][4];
    float acc[8];
    bool full_tile = col_tile + 7u < out_dim;
    termite_q4_0_q8_1_cols_partial<8u>(acc, q8_input, weight, row, row_blocks, out_dim, col_tile, full_tile, tid);
    termite_store_q4_0_cols_warp_sum_warps<8u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}
// native-program:entry termite_linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4
extern "C" __global__ void termite_linear_q4_0_activation_slice_last_dim_q8_1_f32_tile4(
    float* dst,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const float* source,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int source_cols,
    unsigned int source_start,
    unsigned int activation
) {
    unsigned int col_tile = blockIdx.x * 4u;
    unsigned int row = blockIdx.y;
    if (row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][4];
    float acc[4];
    bool full_tile = col_tile + 3u < out_dim;
    termite_q4_0_q8_1_cols_partial<4u>(acc, q8_input, weight, row, row_blocks, out_dim, col_tile, full_tile, tid);
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 4u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (full_tile || col < out_dim) {
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0; w < 4u; ++w) y += warp_partial[c][w];
                dst[row * out_dim + col] =
                    termite_decoder_activation_f32(y, activation) *
                    source[row * source_cols + source_start + col];
            }
        }
    }
}
// native-program:entry termite_linear_q4_0_pair_nobias_f32_tile4_w4
extern "C" __global__ void termite_linear_q4_0_pair_nobias_f32_tile4_w4(ANTFLY_CUDA_QUANT_PAIR_PARAMS) {
    const unsigned int cols = 4u;
    unsigned int tiles = (out_dim + cols - 1u) / cols;
    unsigned int tiles_per_row = tiles * 2u;
{{pair_projection_tile}}
    termite_q4_0_tile_cols_at<4u, 4u>(dst, input, weight, rows, in_dim, out_dim, row, col_tile);
}
// native-program:entry termite_linear_q4_0_pair_nobias_q8_1_f32_tile8
extern "C" __global__ void termite_linear_q4_0_pair_nobias_q8_1_f32_tile8(ANTFLY_CUDA_Q8_INPUT_PAIR_PARAMS) {
    const unsigned int cols = 8u;
    unsigned int tiles = (out_dim + cols - 1u) / cols;
    unsigned int tiles_per_row = tiles * 2u;
{{pair_projection_tile}}
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[8][4];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < cols; ++c) acc[c] = 0.0f;

{{q4_q8_pair_dot}}
    termite_store_q4_0_cols_warp_sum_warps<8u, 4u>(dst, row, out_dim, col_tile, acc, &warp_partial[0][0], tid, lane, warp);
}
// native-program:entry termite_linear_q4_0_pair_activation_f32_tile4_w4
extern "C" __global__ void termite_linear_q4_0_pair_activation_f32_tile4_w4(
    float* dst,
    const float* input,
    const unsigned char* weight_gate,
    const unsigned char* weight_up,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int activation
) {
    const unsigned int cols = 4u;
    unsigned int tiles = (out_dim + cols - 1u) / cols;
    unsigned int global = blockIdx.x;
    unsigned int row = global / tiles;
    if (row >= rows) return;
    unsigned int col_tile = (global - row * tiles) * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float gate_partial[4][4];
    __shared__ float up_partial[4][4];
    float gate_acc[4];
    float up_acc[4];
    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        float x = input[row * in_dim + i];
        unsigned int block = i / 32u;
        unsigned int lane = i - block * 32u;
        unsigned int q_offset = 2u + (lane & 15u);
        unsigned int high_nibble = lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                gate_acc[c] += x * termite_q4_0_value_nibble(gate_bp, q_offset, high_nibble);
                up_acc[c] += x * termite_q4_0_value_nibble(up_bp, q_offset, high_nibble);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0; c < 4u; ++c) {
        float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 4u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 4u; ++c) {
{{gate_up_warp_publish}}
        }
    }
}
// native-program:entry termite_linear_q4_0_pair_activation_q8_1_f32_tile8
extern "C" __global__ void termite_linear_q4_0_pair_activation_q8_1_f32_tile8(ANTFLY_CUDA_Q8_INPUT_PAIR_ACTIVATION_PARAMS) {
    const unsigned int cols = 8u;
    unsigned int tiles = (out_dim + cols - 1u) / cols;
    unsigned int global = blockIdx.x;
    unsigned int row = global / tiles;
    if (row >= rows) return;
    unsigned int col_tile = (global - row * tiles) * cols;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float gate_partial[8][4];
    __shared__ float up_partial[8][4];
    float gate_acc[8];
    float up_acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
#include "../kernels/q4_a4b_q8_acquire.cuh"

        #pragma unroll
        for (unsigned int c = 0u; c < 8u; ++c) {
            unsigned int col = col_tile + c;
            if (col < out_dim) {
{{q4q8_pair_dot_u32_inner}}
            }
        }
    }

{{q4q8_pair_warp_tile8}}
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
{{gate_up_warp_publish}}
        }
    }
}
// native-program:entry termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8
extern "C" __global__ void termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8(ANTFLY_CUDA_Q8_QUANT_ARGMAX_PARAMS) {
    const unsigned int cols = 8u;
    unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    unsigned int global_tile = blockIdx.x;
    unsigned int row = col_tiles == 0u ? 0u : global_tile / col_tiles;
    unsigned int tile = col_tiles == 0u ? 0u : global_tile - row * col_tiles;
    unsigned int col_tile = tile * cols;
    bool full_tile = col_tile + 7u < out_dim;
    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 256u;
    unsigned int q8_row_blocks = in_dim / 32u;
    unsigned int task_count = row_blocks * 16u;
    __shared__ float warp_partial[8][8];
    float acc[8];
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) acc[c] = 0.0f;

    if (row < rows && tid < task_count) {
        unsigned int block = tid / 16u;
        unsigned int sub = tid - block * 16u;
        unsigned int q8_sub_block = sub >> 1u;
        unsigned int q8_lane_base = (sub & 1u) * 16u;
        const unsigned char* q8_bp = input_q8_1 + (row * q8_row_blocks + block * 8u + q8_sub_block) * 36u;
        unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
        float q8_d = termite_half_to_float(q8_d_h);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        int q8_pack0 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 0u);
        int q8_pack1 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 4u);
        int q8_pack2 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 8u);
        int q8_pack3 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 12u);
        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0; c < 8u; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
                int sumi = 0;
                sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 0u), q8_pack0, sumi);
                sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 4u), q8_pack1, sumi);
                sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 8u), q8_pack2, sumi);
                sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 12u), q8_pack3, sumi);
                acc[c] = (q8_d * termite_q6k_sub_scale_f32(bp, sub)) * (float)sumi;
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0; c < 8u; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
                    int sumi = 0;
                    sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 0u), q8_pack0, sumi);
                    sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 4u), q8_pack1, sumi);
                    sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 8u), q8_pack2, sumi);
                    sumi = __dp4a(termite_pack_q6k_i8x4_sub(bp, sub, 12u), q8_pack3, sumi);
                    acc[c] = (q8_d * termite_q6k_sub_scale_f32(bp, sub)) * (float)sumi;
                }
            }
        }
    }
    #pragma unroll
    for (unsigned int c = 0; c < 8u; ++c) {
        float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        unsigned int warp_count = blockDim.x >> 5;
        if (row < rows) {
            if (full_tile) {
                #pragma unroll
                for (unsigned int c = 0; c < 8u; ++c) {
                    unsigned int col = col_tile + c;
                    float value = 0.0f;
{{q6_suppressed_selection}}
                }
            } else {
                #pragma unroll
                for (unsigned int c = 0; c < 8u; ++c) {
                    unsigned int col = col_tile + c;
                    if (col >= out_dim) continue;
                    float value = 0.0f;
{{q6_suppressed_selection}}
                }
            }
        }
        partial_values[global_tile] = best_value;
        partial_indices[global_tile] = best_index;
    }
}
// native-program:entry termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_a4b
extern "C" __global__ void termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_a4b(ANTFLY_CUDA_Q8_QUANT_ARGMAX_PARAMS) {
    (void)suppress_token_ids;
    if (rows != 1u || in_dim != 2816u || out_dim == 0u || (out_dim & 7u) != 0u ||
        suppress_count != 0u || blockDim.x != 192u) return;
    const unsigned int cols = 8u;
    const unsigned int row_blocks = 11u;
    const unsigned int global_tile = blockIdx.x;
    const unsigned int col_tile = global_tile * cols;
    if (col_tile >= out_dim) return;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    __shared__ float warp_partial[8][6];
    float acc[8];
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    if (tid < 176u) {
        const unsigned int block = tid >> 4u;
        const unsigned int sub = tid & 15u;
        const unsigned int q8_sub_block = sub >> 1u;
        const unsigned int q8_lane_base = (sub & 1u) * 16u;
        const unsigned char* q8_bp = input_q8_1 + (block * 8u + q8_sub_block) * 36u;
        const float q8_d = termite_half_to_float(((const unsigned short*)q8_bp)[0]);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        const int q8_pack0 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 0u);
        const int q8_pack1 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 4u);
        const int q8_pack2 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 8u);
        const int q8_pack3 = termite_load_i8x4_aligned(q8_values + q8_lane_base + 12u);
#pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned char* bp = weight + ((size_t)(col_tile + c) * row_blocks + block) * 210u;
            const int sumi = termite_q6k_q8_1_dot16_exact_2816(
                bp,
                sub,
                q8_pack0,
                q8_pack1,
                q8_pack2,
                q8_pack3
            );
            acc[c] = (q8_d * termite_q6k_sub_scale_f32(bp, sub)) * (float)sumi;
        }
    }
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid != 0u) return;

    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0xffffffffu;
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        float value = 0.0f;
#pragma unroll
        for (unsigned int w = 0u; w < 6u; ++w) value += warp_partial[c][w];
        const unsigned int col = col_tile + c;
        if (value > best_value || (value == best_value && col < best_index)) {
            best_value = value;
            best_index = col;
        }
    }
    partial_values[global_tile] = best_value;
    partial_indices[global_tile] = best_index;
}
// native-program:entry termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b
extern "C" __global__ void termite_linear_q6_k_q8_1_argmax_rows_stage1_tile8_e4b(ANTFLY_CUDA_Q8_QUANT_ARGMAX_PARAMS) {
    (void)suppress_token_ids;
    (void)rows;
    (void)in_dim;
    (void)out_dim;
    (void)suppress_count;
    const unsigned int row_blocks = 10u;
    const unsigned int cols = 8u;
    unsigned int global_tile = blockIdx.x;
    unsigned int col_tile = global_tile * cols;
#include "../kernels/q6_e4b_tile8_partials.cuh"
    __syncthreads();
    if (tid == 0u) {
        float best_value = -3.402823466e+38f;
        unsigned int best_index = 0xffffffffu;
        #pragma unroll
        for (unsigned int c = 0; c < 8u; ++c) {
            float value =
                warp_partial[c][0] +
                warp_partial[c][1] +
                warp_partial[c][2] +
                warp_partial[c][3] +
                warp_partial[c][4];
            unsigned int col = col_tile + c;
            if (value > best_value || (value == best_value && col < best_index)) {
                best_value = value;
                best_index = col;
            }
        }
        partial_values[global_tile] = best_value;
        partial_indices[global_tile] = best_index;
    }
}
// native-program:entry termite_linear_q6_k_q8_1_f32_tile8_e4b
extern "C" __global__ void termite_linear_q6_k_q8_1_f32_tile8_e4b(
    float* dst,
    const unsigned char* input_q8_1,
    const unsigned char* weight,
    unsigned int out_dim
) {
    const unsigned int row_blocks = 10u;
    const unsigned int cols = 8u;
    unsigned int global_tile = blockIdx.x;
    unsigned int col_tile = global_tile * cols;
    if (col_tile >= out_dim) return;
#include "../kernels/q6_e4b_tile8_partials.cuh"
    __syncthreads();
    if (tid < 8u) {
        unsigned int col = col_tile + tid;
        if (col < out_dim) {
            dst[col] =
                warp_partial[tid][0] +
                warp_partial[tid][1] +
                warp_partial[tid][2] +
                warp_partial[tid][3] +
                warp_partial[tid][4];
        }
    }
}
// native-program:entry termite_linear_q8_0_qkv_nobias_f32_tile4
extern "C" __global__ void termite_linear_q8_0_qkv_nobias_f32_tile4(ANTFLY_CUDA_QUANT_QKV_PARAMS) {
    const unsigned int cols = 4u;
{{qkv_projection_tile}}

{{qkv_planes}}

#include "../kernels/q8_projection_tile4.cuh"
}
// native-program:entry termite_linear_q4_0_qkv_nobias_f32_tile4_w4
extern "C" __global__ void termite_linear_q4_0_qkv_nobias_f32_tile4_w4(ANTFLY_CUDA_QUANT_QKV_PARAMS) {
    termite_q4_0_qkv_projection_impl<4u, 4u>(dst_q, dst_k, dst_v, input, weight_q, weight_k, weight_v, rows, in_dim, q_out_dim, kv_out_dim);
}
// native-program:entry termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8
extern "C" __global__ void termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8(ANTFLY_CUDA_Q8_INPUT_QKV_PARAMS) {
    termite_packed_qkv_projection_impl<8u>(dst_q, dst_k, dst_v, q8_input, weight_q, weight_k, weight_v, rows, in_dim, q_out_dim, kv_out_dim);
}
// native-program:entry termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4
extern "C" __global__ void termite_linear_q4_0_qkv_nobias_q8_1_f32_tile8_rows4(ANTFLY_CUDA_Q8_INPUT_QKV_PARAMS) {
    const unsigned int cols = 8u;
    const unsigned int rows_per_block = 4u;
    unsigned int q_tiles = (q_out_dim + cols - 1u) / cols;
    unsigned int kv_tiles = (kv_out_dim + cols - 1u) / cols;
    unsigned int tiles_per_row_group = q_tiles + kv_tiles * 2u;
    unsigned int global = blockIdx.x;
    unsigned int row_group = global / tiles_per_row_group;
    unsigned int row_base = row_group * rows_per_block;
    if (row_base >= rows) return;
    unsigned int tile = global - row_group * tiles_per_row_group;

{{qkv_planes}}

    unsigned int tid = threadIdx.x;
    unsigned int lane = tid & 31u;
    unsigned int warp = tid >> 5;
    unsigned int row_blocks = in_dim / 32u;
    __shared__ float warp_partial[4][8][4];
    float acc[4][8];
    bool full_tile = col_tile + 7u < out_dim;
    #pragma unroll
    for (unsigned int r = 0u; r < rows_per_block; ++r) {
        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) acc[r][c] = 0.0f;
    }

    unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
        float q8_d[4];
        int q8_low0[4];
        int q8_high0[4];
        int q8_low1[4];
        int q8_high1[4];
        unsigned int q8_base0 = iqs * 4u;
        unsigned int q8_base1 = q8_base0 + 4u;
        #pragma unroll
        for (unsigned int r = 0u; r < rows_per_block; ++r) {
            unsigned int row = row_base + r;
            if (row < rows) {
                const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
                unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
                q8_d[r] = termite_half_to_float(q8_d_h);
                const signed char* q8_values = (const signed char*)(q8_bp + 4u);
                q8_low0[r] = termite_load_i8x4_aligned(q8_values + q8_base0);
                q8_high0[r] = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
                q8_low1[r] = termite_load_i8x4_aligned(q8_values + q8_base1);
                q8_high1[r] = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
            } else {
                q8_d[r] = 0.0f;
                q8_low0[r] = 0;
                q8_high0[r] = 0;
                q8_low1[r] = 0;
                q8_high1[r] = 0;
            }
        }

        if (full_tile) {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                #pragma unroll
                for (unsigned int r = 0u; r < rows_per_block; ++r) {
                    if (row_base + r < rows) {
                        acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                    }
                }
            }
        } else {
            #pragma unroll
            for (unsigned int c = 0u; c < cols; ++c) {
                unsigned int col = col_tile + c;
                if (col < out_dim) {
                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                    #pragma unroll
                    for (unsigned int r = 0u; r < rows_per_block; ++r) {
                        if (row_base + r < rows) {
                            acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
                        }
                    }
                }
            }
        }
    }

    #pragma unroll
    for (unsigned int r = 0u; r < rows_per_block; ++r) {
        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            float sum = termite_warp_reduce_sum(acc[r][c]);
            if (lane == 0u && warp < 4u) warp_partial[r][c][warp] = sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int r = 0u; r < rows_per_block; ++r) {
            unsigned int row = row_base + r;
            if (row < rows) {
                #pragma unroll
                for (unsigned int c = 0u; c < cols; ++c) {
                    unsigned int col = col_tile + c;
                    if (col < out_dim) {
                        float y = 0.0f;
                        #pragma unroll
                        for (unsigned int w = 0u; w < 4u; ++w) y += warp_partial[r][c][w];
                        dst[row * out_dim + col] = y;
                    }
                }
            }
        }
    }
}
// native-program:entry termite_linear_q4_k_qkv_nobias_f32_tiled
extern "C" __global__ void termite_linear_q4_k_qkv_nobias_f32_tiled(ANTFLY_CUDA_QUANT_QKV_PARAMS) {
    unsigned int q_plane = rows * q_out_dim;
    unsigned int kv_plane = rows * kv_out_dim;
    unsigned int total = q_plane + kv_plane * 2u;
    unsigned int global = blockIdx.x;
    if (global >= total) return;

    const unsigned char* weight;
    float* dst;
    unsigned int out_dim;
    unsigned int local;
    if (global < q_plane) {
        weight = weight_q;
        dst = dst_q;
        out_dim = q_out_dim;
        local = global;
    } else if (global < q_plane + kv_plane) {
        weight = weight_k;
        dst = dst_k;
        out_dim = kv_out_dim;
        local = global - q_plane;
    } else {
        weight = weight_v;
        dst = dst_v;
        out_dim = kv_out_dim;
        local = global - q_plane - kv_plane;
    }

    unsigned int row = local / out_dim;
    unsigned int col = local - row * out_dim;
    unsigned int row_blocks = in_dim / 256u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    if (tid < 256u) {
        for (unsigned int block = 0; block < row_blocks; ++block) {
            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
        }
        partial[tid] = acc;
    }
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[local] = partial[0];
}
// native-program:entry termite_linear_q4_k_q4_k_f32_qkv_nobias_tiled
extern "C" __global__ void termite_linear_q4_k_q4_k_f32_qkv_nobias_tiled(ANTFLY_CUDA_LINEAR_Q4_K_Q4_K_F32_QKV_NOBIAS_TILED_PARAMS) {
    unsigned int q_plane = rows * q_out_dim;
    unsigned int kv_plane = rows * kv_out_dim;
    unsigned int total = q_plane + kv_plane * 2u;
    unsigned int global = blockIdx.x;
    if (global >= total) return;

    const unsigned char* qweight = weight_q;
    const unsigned char* kweight = weight_k;
    const float* vweight = weight_v;
    float* dst;
    unsigned int out_dim;
    unsigned int local;
    unsigned int projection;
    if (global < q_plane) {
        dst = dst_q;
        out_dim = q_out_dim;
        local = global;
        projection = 0u;
    } else if (global < q_plane + kv_plane) {
        dst = dst_k;
        out_dim = kv_out_dim;
        local = global - q_plane;
        projection = 1u;
    } else {
        dst = dst_v;
        out_dim = kv_out_dim;
        local = global - q_plane - kv_plane;
        projection = 2u;
    }

    unsigned int row = local / out_dim;
    unsigned int col = local - row * out_dim;
    unsigned int row_blocks = in_dim / 256u;
    unsigned int tid = threadIdx.x;
    __shared__ float partial[256];
    float acc = 0.0f;
    if (projection == 2u) {
        for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
            acc += input[row * in_dim + i] * vweight[col * in_dim + i];
        }
    } else if (tid < 256u) {
        const unsigned char* weight = projection == 0u ? qweight : kweight;
        for (unsigned int block = 0; block < row_blocks; ++block) {
            const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
            acc += input[row * in_dim + block * 256u + tid] * termite_q4k_value(bp, tid);
        }
    }
    partial[tid] = acc;
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) partial[tid] += partial[tid + stride];
        __syncthreads();
    }
    if (tid == 0u) dst[local] = partial[0];
}
