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

const cuda_phases = @import("cuda_phases.zig");

const program = @import("../../native_program.zig");

const program_0 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_QUANT_PAIR_PARAMS) {
    \\    const unsigned int cols = {{literal_0}};
    \\    unsigned int tiles = (out_dim + cols - 1u) / cols;
    \\    unsigned int tiles_per_row = tiles * 2u;
    \\{{pair_projection_tile}}
    \\    termite_q4_0_tile_cols_at<{{literal_5}}, 8u>(dst, input, weight, rows, in_dim, out_dim, row, col_tile);
    \\}
    \\
;

const program_1 =
    \\extern "C" __global__ void {{name}}(
    \\    float* dst,
    \\    const unsigned char* q8_input,
    \\    const unsigned char* weight_gate,
    \\    const unsigned char* weight_up,
    \\    unsigned int activation
    \\) {
    \\    termite_e4b_ffn_pair_impl<{{literal_0}}>(dst, q8_input, weight_gate, weight_up, activation);
    \\}
    \\
;

const program_2 =
    \\extern "C" __global__ void {{name}}(ANTFLY_CUDA_QUANT_ARGMAX_PARAMS) {
    \\    termite_k_projection_argmax_impl<true, {{literal_1}}>(partial_values, partial_indices, input, weight, suppress_token_ids, rows, in_dim, out_dim, suppress_count);
    \\}
    \\
;

pub const source_0 = program.render(program_0, .{ .name = "termite_linear_q4_0_pair_nobias_f32_tile4", .literal_0 = "4u", .literal_5 = "4u", .pair_projection_tile = cuda_phases.projection.pair_projection_tile });

pub const source_1 = program.join(&.{
    \\extern "C" __global__ void termite_linear_q4_0_pair_nobias_q8_1_f32_tile4(ANTFLY_CUDA_Q8_INPUT_PAIR_PARAMS) {
    \\    termite_q4_0_pair_q8_1_tile4_impl<4u>(dst_a, dst_b, q8_input, weight_a, weight_b, rows, in_dim, out_dim);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_0_pair_nobias_q8_1_f32_tile4_w8(ANTFLY_CUDA_Q8_INPUT_PAIR_PARAMS) {
    \\    termite_q4_0_pair_q8_1_tile4_impl<8u>(dst_a, dst_b, q8_input, weight_a, weight_b, rows, in_dim, out_dim);
    \\}
    \\
});

pub const source_2 = program.join(&.{
    fixed_row_program,
    program.render(program_1, .{ .name = "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w8_e4b_ffn", .literal_0 = "8u" }),

    \\
    \\
    ,
    program.render(program_1, .{ .name = "termite_linear_q4_0_pair_activation_q8_1_f32_tile4_w5_e4b_ffn", .literal_0 = "5u" }),
});

pub const source_3 = program.render(program_0, .{ .name = "termite_linear_q4_0_pair_nobias_f32_tile8", .literal_0 = "8u", .literal_5 = "8u", .pair_projection_tile = cuda_phases.projection.pair_projection_tile });

pub const source_4 = program.join(&.{
    \\extern "C" __global__ void termite_linear_q4_0_argmax_rows_stage1_tile4(ANTFLY_CUDA_QUANT_ARGMAX_PARAMS) {
    \\    termite_scalar_projection_argmax_impl<false, 4u>(partial_values, partial_indices, input, weight, suppress_token_ids, rows, in_dim, out_dim, suppress_count);
    \\}
    \\
    ,

    \\
    \\
    ,

    \\extern "C" __global__ void termite_linear_q4_0_argmax_rows_stage1_tile16(ANTFLY_CUDA_QUANT_ARGMAX_PARAMS) {
    \\    termite_scalar_projection_argmax_impl<false, 16u>(partial_values, partial_indices, input, weight, suppress_token_ids, rows, in_dim, out_dim, suppress_count);
    \\}
    \\
});

const q6_register_argmax_program =
    \\// Keep the existing FP32 accumulation tree while moving its eight input
    \\// stripes into per-lane registers. Each warp owns two vocabulary columns;
    \\// only the final eight winners require a block-wide barrier.
    \\__device__ __forceinline__ float antfly_q6_f32_half(unsigned short h) {
    \\    float x;
    \\    asm("cvt.f32.f16 %0, %1;" : "=f"(x) : "h"(h));
    \\    return x;
    \\}
    \\
    \\__device__ __forceinline__ float antfly_q6_f32_value(const unsigned char* bp, unsigned i) {
    \\    unsigned sub = i / 16, half = sub / 8, group = (sub % 8) / 2, lane = i & 31;
    \\    unsigned lo = bp[half * 64 + (group & 1) * 32 + lane];
    \\    unsigned hi = bp[128 + half * 32 + lane];
    \\    int q = int(((lo >> ((group / 2) * 4)) & 15) | (((hi >> (group * 2)) & 3) << 4)) - 32;
    \\    unsigned short dh = unsigned(bp[208]) | (unsigned(bp[209]) << 8);
    \\    float scale = antfly_q6_f32_half(dh) * float((signed char)bp[192 + sub]);
    \\    return scale * float(q);
    \\}
    \\
    \\extern "C" __global__ void termite_linear_q6_k_argmax_rows_stage1_tile16(
    \\    float* partial_values,
    \\    unsigned* partial_indices,
    \\    const float* input,
    \\    const unsigned char* weight,
    \\    const int* suppress_token_ids,
    \\    unsigned rows,
    \\    unsigned in_dim,
    \\    unsigned out_dim,
    \\    unsigned suppress_count
    \\) {
    \\    unsigned tiles = (out_dim + 15) / 16, row = blockIdx.x / tiles, tile = blockIdx.x % tiles;
    \\    unsigned lane = threadIdx.x & 31, warp = threadIdx.x >> 5, blocks = in_dim / 256;
    \\    float acc[2][8];
    \\    #pragma unroll
    \\    for (unsigned c = 0; c < 2; ++c) {
    \\        #pragma unroll
    \\        for (unsigned p = 0; p < 8; ++p) acc[c][p] = 0;
    \\    }
    \\    if (row < rows) for (unsigned b = 0; b < blocks; ++b) {
    \\        #pragma unroll
    \\        for (unsigned c = 0; c < 2; ++c) {
    \\            unsigned col = tile * 16 + warp * 2 + c;
    \\            if (col < out_dim) {
    \\                const unsigned char* bp = weight + (col * blocks + b) * 210;
    \\                #pragma unroll
    \\                for (unsigned p = 0; p < 8; ++p) {
    \\                    unsigned k = lane + p * 32;
    \\                    acc[c][p] = fmaf(input[row * in_dim + b * 256 + k], antfly_q6_f32_value(bp, k), acc[c][p]);
    \\                }
    \\            }
    \\        }
    \\    }
    \\    float best = -3.402823466e+38f;
    \\    unsigned best_i = 0xffffffffu;
    \\    #pragma unroll
    \\    for (unsigned c = 0; c < 2; ++c) {
    \\        // Preserve the old shared-memory reduction's 128/64/32 tree exactly.
    \\        acc[c][0] += acc[c][4];
    \\        acc[c][1] += acc[c][5];
    \\        acc[c][2] += acc[c][6];
    \\        acc[c][3] += acc[c][7];
    \\        acc[c][0] += acc[c][2];
    \\        acc[c][1] += acc[c][3];
    \\        acc[c][0] += acc[c][1];
    \\        float v = acc[c][0];
    \\        #pragma unroll
    \\        for (unsigned delta = 16; delta > 0; delta >>= 1) v += __shfl_down_sync(0xffffffffu, v, delta);
    \\        unsigned col = tile * 16 + warp * 2 + c;
    \\        bool suppressed = false;
    \\        for (unsigned j = 0; j < suppress_count; ++j) {
    \\            if (suppress_token_ids[j] >= 0 && unsigned(suppress_token_ids[j]) == col) suppressed = true;
    \\        }
    \\        if (row < rows && col < out_dim && !suppressed && (v > best || (v == best && col < best_i))) {
    \\            best = v;
    \\            best_i = col;
    \\        }
    \\    }
    \\    __shared__ float scores[8];
    \\    __shared__ unsigned ids[8];
    \\    if (lane == 0) {
    \\        scores[warp] = best;
    \\        ids[warp] = best_i;
    \\    }
    \\    __syncthreads();
    \\    if (warp == 0) {
    \\        best = lane < 8 ? scores[lane] : -3.402823466e+38f;
    \\        best_i = lane < 8 ? ids[lane] : 0xffffffffu;
    \\        #pragma unroll
    \\        for (unsigned delta = 16; delta > 0; delta >>= 1) {
    \\            float v = __shfl_down_sync(0xffffffffu, best, delta);
    \\            unsigned id = __shfl_down_sync(0xffffffffu, best_i, delta);
    \\            if (id != 0xffffffffu && (v > best || (v == best && id < best_i))) {
    \\                best = v;
    \\                best_i = id;
    \\            }
    \\        }
    \\        if (lane == 0) {
    \\            partial_values[blockIdx.x] = best;
    \\            partial_indices[blockIdx.x] = best_i;
    \\        }
    \\    }
    \\}
    \\
;

pub const source_5 = program.join(&.{
    program.render(program_2, .{ .name = "termite_linear_q6_k_argmax_rows_stage1_tile4", .literal_1 = "4u" }),

    \\
    \\
    ,
    program.render(program_2, .{ .name = "termite_linear_q6_k_argmax_rows_stage1_tile8", .literal_1 = "8u" }),

    \\
    \\
    ,
    q6_register_argmax_program,
});

pub const source_6 =
    \\extern "C" __global__ void termite_linear_q4_0_qkv_nobias_f32_tile4(ANTFLY_CUDA_QUANT_QKV_PARAMS) {
    \\    termite_q4_0_qkv_projection_impl<4u, 8u>(dst_q, dst_k, dst_v, input, weight_q, weight_k, weight_v, rows, in_dim, q_out_dim, kv_out_dim);
    \\}
    \\
;

pub const source_7 =
    \\extern "C" __global__ void termite_linear_q4_0_qkv_nobias_f32_tile8(ANTFLY_CUDA_QUANT_QKV_PARAMS) {
    \\    termite_q4_0_qkv_projection_impl<8u, 8u>(dst_q, dst_k, dst_v, input, weight_q, weight_k, weight_v, rows, in_dim, q_out_dim, kv_out_dim);
    \\}
    \\
;

// Static native helper owned alongside its specialization exports.
const fixed_row_program =
    \\template<unsigned int Warps>
    \\__device__ __forceinline__ void termite_e4b_ffn_pair_impl(float* dst,
    \\    const unsigned char* q8_input,
    \\    const unsigned char* weight_gate,
    \\    const unsigned char* weight_up,
    \\    unsigned int activation){
    \\
++ cuda_phases.fixedRowPair(80, false) ++ "\n}\n\n";
