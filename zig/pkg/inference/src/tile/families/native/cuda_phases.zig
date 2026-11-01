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

//! Exact numerical and ownership phases shared before CUDA compilation.

const program = @import("../../native_program.zig");

pub const attention = .{
    .online_softmax_shared_half = program.render(online_softmax, .{ .probability_store = "                probabilities[row * N + lane + j * 32u] = __float2half_rn(p);" }),
    .online_softmax_register_f32 = program.render(online_softmax, .{ .probability_store = "                staged_probabilities[g][j] = p;" }),
    .dense_gqa_parameters =
    \\    float* dst,
    \\    const float* q,
    \\    const float* k,
    \\    const float* v,
    \\    const unsigned char* attn_or_mask,
    \\    const float* bias,
    \\    unsigned int batch,
    \\    unsigned int q_seq_len,
    \\    unsigned int kv_seq_len,
    \\    unsigned int num_heads,
    \\    unsigned int num_kv_heads,
    \\    unsigned int head_dim,
    \\    unsigned int query_position_offset,
    \\    unsigned int kv_position_offset,
    \\    unsigned int sliding_window,
    \\    unsigned int total_sequence_len,
    \\    unsigned int mask_len,
    ,
    .visible_key_window =
    \\    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
    \\        unsigned int visible = query_pos - kv_position_offset + 1u;
    \\        key_end = visible < kv_seq_len ? visible : kv_seq_len;
    \\        if (sliding_window != 0u) {
    \\            unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
    \\            if (window_start_abs > kv_position_offset) {
    \\                key_start = window_start_abs - kv_position_offset;
    \\                if (key_start > key_end) key_start = key_end;
    \\            }
    \\        }
    \\    }
    ,
    .tensor_attention_tile =
    \\    const unsigned int tid = threadIdx.x, lane = tid & 31u, warp = tid >> 5u;
    \\    const unsigned int tiles = (seq + M - 1u) / M;
    \\    const unsigned int start = (blockIdx.x % tiles) * M;
    \\    const unsigned int matrix = blockIdx.x / tiles, head = matrix % heads, b = matrix / heads;
    \\    if (b >= batch) return;
    \\    const unsigned int hidden = heads * D, head_off = head * D;
    ,
    .cross_attention_scores =
    \\    for (unsigned int ki = 0; ki < enc_seq; ++ki) {
    \\        if (mask != nullptr && mask[b * enc_seq + ki] == 0ll) continue;
    \\        unsigned int k_base = (b * enc_seq + ki) * hidden + head * head_dim;
    \\        float score = 0.0f;
    \\        for (unsigned int j = 0; j < head_dim; ++j) {
    \\            score += q[q_base + j] * k[k_base + j];
    \\        }
    \\        score *= scale;
    ,
    .self_scores =
    \\        if (causal && ki > qi) continue;
    \\        if (has_mask && mask[b * seq_len + ki] == 0ll) continue;
    \\        float score = 0.0f;
    \\        unsigned int q_base = head_major ? ((b * num_heads + head) * seq_len + qi) * head_dim : (b * seq_len + qi) * hidden + head * head_dim;
    \\        unsigned int k_base = head_major ? ((b * num_heads + head) * seq_len + ki) * head_dim : (b * seq_len + ki) * hidden + head * head_dim;
    \\        for (unsigned int j = 0; j < head_dim; ++j) score += q[q_base + j] * k[k_base + j];
    \\        score *= scale;
    \\        if (bias_mode == 1u) score += bias[(head * seq_len + qi) * seq_len + ki];
    \\        if (bias_mode == 2u) score += bias[((b * num_heads + head) * seq_len + qi) * seq_len + ki];
    ,
    .masked_cross_scores =
    \\        unsigned int key_pos = kv_position_offset + ki;
    \\        unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
    \\        bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
    \\        bool future_blocked = key_pos > query_pos && !future_allowed;
    \\        bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
    \\        bool valid = !(future_blocked || past_blocked);
    \\        if (!valid) continue;
    \\
    \\        unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
    \\        unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
    \\        float score = 0.0f;
    \\        for (unsigned int j = 0; j < head_dim; ++j) score += q[q_base + j] * k[k_base + j];
    \\        score *= scale;
    \\        if (bias_mode == 1u) score += bias[(head * q_seq_len + qi) * kv_seq_len + ki];
    \\        if (bias_mode == 2u) score += bias[((b * num_heads + head) * q_seq_len + qi) * kv_seq_len + ki];
    ,
};

pub const projection = .{
    .q4q8_rows_linear = program.render(q4q8_rows, .{ .array_rows = "Rows", .warp_count = "8" }),
    .q4q8_rows_qkv = program.render(q4q8_rows, .{ .array_rows = "4", .warp_count = "4" }),
    .q4q8_pair_dot_u32_inner = program.render(q4q8_pair_dot, .{ .indent = "                ", .index_cast = "" }),
    .q4q8_pair_dot_u64_inner = program.render(q4q8_pair_dot, .{ .indent = "                ", .index_cast = "(size_t)" }),
    .q4q8_pair_dot_u32_tail = program.render(q4q8_pair_dot, .{ .indent = "                    ", .index_cast = "" }),
    .q4q8_pair_dot_u32_full = program.render(q4q8_pair_dot, .{ .indent = "            ", .index_cast = "" }),
    .q4q8_pair_warp_tile8 = program.render(q4q8_pair_warp, .{ .cols = "8u", .warps = "4u" }),
    .q4q8_pair_warp_tile4 = program.render(q4q8_pair_warp, .{ .cols = "4u", .warps = "Warps" }),
    .q4q8_pair_warp_fixed = program.render(q4q8_pair_warp, .{ .cols = "cols", .warps = "Warps" }),
    .pair_projection_tile =
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int row = global / tiles_per_row;
    \\    if (row >= rows) return;
    \\    unsigned int tile_local = global - row * tiles_per_row;
    \\    unsigned int projection = tile_local / tiles;
    \\    unsigned int col_tile = (tile_local - projection * tiles) * cols;
    \\    const unsigned char* weight = projection == 0u ? weight_a : weight_b;
    \\    float* dst = projection == 0u ? dst_a : dst_b;
    ,
    .q4_q8_pair_dot =
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
    ,
    .qkv_projection_tile =
    \\    unsigned int q_tiles = (q_out_dim + cols - 1u) / cols;
    \\    unsigned int kv_tiles = (kv_out_dim + cols - 1u) / cols;
    \\    unsigned int tiles_per_row = q_tiles + kv_tiles * 2u;
    \\    unsigned int global = blockIdx.x;
    \\    unsigned int row = global / tiles_per_row;
    \\    if (row >= rows) return;
    \\    unsigned int tile = global - row * tiles_per_row;
    ,
    .q4k_biased_dot =
    \\    unsigned int row_blocks = in_dim / 256u;
    \\    float acc = bias[col];
    \\    for (unsigned int block = 0; block < row_blocks; ++block) {
    \\        const unsigned char* bp = weight + (col * row_blocks + block) * 144u;
    \\        for (unsigned int i = 0; i < 256u; ++i) {
    \\            acc += input[row * in_dim + block * 256u + i] * termite_q4k_value(bp, i);
    \\        }
    \\    }
    ,
    .gate_up_warp_publish =
    \\            unsigned int col = col_tile + c;
    \\            if (col < out_dim) {
    \\                float gate_y = 0.0f;
    \\                float up_y = 0.0f;
    \\                #pragma unroll
    \\                for (unsigned int w = 0; w < 4u; ++w) {
    \\                    gate_y += gate_partial[c][w];
    \\                    up_y += up_partial[c][w];
    \\                }
    \\                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
    \\            }
    ,
    .qkv_planes =
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
    ,
    .q6_suppressed_selection =
    \\                    #pragma unroll
    \\                    for (unsigned int w = 0u; w < 8u; ++w) {
    \\                        if (w >= warp_count) continue;
    \\                        value += warp_partial[c][w];
    \\                    }
    \\                    bool suppressed = false;
    \\                    if (suppress_count != 0u) {
    \\                        for (unsigned int j = 0; j < suppress_count; ++j) {
    \\                            int token_id = suppress_token_ids[j];
    \\                            if (token_id >= 0 && (unsigned int)token_id == col) {
    \\                                suppressed = true;
    \\                                break;
    \\                            }
    \\                        }
    \\                    }
    \\                    if (suppressed) continue;
    \\                    if (value > best_value || (value == best_value && col < best_index)) {
    \\                        best_value = value;
    \\                        best_index = col;
    \\                    }
    ,
};

// Publication precision and storage remain properties of the static schedule.
const online_softmax =
    \\            for (unsigned int shift = 16u; shift; shift >>= 1u)
    \\                next_max = fmaxf(next_max, __shfl_down_sync(0xffffffffu, next_max, shift));
    \\            next_max = fmaxf(maximum[row], __shfl_sync(0xffffffffu, next_max, 0u));
    \\            const float a = isfinite(maximum[row]) ? __expf(maximum[row] - next_max) : 0.0f;
    \\            float sum = 0.0f;
    \\            #pragma unroll
    \\            for (unsigned int j = 0u; j < 2u; ++j) {
    \\                const float p = isfinite(next_max) ? __expf(values[j] - next_max) : 0.0f;
    \\{{probability_store}}
    \\                sum += p;
    \\            }
    \\            for (unsigned int shift = 16u; shift; shift >>= 1u)
    \\                sum += __shfl_down_sync(0xffffffffu, sum, shift);
    \\            if (lane == 0u) {
    \\                alpha[row] = a;
    \\                denominator[row] = denominator[row] * a + sum;
    \\                maximum[row] = next_max;
    \\            }
;

const q4q8_pair_dot =
    \\{{indent}}const unsigned char* gate_bp = weight_gate + ({{index_cast}}col * row_blocks + block) * 18u;
    \\{{indent}}const unsigned char* up_bp = weight_up + ({{index_cast}}col * row_blocks + block) * 18u;
    \\{{indent}}gate_acc[c] += termite_q4_0_q8_1_partial_mmvq2(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
    \\{{indent}}up_acc[c] += termite_q4_0_q8_1_partial_mmvq2(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
;

const q4q8_pair_warp =
    \\    #pragma unroll
    \\    for (unsigned int c = 0; c < {{cols}}; ++c) {
    \\        float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
    \\        float up_sum = termite_warp_reduce_sum(up_acc[c]);
    \\        if (lane == 0u && warp < {{warps}}) {
    \\            gate_partial[c][warp] = gate_sum;
    \\            up_partial[c][warp] = up_sum;
    \\        }
    \\    }
;

// Preserve the array extents, lane reductions, and full/tail paths of each schedule.
const q4q8_rows =
    \\    #pragma unroll
    \\    for (unsigned int r = 0u; r < rows_per_block; ++r) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0u; c < cols; ++c) acc[r][c] = 0.0f;
    \\    }
    \\
    \\    unsigned int iqs = (tid & 1u) * 2u;
    \\    for (unsigned int block = tid >> 1u; block < row_blocks; block += (blockDim.x >> 1u)) {
    \\        float q8_d[{{array_rows}}];
    \\        int q8_low0[{{array_rows}}];
    \\        int q8_high0[{{array_rows}}];
    \\        int q8_low1[{{array_rows}}];
    \\        int q8_high1[{{array_rows}}];
    \\        unsigned int q8_base0 = iqs * 4u;
    \\        unsigned int q8_base1 = q8_base0 + 4u;
    \\        #pragma unroll
    \\        for (unsigned int r = 0u; r < rows_per_block; ++r) {
    \\            unsigned int row = row_base + r;
    \\            if (row < rows) {
    \\                const unsigned char* q8_bp = q8_input + (row * row_blocks + block) * 36u;
    \\                unsigned short q8_d_h = (unsigned short)q8_bp[0] | ((unsigned short)q8_bp[1] << 8);
    \\                q8_d[r] = termite_half_to_float(q8_d_h);
    \\                const signed char* q8_values = (const signed char*)(q8_bp + 4u);
    \\                q8_low0[r] = termite_load_i8x4_aligned(q8_values + q8_base0);
    \\                q8_high0[r] = termite_load_i8x4_aligned(q8_values + q8_base0 + 16u);
    \\                q8_low1[r] = termite_load_i8x4_aligned(q8_values + q8_base1);
    \\                q8_high1[r] = termite_load_i8x4_aligned(q8_values + q8_base1 + 16u);
    \\            } else {
    \\                q8_d[r] = 0.0f;
    \\                q8_low0[r] = 0;
    \\                q8_high0[r] = 0;
    \\                q8_low1[r] = 0;
    \\                q8_high1[r] = 0;
    \\            }
    \\        }
    \\
    \\        if (full_tile) {
    \\            #pragma unroll
    \\            for (unsigned int c = 0u; c < cols; ++c) {
    \\                unsigned int col = col_tile + c;
    \\                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
    \\                #pragma unroll
    \\                for (unsigned int r = 0u; r < rows_per_block; ++r) {
    \\                    if (row_base + r < rows) {
    \\                        acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
    \\                    }
    \\                }
    \\            }
    \\        } else {
    \\            #pragma unroll
    \\            for (unsigned int c = 0u; c < cols; ++c) {
    \\                unsigned int col = col_tile + c;
    \\                if (col < out_dim) {
    \\                    const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
    \\                    #pragma unroll
    \\                    for (unsigned int r = 0u; r < rows_per_block; ++r) {
    \\                        if (row_base + r < rows) {
    \\                            acc[r][c] += termite_q4_0_q8_1_partial_mmvq2(bp, q8_d[r], iqs, q8_low0[r], q8_high0[r], q8_low1[r], q8_high1[r]);
    \\                        }
    \\                    }
    \\                }
    \\            }
    \\        }
    \\    }
    \\
    \\    #pragma unroll
    \\    for (unsigned int r = 0u; r < rows_per_block; ++r) {
    \\        #pragma unroll
    \\        for (unsigned int c = 0u; c < cols; ++c) {
    \\            float sum = termite_warp_reduce_sum(acc[r][c]);
    \\            if (lane == 0u && warp < {{warp_count}}u) warp_partial[r][c][warp] = sum;
    \\        }
    \\    }
    \\    __syncthreads();
    \\    if (tid == 0u) {
    \\        #pragma unroll
    \\        for (unsigned int r = 0u; r < rows_per_block; ++r) {
    \\            unsigned int row = row_base + r;
    \\            if (row < rows) {
    \\                #pragma unroll
    \\                for (unsigned int c = 0u; c < cols; ++c) {
    \\                    unsigned int col = col_tile + c;
    \\                    if (col < out_dim) {
    \\                        float y = 0.0f;
    \\                        #pragma unroll
    \\                        for (unsigned int w = 0u; w < {{warp_count}}u; ++w) y += warp_partial[r][c][w];
    \\                        dst[row * out_dim + col] = y;
    \\                    }
    \\                }
    \\            }
    \\        }
    \\    }
;
