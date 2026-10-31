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

pub const attention = .{
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
