// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Included after native route guards and live device-scalar acquisition; no include guard.
// Decode supplies b=qi=0; prefill supplies its original batch/query indices.
// The caller retains exact shared storage. Online folds and publication are one native phase.
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_hidden = num_heads * head_dim;
    unsigned int kv_hidden = num_kv_heads * head_dim;
    unsigned int q_base = (b * q_seq_len + qi) * q_hidden + head * head_dim;
    float scale = rsqrtf((float)head_dim);
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        float partial = 0.0f;
        if (lane < head_dim) {
            unsigned int k_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            float score = dot * scale;
            float next_max = fmaxf(shared_max_score, score);
            shared_alpha = expf(shared_max_score - next_max);
            shared_beta = expf(score - next_max);
            shared_denom = shared_denom * shared_alpha + shared_beta;
            shared_max_score = next_max;
        }
        __syncthreads();
        if (lane < head_dim) {
            unsigned int v_idx = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim + lane;
            acc = acc * shared_alpha + shared_beta * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        unsigned int out_idx = (b * q_seq_len + qi) * q_hidden + head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
