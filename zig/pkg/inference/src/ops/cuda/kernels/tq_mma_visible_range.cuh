// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Exact shared query-tile visibility bounds and KV head selection. Native scope; no include guard.
    if (qi_last >= q_seq_len) qi_last = q_seq_len - 1u;
    unsigned int block_key_start = 0u;
    unsigned int block_key_end = 0u;
    {
        unsigned int first_pos = query_position_offset + tile_row_start;
        unsigned int last_pos = query_position_offset + qi_last;
        if (kv_seq_len != 0u && last_pos >= kv_position_offset) {
            unsigned int visible = last_pos - kv_position_offset + 1u;
            block_key_end = visible < kv_seq_len ? visible : kv_seq_len;
            if (sliding_window != 0u && first_pos >= kv_position_offset) {
                unsigned int window_start_abs = (first_pos + 1u > sliding_window) ? (first_pos + 1u - sliding_window) : 0u;
                if (window_start_abs > kv_position_offset) {
                    block_key_start = window_start_abs - kv_position_offset;
                    if (block_key_start > block_key_end) block_key_start = block_key_end;
                }
            }
        }
    }

    unsigned int heads_per_group = num_heads / num_kv_heads;
    unsigned int kv_head = head / heads_per_group;
    unsigned int q_hidden = num_heads * head_dim;
    float scale = rsqrtf((float)head_dim);
