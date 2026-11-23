// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Exact key/value tile acquisition, conversion and barriers; native per-schedule arrays stay local. Native scope; no include guard.
#ifdef TERMITE_TQ_MMA_STAGE_VALUES
        for (unsigned int idx = tid; idx < tile_n * head_dim; idx += 256u) {
            unsigned int row = idx / head_dim;
            unsigned int col = idx - row * head_dim;
            unsigned int phys = tile_phys[row];
            float value = 0.0f;
            if (phys != 0xffffffffu) {
                const unsigned char* v_row = v + (size_t)phys * value_row_bytes;
                value = termite_tq_value_at(v_row, kv_head, col, head_dim, value_format);
            }
            kv_tile[row * kv_pitch + col] = __float2half(value);
        }
        __syncthreads();
#else
        if (tid < tile_n) {
            tile_phys[tid] = tid < n_count
                ? termite_tq_physical_token(n0 + tid, block_table, block_count, page_size_tokens, physical_token_capacity)
                : 0xffffffffu;
        }
        __syncthreads();

        for (unsigned int idx = tid; idx < tile_n * head_dim; idx += 256u) {
            unsigned int row = idx / head_dim;
            unsigned int col = idx - row * head_dim;
            unsigned int phys = tile_phys[row];
            float key_value = 0.0f;
            if (phys != 0xffffffffu) {
                const unsigned char* k_row = k + (size_t)phys * key_row_bytes;
                unsigned int value_index = kv_head * head_dim + col;
                key_value = format == 0u
                    ? termite_tq_decode_polar4_at(k_row, value_index)
                    : termite_tq_f16_value(k_row, value_index);
            }
            kv_tile[row * kv_pitch + col] = __float2half(key_value);
        }
        __syncthreads();
#endif
#undef TERMITE_TQ_MMA_STAGE_VALUES
