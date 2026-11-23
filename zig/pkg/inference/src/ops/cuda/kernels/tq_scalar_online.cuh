// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Native TurboQuant scalar online loop. Full and split routes retain their
// address/decode policies and publish normalized output or split metadata locally.
// Native scope; no include guard. SPLIT supplies chunk bounds and Identity.
    float scale = rsqrtf((float)head_dim);
    if (lane == 0u) {
        shared_max_score = neg_inf;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
#ifdef TERMITE_TQ_SCALAR_SPLIT
    for (unsigned int ki = chunk_start; ki < chunk_end; ++ki) {
#else
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
#endif
#ifdef TERMITE_TQ_SCALAR_SPLIT
        unsigned int physical_token = Identity ? ki : termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
#else
        unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
#endif
#ifdef TERMITE_TQ_SCALAR_SPLIT
        bool valid = Identity ? ki < physical_token_capacity : physical_token != 0xffffffffu;
#else
        bool valid = physical_token != 0xffffffffu;
#endif
        const unsigned char* k_row = valid ? k + physical_token * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            unsigned int value_index = kv_head * head_dim + lane;
#ifdef TERMITE_TQ_SCALAR_SPLIT
            float key_value = Identity || format == 0u
#else
            float key_value = format == 0u
#endif
                ? termite_tq_decode_polar4_at(k_row, value_index)
                : termite_tq_f16_value(k_row, value_index);
            partial = q[q_base + lane] * key_value;
        }
        float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            if (valid) {
                float score = dot * scale;
                float next_max = fmaxf(shared_max_score, score);
                shared_alpha = expf(shared_max_score - next_max);
                shared_beta = expf(score - next_max);
                shared_denom = shared_denom * shared_alpha + shared_beta;
                shared_max_score = next_max;
            } else {
                shared_alpha = 1.0f;
                shared_beta = 0.0f;
            }
        }
        __syncthreads();
        if (lane < head_dim) {
            acc *= shared_alpha;
            if (valid) {
                const unsigned char* v_row = v + physical_token * value_row_bytes;
#ifdef TERMITE_TQ_SCALAR_SPLIT
                float value = Identity ? termite_tq_value_int8_per_head(v_row, kv_head, lane, head_dim) : termite_tq_value_at(v_row, kv_head, lane, head_dim, value_format);
#else
                float value = termite_tq_value_at(v_row, kv_head, lane, head_dim, value_format);
#endif
                acc += shared_beta * value;
            }
        }
        __syncthreads();
    }

#undef TERMITE_TQ_SCALAR_SPLIT
