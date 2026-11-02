// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Exact score prework and chronological online-softmax consumers. Geometry
// specializes statically; serial and tiled schedules retain their own lane
// ownership, scratch capacity, launch admission, and floating-point recurrence.
#ifndef ANTFLY_GQA_SCORE_PREWORK_F32
#define ANTFLY_GQA_SCORE_PREWORK_F32

#define ANTFLY_GQA_SCORE_PARAMS \
    float* scores,\
    const float* q,\
    const unsigned char* k,\
    const unsigned int* block_table,\
    unsigned int batch,\
    unsigned int q_seq_len,\
    unsigned int kv_seq_len,\
    unsigned int num_heads,\
    unsigned int num_kv_heads,\
    unsigned int head_dim,\
    unsigned int query_position_offset,\
    unsigned int kv_position_offset,\
    unsigned int sliding_window,\
    unsigned int total_sequence_len,\
    unsigned int key_row_bytes,\
    unsigned int base_key_row_bytes,\
    unsigned int block_count,\
    unsigned int page_size_tokens,\
    unsigned int format,\
    unsigned int physical_token_capacity,\
    unsigned int score_capacity,\
    unsigned int chunk_size,\
    unsigned int chunk_count,\
    const unsigned int* decode_scalars
#define ANTFLY_GQA_SCORE_ARGS scores, q, k, block_table, batch, q_seq_len, kv_seq_len, num_heads, num_kv_heads, head_dim, query_position_offset, kv_position_offset, sliding_window, total_sequence_len, key_row_bytes, base_key_row_bytes, block_count, page_size_tokens, format, physical_token_capacity, score_capacity, chunk_size, chunk_count, decode_scalars

#define ANTFLY_GQA_SCORE_CONSUMER_PARAMS \
    float* dst,\
    const float* scores,\
    const unsigned char* v,\
    const unsigned int* block_table,\
    unsigned int batch,\
    unsigned int q_seq_len,\
    unsigned int kv_seq_len,\
    unsigned int num_heads,\
    unsigned int num_kv_heads,\
    unsigned int head_dim,\
    unsigned int query_position_offset,\
    unsigned int kv_position_offset,\
    unsigned int sliding_window,\
    unsigned int total_sequence_len,\
    unsigned int value_row_bytes,\
    unsigned int block_count,\
    unsigned int page_size_tokens,\
    unsigned int value_format,\
    unsigned int physical_token_capacity,\
    unsigned int score_capacity,\
    const unsigned int* decode_scalars
#define ANTFLY_GQA_SCORE_CONSUMER_ARGS dst, scores, v, block_table, batch, q_seq_len, kv_seq_len, num_heads, num_kv_heads, head_dim, query_position_offset, kv_position_offset, sliding_window, total_sequence_len, value_row_bytes, block_count, page_size_tokens, value_format, physical_token_capacity, score_capacity, decode_scalars

template<unsigned HeadDim, unsigned Threads, unsigned HeadsPerKv, unsigned MaxHeads, unsigned Chunks, unsigned MaxScores>
__device__ __forceinline__ void antfly_gqa_score_prework(ANTFLY_GQA_SCORE_PARAMS) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (batch != 1u || q_seq_len != 1u || head_dim != HeadDim || blockDim.x != Threads ||
        score_capacity == 0u || score_capacity > MaxScores || chunk_size == 0u ||
        chunk_count != Chunks || key_row_bytes == 0u || base_key_row_bytes != key_row_bytes ||
        (format != 0u && format != 2u) || num_kv_heads == 0u || num_heads == 0u ||
        (num_heads % num_kv_heads) != 0u || (num_heads / num_kv_heads) > HeadsPerKv ||
        num_heads > MaxHeads) return;
    const unsigned int block = blockIdx.x;
    const unsigned int head = block / chunk_count;
    const unsigned int chunk = block - head * chunk_count;
    if (head >= num_heads) return;
    __shared__ float warp_sums[Threads / 32u];
    const unsigned int lane = threadIdx.x;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned int visible_count = key_end - key_start;
    if (visible_count > score_capacity) return;
    unsigned int chunk_begin = key_start + chunk * chunk_size;
    unsigned int chunk_end = chunk_begin + chunk_size;
    if (chunk_end > key_end) chunk_end = key_end;
    if (chunk_begin >= chunk_end) return;
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int q_base = head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
    for (unsigned int ki = chunk_begin; ki < chunk_end; ++ki) {
        const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        const bool valid = physical_token != 0xffffffffu;
        const unsigned char* k_row = valid ? k + (size_t)physical_token * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            const unsigned int value_index = kv_head * head_dim + lane;
            const float key_value = format == 0u
                ? termite_tq_decode_polar4_at(k_row, value_index)
                : termite_tq_f16_value(k_row, value_index);
            partial = q[q_base + lane] * key_value;
        }
        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u && valid) scores[(size_t)head * score_capacity + (ki - key_start)] = dot * scale;
    }
    (void)total_sequence_len;
}

template<unsigned HeadDim, unsigned Threads, unsigned HeadsPerKv, unsigned MaxHeads, unsigned MaxScores, bool Tiled>
__device__ __forceinline__ void antfly_gqa_score_consumer(ANTFLY_GQA_SCORE_CONSUMER_PARAMS) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    const unsigned int head = blockIdx.x;
    const unsigned int output_tile = blockIdx.y;
    if (batch != 1u || q_seq_len != 1u || head >= num_heads ||
        head_dim != HeadDim || blockDim.x != Threads ||
        (Tiled && output_tile >= head_dim / 64u) || value_row_bytes == 0u ||
        (value_format != 0u && value_format != 2u) || score_capacity == 0u || score_capacity > MaxScores || num_kv_heads == 0u ||
        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > HeadsPerKv || num_heads > MaxHeads) return;
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha[MaxScores];
    __shared__ float shared_beta[MaxScores];
    const unsigned int tile_lane = threadIdx.x;
    const unsigned int lane = Tiled ? output_tile * 64u + tile_lane : tile_lane;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    if (key_start > key_end) key_start = key_end;
    if (key_end - key_start > score_capacity) return;
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    if (tile_lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
            const bool valid = physical_token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            if (valid) {
                const float score = scores[(size_t)head * score_capacity + (ki - key_start)];
                const float next_max = fmaxf(shared_max_score, score);
                shared_alpha[recurrence_index] = expf(shared_max_score - next_max);
                shared_beta[recurrence_index] = expf(score - next_max);
                shared_denom = shared_denom * shared_alpha[recurrence_index] + shared_beta[recurrence_index];
                shared_max_score = next_max;
            } else {
                shared_alpha[recurrence_index] = 1.0f;
                shared_beta[recurrence_index] = 0.0f;
            }
        }
    }
    __syncthreads();
    float acc = 0.0f;
    if (lane < head_dim) {
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
            const bool valid = physical_token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            acc *= shared_alpha[recurrence_index];
            if (valid) {
                const unsigned char* v_row = v + (size_t)physical_token * value_row_bytes;
                const unsigned int value_index = kv_head * head_dim + lane;
                const float value = value_format == 0u
                    ? reinterpret_cast<const float*>(v_row)[value_index]
                    : termite_tq_f16_value(v_row, value_index);
                acc += shared_beta[recurrence_index] * value;
            }
        }
    }
    if (lane < head_dim) dst[head * head_dim + lane] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    (void)total_sequence_len;
}

#endif
