// Copyright 2026 Antfly, Inc.
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

// Production generated attention kernel from graph/quant_kernel_compiler.zig.
// plan_id=cuda/attention/decode_1x/hd256/gqa16/score-prework/max4096/consumers-serial+tiled64-max512/f32/device_scalars
// source_id=antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v2
// kernel_id=antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v1
// serial_kernel_id=antfly_gqa_attention_decode_turboquant_score_prework_serial_hd256_f32_v1
// tiled64_kernel_id=antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd256_f32_v1
// reduction_kernel_id=antfly_gqa_attention_decode_turboquant_score_prework_serial_hd256_f32_v1
// production_baseline=termite_gqa_attention_decode_turboquant_fast_f32
// production_enabled=true
// Default-on automatic SM89 selection; rollback: ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK=0.
// Tiled consumer opt-in: ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_SCORE_PREWORK_CONSUMER=tiled64.

#include <math.h>
#include <cuda_fp16.h>

__device__ __forceinline__ float termite_warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

__device__ __forceinline__ float termite_block_reduce_sum_f32(float value, float* warp_sums) {
    unsigned int lane = threadIdx.x & 31u;
    unsigned int warp = threadIdx.x >> 5;
    unsigned int warp_count = (blockDim.x + 31u) >> 5;
    value = termite_warp_reduce_sum_f32(value);
    if (lane == 0u) warp_sums[warp] = value;
    __syncthreads();
    value = (warp == 0u && lane < warp_count) ? warp_sums[lane] : 0.0f;
    if (warp == 0u) value = termite_warp_reduce_sum_f32(value);
    return value;
}

__device__ __forceinline__ float termite_tq_decode_polar4_scalar(unsigned char code) {
    return ((float)(code & 0x0fu) / 7.5f) - 1.0f;
}

__device__ __forceinline__ float termite_tq_decode_polar4_at(const unsigned char* encoded, unsigned int value_index) {
    unsigned char packed = encoded[value_index >> 1];
    unsigned char code = (value_index & 1u) == 0u ? (packed & 0x0fu) : ((packed >> 4) & 0x0fu);
    return termite_tq_decode_polar4_scalar(code);
}

__device__ __forceinline__ unsigned int termite_tq_physical_token(
    unsigned int logical_token,
    const unsigned int* block_table,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int physical_token_capacity
) {
    unsigned long long physical = (unsigned long long)logical_token;
    if (block_table != 0 && block_count != 0u && page_size_tokens != 0u) {
        unsigned int block_index = logical_token / page_size_tokens;
        if (block_index >= block_count) return 0xffffffffu;
        unsigned int token_offset = logical_token - block_index * page_size_tokens;
        const unsigned long long physical_block = (unsigned long long)block_table[block_index];
        physical = physical_block * (unsigned long long)page_size_tokens +
            (unsigned long long)token_offset;
    }
    if (physical > 0xffffffffull ||
        physical >= (unsigned long long)physical_token_capacity) return 0xffffffffu;
    return (unsigned int)physical;
}

__device__ __forceinline__ float termite_tq_f16_value(const unsigned char* row, unsigned int value_index) {
    const unsigned char* src = row + value_index * 2u;
    unsigned short raw = (unsigned short)src[0] | ((unsigned short)src[1] << 8);
    return __half2float(__ushort_as_half(raw));
}

extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v1(
    float* scores,
    const float* q,
    const unsigned char* k,
    const unsigned int* block_table,
    unsigned int batch,
    unsigned int q_seq_len,
    unsigned int kv_seq_len,
    unsigned int num_heads,
    unsigned int num_kv_heads,
    unsigned int head_dim,
    unsigned int query_position_offset,
    unsigned int kv_position_offset,
    unsigned int sliding_window,
    unsigned int total_sequence_len,
    unsigned int key_row_bytes,
    unsigned int base_key_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int format,
    unsigned int physical_token_capacity,
    unsigned int score_capacity,
    unsigned int chunk_size,
    unsigned int chunk_count,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (batch != 1u || q_seq_len != 1u || head_dim != 256u || blockDim.x != 256u ||
        score_capacity == 0u || score_capacity > 4096u || chunk_size == 0u ||
        chunk_count != 128u || key_row_bytes == 0u || base_key_row_bytes != key_row_bytes ||
        (format != 0u && format != 2u) || num_kv_heads == 0u || num_heads == 0u ||
        (num_heads % num_kv_heads) != 0u || (num_heads / num_kv_heads) > 16u ||
        num_heads > 32u) return;
    const unsigned int block = blockIdx.x;
    const unsigned int head = block / chunk_count;
    const unsigned int chunk = block - head * chunk_count;
    if (head >= num_heads) return;
    __shared__ float warp_sums[8];
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


extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_serial_hd256_f32_v1(
    float* dst,
    const float* scores,
    const unsigned char* v,
    const unsigned int* block_table,
    unsigned int batch,
    unsigned int q_seq_len,
    unsigned int kv_seq_len,
    unsigned int num_heads,
    unsigned int num_kv_heads,
    unsigned int head_dim,
    unsigned int query_position_offset,
    unsigned int kv_position_offset,
    unsigned int sliding_window,
    unsigned int total_sequence_len,
    unsigned int value_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int value_format,
    unsigned int physical_token_capacity,
    unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    const unsigned int head = blockIdx.x;
    if (batch != 1u || q_seq_len != 1u || head >= num_heads ||
        head_dim != 256u || blockDim.x != 256u || value_row_bytes == 0u ||
        (value_format != 0u && value_format != 2u) || score_capacity == 0u || score_capacity > 4096u || num_kv_heads == 0u ||
        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha[4096];
    __shared__ float shared_beta[4096];
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
    if (key_start > key_end) key_start = key_end;
    if (key_end - key_start > score_capacity) return;
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    if (lane == 0u) {
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


extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd256_f32_v1(
    float* dst,
    const float* scores,
    const unsigned char* v,
    const unsigned int* block_table,
    unsigned int batch,
    unsigned int q_seq_len,
    unsigned int kv_seq_len,
    unsigned int num_heads,
    unsigned int num_kv_heads,
    unsigned int head_dim,
    unsigned int query_position_offset,
    unsigned int kv_position_offset,
    unsigned int sliding_window,
    unsigned int total_sequence_len,
    unsigned int value_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int value_format,
    unsigned int physical_token_capacity,
    unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    const unsigned int head = blockIdx.x;
    const unsigned int output_tile = blockIdx.y;
    if (batch != 1u || q_seq_len != 1u || head >= num_heads ||
        head_dim != 256u || blockDim.x != 64u || output_tile >= head_dim / 64u ||
        value_row_bytes == 0u || (value_format != 0u && value_format != 2u) ||
        score_capacity == 0u || score_capacity > 512u || num_kv_heads == 0u ||
        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha[512];
    __shared__ float shared_beta[512];
    const unsigned int tile_lane = threadIdx.x;
    const unsigned int lane = output_tile * 64u + tile_lane;
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
