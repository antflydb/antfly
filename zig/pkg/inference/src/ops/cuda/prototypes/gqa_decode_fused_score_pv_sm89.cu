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

// Standalone, default-off SM89 experiment.  This file is deliberately not
// included by the canonical CUDA bundle and exports prototype-only symbols.
//
// The first experiment fuses the production q=1 score-prework producer with
// its exact chronological softmax/PV consumer.  The second keeps the parallel
// canonical score producer, materializes chronological recurrence
// coefficients once per head, and consumes them from 64-column PV CTAs.  Both
// retain the checked-in F32 operation order and remain unreachable at runtime.

#include <cuda_fp16.h>

namespace antfly_gqa_decode_fused_prototype {

__device__ __forceinline__ float warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

__device__ __forceinline__ float block_reduce_sum_f32(float value, float* warp_sums) {
    const unsigned int lane = threadIdx.x & 31u;
    const unsigned int warp = threadIdx.x >> 5;
    const unsigned int warp_count = (blockDim.x + 31u) >> 5;
    value = warp_reduce_sum_f32(value);
    if (lane == 0u) warp_sums[warp] = value;
    __syncthreads();
    value = (warp == 0u && lane < warp_count) ? warp_sums[lane] : 0.0f;
    if (warp == 0u) value = warp_reduce_sum_f32(value);
    return value;
}

__device__ __forceinline__ unsigned int physical_token(
    unsigned int logical_token,
    const unsigned int* block_table,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int physical_token_capacity
) {
    unsigned long long physical = (unsigned long long)logical_token;
    if (block_table != 0 && block_count != 0u && page_size_tokens != 0u) {
        const unsigned int block_index = logical_token / page_size_tokens;
        if (block_index >= block_count) return 0xffffffffu;
        const unsigned int token_offset = logical_token - block_index * page_size_tokens;
        const unsigned long long physical_block = (unsigned long long)block_table[block_index];
        physical = physical_block * (unsigned long long)page_size_tokens +
            (unsigned long long)token_offset;
    }
    if (physical > 0xffffffffull ||
        physical >= (unsigned long long)physical_token_capacity) return 0xffffffffu;
    return (unsigned int)physical;
}

__device__ __forceinline__ float f16_value(const unsigned char* row, unsigned int value_index) {
    const unsigned char* src = row + value_index * 2u;
    const unsigned short raw = (unsigned short)src[0] | ((unsigned short)src[1] << 8);
    return __half2float(__ushort_as_half(raw));
}

template <unsigned int HeadDim, unsigned int SpecializedWindow>
__device__ __forceinline__ void fused_score_softmax_pv(
    float* dst,
    const float* q,
    const unsigned char* k,
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
    unsigned int key_row_bytes,
    unsigned int base_key_row_bytes,
    unsigned int value_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int key_format,
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
        head_dim != HeadDim || blockDim.x != HeadDim || key_row_bytes == 0u ||
        base_key_row_bytes != key_row_bytes || value_row_bytes == 0u ||
        key_format != 2u || value_format != 2u || score_capacity == 0u ||
        score_capacity > 4096u || num_kv_heads == 0u || num_heads == 0u ||
        (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u ||
        sliding_window != SpecializedWindow) return;

    __shared__ float warp_sums[HeadDim / 32u];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    __shared__ float shared_scale_input;
    __shared__ unsigned int shared_physical_token;

    const unsigned int lane = threadIdx.x;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs =
                (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
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
    const unsigned int q_base = head * HeadDim;
    // The production producer evaluates rsqrt.approx from its runtime
    // head_dim argument.  Materialize that argument through shared memory so
    // ptxas cannot constant-fold HD512 to a mathematically rounded literal;
    // that literal differs from the hardware MUFU.RSQ approximation.
    if (lane == 0u) shared_scale_input = (float)head_dim;
    __syncthreads();
    const float scale_input = shared_scale_input;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        const unsigned int token = physical_token(
            ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        const bool valid = token != 0xffffffffu;
        const unsigned char* k_row = valid ? k + (size_t)token * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < HeadDim) {
            const unsigned int value_index = kv_head * HeadDim + lane;
            const float key_value = f16_value(k_row, value_index);
            partial = q[q_base + lane] * key_value;
        }
        const float dot = block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            shared_physical_token = token;
            if (valid) {
                const float score = dot * scale;
                const float next_max = fmaxf(shared_max_score, score);
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

        acc *= shared_alpha;
        if (shared_physical_token != 0xffffffffu) {
            const unsigned char* v_row =
                v + (size_t)shared_physical_token * value_row_bytes;
            const unsigned int value_index = kv_head * HeadDim + lane;
            const float value = f16_value(v_row, value_index);
            acc += shared_beta * value;
        }
        // Prevent lane 0 from publishing the next key's recurrence scalars
        // before every warp has consumed the current ones.
        __syncthreads();
    }

    dst[head * HeadDim + lane] =
        shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    // Match the canonical ABI even though decode visibility is fully defined
    // by the offsets and kv_seq_len.  The empty asm is observable only to the
    // compiler and emits no instruction.
    asm volatile ("" : : "r"(total_sequence_len));
}

template <unsigned int HeadDim, unsigned int SpecializedWindow, unsigned int MaxKvTokens>
__device__ __forceinline__ void coefficient_precompute(
    float* alpha,
    float* beta,
    float* denom,
    const float* scores,
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
    unsigned int block_count,
    unsigned int page_size_tokens,
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
    if (threadIdx.x != 0u) return;
    if (batch != 1u || q_seq_len != 1u || head >= num_heads ||
        head_dim != HeadDim || sliding_window != SpecializedWindow ||
        score_capacity != MaxKvTokens || num_kv_heads == 0u ||
        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;

    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs =
                (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    if (key_start > key_end) key_start = key_end;
    if (key_end - key_start > score_capacity) return;

    float running_max = -3.402823466e+38f;
    float running_denom = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        const unsigned int token = physical_token(
            ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        const bool valid = token != 0xffffffffu;
        const unsigned int recurrence_index = ki - key_start;
        const size_t coefficient_index =
            (size_t)head * score_capacity + recurrence_index;
        if (valid) {
            const float score = scores[coefficient_index];
            const float next_max = fmaxf(running_max, score);
            const float next_alpha = expf(running_max - next_max);
            const float next_beta = expf(score - next_max);
            alpha[coefficient_index] = next_alpha;
            beta[coefficient_index] = next_beta;
            running_denom = running_denom * next_alpha + next_beta;
            running_max = next_max;
        } else {
            alpha[coefficient_index] = 1.0f;
            beta[coefficient_index] = 0.0f;
        }
    }
    denom[head] = running_denom;
    asm volatile ("" : : "r"(total_sequence_len));
}

template <unsigned int HeadDim, unsigned int SpecializedWindow, unsigned int MaxKvTokens>
__device__ __forceinline__ void coefficient_pv_shared(
    float* dst,
    const float* alpha,
    const float* beta,
    const float* denom,
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
        head_dim != HeadDim || blockDim.x != 64u ||
        output_tile >= HeadDim / 64u || sliding_window != SpecializedWindow ||
        value_row_bytes == 0u || value_format != 2u ||
        score_capacity != MaxKvTokens || num_kv_heads == 0u ||
        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;

    __shared__ float shared_alpha[MaxKvTokens];
    __shared__ float shared_beta[MaxKvTokens];
    __shared__ float shared_denom;

    const unsigned int tile_lane = threadIdx.x;
    const unsigned int lane = output_tile * 64u + tile_lane;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs =
                (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    if (key_start > key_end) key_start = key_end;
    const unsigned int visible_count = key_end - key_start;
    if (visible_count > score_capacity) return;

    const size_t coefficient_base = (size_t)head * score_capacity;
    for (unsigned int index = tile_lane; index < visible_count; index += 64u) {
        shared_alpha[index] = alpha[coefficient_base + index];
        shared_beta[index] = beta[coefficient_base + index];
    }
    if (tile_lane == 0u) shared_denom = denom[head];
    __syncthreads();

    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        const unsigned int token = physical_token(
            ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        const bool valid = token != 0xffffffffu;
        const unsigned int recurrence_index = ki - key_start;
        acc *= shared_alpha[recurrence_index];
        if (valid) {
            const unsigned char* v_row = v + (size_t)token * value_row_bytes;
            const unsigned int value_index = kv_head * HeadDim + lane;
            const float value = f16_value(v_row, value_index);
            acc += shared_beta[recurrence_index] * value;
        }
    }
    dst[head * HeadDim + lane] =
        shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    asm volatile ("" : : "r"(total_sequence_len));
}

}  // namespace antfly_gqa_decode_fused_prototype

extern "C" __global__ void antfly_gqa_attention_decode_fused_score_pv_hd256_swa512_f32_prototype(
    float* dst, const float* q, const unsigned char* k, const unsigned char* v,
    const unsigned int* block_table, unsigned int batch, unsigned int q_seq_len,
    unsigned int kv_seq_len, unsigned int num_heads, unsigned int num_kv_heads,
    unsigned int head_dim, unsigned int query_position_offset,
    unsigned int kv_position_offset, unsigned int sliding_window,
    unsigned int total_sequence_len, unsigned int key_row_bytes,
    unsigned int base_key_row_bytes, unsigned int value_row_bytes,
    unsigned int block_count, unsigned int page_size_tokens,
    unsigned int key_format, unsigned int value_format,
    unsigned int physical_token_capacity, unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    antfly_gqa_decode_fused_prototype::fused_score_softmax_pv<256u, 512u>(
        dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len, num_heads,
        num_kv_heads, head_dim, query_position_offset, kv_position_offset,
        sliding_window, total_sequence_len, key_row_bytes, base_key_row_bytes,
        value_row_bytes, block_count, page_size_tokens, key_format, value_format,
        physical_token_capacity, score_capacity, decode_scalars);
}

extern "C" __global__ void antfly_gqa_attention_decode_fused_score_pv_hd512_global_f32_prototype(
    float* dst, const float* q, const unsigned char* k, const unsigned char* v,
    const unsigned int* block_table, unsigned int batch, unsigned int q_seq_len,
    unsigned int kv_seq_len, unsigned int num_heads, unsigned int num_kv_heads,
    unsigned int head_dim, unsigned int query_position_offset,
    unsigned int kv_position_offset, unsigned int sliding_window,
    unsigned int total_sequence_len, unsigned int key_row_bytes,
    unsigned int base_key_row_bytes, unsigned int value_row_bytes,
    unsigned int block_count, unsigned int page_size_tokens,
    unsigned int key_format, unsigned int value_format,
    unsigned int physical_token_capacity, unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    antfly_gqa_decode_fused_prototype::fused_score_softmax_pv<512u, 0u>(
        dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len, num_heads,
        num_kv_heads, head_dim, query_position_offset, kv_position_offset,
        sliding_window, total_sequence_len, key_row_bytes, base_key_row_bytes,
        value_row_bytes, block_count, page_size_tokens, key_format, value_format,
        physical_token_capacity, score_capacity, decode_scalars);
}

extern "C" __global__ void antfly_gqa_attention_decode_score_coefficients_hd256_swa512_f32_prototype(
    float* alpha, float* beta, float* denom, const float* scores,
    const unsigned int* block_table, unsigned int batch,
    unsigned int q_seq_len, unsigned int kv_seq_len, unsigned int num_heads,
    unsigned int num_kv_heads, unsigned int head_dim,
    unsigned int query_position_offset, unsigned int kv_position_offset,
    unsigned int sliding_window, unsigned int total_sequence_len,
    unsigned int block_count, unsigned int page_size_tokens,
    unsigned int physical_token_capacity, unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    antfly_gqa_decode_fused_prototype::coefficient_precompute<256u, 512u, 512u>(
        alpha, beta, denom, scores, block_table, batch, q_seq_len, kv_seq_len,
        num_heads, num_kv_heads, head_dim, query_position_offset,
        kv_position_offset, sliding_window, total_sequence_len, block_count,
        page_size_tokens, physical_token_capacity, score_capacity,
        decode_scalars);
}

extern "C" __global__ void antfly_gqa_attention_decode_score_coefficients_hd512_global_f32_prototype(
    float* alpha, float* beta, float* denom, const float* scores,
    const unsigned int* block_table, unsigned int batch,
    unsigned int q_seq_len, unsigned int kv_seq_len, unsigned int num_heads,
    unsigned int num_kv_heads, unsigned int head_dim,
    unsigned int query_position_offset, unsigned int kv_position_offset,
    unsigned int sliding_window, unsigned int total_sequence_len,
    unsigned int block_count, unsigned int page_size_tokens,
    unsigned int physical_token_capacity, unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    antfly_gqa_decode_fused_prototype::coefficient_precompute<512u, 0u, 4096u>(
        alpha, beta, denom, scores, block_table, batch, q_seq_len, kv_seq_len,
        num_heads, num_kv_heads, head_dim, query_position_offset,
        kv_position_offset, sliding_window, total_sequence_len, block_count,
        page_size_tokens, physical_token_capacity, score_capacity,
        decode_scalars);
}

extern "C" __global__ void antfly_gqa_attention_decode_coefficients_pv_shared_hd256_swa512_f32_prototype(
    float* dst, const float* alpha, const float* beta, const float* denom,
    const unsigned char* v, const unsigned int* block_table,
    unsigned int batch, unsigned int q_seq_len, unsigned int kv_seq_len,
    unsigned int num_heads, unsigned int num_kv_heads, unsigned int head_dim,
    unsigned int query_position_offset, unsigned int kv_position_offset,
    unsigned int sliding_window, unsigned int total_sequence_len,
    unsigned int value_row_bytes, unsigned int block_count,
    unsigned int page_size_tokens, unsigned int value_format,
    unsigned int physical_token_capacity, unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    antfly_gqa_decode_fused_prototype::coefficient_pv_shared<256u, 512u, 512u>(
        dst, alpha, beta, denom, v, block_table, batch, q_seq_len, kv_seq_len,
        num_heads, num_kv_heads, head_dim, query_position_offset,
        kv_position_offset, sliding_window, total_sequence_len, value_row_bytes,
        block_count, page_size_tokens, value_format, physical_token_capacity,
        score_capacity, decode_scalars);
}

extern "C" __global__ void antfly_gqa_attention_decode_coefficients_pv_shared_hd512_global_f32_prototype(
    float* dst, const float* alpha, const float* beta, const float* denom,
    const unsigned char* v, const unsigned int* block_table,
    unsigned int batch, unsigned int q_seq_len, unsigned int kv_seq_len,
    unsigned int num_heads, unsigned int num_kv_heads, unsigned int head_dim,
    unsigned int query_position_offset, unsigned int kv_position_offset,
    unsigned int sliding_window, unsigned int total_sequence_len,
    unsigned int value_row_bytes, unsigned int block_count,
    unsigned int page_size_tokens, unsigned int value_format,
    unsigned int physical_token_capacity, unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    antfly_gqa_decode_fused_prototype::coefficient_pv_shared<512u, 0u, 4096u>(
        dst, alpha, beta, denom, v, block_table, batch, q_seq_len, kv_seq_len,
        num_heads, num_kv_heads, head_dim, query_position_offset,
        kv_position_offset, sliding_window, total_sequence_len, value_row_bytes,
        block_count, page_size_tokens, value_format, physical_token_capacity,
        score_capacity, decode_scalars);
}
