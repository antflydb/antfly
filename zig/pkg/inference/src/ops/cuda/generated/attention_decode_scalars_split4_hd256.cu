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

// Dev-only generated attention kernel from graph/quant_kernel_compiler.zig.
// plan_id=cuda/attention/decode_1x/hd256/gqa16/split4/min512/f32/device_scalars
// source_id=antfly_gqa_attention_decode_split4_kv_hd256_f32_v1
// kernel_id=antfly_gqa_attention_decode_split4_kv_hd256_f32_stage1_v1
// serial_kernel_id=antfly_gqa_attention_decode_scalars_split4_hd256_f32_v1
// reduction_kernel_id=antfly_gqa_attention_decode_split4_kv_hd256_f32_stage2_v1
// production_baseline=termite_gqa_attention_decode_scalars_fast_f32
// production_enabled=false
// Runtime opt-in: ANTFLY_INFERENCE_CUDA_GENERATED_ATTENTION_DECODE=1.

#include <math.h>

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

extern "C" __global__ void antfly_gqa_attention_decode_scalars_split4_hd256_f32_v1(
    float* dst,
    const float* q,
    const float* k,
    const float* v,
    const unsigned char* attn_or_mask,
    const float* bias,
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
    unsigned int mask_len,
    unsigned int bias_mode,
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
    }
    if (kv_seq_len >= split_kv_min_tokens) return;
    const unsigned int block = blockIdx.x;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        block >= num_heads || head_dim != 256u || blockDim.x != 256u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    const unsigned int lane = threadIdx.x;
    const unsigned int head = block;
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
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        float partial = 0.0f;
        if (lane < head_dim) {
            const unsigned int k_base = ki * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            const float score = dot * scale;
            const float next_max = fmaxf(shared_max_score, score);
            shared_alpha = expf(shared_max_score - next_max);
            shared_beta = expf(score - next_max);
            shared_denom = shared_denom * shared_alpha + shared_beta;
            shared_max_score = next_max;
        }
        __syncthreads();
        if (lane < head_dim) {
            const unsigned int v_idx = ki * kv_hidden + kv_head * head_dim + lane;
            acc = acc * shared_alpha + shared_beta * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        const unsigned int out_idx = head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }

    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
}

extern "C" __global__ void antfly_gqa_attention_decode_split4_kv_hd256_f32_stage1_v1(
    float* partial_values,
    float* partial_max,
    float* partial_denom,
    const float* q,
    const float* k,
    const float* v,
    const unsigned char* attn_or_mask,
    const float* bias,
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
    unsigned int mask_len,
    unsigned int bias_mode,
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        head_dim != 256u || blockDim.x != 256u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    const unsigned int splits = 4u;
    const unsigned int split = blockIdx.x % splits;
    const unsigned int head_block = blockIdx.x / splits;
    const unsigned int head = head_block % num_heads;
    const unsigned int b = head_block / num_heads;
    if (b >= batch) return;
    const unsigned int heads_per_kv = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_kv;
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
    unsigned int split_begin = (kv_seq_len * split) / splits;
    unsigned int split_end = (kv_seq_len * (split + 1u)) / splits;
    if (split_begin < key_start) split_begin = key_start;
    if (split_end > key_end) split_end = key_end;
    if (split_begin > split_end) split_begin = split_end;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int q_hidden = num_heads * head_dim;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = (b * q_seq_len) * q_hidden + head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    __shared__ float warp_sums[8];
    __shared__ float head_max;
    __shared__ float head_denom;
    __shared__ float head_alpha;
    __shared__ float head_beta;
    float q_values[1];
    float acc[1];
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        q_values[item] = q[q_base + d];
        acc[item] = 0.0f;
    }
    if (tid == 0u) {
        head_max = -3.402823466e+38f;
        head_denom = 0.0f;
        head_alpha = 0.0f;
        head_beta = 0.0f;
    }
    __syncthreads();

    for (unsigned int ki = split_begin; ki < split_end; ++ki) {
        const unsigned int key_pos = kv_position_offset + ki;
        const unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        const bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        const bool future_blocked = key_pos > query_pos && !future_allowed;
        const bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        const bool valid = !(future_blocked || past_blocked);
        const unsigned int kv_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;

        float dot = 0.0f;
#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float key_value = valid ? k[kv_base + d] : 0.0f;
            dot += q_values[item] * key_value;
        }
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
            dot += __shfl_down_sync(0xffffffffu, dot, offset);
        }
        if (lane == 0u) warp_sums[warp] = dot;
        __syncthreads();

        float block_dot = (warp == 0u && lane < 8u) ? warp_sums[lane] : 0.0f;
        if (warp == 0u) {
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                block_dot += __shfl_down_sync(0xffffffffu, block_dot, offset);
            }
            if (lane == 0u) {
                float score = valid ? block_dot * scale : -3.402823466e+38f;
                if (valid && bias_mode == 1u) score += bias[head * kv_seq_len + ki];
                if (valid && bias_mode == 2u) score += bias[(b * num_heads + head) * kv_seq_len + ki];
                const float next_max = fmaxf(head_max, score);
                const float alpha = head_denom > 0.0f ? expf(head_max - next_max) : 0.0f;
                const float beta = valid ? expf(score - next_max) : 0.0f;
                head_denom = head_denom * alpha + beta;
                head_max = next_max;
                head_alpha = alpha;
                head_beta = beta;
            }
        }
        __syncthreads();

#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float value = valid ? v[kv_base + d] : 0.0f;
            acc[item] = acc[item] * head_alpha + head_beta * value;
        }
    }

    const unsigned int partial = (b * num_heads + head) * splits + split;
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        partial_values[(size_t)partial * head_dim + d] = acc[item];
    }
    if (tid == 0u) {
        partial_max[partial] = head_max;
        partial_denom[partial] = head_denom;
    }
}

extern "C" __global__ void antfly_gqa_attention_decode_split4_kv_hd256_f32_stage2_v1(
    float* dst,
    const float* partial_values,
    const float* partial_max,
    const float* partial_denom,
    unsigned int batch,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int kv_seq_len,
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) kv_seq_len = decode_scalars[2];
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || head_dim != 256u || blockDim.x != 256u || num_heads > 32u) return;
    const unsigned int head_block = blockIdx.x;
    if (head_block >= batch * num_heads) return;
    const unsigned int splits = 4u;
    __shared__ float merged_denom;
    __shared__ float merge_alpha[4];
    __shared__ float merge_beta[4];
    if (threadIdx.x == 0u) {
        float merged_max = -3.402823466e+38f;
        float denom = 0.0f;
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            const float local_denom = partial_denom[partial];
            if (local_denom > 0.0f) {
                const float next_max = fmaxf(merged_max, partial_max[partial]);
                const float alpha = denom > 0.0f ? expf(merged_max - next_max) : 0.0f;
                const float beta = expf(partial_max[partial] - next_max);
                denom = denom * alpha + local_denom * beta;
                merged_max = next_max;
                merge_alpha[split] = alpha;
                merge_beta[split] = beta;
            } else {
                merge_alpha[split] = 1.0f;
                merge_beta[split] = 0.0f;
            }
        }
        merged_denom = denom;
    }
    __syncthreads();
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = threadIdx.x + item * blockDim.x;
        float numerator = 0.0f;
#pragma unroll
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            numerator = numerator * merge_alpha[split] + partial_values[(size_t)partial * head_dim + d] * merge_beta[split];
        }
        dst[(size_t)head_block * head_dim + d] = merged_denom > 0.0f ? numerator / merged_denom : 0.0f;
    }
}
