// Default-off generated split-K online decode from graph/quant_kernel_compiler.zig.
// plan_id=cuda/attention/decode_1x/splitk-online/sm89/hd512/gqa8/split64/t128/page16/swa0/f32q-f16kv-f32o
// source_id=antfly_gqa_attention_decode_splitk_online_sm89_hd512_global_f16_f32_v1
// kernel_id=antfly_gqa_attention_decode_splitk_online_sm89_hd512_global_f16_f32_v1
// production_baseline=termite_gqa_attention_decode_turboquant_fast_f32
// runtime_default_enabled=false
// exact_token_parity_qualified=false
#define ANTFLY_SPLITK_ONLINE_NAMESPACE antfly_splitk_online_decode_sm89_hd512_global_f16_f32_v1
#define ANTFLY_SPLITK_ONLINE_KERNEL antfly_gqa_attention_decode_splitk_online_sm89_hd512_global_f16_f32_v1
#define ANTFLY_SPLITK_ONLINE_HEAD_DIM 512
#define ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW 0
#define ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS 4096
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

// Typed, default-off SM89 q=1 paged-GQA split-K online-softmax template.
//
// One CTA owns one (query head, chronological KV split). It publishes a stable
// online-softmax partial into a persistent, route-exclusive workspace. The
// last arriving CTA for each head observes all partials after `__threadfence`,
// merges them in fixed chronological split order, writes F32 output, and
// re-arms the head-local completion counter. The same stream-ordering contract
// works for ordinary launches and CUDA graph replay without host-side memset.

#include <cuda_fp16.h>
#include <float.h>
#include <stddef.h>
#include <stdint.h>

#ifndef ANTFLY_SPLITK_ONLINE_NAMESPACE
#error "ANTFLY_SPLITK_ONLINE_NAMESPACE must name a unique renderer-owned namespace"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_KERNEL
#error "ANTFLY_SPLITK_ONLINE_KERNEL must name the exported production entry point"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_HEAD_DIM
#error "ANTFLY_SPLITK_ONLINE_HEAD_DIM must be 256 or 512"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW
#error "ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW must be 512 for HD256 or 0 for HD512"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS
#error "ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS must match the qualified policy"
#endif

namespace ANTFLY_SPLITK_ONLINE_NAMESPACE {

constexpr unsigned kHeadDim = ANTFLY_SPLITK_ONLINE_HEAD_DIM;
constexpr unsigned kSlidingWindow = ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW;
constexpr unsigned kMaxVisibleTokens = ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS;
constexpr unsigned kHeads = 8u;
constexpr unsigned kKvHeads = 1u;
constexpr unsigned kSplits = 64u;
constexpr unsigned kThreads = 128u;
constexpr unsigned kPageTokens = 16u;
constexpr unsigned kF16Format = 2u;
constexpr unsigned kInvalidToken = 0xffffffffu;
constexpr unsigned kMaxItems = kHeadDim / kThreads;

__device__ __forceinline__ float warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

__device__ __forceinline__ float block_reduce_sum_f32(
    float value,
    float* warp_sums
) {
    const unsigned lane = threadIdx.x & 31u;
    const unsigned warp = threadIdx.x >> 5u;
    value = warp_reduce_sum_f32(value);
    if (lane == 0u) warp_sums[warp] = value;
    __syncthreads();
    value = (warp == 0u && lane < kThreads / 32u)
        ? warp_sums[lane]
        : 0.0f;
    if (warp == 0u) value = warp_reduce_sum_f32(value);
    return value;
}

__device__ __forceinline__ unsigned physical_token(
    unsigned logical_token,
    const unsigned* block_table,
    unsigned block_count,
    unsigned physical_token_capacity
) {
    if (block_table == nullptr) {
        return logical_token < physical_token_capacity
            ? logical_token
            : kInvalidToken;
    }
    const unsigned block_index = logical_token / kPageTokens;
    if (block_index >= block_count) return kInvalidToken;
    const unsigned token_offset = logical_token - block_index * kPageTokens;
    const unsigned long long physical =
        (unsigned long long)block_table[block_index] * kPageTokens +
        token_offset;
    if (physical >= (unsigned long long)physical_token_capacity) {
        return kInvalidToken;
    }
    return (unsigned)physical;
}

__device__ __forceinline__ float f16_value(
    const unsigned char* row,
    unsigned index
) {
    return __half2float(reinterpret_cast<const __half*>(row)[index]);
}

}  // namespace ANTFLY_SPLITK_ONLINE_NAMESPACE

extern "C" __global__ void ANTFLY_SPLITK_ONLINE_KERNEL(
    float* dst,
    unsigned* completion_counters,
    // Volatile is part of the cross-CTA publication contract: after each
    // producer's __threadfence and atomic counter increment, the last CTA must
    // reload globally visible partials rather than satisfy reads from stale L1.
    volatile float* partial_values,
    volatile float* partial_max,
    volatile float* partial_denom,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned* block_table,
    unsigned batch,
    unsigned q_seq_len,
    unsigned kv_seq_len,
    unsigned num_heads,
    unsigned num_kv_heads,
    unsigned head_dim,
    unsigned query_position_offset,
    unsigned kv_position_offset,
    unsigned sliding_window,
    unsigned total_sequence_len,
    unsigned key_row_bytes,
    unsigned base_key_row_bytes,
    unsigned value_row_bytes,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned key_format,
    unsigned value_format,
    unsigned physical_token_capacity,
    unsigned score_capacity,
    const unsigned* decode_scalars
) {
    using namespace ANTFLY_SPLITK_ONLINE_NAMESPACE;
    if (decode_scalars != nullptr) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    constexpr unsigned kRowBytes = kHeadDim * sizeof(__half);
    if (dst == nullptr || completion_counters == nullptr ||
        partial_values == nullptr || partial_max == nullptr ||
        partial_denom == nullptr || q == nullptr || k == nullptr ||
        v == nullptr || batch != 1u ||
        q_seq_len != 1u || num_heads != kHeads ||
        num_kv_heads != kKvHeads || head_dim != kHeadDim ||
        blockDim.x != kThreads || sliding_window != kSlidingWindow ||
        key_row_bytes != kRowBytes || base_key_row_bytes != kRowBytes ||
        value_row_bytes != kRowBytes ||
        ((block_table == nullptr) != (block_count == 0u)) ||
        page_size_tokens != kPageTokens || key_format != kF16Format ||
        value_format != kF16Format || physical_token_capacity == 0u ||
        physical_token_capacity % kPageTokens != 0u ||
        score_capacity != kMaxVisibleTokens) {
        return;
    }

    const unsigned block = blockIdx.x;
    const unsigned head = block / kSplits;
    const unsigned split = block - head * kSplits;
    if (head >= kHeads) return;

    unsigned key_start = 0u;
    unsigned key_end = 0u;
    if (kv_seq_len != 0u && query_position_offset >= kv_position_offset) {
        const unsigned visible = query_position_offset - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (kSlidingWindow != 0u) {
            const unsigned window_start_abs =
                query_position_offset + 1u > kSlidingWindow
                ? query_position_offset + 1u - kSlidingWindow
                : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned visible_count = key_end - key_start;
    // Host admission rejects dynamic replay scalars outside the qualified
    // range before graph launch. Still let every CTA publish an empty partial
    // if an invariant is ever violated: the last-CTA protocol then writes a
    // deterministic zero row and re-arms the counter instead of leaving stale
    // output or a partially advanced completion counter.
    const bool scalar_contract_valid =
        kv_seq_len != 0u && kv_seq_len <= 4096u &&
        total_sequence_len != 0u &&
        total_sequence_len <= 4096u &&
        query_position_offset != 0xffffffffu &&
        query_position_offset + 1u == total_sequence_len &&
        kv_position_offset <= total_sequence_len &&
        kv_seq_len == total_sequence_len - kv_position_offset &&
        (block_table != nullptr || kv_seq_len <= physical_token_capacity) &&
        (block_table == nullptr ||
            block_count >= (kv_seq_len + kPageTokens - 1u) / kPageTokens);
    const bool visible_valid = scalar_contract_valid &&
        visible_count != 0u && visible_count <= kMaxVisibleTokens;
    const unsigned split_begin = visible_valid
        ? key_start +
            (unsigned)(((unsigned long long)visible_count * split) / kSplits)
        : 0u;
    const unsigned split_end = visible_valid
        ? key_start +
            (unsigned)(((unsigned long long)visible_count * (split + 1u)) / kSplits)
        : 0u;
    const unsigned tid = threadIdx.x;

    float q_items[kMaxItems];
    float acc[kMaxItems];
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) {
        const unsigned dimension = tid + item * kThreads;
        q_items[item] = q[head * kHeadDim + dimension];
        acc[item] = 0.0f;
    }

    __shared__ float warp_sums[kThreads / 32u];
    __shared__ float running_max;
    __shared__ float running_denom;
    __shared__ float alpha;
    __shared__ float beta;
    __shared__ float scale_input;
    if (tid == 0u) {
        running_max = -FLT_MAX;
        running_denom = 0.0f;
        scale_input = (float)kHeadDim;
    }
    __syncthreads();
    float scale;
    asm volatile("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    for (unsigned logical_token = split_begin;
         logical_token < split_end;
         ++logical_token) {
        const unsigned token = physical_token(
            logical_token, block_table, block_count, physical_token_capacity);
        const bool valid = token != kInvalidToken;
        const unsigned char* k_row = valid
            ? k + (size_t)token * key_row_bytes
            : k;
        float dot_item = 0.0f;
#pragma unroll
        for (unsigned item = 0u; item < kMaxItems; ++item) {
            const unsigned dimension = tid + item * kThreads;
            if (valid) {
                dot_item += q_items[item] * f16_value(k_row, dimension);
            }
        }
        const float dot = block_reduce_sum_f32(dot_item, warp_sums);
        if (tid == 0u) {
            if (valid) {
                const float score = dot * scale;
                const float next_max = fmaxf(running_max, score);
                alpha = running_denom > 0.0f
                    ? expf(running_max - next_max)
                    : 0.0f;
                beta = expf(score - next_max);
                running_denom = running_denom * alpha + beta;
                running_max = next_max;
            } else {
                alpha = 1.0f;
                beta = 0.0f;
            }
        }
        __syncthreads();
        const unsigned char* v_row = valid
            ? v + (size_t)token * value_row_bytes
            : v;
#pragma unroll
        for (unsigned item = 0u; item < kMaxItems; ++item) {
            const unsigned dimension = tid + item * kThreads;
            const float value = valid ? f16_value(v_row, dimension) : 0.0f;
            acc[item] = acc[item] * alpha + beta * value;
        }
    }

    const size_t partial = (size_t)head * kSplits + split;
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) {
        const unsigned dimension = tid + item * kThreads;
        partial_values[partial * kHeadDim + dimension] = acc[item];
    }
    if (tid == 0u) {
        partial_max[partial] = running_max;
        partial_denom[partial] = running_denom;
    }
    __threadfence();
    __syncthreads();

    __shared__ unsigned is_last_split;
    if (tid == 0u) {
        is_last_split = atomicAdd(&completion_counters[head], 1u) == kSplits - 1u;
    }
    __syncthreads();
    if (!is_last_split) return;

    __shared__ float merge_alpha[kSplits];
    __shared__ float merge_beta[kSplits];
    __shared__ float final_denom;
    if (tid == 0u) {
        float merge_max = -FLT_MAX;
        float merge_denom = 0.0f;
        for (unsigned merge_split = 0u;
             merge_split < kSplits;
             ++merge_split) {
            const size_t merge_partial = (size_t)head * kSplits + merge_split;
            const float split_denom = partial_denom[merge_partial];
            if (split_denom > 0.0f) {
                const float split_max = partial_max[merge_partial];
                const float next_max = fmaxf(merge_max, split_max);
                const float merge_a = merge_denom > 0.0f
                    ? expf(merge_max - next_max)
                    : 0.0f;
                const float merge_b = expf(split_max - next_max);
                merge_alpha[merge_split] = merge_a;
                merge_beta[merge_split] = merge_b;
                merge_denom = merge_denom * merge_a + split_denom * merge_b;
                merge_max = next_max;
            } else {
                merge_alpha[merge_split] = 1.0f;
                merge_beta[merge_split] = 0.0f;
            }
        }
        final_denom = merge_denom;
    }
    __syncthreads();

    float merge_acc[kMaxItems];
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) merge_acc[item] = 0.0f;
    for (unsigned merge_split = 0u;
         merge_split < kSplits;
         ++merge_split) {
        const size_t merge_partial = (size_t)head * kSplits + merge_split;
#pragma unroll
        for (unsigned item = 0u; item < kMaxItems; ++item) {
            const unsigned dimension = tid + item * kThreads;
            merge_acc[item] = merge_acc[item] * merge_alpha[merge_split] +
                merge_beta[merge_split] *
                    partial_values[merge_partial * kHeadDim + dimension];
        }
    }
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) {
        const unsigned dimension = tid + item * kThreads;
        dst[head * kHeadDim + dimension] = final_denom > 0.0f
            ? merge_acc[item] / final_denom
            : 0.0f;
    }
    if (tid == 0u) atomicExch(&completion_counters[head], 0u);
    asm volatile("" : : "r"(total_sequence_len));
}
#undef ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS
#undef ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW
#undef ANTFLY_SPLITK_ONLINE_HEAD_DIM
#undef ANTFLY_SPLITK_ONLINE_KERNEL
#undef ANTFLY_SPLITK_ONLINE_NAMESPACE
