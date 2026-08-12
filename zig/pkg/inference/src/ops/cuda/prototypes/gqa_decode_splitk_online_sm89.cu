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

// Standalone, default-off SM89 split-K experiment for q=1 paged GQA.  This
// translation unit is intentionally absent from the canonical CUDA bundle and
// exports prototype-only symbols.  Stage 1 partitions the visible KV range in
// chronological order and produces a stable online-softmax partial for each
// (query head, split).  Stage 2 merges those partials in the same chronological
// order.  Q and output are F32; paged K/V are F16, matching the production
// Gemma 4 decode contract.  No fast-math modes or tensor-core input narrowing
// are used.

#include <cuda_fp16.h>
#include <float.h>

namespace antfly_gqa_decode_splitk_online_prototype {

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
    const unsigned int lane = threadIdx.x & 31u;
    const unsigned int warp = threadIdx.x >> 5u;
    const unsigned int warp_count = blockDim.x >> 5u;
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
        const unsigned int token_offset =
            logical_token - block_index * page_size_tokens;
        physical =
            (unsigned long long)block_table[block_index] *
                (unsigned long long)page_size_tokens +
            (unsigned long long)token_offset;
    }
    if (physical > 0xffffffffull ||
        physical >= (unsigned long long)physical_token_capacity) {
        return 0xffffffffu;
    }
    return (unsigned int)physical;
}

__device__ __forceinline__ float f16_value(
    const unsigned char* row,
    unsigned int value_index
) {
    // The production F16 row contract is naturally two-byte aligned.  Keep
    // the typed load here so SASS uses one U16 transaction instead of two U8
    // transactions for every K and V scalar.
    return __half2float(reinterpret_cast<const __half*>(row)[value_index]);
}

__device__ __forceinline__ bool supported_block_size(
    unsigned int head_dim
) {
    return (blockDim.x == 128u || blockDim.x == 256u ||
            blockDim.x == 512u) &&
        blockDim.x <= head_dim;
}

template <
    unsigned int HeadDim,
    unsigned int SpecializedWindow,
    unsigned int MaxVisibleTokens,
    bool CompleteInLastCta,
    bool AccurateMerge = false>
__device__ __forceinline__ void splitk_stage1(
    float* dst,
    unsigned int* completion_counters,
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (batch != 1u || q_seq_len != 1u || head_dim != HeadDim ||
        !supported_block_size(head_dim) || num_kv_heads == 0u ||
        num_heads == 0u || num_heads > 32u ||
        (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u ||
        sliding_window != SpecializedWindow || key_format != 2u ||
        value_format != 2u || key_row_bytes == 0u ||
        base_key_row_bytes != key_row_bytes || value_row_bytes == 0u ||
        (key_row_bytes & 1u) != 0u || (value_row_bytes & 1u) != 0u ||
        key_row_bytes < num_kv_heads * HeadDim * 2u ||
        value_row_bytes < num_kv_heads * HeadDim * 2u ||
        page_size_tokens == 0u || physical_token_capacity == 0u ||
        score_capacity != MaxVisibleTokens || split_count == 0u ||
        split_count > 64u || (split_count & (split_count - 1u)) != 0u ||
        (CompleteInLastCta && (dst == 0 || completion_counters == 0))) {
        return;
    }

    const unsigned int block = blockIdx.x;
    const unsigned int head = block / split_count;
    const unsigned int split = block - head * split_count;
    if (head >= num_heads) return;

    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs =
                query_pos + 1u > sliding_window
                ? query_pos + 1u - sliding_window
                : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    if (key_start > key_end) key_start = key_end;
    const unsigned int visible_count = key_end - key_start;
    if (visible_count > MaxVisibleTokens) return;

    const unsigned int split_begin = key_start +
        (unsigned int)(((unsigned long long)visible_count * split) / split_count);
    const unsigned int split_end = key_start +
        (unsigned int)(((unsigned long long)visible_count * (split + 1u)) /
                       split_count);
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int q_base = head * HeadDim;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    constexpr unsigned int MaxItems = HeadDim / 128u;

    float q_items[MaxItems];
    float acc[MaxItems];
#pragma unroll
    for (unsigned int item = 0u; item < MaxItems; ++item) {
        const unsigned int dimension = tid + item * blockDim.x;
        q_items[item] = dimension < HeadDim ? q[q_base + dimension] : 0.0f;
        acc[item] = 0.0f;
    }

    __shared__ float warp_sums[16];
    __shared__ float running_max;
    __shared__ float running_denom;
    __shared__ float alpha;
    __shared__ float beta;
    __shared__ float scale_input;
    if (tid == 0u) {
        running_max = -FLT_MAX;
        running_denom = 0.0f;
        scale_input = (float)head_dim;
    }
    __syncthreads();
    float scale;
    asm volatile("rsqrt.approx.f32 %0, %1;"
                 : "=f"(scale)
                 : "f"(scale_input));

    for (unsigned int logical_token = split_begin;
         logical_token < split_end;
         ++logical_token) {
        const unsigned int token = physical_token(
            logical_token,
            block_table,
            block_count,
            page_size_tokens,
            physical_token_capacity);
        const bool valid = token != 0xffffffffu;
        const unsigned char* k_row =
            valid ? k + (size_t)token * key_row_bytes : k;
        float dot_item = 0.0f;
#pragma unroll
        for (unsigned int item = 0u; item < MaxItems; ++item) {
            const unsigned int dimension = tid + item * blockDim.x;
            if (valid && dimension < HeadDim) {
                dot_item += q_items[item] *
                    f16_value(k_row, kv_head * HeadDim + dimension);
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
        const unsigned char* v_row =
            valid ? v + (size_t)token * value_row_bytes : v;
#pragma unroll
        for (unsigned int item = 0u; item < MaxItems; ++item) {
            const unsigned int dimension = tid + item * blockDim.x;
            if (dimension < HeadDim) {
                const float value = valid
                    ? f16_value(v_row, kv_head * HeadDim + dimension)
                    : 0.0f;
                acc[item] = acc[item] * alpha + beta * value;
            }
        }
        // The next iteration's block-reduction barrier is also the hand-off
        // barrier for alpha/beta: lane zero cannot publish the next pair until
        // every warp has completed this accumulator update and entered that
        // reduction.  Avoiding a redundant third barrier per token is material
        // for the short HD256 split intervals.
    }

    const size_t partial = (size_t)head * split_count + split;
#pragma unroll
    for (unsigned int item = 0u; item < MaxItems; ++item) {
        const unsigned int dimension = tid + item * blockDim.x;
        if (dimension < HeadDim) {
            partial_values[partial * HeadDim + dimension] = acc[item];
        }
    }
    if (lane == 0u && (tid >> 5u) == 0u) {
        partial_max[partial] = running_max;
        partial_denom[partial] = running_denom;
    }

    if constexpr (CompleteInLastCta) {
        // Publish every thread's partial-value store before the head-local
        // arrival counter is incremented.  The final CTA therefore observes
        // all chronological partials, independent of CTA scheduling order.
        __threadfence();
        __syncthreads();
        __shared__ unsigned int is_last_split;
        if (tid == 0u) {
            is_last_split =
                atomicAdd(&completion_counters[head], 1u) == split_count - 1u;
        }
        __syncthreads();
        if (!is_last_split) return;

        if constexpr (AccurateMerge) {
            // Diagnostic only: retain the production split topology and F32
            // per-split recurrences, but accumulate the fixed-order partial
            // merge in F64.  Exponentials intentionally remain expf so this
            // isolates association/accumulation precision instead of changing
            // the softmax approximation itself.
            __shared__ double merge_alpha_f64[64];
            __shared__ double merge_beta_f64[64];
            __shared__ double merge_final_denom_f64;
            if (tid == 0u) {
                float merge_max = -FLT_MAX;
                double merge_denom = 0.0;
                for (unsigned int merge_split = 0u;
                     merge_split < split_count;
                     ++merge_split) {
                    const size_t merge_partial =
                        (size_t)head * split_count + merge_split;
                    const float split_denom = partial_denom[merge_partial];
                    if (split_denom > 0.0f) {
                        const float split_max = partial_max[merge_partial];
                        const float next_max = fmaxf(merge_max, split_max);
                        const float merge_a = merge_denom > 0.0
                            ? expf(merge_max - next_max)
                            : 0.0f;
                        const float merge_b = expf(split_max - next_max);
                        merge_alpha_f64[merge_split] = (double)merge_a;
                        merge_beta_f64[merge_split] = (double)merge_b;
                        merge_denom =
                            merge_denom * (double)merge_a +
                            (double)split_denom * (double)merge_b;
                        merge_max = next_max;
                    } else {
                        merge_alpha_f64[merge_split] = 1.0;
                        merge_beta_f64[merge_split] = 0.0;
                    }
                }
                merge_final_denom_f64 = merge_denom;
            }
            __syncthreads();

            double merge_acc_f64[MaxItems];
#pragma unroll
            for (unsigned int item = 0u; item < MaxItems; ++item) {
                merge_acc_f64[item] = 0.0;
            }
            for (unsigned int merge_split = 0u;
                 merge_split < split_count;
                 ++merge_split) {
                const size_t merge_partial =
                    (size_t)head * split_count + merge_split;
#pragma unroll
                for (unsigned int item = 0u; item < MaxItems; ++item) {
                    const unsigned int dimension =
                        tid + item * blockDim.x;
                    if (dimension < HeadDim) {
                        merge_acc_f64[item] =
                            merge_acc_f64[item] *
                                merge_alpha_f64[merge_split] +
                            merge_beta_f64[merge_split] *
                                (double)partial_values[
                                    merge_partial * HeadDim + dimension];
                    }
                }
            }
#pragma unroll
            for (unsigned int item = 0u; item < MaxItems; ++item) {
                const unsigned int dimension = tid + item * blockDim.x;
                if (dimension < HeadDim) {
                    dst[(size_t)head * HeadDim + dimension] =
                        merge_final_denom_f64 > 0.0
                        ? (float)(merge_acc_f64[item] /
                                  merge_final_denom_f64)
                        : 0.0f;
                }
            }
            if (tid == 0u) atomicExch(&completion_counters[head], 0u);
            return;
        }

        __shared__ float merge_alpha[64];
        __shared__ float merge_beta[64];
        __shared__ float merge_final_denom;
        if (tid == 0u) {
            float merge_max = -FLT_MAX;
            float merge_denom = 0.0f;
            for (unsigned int merge_split = 0u;
                 merge_split < split_count;
                 ++merge_split) {
                const size_t merge_partial =
                    (size_t)head * split_count + merge_split;
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
                    merge_denom =
                        merge_denom * merge_a + split_denom * merge_b;
                    merge_max = next_max;
                } else {
                    merge_alpha[merge_split] = 1.0f;
                    merge_beta[merge_split] = 0.0f;
                }
            }
            merge_final_denom = merge_denom;
        }
        __syncthreads();

        float merge_acc[MaxItems];
#pragma unroll
        for (unsigned int item = 0u; item < MaxItems; ++item) {
            merge_acc[item] = 0.0f;
        }
        for (unsigned int merge_split = 0u;
             merge_split < split_count;
             ++merge_split) {
            const size_t merge_partial =
                (size_t)head * split_count + merge_split;
#pragma unroll
            for (unsigned int item = 0u; item < MaxItems; ++item) {
                const unsigned int dimension = tid + item * blockDim.x;
                if (dimension < HeadDim) {
                    merge_acc[item] =
                        merge_acc[item] * merge_alpha[merge_split] +
                        merge_beta[merge_split] *
                            partial_values[
                                merge_partial * HeadDim + dimension];
                }
            }
        }
#pragma unroll
        for (unsigned int item = 0u; item < MaxItems; ++item) {
            const unsigned int dimension = tid + item * blockDim.x;
            if (dimension < HeadDim) {
                dst[(size_t)head * HeadDim + dimension] =
                    merge_final_denom > 0.0f
                    ? merge_acc[item] / merge_final_denom
                    : 0.0f;
            }
        }
        // All launches use the legacy/default stream and are serialized.  The
        // last CTA can safely re-arm its head-local counter for the next launch;
        // subsequent launches cannot observe it until this kernel completes.
        if (tid == 0u) atomicExch(&completion_counters[head], 0u);
    }
    asm volatile("" : : "r"(total_sequence_len));
}

template <unsigned int HeadDim>
__device__ __forceinline__ void splitk_stage2(
    float* dst,
    const float* partial_values,
    const float* partial_max,
    const float* partial_denom,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int split_count
) {
    if (head_dim != HeadDim || !supported_block_size(head_dim) ||
        num_heads == 0u || num_heads > 32u || split_count == 0u ||
        split_count > 64u || (split_count & (split_count - 1u)) != 0u) {
        return;
    }
    const unsigned int head = blockIdx.x;
    if (head >= num_heads) return;
    const unsigned int tid = threadIdx.x;
    constexpr unsigned int MaxItems = HeadDim / 128u;
    float acc[MaxItems];
#pragma unroll
    for (unsigned int item = 0u; item < MaxItems; ++item) acc[item] = 0.0f;

    __shared__ float merge_alpha[64];
    __shared__ float merge_beta[64];
    __shared__ float final_denom;
    if (tid == 0u) {
        float running_max = -FLT_MAX;
        float running_denom = 0.0f;
        // Partial max/denom are scalars.  Materialize the chronological merge
        // coefficients once, then let all value columns consume them after a
        // single barrier.  This retains the exact merge recurrence while
        // avoiding two block barriers per split.
        for (unsigned int split = 0u; split < split_count; ++split) {
            const size_t partial = (size_t)head * split_count + split;
            const float split_denom = partial_denom[partial];
            if (split_denom > 0.0f) {
                const float split_max = partial_max[partial];
                const float next_max = fmaxf(running_max, split_max);
                const float alpha = running_denom > 0.0f
                    ? expf(running_max - next_max)
                    : 0.0f;
                const float beta = expf(split_max - next_max);
                merge_alpha[split] = alpha;
                merge_beta[split] = beta;
                running_denom =
                    running_denom * alpha + split_denom * beta;
                running_max = next_max;
            } else {
                merge_alpha[split] = 1.0f;
                merge_beta[split] = 0.0f;
            }
        }
        final_denom = running_denom;
    }
    __syncthreads();

    for (unsigned int split = 0u; split < split_count; ++split) {
        const size_t partial = (size_t)head * split_count + split;
#pragma unroll
        for (unsigned int item = 0u; item < MaxItems; ++item) {
            const unsigned int dimension = tid + item * blockDim.x;
            if (dimension < HeadDim) {
                acc[item] = acc[item] * merge_alpha[split] +
                    merge_beta[split] *
                        partial_values[partial * HeadDim + dimension];
            }
        }
    }

#pragma unroll
    for (unsigned int item = 0u; item < MaxItems; ++item) {
        const unsigned int dimension = tid + item * blockDim.x;
        if (dimension < HeadDim) {
            dst[(size_t)head * HeadDim + dimension] =
                final_denom > 0.0f ? acc[item] / final_denom : 0.0f;
        }
    }
}

// Parity-oriented control for diagnosing the production split-K drift.  The
// score phase remains parallel across chronological KV ranges, but every dot
// product uses the canonical score producer's exact block width and reduction
// tree.  Once all score CTAs for a head have arrived, the last CTA executes the
// canonical tiled64 consumer's chronological online-softmax and value
// recurrence verbatim.  This deliberately avoids the independent softmax
// normalization and merge performed by splitk_stage1 while retaining one
// launch and parallel QK work.  Like the current prototype completion path,
// its counter re-arm assumes batch=1 and serialized launches on one CUDA
// stream.  A concurrent production implementation must allocate score/counter
// workspace per request or per stream rather than sharing this workspace.
template <
    unsigned int HeadDim,
    unsigned int SpecializedWindow,
    unsigned int MaxVisibleTokens,
    bool CompleteInLastCta>
__device__ __forceinline__ void score_lastcta_exact(
    float* dst,
    unsigned int* completion_counters,
    volatile float* scores,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (dst == 0 || completion_counters == 0 || scores == 0 || q == 0 ||
        k == 0 || v == 0 || batch != 1u || q_seq_len != 1u ||
        head_dim != HeadDim || blockDim.x != HeadDim ||
        num_kv_heads == 0u || num_heads == 0u || num_heads > 32u ||
        (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u ||
        sliding_window != SpecializedWindow || key_format != 2u ||
        value_format != 2u || key_row_bytes == 0u ||
        base_key_row_bytes != key_row_bytes || value_row_bytes == 0u ||
        (key_row_bytes & 1u) != 0u || (value_row_bytes & 1u) != 0u ||
        key_row_bytes < num_kv_heads * HeadDim * 2u ||
        value_row_bytes < num_kv_heads * HeadDim * 2u ||
        page_size_tokens == 0u || physical_token_capacity == 0u ||
        score_capacity != MaxVisibleTokens || split_count == 0u ||
        split_count > 128u || (split_count & (split_count - 1u)) != 0u) {
        return;
    }

    const unsigned int block = blockIdx.x;
    const unsigned int head = block / split_count;
    const unsigned int split = block - head * split_count;
    if (head >= num_heads) return;

    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs =
                query_pos + 1u > sliding_window
                ? query_pos + 1u - sliding_window
                : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    if (key_start > key_end) key_start = key_end;
    const unsigned int visible_count = key_end - key_start;
    if (visible_count > MaxVisibleTokens) return;

    const unsigned int split_begin = key_start +
        (unsigned int)(((unsigned long long)visible_count * split) / split_count);
    const unsigned int split_end = key_start +
        (unsigned int)(((unsigned long long)visible_count * (split + 1u)) /
                       split_count);
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int q_base = head * head_dim;
    const unsigned int lane = threadIdx.x;
    __shared__ float warp_sums[HeadDim / 32u];
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile("rsqrt.approx.f32 %0, %1;"
                 : "=f"(scale)
                 : "f"(scale_input));

    for (unsigned int ki = split_begin; ki < split_end; ++ki) {
        const unsigned int token = physical_token(
            ki,
            block_table,
            block_count,
            page_size_tokens,
            physical_token_capacity);
        const bool valid = token != 0xffffffffu;
        const unsigned char* k_row =
            valid ? k + (size_t)token * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            const unsigned int value_index = kv_head * head_dim + lane;
            partial = q[q_base + lane] * f16_value(k_row, value_index);
        }
        const float dot = block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u && valid) {
            scores[(size_t)head * score_capacity + (ki - key_start)] =
                dot * scale;
        }
    }

    if constexpr (!CompleteInLastCta) return;

    // A fence from every producer thread precedes the head-local arrival
    // counter.  The last CTA then reloads volatile scores in chronological
    // order, independent of CTA scheduling order.
    __threadfence();
    __syncthreads();
    __shared__ unsigned int is_last_split;
    if (lane == 0u) {
        is_last_split =
            atomicAdd(&completion_counters[head], 1u) == split_count - 1u;
    }
    __syncthreads();
    if (!is_last_split) return;

    // Keep this recurrence source-equivalent to the checked-in canonical
    // tiled64 consumer.  In particular, do not replace it with an algebraic
    // softmax rewrite or fuse the two accumulator statements.
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha[MaxVisibleTokens];
    __shared__ float shared_beta[MaxVisibleTokens];
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int token = physical_token(
                ki,
                block_table,
                block_count,
                page_size_tokens,
                physical_token_capacity);
            const bool valid = token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            if (valid) {
                const float score =
                    scores[(size_t)head * score_capacity + recurrence_index];
                const float next_max = fmaxf(shared_max_score, score);
                shared_alpha[recurrence_index] =
                    expf(shared_max_score - next_max);
                shared_beta[recurrence_index] = expf(score - next_max);
                shared_denom =
                    shared_denom * shared_alpha[recurrence_index] +
                    shared_beta[recurrence_index];
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
            const unsigned int token = physical_token(
                ki,
                block_table,
                block_count,
                page_size_tokens,
                physical_token_capacity);
            const bool valid = token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            acc *= shared_alpha[recurrence_index];
            if (valid) {
                const unsigned char* v_row =
                    v + (size_t)token * value_row_bytes;
                const unsigned int value_index = kv_head * head_dim + lane;
                const float value = f16_value(v_row, value_index);
                acc += shared_beta[recurrence_index] * value;
            }
        }
    }
    if (lane < head_dim) {
        dst[head * head_dim + lane] =
            shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
    if (lane == 0u) atomicExch(&completion_counters[head], 0u);
    asm volatile("" : : "r"(total_sequence_len));
}

}  // namespace antfly_gqa_decode_splitk_online_prototype

extern "C" __global__ void
antfly_gqa_attention_decode_splitk_online_hd256_swa512_f16_stage1_prototype(
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::splitk_stage1<
        256u, 512u, 512u, false>(
        0,
        0,
        partial_values,
        partial_max,
        partial_denom,
        q,
        k,
        v,
        block_table,
        batch,
        q_seq_len,
        kv_seq_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        kv_position_offset,
        sliding_window,
        total_sequence_len,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_count,
        page_size_tokens,
        key_format,
        value_format,
        physical_token_capacity,
        score_capacity,
        decode_scalars,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_splitk_online_hd512_global_f16_stage1_prototype(
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::splitk_stage1<
        512u, 0u, 4096u, false>(
        0,
        0,
        partial_values,
        partial_max,
        partial_denom,
        q,
        k,
        v,
        block_table,
        batch,
        q_seq_len,
        kv_seq_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        kv_position_offset,
        sliding_window,
        total_sequence_len,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_count,
        page_size_tokens,
        key_format,
        value_format,
        physical_token_capacity,
        score_capacity,
        decode_scalars,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_splitk_online_hd256_swa512_f16_complete_prototype(
    float* dst,
    unsigned int* completion_counters,
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::splitk_stage1<
        256u, 512u, 512u, true>(
        dst,
        completion_counters,
        partial_values,
        partial_max,
        partial_denom,
        q,
        k,
        v,
        block_table,
        batch,
        q_seq_len,
        kv_seq_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        kv_position_offset,
        sliding_window,
        total_sequence_len,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_count,
        page_size_tokens,
        key_format,
        value_format,
        physical_token_capacity,
        score_capacity,
        decode_scalars,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_splitk_online_hd512_global_f16_complete_prototype(
    float* dst,
    unsigned int* completion_counters,
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::splitk_stage1<
        512u, 0u, 4096u, true>(
        dst,
        completion_counters,
        partial_values,
        partial_max,
        partial_denom,
        q,
        k,
        v,
        block_table,
        batch,
        q_seq_len,
        kv_seq_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        kv_position_offset,
        sliding_window,
        total_sequence_len,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_count,
        page_size_tokens,
        key_format,
        value_format,
        physical_token_capacity,
        score_capacity,
        decode_scalars,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_splitk_online_accurate_merge_hd256_swa512_f16_complete_prototype(
    float* dst,
    unsigned int* completion_counters,
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::splitk_stage1<
        256u, 512u, 512u, true, true>(
        dst,
        completion_counters,
        partial_values,
        partial_max,
        partial_denom,
        q,
        k,
        v,
        block_table,
        batch,
        q_seq_len,
        kv_seq_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        kv_position_offset,
        sliding_window,
        total_sequence_len,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_count,
        page_size_tokens,
        key_format,
        value_format,
        physical_token_capacity,
        score_capacity,
        decode_scalars,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_splitk_online_accurate_merge_hd512_global_f16_complete_prototype(
    float* dst,
    unsigned int* completion_counters,
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::splitk_stage1<
        512u, 0u, 4096u, true, true>(
        dst,
        completion_counters,
        partial_values,
        partial_max,
        partial_denom,
        q,
        k,
        v,
        block_table,
        batch,
        q_seq_len,
        kv_seq_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        kv_position_offset,
        sliding_window,
        total_sequence_len,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_count,
        page_size_tokens,
        key_format,
        value_format,
        physical_token_capacity,
        score_capacity,
        decode_scalars,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_score_parallel_exact_hd256_swa512_f16_prototype(
    float* dst,
    unsigned int* completion_counters,
    volatile float* scores,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::score_lastcta_exact<
        256u, 512u, 512u, false>(
        dst,
        completion_counters,
        scores,
        q,
        k,
        v,
        block_table,
        batch,
        q_seq_len,
        kv_seq_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        kv_position_offset,
        sliding_window,
        total_sequence_len,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_count,
        page_size_tokens,
        key_format,
        value_format,
        physical_token_capacity,
        score_capacity,
        decode_scalars,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_score_parallel_exact_hd512_global_f16_prototype(
    float* dst,
    unsigned int* completion_counters,
    volatile float* scores,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::score_lastcta_exact<
        512u, 0u, 4096u, false>(
        dst,
        completion_counters,
        scores,
        q,
        k,
        v,
        block_table,
        batch,
        q_seq_len,
        kv_seq_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        kv_position_offset,
        sliding_window,
        total_sequence_len,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_count,
        page_size_tokens,
        key_format,
        value_format,
        physical_token_capacity,
        score_capacity,
        decode_scalars,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_score_lastcta_exact_hd256_swa512_f16_prototype(
    float* dst,
    unsigned int* completion_counters,
    volatile float* scores,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::score_lastcta_exact<
        256u, 512u, 512u, true>(
        dst,
        completion_counters,
        scores,
        q,
        k,
        v,
        block_table,
        batch,
        q_seq_len,
        kv_seq_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        kv_position_offset,
        sliding_window,
        total_sequence_len,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_count,
        page_size_tokens,
        key_format,
        value_format,
        physical_token_capacity,
        score_capacity,
        decode_scalars,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_score_lastcta_exact_hd512_global_f16_prototype(
    float* dst,
    unsigned int* completion_counters,
    volatile float* scores,
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
    const unsigned int* decode_scalars,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::score_lastcta_exact<
        512u, 0u, 4096u, true>(
        dst,
        completion_counters,
        scores,
        q,
        k,
        v,
        block_table,
        batch,
        q_seq_len,
        kv_seq_len,
        num_heads,
        num_kv_heads,
        head_dim,
        query_position_offset,
        kv_position_offset,
        sliding_window,
        total_sequence_len,
        key_row_bytes,
        base_key_row_bytes,
        value_row_bytes,
        block_count,
        page_size_tokens,
        key_format,
        value_format,
        physical_token_capacity,
        score_capacity,
        decode_scalars,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_splitk_online_hd256_f16_stage2_prototype(
    float* dst,
    const float* partial_values,
    const float* partial_max,
    const float* partial_denom,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::splitk_stage2<256u>(
        dst,
        partial_values,
        partial_max,
        partial_denom,
        num_heads,
        head_dim,
        split_count);
}

extern "C" __global__ void
antfly_gqa_attention_decode_splitk_online_hd512_f16_stage2_prototype(
    float* dst,
    const float* partial_values,
    const float* partial_max,
    const float* partial_denom,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int split_count
) {
    antfly_gqa_decode_splitk_online_prototype::splitk_stage2<512u>(
        dst,
        partial_values,
        partial_max,
        partial_denom,
        num_heads,
        head_dim,
        split_count);
}
