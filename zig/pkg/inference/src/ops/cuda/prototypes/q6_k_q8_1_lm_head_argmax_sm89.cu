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

// Standalone, default-off SM89 experiments for the batch-one Gemma 4 E2B
// Q6_K x Q8_1 LM head.  This translation unit is intentionally absent from
// the canonical CUDA bundle and exports prototype-only symbols.
//
// Production evaluates eight vocabulary rows per CTA, writes one
// (score,index) pair for every eight rows, then reduces 32,768 pairs.  These
// kernels keep the exact per-row DP4A and F32 reduction order, but let a
// persistent CTA visit a grid-stride sequence of 8/16/32-row groups while
// retaining the Q8_1 activation in registers.  Each CTA writes one pair, so
// the unmodified production pair reducer can consume gridDim.x pairs.

#include <cuda_fp16.h>

namespace antfly_q6_k_q8_1_lm_head_argmax_prototype {

static __device__ __forceinline__ float half_bits_to_float(unsigned short bits) {
    return __half2float(__ushort_as_half(bits));
}

static __device__ __forceinline__ float warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

static __device__ __forceinline__ float q6_k_sub_scale_f32(
    const unsigned char* block,
    unsigned int sub
) {
    const signed char* scales = (const signed char*)(block + 192u);
    const unsigned short d_bits =
        (unsigned short)block[208] | ((unsigned short)block[209] << 8);
    return half_bits_to_float(d_bits) * (float)scales[sub];
}

static __device__ __forceinline__ int q6_k_pack4(
    const unsigned char* ql,
    const unsigned char* qh,
    unsigned int nibble_shift,
    unsigned int qh_shift,
    unsigned int offset
) {
    const unsigned int q0 =
        ((unsigned int)(ql[offset + 0u] >> nibble_shift) & 0x0fu) |
        (((unsigned int)(qh[offset + 0u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q1 =
        ((unsigned int)(ql[offset + 1u] >> nibble_shift) & 0x0fu) |
        (((unsigned int)(qh[offset + 1u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q2 =
        ((unsigned int)(ql[offset + 2u] >> nibble_shift) & 0x0fu) |
        (((unsigned int)(qh[offset + 2u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q3 =
        ((unsigned int)(ql[offset + 3u] >> nibble_shift) & 0x0fu) |
        (((unsigned int)(qh[offset + 3u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int p0 = (q0 - 32u) & 0xffu;
    const unsigned int p1 = (q1 - 32u) & 0xffu;
    const unsigned int p2 = (q2 - 32u) & 0xffu;
    const unsigned int p3 = (q3 - 32u) & 0xffu;
    return (int)(p0 | (p1 << 8u) | (p2 << 16u) | (p3 << 24u));
}

static __device__ __forceinline__ int q6_k_q8_1_dot16_sub(
    const unsigned char* block,
    unsigned int sub,
    int q8_pack0,
    int q8_pack1,
    int q8_pack2,
    int q8_pack3
) {
    const unsigned int half = sub >> 3u;
    const unsigned int group = (sub & 7u) >> 1u;
    const unsigned int l_base = (sub & 1u) * 16u;
    const unsigned int ql_off = half * 64u + (group & 1u) * 32u;
    const unsigned int qh_off = half * 32u;
    const unsigned int qh_shift = group << 1u;
    const unsigned int nibble_shift = (group >> 1u) << 2u;
    const unsigned char* ql = block + ql_off + l_base;
    const unsigned char* qh = block + 128u + qh_off + l_base;
    int sumi = 0;
    sumi = __dp4a(q6_k_pack4(ql, qh, nibble_shift, qh_shift, 0u), q8_pack0, sumi);
    sumi = __dp4a(q6_k_pack4(ql, qh, nibble_shift, qh_shift, 4u), q8_pack1, sumi);
    sumi = __dp4a(q6_k_pack4(ql, qh, nibble_shift, qh_shift, 8u), q8_pack2, sumi);
    sumi = __dp4a(q6_k_pack4(ql, qh, nibble_shift, qh_shift, 12u), q8_pack3, sumi);
    return sumi;
}

template <unsigned int Cols>
__device__ __forceinline__ void persistent_stage1(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    (void)suppress_token_ids;
    static_assert(Cols == 8u || Cols == 16u || Cols == 32u,
        "prototype column group must retain eight-row production tiles");
    const unsigned int row_blocks = 10u;
    const unsigned int task_threads = 160u;
    if (rows != 1u || in_dim != 2560u || out_dim == 0u ||
        out_dim > 262144u || suppress_count != 0u || blockDim.x != 160u ||
        gridDim.x == 0u) return;

    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    __shared__ float warp_partial[Cols][5];

    float q8_d = 0.0f;
    int q8_pack0 = 0;
    int q8_pack1 = 0;
    int q8_pack2 = 0;
    int q8_pack3 = 0;
    unsigned int block = 0u;
    unsigned int sub = 0u;
    if (tid < task_threads) {
        block = tid >> 4u;
        sub = tid & 15u;
        const unsigned int q8_sub_block = sub >> 1u;
        const unsigned int q8_lane_base = (sub & 1u) * 16u;
        const unsigned char* q8_bp =
            q8_input + (block * 8u + q8_sub_block) * 36u;
        q8_d = half_bits_to_float(((const unsigned short*)q8_bp)[0]);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        q8_pack0 = *(const int*)(q8_values + q8_lane_base + 0u);
        q8_pack1 = *(const int*)(q8_values + q8_lane_base + 4u);
        q8_pack2 = *(const int*)(q8_values + q8_lane_base + 8u);
        q8_pack3 = *(const int*)(q8_values + q8_lane_base + 12u);
    }

    float cta_best_value = -3.402823466e+38f;
    unsigned int cta_best_index = 0xffffffffu;
    const unsigned int group_count = (out_dim + Cols - 1u) / Cols;
    for (unsigned int group = blockIdx.x; group < group_count;
         group += gridDim.x) {
        const unsigned int col_base = group * Cols;
        float acc[Cols];
#pragma unroll
        for (unsigned int c = 0u; c < Cols; ++c) acc[c] = 0.0f;

        if (tid < task_threads) {
#pragma unroll
            for (unsigned int c = 0u; c < Cols; ++c) {
                const unsigned int col = col_base + c;
                if (col < out_dim) {
                    const unsigned char* bp =
                        weight + ((size_t)col * row_blocks + block) * 210u;
                    const int sumi = q6_k_q8_1_dot16_sub(
                        bp, sub, q8_pack0, q8_pack1, q8_pack2, q8_pack3);
                    acc[c] = (q8_d * q6_k_sub_scale_f32(bp, sub)) *
                        (float)sumi;
                }
            }
        }

#pragma unroll
        for (unsigned int c = 0u; c < Cols; ++c) {
            const float sum = warp_reduce_sum_f32(acc[c]);
            if (lane == 0u) warp_partial[c][warp] = sum;
        }
        __syncthreads();

        if (tid == 0u) {
#pragma unroll
            for (unsigned int c = 0u; c < Cols; ++c) {
                const unsigned int col = col_base + c;
                if (col >= out_dim) continue;
                float value = 0.0f;
#pragma unroll
                for (unsigned int w = 0u; w < 5u; ++w) {
                    value += warp_partial[c][w];
                }
                // This is the production tie and NaN behavior.  A NaN never
                // wins either comparison; equal finite values choose the
                // lowest vocabulary index independent of CTA scheduling.
                if (value > cta_best_value ||
                    (value == cta_best_value && col < cta_best_index)) {
                    cta_best_value = value;
                    cta_best_index = col;
                }
            }
        }
        // tid 0 must finish consuming the shared partials before another
        // group overwrites them.
        __syncthreads();
    }

    if (tid == 0u) {
        partial_values[blockIdx.x] = cta_best_value;
        partial_indices[blockIdx.x] = cta_best_index;
    }
}

template <unsigned int Cols>
__device__ __forceinline__ void compute_group(
    float* warp_partial,
    const unsigned char* weight,
    unsigned int out_dim,
    unsigned int group,
    float q8_d,
    int q8_pack0,
    int q8_pack1,
    int q8_pack2,
    int q8_pack3,
    unsigned int q6_block,
    unsigned int q6_sub
) {
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int col_base = group * Cols;
    float acc[Cols];
#pragma unroll
    for (unsigned int c = 0u; c < Cols; ++c) acc[c] = 0.0f;
    if (tid < 160u) {
#pragma unroll
        for (unsigned int c = 0u; c < Cols; ++c) {
            const unsigned int col = col_base + c;
            if (col < out_dim) {
                const unsigned char* bp =
                    weight + ((size_t)col * 10u + q6_block) * 210u;
                const int sumi = q6_k_q8_1_dot16_sub(
                    bp, q6_sub, q8_pack0, q8_pack1, q8_pack2, q8_pack3);
                acc[c] = (q8_d * q6_k_sub_scale_f32(bp, q6_sub)) *
                    (float)sumi;
            }
        }
    }
#pragma unroll
    for (unsigned int c = 0u; c < Cols; ++c) {
        const float sum = warp_reduce_sum_f32(acc[c]);
        if (tid < 160u && lane == 0u) warp_partial[c * 5u + warp] = sum;
    }
}

template <unsigned int Cols>
__device__ __forceinline__ void consume_group(
    const float* warp_partial,
    unsigned int out_dim,
    unsigned int group,
    float* best_value,
    unsigned int* best_index
) {
    const unsigned int col_base = group * Cols;
#pragma unroll
    for (unsigned int c = 0u; c < Cols; ++c) {
        const unsigned int col = col_base + c;
        if (col >= out_dim) continue;
        float value = 0.0f;
#pragma unroll
        for (unsigned int warp = 0u; warp < 5u; ++warp) {
            value += warp_partial[c * 5u + warp];
        }
        if (value > *best_value ||
            (value == *best_value && col < *best_index)) {
            *best_value = value;
            *best_index = col;
        }
    }
}

// Double-buffering makes the barrier that publishes the next vocabulary
// group also prove that tid 0 finished consuming the previous group.  This
// removes one block-wide barrier per persistent iteration without changing
// any score arithmetic.
template <unsigned int Cols>
__device__ __forceinline__ void persistent_pipeline_stage1(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    (void)suppress_token_ids;
    static_assert(Cols == 8u || Cols == 16u,
        "pipelined prototype retains eight-row production tiles");
    if (rows != 1u || in_dim != 2560u || out_dim == 0u ||
        out_dim > 262144u || suppress_count != 0u || blockDim.x != 160u ||
        gridDim.x == 0u) return;

    const unsigned int tid = threadIdx.x;
    __shared__ float warp_partial[2][Cols][5];
    float q8_d = 0.0f;
    int q8_pack0 = 0;
    int q8_pack1 = 0;
    int q8_pack2 = 0;
    int q8_pack3 = 0;
    unsigned int q6_block = 0u;
    unsigned int q6_sub = 0u;
    if (tid < 160u) {
        q6_block = tid >> 4u;
        q6_sub = tid & 15u;
        const unsigned int q8_sub_block = q6_sub >> 1u;
        const unsigned int q8_lane_base = (q6_sub & 1u) * 16u;
        const unsigned char* q8_bp =
            q8_input + (q6_block * 8u + q8_sub_block) * 36u;
        q8_d = half_bits_to_float(((const unsigned short*)q8_bp)[0]);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        q8_pack0 = *(const int*)(q8_values + q8_lane_base + 0u);
        q8_pack1 = *(const int*)(q8_values + q8_lane_base + 4u);
        q8_pack2 = *(const int*)(q8_values + q8_lane_base + 8u);
        q8_pack3 = *(const int*)(q8_values + q8_lane_base + 12u);
    }

    const unsigned int group_count = (out_dim + Cols - 1u) / Cols;
    if (blockIdx.x >= group_count) {
        if (tid == 0u) {
            partial_values[blockIdx.x] = -3.402823466e+38f;
            partial_indices[blockIdx.x] = 0xffffffffu;
        }
        return;
    }

    float cta_best_value = -3.402823466e+38f;
    unsigned int cta_best_index = 0xffffffffu;
    unsigned int group = blockIdx.x;
    unsigned int buffer_index = 0u;
    compute_group<Cols>(
        &warp_partial[buffer_index][0][0], weight, out_dim, group, q8_d,
        q8_pack0, q8_pack1, q8_pack2, q8_pack3, q6_block, q6_sub);
    __syncthreads();

    while (true) {
        if (tid == 0u) {
            consume_group<Cols>(
                &warp_partial[buffer_index][0][0], out_dim, group,
                &cta_best_value, &cta_best_index);
        }
        const unsigned int next_group = group + gridDim.x;
        if (next_group >= group_count) break;
        const unsigned int next_buffer = buffer_index ^ 1u;
        compute_group<Cols>(
            &warp_partial[next_buffer][0][0], weight, out_dim, next_group,
            q8_d, q8_pack0, q8_pack1, q8_pack2, q8_pack3,
            q6_block, q6_sub);
        __syncthreads();
        group = next_group;
        buffer_index = next_buffer;
    }

    if (tid == 0u) {
        partial_values[blockIdx.x] = cta_best_value;
        partial_indices[blockIdx.x] = cta_best_index;
    }
}

// Six-warp pipeline: warps 0..4 preserve the production task and reduction
// mapping; warp 5 owns the deterministic cross-warp/CTA fold.  While warp 5
// consumes one shared buffer, the five compute warps fill the other.  No
// compute warp is delayed by the serial pair fold.
__device__ __forceinline__ void persistent_pipeline_dedicated_tile8_stage1(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    (void)suppress_token_ids;
    if (rows != 1u || in_dim != 2560u || out_dim == 0u ||
        out_dim > 262144u || suppress_count != 0u || blockDim.x != 192u ||
        gridDim.x == 0u) return;

    const unsigned int tid = threadIdx.x;
    __shared__ float warp_partial[2][8][5];
    float q8_d = 0.0f;
    int q8_pack0 = 0;
    int q8_pack1 = 0;
    int q8_pack2 = 0;
    int q8_pack3 = 0;
    unsigned int q6_block = 0u;
    unsigned int q6_sub = 0u;
    if (tid < 160u) {
        q6_block = tid >> 4u;
        q6_sub = tid & 15u;
        const unsigned int q8_sub_block = q6_sub >> 1u;
        const unsigned int q8_lane_base = (q6_sub & 1u) * 16u;
        const unsigned char* q8_bp =
            q8_input + (q6_block * 8u + q8_sub_block) * 36u;
        q8_d = half_bits_to_float(((const unsigned short*)q8_bp)[0]);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        q8_pack0 = *(const int*)(q8_values + q8_lane_base + 0u);
        q8_pack1 = *(const int*)(q8_values + q8_lane_base + 4u);
        q8_pack2 = *(const int*)(q8_values + q8_lane_base + 8u);
        q8_pack3 = *(const int*)(q8_values + q8_lane_base + 12u);
    }

    const unsigned int group_count = (out_dim + 7u) / 8u;
    if (blockIdx.x >= group_count) {
        if (tid == 160u) {
            partial_values[blockIdx.x] = -3.402823466e+38f;
            partial_indices[blockIdx.x] = 0xffffffffu;
        }
        return;
    }

    float cta_best_value = -3.402823466e+38f;
    unsigned int cta_best_index = 0xffffffffu;
    unsigned int group = blockIdx.x;
    unsigned int buffer_index = 0u;
    compute_group<8u>(
        &warp_partial[buffer_index][0][0], weight, out_dim, group, q8_d,
        q8_pack0, q8_pack1, q8_pack2, q8_pack3, q6_block, q6_sub);
    __syncthreads();

    while (true) {
        if (tid == 160u) {
            consume_group<8u>(
                &warp_partial[buffer_index][0][0], out_dim, group,
                &cta_best_value, &cta_best_index);
        }
        const unsigned int next_group = group + gridDim.x;
        if (next_group >= group_count) break;
        const unsigned int next_buffer = buffer_index ^ 1u;
        compute_group<8u>(
            &warp_partial[next_buffer][0][0], weight, out_dim, next_group,
            q8_d, q8_pack0, q8_pack1, q8_pack2, q8_pack3,
            q6_block, q6_sub);
        __syncthreads();
        group = next_group;
        buffer_index = next_buffer;
    }

    if (tid == 160u) {
        partial_values[blockIdx.x] = cta_best_value;
        partial_indices[blockIdx.x] = cta_best_index;
    }
}

} // namespace antfly_q6_k_q8_1_lm_head_argmax_prototype

extern "C" __global__ void
antfly_q6_k_q8_1_lm_head_argmax_persistent_tile8_sm89_prototype(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    antfly_q6_k_q8_1_lm_head_argmax_prototype::persistent_stage1<8u>(
        partial_values, partial_indices, q8_input, weight, suppress_token_ids,
        rows, in_dim, out_dim, suppress_count);
}

extern "C" __global__ void
antfly_q6_k_q8_1_lm_head_argmax_persistent_tile16_sm89_prototype(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    antfly_q6_k_q8_1_lm_head_argmax_prototype::persistent_stage1<16u>(
        partial_values, partial_indices, q8_input, weight, suppress_token_ids,
        rows, in_dim, out_dim, suppress_count);
}

extern "C" __global__ void
antfly_q6_k_q8_1_lm_head_argmax_persistent_tile32_sm89_prototype(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    antfly_q6_k_q8_1_lm_head_argmax_prototype::persistent_stage1<32u>(
        partial_values, partial_indices, q8_input, weight, suppress_token_ids,
        rows, in_dim, out_dim, suppress_count);
}

extern "C" __global__ void
antfly_q6_k_q8_1_lm_head_argmax_persistent_pipeline_tile8_sm89_prototype(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    antfly_q6_k_q8_1_lm_head_argmax_prototype::persistent_pipeline_stage1<8u>(
        partial_values, partial_indices, q8_input, weight, suppress_token_ids,
        rows, in_dim, out_dim, suppress_count);
}

extern "C" __global__ void
antfly_q6_k_q8_1_lm_head_argmax_persistent_pipeline_tile16_sm89_prototype(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    antfly_q6_k_q8_1_lm_head_argmax_prototype::persistent_pipeline_stage1<16u>(
        partial_values, partial_indices, q8_input, weight, suppress_token_ids,
        rows, in_dim, out_dim, suppress_count);
}

extern "C" __global__ void
antfly_q6_k_q8_1_lm_head_argmax_persistent_pipeline_dedicated_tile8_sm89_prototype(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    antfly_q6_k_q8_1_lm_head_argmax_prototype::
        persistent_pipeline_dedicated_tile8_stage1(
            partial_values, partial_indices, q8_input, weight,
            suppress_token_ids, rows, in_dim, out_dim, suppress_count);
}
