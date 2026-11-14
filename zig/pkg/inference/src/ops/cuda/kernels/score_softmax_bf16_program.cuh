// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once
#include <cuda_bf16.h>

// Scores remain F32. Masking and publication are independent static policies;
// historical adapters retain their original pre-normalization BF16 boundary.
enum class AntflyScoreMask { rows_and_keys, prefix_keys };
enum class AntflyProbabilityPublication { legacy_beta_bf16, normalized_bf16 };

template<AntflyScoreMask Mask>
__device__ __forceinline__ bool antfly_score_key_visible(
    const long long* mask, unsigned int batch_index, unsigned int sequence,
    unsigned int key, unsigned int valid_keys) {
    if constexpr (Mask == AntflyScoreMask::prefix_keys) return key < valid_keys;
    return !mask || mask[(size_t)batch_index * sequence + key] != 0;
}

template<AntflyScoreMask Mask, AntflyProbabilityPublication Publication>
__device__ __forceinline__ void antfly_score_softmax_bf16(
    __nv_bfloat16* probs, const float* scores, const long long* mask,
    unsigned int sequence, unsigned int query_base, unsigned int rows,
    unsigned int batch_index, unsigned int tile_capacity, unsigned int valid_keys) {

    unsigned int row = blockIdx.x % rows;
    size_t matrix_offset = (size_t)(blockIdx.x / rows) * tile_capacity * sequence;
    unsigned int lane = threadIdx.x, query = query_base + row;
    bool query_valid = Mask == AntflyScoreMask::prefix_keys || !mask || mask[(size_t)batch_index * sequence + query] != 0;
    extern __shared__ float reduce[];
    float local_max = -INFINITY;
    for (unsigned int key = lane; key < sequence; key += blockDim.x)
        if (query_valid && antfly_score_key_visible<Mask>(mask, batch_index, sequence, key, valid_keys))
            local_max = fmaxf(local_max, scores[matrix_offset + (size_t)row * sequence + key]);
    reduce[lane] = local_max; __syncthreads();
    for (unsigned int stride = blockDim.x / 2; stride; stride >>= 1) {
        if (lane < stride) reduce[lane] = fmaxf(reduce[lane], reduce[lane + stride]);
        __syncthreads();
    }
    float row_max = reduce[0], local_sum = 0.0f;
    // Every warp must snapshot the maximum before any warp reuses reduce[0]
    // for its local sum; the preceding reduction barrier does not order this.
    __syncthreads();
    for (unsigned int key = lane; key < sequence; key += blockDim.x) {
        bool visible = query_valid && antfly_score_key_visible<Mask>(mask, batch_index, sequence, key, valid_keys);
        float value = visible ? expf(scores[matrix_offset + (size_t)row * sequence + key] - row_max) : 0.0f;
        if constexpr (Publication == AntflyProbabilityPublication::legacy_beta_bf16)
            probs[matrix_offset + (size_t)row * sequence + key] = __float2bfloat16_rn(value);
        local_sum += value;
    }
    reduce[lane] = local_sum; __syncthreads();
    for (unsigned int stride = blockDim.x / 2; stride; stride >>= 1) {
        if (lane < stride) reduce[lane] += reduce[lane + stride];
        __syncthreads();
    }
    float inverse = reduce[0] > 0.0f ? 1.0f / reduce[0] : 0.0f;
    if constexpr (Publication == AntflyProbabilityPublication::legacy_beta_bf16) {
    for (unsigned int key = lane; key < sequence; key += blockDim.x)
        probs[matrix_offset + (size_t)row * sequence + key] = __float2bfloat16_rn(
            __bfloat162float(probs[matrix_offset + (size_t)row * sequence + key]) * inverse);
    } else {
        // Keep the exponent and denominator in F32 until final publication.
        // Recompute rather than materializing a prematurely rounded BF16 beta.
        for (unsigned int key = lane; key < sequence; key += blockDim.x) {
            bool visible = query_valid && antfly_score_key_visible<Mask>(mask, batch_index, sequence, key, valid_keys);
            float value = visible ? expf(scores[matrix_offset + (size_t)row * sequence + key] - row_max) : 0.0f;
            probs[matrix_offset + (size_t)row * sequence + key] = __float2bfloat16_rn(value * inverse);
        }
    }
}
