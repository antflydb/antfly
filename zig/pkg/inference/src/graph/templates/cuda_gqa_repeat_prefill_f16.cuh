// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// Exact E4B repeat attention. Eight independent warps own eight queries for one
// query head. Each warp reproduces the reference's 32-dimension reductions,
// then its reduction of those partial sums. Keys are consumed chronologically.
// No shared memory or block barriers are required. Keep explicit rounding:
// reassociating the output multiply/FMA changes downstream quantized activations.
// The host restricts this route to the isolated SM89 f16 repeat cache.
#include <cuda_fp16.h>
#include <stdint.h>
#include <stddef.h>

#define ANTFLY_REPEAT_ARGS \
    float* dst, const float* q, const unsigned char* k, const unsigned char* v, \
    const unsigned* table, const unsigned char* mask, const float* bias, \
    unsigned batch, unsigned qr, unsigned kr, unsigned heads, unsigned kh, \
    unsigned dim, unsigned qo, unsigned ko, unsigned window, unsigned total, \
    unsigned masklen, unsigned biasmode, unsigned kb, unsigned basekb, unsigned vb, \
    unsigned blocks, unsigned page, unsigned format, unsigned vf, unsigned capacity, \
    const unsigned* scalars
#define ANTFLY_REPEAT_FORWARD \
    dst, q, k, v, table, mask, bias, batch, qr, kr, heads, kh, dim, qo, ko, \
    window, total, masklen, biasmode, kb, basekb, vb, blocks, page, format, vf, \
    capacity, scalars

namespace antfly_repeat_exact {

// The butterfly swaps only addition operands within the same reduction tree.
// For finite inputs every lane gets the reference lane-zero result, removing
// a separate broadcast for each 32-dimension group.
__device__ __forceinline__ float reduce(float value) {
#pragma unroll
    for (unsigned shift = 16; shift > 0; shift >>= 1)
        value += __shfl_xor_sync(0xffffffffu, value, shift);
    return value;
}

template<unsigned D, unsigned W>
__device__ void attention(ANTFLY_REPEAT_ARGS) {
    (void)mask;
    (void)bias;
    if (!dst || !q || !k || !v || batch != 1 || qr < 2 || qr > 512 ||
        kr == 0 || kr > 65536 || heads != 8 || kh != 2 || dim != D || ko != 0 ||
        qo > 65536 - qr || qo + qr != kr || total != kr || window != W ||
        masklen || biasmode || format != 2 || vf != 2 || kb != D * 4 ||
        basekb != kb || vb != kb || page != 16 || scalars ||
        capacity < kr || capacity > 65536 || capacity % 16 != 0 ||
        (table != nullptr) != (blocks != 0) ||
        (table && blocks < (kr + 15) / 16) ||
        blockDim.x != 256 || blockDim.y != 1 || blockDim.z != 1 ||
        gridDim.x != 8 || gridDim.y != (qr + 7) / 8 || gridDim.z != 1)
        return;

    const unsigned lane = threadIdx.x % 32;
    const unsigned warp = threadIdx.x / 32;
    const unsigned qi = blockIdx.y * 8 + warp;
    const unsigned head = blockIdx.x;
    const unsigned kv_head = head / 4;
    if (qi >= qr) return; // Entire warp returns; there are no block barriers.

    constexpr unsigned groups = D / 32;
    float queries[groups];
    float output[groups];
#pragma unroll
    for (unsigned group = 0; group < groups; ++group) {
        queries[group] = q[(qi * 8 + head) * D + group * 32 + lane];
        output[group] = 0;
    }
    const unsigned end = qo + qi + 1;
    const unsigned begin = W && end > W ? end - W : 0;
    float maximum = -3.402823466e+38f;
    float denominator = 0;
    float scale = 0;
    if (lane == 0) {
        // Validation can let ptxas constant-fold rsqrt even inside inline PTX.
        // One volatile scalar per warp keeps the reference's runtime MUFU
        // rounding (HD512 differs from the constant-folded result).
        volatile unsigned runtime_dimension = dim;
        scale = rsqrtf(static_cast<float>(runtime_dimension));
    }
    scale = __shfl_sync(0xffffffffu, scale, 0);

    for (unsigned ki = begin; ki < end; ++ki) {
        const unsigned block = ki / 16;
        const uint64_t mapped = table
            ? static_cast<uint64_t>(table[block]) * 16 + ki % 16
            : ki;
        if (mapped >= capacity) return;
        const unsigned physical = static_cast<unsigned>(mapped);
        const half* key = reinterpret_cast<const half*>(k + static_cast<size_t>(physical) * kb) + kv_head * D;
        const half* value = reinterpret_cast<const half*>(v + static_cast<size_t>(physical) * vb) + kv_head * D;

        float sum = 0;
#pragma unroll
        for (unsigned group = 0; group < groups; ++group) {
            float partial = __fmul_rn(queries[group], __half2float(key[group * 32 + lane]));
            partial = reduce(partial);
            const float group_sum = partial;
            if (lane == group) sum = group_sum;
        }
        const float dot = reduce(sum);
        float alpha = 0;
        float beta = 0;
        if (lane == 0) {
            const float score = __fmul_rn(dot, scale);
            const float next = fmaxf(maximum, score);
            alpha = expf(__fsub_rn(maximum, next));
            beta = expf(__fsub_rn(score, next));
            denominator = __fmaf_rn(denominator, alpha, beta);
            maximum = next;
        }
        alpha = __shfl_sync(0xffffffffu, alpha, 0);
        beta = __shfl_sync(0xffffffffu, beta, 0);
#pragma unroll
        for (unsigned group = 0; group < groups; ++group) {
            output[group] = __fmul_rn(output[group], alpha);
            output[group] = __fmaf_rn(__half2float(value[group * 32 + lane]), beta, output[group]);
        }
    }
    denominator = __shfl_sync(0xffffffffu, denominator, 0);
#pragma unroll
    for (unsigned group = 0; group < groups; ++group)
        dst[(qi * 8 + head) * D + group * 32 + lane] =
            denominator > 0 ? output[group] / denominator : 0;
}

} // namespace antfly_repeat_exact

extern "C" __global__ void antfly_gqa_repeat_prefill_exact_hd256_f32(ANTFLY_REPEAT_ARGS) {
    antfly_repeat_exact::attention<256, 512>(ANTFLY_REPEAT_FORWARD);
}
extern "C" __global__ void antfly_gqa_repeat_prefill_exact_hd512_f32(ANTFLY_REPEAT_ARGS) {
    antfly_repeat_exact::attention<512, 0>(ANTFLY_REPEAT_FORWARD);
}

#undef ANTFLY_REPEAT_FORWARD
#undef ANTFLY_REPEAT_ARGS
