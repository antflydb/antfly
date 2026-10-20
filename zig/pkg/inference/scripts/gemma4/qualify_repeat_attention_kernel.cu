// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#include "../../src/graph/templates/cuda_gqa_repeat_split_f16.cuh"

// Test-only chronological warp oracle. Unlike the candidate this computes each
// score immediately before consuming it and never materializes score slabs.
extern "C" __global__ void antfly_repeat_trace_oracle(
    float* trace, const float* q, const half* k, const unsigned* table,
    unsigned queries, unsigned offset, unsigned rows, unsigned capacity) {
    const unsigned test = blockIdx.x, lane = threadIdx.x;
    const unsigned qi = test / 2 ? queries - 1 : 0;
    const unsigned head = test % 2 ? 4 : 0;
    float query[16];
#pragma unroll
    for (unsigned group = 0; group < 16; ++group)
        query[group] = q[(qi * 8 + head) * 512 + group * 32 + lane];
    float maximum = -3.402823466e+38f, denominator = 0, scale = 0;
    if (lane == 0) { volatile unsigned dim = 512; scale = rsqrtf(float(dim)); }
    for (unsigned key = 0; key <= offset + qi; ++key) {
        const uint64_t mapped = antfly_repeat_split::physical(key, table);
        if (mapped >= capacity) return;
        const half* keys = k + size_t(mapped) * 1024 + (head / 4) * 512;
        float sum = 0;
#pragma unroll
        for (unsigned group = 0; group < 16; ++group) {
            float partial = __fmul_rn(query[group], __half2float(keys[group * 32 + lane]));
            partial = antfly_repeat_exact::reduce(partial);
            if (lane == group) sum = partial;
        }
        const float dot = antfly_repeat_exact::reduce(sum);
        if (lane == 0) {
            const float score = __fmul_rn(dot, scale);
            const float next = fmaxf(maximum, score);
            const float alpha = expf(__fsub_rn(maximum, next));
            const float beta = expf(__fsub_rn(score, next));
            denominator = __fmaf_rn(denominator, alpha, beta);
            maximum = next;
            float* result = trace + (size_t(test) * rows + key) * 5;
            result[0] = score; result[1] = alpha; result[2] = beta;
            result[3] = maximum; result[4] = denominator;
        }
    }
}
