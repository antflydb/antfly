// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// native-program:entry termite_rotary_pair_i32_bf16_source
#include "../kernels/rotary_program.cuh"
extern "C" __global__ void termite_rotary_pair_i32_bf16_source(
    ANTFLY_CUDA_ROTARY_PAIR_I32_BF16_SOURCE_PARAMS) {
    const unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const unsigned int d = i % 32u, pair = d % 16u, base = i - d;
    const float frequency = antfly_rotary_source_frequency(theta, 32u, pair);
    const float angle = __fmul_rn((float)positions[i / 32u], frequency);
    const float c = cosf(angle), s = sinf(angle);
    const float qv = antfly_rotary_component<antfly_rotary_products::bf16_source>(
        __bfloat162float(q[base + pair]), __bfloat162float(q[base + 16u + pair]), c, s, d >= 16u);
    const float kv = antfly_rotary_component<antfly_rotary_products::bf16_source>(
        __bfloat162float(k[base + pair]), __bfloat162float(k[base + 16u + pair]), c, s, d >= 16u);
    qo[i] = __float2bfloat16_rn(__fmul_rn(qv, scale));
    ko[i] = __float2bfloat16_rn(kv);
}
