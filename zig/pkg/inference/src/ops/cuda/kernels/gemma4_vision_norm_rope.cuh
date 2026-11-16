// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#pragma once

// Gemma's spatial pairs occupy adjacent half-heads. Keep normalization,
// learned scale, rotary arithmetic and output in F32; avoid the generic
// reshape/transpose/position-upload sequence for each Q and K projection.
extern "C" __global__ __launch_bounds__(64) void antfly_gemma4_vision_norm_rope_f32(
    float* dst, const float* input, const float* weight,
    unsigned rows, unsigned grid_x, unsigned grid_y, float eps, float theta, float scale
) {
    const unsigned head = blockIdx.x, tid = threadIdx.x;
    if (head >= rows * 12u || blockDim.x != 64u || !grid_x || !grid_y) return;
    const unsigned base = head * 64u;
    const float x = input[base + tid];
    float sum = x * x;
    for (unsigned delta = 16u; delta; delta >>= 1u)
        sum += __shfl_down_sync(0xffffffffu, sum, delta);
    __shared__ float halves[2], normalized[64];
    if ((tid & 31u) == 0u) halves[tid >> 5u] = sum;
    __syncthreads();
    const float inverse = rsqrtf((halves[0] + halves[1]) * (1.0f / 64.0f) + eps);
    normalized[tid] = (x * inverse) * weight[tid];
    __syncthreads();
    if (tid < 32u) {
        const unsigned axis = tid / 16u, j = tid % 16u;
        const unsigned row = head / 12u;
        const unsigned position = axis == 0u ? row % grid_x : (row / grid_x) % grid_y;
        const float inverse_frequency = 1.0f / powf(theta, float(j * 2u) / 32.0f);
        const float angle = float(position) * inverse_frequency;
        const float c = cosf(angle), s = sinf(angle);
        const unsigned a = axis * 32u + j, b = a + 16u;
        const float va = normalized[a], vb = normalized[b];
        dst[base + a] = (va * c - vb * s) * scale;
        dst[base + b] = (vb * c + va * s) * scale;
    }
}
