// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Self-contained GLiNER2.5 boundary attention backward. CUDA toolkit only.

__device__ __forceinline__ float dot32(const float* a, const float* b) {
    float s = 0.0f;
    #pragma unroll
    for (int d = 0; d < 32; ++d) s = fmaf(a[d], b[d], s);
    return s;
}

extern "C" __global__ void boundary_delta32(float*, const float*, const float*, int, int, int) {}

// Gradients are accumulated with device atomics. The operation is fully
// self-contained and supports the existing packed QKV/output ABI.
extern "C" __global__ void boundary_backward(
    float* grad, const float* saved, const float* upstream, const float* lse,
    const float*, const float* packed, const float* bias, const float*,
    int batch, int sequence, int heads, int bias_columns) {
    int q = blockIdx.x * blockDim.x + threadIdx.x;
    int h = blockIdx.y, b = blockIdx.z;
    if (q >= sequence || h >= heads || b >= batch) return;
    const int hidden = heads * 32;
    const int row = b * sequence + q;
    const float* qptr = packed + row * (3 * hidden) + h * 32;
    const float* dy = upstream + row * hidden + h * 32;
    float* dq = grad + row * (3 * hidden) + h * 32;
    const float* y = saved + row * hidden + h * 32;
    float qgrad[32];
    #pragma unroll
    for (int d = 0; d < 32; ++d) qgrad[d] = 0.0f;
    float scale = 0.17677669529663687F;
    float lse_q = lse[(b * heads + h) * ((sequence + 31) / 32 * 32) + q];
    float dotdy = 0.0f;
    #pragma unroll
    for (int d = 0; d < 32; ++d) dotdy = fmaf(dy[d], y[d], dotdy);
    for (int k = 0; k < sequence; ++k) {
        const float* kptr = packed + (b * sequence + k) * (3 * hidden) + hidden + h * 32;
        const float* vptr = kptr + hidden;
        float p = expf(dot32(qptr, kptr) * scale + bias[row * bias_columns + k] - lse_q);
        float dv = 0.0f;
        #pragma unroll
        for (int d = 0; d < 32; ++d) dv = fmaf(dy[d], vptr[d], dv);
        float ds = p * (dv - dotdy);
        for (int d = 0; d < 32; ++d) {
            qgrad[d] = fmaf(ds * scale, kptr[d], qgrad[d]);
            atomicAdd(grad + (b * sequence + k) * (3 * hidden) + hidden + h * 32 + d, ds * qptr[d] * scale);
            atomicAdd(grad + (b * sequence + k) * (3 * hidden) + 2 * hidden + h * 32 + d, p * dy[d]);
        }
    }
    for (int d = 0; d < 32; ++d) dq[d] = qgrad[d];
}
