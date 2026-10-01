// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Self-contained GLiNER2.5 boundary attention. CUDA toolkit only.

__device__ __forceinline__ float dot32(const float* a, const float* b) {
    float s = 0.0f;
    #pragma unroll
    for (int d = 0; d < 32; ++d) s = fmaf(a[d], b[d], s);
    return s;
}

extern "C" __global__ void boundary_bias(float* bias, const float* valid,
    int batch, int sequence, int columns, int window) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= batch * sequence * columns) return;
    int k = i % columns, q = (i / columns) % sequence, b = i / (columns * sequence);
    bool allowed = k < sequence && (q == k || (valid[b * sequence + k] != 0.0f &&
        (window == 0 || abs(q - k) <= window)));
    bias[i] = allowed ? 0.0f : -__int_as_float(0x7f800000);
}

// One thread computes one query row. This intentionally favors portability and
// independent provenance over the specialized PyTorch/CUTLASS implementation.
extern "C" __global__ void boundary_forward(
    float* output, float* lse, const float* packed, const float* bias,
    int batch, int sequence, int heads, int bias_columns) {
    int q = blockIdx.x * blockDim.x + threadIdx.x;
    int h = blockIdx.y, b = blockIdx.z;
    if (q >= sequence || h >= heads || b >= batch) return;
    const int hidden = heads * 32;
    const int row = b * sequence + q;
    const float* qptr = packed + row * (3 * hidden) + h * 32;
    float* out = output + row * hidden + h * 32;
    float maxv = -3.402823466e+38F;
    for (int k = 0; k < sequence; ++k) {
        const float* kptr = packed + (b * sequence + k) * (3 * hidden) + hidden + h * 32;
        float s = dot32(qptr, kptr) * 0.17677669529663687F + bias[row * bias_columns + k];
        maxv = fmaxf(maxv, s);
    }
    float denom = 0.0f;
    for (int d = 0; d < 32; ++d) out[d] = 0.0f;
    for (int k = 0; k < sequence; ++k) {
        const float* kptr = packed + (b * sequence + k) * (3 * hidden) + hidden + h * 32;
        const float* vptr = kptr + hidden;
        float p = expf(dot32(qptr, kptr) * 0.17677669529663687F + bias[row * bias_columns + k] - maxv);
        denom += p;
        for (int d = 0; d < 32; ++d) out[d] = fmaf(p, vptr[d], out[d]);
    }
    float inv = 1.0f / denom;
    for (int d = 0; d < 32; ++d) out[d] *= inv;
    lse[(b * heads + h) * ((sequence + 31) / 32 * 32) + q] = logf(denom) + maxv;
}

extern "C" __global__ void boundary_zero(float* output, int elements) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < elements) output[i] = 0.0f;
}
