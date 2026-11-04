// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Exact RMSNorm/Q8_1 fusion. Runtime dispatch remains an explicit opt-in.
#pragma once
#include <cuda_fp16.h>

// Exactly the production 256-thread reduction tree, including addition order.
__device__ __forceinline__ float rms_q8_tree(float value, float* partial) {
    const unsigned tid = threadIdx.x;
    partial[tid] = value;
    __syncthreads();
    if (tid < 32u) {
        const float a = partial[tid] + partial[tid + 128u];
        const float b = partial[tid + 64u] + partial[tid + 192u];
        const float c = partial[tid + 32u] + partial[tid + 160u];
        const float d = partial[tid + 96u] + partial[tid + 224u];
        float sum = (a + b) + (c + d);
        for (unsigned offset = 16u; offset; offset >>= 1u)
            sum += __shfl_down_sync(0xffffffffu, sum, offset);
        if (tid == 0u) partial[0] = sum;
    }
    __syncthreads();
    return partial[0];
}

__device__ __forceinline__ void rms_q8_store(unsigned char* block, float value) {
    const unsigned lane = threadIdx.x & 31u;
    float maximum = fabsf(value);
    for (int offset = 16; offset; offset >>= 1)
        maximum = fmaxf(maximum, __shfl_down_sync(0xffffffffu, maximum, offset));
    maximum = __shfl_sync(0xffffffffu, maximum, 0);
    const float scale = maximum > 0.0f ? maximum / 127.0f : 0.0f;
    int quantized = 0;
    if (scale > 0.0f) {
        quantized = __float2int_rn(value / scale);
        quantized = max(-127, min(127, quantized));
    }
    block[4u + lane] = (unsigned char)(signed char)quantized;
    if (lane == 0u) {
        const unsigned short raw = __half_as_ushort(__float2half_rn(scale));
        block[0] = (unsigned char)(raw & 255u);
        block[1] = (unsigned char)(raw >> 8u);
        block[2] = block[3] = 0u;
    }
}

// Op0 = weighted RMS; Op1 = weighted RMS + residual; Op2 = scale after add.
// Tiled=false supports in-place F32 output: all inputs are retained before
// publication. Tiled=true repeats reduction across10CTAs to expose output
// quantization parallelism; its F32 output MUST NOT alias either input.
template <unsigned Op, bool Tiled>
__device__ __forceinline__ void rms_q8_body(
    float* output, unsigned char* q8, unsigned short* bf16,
    const float* input, const float* weight, const float* residual,
    const float* scalar, unsigned rows, float eps
) {
    const unsigned row = Tiled ? blockIdx.x / 10u : blockIdx.x;
    const unsigned tile = Tiled ? blockIdx.x % 10u : 0u;
    if (row >= rows) return;
    const unsigned tid = threadIdx.x;
    const unsigned base = row * 2560u;
    __shared__ float partial[256];
    float values[10], weights[10], residuals[10];
#pragma unroll
    for (unsigned j = 0; j < 10u; ++j) {
        const unsigned index = tid + j * 256u;
        values[j] = input[base + index];
        if (!Tiled || j == tile) {
            weights[j] = weight[index];
            if (Op != 0u) residuals[j] = residual[base + index];
        }
    }
    float sum = 0.0f;
#pragma unroll
    for (unsigned j = 0; j < 10u; ++j) sum += values[j] * values[j];
    const float sumsq = rms_q8_tree(sum, partial);
    float mean;
    asm volatile("div.rn.f32 %0, %1, %2;" : "=f"(mean) : "f"(sumsq), "f"(2560.0f));
    const float norm_scale = rsqrtf(mean + eps);
    const float output_scale = Op == 2u ? scalar[0] : 1.0f;
#pragma unroll
    for (unsigned j = 0u; j < 10u; ++j) {
        if (Tiled && j != tile) continue;
        float value = values[j] * norm_scale * weights[j];
        if (Op != 0u) value += residuals[j];
        if (Op == 2u) value *= output_scale;
        const unsigned index = base + tid + j * 256u;
        output[index] = value;
        if (bf16) {
            const unsigned bits = __float_as_uint(value);
            bf16[index] = (unsigned short)((bits + 0x7fffu + ((bits >> 16u) & 1u)) >> 16u);
        }
        const unsigned block = row * 80u + j * 8u + (tid >> 5u);
        rms_q8_store(q8 + block * 36u, value);
    }
}

#define RMS_Q8_ENTRY(name, operation, tiled) \
extern "C" __global__ void name(float* output, unsigned char* q8, unsigned short* bf16, \
    const float* input, const float* weight, const float* residual, const float* scalar, \
    unsigned rows, float eps) { \
    rms_q8_body<operation, tiled>(output, q8, bf16, input, weight, residual, scalar, rows, eps); \
}
RMS_Q8_ENTRY(antfly_rms_q8_1_2560, 0u, false)
RMS_Q8_ENTRY(antfly_rms_add_q8_1_2560, 1u, false)
RMS_Q8_ENTRY(antfly_rms_add_scale_q8_1_2560, 2u, false)
RMS_Q8_ENTRY(antfly_rms_q8_1_2560_tile256, 0u, true)
RMS_Q8_ENTRY(antfly_rms_add_q8_1_2560_tile256, 1u, true)
RMS_Q8_ENTRY(antfly_rms_add_scale_q8_1_2560_tile256, 2u, true)
#undef RMS_Q8_ENTRY
