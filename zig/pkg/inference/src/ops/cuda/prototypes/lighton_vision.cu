// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

#include <math.h>

__device__ __forceinline__ unsigned short lighton_bf16_rne(float value) {
    const unsigned bits = __float_as_uint(value);
    return (unsigned short)((bits + 0x7fffu + ((bits >> 16) & 1u)) >> 16);
}
extern "C" __global__ void lighton_vision_norm_bf16(
    unsigned short* out, const float* input, const float* gamma, const float* beta,
    unsigned rows, unsigned dim, float eps) {
    const unsigned row = blockIdx.x;
    if (row >= rows) return;
    const unsigned base = row * dim, tid = threadIdx.x;
    __shared__ float sums[256], squares[256];
    float sum = 0.f, square = 0.f;
    for (unsigned i = tid; i < dim; i += 256) {
        const float x = input[base + i];
        sum += x;
        square += x * x;
    }
    sums[tid] = sum; squares[tid] = square;
    __syncthreads();
    for (unsigned stride = 128; stride; stride >>= 1) {
        if (tid < stride) { sums[tid] += sums[tid + stride]; squares[tid] += squares[tid + stride]; }
        __syncthreads();
    }
    const float mean = sums[0] / (float)dim;
    const float variance = fmaxf(squares[0] / (float)dim - mean * mean, 0.f);
    const float inverse = rsqrtf(variance + eps);
    for (unsigned i = tid; i < dim; i += 256) {
        const float value = (input[base + i] - mean) * inverse * gamma[i] + beta[i];
        out[base + i] = lighton_bf16_rne(value);
    }
}
extern "C" __global__ void lighton_vision_bias_gelu_bf16(
    unsigned short* out, const float* input, const float* bias,
    unsigned count, unsigned dim) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    // The original bias kernel publishes this rounded F32 sum before GELU.
    const float x = __fadd_rn(input[i], bias[i % dim]);
    const float value = 0.5f * x * (1.f + tanhf(0.7978845608028654f * (x + 0.044715f * x * x * x)));
    out[i] = lighton_bf16_rne(value);
}
extern "C" __global__ void lighton_vision_bias_residual_f32(
    float* out, const float* input, const float* bias, const float* residual,
    unsigned count, unsigned dim) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    const float biased = __fadd_rn(input[i], bias[i % dim]);
    out[i] = __fadd_rn(residual[i], biased);
}

extern "C" __global__ void lighton_vision_qkv_rope_bf16(
    unsigned short* q, unsigned short* k, unsigned short* v,
    const float* packed, const unsigned* positions,
    unsigned rows, unsigned heads, unsigned dim, float theta) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned half = dim / 2;
    const unsigned pairs = rows * heads * half;
    if (i >= pairs) return;
    const unsigned chunk = i / half, pair = i % half;
    const unsigned token = chunk / heads, head = chunk % heads;
    const unsigned width = heads * dim;
    const unsigned axis_pairs = half / 2;
    const unsigned axis = pair < axis_pairs ? 0u : 1u;
    const unsigned local_pair = pair % axis_pairs;
    const unsigned position = positions[axis * rows + token];
    const float exponent = (2.f * (float)local_pair) / (float)(dim / 2u);
    const float frequency = 1.f / powf(theta, exponent);
    const float angle = (float)position * frequency;
    const float s = sinf(angle), c = cosf(angle);
    const unsigned src = token * 3 * width + head * dim + pair;
    const unsigned dst = chunk * dim + pair;
    const float ql = packed[src], qr = packed[src + half];
    const float kl = packed[src + width], kr = packed[src + width + half];
    q[dst] = lighton_bf16_rne(ql * c - qr * s);
    q[dst + half] = lighton_bf16_rne(ql * s + qr * c);
    k[dst] = lighton_bf16_rne(kl * c - kr * s);
    k[dst + half] = lighton_bf16_rne(kl * s + kr * c);
    v[dst] = lighton_bf16_rne(packed[src + 2 * width]);
    v[dst + half] = lighton_bf16_rne(packed[src + 2 * width + half]);
}
