#include "../kernels/rotary_program.cuh"
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
#include "../kernels/row_program.cuh"

__device__ __forceinline__ unsigned short lighton_bf16_rne(float value) {
    const unsigned bits = __float_as_uint(value);
    return (unsigned short)((bits + 0x7fffu + ((bits >> 16) & 1u)) >> 16);
}
extern "C" __global__ void lighton_vision_norm_bf16(
    unsigned short* out, const float* input, const float* gamma, const float* beta,
    unsigned rows, unsigned dim, float eps) {
    const unsigned row = blockIdx.x;
    if (row >= rows) return;
    const unsigned base = row * dim;
    __shared__ float sums[256], squares[256];
    antfly_row_layer_norm<256>(dim, eps, sums, squares,
        [&](unsigned i) { return input[base + i]; },
        [&](unsigned i, float x, float mean, float inverse) {
            out[base + i] = lighton_bf16_rne((x - mean) * inverse * gamma[i] + beta[i]);
        });
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

extern "C" __global__ void antfly_qkv_rope_two_axis_bf16(
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
    const auto rotary=antfly_rotary_pair_indices<false,true>(pair,axis_pairs);
    const unsigned axis=rotary.axis,local_pair=rotary.local;
    const unsigned position = positions[axis * rows + token];
    const float exponent = (2.f * (float)local_pair) / (float)(dim / 2u);
    const float frequency = 1.f / powf(theta, exponent);
    const float angle = (float)position * frequency;
    const float s = sinf(angle), c = cosf(angle);
    const unsigned src = token * 3 * width + head * dim + pair;
    const unsigned dst = chunk * dim + pair;
    const float ql = packed[src], qr = packed[src + half];
    const float kl = packed[src + width], kr = packed[src + width + half];
    q[dst] = lighton_bf16_rne(antfly_rotary_component(ql,qr,c,s,false));
    q[dst + half] = lighton_bf16_rne(antfly_rotary_component(ql,qr,c,s,true));
    k[dst] = lighton_bf16_rne(antfly_rotary_component(kl,kr,c,s,false));
    k[dst + half] = lighton_bf16_rne(antfly_rotary_component(kl,kr,c,s,true));
    v[dst] = lighton_bf16_rne(packed[src + 2 * width]);
    v[dst + half] = lighton_bf16_rne(packed[src + 2 * width + half]);
}

// Learned positions use align-corners interpolation in merge-major order.
// Explicit operations preserve the host reference's F32 rounding boundaries.
extern "C" __global__ void lighton_vision_learned_positions_f32(
    float* out, const float* input, const void* table,
    unsigned grid_x, unsigned grid_y, unsigned merge, unsigned side,
    unsigned dim, unsigned table_bf16) {
    const unsigned index = blockIdx.x * blockDim.x + threadIdx.x;
    if (index >= grid_x * grid_y * dim) return;
    const unsigned row = index / dim, h = index % dim;
    const unsigned inner = row % (merge * merge), group = row / (merge * merge);
    const unsigned x = (group % (grid_x / merge)) * merge + inner % merge;
    const unsigned y = (group / (grid_x / merge)) * merge + inner / merge;
    const float fx = grid_x <= 1 || side <= 1 ? 0.f :
        __fdiv_rn(__fmul_rn(float(x), float(side - 1)), float(grid_x - 1));
    const float fy = grid_y <= 1 || side <= 1 ? 0.f :
        __fdiv_rn(__fmul_rn(float(y), float(side - 1)), float(grid_y - 1));
    const unsigned x0 = unsigned(floorf(fx)), y0 = unsigned(floorf(fy));
    const unsigned x1 = min(x0 + 1, side - 1), y1 = min(y0 + 1, side - 1);
    const float wx = __fsub_rn(fx, float(x0)), wy = __fsub_rn(fy, float(y0));
    const unsigned ids[4] = {
        (y0 * side + x0) * dim + h, (y0 * side + x1) * dim + h,
        (y1 * side + x0) * dim + h, (y1 * side + x1) * dim + h
    };
    float values[4];
    #pragma unroll
    for (unsigned i = 0; i < 4; ++i) values[i] = table_bf16 ?
        __uint_as_float(unsigned(static_cast<const unsigned short*>(table)[ids[i]]) << 16) :
        static_cast<const float*>(table)[ids[i]];
    const float top = __fadd_rn(__fmul_rn(values[0], __fsub_rn(1.f, wx)), __fmul_rn(values[1], wx));
    const float bottom = __fadd_rn(__fmul_rn(values[2], __fsub_rn(1.f, wx)), __fmul_rn(values[3], wx));
    const float position = __fadd_rn(__fmul_rn(top, __fsub_rn(1.f, wy)), __fmul_rn(bottom, wy));
    out[index] = __fadd_rn(input[index], position);
}
