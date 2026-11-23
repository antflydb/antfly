// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
//
// https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cuda_bf16.h>
#include <cuda_fp4.h>
#include <cuda_fp8.h>
#include <float.h>
#include <math.h>
#include <stdint.h>
#include "row_program.cuh"

namespace antfly_nvfp4 {

__device__ inline float e4m3(uint32_t code) {
    const uint32_t mag = code & 127u;
    const uint32_t exp = mag >> 3;
    const uint32_t fraction = mag & 7u;
    if (mag == 127u) return __uint_as_float(0x7fc00000u);
    const float value = exp == 0u
        ? (float)fraction * (1.0f / 512.0f)
        : __uint_as_float(((exp + 120u) << 23) | (fraction << 20));
    return code & 128u ? -value : value;
}

// E2M1 values are exact multiples of one half. Sum each 16-element inner
// product as integers before applying the two block scales and the factor 1/4.
__device__ inline int e2m1_twice(uint32_t code) {
    const uint32_t mag = code & 7u;
    const int value = mag < 4u ? (int)mag : (2 + (int)(mag & 1u)) << ((mag >> 1) - 1);
    return code & 8u ? -value : value;
}

__device__ inline uint8_t encode_e2m1(float x) {
    const float v = fabsf(x);
    const unsigned mag = (v > 0.25f) + (v >= 0.75f) + (v > 1.25f) +
        (v >= 1.75f) + (v > 2.5f) + (v >= 3.5f) + (v > 5.0f);
    return (uint8_t)(mag | ((__float_as_uint(x) >> 28) & 8u));
}

template<bool Native>
__device__ inline float quantize_e2m1_candidate(
    uint8_t* packed, const float* input, float inverse, float scale, float outer
) {
    float errors[16];
    #pragma unroll
    for (unsigned i = 0; i < 16u; i += 2u) {
        const float left = input[i] * inverse;
        const float right = input[i + 1] * inverse;
        float2 decoded;
        if constexpr (Native) {
            // The software encoder maps a NaN to zero with its original sign.
            const float x = isnan(left) ? copysignf(0.0f, left) : left;
            const float y = isnan(right) ? copysignf(0.0f, right) : right;
            const uint8_t code = __nv_cvt_float2_to_fp4x2(make_float2(x, y), __NV_E2M1, cudaRoundNearest);
            packed[i / 2] = code;
            decoded = __half22float2(static_cast<__half2>(__nv_cvt_fp4x2_to_halfraw2(code, __NV_E2M1)));
        } else {
            const uint8_t x = encode_e2m1(left);
            const uint8_t y = encode_e2m1(right);
            packed[i / 2] = x | (y << 4);
            decoded = make_float2((float)e2m1_twice(x) * 0.5f, (float)e2m1_twice(y) * 0.5f);
        }
        errors[i] = fabsf((decoded.x * scale) * outer - input[i]);
        errors[i + 1] = fabsf((decoded.y * scale) * outer - input[i + 1]);
    }
    #pragma unroll
    for (unsigned stride = 1; stride < 16u; stride <<= 1) {
        #pragma unroll
        for (unsigned i = 0; i < 16u; i += stride * 2u) errors[i] += errors[i + stride];
    }
    return errors[0];
}

template<bool Native>
__device__ inline uint8_t e2m1_block(uint8_t* packed, const __nv_bfloat16* source, float amax) {
    float input[16];
    float block_max = 0.0f;
    #pragma unroll
    for (unsigned i = 0; i < 16u; ++i) {
        input[i] = __bfloat162float(source[i]);
        block_max = fmaxf(block_max, fabsf(input[i]));
    }
    const float encoder = fminf(__fdiv_rn(1536.0f, amax), FLT_MAX);
    const float decoder = __frcp_rn(encoder);
    const float outer = amax * (1.0f / 1536.0f);
    const uint8_t code6 = __nv_cvt_float_to_fp8(fminf(block_max * (encoder * (1.0f / 6.0f)), 448.0f), __NV_SATFINITE, __NV_E4M3);
    const uint8_t code4 = __nv_cvt_float_to_fp8(fminf(block_max * (encoder * 0.25f), 448.0f), __NV_SATFINITE, __NV_E4M3);
    const float scale6 = e4m3(code6);
    const float scale4 = e4m3(code4);
    uint8_t values6[8], values4[8];
    const float error6 = quantize_e2m1_candidate<Native>(values6, input, fminf(__frcp_rn(scale6 * decoder), FLT_MAX), scale6, outer);
    const float error4 = quantize_e2m1_candidate<Native>(values4, input, fminf(__frcp_rn(scale4 * decoder), FLT_MAX), scale4, outer);
    const bool choose4 = error4 < error6;
    #pragma unroll
    for (unsigned i = 0; i < 8u; ++i)
        packed[i] = choose4 ? values4[i] : values6[i];
    return choose4 ? code4 : code6;
}

template<bool Native>
__device__ inline void quantize_nvfp4_rows(
    uint8_t* values, uint8_t* scales, float* outer, const __nv_bfloat16* input,
    const uint32_t* ids, const float* weight_amax, const float* weight_denominator,
    uint32_t width, uint32_t repeat, uint32_t expert_count
) {
    const unsigned row = blockIdx.x;
    const unsigned tid = threadIdx.x;
    const __nv_bfloat16* source = input + (size_t)(row / repeat) * width;
    __shared__ float warp_maxima[8];
    const unsigned lane = tid % 32;
    float maximum = 0.0f;
    for (unsigned i = tid; i < width; i += blockDim.x)
        maximum = fmaxf(maximum, fabsf(__bfloat162float(source[i])));
    maximum = antfly_row_warp_max(maximum);
    if (lane == 0) warp_maxima[tid / 32] = maximum;
    __syncthreads();
    maximum = antfly_row_warp_max(lane < blockDim.x / 32 ? warp_maxima[lane] : 0.0f);
    maximum = __shfl_sync(0xffffffffu, maximum, 0);
    const unsigned group_count = width / 16u;
    for (unsigned group = tid; group < group_count; group += blockDim.x) {
        uint8_t* dst = values + (size_t)row * (width / 2u) + group * 8u;
        scales[(size_t)row * group_count + group] = e2m1_block<Native>(dst, source + group * 16u, maximum);
    }
    if (tid == 0) {
        const uint32_t expert = ids[row];
        outer[row] = expert < expert_count ? (maximum * weight_amax[expert]) * __frcp_rn(1536.0f * *weight_denominator) : 0.0f;
    }
}

} // namespace antfly_nvfp4
