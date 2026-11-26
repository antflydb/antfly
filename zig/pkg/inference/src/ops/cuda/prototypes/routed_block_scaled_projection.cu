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

#include <cuda_bf16.h>
#include <cuda_fp8.h>
#include <float.h>
#include <math.h>
#include <stdint.h>
#include "../kernels/quantize_bf16_nvfp4.cuh"
#include "../kernels/quantize_bf16_mxfp8.cuh"
#include "../kernels/routed_swiglu.cuh"

namespace {

using antfly_nvfp4::e4m3;
using antfly_nvfp4::e2m1_twice;

using antfly_mxfp8::e8m0;

__device__ uint8_t e4m3_block(uint8_t* encoded, const __nv_bfloat16* source) {
    float maximum = 0.0f;
    #pragma unroll
    for (unsigned i = 0; i < 32u; ++i)
        maximum = fmaxf(maximum, fabsf(__bfloat162float(source[i])));
    const unsigned exponent = antfly_mxfp8::block_exponent(maximum);
    const float inverse = antfly_mxfp8::inverse_scale(exponent);
    #pragma unroll
    for (unsigned i = 0; i < 32u; ++i)
        encoded[i] = __nv_cvt_float_to_fp8(__bfloat162float(source[i]) * inverse, __NV_SATFINITE, __NV_E4M3);
    return (uint8_t)exponent;
}

__device__ float stable_log_sigmoid(float x) {
    return fminf(x, 0.0f) - log1pf(expf(-fabsf(x)));
}

template <bool FP4>
__device__ void project(
    __nv_bfloat16* output,
    const uint8_t* act,
    const uint8_t* act_scales,
    const uint8_t* weights,
    const uint8_t* weight_scales,
    const uint32_t* experts,
    const float* outer_scale,
    uint32_t k,
    uint32_t n,
    uint32_t expert_count
) {
    constexpr unsigned block_values = FP4 ? 16 : 32;
    const unsigned lane = threadIdx.x & 31u;
    const unsigned column = blockIdx.x * (blockDim.x / 32u) + threadIdx.x / 32u;
    if (column >= n) return;
    const unsigned row = blockIdx.y;
    const unsigned expert = experts[row];
    if (expert >= expert_count) {
        if (lane == 0) output[(size_t)row * n + column] = __float2bfloat16_rn(0);
        return;
    }

    const size_t packed_k = FP4 ? k / 2 : k;
    const unsigned group_count = k / block_values;
    const size_t weight_row = (size_t)expert * n + column;
    const uint8_t* a = act + (size_t)row * packed_k;
    const uint8_t* w = weights + weight_row * packed_k;
    const uint8_t* sa = act_scales + (size_t)row * group_count;
    const uint8_t* sw = weight_scales + weight_row * group_count;
    float sum = 0.0f;
    for (unsigned group = lane; group < group_count; group += 32u) {
        if (FP4) {
            int dot = 0;
            #pragma unroll
            for (unsigned j = 0; j < 8u; ++j) {
                const uint32_t ai = a[group * 8u + j];
                const uint32_t wi = w[group * 8u + j];
                dot += e2m1_twice(ai) * e2m1_twice(wi);
                dot += e2m1_twice(ai >> 4) * e2m1_twice(wi >> 4);
            }
            const float scale = e4m3(sa[group]) * e4m3(sw[group]);
            sum = __fmaf_rn((float)dot * 0.25f, scale, sum);
        } else {
            const float input_scale = e8m0(sa[group]);
            const float weight_scale = e8m0(sw[group]);
            #pragma unroll
            for (unsigned j = 0; j < 32u; ++j) {
                const unsigned index = group * 32u + j;
                const float av = __bfloat162float(__float2bfloat16_rn(e4m3(a[index]) * input_scale));
                const float wv = __bfloat162float(__float2bfloat16_rn(e4m3(w[index]) * weight_scale));
                sum = __fmaf_rn(av, wv, sum);
            }
        }
    }
    #pragma unroll
    for (int stride = 16; stride > 0; stride >>= 1)
        sum += __shfl_down_sync(0xffffffffu, sum, stride);
    if (lane == 0) {
        if (FP4) sum *= outer_scale[row];
        output[(size_t)row * n + column] = __float2bfloat16_rn(sum);
    }
}

} // namespace

namespace {
__device__ void normalize_selected_routes(float* weights, float (&selected)[8], unsigned topk, float maximum, float route_scale) {
    float sum = 0.0f;
    for (unsigned k = 0; k < topk; ++k) {
        selected[k] = expf(selected[k] - maximum);
        sum += selected[k];
    }
    for (unsigned k = 0; k < topk; ++k) weights[k] = (selected[k] / sum) * route_scale;
}

__device__ void route_warp_128(uint32_t* ids, float* weights, const float* logits, const float* bias, unsigned topk, float route_scale) {
    const unsigned lane = threadIdx.x;
    unsigned long long keys[4];
    #pragma unroll
    for (unsigned tile = 0; tile < 4; ++tile) {
        const unsigned expert = tile * 32 + lane;
        const float score = __frcp_rn(1.0f + expf(-logits[expert])) + bias[expert];
        const unsigned bits = score == 0.0f ? 0 : __float_as_uint(score);
        const unsigned ordered = bits >> 31 ? ~bits : bits ^ 0x80000000u;
        // The scalar scan never selects NaN or -infinity and resolves ties
        // by advancing in expert order. Keep both rules in the key.
        keys[tile] = score > -INFINITY ? ((unsigned long long)ordered << 32) | ~expert : 0;
    }

    unsigned selected_expert = 0;
    for (unsigned k = 0; k < topk; ++k) {
        auto best = max(max(keys[0], keys[1]), max(keys[2], keys[3]));
        #pragma unroll
        for (unsigned offset = 16; offset != 0; offset /= 2)
            best = max(best, __shfl_down_sync(0xffffffffu, best, offset));
        best = __shfl_sync(0xffffffffu, best, 0);
        const unsigned expert = best != 0 ? ~(unsigned)best : 0;
        #pragma unroll
        for (unsigned tile = 0; tile < 4; ++tile)
            if (expert == tile * 32 + lane) keys[tile] = 0;
        if (lane == k) selected_expert = expert;
        if (lane == 0) ids[k] = expert;
    }
    const float log_weight = lane < topk ? stable_log_sigmoid(logits[selected_expert]) : -INFINITY;
    const float maximum = __shfl_sync(0xffffffffu, antfly_row_warp_max(log_weight), 0);
    const float value = expf(log_weight - maximum);
    float sum = 0.0f;
    for (unsigned k = 0; k < topk; ++k) sum += __shfl_sync(0xffffffffu, value, k);
    if (lane < topk) weights[lane] = (value / sum) * route_scale;
}
} // namespace

extern "C" __global__ void antfly_route_biased_sigmoid_topk_f32(
    uint32_t* ids, float* weights, const float* logits, const float* bias,
    uint32_t experts, uint32_t topk, float route_scale
) {
    const unsigned row = blockIdx.x;
    const unsigned tid = threadIdx.x;
    const float* row_logits = logits + (size_t)row * experts;
    if (experts == 128 && blockDim.x == 32) {
        route_warp_128(ids + (size_t)row * topk, weights + (size_t)row * topk, row_logits, bias, topk, route_scale);
        return;
    }
    __shared__ float scores[256];
    if (tid < experts) scores[tid] = __frcp_rn(1.0f + expf(-row_logits[tid])) + bias[tid];
    __syncthreads();
    if (tid != 0) return;

    float selected[8];
    float maximum = -INFINITY;
    for (unsigned k = 0; k < topk; ++k) {
        unsigned best = 0;
        float best_score = -INFINITY;
        for (unsigned expert = 0; expert < experts; ++expert) {
            const float value = scores[expert];
            if (value > best_score) {
                best_score = value;
                best = expert;
            }
        }
        scores[best] = -INFINITY;
        ids[(size_t)row * topk + k] = best;
        selected[k] = stable_log_sigmoid(row_logits[best]);
        maximum = fmaxf(maximum, selected[k]);
    }
    normalize_selected_routes(weights + (size_t)row * topk, selected, topk, maximum, route_scale);
}

extern "C" __global__ void antfly_weighted_softcapped_swiglu_interleaved_bf16(
    __nv_bfloat16* output, const __nv_bfloat16* gate_up, const float* route_weights,
    uint32_t columns, uint32_t rows, float gate_cap, float up_cap
) {
    const unsigned column = blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned row = blockIdx.y;
    if (column >= columns || row >= rows) return;
    const size_t index = (size_t)row * columns + column;
    output[index] = antfly_routed_swiglu::weighted(gate_up[2 * index], gate_up[2 * index + 1], route_weights[row], gate_cap, up_cap);
}

extern "C" __global__ void antfly_reduce_routed_rows_bf16(
    __nv_bfloat16* output, const __nv_bfloat16* input,
    uint32_t columns, uint32_t rows, uint32_t route_count
) {
    const unsigned column = blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned row = blockIdx.y;
    if (column >= columns || row >= rows) return;
    const size_t base = (size_t)row * route_count * columns + column;
    float sum = 0.0f;
    for (unsigned i = 0; i < route_count; ++i)
        sum += __bfloat162float(input[base + (size_t)i * columns]);
    output[(size_t)row * columns + column] = __float2bfloat16_rn(sum);
}

extern "C" __global__ void antfly_quantize_bf16_rows_e2m1_e4m3(
    uint8_t* values, uint8_t* scales, float* outer, const __nv_bfloat16* input,
    const uint32_t* ids, const float* weight_amax, const float* weight_denominator,
    uint32_t width, uint32_t repeat, uint32_t expert_count
) {
    antfly_nvfp4::quantize_nvfp4_rows<false>(values, scales, outer, input, ids, weight_amax,
                                              weight_denominator, width, repeat, expert_count);
}

extern "C" __global__ void antfly_quantize_bf16_rows_e4m3_e8m0(
    uint8_t* values, uint8_t* scales, float*, const __nv_bfloat16* input,
    const uint32_t*, const float*, const float*,
    uint32_t width, uint32_t repeat, uint32_t
) {
    const unsigned row = blockIdx.x;
    const unsigned group_count = width / 32u;
    const __nv_bfloat16* source = input + (size_t)(row / repeat) * width;
    for (unsigned group = threadIdx.x; group < group_count; group += blockDim.x) {
        uint8_t* dst = values + (size_t)row * width + group * 32u;
        scales[(size_t)row * group_count + group] = e4m3_block(dst, source + group * 32u);
    }
}

extern "C" __global__ void antfly_routed_e2m1_e4m3_projection_bf16(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as,
    const uint8_t* w, const uint8_t* ws, const uint32_t* ids,
    const float* scale, uint32_t k, uint32_t n, uint32_t expert_count
) {
    project<true>(out, a, as, w, ws, ids, scale, k, n, expert_count);
}

extern "C" __global__ void antfly_routed_e4m3_e8m0_projection_bf16(
    __nv_bfloat16* out, const uint8_t* a, const uint8_t* as,
    const uint8_t* w, const uint8_t* ws, const uint32_t* ids,
    const float* scale, uint32_t k, uint32_t n, uint32_t expert_count
) {
    project<false>(out, a, as, w, ws, ids, scale, k, n, expert_count);
}
