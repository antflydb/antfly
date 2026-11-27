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

#include <cuda_bf16.h>
#include <stdint.h>
#include <math.h>
#include "../kernels/row_program.cuh"

namespace {
__device__ float block_sum(float x, float* shared) {
    const unsigned lane = threadIdx.x % 32u;
    const unsigned warp = threadIdx.x / 32u;
    x = antfly_row_warp_sum(x);
    if (lane == 0u) shared[warp] = x;
    __syncthreads();
    x = lane < 4u ? shared[lane] : 0.0f;
    x = __shfl_sync(0xffffffffu, antfly_row_warp_sum(x), 0);
    __syncthreads();
    return x;
}

__device__ float tanh_approx(float x) {
    float y;
    asm("tanh.approx.f32 %0, %1;" : "=f"(y) : "f"(x));
    return y;
}
} // namespace

namespace {
__device__ void residual_weights(
    const __nv_bfloat16* blocks, const __nv_bfloat16* last, const __nv_bfloat16* add_last,
    const float* query, unsigned rows, unsigned row, unsigned width, unsigned source_count, float eps,
    float* reduce, float* logits, float* normalized_weights
) {
    const unsigned tid = threadIdx.x;
    const float query_scale = rsqrtf((float)width);
    for (unsigned source = 0; source < source_count; ++source) {
        const bool is_last = source + 1 == source_count;
        const __nv_bfloat16* base = is_last ? last + (size_t)row * width : blocks + ((size_t)source * rows + row) * width;
        const bool has_add = add_last && is_last;
        float dot = 0.0f, square = 0.0f;
        for (unsigned i = tid; i < width; i += 128u) {
            const float term = __bfloat162float(base[i]) + (has_add ? __bfloat162float(add_last[(size_t)row * width + i]) : 0.0f);
            dot = __fmaf_rn(term, query[i] * query_scale, dot);
            square = __fmaf_rn(term, term, square);
        }
        dot = block_sum(dot, reduce);
        const float inverse = rsqrtf(block_sum(square, reduce) / width + eps);
        if (tid == 0u) {
            logits[source] = dot * inverse;
            normalized_weights[source] = inverse;
        }
    }
    if (tid == 0u) {
        float max_logit = -INFINITY;
        for (unsigned source = 0; source < source_count; ++source)
            max_logit = fmaxf(max_logit, logits[source]);
        float denominator = 0.0f;
        for (unsigned source = 0; source < source_count; ++source) {
            logits[source] = expf(logits[source] - max_logit);
            denominator += logits[source];
        }
        for (unsigned source = 0; source < source_count; ++source)
            normalized_weights[source] *= logits[source] / denominator;
    }
    __syncthreads();
}

__device__ float residual_value(
    const __nv_bfloat16* blocks, const __nv_bfloat16* last, const __nv_bfloat16* add_last,
    unsigned rows, unsigned row, unsigned width, unsigned sources, unsigned column, const float* weights
) {
    float value = 0;
    for (unsigned i = 0; i < sources; ++i) {
        const bool final = i + 1 == sources;
        const __nv_bfloat16* base = final ? last + (size_t)row * width : blocks + ((size_t)i * rows + row) * width;
        const float update = final && add_last ? __bfloat162float(add_last[(size_t)row * width + column]) : 0.0f;
        value = __fmaf_rn(__bfloat162float(base[column]) + update, weights[i], value);
    }
    return value;
}
} // namespace

extern "C" __global__ void antfly_mix_normalized_residual_blocks_bf16_f32(
    float* output, const __nv_bfloat16* blocks, const __nv_bfloat16* last, const __nv_bfloat16* add_last,
    const float* query, uint32_t rows, uint32_t width, uint32_t source_count, float eps
) {
    const unsigned row = blockIdx.x;
    __shared__ float reduce[4], logits[8], weights[8];
    residual_weights(blocks, last, add_last, query, rows, row, width, source_count, eps, reduce, logits, weights);
    for (unsigned i = threadIdx.x; i < width; i += 128u)
        output[(size_t)row * width + i] = residual_value(blocks, last, add_last, rows, row, width, source_count, i, weights);
}

extern "C" __global__ void antfly_mix_residual_blocks_rms_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* blocks, const __nv_bfloat16* last, const __nv_bfloat16* add_last,
    const float* query, const __nv_bfloat16* gamma, uint32_t rows, uint32_t width, uint32_t sources,
    float epsilon, float norm_epsilon
) {
    const unsigned row = blockIdx.x;
    __shared__ float reduce[4], logits[8], weights[8], mixed[4096];
    residual_weights(blocks, last, add_last, query, rows, row, width, sources, epsilon, reduce, logits, weights);
    float square = 0;
    for (unsigned i = threadIdx.x; i < width; i += 128u) {
        const float value = residual_value(blocks, last, add_last, rows, row, width, sources, i, weights);
        mixed[i] = value;
        square = __fmaf_rn(value, value, square);
    }
    const float norm = rsqrtf(block_sum(square, reduce) / width + norm_epsilon);
    for (unsigned i = threadIdx.x; i < width; i += 128u)
        out[(size_t)row * width + i] = __float2bfloat16_rn((mixed[i] * norm) * __bfloat162float(gamma[i]));
}

namespace {
template<class T> struct SharedRows { const T* values[3]; };
static_assert(sizeof(SharedRows<float>) == 24, "row pointer group ABI");

__device__ void mix_normalize_pair(
    __nv_bfloat16* pair, float* message, const float* dest,
    const __nv_bfloat16* first, const __nv_bfloat16* second, const float* query,
    uint32_t rows, uint32_t row, uint32_t width, float eps
) {
    const size_t base = (size_t)row * width;
    __shared__ float reduce[4], logits[2], weights[2];
    residual_weights(first, second, nullptr, query, rows, row, width, 2, eps, reduce, logits, weights);
    float dest_square = 0, msg_square = 0;
    for (unsigned i = threadIdx.x; i < width; i += 128u) {
        const float value = residual_value(first, second, nullptr, rows, row, width, 2, i, weights);
        message[base + i] = value;
        dest_square = __fmaf_rn(dest[base + i], dest[base + i], dest_square);
        msg_square = __fmaf_rn(value, value, msg_square);
    }
    const float dest_norm = rsqrtf(block_sum(dest_square, reduce) / width + eps);
    const float msg_norm = rsqrtf(block_sum(msg_square, reduce) / width + eps);
    for (unsigned i = threadIdx.x; i < width; i += 128u) {
        pair[base * 2 + i] = __float2bfloat16_rn(dest[base + i] * dest_norm);
        pair[base * 2 + width + i] = __float2bfloat16_rn(message[base + i] * msg_norm);
    }
}

__device__ void blend_rms(
    __nv_bfloat16* output, const float* input, const float* message, const float* gain,
    const float* forget, const __nv_bfloat16* gamma, uint32_t width, float eps
) {
    const unsigned tid = threadIdx.x;
    __shared__ float reduce[4];
    float square = 0.0f;
    for (unsigned i = tid; i < width; i += 128u) {
        const float x = (1.0f - tanh_approx(forget[i])) * input[i] + tanh_approx(gain[i]) * message[i];
        square = __fmaf_rn(x, x, square);
    }
    const float norm = rsqrtf(block_sum(square, reduce) / width + eps);
    for (unsigned i = tid; i < width; i += 128u) {
        const float x = (1.0f - tanh_approx(forget[i])) * input[i] + tanh_approx(gain[i]) * message[i];
        output[i] = __float2bfloat16_rn((x * norm) * __bfloat162float(gamma[i]));
    }
}
} // namespace

extern "C" __global__ void antfly_mix_two_blocks_normalize_pair_bf16(
    __nv_bfloat16* pair, float* message, const float* dest,
    const __nv_bfloat16* first, const __nv_bfloat16* second, const float* query,
    uint32_t rows, uint32_t width, float eps
) {
    mix_normalize_pair(pair, message, dest, first, second, query, rows, blockIdx.x, width, eps);
}

extern "C" __global__ void antfly_mix_two_blocks_normalize_three_pairs_bf16(
    __nv_bfloat16* pairs, float* messages, const float* dest, uint32_t dest_stride,
    const __nv_bfloat16* first, const __nv_bfloat16* second, const __grid_constant__ SharedRows<float> queries,
    uint32_t width, float eps
) {
    const unsigned row = blockIdx.x;
    const size_t base = (size_t)row * width;
    mix_normalize_pair(pairs + base * 2, messages + base, dest + (size_t)row * dest_stride,
                       first, second, queries.values[row], 1, 0, width, eps);
}

extern "C" __global__ void antfly_controller_blend_rms_f32_bf16(
    __nv_bfloat16* output, const float* input, const float* message, const float* gain,
    const float* forget, const __nv_bfloat16* gamma, uint32_t width, float eps
) {
    const size_t base = (size_t)blockIdx.x * width;
    blend_rms(output + base, input + base, message + base, gain + base, forget + base, gamma, width, eps);
}

extern "C" __global__ void antfly_controller_blend_three_rms_f32_bf16(
    __nv_bfloat16* output, const float* dest, uint32_t dest_stride, const float* messages,
    const float* gains_and_forgets, const __grid_constant__ SharedRows<__nv_bfloat16> gammas, uint32_t width, float eps
) {
    const unsigned row = blockIdx.x;
    const size_t base = (size_t)row * width;
    const float* gains = gains_and_forgets + base * 2;
    blend_rms(output + base, dest + (size_t)row * dest_stride, messages + base,
              gains, gains + width, gammas.values[row], width, eps);
}

extern "C" __global__ void antfly_rms_norm_to_bf16(
    __nv_bfloat16* output, const void* source, const __nv_bfloat16* gamma,
    uint32_t width, uint32_t input_f32, float eps
) {
    const unsigned row = blockIdx.x;
    const unsigned tid = threadIdx.x;
    const size_t base = (size_t)row * width;
    __shared__ float reduce[4];
    float square = 0.0f;
    for (unsigned i = tid; i < width; i += 128u) {
        const float x = input_f32 ? ((const float*)source)[base + i] : __bfloat162float(((const __nv_bfloat16*)source)[base + i]);
        square = __fmaf_rn(x, x, square);
    }
    const float norm = rsqrtf(block_sum(square, reduce) / width + eps);
    for (unsigned i = tid; i < width; i += 128u) {
        const float x = input_f32 ? ((const float*)source)[base + i] : __bfloat162float(((const __nv_bfloat16*)source)[base + i]);
        output[base + i] = __float2bfloat16_rn((x * norm) * __bfloat162float(gamma[i]));
    }
}

extern "C" __global__ void antfly_residual_sum3_bf16(
    __nv_bfloat16* out, const __nv_bfloat16* prev, const __nv_bfloat16* a,
    const __nv_bfloat16* b, uint32_t values
) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < values) {
        const float p = prev ? __bfloat162float(prev[i]) : 0.0f;
        out[i] = __float2bfloat16_rn((p + __bfloat162float(a[i])) + __bfloat162float(b[i]));
    }
}

extern "C" __global__ void antfly_softcap_logits_f32(float* logits, uint32_t count, float cap) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < count) logits[i] = tanh_approx(logits[i] / cap) * cap;
}
