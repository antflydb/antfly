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
#include "../kernels/row_program.cuh"

namespace {

// Right-aligned history stores the last three original input rows. Old and
// candidate history are disjoint so an aborted append retains its source.
__device__ __nv_bfloat16 sample(
    const __nv_bfloat16* input,
    const __nv_bfloat16* history,
    int row,
    unsigned c,
    unsigned channels,
    unsigned valid_history
) {
    if (row >= 0) return input[(size_t)row * channels + c];
    if ((unsigned)-row > valid_history) return __float2bfloat16_rn(0);
    return history[(size_t)(row + 3) * channels + c];
}

__device__ __nv_bfloat16 convolve(
    const __nv_bfloat16* input, const __nv_bfloat16* history, const __nv_bfloat16* weight,
    const __nv_bfloat16* dynamic_taps, int row, unsigned c, unsigned channels,
    unsigned head_size, unsigned valid_history, float output_scale
) {
    const bool dynamic = dynamic_taps != nullptr;
    const unsigned heads = channels / head_size;
    const unsigned head = c / head_size;
    // Static checkpoint tensors are channel-major and oldest-to-current.
    // Dynamic checkpoint tensors are tap-major and current-to-oldest.
    float sum = 0;
    #pragma unroll
    for (unsigned tap = 0; tap < 4; ++tap) {
        const unsigned lag = dynamic ? tap : 3 - tap;
        const float next = __bfloat162float(sample(input, history, row - (int)lag, c, channels, valid_history));
        float w = __bfloat162float(weight[dynamic ? (size_t)tap * channels + c : (size_t)c * 4 + tap]);
        if (dynamic) w += __bfloat162float(dynamic_taps[((size_t)row * heads + head) * 4 + tap]);
        sum = __fmaf_rn(next, w, sum);
    }
    const size_t index = (size_t)row * channels + c;
    return __float2bfloat16_rn((sum + __bfloat162float(input[index])) * output_scale);
}

__device__ void stage_history(__nv_bfloat16* next_history, const __nv_bfloat16* input,
                              const __nv_bfloat16* history, unsigned c, unsigned channels,
                              unsigned rows, unsigned valid_history) {
    #pragma unroll
    for (unsigned i = 0; i < 3; ++i)
        next_history[(size_t)i * channels + c] = sample(input, history, (int)rows - 3 + (int)i, c, channels, valid_history);
}

} // namespace

extern "C" __global__ void antfly_causal_depthwise4_bf16_staged_history(
    __nv_bfloat16* output, __nv_bfloat16* next_history,
    const __nv_bfloat16* input, const __nv_bfloat16* history, const __nv_bfloat16* weight,
    const __nv_bfloat16* dynamic_taps,
    uint32_t channels, uint32_t head_size, uint32_t rows, uint32_t valid_history,
    float output_scale
) {
    const uint32_t c = blockIdx.x * blockDim.x + threadIdx.x;
    const int row = (int)blockIdx.y;
    if (c >= channels) return;
    output[(size_t)row * channels + c] = convolve(input, history, weight, dynamic_taps,
                                                     row, c, channels, head_size, valid_history, output_scale);
    if (row == 0) stage_history(next_history, input, history, c, channels, rows, valid_history);
}

extern "C" __global__ void antfly_causal_depthwise4_head_norm_d128_bf16(
    __nv_bfloat16* output, __nv_bfloat16* next_history,
    const __nv_bfloat16* input, const __nv_bfloat16* history, const __nv_bfloat16* weight,
    const __nv_bfloat16* dynamic_taps, uint32_t channels, uint32_t rows, uint32_t valid_history,
    float output_scale, float clip, float eps
) {
    constexpr unsigned dim = 128;
    const unsigned lane = threadIdx.x;
    const int row = (int)blockIdx.y;
    const unsigned head_base = blockIdx.x * dim;
    const unsigned c = head_base + lane;
    // Keep the original BF16 stage and one thread per stencil channel.
    __shared__ __nv_bfloat16 values[dim];
    values[lane] = convolve(input, history, weight, dynamic_taps, row, c, channels, dim, valid_history, output_scale);
    if (row == 0) stage_history(next_history, input, history, c, channels, rows, valid_history);
    __syncthreads();
    if (lane >= 32) return;

    // A single warp preserves the original head normalization accumulation order.
    float data[4];
    float square = 0.0f;
    #pragma unroll
    for (unsigned i = 0; i < 4; ++i) {
        data[i] = __bfloat162float(values[i * 32 + lane]);
        square = __fmaf_rn(data[i], data[i], square);
    }
    const float norm = antfly_head_norm_factor(square, dim, clip, eps);
    const size_t base = (size_t)row * channels + head_base;
    #pragma unroll
    for (unsigned i = 0; i < 4; ++i)
        output[base + i * 32 + lane] = __float2bfloat16_rn(data[i] * norm);
}
