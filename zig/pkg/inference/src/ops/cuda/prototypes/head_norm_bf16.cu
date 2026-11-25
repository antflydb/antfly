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
#include <stdint.h>
#include <math.h>

#include "../kernels/row_program.cuh"

namespace {

struct NormPlane {
    __nv_bfloat16* output;
    const __nv_bfloat16* input;
    const __nv_bfloat16* gamma;
    uint32_t dim;
    float clip;
};

struct NormPair {
    NormPlane planes[2];
};
static_assert(sizeof(NormPair) == 64, "head normalization pair ABI");

__device__ __forceinline__ void normalize_heads(
    __nv_bfloat16* output, const __nv_bfloat16* input, const __nv_bfloat16* gamma,
    uint32_t heads, uint32_t dim, uint32_t position, uint32_t log_floor,
    float clip, float eps, float log_alpha
) {
    const unsigned row = blockIdx.x;
    const unsigned head = row % heads;
    const unsigned lane = threadIdx.x;
    const size_t base = (size_t)row * dim;
    float values[4] = {};
    float square = 0.0f;
    #pragma unroll
    for (unsigned i = 0; i < 4; ++i) {
        const unsigned d = i * 32u + lane;
        if (d < dim) {
            const float value = __bfloat162float(input[base + d]);
            values[i] = gamma ? value * __bfloat162float(gamma[head * dim + d]) : value;
            square = __fmaf_rn(values[i], values[i], square);
        }
    }
    const float norm = antfly_head_norm_factor(square, dim, clip, eps);
    const unsigned absolute = position + row / heads + 1;
    const float tau = log_floor != 0 && absolute > log_floor
        ? 1.0f + log_alpha * logf((float)absolute / (float)log_floor) : 1.0f;
    #pragma unroll
    for (unsigned i = 0; i < 4; ++i) {
        const unsigned d = i * 32u + lane;
        if (d < dim) {
            const __nv_bfloat16 reduced = __float2bfloat16_rn(values[i] * norm);
            output[base + d] = tau == 1.0f ? reduced : __float2bfloat16_rn(__bfloat162float(reduced) * tau);
        }
    }
}

} // namespace

extern "C" __global__ void antfly_head_scaled_norm_bf16(
    __nv_bfloat16* output, const __nv_bfloat16* input, const __nv_bfloat16* gamma,
    uint32_t heads, uint32_t dim, uint32_t position, uint32_t log_floor,
    float clip, float eps, float log_alpha
) {
    normalize_heads(output, input, gamma, heads, dim, position, log_floor, clip, eps, log_alpha);
}

extern "C" __global__ void antfly_head_scaled_norm_pair_bf16(
    const __grid_constant__ NormPair pair, uint32_t heads, uint32_t position,
    uint32_t log_floor, float eps, float log_alpha
) {
    const NormPlane& plane = pair.planes[blockIdx.y];
    normalize_heads(plane.output, plane.input, plane.gamma, heads, plane.dim, position, log_floor,
                    plane.clip, eps, log_alpha);
}
