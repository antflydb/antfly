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

// native-program:entry antfly_spatial_pool_gemm_f32_bf16_phases
// Physical geometry belongs to the source GEMM, independently of selected rows.
extern "C" __global__ void antfly_spatial_pool_gemm_f32_bf16_phases(
    float* output, const float* input, const float* bias, const float* scale,
    unsigned physical, unsigned pooled, unsigned gx, unsigned gy,
    unsigned merge, unsigned dim, unsigned phase) {
    size_t i = (size_t)blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned logical = gx * gy;
    const unsigned selected = logical / (merge * merge);
    if (phase == 0u) {
        if (i >= (size_t)pooled * physical) return;
        const unsigned token = i / pooled, row = i % pooled;
        // Padding positions clamp to zero in the source one-hot mapping.
        const unsigned x = token < logical ? token % gx : 0u;
        const unsigned y = token < logical ? token / gx : 0u;
        const unsigned group = x / merge + (gx / merge) * (y / merge);
        output[i] = row == group ? __fdiv_rn(1.f, (float)(merge * merge)) : 0.f;
    } else if (phase == 1u) {
        if (i >= (size_t)physical * dim) return;
        // Source masks padded hidden states immediately before the GEMM.
        output[i] = i / dim < logical ? input[i] : 0.f;
    } else {
        if (i >= (size_t)selected * dim) return;
        const unsigned channel = i % dim;
        const float mean = __bfloat162float(__float2bfloat16_rn(input[i]));
        float value = __fmul_rn(mean, sqrtf((float)dim));
        if (bias) value = __fmul_rn(__fsub_rn(value, bias[channel]), scale[channel]);
        output[i] = __bfloat162float(__float2bfloat16_rn(value));
    }
}
