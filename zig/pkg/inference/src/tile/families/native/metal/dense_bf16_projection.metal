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

// native-program:entry antfly_dense_bf16_projection_simd4
struct DenseBf16ProjectionParams {
    uint rows, input_width, output_width, f32_output, has_bias;
    float softcap;
};

kernel void antfly_dense_bf16_projection_simd4(
    device const bfloat *input [[buffer(0)]],
    device const bfloat *weight [[buffer(1)]],
    device uchar *output [[buffer(2)]],
    device const bfloat *bias [[buffer(3)]],
    constant DenseBf16ProjectionParams &p [[buffer(4)]],
    uint2 group [[threadgroup_position_in_grid]],
    uint simd_group [[simdgroup_index_in_threadgroup]],
    uint lane [[thread_index_in_simdgroup]]
) {
    const uint column = group.x * 4u + simd_group;
    if (column >= p.output_width) return;
    const ulong a = ulong(group.y) * p.input_width;
    const ulong b = ulong(column) * p.input_width;
    float product = 0.0f;
    for (uint k = lane; k < p.input_width; k += 32u)
        product = fma(float(input[a + k]), float(weight[b + k]), product);
    const float dot = simd_sum(product);
    if (lane == 0u) {
        const float total = p.has_bias ? dot + float(bias[column]) : dot;
        const ulong index = ulong(group.y) * p.output_width + column;
        if (p.f32_output) reinterpret_cast<device float *>(output)[index] = p.softcap > 0.0f ? tanh(total / p.softcap) * p.softcap : total;
        else reinterpret_cast<device bfloat *>(output)[index] = bfloat(total);
    }
}
