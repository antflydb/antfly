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

inline void antfly_dense_bf16_store(
    device uchar *output, device const bfloat *bias,
    constant DenseBf16ProjectionParams &p, uint row, uint column, float dot
) {
    const ulong index = ulong(row) * p.output_width + column;
    const float value = p.has_bias ? dot + float(bias[column]) : dot;
    if (p.f32_output)
        reinterpret_cast<device float *>(output)[index] = p.softcap > 0.0f ? tanh(value / p.softcap) * p.softcap : value;
    else
        reinterpret_cast<device bfloat *>(output)[index] = bfloat(value);
}

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
    if (lane == 0u) antfly_dense_bf16_store(output, bias, p, group.y, column, dot);
}

template<bool complete>
inline void antfly_dense_bf16_projection_four_rows(
    device const bfloat *input, device const bfloat *weight,
    device uchar *output, device const bfloat *bias,
    constant DenseBf16ProjectionParams &p,
    uint2 group, uint warp,
    uint lane
) {
    const uint column = group.x * 4u + warp;
    const uint first = group.y * 4u;
    if (column >= p.output_width) return;
    const uint count = complete ? 4u : min(4u, p.rows - first);
    const device bfloat *a = input + ulong(first) * p.input_width;
    const device bfloat *w = weight + ulong(column) * p.input_width;
    float4 product = 0.0f;
    uint k = lane;
    if (complete || (count == 4u && p.output_width >= 256u)) {
        // Keep each accumulator's FMA order while amortizing the loop
        // branch across four SIMD-width steps.
        const uint full_width = p.input_width & ~127u;
        for (; k < full_width; k += 128u) {
            #pragma unroll
            for (uint step = 0u; step < 4u; ++step) {
                const uint index = k + step * 32u;
                const float b = float(w[index]);
                #pragma unroll
                for (uint i = 0u; i < 4u; ++i)
                    product[i] = fma(float(a[ulong(i) * p.input_width + index]), b, product[i]);
            }
        }
    }
    for (; k < p.input_width; k += 32u) {
        const float b = float(w[k]);
        #pragma unroll
        for (uint i = 0u; i < 4u; ++i)
            if (i < count) product[i] = fma(float(a[ulong(i) * p.input_width + k]), b, product[i]);
    }
    #pragma unroll
    for (uint i = 0u; i < 4u; ++i) {
        if (i < count) {
            const float dot = simd_sum(product[i]);
            if (lane == 0u) antfly_dense_bf16_store(output, bias, p, first + i, column, dot);
        }
    }
}

// native-program:entry antfly_dense_bf16_projection_reuse4
kernel void antfly_dense_bf16_projection_reuse4(
    device const bfloat *input [[buffer(0)]], device const bfloat *weight [[buffer(1)]],
    device uchar *output [[buffer(2)]], device const bfloat *bias [[buffer(3)]],
    constant DenseBf16ProjectionParams &p [[buffer(4)]],
    uint2 group [[threadgroup_position_in_grid]], uint warp [[simdgroup_index_in_threadgroup]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_dense_bf16_projection_four_rows<false>(input, weight, output, bias, p, group, warp, lane);
}

// native-program:entry antfly_dense_bf16_projection_reuse4_full
kernel void antfly_dense_bf16_projection_reuse4_full(
    device const bfloat *input [[buffer(0)]], device const bfloat *weight [[buffer(1)]],
    device uchar *output [[buffer(2)]], device const bfloat *bias [[buffer(3)]],
    constant DenseBf16ProjectionParams &p [[buffer(4)]],
    uint2 group [[threadgroup_position_in_grid]], uint warp [[simdgroup_index_in_threadgroup]],
    uint lane [[thread_index_in_simdgroup]]
) {
    antfly_dense_bf16_projection_four_rows<true>(input, weight, output, bias, p, group, warp, lane);
}
