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

// native-program:entry antfly_head_norm_bf16_simd32
struct HeadNormBf16Params {
    uint rows, heads, dim, position, log_floor, weighted;
    float clip, epsilon, log_alpha;
};

kernel void antfly_head_norm_bf16_simd32(
    device const bfloat *input [[buffer(0)]],
    device const bfloat *gamma [[buffer(1)]],
    device bfloat *output [[buffer(2)]],
    constant HeadNormBf16Params &p [[buffer(3)]],
    uint row_head [[threadgroup_position_in_grid]],
    uint lane [[thread_index_in_simdgroup]]
) {
    const ulong base = ulong(row_head) * p.dim;
    const ulong gamma_base = ulong(row_head % p.heads) * p.dim;
    float4 values = 0.0f;
    float square = 0.0f;
    for (uint i = 0u; i < 4u; ++i) {
        const uint d = i * 32u + lane;
        if (d >= p.dim) continue;
        const float value = float(input[base + d]);
        values[i] = p.weighted ? value * float(gamma[gamma_base + d]) : value;
        square = fma(values[i], values[i], square);
    }
    for (uint delta = 16u; delta != 0u; delta >>= 1u)
        square += simd_shuffle_down(square, delta);
    square = simd_broadcast(square, 0);
    const float factor = p.clip > 0.0f ? min(1.0f, p.clip / (sqrt(square) + p.epsilon))
                                       : rsqrt(square / float(p.dim) + p.epsilon);
    const uint absolute = p.position + row_head / p.heads + 1u;
    const float tau = p.log_floor != 0u && absolute > p.log_floor
        ? 1.0f + p.log_alpha * log(float(absolute) / float(p.log_floor)) : 1.0f;
    for (uint i = 0u; i < 4u; ++i) {
        const uint d = i * 32u + lane;
        if (d >= p.dim) continue;
        const bfloat rounded = bfloat(values[i] * factor);
        output[base + d] = tau == 1.0f ? rounded : bfloat(float(rounded) * tau);
    }
}
