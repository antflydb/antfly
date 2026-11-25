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

// native-program:entry antfly_residual_mix_bf16
struct ResidualMixBf16Params {
    uint rows, width, sources, add_last, normalize;
    float epsilon, norm_epsilon;
};

inline float antfly_residual_mix_reduce(float value, uint tid, threadgroup float *partial) {
    const uint lane = tid % 32u;
    for (uint step = 16; step; step >>= 1) value += simd_shuffle_down(value, step);
    if (lane == 0) partial[tid / 32u] = value;
    threadgroup_barrier(mem_flags::mem_threadgroup);
    value = lane < 4u ? partial[lane] : 0.0f;
    for (uint step = 16; step; step >>= 1) value += simd_shuffle_down(value, step);
    value = simd_broadcast(value, 0);
    threadgroup_barrier(mem_flags::mem_threadgroup);
    return value;
}

kernel void antfly_residual_mix_bf16(
    device const bfloat *blocks [[buffer(0)]],
    device const bfloat *last [[buffer(1)]],
    device const bfloat *add [[buffer(2)]],
    device const float *query [[buffer(3)]],
    device const bfloat *gamma [[buffer(4)]],
    device uchar *output [[buffer(5)]],
    constant ResidualMixBf16Params &p [[buffer(6)]],
    uint row [[threadgroup_position_in_grid]],
    uint tid [[thread_index_in_threadgroup]]
) {
    threadgroup float partial[4], scores[8], weights[8], mixed[4096];
    const ulong base = ulong(row) * p.width;
    const float scale = rsqrt(float(p.width));
    for (uint source = 0; source < p.sources; ++source) {
        const bool final = source + 1u == p.sources;
        const device bfloat *buf = final ? last + base : blocks + ulong(source) * p.rows * p.width + base;
        float dot = 0.0f, square = 0.0f;
        for (uint column = tid; column < p.width; column += 128u) {
            const float x = float(buf[column]) + (final && p.add_last ? float(add[base + column]) : 0.0f);
            dot = fma(x, query[column] * scale, dot);
            square = fma(x, x, square);
        }
        dot = antfly_residual_mix_reduce(dot, tid, partial);
        const float inv = rsqrt(antfly_residual_mix_reduce(square, tid, partial) / float(p.width) + p.epsilon);
        if (tid == 0u) {
            scores[source] = dot * inv;
            weights[source] = inv;
        }
    }
    if (tid == 0u) {
        float maximum = -INFINITY;
        for (uint i = 0; i < p.sources; ++i) maximum = max(maximum, scores[i]);
        float denom = 0.0f;
        for (uint i = 0; i < p.sources; ++i) {
            scores[i] = exp(scores[i] - maximum);
            denom += scores[i];
        }
        for (uint i = 0; i < p.sources; ++i) weights[i] *= scores[i] / denom;
    }
    threadgroup_barrier(mem_flags::mem_threadgroup);
    float square = 0.0f;
    for (uint column = tid; column < p.width; column += 128u) {
        float result = 0.0f;
        for (uint source = 0; source < p.sources; ++source) {
            const bool final = source + 1u == p.sources;
            const ulong index = base + column;
            const float x = final ? float(last[index]) + (p.add_last ? float(add[index]) : 0.0f)
                                  : float(blocks[ulong(source) * p.rows * p.width + index]);
            result = fma(x, weights[source], result);
        }
        if (p.normalize) {
            mixed[column] = result;
            square = fma(result, result, square);
        } else {
            reinterpret_cast<device float *>(output)[base + column] = result;
        }
    }
    if (!p.normalize) return;
    const float norm = rsqrt(antfly_residual_mix_reduce(square, tid, partial) / float(p.width) + p.norm_epsilon);
    for (uint column = tid; column < p.width; column += 128u)
        reinterpret_cast<device bfloat *>(output)[base + column] = bfloat((mixed[column] * norm) * float(gamma[column]));
}
