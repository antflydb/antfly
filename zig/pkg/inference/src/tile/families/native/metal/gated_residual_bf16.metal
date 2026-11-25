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

// native-program:entry antfly_normalize_pair_f32_to_bf16
struct GatedResidualParams { uint width; float epsilon; };

kernel void antfly_normalize_pair_f32_to_bf16(
    device const float *destination [[buffer(0)]],
    device const float *message [[buffer(1)]],
    device bfloat *pair [[buffer(2)]],
    constant GatedResidualParams &p [[buffer(3)]],
    uint row [[threadgroup_position_in_grid]],
    uint tid [[thread_index_in_threadgroup]]
) {
    const ulong base = ulong(row) * p.width;
    threadgroup float partial[4];
    float d = 0.0f, m = 0.0f;
    for (uint i = tid; i < p.width; i += 128u) {
        d = fma(destination[base + i], destination[base + i], d);
        m = fma(message[base + i], message[base + i], m);
    }
    const float dn = rsqrt(antfly_residual_mix_reduce(d, tid, partial) / float(p.width) + p.epsilon);
    const float mn = rsqrt(antfly_residual_mix_reduce(m, tid, partial) / float(p.width) + p.epsilon);
    for (uint i = tid; i < p.width; i += 128u) {
        pair[2u * base + i] = bfloat(destination[base + i] * dn);
        pair[2u * base + p.width + i] = bfloat(message[base + i] * mn);
    }
}

// native-program:entry antfly_gated_residual_rms_f32_to_bf16
inline float antfly_gated_residual(float destination, float message, float gain, float forget) {
    return (1.0f - tanh(forget)) * destination + tanh(gain) * message;
}

kernel void antfly_gated_residual_rms_f32_to_bf16(
    device const float *destination [[buffer(0)]],
    device const float *message [[buffer(1)]],
    device const float *gates [[buffer(2)]],
    device const bfloat *gamma [[buffer(3)]],
    device bfloat *output [[buffer(4)]],
    constant GatedResidualParams &p [[buffer(5)]],
    uint row [[threadgroup_position_in_grid]],
    uint tid [[thread_index_in_threadgroup]]
) {
    const ulong base = ulong(row) * p.width;
    threadgroup float partial[4];
    float square = 0.0f;
    for (uint i = tid; i < p.width; i += 128u) {
        const float value = antfly_gated_residual(destination[base + i], message[base + i],
            gates[2u * base + i], gates[2u * base + p.width + i]);
        square = fma(value, value, square);
    }
    const float norm = rsqrt(antfly_residual_mix_reduce(square, tid, partial) / float(p.width) + p.epsilon);
    for (uint i = tid; i < p.width; i += 128u) {
        const float value = antfly_gated_residual(destination[base + i], message[base + i],
            gates[2u * base + i], gates[2u * base + p.width + i]);
        output[base + i] = bfloat((value * norm) * float(gamma[i]));
    }
}
