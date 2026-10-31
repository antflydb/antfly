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

#include <math.h>
#include <stdint.h>

// Caller-owned, non-overlapping F32 outputs. Coordinates are axis-major
// [3, rows], matching the native M-RoPE operation. No KV/state access occurs.
struct AttentionInputs {
    const float* query_gate;
    const float* key;
    const float* query_weight;
    const float* key_weight;
    const unsigned* positions;
    float* query_out;
    float* key_out;
    float* gate_out;
    unsigned rows, query_heads, key_heads, dim, rotary_dim;
    float theta, frequency_scale, epsilon;
    unsigned section_height, section_width;
};
static_assert(sizeof(AttentionInputs) == 104, "attention input ABI");

extern "C" __global__ void lighton_qk_norm_partial_mrope_gate(
    const __grid_constant__ AttentionInputs p) {
    const unsigned row = blockIdx.y, id = blockIdx.x, d = threadIdx.x;
    if (row >= p.rows || id >= p.query_heads + p.key_heads || p.dim != 256 ||
        blockDim.x != 256 || p.rotary_dim == 0 || p.rotary_dim > p.dim ||
        (p.rotary_dim & 1)) return;
    const bool query = id < p.query_heads;
    const unsigned head = query ? id : id - p.query_heads;
    const unsigned heads = query ? p.query_heads : p.key_heads;
    const unsigned output_base = (row * heads + head) * p.dim;
    const unsigned input_base = query ? (row * heads + head) * p.dim * 2 : output_base;
    const float x = query ? p.query_gate[input_base + d] : p.key[input_base + d];
    const float* weight = query ? p.query_weight : p.key_weight;
    __shared__ float partial[256];
    __shared__ float normalized[256];
    // Same complete-head reduction tree and F32 rounding boundary as the
    // production RMS kernel. Keep the rounded normalization for the tail too.
    partial[d] = x * x;
    __syncthreads();
    for (unsigned stride = 128; stride; stride >>= 1) {
        if (d < stride) partial[d] += partial[d + stride];
        __syncthreads();
    }
    const float inverse = rsqrtf(partial[0] / (float)p.dim + p.epsilon);
    const float value = (x * inverse) * weight[d];
    normalized[d] = value;
    __syncthreads();
    float output = value;
    if (d < p.rotary_dim) {
        const unsigned half = p.rotary_dim / 2;
        const unsigned pair = d < half ? d : d - half;
        unsigned axis = 0;
        if (pair % 3 == 1 && pair < p.section_height * 3) axis = 1;
        else if (pair % 3 == 2 && pair < p.section_width * 3) axis = 2;
        const unsigned position = p.positions[axis * p.rows + row];
        const float exponent = (2.f * (float)pair) / (float)p.rotary_dim;
        const float frequency = 1.f / powf(p.theta, exponent);
        const float angle = ((float)position * p.frequency_scale) * frequency;
        const float s = sinf(angle), c = cosf(angle);
        const float left = normalized[pair], right = normalized[half + pair];
        output = d < half ? left * c - right * s : left * s + right * c;
    }
    if (query) {
        p.query_out[output_base + d] = output;
        const float gate = p.query_gate[input_base + p.dim + d];
        p.gate_out[output_base + d] = 1.f / (1.f + expf(-gate));
    } else p.key_out[output_base + d] = output;
}
