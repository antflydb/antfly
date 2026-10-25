// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http: /  / www.apache.org / licenses / LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "gliner25_boundary_attention_sm120.cuh"
extern "C" __global__ void boundary_bias(
    float* bias,
    const float* valid,
    int batch,
    int sequence,
    int columns,
    int window) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= batch * sequence * columns) return;
    int k = i % columns, q = (i / columns) % sequence, b = i / (columns * sequence);
    bool allowed = k < sequence && (q == k || (valid[b * sequence + k] != 0.0f && (window == 0 || abs(q-k) <= window)));
    bias[i] = allowed?0.0f:-__int_as_float(0x7f800000);
}
extern "C" __global__ void boundary_zero(float* out, int elements) {
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < elements) out[i] = 0;
}
extern "C" __global__ void boundary_forward(
    float* output,
    float* lse,
    const float* packed,
    const float* bias,
    int batch,
    int sequence,
    int heads,
    int columns) {
    int q = blockIdx.x * (blockDim.x / 32) + threadIdx.x / 32, h = blockIdx.y, b = blockIdx.z;
    if (q >= sequence || h >= heads || b >= batch) return;
    int qi = boundary_packed_index(b, q, h, sequence, heads), hidden = heads * 32;
    float qv = packed[qi], maximum = -__int_as_float(0x7f800000);
    for (int k = 0; k < sequence; ++k) {
        float score = boundary_sum32(qv * packed[boundary_packed_index(b, k, h, sequence, heads) + hidden]) * 0.17677669529663687F + bias[(b * sequence + q) * columns + k];
        maximum = fmaxf(maximum, score);
    }
    float denominator = 0, acc = 0;
    for (int k = 0; k < sequence; ++k) {
        int ki = boundary_packed_index(b, k, h, sequence, heads);
        float score = boundary_sum32(qv * packed[ki + hidden]) * 0.17677669529663687F + bias[(b * sequence + q) * columns + k];
        float probability = expf(score-maximum);
        denominator += probability;
        acc = fmaf(probability, packed[ki + 2 * hidden], acc);
    }
    output[boundary_row_index(b, q, h, sequence, heads)] = acc / denominator;
    if ((threadIdx.x & 31) == 0) lse[(b * heads + h) * ((sequence + 31) / 32 * 32) + q] = logf(denominator) + maximum;
}
