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

#include "gliner25_boundary_attention_sm120.cuh"
extern "C" __global__ void boundary_delta32(
    float* delta,
    const float* saved,
    const float* upstream,
    int batch,
    int sequence,
    int heads) {
    int i = blockIdx.x * blockDim.y + threadIdx.y;
    if (i >= batch * sequence * heads) return;
    int q = i % sequence, h = i / sequence % heads, b = i / (sequence * heads);
    int row = (b * sequence + q) * heads * 32 + h * 32 + threadIdx.x;
    float d = boundary_sum32(saved[row] * upstream[row]);
    if (threadIdx.x == 0) delta[i] = d;
}
extern "C" __global__ void boundary_backward(
    float* grad,
    const float* saved,
    const float* upstream,
    const float* lse,
    const float* delta,
    const float* packed,
    const float* bias,
    const float * ,
    int batch,
    int sequence,
    int heads,
    int columns) {
    int t = blockIdx.x * (blockDim.x / 32) + threadIdx.x / 32, h = blockIdx.y, b = blockIdx.z;
    if (t >= sequence || h >= heads || b >= batch) return;
    int ti = boundary_packed_index(b, t, h, sequence, heads), hidden = heads * 32, padded = (sequence + 31) / 32 * 32;
    float qv = packed[ti], dy = upstream[boundary_row_index(b, t, h, sequence, heads)], dq = 0;
    float delta_q = delta[(b * heads + h) * sequence + t], lse_q = lse[(b * heads + h) * padded + t];
    for (int k = 0; k < sequence; ++k) {
        int ki = boundary_packed_index(b, k, h, sequence, heads);
        float score = boundary_sum32(qv * packed[ki + hidden]) * 0.17677669529663687F + bias[(b * sequence + t) * columns + k];
        float probability = expf(score-lse_q), dv = boundary_sum32(dy * packed[ki + 2 * hidden]);
        float ds = probability * (dv-delta_q) * 0.17677669529663687F;
        dq = fmaf(ds, packed[ki + hidden], dq);
    }
    grad[ti] = dq;
// This warp is also the sole owner of the key / value row t. Chronological
// query traversal makes the reduction repeatable without atomics or tiles.
    float key = packed[ti + hidden], value = packed[ti + 2 * hidden], dk = 0, dv = 0;
    for (int q = 0; q < sequence; ++q) {
        int qi = boundary_packed_index(b, q, h, sequence, heads), yi = boundary_row_index(b, q, h, sequence, heads);
        float query = packed[qi], up = upstream[yi];
        float score = boundary_sum32(query * key) * 0.17677669529663687F + bias[(b * sequence + q) * columns + t];
        float probability = expf(score-lse[(b * heads + h) * padded + q]);
        float ds = probability * (boundary_sum32(up * value)-delta[(b * heads + h) * sequence + q]) * 0.17677669529663687F;
        dk = fmaf(ds, query, dk);
        dv = fmaf(probability, up, dv);
    }
    grad[ti + hidden] = dk;
    grad[ti + 2 * hidden] = dv;
}
