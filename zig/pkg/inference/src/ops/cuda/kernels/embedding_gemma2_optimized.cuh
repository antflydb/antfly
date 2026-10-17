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

#pragma once
#include <cuda_bf16.h>

// Split a packed QKV or gate/up GEMM without changing BF16 values.
extern "C" __global__ void termite_embedding_gemma2_split_columns_bf16(
    __nv_bfloat16* a, __nv_bfloat16* b, __nv_bfloat16* c,
    const __nv_bfloat16* input, unsigned rows, unsigned wa, unsigned wb, unsigned wc) {
    unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned width = wa + wb + wc;
    if (i >= rows * width) return;
    unsigned row = i / width, col = i % width;
    if (col < wa) a[(size_t)row * wa + col] = input[i];
    else if (col < wa + wb) b[(size_t)row * wb + col - wa] = input[i];
    else c[(size_t)row * wc + col - wa - wb] = input[i];
}

// Match the F32 rotary and Q scaling boundaries, rounding once into the BF16
// SDPA inputs. Positions retain the existing two-axis half-head layout.
extern "C" __global__ void termite_embedding_gemma2_vision_rope_pack_bf16(
    __nv_bfloat16* qo, __nv_bfloat16* ko, const __nv_bfloat16* q,
    const __nv_bfloat16* k, const unsigned* positions, unsigned count, float theta, float scale) {
    unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= count) return;
    unsigned d = i % 32, pair = d % 16, base = i - d;
    float angle = (float)positions[i / 32] * (1.0f / powf(theta, (2.0f * pair) / 32.0f));
    float s = sinf(angle), c = cosf(angle);
    float ql = __bfloat162float(q[base + pair]), qr = __bfloat162float(q[base + 16 + pair]);
    float kl = __bfloat162float(k[base + pair]), kr = __bfloat162float(k[base + 16 + pair]);
    float qv = d < 16 ? ql * c - qr * s : ql * s + qr * c;
    float kv = d < 16 ? kl * c - kr * s : kl * s + kr * c;
    qo[i] = __float2bfloat16_rn(qv * scale);
    ko[i] = __float2bfloat16_rn(kv);
}
