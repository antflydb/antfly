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

#include <metal_stdlib>
using namespace metal;
struct Params { uint op, n, dim, groups, position, stride, mode, index; float epsilon, scale, theta, reserved; };
// Device-only copies, f32->f16 appends, and stratified real-query capture.
// No helper that maps shared storage for memcpy is used.
kernel void context_storage(device const uint *src0 [[buffer(0)]],
    device const uint *src1 [[buffer(1)]], device uint *dst0 [[buffer(2)]],
    device uint *dst1 [[buffer(3)]], device uint *unused4 [[buffer(4)]],
    device uint *unused5 [[buffer(5)]], device uint *unused6 [[buffer(6)]],
    device uint *unused7 [[buffer(7)]], constant Params &p [[buffer(8)]],
    uint tid [[thread_position_in_grid]]) {
    if (tid >= p.n) return;
    if (p.op == 0) { dst0[tid] = src0[tid]; return; }
    if (p.op == 1) {
        const uint row = tid / p.dim, d = tid % p.dim;
        const uint logical = p.position + row;
        const uint physical = logical < p.index ? logical : p.index + (logical - p.index) % p.stride;
        ((device half *)dst0)[physical * p.dim + d] = half(((device const float *)src0)[tid]);
        ((device half *)dst1)[physical * p.dim + d] = half(((device const float *)src1)[tid]);
        return;
    }
    if (p.op == 3 || p.op == 4) {
        // Gather one head from interleaved/packed exact KV, or append f32
        // projections into a packed generation's unpublished tail capacity.
        const uint row = tid / p.dim, col = tid % p.dim;
        const uint source = ((p.position + row) * p.groups + p.index) * p.dim + col;
        const uint dest = (p.stride + row) * p.dim + col;
        if (p.op == 3) {
            ((device half *)dst0)[dest] = ((device const half *)src0)[source];
            ((device half *)dst1)[dest] = ((device const half *)src1)[source];
        } else {
            ((device half *)dst0)[dest] = half(((device const float *)src0)[source]);
            ((device half *)dst1)[dest] = half(((device const float *)src1)[source]);
        }
        return;
    }
    if (p.op == 2) {
        const uint vector = tid / p.dim, d = tid % p.dim;
        const uint source = src1[vector];
        if (source != 0xffffffffu)
            ((device float *)dst0)[tid] = ((device const float *)src0)[source * p.dim + d];
    }
}
