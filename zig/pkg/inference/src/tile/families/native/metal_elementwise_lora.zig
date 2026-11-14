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

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

pub const source_0 =
    program.metal(
        \\kernel void termite_lora_after_a_f32(device const float *input [[buffer(0)]], device const float *lora_a [[buffer(1)]], device float *after_a [[buffer(2)]], constant termite_metal_lora_linear_f32_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
        \\    uint total = p.rows * p.rank; if (gid >= total || p.in_dim == 0u) return; uint row = gid / p.rank; uint rk = gid - row * p.rank; float acc = 0.0f;
        \\    uint in_base = row * p.in_dim; uint a_base = rk * p.in_dim;
        \\    for (uint i = 0u; i < p.in_dim; ++i) acc += input[in_base + i] * lora_a[a_base + i];
        \\    after_a[gid] = acc;
        \\}
        \\kernel void termite_lora_finish_f32(device const float *base [[buffer(0)]], device const float *after_a [[buffer(1)]], device const float *lora_b [[buffer(2)]], device float *after_b [[buffer(3)]], device float *output [[buffer(4)]], constant termite_metal_lora_linear_f32_params &p [[buffer(5)]], uint gid [[thread_position_in_grid]]) {
        \\    uint total = p.rows * p.out_dim; if (gid >= total || p.rank == 0u) return; uint row = gid / p.out_dim; uint out = gid - row * p.out_dim; float acc = 0.0f;
        \\    uint a_base = row * p.rank; uint b_base = out * p.rank;
        \\    for (uint rk = 0u; rk < p.rank; ++rk) acc += after_a[a_base + rk] * lora_b[b_base + rk];
        \\    after_b[gid] = acc; output[gid] = base[gid] + acc * p.scale;
        \\}
        \\
    );

pub const source_1 =
    program.metal(
        \\kernel void termite_lora_grad_b_f32(device const float *output_grad [[buffer(0)]], device const float *after_a [[buffer(1)]], device float *grad_b [[buffer(2)]], constant termite_metal_lora_linear_f32_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
        \\    uint total = p.out_dim * p.rank; if (gid >= total || p.rows == 0u) return; uint out = gid / p.rank; uint rk = gid - out * p.rank; float acc = 0.0f;
        \\    for (uint row = 0u; row < p.rows; ++row) acc += output_grad[row * p.out_dim + out] * after_a[row * p.rank + rk];
        \\    grad_b[gid] = acc * p.scale;
        \\}
        \\kernel void termite_lora_grad_b_transposed_f32(device const float *output_grad [[buffer(0)]], device const float *after_a [[buffer(1)]], device float *grad_b_t [[buffer(2)]], constant termite_metal_lora_linear_f32_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
        \\    uint total = p.rank * p.out_dim; if (gid >= total || p.rows == 0u) return; uint rk = gid / p.out_dim; uint out = gid - rk * p.out_dim; float acc = 0.0f;
        \\    for (uint row = 0u; row < p.rows; ++row) acc += after_a[row * p.rank + rk] * output_grad[row * p.out_dim + out];
        \\    grad_b_t[gid] = acc * p.scale;
        \\}
        \\kernel void termite_lora_grad_after_a_f32(device const float *output_grad [[buffer(0)]], device const float *lora_b [[buffer(1)]], device float *grad_after_a [[buffer(2)]], constant termite_metal_lora_linear_f32_params &p [[buffer(3)]], threadgroup float *partials [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {
        \\    uint total = p.rows * p.rank; if (group >= total || p.out_dim == 0u) return; uint row = group / p.rank; uint rk = group - row * p.rank; float acc = 0.0f;
        \\    for (uint out = uint(sgitg) * 32u + uint(lane); out < p.out_dim; out += 256u) acc += output_grad[row * p.out_dim + out] * lora_b[out * p.rank + rk];
        \\    if (sgitg == 0u) partials[lane] = 0.0f; acc = simd_sum(acc); threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    if (lane == 0u) partials[sgitg] = acc; threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    float total_acc = simd_sum(partials[lane]); if (lane == 0u && sgitg == 0u) grad_after_a[group] = total_acc * p.scale;
        \\}
        \\kernel void termite_lora_grad_a_f32(device const float *grad_after_a [[buffer(0)]], device const float *input [[buffer(1)]], device float *grad_a [[buffer(2)]], constant termite_metal_lora_linear_f32_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
        \\    uint total = p.rank * p.in_dim; if (gid >= total || p.rows == 0u) return; uint rk = gid / p.in_dim; uint in_col = gid - rk * p.in_dim; float acc = 0.0f;
        \\    for (uint row = 0u; row < p.rows; ++row) acc += grad_after_a[row * p.rank + rk] * input[row * p.in_dim + in_col];
        \\    grad_a[gid] = acc;
        \\}
        \\kernel void termite_lora_rank1_grad_b_after_a_f32(device const float *output_grad [[buffer(0)]], device const float *after_a [[buffer(1)]], device const float *lora_b [[buffer(2)]], device float *grad_b [[buffer(3)]], device float *grad_after_a [[buffer(4)]], constant termite_metal_lora_linear_f32_params &p [[buffer(5)]], threadgroup float *partials [[threadgroup(0)]], ushort lane [[thread_index_in_simdgroup]], ushort sgitg [[simdgroup_index_in_threadgroup]], uint group [[threadgroup_position_in_grid]]) {
        \\    if (p.rank != 1u || p.rows == 0u || p.out_dim == 0u) return; uint total_grad_b = p.out_dim; uint total = total_grad_b + p.rows; if (group >= total) return;
        \\    if (group < total_grad_b) { if (lane == 0u && sgitg == 0u) { uint out = group; float acc = 0.0f; for (uint row = 0u; row < p.rows; ++row) acc += output_grad[row * p.out_dim + out] * after_a[row]; grad_b[out] = acc * p.scale; } return; }
        \\    uint row = group - total_grad_b; float acc = 0.0f; for (uint out = uint(sgitg) * 32u + uint(lane); out < p.out_dim; out += 256u) acc += output_grad[row * p.out_dim + out] * lora_b[out];
        \\    if (sgitg == 0u) partials[lane] = 0.0f; acc = simd_sum(acc); threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    if (lane == 0u) partials[sgitg] = acc; threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    float total_acc = simd_sum(partials[lane]); if (lane == 0u && sgitg == 0u) grad_after_a[row] = total_acc * p.scale;
        \\}
        \\
    );
