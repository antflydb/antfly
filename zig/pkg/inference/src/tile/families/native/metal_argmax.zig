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
        \\kernel void termite_argmax_logits_1x(device const float *logits [[buffer(0)]], device uint *output [[buffer(1)]], constant uint &out_dim [[buffer(2)]], uint gid [[thread_position_in_grid]]) {
        \\    if (gid != 0) return;
        \\    if (out_dim == 0u) { output[0] = 0u; return; }
        \\    float best = -INFINITY;
        \\    uint best_idx = 0u;
        \\    for (uint i = 0u; i < out_dim; ++i) {
        \\        float value = logits[i];
        \\        if (value > best) { best = value; best_idx = i; }
        \\    }
        \\    output[0] = best_idx;
        \\}
        \\
    );

pub const source_1 =
    program.metal(
        \\kernel void termite_argmax_logits_partials(device const float *logits [[buffer(0)]], device float *partial_values [[buffer(1)]], device uint *partial_ids [[buffer(2)]], constant uint &out_dim [[buffer(3)]], threadgroup float *sh_vals [[threadgroup(0)]], threadgroup uint *sh_ids [[threadgroup(1)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]], uint block [[threadgroup_position_in_grid]]) {
        \\    const uint block_size = 1024u;
        \\    uint start = block * block_size;
        \\    uint end = min(start + block_size, out_dim);
        \\    float best = -INFINITY;
        \\    uint best_idx = 0u;
        \\    for (uint i = start + uint(tid); i < end; i += uint(tg_size)) {
        \\        float value = logits[i];
        \\        if (value > best || (value == best && i < best_idx)) { best = value; best_idx = i; }
        \\    }
        \\    sh_vals[tid] = best;
        \\    sh_ids[tid] = best_idx;
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    termite_argmax_partial_tree(sh_vals, sh_ids, tid, tg_size);
        \\    if (tid == 0u) { partial_values[block] = sh_vals[0]; partial_ids[block] = sh_ids[0]; }
        \\}
        \\
    );

pub const source_2 =
    program.metal(
        \\kernel void termite_argmax_logits_suppress_partials(device const float *logits [[buffer(0)]], device const int *suppress_ids [[buffer(1)]], device float *partial_values [[buffer(2)]], device uint *partial_ids [[buffer(3)]], constant termite_metal_argmax_suppress_params &p [[buffer(4)]], threadgroup float *sh_vals [[threadgroup(0)]], threadgroup uint *sh_ids [[threadgroup(1)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]], uint block [[threadgroup_position_in_grid]]) {
        \\    const uint block_size = 1024u;
        \\    uint start = block * block_size;
        \\    uint end = min(start + block_size, p.out_dim);
        \\    float best = -INFINITY;
        \\    uint best_idx = 0xffffffffu;
        \\    for (uint i = start + uint(tid); i < end; i += uint(tg_size)) {
        \\        if (termite_argmax_suppressed_token(i, suppress_ids, p.suppress_count)) continue;
        \\        float value = logits[i];
        \\        if (value > best || (value == best && i < best_idx)) { best = value; best_idx = i; }
        \\    }
        \\    if (best_idx == 0xffffffffu) best_idx = 0u;
        \\    sh_vals[tid] = best;
        \\    sh_ids[tid] = best_idx;
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    termite_argmax_partial_tree(sh_vals, sh_ids, tid, tg_size);
        \\    if (tid == 0u) { partial_values[block] = sh_vals[0]; partial_ids[block] = sh_ids[0]; }
        \\}
        \\kernel void termite_argmax_logits_reduce(device const float *partial_values [[buffer(0)]], device const uint *partial_ids [[buffer(1)]], device uint *output [[buffer(2)]], constant uint &partial_count [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
        \\    if (gid != 0u) return;
        \\    if (partial_count == 0u) { output[0] = 0u; return; }
        \\    float best = partial_values[0];
        \\    uint best_idx = partial_ids[0];
        \\    for (uint i = 1u; i < partial_count; ++i) {
        \\        float value = partial_values[i];
        \\        uint idx = partial_ids[i];
        \\        if (value > best || (value == best && idx < best_idx)) { best = value; best_idx = idx; }
        \\    }
        \\    output[0] = best_idx;
        \\}
        \\kernel void termite_lighton_batch_argmax_partials(device const float *logits [[buffer(0)]], device const int *suppress_ids [[buffer(1)]], device float *partial_values [[buffer(2)]], device uint *partial_ids [[buffer(3)]], constant termite_metal_argmax_suppress_params &p [[buffer(4)]], threadgroup float *sh_vals [[threadgroup(0)]], threadgroup uint *sh_ids [[threadgroup(1)]], ushort tid [[thread_index_in_threadgroup]], ushort3 tg_shape [[threads_per_threadgroup]], uint3 group [[threadgroup_position_in_grid]]) {
        \\    const ushort tg_size = tg_shape.x;
        \\    const uint block = group.x;
        \\    logits += size_t(group.y) * p.out_dim;
        \\    partial_values += size_t(group.y) * ((p.out_dim + 1023u) / 1024u);
        \\    partial_ids += size_t(group.y) * ((p.out_dim + 1023u) / 1024u);
        \\    const uint block_size = 1024u;
        \\    uint start = block * block_size;
        \\    uint end = min(start + block_size, p.out_dim);
        \\    float best = -INFINITY;
        \\    uint best_idx = 0xffffffffu;
        \\    for (uint i = start + uint(tid); i < end; i += uint(tg_size)) {
        \\        if (termite_argmax_suppressed_token(i, suppress_ids, p.suppress_count)) continue;
        \\        float value = logits[i];
        \\        if (value > best || (value == best && i < best_idx)) { best = value; best_idx = i; }
        \\    }
        \\    if (best_idx == 0xffffffffu) best_idx = 0u;
        \\    sh_vals[tid] = best;
        \\    sh_ids[tid] = best_idx;
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    for (uint stride = uint(tg_size) >> 1u; stride > 0u; stride >>= 1u) {
        \\        if (uint(tid) < stride) {
        \\            float other = sh_vals[uint(tid) + stride];
        \\            uint other_idx = sh_ids[uint(tid) + stride];
        \\            if (other > sh_vals[tid] || (other == sh_vals[tid] && other_idx < sh_ids[tid])) { sh_vals[tid] = other; sh_ids[tid] = other_idx; }
        \\        }
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    }
        \\    if (tid == 0u) { partial_values[block] = sh_vals[0]; partial_ids[block] = sh_ids[0]; }
        \\}
        \\kernel void termite_lighton_batch_argmax_reduce(device const float *partial_values [[buffer(0)]], device const uint *partial_ids [[buffer(1)]], device uint *output [[buffer(2)]], constant uint &partial_count [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
        \\    output += gid;
        \\    partial_values += size_t(gid) * partial_count;
        \\    partial_ids += size_t(gid) * partial_count;
        \\    if (partial_count == 0u) { output[0] = 0u; return; }
        \\    float best = partial_values[0];
        \\    uint best_idx = partial_ids[0];
        \\    for (uint i = 1u; i < partial_count; ++i) {
        \\        float value = partial_values[i];
        \\        uint idx = partial_ids[i];
        \\        if (value > best || (value == best && idx < best_idx)) { best = value; best_idx = idx; }
        \\    }
        \\    output[0] = best_idx;
        \\}
        \\
    );

pub const source_3 =
    program.metal(
        \\kernel void termite_argmax_logits_reduce_tg(device const float *partial_values [[buffer(0)]], device const uint *partial_ids [[buffer(1)]], device uint *output [[buffer(2)]], constant uint &partial_count [[buffer(3)]], threadgroup float *sh_vals [[threadgroup(0)]], threadgroup uint *sh_ids [[threadgroup(1)]], ushort tid [[thread_index_in_threadgroup]], ushort tg_size [[threads_per_threadgroup]]) {
        \\    float best = -INFINITY; uint best_idx = 0xffffffffu;
        \\    for (uint i = uint(tid); i < partial_count; i += uint(tg_size)) { float value = partial_values[i]; uint idx = partial_ids[i]; if (value > best || (value == best && idx < best_idx)) { best = value; best_idx = idx; } }
        \\    sh_vals[tid] = best; sh_ids[tid] = best_idx; threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    termite_argmax_partial_tree(sh_vals, sh_ids, tid, tg_size);
        \\    if (tid == 0u) { if (partial_count == 0u) output[0] = 0u; else if (isnan(partial_values[0])) output[0] = partial_ids[0]; else output[0] = sh_ids[0] == 0xffffffffu ? 0u : sh_ids[0]; }
        \\}
        \\
    );
