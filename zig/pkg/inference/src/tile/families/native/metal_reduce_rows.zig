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
const Tree = @import("antfly_tile").native_statistics.Tree;
fn tree(comptime value: []const u8, comptime op: Tree.Operation, comptime select: ?[]const u8) []const u8 {
    return Tree.native(.{ .integer = "uint", .lane = "tid", .threads = "threads", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .operation = op, .maximum_when = select, .planes = &.{.{ .partial = "shmem", .value = value }} });
}
const phases = .{ .maximum = tree("local_max", .maximum, null), .sum = tree("local_sum", .sum, null), .reduce = tree("local", .sum, "p.kind == 1u"), .product = tree("local", .sum, null) };

pub const source_0 =
    program.metal(program.render(
        \\kernel void termite_apply_softmax_rows(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_apply_softmax_params &p [[buffer(2)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
        \\    if (row >= p.rows || p.dim == 0u) return;
        \\    const uint threads = 256u;
        \\    const uint base = row * p.dim;
        \\    float local_max = -3.4028234663852886e+38f;
        \\    for (uint col = tid; col < p.dim; col += threads) local_max = max(local_max, input[base + col]);
        \\{{maximum}}
        \\    const float row_max = shmem[0]; threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    float local_sum = 0.0f;
        \\    for (uint col = tid; col < p.dim; col += threads) local_sum += exp(input[base + col] - row_max);
        \\{{sum}}
        \\    const float row_sum = max(shmem[0], 1.0e-30f);
        \\    const float log_sum = log(row_sum);
        \\    for (uint col = tid; col < p.dim; col += threads) {
        \\        const float shifted = input[base + col] - row_max;
        \\        output[base + col] = p.log_softmax != 0u ? shifted - log_sum : exp(shifted) / row_sum;
        \\    }
        \\}
        \\
    , phases));

pub const source_1 =
    program.metal(program.render(
        \\kernel void termite_reduce_last_dim_rows(device const float *input [[buffer(0)]], device float *output [[buffer(1)]], constant termite_metal_reduce_last_dim_params &p [[buffer(2)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
        \\    if (row >= p.rows || p.dim == 0u) return;
        \\    const uint threads = 256u;
        \\    const uint base = row * p.dim;
        \\    float local = p.kind == 1u ? -3.4028234663852886e+38f : 0.0f;
        \\    if (p.kind == 1u) {
        \\        for (uint col = tid; col < p.dim; col += threads) local = max(local, input[base + col]);
        \\    } else {
        \\        for (uint col = tid; col < p.dim; col += threads) local += input[base + col];
        \\    }
        \\{{reduce}}
        \\    if (tid == 0u) output[row] = p.kind == 2u ? shmem[0] / float(p.dim) : shmem[0];
        \\}
        \\
    , phases));

pub const source_2 =
    program.metal(program.render(
        \\kernel void termite_multiply_reduce_last_dim_rows(device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_reduce_last_dim_params &p [[buffer(3)]], threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {
        \\    if (row >= p.rows || p.dim == 0u) return;
        \\    const uint threads = 256u;
        \\    const uint base = row * p.dim;
        \\    float local = 0.0f;
        \\    for (uint col = tid; col < p.dim; col += threads) local += lhs[base + col] * rhs[base + col];
        \\{{product}}
        \\    if (tid == 0u) output[row] = shmem[0];
        \\}
        \\
    , phases));
