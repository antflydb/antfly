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

const tile = @import("antfly_tile");
const program = @import("../../native_program.zig");
pub const metal = struct {
    pub const source_0 = program.metal(program.render(
        \\static inline float termite_bf16_to_f32(ushort bits) { return as_type<float>((uint(bits) << 16u)); }
        \\inline float termite_dense_weight(float x) { return x; }
        \\inline float termite_dense_weight(half x) { return float(x); }
        \\inline float termite_dense_weight(ushort x) { return termite_bf16_to_f32(x); }
        \\template<typename W> inline void termite_dense_scalar(device const float *input, device const W *weight, device const float *bias, device float *output, uint in_dim, uint out_dim, uint row, uint col) {
        \\    float acc = bias[col]; uint in_base = row * in_dim; uint w_base = col * in_dim;
        \\{{serial}}
        \\    output[row * out_dim + col] = acc;
        \\}
        \\template<typename W> inline void termite_dense_reduce(device const float *input, device const W *weight, device const float *bias, device float *output, threadgroup float *partials, uint in_dim, uint out_dim, uint row, uint col, uint lane) {
        \\    float acc = lane == 0u ? bias[col] : 0.0f; uint in_base = row * in_dim; uint w_base = col * in_dim;
        \\{{parallel}}
        \\{{tree}}
        \\    if (lane == 0u) output[row * out_dim + col] = partials[0];
        \\}
        \\
    , .{
        .serial = (tile.native_projection.Dot{ .integer = "uint", .index = "i", .begin = "0u", .end = "in_dim", .left = "input[in_base + i]", .right = "termite_dense_weight(weight[w_base + i])" }).native(),
        .parallel = (tile.native_projection.Dot{ .integer = "uint", .index = "i", .begin = "lane", .end = "in_dim", .step = "32u", .left = "input[in_base + i]", .right = "termite_dense_weight(weight[w_base + i])" }).native(),
        .tree = tile.native_statistics.Tree.native(.{ .integer = "uint", .lane = "lane", .threads = "32u", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .planes = &.{.{ .partial = "partials", .value = "acc" }} }),
    }));
};

/// One qualified row/column schedule replaces separate scalar-register bodies
/// for 1x8 and 4x4, and the former 4x8/4x16 helper. Export ABIs remain adapters.
pub const metal_matrix = struct {
    pub const source_0 = program.metal(program.render(
        \\template<uint Rows, uint Cols> __attribute__((always_inline)) inline void antfly_dot_rows_cols(
        \\    device const float *lhs, device const float *rhs, device float *output,
        \\    constant termite_metal_dot_general_2d_f32_params &p, threadgroup float *partials,
        \\    ushort tid, uint3 tpg, uint3 tg) {
        \\    uint row0 = tg.y * Rows; uint col0 = tg.x * Cols; uint width = tpg.x; if (row0 >= p.m || col0 >= p.n || p.k == 0u || width == 0u || p.rhs_contract_axis > 1u) return;
        \\    float acc[Rows * Cols]; for (uint i = 0u; i < Rows * Cols; ++i) acc[i] = 0.0f;
        \\    bool valid_r[Rows]; valid_r[0] = true; for (uint r = 1u; r < Rows; ++r) valid_r[r] = row0 + r < p.m;
        \\    bool valid_c[Cols]; valid_c[0] = true; for (uint c = 1u; c < Cols; ++c) valid_c[c] = col0 + c < p.n;
        \\    for (uint kk = uint(tid); kk < p.k; kk += width) {
        \\        float x[Rows]; x[0] = lhs[row0 * p.k + kk]; for (uint r = 1u; r < Rows; ++r) x[r] = valid_r[r] ? lhs[(row0 + r) * p.k + kk] : 0.0f;
        \\        float y[Cols]; uint rhs_base = p.rhs_contract_axis == 0u ? kk * p.n + col0 : col0 * p.k + kk; uint rhs_stride = p.rhs_contract_axis == 0u ? 1u : p.k;
        \\        for (uint c = 0u; c < Cols; ++c) y[c] = valid_c[c] ? rhs[rhs_base + c * rhs_stride] : 0.0f;
        \\        for (uint r = 0u; r < Rows; ++r) {
        \\{{products}}
        \\        }
        \\    }
        \\    uint t = uint(tid);
        \\{{tree}}
        \\    if (tid == 0u) { for (uint r = 0u; r < Rows; ++r) { if (!valid_r[r]) continue; uint base = (row0 + r) * p.n + col0; for (uint c = 0u; c < Cols; ++c) { if (valid_c[c]) output[base + c] = partials[(r * Cols + c) * width]; } } }
        \\}
        \\
    , .{
        .products = (tile.native_projection.Dot{ .integer = "uint", .index = "c", .begin = "0u", .end = "Cols", .left = "x[r]", .right = "y[c]", .accumulator = "acc[r * Cols + c]", .indent = "            " }).native(),
        .tree = tile.native_statistics.Tree.native(.{ .integer = "uint", .lane = "t", .threads = "width", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .axes = &.{.{ .name = "i", .extent = "Rows * Cols", .unroll = false }}, .planes = &.{.{ .partial = "partials", .offset = "i * width", .value = "acc[i]" }} }),
    }));
};
