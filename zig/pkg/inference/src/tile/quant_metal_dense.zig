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

//! Dense linear with bias, `termite_apply_linear[_<dtype>]_{1x,1x_reduce,
//! multi_row,multi_row_reduce}`: one thread per output, or one 32-thread
//! threadgroup per output with a shared-memory tree. The weight dtype
//! (f32, f16, bf16) only changes the element type and its load.

const msl = @import("quant_metal_msl.zig");
const fill = msl.fill;

/// `inline_tree`: the multi_row_reduce kernel writes its tree loop on one line.
pub const Dtype = struct { tag: []const u8, ty: []const u8, load: []const u8, inline_tree: bool = false };

pub const f32_ = Dtype{ .tag = "", .ty = "float", .load = "${x}" };
pub const f16_ = Dtype{ .tag = "_f16", .ty = "half", .load = "float(${x})", .inline_tree = true };
pub const bf16_ = Dtype{ .tag = "_bf16", .ty = "ushort", .load = "termite_bf16_to_f32(${x})" };

fn args(comptime t: Dtype, comptime params: []const u8) [5][]const u8 {
    return .{ "device const float *input", "device const " ++ t.ty ++ " *weight", "device const float *bias", "device float *output", "constant " ++ params ++ " &p" };
}

const tree =
    \\    partials[lane] = acc;
    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    for (uint stride = 16u; stride > 0u; stride >>= 1u) {
    \\        if (lane < stride) partials[lane] += partials[lane + stride];
    \\        threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    }
    \\
;

/// The same tree with its loop on one line.
const tree_inline =
    \\    partials[lane] = acc;
    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
    \\    for (uint stride = 16u; stride > 0u; stride >>= 1u) { if (lane < stride) partials[lane] += partials[lane + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); }
    \\
;

/// One input row, thread per output.
pub fn single(comptime t: Dtype) msl.Item {
    return msl.msl(msl.signature("termite_apply_linear" ++ t.tag ++ "_1x", &(args(t, "termite_metal_apply_linear_params") ++ .{"gid"})) ++ fill(
        \\
        \\    if (gid >= p.out_dim) return;
        \\    float acc = bias[gid];
        \\    uint row_base = gid * p.in_dim;
        \\    for (uint i = 0; i < p.in_dim; ++i) acc += input[i] * ${w};
        \\    output[gid] = acc;
        \\}
        \\
    , .{ .w = fill(t.load, .{ .x = "weight[row_base + i]" }) }));
}

/// One input row, threadgroup per output.
pub fn singleReduce(comptime t: Dtype) msl.Item {
    return msl.msl(msl.signature("termite_apply_linear" ++ t.tag ++ "_1x_reduce", &(args(t, "termite_metal_apply_linear_params") ++ .{ "uint lane [[thread_index_in_threadgroup]]", "uint group [[threadgroup_position_in_grid]]" })) ++ fill(
        \\
        \\    if (group >= p.out_dim) return;
        \\    threadgroup float partials[32];
        \\    float acc = lane == 0u ? bias[group] : 0.0f;
        \\    uint row_base = group * p.in_dim;
        \\    for (uint i = lane; i < p.in_dim; i += 32u) acc += input[i] * ${w};
        \\${tree}    if (lane == 0u) output[group] = partials[0];
        \\}
        \\
    , .{ .w = fill(t.load, .{ .x = "weight[row_base + i]" }), .tree = tree }));
}

/// Many input rows, thread per (row, output).
pub fn multiRow(comptime t: Dtype) msl.Item {
    return msl.msl(msl.signature("termite_apply_linear" ++ t.tag ++ "_multi_row", &(args(t, "termite_metal_linear_params") ++ .{"gid"})) ++ fill(
        \\
        \\    uint total = p.rows * p.out_dim;
        \\    if (gid >= total) return;
        \\    uint r = gid / p.out_dim;
        \\    uint o = gid % p.out_dim;
        \\    float acc = bias[o];
        \\    uint in_base = r * p.in_dim;
        \\    uint w_base = o * p.in_dim;
        \\    for (uint i = 0; i < p.in_dim; ++i) acc += input[in_base + i] * ${w};
        \\    output[gid] = acc;
        \\}
        \\
    , .{ .w = fill(t.load, .{ .x = "weight[w_base + i]" }) }));
}

/// Many input rows, threadgroup per (output, row).
pub fn multiRowReduce(comptime t: Dtype) msl.Item {
    return msl.msl(msl.signature("termite_apply_linear" ++ t.tag ++ "_multi_row_reduce", &(args(t, "termite_metal_linear_params") ++ .{ "threadgroup float *partials", "ushort lane [[thread_index_in_threadgroup]]", "tg2" })) ++ fill(
        \\
        \\    uint o = tg.x;
        \\    uint r = tg.y;
        \\    if (r >= p.rows || o >= p.out_dim) return;
        \\    float acc = lane == 0u ? bias[o] : 0.0f;
        \\    uint in_base = r * p.in_dim;
        \\    uint w_base = o * p.in_dim;
        \\    for (uint i = lane; i < p.in_dim; i += 32u) acc += input[in_base + i] * ${w};
        \\${tree}    if (lane == 0u) output[r * p.out_dim + o] = partials[0];
        \\}
        \\
    , .{ .w = fill(t.load, .{ .x = "weight[w_base + i]" }), .tree = if (t.inline_tree) tree_inline else tree }));
}
