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

//! One-line entry points: tuned variants of a shared `*_impl` body that
//! differ only in the constants they pass (rows per simdgroup, simdgroup
//! count, tail guard, output type). Each is its ABI plus the call.

const std = @import("std");
const msl = @import("quant_metal_msl.zig");
const fill = msl.fill;

pub const in_f32 = "device const float *input";
pub const in_f16 = "device const half *input";
pub const w = "device const uchar *weight";
pub const out_f32 = "device float *output";
pub const linear_p = "constant termite_metal_linear_params &p";
pub const act_p = "constant termite_metal_apply_activation_params &ap";
pub const gate_up = [_][]const u8{ "device const uchar *weight_gate", "device const uchar *weight_up" };

/// Simdgroup-reduction schedules: lane, simdgroup and threadgroup ids.
pub const sg = [_][]const u8{ "lane", "sgitg", "tg" };
/// Simdgroup-matrix schedules: a char scratch tile and thread ids.
pub const mm = [_][]const u8{ "threadgroup char *shmem", "tiitg", "sgitg", "tg" };

/// The name an argument binds (`ushort lane [[...]]` and `lane` give "lane").
fn argName(comptime a: []const u8) []const u8 {
    const decl = if (std.mem.indexOf(u8, a, " [[")) |i| a[0..i] else a;
    const start = if (std.mem.lastIndexOfAny(u8, decl, " *&")) |i| i + 1 else 0;
    return decl[start..];
}

/// `kernel void name(args) { body }` on one line.
pub fn oneLine(comptime name: []const u8, comptime args: []const []const u8, comptime body: []const u8) msl.Item {
    return msl.msl(msl.signature(name, args) ++ " " ++ body ++ " }\n");
}

/// Forwards every argument, in order, then `extra`.
pub fn forward(comptime name: []const u8, comptime args: []const []const u8, comptime callee: []const u8, comptime extra: []const u8) msl.Item {
    comptime {
        var call: []const u8 = callee ++ "(";
        for (args) |a| call = call ++ argName(a) ++ ", ";
        return oneLine(name, args, call ++ extra ++ ");");
    }
}

pub const linear = [_][]const u8{ in_f32, w, out_f32, linear_p };
pub const gated = [_][]const u8{in_f32} ++ gate_up ++ .{ out_f32, linear_p, act_p };

/// `name_r<n>_ext` for each (suffix, constant) pair.
pub fn rows(comptime prefix: []const u8, comptime suffix: []const u8, comptime args: []const []const u8, comptime callee: []const u8, comptime variants: []const [2][]const u8) []const msl.Item {
    comptime {
        var items: []const msl.Item = &.{};
        for (variants) |v| items = items ++ .{forward(prefix ++ v[0] ++ suffix, args, callee, v[1])};
        return items;
    }
}

/// `<name><aligned>` and `<name>_tail` (the guarded last tile) of an mm impl.
pub fn tailPair(comptime name: []const u8, comptime aligned: []const u8, comptime args: []const []const u8, comptime callee: []const u8) [2]msl.Item {
    return .{ forward(name ++ aligned, args, callee, "false"), forward(name ++ "_tail", args, callee, "true") };
}

/// Q4_0 shared-cache mmv: RPTG input rows x NSG simdgroups x NR0 rows, plain
/// or gate/up with the activation applied.
pub fn q4_0Shared() []const msl.Item {
    comptime {
        var items: []const msl.Item = &.{};
        for ([_]bool{ false, true }) |pair| for ([_]u32{ 2, 4 }) |nsg| for ([_]u32{ 4, 8 }) |nr| for ([_]u32{ 2, 4 }) |rptg| {
            const name = fill("termite_q4_0_shared_${k}_nr${nr}_nsg${nsg}_r${r}", .{ .k = if (pair) "pair" else "linear", .nr = nr, .nsg = nsg, .r = rptg });
            const args = if (pair)
                .{ in_f32, w, "device const uchar *up", out_f32, linear_p, act_p }
            else
                linear;
            items = items ++ .{oneLine(name, &(args ++ .{"threadgroup uchar *cache"} ++ sg), fill("termite_q4_0_shared_mmv<${r}u, ${nr}u, ${nsg}u, ${pair}>(input, weight, ${up}, output, p, ${act}, cache, lane, sgitg, tg);", .{
                .r = rptg,
                .nr = nr,
                .nsg = nsg,
                .pair = if (pair) "true" else "false",
                .up = if (pair) "up" else "weight",
                .act = if (pair) "ap.activation_kind" else "0u",
            }))};
        };
        return items;
    }
}

/// `termite_q4_0_pair_activation_1x_reduce_nr<NR0>_nsg<NSG>`.
pub fn q4_0PairActivation() []const msl.Item {
    comptime {
        var items: []const msl.Item = &.{};
        for ([_]u32{ 2, 4 }) |nsg| for ([_]u32{ 4, 8 }) |nr| {
            items = items ++ .{forward(fill("termite_q4_0_pair_activation_1x_reduce_nr${nr}_nsg${nsg}", .{ .nr = nr, .nsg = nsg }), &(gated ++ sg), "termite_q4_0_pair_activation_mmv_impl", fill("${nr}u, ${nsg}u", .{ .nr = nr, .nsg = nsg }))};
        };
        return items;
    }
}

const r2to5 = [_][2][]const u8{ .{ "2", "2u" }, .{ "3", "3u" }, .{ "4", "4u" }, .{ "5", "5u" } };
/// The r8 variants run five rows; the name keeps the dispatch's bucket.
const r248 = [_][2][]const u8{ .{ "2", "2u" }, .{ "4", "4u" }, .{ "8", "5u" } };

pub const q8_0_linear_r_ext = rows("termite_q8_0_linear_r", "_ext", &(linear ++ sg), "termite_q8_0_linear_r_ext_impl", &r2to5);
pub const q4_0_linear_r_ext = rows("termite_q4_0_linear_r", "_ext", &(linear ++ sg), "termite_q4_0_linear_r_ext_impl", &r248);
pub const q4_0_linear_r_ext4 = rows("termite_q4_0_linear_r", "_ext4", &(linear ++ sg), "termite_q4_0_linear_r_ext4_impl", &r248);
pub const q8_0_pair_linear_r_ext = rows("termite_q8_0_pair_linear_r", "_ext", &([_][]const u8{ in_f32, "device const uchar *weight_a", "device const uchar *weight_b", "device float *output_a", "device float *output_b", linear_p } ++ sg), "termite_q8_0_pair_linear_r_ext_impl", &r2to5);
pub const q8_0_pair_activation_multiply_r_ext = rows("termite_q8_0_pair_activation_multiply_r", "_ext", &(gated ++ sg), "termite_q8_0_pair_activation_multiply_r_ext_impl", &r2to5);
pub const q4_0_pair_activation_multiply_r_ext = rows("termite_q4_0_pair_activation_multiply_r", "_ext", &(gated ++ sg), "termite_q4_0_pair_activation_multiply_r_ext_impl", &r2to5);

/// Short-batch Q4_0 with <rows, simdgroups> baked in.
pub fn q4_0Specialized(comptime rows_: u32, comptime nsg: u32) msl.Item {
    return oneLine(fill("termite_q4_0_linear_r${r}_specialized", .{ .r = rows_ }), &(linear ++ sg), fill("termite_q4_0_linear_short_specialized_impl<${r}u,${n}u>(input,weight,output,p,lane,sgitg,tg);", .{ .r = rows_, .n = nsg }));
}

/// A4B LM-head Q6_K for odd superblock counts: (suffix, NR0, NSG).
pub fn q6_kNbOdd() []const msl.Item {
    comptime {
        const variants = [_]struct { []const u8, []const u8 }{ .{ "", "2u, 2u" }, .{ "_nr1", "1u, 2u" }, .{ "_nr4_nsg1", "4u, 1u" }, .{ "_nr4_nsg2", "4u, 2u" }, .{ "_nr6_nsg1", "6u, 1u" }, .{ "_nr8_nsg1", "8u, 1u" } };
        var items: []const msl.Item = &.{};
        for (variants) |v| items = items ++ .{forward("termite_q6_k_linear_1x_reduce_nbodd" ++ v[0], &(linear ++ .{"threadgroup float *shmem"} ++ sg), "termite_q6_k_linear_1x_reduce_nbodd_impl", v[1])};
        return items;
    }
}

const linear_mm = linear ++ mm;
const linear_mm_f16 = [_][]const u8{ in_f16, w, out_f32, linear_p } ++ mm;
/// Gate/up writes f32 or f16 through a byte pointer; the flag picks.
const gated_mm = [_][]const u8{in_f32} ++ gate_up ++ .{ "device uchar *output", linear_p, act_p } ++ mm;

pub const q4_0_pair_activation_mm_m32_n64 = tailPair("termite_q4_0_pair_activation_mm_m32_n64", "_aligned", &(gated ++ mm), "termite_q4_0_pair_activation_mm_m32_n64_impl");
pub const q4_0_pair_activation_mm_m32_n32 = tailPair("termite_q4_0_pair_activation_mm_m32_n32", "_aligned", &(gated ++ mm), "termite_q4_0_pair_activation_mm_m32_n32_impl");
pub const q8_0_linear_mm_sg_v2 = tailPair("termite_q8_0_linear_mm_sg_v2", "", &linear_mm, "termite_q8_0_linear_mm_sg_v2_f32_impl") ++
    tailPair("termite_q8_0_linear_mm_sg_v2_in_f16", "", &linear_mm_f16, "termite_q8_0_linear_mm_sg_v2_f16_impl");
pub const q8_0_linear_mm_sg_m64 = tailPair("termite_q8_0_linear_mm_sg_m64", "", &linear_mm, "termite_q8_0_linear_mm_sg_m64_impl");
pub const q8_0_linear_mm_sg_m64_f16 = tailPair("termite_q8_0_linear_mm_sg_m64_in_f16", "", &linear_mm_f16, "termite_q8_0_linear_mm_sg_m64_f16_impl");
pub const q8_0_kv_pair_mm_sg = tailPair("termite_q8_0_kv_pair_mm_sg", "", &([_][]const u8{ in_f32, "device const uchar *weight_k", "device const uchar *weight_v", "device float *output_k", "device float *output_v", linear_p } ++ mm), "termite_q8_0_kv_pair_mm_sg_impl");
pub const q8_0_pair_activation_mm_sg_v2 = [_]msl.Item{
    forward("termite_q8_0_pair_activation_mm_sg_v2", &gated_mm, "termite_q8_0_pair_activation_mm_sg_v2_impl", "false"),
    forward("termite_q8_0_pair_activation_mm_sg_v2_out_f16", &gated_mm, "termite_q8_0_pair_activation_mm_sg_v2_impl", "true"),
};
