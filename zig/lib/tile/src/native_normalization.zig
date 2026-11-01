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

//! Native normalization adapters select a statistics algorithm explicitly.
const std = @import("std");
const gpu = @import("gpu_model.zig");
const math = @import("native_math.zig");
pub const Input = enum(u32) { plain = 0, residual = 1, bias_residual = 2 };
pub const LayerExport = struct { name: []const u8, input: Input = .plain, save_sum: bool = false, statistics: math.Statistics = .centered_two_pass };
pub fn metalLayer(comptime e: LayerExport, comptime params_type: []const u8, comptime family: []const u8) []const u8 {
    if (e.statistics != .centered_two_pass and e.statistics != .raw_moments_tree) @compileError("native layer export requires its existing serial-centered or row-moments schedule");
    const rows = e.statistics == .raw_moments_tree;
    const first = if (e.input == .plain) "input" else if (e.input == .residual) "a" else "projected";
    const other = if (e.input == .plain) "input" else if (e.input == .residual) "b" else "residual";
    const bias = if (e.input == .bias_residual) "bias" else first;
    var buffers: []const gpu.NativeBuffer = &.{.{ .name = first }};
    if (e.input == .bias_residual) buffers = buffers ++ [_]gpu.NativeBuffer{.{ .name = "bias" }};
    if (e.input != .plain) buffers = buffers ++ [_]gpu.NativeBuffer{.{ .name = other }};
    buffers = buffers ++ [_]gpu.NativeBuffer{ .{ .name = "gamma" }, .{ .name = "beta" } };
    if (e.save_sum) buffers = buffers ++ [_]gpu.NativeBuffer{.{ .name = "sum_out", .writable = true }};
    buffers = buffers ++ [_]gpu.NativeBuffer{.{ .name = "output", .writable = true }};
    const attrs: []const []const u8 = if (rows) &.{ "threadgroup float *scratch [[threadgroup(0)]]", "uint tid [[thread_index_in_threadgroup]]", "uint3 tg [[threadgroup_position_in_grid]]", "uint3 threads_per_tg [[threads_per_threadgroup]]" } else &.{"uint gid [[thread_position_in_grid]]"};
    const body = std.fmt.comptimePrint("    {s}<{d}u, {s}, {s}>({s}, {s}, {s}, gamma, beta, {s}, output, p, {s});\n", .{ family, @backingInt(e.input), if (e.save_sum) "true" else "false", if (rows) "true" else "false", first, other, bias, if (e.save_sum) "sum_out" else "output", if (rows) "scratch, tg.x, tid, threads_per_tg.x" else "nullptr, gid, 0u, 1u" });
    return gpu.nativeMetalExport(e.name, buffers, params_type, attrs, body);
}

pub const ScaleStage = enum(u32) { none = 0, before_residual = 1, output = 2 };
pub const RmsExport = struct { name: []const u8, params_macro: []const u8, weighted: bool = true, residual: bool = false, bf16: bool = false, scale: ScaleStage = .none };
pub fn cudaRms(comptime e: RmsExport, comptime family: []const u8) []const u8 {
    return std.fmt.comptimePrint("extern \"C\" __global__ void {s}({s}) {{\n    {s}<{s}, {s}, {s}, {d}>(dst, {s}, input, {s}, {s}, {s}, rows, dim, eps);\n}}\n", .{ e.name, e.params_macro, family, if (e.weighted) "true" else "false", if (e.residual) "true" else "false", if (e.bf16) "true" else "false", @backingInt(e.scale), if (e.bf16) "dst_bf16" else "nullptr", if (e.weighted) "weight" else "nullptr", if (e.residual) "residual" else "nullptr", if (e.scale == .none) "nullptr" else "scalar" });
}
/// Serial RMS has its own ordered sum-of-squares schedule. Row and one-row
/// adapters differ only in addressing; neither selects a parallel tree.
pub fn metalSerialRms(comptime name: []const u8, comptime rows: bool, comptime params_type: []const u8) []const u8 {
    const base = if (rows) "row_base + i" else "i";
    const normalized = math.rms(.scale_then_weight).renderNative(math.metal, .{ .value = "input[" ++ base ++ "]", .bias = "weight[i]", .gate = "inv_rms", .residual = "" }, null);
    const body = (if (rows) "    if (gid >= p.rows) return;\n" else "    if (gid != 0) return;\n") ++ "    if (p.hidden_size == 0) return;\n" ++ (if (rows) "    uint row_base = gid * p.hidden_size;\n" else "") ++
        "    float mean_square = 0.0f;\n    for (uint i = 0; i < p.hidden_size; ++i) { float x = input[" ++ base ++ "]; mean_square += x * x; }\n    mean_square /= float(p.hidden_size);\n    float inv_rms = rsqrt(mean_square + p.eps);\n    for (uint i = 0; i < p.hidden_size; ++i) output[" ++ base ++ "] = " ++ normalized ++ ";\n";
    return gpu.nativeMetalExport(name, &.{ .{ .name = "input" }, .{ .name = "weight" }, .{ .name = "output", .writable = true } }, params_type, &.{"uint gid [[thread_position_in_grid]]"}, body);
}
