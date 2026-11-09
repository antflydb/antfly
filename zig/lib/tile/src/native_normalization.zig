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
pub const RmsExport = struct { name: []const u8, params_macro: []const u8, weighted: bool = true, residual: bool = false, bf16: bool = false, scale: ScaleStage = .none, register_row: ?RegisterRms = null };
pub fn cudaRms(comptime e: RmsExport, comptime family: []const u8) []const u8 {
    const register = if (e.register_row) |schedule| blk: {
        if (!e.weighted or e.bf16 or e.scale == .before_residual or (!e.residual and e.scale != .none)) @compileError("register RMS requires weighted F32 and the qualified residual/output-scale order");
        break :blk std.fmt.comptimePrint("    if (blockIdx.x >= rows) return;\n    if (dim == {d}u && blockDim.x == 256u) {{\n        termite_rms_register_{d}_f32<{d}u>(dst, input, weight, {s}, {s}, eps);\n        return;\n    }}\n", .{ schedule.width, schedule.width, if (!e.residual) @as(u32, 0) else if (e.scale == .output) @as(u32, 2) else @as(u32, 1), if (e.residual) "residual" else "nullptr", if (e.scale == .output) "scalar" else "nullptr" });
    } else "";
    return std.fmt.comptimePrint("extern \"C\" __global__ void {s}({s}) {{\n{s}    {s}<{s}, {s}, {s}, {d}>(dst, {s}, input, {s}, {s}, {s}, rows, dim, eps);\n}}\n", .{ e.name, e.params_macro, register, family, if (e.weighted) "true" else "false", if (e.residual) "true" else "false", if (e.bf16) "true" else "false", @backingInt(e.scale), if (e.bf16) "dst_bf16" else "nullptr", if (e.weighted) "weight" else "nullptr", if (e.residual) "residual" else "nullptr", if (e.scale == .none) "nullptr" else "scalar" });
}

/// Serial RMS has its own ordered sum-of-squares schedule. Row and one-row
/// adapters differ only in addressing; neither selects a parallel tree.
pub fn metalSerialRms(comptime name: []const u8, comptime rows: bool, comptime params_type: []const u8) []const u8 {
    const base = if (rows) "row_base + i" else "i";
    const normalized = math.rms(.scale_then_weight).renderNative(math.metal, .{ .value = "input[" ++ base ++ "]", .weight = "weight[i]", .inverse = "inv_rms" }, null);
    const body = (if (rows) "    if (gid >= p.rows) return;\n" else "    if (gid != 0) return;\n") ++ "    if (p.hidden_size == 0) return;\n" ++ (if (rows) "    uint row_base = gid * p.hidden_size;\n" else "") ++
        "    float mean_square = 0.0f;\n    for (uint i = 0; i < p.hidden_size; ++i) { float x = input[" ++ base ++ "]; mean_square += x * x; }\n    mean_square /= float(p.hidden_size);\n    float inv_rms = rsqrt(mean_square + p.eps);\n    for (uint i = 0; i < p.hidden_size; ++i) output[" ++ base ++ "] = " ++ normalized ++ ";\n";
    return gpu.nativeMetalExport(name, &.{ .{ .name = "input" }, .{ .name = "weight" }, .{ .name = "output", .writable = true } }, params_type, &.{"uint gid [[thread_position_in_grid]]"}, body);
}

/// The exact qualified register-row schedule uses a 256-thread shared tree,
/// retaining each lane's strided inputs across the reduction. Geometry is a
/// compile-time layout fact, never a model name or runtime policy override.
pub const RegisterRms = struct { width: u32 };
pub fn cudaRegisterRms(comptime schedule: RegisterRms) []const u8 {
    if (schedule.width == 0 or schedule.width % 256 != 0) @compileError("register RMS width must be a positive multiple of 256");
    return std.fmt.comptimePrint(
        \\// Keep the original 256-thread reduction tree, but publish its final five
        \\// levels with warp shuffles instead of block-wide barriers.
        \\__device__ __forceinline__ float termite_rms_sum_tree_256_f32(float value, float* partial) {{
        \\    const unsigned tid = threadIdx.x;
        \\    partial[tid] = value;
        \\    __syncthreads();
        \\    if (tid < 32u) {{
        \\        const float a = partial[tid] + partial[tid + 128u];
        \\        const float b = partial[tid + 64u] + partial[tid + 192u];
        \\        const float c = partial[tid + 32u] + partial[tid + 160u];
        \\        const float d = partial[tid + 96u] + partial[tid + 224u];
        \\        float sum = (a + b) + (c + d);
        \\        for (unsigned offset = 16u; offset != 0u; offset >>= 1u) {{
        \\            sum += __shfl_down_sync(0xffffffffu, sum, offset);
        \\        }}
        \\        if (tid == 0u) partial[0] = sum;
        \\    }}
        \\    __syncthreads();
        \\    return partial[0];
        \\}}
        \\
        \\// Short register-row norms cannot hide dependent global loads behind
        \\// other rows. Retain the strided inputs/weights/residuals in registers across
        \\// the reduction. Each lane accumulates in exactly the original order.
        \\// Op 0: weighted RMS; 1: weighted RMS + residual; 2: scale after residual.
        \\template <unsigned Op>
        \\__device__ __forceinline__ void termite_rms_register_{d}_f32(
        \\    float* dst, const float* input, const float* weight,
        \\    const float* residual, const float* scalar, float eps
        \\) {{
        \\    const unsigned tid = threadIdx.x;
        \\    const unsigned base = blockIdx.x * {d}u;
        \\    __shared__ float partial[256];
        \\    float values[{d}], weights[{d}], residuals[{d}];
        \\#pragma unroll
        \\    for (unsigned j = 0u; j < {d}u; ++j) {{
        \\        const unsigned i = tid + j * 256u;
        \\        values[j] = input[base + i];
        \\        weights[j] = weight[i];
        \\        if (Op != 0u) residuals[j] = residual[base + i];
        \\    }}
        \\    float sum = 0.0f;
        \\#pragma unroll
        \\    for (unsigned j = 0u; j < {d}u; ++j) sum += values[j] * values[j];
        \\    const float sumsq = termite_rms_sum_tree_256_f32(sum, partial);
        \\    // Match the historical runtime-divisor rounding, even though this shape
        \\    // is specialized: a constant reciprocal multiply is not equivalent.
        \\    float mean;
        \\    asm volatile("div.rn.f32 %0, %1, %2;" : "=f"(mean) : "f"(sumsq), "f"({d}.0f));
        \\    const float scale = rsqrtf(mean + eps);
        \\    const float output_scale = Op == 2u ? scalar[0] : 1.0f;
        \\#pragma unroll
        \\    for (unsigned j = 0u; j < {d}u; ++j) {{
        \\        float value = values[j] * scale * weights[j];
        \\        if (Op != 0u) value += residuals[j];
        \\        if (Op == 2u) value *= output_scale;
        \\        dst[base + tid + j * 256u] = value;
        \\    }}
        \\}}
        \\
    , .{ schedule.width, schedule.width, schedule.width / 256, schedule.width / 256, schedule.width / 256, schedule.width / 256, schedule.width / 256, schedule.width, schedule.width / 256 });
}

test "CUDA register row RMS keeps the original shared tree addition order" {
    // Independent host oracle for the old full shared-memory tree. Exercise
    // cancellation and widely differing magnitudes, not only equal inputs.
    for (0..8) |seed| {
        var old: [256]f32 = undefined;
        for (&old, 0..) |*value, lane| {
            const signed: i32 = @as(i32, @intCast((lane * 73 + seed * 41) % 1021)) - 510;
            value.* = @as(f32, @floatFromInt(signed)) * (if (lane % 3 == 0) @as(f32, 0x1p16) else @as(f32, 0x1p-8));
        }
        var compact: [32]f32 = undefined;
        for (&compact, 0..) |*value, lane| {
            const a: f32 = old[lane] + old[lane + 128];
            const b: f32 = old[lane + 64] + old[lane + 192];
            const c: f32 = old[lane + 32] + old[lane + 160];
            const d: f32 = old[lane + 96] + old[lane + 224];
            value.* = (a + b) + (c + d);
        }
        var stride: usize = 128;
        while (stride != 0) : (stride >>= 1) for (0..stride) |lane| {
            old[lane] += old[lane + stride];
        };
        stride = 16;
        while (stride != 0) : (stride >>= 1) for (0..stride) |lane| {
            compact[lane] += compact[lane + stride];
        };
        try std.testing.expectEqual(@as(u32, @bitCast(old[0])), @as(u32, @bitCast(compact[0])));
    }
}

test "CUDA register row RMS is explicit and preserves ordinary fallback exports" {
    const ordinary: RmsExport = .{ .name = "ordinary", .params_macro = "PARAMS" };
    const tuned: RmsExport = .{ .name = "tuned", .params_macro = "PARAMS", .residual = true, .scale = .output, .register_row = .{ .width = 2560 } };
    const plain = comptime cudaRms(ordinary, "fallback");
    const specialized = comptime cudaRms(tuned, "fallback");
    try std.testing.expect(std.mem.indexOf(u8, plain, "termite_rms_register_") == null);
    try std.testing.expect(std.mem.indexOf(u8, specialized, "dim == 2560u && blockDim.x == 256u") != null);
    try std.testing.expect(std.mem.indexOf(u8, specialized, "termite_rms_register_2560_f32<2u>") != null);
    // Every unsupported dimension/block still invokes the original adapter.
    try std.testing.expect(std.mem.indexOf(u8, specialized, "fallback<true, true, false, 2>") != null);
}
