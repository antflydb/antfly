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

//! Compact vision-block execution. This is a distinct BF16 activation
//! policy; the default encoder and recurrent/decoder arithmetic stay F32.
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");
const buffer = @import("buffer.zig");
const cublas = @import("cublaslt.zig");
const cudnn = @import("cudnn_sdpa.zig");

pub const Precision = enum {
    f32,
    bf16_vision,

    pub fn resolve(raw: ?[]const u8) !Precision {
        return if (raw) |value| std.meta.stringToEnum(Precision, value) orelse error.InvalidLightOnPrecisionProfile else .f32;
    }

    pub fn resolveWithAttention(raw: ?[]const u8, attention_enabled: bool) !Precision {
        const selected = try resolve(raw);
        return if (attention_enabled) selected else .f32;
    }
};

pub const Layout = struct {
    offsets: [9]usize,
    lengths: [9]usize,
    bytes: usize,

    pub fn init(rows: usize, hidden: usize, intermediate: usize, workspace: usize) !Layout {
        if (rows == 0 or rows > 65536 or !((hidden == 768 and intermediate == 3072) or (hidden == 1024 and intermediate == 4096))) return error.InvalidShape;
        const h = try std.math.mul(usize, try std.math.mul(usize, rows, hidden), 2);
        const wide = try std.math.mul(usize, try std.math.mul(usize, rows, intermediate), 2);
        const lengths = [9]usize{ h, try std.math.mul(usize, h, 3), h, h, h, h, h, wide, workspace };
        var result = Layout{ .offsets = undefined, .lengths = lengths, .bytes = 0 };
        for (lengths, 0..) |length, index| {
            result.bytes = (try std.math.add(usize, result.bytes, 255)) & ~@as(usize, 255);
            result.offsets[index] = result.bytes;
            result.bytes = try std.math.add(usize, result.bytes, length);
        }
        return result;
    }

    fn regions(self: Layout, arena: buffer.DeviceBuffer) ![9]buffer.DeviceBuffer {
        if (arena.ptr == 0 or arena.len < self.bytes or arena.ptr & 255 != 0) return error.InvalidShape;
        _ = try std.math.add(u64, arena.ptr, self.bytes);
        var result: [9]buffer.DeviceBuffer = undefined;
        for (&result, self.offsets, self.lengths) |*item, offset, length|
            item.* = .{ .ptr = arena.ptr + offset, .len = length, .borrowed = true };
        return result;
    }
};

pub const Request = struct {
    fused_up: bool = true,
    input: buffer.DeviceBuffer,
    output: buffer.DeviceBuffer,
    // norm1 weight/bias, QKV weight/bias, out weight/bias,
    // norm2 weight/bias, up weight/bias, down weight/bias.
    weights: [12]buffer.DeviceBuffer,
    positions: buffer.DeviceBuffer,
    rows: usize,
    hidden: usize,
    intermediate: usize,
    batch: usize,
    epsilon: f32,
    theta: f32,
    // Serving reuses the existing independently admitted cuDNN workspace.
    // Standalone qualification may supply its workspace in the arena.
    attention_workspace: ?buffer.DeviceBuffer = null,
};

fn disjoint(a: buffer.DeviceBuffer, a_bytes: usize, b: buffer.DeviceBuffer, b_bytes: usize) !void {
    if (a_bytes == 0 or b_bytes == 0) return;
    const a_end = try std.math.add(u64, a.ptr, a_bytes);
    const b_end = try std.math.add(u64, b.ptr, b_bytes);
    if (a.ptr == 0 or b.ptr == 0 or a.ptr < b_end and b.ptr < a_end) return error.InvalidShape;
}

pub const Module = struct {
    module: driver.CUmodule = null,
    functions: [4]driver.CUfunction = @splat(null),
    blocks: usize = 0,
    up: @import("lighton_vision_gemm.zig").Module = .{},

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        if (self.module != null) return;
        try ctx.makeCurrent();
        var module: driver.CUmodule = null;
        const image = @embedFile("lighton_vision_bf16.ptx") ++ "\x00";
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var functions: [4]driver.CUfunction = @splat(null);
        inline for (.{ "lighton_bf16_vision_norm", "lighton_bf16_vision_add", "lighton_bf16_vision_gelu", "lighton_bf16_vision_qkv_rope" }, 0..) |name, index|
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&functions[index], module, name));
        self.module = module;
        self.functions = functions;
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        self.up.deinit(ctx);
        if (self.module != null) {
            ctx.makeCurrent() catch return;
            _ = ctx.driver.fns.cuModuleUnload(self.module);
        }
        self.* = .{};
    }

    fn launch(self: *Module, ctx: *context.CudaContext, function: usize, grid: usize, args: []?*anyopaque) !void {
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[function], @intCast(grid), 1, 1, 256, 1, 1, 0, ctx.stream, args.ptr, null));
        ctx.noteKernelLaunch();
    }

    fn norm(self: *Module, ctx: *context.CudaContext, output: buffer.DeviceBuffer, input: buffer.DeviceBuffer, gamma: buffer.DeviceBuffer, beta: buffer.DeviceBuffer, rows: usize, hidden: usize, epsilon: f32) !void {
        var p = [4]u64{ output.ptr, input.ptr, gamma.ptr, beta.ptr };
        var r: u32 = @intCast(rows);
        var h: u32 = @intCast(hidden);
        var eps = epsilon;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &r, &h, &eps };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[0], @intCast(rows), 1, 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }

    fn add(self: *Module, ctx: *context.CudaContext, output: buffer.DeviceBuffer, a: buffer.DeviceBuffer, b: buffer.DeviceBuffer, count: usize) !void {
        var p = [3]u64{ output.ptr, a.ptr, b.ptr };
        var n: u32 = @intCast(count);
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &n };
        try self.launch(ctx, 1, (count + 255) / 256, &args);
    }

    /// All scratch is supplied by the caller and retained through the last
    /// stream consumer. Nothing in this module owns page or decoder state.
    pub fn block(self: *Module, ctx: *context.CudaContext, blas: *cublas.CublasLt, attention: *cudnn.Module, key: cudnn.Key, arena: buffer.DeviceBuffer, layout: Layout, gemm_workspace: buffer.DeviceBuffer, request: Request) !void {
        const r = request;
        if (r.rows == 0 or r.rows > 65536 or r.batch == 0 or r.batch > 16 or r.rows % r.batch != 0 or r.rows / r.batch > 4096 or !std.math.isFinite(r.epsilon) or r.epsilon <= 0 or !std.math.isFinite(r.theta) or r.theta <= 0) return error.InvalidShape;
        const expected = try Layout.init(r.rows, r.hidden, r.intermediate, layout.lengths[8]);
        if (!std.meta.eql(expected, layout)) return error.InvalidShape;
        const h_bytes = layout.lengths[0];
        if (r.input.ptr == 0 or r.output.ptr == 0 or r.input.len < h_bytes or r.output.len < h_bytes or r.positions.ptr == 0 or r.positions.len < r.rows * 2 * @sizeOf(u32)) return error.InvalidShape;
        if (key.batch != r.batch or key.sequence != r.rows / r.batch or key.q_heads != r.hidden / 64 or key.kv_heads != r.hidden / 64 or key.dim != 64 or key.scale != 0.125 or !key.ocr_vision or key.dtype != .bf16) return error.InvalidShape;
        const workspace_bytes = (try attention.workspaceBytesFor(key)) orelse return error.CudnnFailure;
        if (r.input.ptr & 7 != 0 or r.output.ptr & 1 != 0 or r.positions.ptr & 3 != 0) return error.InvalidShape;
        const s = try layout.regions(arena);
        const vendor_workspace = r.attention_workspace orelse s[8];
        if (workspace_bytes > vendor_workspace.len or (workspace_bytes != 0 and vendor_workspace.ptr == 0)) return error.InvalidShape;
        if (r.attention_workspace) |external| {
            try disjoint(external, workspace_bytes, arena, layout.bytes);
            try disjoint(external, workspace_bytes, r.input, h_bytes);
            try disjoint(external, workspace_bytes, r.output, h_bytes);
            try disjoint(external, workspace_bytes, r.positions, r.rows * 8);
            try disjoint(external, workspace_bytes, gemm_workspace, gemm_workspace.len);
        }
        try disjoint(r.input, h_bytes, r.output, h_bytes);
        try disjoint(arena, layout.bytes, r.input, h_bytes);
        try disjoint(arena, layout.bytes, r.output, h_bytes);
        try disjoint(arena, layout.bytes, r.positions, r.rows * 8);
        try disjoint(arena, layout.bytes, gemm_workspace, gemm_workspace.len);
        try disjoint(r.output, h_bytes, r.positions, r.rows * 8);
        try disjoint(r.output, h_bytes, gemm_workspace, gemm_workspace.len);
        try disjoint(r.input, h_bytes, gemm_workspace, gemm_workspace.len);
        try disjoint(r.positions, r.rows * 8, gemm_workspace, gemm_workspace.len);
        const w = r.weights;
        const weight_lengths = [12]usize{ r.hidden * 4, r.hidden * 4, 3 * r.hidden * r.hidden * 2, 3 * r.hidden * 2, r.hidden * r.hidden * 2, r.hidden * 2, r.hidden * 4, r.hidden * 4, r.hidden * r.intermediate * 2, r.intermediate * 2, r.intermediate * r.hidden * 2, r.hidden * 2 };
        for (w, weight_lengths) |weight, bytes| {
            if (weight.ptr == 0 or weight.ptr & 15 != 0 or weight.len < bytes) return error.InvalidShape;
            try disjoint(weight, bytes, arena, layout.bytes);
            try disjoint(weight, bytes, r.output, h_bytes);
            try disjoint(weight, bytes, gemm_workspace, gemm_workspace.len);
            try disjoint(weight, bytes, vendor_workspace, workspace_bytes);
        }

        try self.init(ctx);
        try self.norm(ctx, s[0], r.input, w[0], w[1], r.rows, r.hidden, r.epsilon);
        try blas.matmulBf16WeightBiasBf16Out(ctx, s[1], s[0], w[2], w[3], gemm_workspace, r.rows, r.hidden, 3 * r.hidden);
        var p = [5]u64{ s[2].ptr, s[3].ptr, s[4].ptr, s[1].ptr, r.positions.ptr };
        var rows: u32 = @intCast(r.rows);
        var heads: u32 = @intCast(r.hidden / 64);
        var dim: u32 = 64;
        var theta = r.theta;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &p[4], &rows, &heads, &dim, &theta };
        try self.launch(ctx, 3, (r.rows * r.hidden / 2 + 255) / 256, &args);
        if (!try attention.executeVision(key, s[2].ptr, s[3].ptr, s[4].ptr, s[5].ptr, if (workspace_bytes == 0) 0 else vendor_workspace.ptr)) return error.CudnnFailure;
        // QKV is dead after RoPE; reuse its first H rows for the projection.
        const projected = buffer.DeviceBuffer{ .ptr = s[1].ptr, .len = h_bytes, .borrowed = true };
        try blas.matmulBf16WeightBiasBf16Out(ctx, projected, s[5], w[4], w[5], gemm_workspace, r.rows, r.hidden, r.hidden);
        try self.add(ctx, s[6], r.input, projected, r.rows * r.hidden);
        try self.norm(ctx, s[0], s[6], w[6], w[7], r.rows, r.hidden, r.epsilon);
        if (r.fused_up and @import("lighton_vision_gemm.zig").supported(ctx.info.compute_major, ctx.info.compute_minor, r.rows, r.hidden, r.intermediate)) {
            try self.up.run(ctx, .{ .input = s[0], .weight = w[8], .bias = w[9], .output = s[7], .rows = r.rows, .hidden = r.hidden, .wide = r.intermediate, .round_before_gelu = true });
        } else {
            try blas.matmulBf16WeightBiasBf16Out(ctx, s[7], s[0], w[8], w[9], gemm_workspace, r.rows, r.hidden, r.intermediate);
            var gelu = [2]u64{ s[7].ptr, s[7].ptr };
            var count: u32 = @intCast(r.rows * r.intermediate);
            var gelu_args = [_]?*anyopaque{ &gelu[0], &gelu[1], &count };
            try self.launch(ctx, 2, (r.rows * r.intermediate + 255) / 256, &gelu_args);
        }
        try blas.matmulBf16WeightBiasBf16Out(ctx, s[0], s[7], w[10], w[11], gemm_workspace, r.rows, r.intermediate, r.hidden);
        try self.add(ctx, r.output, s[6], s[0], r.rows * r.hidden);
        self.blocks += 1;
    }
};

test "LightOnOCR3 BF16 vision layout keeps aligned disjoint scratch and bounded shapes" {
    const layout = try Layout.init(2592, 1024, 4096, 12345);
    for (layout.offsets, 0..) |offset, i| {
        try std.testing.expectEqual(@as(usize, 0), offset % 256);
        if (i != 0) try std.testing.expect(offset >= layout.offsets[i - 1] + layout.lengths[i - 1]);
    }
    try std.testing.expectEqual(layout.offsets[8] + 12345, layout.bytes);
    try std.testing.expectError(error.InvalidShape, Layout.init(65537, 1024, 4096, 0));
    try std.testing.expectError(error.InvalidShape, Layout.init(1, 1024, 3072, 0));
    try std.testing.expectError(error.Overflow, Layout.init(1, 1024, 4096, std.math.maxInt(usize)));
    try std.testing.expectEqual(Precision.f32, try Precision.resolve(null));
    try std.testing.expectEqual(Precision.bf16_vision, try Precision.resolve("bf16_vision"));
    try std.testing.expectError(error.InvalidLightOnPrecisionProfile, Precision.resolve("bf16"));
    try std.testing.expectEqual(Precision.f32, try Precision.resolveWithAttention("bf16_vision", false));
    try std.testing.expectEqual(Precision.bf16_vision, try Precision.resolveWithAttention("bf16_vision", true));
    try std.testing.expectError(error.InvalidLightOnPrecisionProfile, Precision.resolveWithAttention("typo", false));
}
