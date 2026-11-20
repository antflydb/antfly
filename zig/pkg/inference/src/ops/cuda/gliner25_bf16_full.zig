// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Diagnostic full-BF16 graph boundaries; managed promotion is separately gated.
const Context = @import("context.zig").CudaContext;
const std = @import("std");
const driver = @import("driver.zig");
const Buffer = @import("buffer.zig").DeviceBuffer;
/// Python CUDA BF16 boundaries: scaled keys, rounded relative scores, BF16
/// linear/GELU outputs, and fused first-FFN bias with an F32-dot fallback.
pub const python_bf16_flags: u32 = 8 | 16 | 32768 | 131072 | 1048576 | 2097152 | separate_output_bias_flag;
pub const separate_output_bias_flag: u32 = 8388608;
pub const Module = struct {
    module: driver.CUmodule = null,
    functions: [8]driver.CUfunction = @splat(null),
    flags: u32 = python_bf16_flags,
    fuse_views: bool = true,
    pub fn init(self: *Module, ctx: *Context) !void {
        if (self.module != null) return;
        if (ctx.info.compute_major != 8 or ctx.info.compute_minor != 9) return error.UnsupportedCudaArchitecture;
        try ctx.makeCurrent();
        var m: driver.CUmodule = null;
        const image = @embedFile("artifacts/gliner25_bf16_full_sm89.cubin");
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&m, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(m);
        var f: [8]driver.CUfunction = @splat(null);
        for ([_][:0]const u8{ "antfly_gliner_bf16_full_round_v1", "antfly_gliner_bf16_full_norm_v1", "antfly_gliner_bf16_full_scale_key_v1", "antfly_gliner_bf16_full_views_v1", "antfly_gliner_bf16_full_softmax_v1", "antfly_gliner_bf16_full_gelu_v1", "antfly_gliner_bf16_full_norm_dot_bias_v1", "antfly_gliner_bf16_full_scale_views_v1" }, 0..) |name, i| try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&f[i], m, name));
        self.module = m;
        self.functions = f;
    }
    pub fn deinit(self: *Module, ctx: *Context) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
        }
        self.* = .{};
    }
    fn launch(self: *Module, ctx: *Context, index: usize, grid: usize, block: u32, args: []?*anyopaque) !void {
        if (self.module == null or grid == 0) return error.InvalidCudaState;
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[index], @intCast(grid), 1, 1, block, 1, 1, 0, ctx.stream, args.ptr, null));
        ctx.noteKernelLaunch();
    }
    pub fn round(self: *Module, ctx: *Context, output: Buffer, count: usize) !void {
        if (count == 0) return;
        if (output.ptr == 0 or output.len / 4 < count) return error.InvalidShape;
        var ptr = output.ptr;
        var n: u32 = @intCast(count);
        var args = [_]?*anyopaque{ &ptr, &n };
        try self.launch(ctx, 0, (count + 255) / 256, 256, &args);
    }
    pub fn norm(self: *Module, ctx: *Context, output: Buffer, mirror: Buffer, inputs: [4]Buffer, rows: usize, width: usize, eps: f32) !void {
        if (rows == 0 or rows > 4096 or width == 0 or width > 1024 or !std.math.isFinite(eps) or eps <= 0) return error.InvalidShape;
        try check(output, rows * width * 4);
        try check(inputs[0], rows * width * 4);
        if (inputs[1].ptr != 0) try check(inputs[1], rows * width * 4);
        for (inputs[2..]) |input| try check(input, width * 4);
        if (mirror.ptr != 0) try check(mirror, rows * width * 2);
        var pointers = [_]u64{ output.ptr, mirror.ptr, inputs[0].ptr, inputs[1].ptr, inputs[2].ptr, inputs[3].ptr };
        var rs: u32 = @intCast(rows);
        var w: u32 = @intCast(width);
        var e = eps;
        var flags = self.flags;
        var args = [_]?*anyopaque{ &pointers[0], &pointers[1], &pointers[2], &pointers[3], &pointers[4], &pointers[5], &rs, &w, &e, &flags };
        try self.launch(ctx, 1, rows, 256, &args);
    }
    pub fn scaleKey(self: *Module, ctx: *Context, output: Buffer, input: Buffer, count: usize) !void {
        if (count == 0 or count > 4096 * 1024) return error.InvalidShape;
        try check(output, count * 2);
        try check(input, count * 2);
        var pointers = [_]u64{ output.ptr, input.ptr };
        var n: u32 = @intCast(count);
        var flags = self.flags;
        var args = [_]?*anyopaque{ &pointers[0], &pointers[1], &n, &flags };
        try self.launch(ctx, 2, (count + 255) / 256, 256, &args);
    }
    /// Preserve the upstream down-projection's BF16 dot, then BF16 bias-add
    /// boundary before the residual and normalization. No extra bias buffer.
    pub fn normDotBias(self: *Module, ctx: *Context, output: Buffer, mirror: Buffer, inputs: [5]Buffer, rows: usize, width: usize, eps: f32) !void {
        if (rows == 0 or rows > 4096 or width == 0 or width > 1024 or !std.math.isFinite(eps) or eps <= 0) return error.InvalidShape;
        try check(output, rows * width * 4);
        try check(inputs[0], rows * width * 2);
        try check(inputs[1], width * 4);
        try check(inputs[2], rows * width * 4);
        for (inputs[3..]) |input| try check(input, width * 4);
        if (mirror.ptr != 0) try check(mirror, rows * width * 2);
        var pointers = [_]u64{ output.ptr, mirror.ptr, inputs[0].ptr, inputs[1].ptr, inputs[2].ptr, inputs[3].ptr, inputs[4].ptr };
        var rs: u32 = @intCast(rows);
        var w: u32 = @intCast(width);
        var e = eps;
        var flags = self.flags;
        var args = [_]?*anyopaque{ &pointers[0], &pointers[1], &pointers[2], &pointers[3], &pointers[4], &pointers[5], &pointers[6], &rs, &w, &e, &flags };
        try self.launch(ctx, 6, rows, 256, &args);
    }
    pub fn views(self: *Module, ctx: *Context, buffers: [12]Buffer, sequence: usize, heads: usize, relative: usize, first: usize, count: usize) !void {
        var pointers: [12]u64 = undefined;
        var values = [_]u32{ @intCast(sequence), @intCast(heads), @intCast(relative), @intCast(first), @intCast(count) };
        var args: [17]?*anyopaque = undefined;
        for (buffers, &pointers, 0..) |b, *p, i| {
            p.* = b.ptr;
            args[i] = p;
        }
        for (&values, 12..) |*v, i| args[i] = v;
        try self.launch(ctx, 3, (count + 127) / 128, 128, &args);
    }
    /// Batch-one attention fits one complete head group. Scaling its keys and
    /// preparing pointer arrays share one launch with unchanged arithmetic.
    pub fn scaleViews(self: *Module, ctx: *Context, buffers: [12]Buffer, sequence: usize, heads: usize, relative: usize, count: usize, tokens: usize) !void {
        if (tokens == 0 or tokens > 512 * 1024 or count == 0 or count > heads) return error.InvalidShape;
        var pointers: [12]u64 = undefined;
        var values = [_]u32{ @intCast(sequence), @intCast(heads), @intCast(relative), 0, @intCast(count), @intCast(tokens), self.flags };
        var args: [19]?*anyopaque = undefined;
        for (buffers, &pointers, 0..) |b, *p, i| {
            p.* = b.ptr;
            args[i] = p;
        }
        for (&values, 12..) |*v, i| args[i] = v;
        try self.launch(ctx, 7, (tokens + 255) / 256, 256, &args);
    }
    pub fn softmax(self: *Module, ctx: *Context, buffers: [6]Buffer, sequence: usize, relative: usize, count: usize, first: usize, boundary: bool, heads: usize, pitch: usize) !void {
        var pointers: [6]u64 = undefined;
        var values = [_]u32{ @intCast(sequence), @intCast(relative), @intCast(count), @intCast(first), @intFromBool(boundary), @intCast(heads), @intCast(pitch), self.flags };
        var args: [14]?*anyopaque = undefined;
        for (buffers, &pointers, 0..) |b, *p, i| {
            p.* = b.ptr;
            args[i] = p;
        }
        for (&values, 6..) |*v, i| args[i] = v;
        try self.launch(ctx, 4, (count * sequence + 7) / 8, 256, &args);
    }
    pub fn gelu(self: *Module, ctx: *Context, output: Buffer, input: Buffer, bias: Buffer, count: usize, width: usize, exact: bool) !void {
        if (count == 0 or count > 4096 * 4096 or width == 0 or width > 4096 or count % width != 0) return error.InvalidShape;
        try check(output, count * 2);
        try check(input, count * @as(usize, if (self.flags & 131072 != 0 and self.flags & 2097152 == 0) 4 else 2));
        if (self.flags & 2097152 == 0) try check(bias, width * 4);
        var pointers = [_]u64{ output.ptr, input.ptr, bias.ptr };
        var values = [_]u32{ @intCast(count), @intCast(width), @intFromBool(exact), self.flags };
        var args = [_]?*anyopaque{ &pointers[0], &pointers[1], &pointers[2], &values[0], &values[1], &values[2], &values[3] };
        try self.launch(ctx, 5, (count + 255) / 256, 256, &args);
    }
};
fn check(buffer: Buffer, bytes: usize) !void {
    if (buffer.ptr == 0 or buffer.len < bytes) return error.InvalidShape;
}
