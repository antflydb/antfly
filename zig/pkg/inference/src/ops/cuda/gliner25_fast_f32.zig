// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Accurate Tensor Core projections. FP32 storage and accumulation; no scratch.
const std = @import("std");
const Context = @import("context.zig").CudaContext;
const driver = @import("driver.zig");
const Buffer = @import("buffer.zig").DeviceBuffer;
pub const Kind = enum { tf32_small, bf16_small, bf16_medium, bf16_large };
pub const Plan = struct { kind: Kind, tile_rows: u32, tile_columns: u32 };

pub fn deviceSupported(major: i32, minor: i32) bool {
    return major == 8 and minor == 9;
}

/// Only encoder projection geometries with a measured benefit on L4 SM89.
/// Classifier/readout weights are excluded separately by immutable weight tags.
pub fn plan(rows: usize, width: usize, columns: usize) ?Plan {
    if (rows < 64 or rows > 4096) return null;
    const kind: Kind = if (width == 768 and columns == 768)
        (if (rows < 96) .tf32_small else if (rows < 256) .bf16_small else .bf16_large)
    else if (width == 1024 and columns == 1024 and rows >= 96)
        .bf16_large
    else if (width == 768 and columns == 3072)
        .bf16_large
    else if (width == 1024 and columns == 4096)
        (if (rows < 96) .bf16_medium else .bf16_large)
    else if (width == 3072 and columns == 768 and rows >= 256 and rows <= 1024)
        .bf16_large
    else
        return null;
    return switch (kind) {
        .tf32_small, .bf16_small => .{ .kind = kind, .tile_rows = 32, .tile_columns = 32 },
        .bf16_medium => .{ .kind = kind, .tile_rows = 32, .tile_columns = 64 },
        .bf16_large => .{ .kind = kind, .tile_rows = 64, .tile_columns = 64 },
    };
}

fn validBuffer(b: Buffer, bytes: usize) bool {
    return b.ptr != 0 and b.ptr % 16 == 0 and b.len >= bytes and b.ptr <= std.math.maxInt(u64) - bytes;
}
fn overlaps(a: Buffer, a_bytes: usize, b: Buffer, b_bytes: usize) bool {
    return a.ptr < b.ptr + b_bytes and b.ptr < a.ptr + a_bytes;
}

pub const Module = struct {
    module: driver.CUmodule = null,
    functions: [4]driver.CUfunction = @splat(null),

    pub fn init(self: *Module, ctx: *Context) !void {
        if (self.module != null) return;
        if (!deviceSupported(ctx.info.compute_major, ctx.info.compute_minor)) return error.UnsupportedCudaArchitecture;
        try ctx.makeCurrent();
        var module: driver.CUmodule = null;
        const image = @embedFile("artifacts/gliner25_fast_f32_sm89.cubin");
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var functions: [4]driver.CUfunction = @splat(null);
        for ([_][:0]const u8{ "antfly_gliner_tf32x3_small_v1", "antfly_gliner_bf16x3_small_v1", "antfly_gliner_bf16x3_medium_v1", "antfly_gliner_bf16x3_large_v1" }, 0..) |name, i| {
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&functions[i], module, name));
        }
        self.* = .{ .module = module, .functions = functions };
    }

    pub fn deinit(self: *Module, ctx: *Context) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
        }
        self.* = .{};
    }

    pub fn run(self: *Module, ctx: *Context, output: Buffer, input: Buffer, weight: Buffer, bias: ?Buffer, rows: usize, width: usize, columns: usize) !void {
        const p = plan(rows, width, columns) orelse return error.InvalidShape;
        if (self.module == null or !deviceSupported(ctx.info.compute_major, ctx.info.compute_minor)) return error.InvalidCudaState;
        const out_bytes = rows * columns * 4;
        const in_bytes = rows * width * 4;
        const weight_bytes = columns * width * 4;
        if (!validBuffer(output, out_bytes) or !validBuffer(input, in_bytes) or !validBuffer(weight, weight_bytes)) return error.InvalidShape;
        if (overlaps(output, out_bytes, input, in_bytes) or overlaps(output, out_bytes, weight, weight_bytes)) return error.InvalidShape;
        if (bias) |b| {
            if (!validBuffer(b, columns * 4) or overlaps(output, out_bytes, b, columns * 4)) return error.InvalidShape;
        }
        var pointers = [_]driver.CUdeviceptr{ output.ptr, input.ptr, weight.ptr, if (bias) |b| b.ptr else 0 };
        var dimensions = [_]u32{ @intCast(rows), @intCast(width), @intCast(columns) };
        var args = [_]?*anyopaque{ &pointers[0], &pointers[1], &pointers[2], &pointers[3], &dimensions[0], &dimensions[1], &dimensions[2] };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[@backingInt(p.kind)], @intCast((rows + p.tile_rows - 1) / p.tile_rows), @intCast((columns + p.tile_columns - 1) / p.tile_columns), 1, 128, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
    }
};

test "GLiNER fast FP32 bounded dispatch" {
    try std.testing.expect(deviceSupported(8, 9));
    try std.testing.expect(!deviceSupported(8, 0));
    try std.testing.expect(!deviceSupported(9, 0));
    try std.testing.expectEqual(Kind.tf32_small, plan(64, 768, 768).?.kind);
    try std.testing.expectEqual(Kind.bf16_small, plan(128, 768, 768).?.kind);
    try std.testing.expectEqual(Kind.bf16_medium, plan(64, 1024, 4096).?.kind);
    try std.testing.expectEqual(Kind.bf16_large, plan(4096, 1024, 4096).?.kind);
    for ([_][3]usize{ .{ 63, 768, 768 }, .{ 4097, 768, 768 }, .{ 64, 1024, 1024 }, .{ 128, 3072, 768 }, .{ 2048, 3072, 768 }, .{ 512, 4096, 1024 }, .{ 64, 768, 5 } }) |shape| try std.testing.expect(plan(shape[0], shape[1], shape[2]) == null);
}

test "GLiNER fast FP32 CUDA biased ragged projections match CPU and reload" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var ctx = Context.initDefault() catch |err| {
        if (@import("antfly_platform").env.getenvBool("TERMITE_REQUIRE_CUDA_TESTS")) return err;
        return error.SkipZigTest;
    };
    defer ctx.deinit();
    if (!deviceSupported(ctx.info.compute_major, ctx.info.compute_minor)) return error.SkipZigTest;
    var module = Module{};
    defer module.deinit(&ctx);
    var lt = try @import("cublaslt.zig").CublasLt.openWithAllocator(a, &ctx);
    defer lt.deinit();
    for ([_][3]usize{ .{ 65, 768, 768 }, .{ 129, 768, 768 }, .{ 65, 1024, 4096 }, .{ 257, 768, 768 }, .{ 257, 3072, 768 } }) |shape| {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const temp = arena.allocator();
        const rows = shape[0];
        const width = shape[1];
        const columns = shape[2];
        const x = try temp.alloc(f32, rows * width);
        const w = try temp.alloc(f32, columns * width);
        const b = try temp.alloc(f32, columns);
        for (x, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i % 199)) * 0.13) * 0.13;
        for (w, 0..) |*v, i| v.* = @cos(@as(f32, @floatFromInt(i % 197)) * 0.17) * 0.13;
        for (b, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.05) * 0.3;
        var input = try Buffer.alloc(&ctx, x.len * 4);
        defer input.free(&ctx);
        var weight = try Buffer.alloc(&ctx, w.len * 4);
        defer weight.free(&ctx);
        var bias = try Buffer.alloc(&ctx, b.len * 4);
        defer bias.free(&ctx);
        var output = try Buffer.alloc(&ctx, rows * columns * 4);
        defer output.free(&ctx);
        try input.copyFromHost(&ctx, std.mem.sliceAsBytes(x));
        try weight.copyFromHost(&ctx, std.mem.sliceAsBytes(w));
        try bias.copyFromHost(&ctx, std.mem.sliceAsBytes(b));
        try lt.matmulF32WeightF32OutWithWorkspace(&ctx, output, input, weight, .{}, rows, width, columns);
        const reference = try temp.alloc(f32, rows * columns);
        const actual = try temp.alloc(f32, rows * columns);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(reference));
        try ctx.synchronize();
        try module.init(&ctx);
        try module.init(&ctx);
        try std.testing.expectError(error.InvalidShape, module.run(&ctx, input, input, weight, null, rows, width, columns));
        for ([_]bool{ false, true, true }) |with_bias| {
            try module.run(&ctx, output, input, weight, if (with_bias) bias else null, rows, width, columns);
            try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
            try ctx.synchronize();
            for (actual, reference, 0..) |got, expected, i| {
                try std.testing.expect(std.math.isFinite(got));
                try std.testing.expectApproxEqAbs(expected + (if (with_bias) b[i % columns] else 0), got, 0.0001);
            }
            for ([_]usize{ 0, rows / 2, rows - 1 }) |row| for ([_]usize{ 0, columns / 2, columns - 1 }) |column| {
                var expected: f64 = if (with_bias) b[column] else 0;
                for (0..width) |k| expected += @as(f64, x[row * width + k]) * w[column * width + k];
                try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected)), actual[row * columns + column], 0.0001);
            };
        }
        module.deinit(&ctx);
        try std.testing.expect(module.module == null);
    }
}
