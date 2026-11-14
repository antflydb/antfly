// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");
pub const Params = extern struct {
    conv: u64,
    gates: u64,
    w: u64,
    attention: u64,
    scaled_k: u64,
    u: u64,
    decay: u64,
    state: u64,
    out: u64,
    tokens: u32,
    heads: u32,
};
comptime {
    if (@sizeOf(Params) != 80 or @offsetOf(Params, "tokens") != 72)
        @compileError("GLiDE DeltaNet CUDA parameter ABI changed");
}
pub const Layout = struct { offsets: [5]usize, sizes: [5]usize, total: usize };
pub fn layout(tokens: usize, heads: usize) !Layout {
    if (tokens == 0 or tokens > 65535 or heads != 16) return error.InvalidShape;
    const rows = try std.math.mul(usize, std.mem.alignForward(usize, tokens, 32), heads);
    var result = Layout{ .offsets = undefined, .sizes = .{ rows * 256, rows * 64, rows * 256, rows * 512, rows * 4 }, .total = 0 };
    for (&result.offsets, result.sizes) |*offset, size| {
        result.total = std.mem.alignForward(usize, result.total, 256);
        offset.* = result.total;
        result.total = try std.math.add(usize, result.total, size);
    }
    return result;
}

test "GLiDE CUDA chunk workspace bounds and aligned nonoverlapping regions" {
    for ([_]usize{ 1, 31, 32, 33, 256, 5129, 40000, 65535 }) |tokens| {
        const value = try layout(tokens, 16);
        try std.testing.expect(value.total <= std.mem.alignForward(usize, tokens, 32) * 17472 + 1280);
        for (value.offsets, value.sizes, 0..) |offset, size, i| {
            try std.testing.expectEqual(@as(usize, 0), offset % 256);
            try std.testing.expect(offset + size <= value.total);
            if (i > 0) try std.testing.expect(offset >= value.offsets[i - 1] + value.sizes[i - 1]);
        }
    }
    try std.testing.expectError(error.InvalidShape, layout(0, 16));
    try std.testing.expectError(error.InvalidShape, layout(65536, 16));
    try std.testing.expectError(error.InvalidShape, layout(256, 8));
}
pub const Module = struct {
    module: driver.CUmodule = null,
    functions: [2]driver.CUfunction = @splat(null),
    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch return;
            _ = ctx.driver.fns.cuModuleUnload(self.module);
        }
        self.* = .{};
    }
    pub fn run(self: *Module, ctx: *context.CudaContext, parameters: Params) !void {
        try ctx.makeCurrent();
        if (self.module == null) {
            var module: driver.CUmodule = null;
            const image = @embedFile("glide_delta.ptx") ++ "\x00";
            try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, image.ptr, 0, null, null));
            errdefer _ = ctx.driver.fns.cuModuleUnload(module);
            var functions: [2]driver.CUfunction = @splat(null);
            for (&functions, [_][:0]const u8{ "glide_delta_prepare", "glide_delta_scan" }) |*fun, name|
                try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(fun, module, name));
            self.module = module;
            self.functions = functions;
        }
        var p = parameters;
        var args = [_]?*anyopaque{&p};
        for (self.functions, [_]u32{ (p.tokens + 31) / 32, 4 }, [_]u32{ 128, 256 }) |function, x, threads| {
            try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(function, x, p.heads, 1, threads, 1, 1, 0, ctx.stream, &args, null));
            ctx.noteKernelLaunch();
        }
    }
};
