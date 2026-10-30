// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Prefix L2 normalization with a dense output and a strided input/gradient.
//! The VJP accumulates into the prefix; suffix gradients are untouched.
const std = @import("std");
const driver = @import("driver.zig");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const abi = @import("launch_abi.zig");

pub const Geometry = struct {
    rows: u32,
    width: u32,
    input_stride: u32,

    pub fn bytes(self: Geometry, input: bool) !usize {
        if (self.rows == 0 or self.rows > std.math.maxInt(i32) or self.width == 0 or self.width > std.math.maxInt(i32) or self.width > self.input_stride)
            return error.InvalidPrefixNormalizationShape;
        return std.math.mul(usize, try std.math.mul(usize, self.rows, if (input) self.input_stride else self.width), @sizeOf(f32));
    }
};

/// Resolves symbols in a caller-owned module; owns no allocation or module.
/// A static 768-stride specialization retains the established training schedule.
pub const Program = struct {
    forward: driver.CUfunction,
    backward: driver.CUfunction,
    forward_768: driver.CUfunction,
    backward_768: driver.CUfunction,

    pub fn init(ctx: *Context, module: driver.CUmodule) !Program {
        var result: Program = undefined;
        inline for (.{ "antfly_prefix_l2_normalize_f32", "antfly_prefix_l2_accumulate_vjp_f32", "termite_embedding_gemma2_mrl_normalize_f32", "termite_embedding_gemma2_mrl_normalize_vjp_f32" }, .{ "forward", "backward", "forward_768", "backward_768" }) |symbol, field| {
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&@field(result, field), module, symbol));
        }
        return result;
    }

    pub fn normalize(self: Program, ctx: *Context, out: Buffer, input: Buffer, shape: Geometry) !void {
        try require(input, try shape.bytes(true));
        try require(out, try shape.bytes(false));
        var slots = .{ .out = out.ptr, .input = input.ptr, .rows = shape.rows, .width = shape.width, .stride = shape.input_stride };
        var args = abi.fields(&slots);
        const specialized = shape.input_stride == 768;
        try launch(ctx, if (specialized) self.forward_768 else self.forward, args[0..if (specialized) 4 else 5], shape.rows, if (specialized) 8 * @sizeOf(f32) else 0);
    }

    pub fn accumulateVjp(self: Program, ctx: *Context, dinput: Buffer, dout: Buffer, input: Buffer, shape: Geometry, weight: f32) !void {
        if (!std.math.isFinite(weight)) return error.InvalidPrefixNormalizationShape;
        try require(input, try shape.bytes(true));
        try require(dinput, try shape.bytes(true));
        try require(dout, try shape.bytes(false));
        var slots = .{ .dinput = dinput.ptr, .dout = dout.ptr, .input = input.ptr, .rows = shape.rows, .width = shape.width, .weight = weight, .stride = shape.input_stride };
        var args = abi.fields(&slots);
        const specialized = shape.input_stride == 768;
        try launch(ctx, if (specialized) self.backward_768 else self.backward, args[0..if (specialized) 6 else 7], shape.rows, if (specialized) 16 * @sizeOf(f32) else 0);
    }
};

fn require(buffer: Buffer, n: usize) !void {
    if (buffer.ptr == 0 or buffer.len < n) return error.InvalidCudaState;
}
fn launch(ctx: *Context, function: driver.CUfunction, args: []?*anyopaque, rows: u32, shared: u32) !void {
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(function, rows, 1, 1, 256, 1, 1, shared, ctx.stream, args.ptr, null));
    ctx.noteKernelLaunch();
}

test "prefix normalization geometry admits arbitrary stride and rejects invalid extents" {
    const shape = Geometry{ .rows = 513, .width = 257, .input_stride = 1024 };
    try std.testing.expectEqual(513 * 1024 * 4, try shape.bytes(true));
    try std.testing.expectEqual(513 * 257 * 4, try shape.bytes(false));
    try std.testing.expectError(error.Overflow, (Geometry{ .rows = std.math.maxInt(i32), .width = 1, .input_stride = std.math.maxInt(u32) }).bytes(true));
    for ([_]Geometry{ .{ .rows = 0, .width = 1, .input_stride = 1 }, .{ .rows = 1, .width = 0, .input_stride = 1 }, .{ .rows = 1, .width = 2, .input_stride = 1 }, .{ .rows = std.math.maxInt(u32), .width = 1, .input_stride = 1 } }) |bad|
        try std.testing.expectError(error.InvalidPrefixNormalizationShape, bad.bytes(true));
}
