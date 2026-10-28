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

//! SM121 vision up projection with the original GELU rounding boundaries.
//! Caller-owned inputs/output only; no global workspace, KV or recurrent state.
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");
const buffer = @import("buffer.zig");

pub const Request = struct {
    input: buffer.DeviceBuffer,
    weight: buffer.DeviceBuffer,
    bias: buffer.DeviceBuffer,
    output: buffer.DeviceBuffer,
    rows: usize,
    hidden: usize,
    wide: usize,
    round_before_gelu: bool,
};

pub fn supported(major: i32, minor: i32, rows: usize, hidden: usize, wide: usize) bool {
    return major == 12 and minor == 1 and rows >= 64 and rows <= 65536 and
        ((hidden == 768 and wide == 3072) or (hidden == 1024 and wide == 4096));
}

pub fn validate(r: Request) !void {
    if (!supported(12, 1, r.rows, r.hidden, r.wide)) return error.InvalidShape;
    const regions = [_]buffer.DeviceBuffer{ r.input, r.weight, r.bias, r.output };
    const lengths = [_]usize{ r.rows * r.hidden * 2, r.hidden * r.wide * 2, r.wide * @as(usize, if (r.round_before_gelu) 2 else 4), r.rows * r.wide * 2 };
    var ends: [4]u64 = undefined;
    for (regions, lengths, 0..) |region, length, i| {
        // The generated tensor-core loads specialize all four pointers to 16B.
        if (region.ptr == 0 or region.ptr & 15 != 0 or region.len < length) return error.InvalidShape;
        ends[i] = try std.math.add(u64, region.ptr, length);
    }
    for (0..3) |i| if (r.output.ptr < ends[i] and regions[i].ptr < ends[3]) return error.InvalidShape;
}

pub const Module = struct {
    modules: [4]driver.CUmodule = @splat(null),
    functions: [4]driver.CUfunction = @splat(null),
    calls: usize = 0,

    fn init(self: *Module, ctx: *context.CudaContext, index: usize) !void {
        if (self.modules[index] != null) return;
        const images = [_][]const u8{
            @embedFile("lighton_vision_up_768_f32.ptx") ++ "\x00",
            @embedFile("lighton_vision_up_768_bf16.ptx") ++ "\x00",
            @embedFile("lighton_vision_up_1024_f32.ptx") ++ "\x00",
            @embedFile("lighton_vision_up_1024_bf16.ptx") ++ "\x00",
        };
        try ctx.makeCurrent();
        var module: driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, images[index].ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var function: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, module, "lighton_vision_up"));
        self.modules[index] = module;
        self.functions[index] = function;
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        ctx.makeCurrent() catch return;
        for (self.modules) |module| if (module != null) {
            _ = ctx.driver.fns.cuModuleUnload(module);
        };
        self.* = .{};
    }

    pub fn run(self: *Module, ctx: *context.CudaContext, r: Request) !void {
        try validate(r);
        if (!supported(ctx.info.compute_major, ctx.info.compute_minor, r.rows, r.hidden, r.wide)) return error.InvalidShape;
        const index: usize = @as(usize, if (r.hidden == 1024) 2 else 0) + @intFromBool(r.round_before_gelu);
        try self.init(ctx, index);
        var p = [4]u64{ r.input.ptr, r.weight.ptr, r.bias.ptr, r.output.ptr };
        var rows: u32 = @intCast(r.rows);
        // Triton's ABI includes global/profile scratch pointers. Both sizes
        // are asserted zero by the generator; neither is allocated or read.
        var scratch: u64 = 0;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &rows, &scratch, &scratch };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[index], @intCast((r.wide + 127) / 128), @intCast((r.rows + 63) / 64), 1, 128, 1, 1, 24576, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
    }
};

test "LightOnOCR3 vision up GEMM bounds alignment and output ownership" {
    const valid = Request{ .input = .{ .ptr = 0x100000, .len = 64 * 768 * 2 }, .weight = .{ .ptr = 0x200000, .len = 768 * 3072 * 2 }, .bias = .{ .ptr = 0x800000, .len = 3072 * 4 }, .output = .{ .ptr = 0x900000, .len = 64 * 3072 * 2 }, .rows = 64, .hidden = 768, .wide = 3072, .round_before_gelu = false };
    try validate(valid);
    inline for (.{ "input", "weight", "bias", "output" }) |field| {
        var bad = valid;
        @field(bad, field).len -= 1;
        try std.testing.expectError(error.InvalidShape, validate(bad));
        bad = valid;
        @field(bad, field).ptr += 2;
        try std.testing.expectError(error.InvalidShape, validate(bad));
    }
    inline for (.{ "input", "weight", "bias" }) |field| {
        var bad = valid;
        bad.output.ptr = @field(bad, field).ptr;
        try std.testing.expectError(error.InvalidShape, validate(bad));
    }
    try std.testing.expect(!supported(12, 0, 64, 768, 3072));
    try std.testing.expect(!supported(12, 1, 63, 768, 3072));
    try std.testing.expect(!supported(12, 1, 65537, 768, 3072));
    try std.testing.expect(!supported(12, 1, 64, 1024, 3072));
}
