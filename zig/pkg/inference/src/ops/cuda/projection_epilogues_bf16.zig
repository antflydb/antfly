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
    try buffer.validateRegions(4, regions, lengths, @splat(16), 3);
}

const catalog = @import("../../tile/cuda_standalone.zig");
const image = @import("embedded_module.zig").profile;
const EmbeddedPrograms = @import("embedded_module.zig").EmbeddedPrograms;
const UpPrograms = EmbeddedPrograms(.{
    .k768_f32 = image(catalog.gelu[0]),
    .k768_bf16 = image(catalog.gelu[1]),
    .k1024_f32 = image(catalog.gelu[2]),
    .k1024_bf16 = image(catalog.gelu[3]),
});
const DownPrograms = EmbeddedPrograms(.{
    .k768_f32 = image(catalog.residual[0]),
    .k768_bf16 = image(catalog.residual[1]),
    .k1024_f32 = image(catalog.residual[2]),
    .k1024_bf16 = image(catalog.residual[3]),
});

pub const Module = struct {
    programs: UpPrograms = .{},
    calls: usize = 0,

    fn init(self: *Module, ctx: *context.CudaContext, index: usize) !void {
        try self.programs.init(ctx, @fromBackingInt(@intCast(index)));
    }

    pub fn deinitChecked(self: *Module, ctx: anytype) !void {
        try self.programs.deinitChecked(ctx);
        self.* = .{};
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        self.deinitChecked(ctx) catch {};
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
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.programs.function(@fromBackingInt(@intCast(index))), @intCast((r.wide + 127) / 128), @intCast((r.rows + 63) / 64), 1, catalog.gelu[0].threads, 1, 1, catalog.gelu[0].shared, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
    }
};

test "CUDA BF16 GELU projection bounds alignment and output ownership" {
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

pub const DownRequest = struct {
    input: buffer.DeviceBuffer,
    weight: buffer.DeviceBuffer,
    bias: buffer.DeviceBuffer,
    residual: buffer.DeviceBuffer,
    output: buffer.DeviceBuffer,
    rows: usize,
    hidden: usize,
    wide: usize,
    bf16_io: bool,
};

pub fn supportedDown(major: i32, minor: i32, rows: usize, hidden: usize, wide: usize, bf16_io: bool) bool {
    const minimum: usize = if (bf16_io and hidden == 1024) 256 else 4096;
    return major == 12 and minor == 1 and rows >= minimum and rows <= 65536 and
        ((hidden == 768 and wide == 3072) or (hidden == 1024 and wide == 4096));
}

pub fn validateDown(r: DownRequest) !void {
    if (!supportedDown(12, 1, r.rows, r.hidden, r.wide, r.bf16_io)) return error.InvalidShape;
    const scalar: usize = if (r.bf16_io) 2 else 4;
    const regions = [_]buffer.DeviceBuffer{ r.input, r.weight, r.bias, r.residual, r.output };
    const lengths = [_]usize{ r.rows * r.wide * 2, r.hidden * r.wide * 2, r.hidden * scalar, r.rows * r.hidden * scalar, r.rows * r.hidden * scalar };
    try buffer.validateRegions(5, regions, lengths, @splat(16), 4);
}

/// Down projection, bias and residual with separate rounding at both adds.
/// BF16 mode also rounds the biased projection before adding the residual.
/// No intermediate allocation, mutable state or persistent scratch.
pub const DownModule = struct {
    programs: DownPrograms = .{},
    calls: usize = 0,

    fn init(self: *DownModule, ctx: *context.CudaContext, index: usize) !void {
        try self.programs.init(ctx, @fromBackingInt(@intCast(index)));
    }

    pub fn deinitChecked(self: *DownModule, ctx: anytype) !void {
        try self.programs.deinitChecked(ctx);
        self.* = .{};
    }

    pub fn deinit(self: *DownModule, ctx: *context.CudaContext) void {
        self.deinitChecked(ctx) catch {};
    }

    pub fn run(self: *DownModule, ctx: *context.CudaContext, r: DownRequest) !void {
        try validateDown(r);
        if (!supportedDown(ctx.info.compute_major, ctx.info.compute_minor, r.rows, r.hidden, r.wide, r.bf16_io)) return error.InvalidShape;
        const index: usize = @as(usize, if (r.hidden == 1024) 2 else 0) + @intFromBool(r.bf16_io);
        try self.init(ctx, index);
        var p = [5]u64{ r.input.ptr, r.weight.ptr, r.bias.ptr, r.residual.ptr, r.output.ptr };
        var rows: u32 = @intCast(r.rows);
        var scratch: u64 = 0;
        var args = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &p[4], &rows, &scratch, &scratch };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.programs.function(@fromBackingInt(@intCast(index))), @intCast((r.hidden + 127) / 128), @intCast((r.rows + 63) / 64), 1, catalog.residual[0].threads, 1, 1, catalog.residual[0].shared, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
    }
};

test "CUDA BF16 residual projection validates precision bounds and output ownership" {
    for ([_]bool{ false, true }) |bf16| {
        const scalar: usize = if (bf16) 2 else 4;
        const valid = DownRequest{ .input = .{ .ptr = 0x10000000, .len = 8192 * 4096 * 2 }, .weight = .{ .ptr = 0x20000000, .len = 1024 * 4096 * 2 }, .bias = .{ .ptr = 0x30000000, .len = 1024 * scalar }, .residual = .{ .ptr = 0x40000000, .len = 8192 * 1024 * scalar }, .output = .{ .ptr = 0x50000000, .len = 8192 * 1024 * scalar }, .rows = 8192, .hidden = 1024, .wide = 4096, .bf16_io = bf16 };
        try validateDown(valid);
        inline for (.{ "input", "weight", "bias", "residual", "output" }) |field| {
            var bad = valid;
            @field(bad, field).len -= 1;
            try std.testing.expectError(error.InvalidShape, validateDown(bad));
            bad = valid;
            @field(bad, field).ptr += 2;
            try std.testing.expectError(error.InvalidShape, validateDown(bad));
        }
        inline for (.{ "input", "weight", "bias", "residual" }) |field| {
            var bad = valid;
            bad.output.ptr = @field(bad, field).ptr;
            try std.testing.expectError(error.InvalidShape, validateDown(bad));
        }
        try std.testing.expect(!supportedDown(12, 0, 8192, 1024, 4096, bf16));
        try std.testing.expect(!supportedDown(12, 1, 65537, 1024, 4096, bf16));
        try std.testing.expect(!supportedDown(12, 1, 4095, 768, 3072, bf16));
        if (bf16) {
            try std.testing.expect(!supportedDown(12, 1, 255, 1024, 4096, true));
            try std.testing.expect(supportedDown(12, 1, 256, 1024, 4096, true));
        }
    }
}
