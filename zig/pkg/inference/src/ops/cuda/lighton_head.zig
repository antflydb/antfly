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

//! Fused 0.8B head projection/top1. F32 input is rounded to BF16 exactly
//! as in the ordinary projection; no full vocabulary logits are materialized.
//! All buffers belong to the caller and contain actual request rows only.
const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");
const buffer = @import("buffer.zig");

pub const tiles = 970;
pub const Request = struct {
    input: buffer.DeviceBuffer,
    weight: buffer.DeviceBuffer,
    partial: buffer.DeviceBuffer,
    indices: buffer.DeviceBuffer,
    suppress: buffer.DeviceBuffer,
    output: buffer.DeviceBuffer,
    rows: usize,
    suppress_count: usize,
};

pub fn supported(major: i32, minor: i32, rows: usize, hidden: usize, vocab: usize) bool {
    return major == 12 and minor == 1 and rows >= 1 and rows <= 128 and hidden == 1024 and vocab == 248320;
}

pub fn validate(r: Request) !void {
    if (!supported(12, 1, r.rows, 1024, 248320) or r.suppress_count > std.math.maxInt(i32)) return error.InvalidShape;
    const regions = [_]buffer.DeviceBuffer{ r.input, r.weight, r.suppress, r.partial, r.indices, r.output };
    const lengths = [_]usize{ r.rows * 1024 * 4, 248320 * 1024 * 2, try std.math.mul(usize, r.suppress_count, 4), r.rows * tiles * 4, r.rows * tiles * 4, r.rows * 4 };
    const alignment = [_]u64{ 4, 16, 4, 16, 16, 4 };
    var ends: [6]u64 = undefined;
    for (regions, lengths, alignment, 0..) |region, length, align_bytes, i| {
        if (length != 0 and (region.ptr == 0 or region.ptr % align_bytes != 0 or region.len < length)) return error.InvalidShape;
        ends[i] = try std.math.add(u64, region.ptr, length);
    }
    for (3..6) |written| for (0..written) |other| {
        if (lengths[other] != 0 and regions[written].ptr < ends[other] and regions[other].ptr < ends[written]) return error.InvalidShape;
    };
}

pub const Module = struct {
    modules: [2]driver.CUmodule = @splat(null),
    functions: [2]driver.CUfunction = @splat(null),
    calls: usize = 0,

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        if (self.modules[1] != null) return;
        try ctx.makeCurrent();
        errdefer self.deinit(ctx);
        const images = [_][]const u8{ @embedFile("lighton_head_top1_partial.ptx") ++ "\x00", @embedFile("lighton_head_top1_finish.ptx") ++ "\x00" };
        const names = [_][:0]const u8{ "lighton_head_top1_partial", "lighton_head_top1_finish" };
        for (images, names, 0..) |image, name, i| {
            try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&self.modules[i], image.ptr, 0, null, null));
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&self.functions[i], self.modules[i], name.ptr));
        }
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
        if (!supported(ctx.info.compute_major, ctx.info.compute_minor, r.rows, 1024, 248320)) return error.InvalidShape;
        try self.init(ctx);
        var p = [6]u64{ r.input.ptr, r.weight.ptr, r.partial.ptr, r.indices.ptr, r.suppress.ptr, r.output.ptr };
        var rows: u32 = @intCast(r.rows);
        var count: u32 = @intCast(r.suppress_count);
        // The pinned generator asserts both hidden ABI scratch sizes are zero.
        var scratch: u64 = 0;
        var first = [_]?*anyopaque{ &p[0], &p[1], &p[2], &p[3], &p[4], &rows, &count, &scratch, &scratch };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[0], tiles, 1, 1, 256, 1, 1, 32768, ctx.stream, &first, null));
        ctx.noteKernelLaunch();
        var last = [_]?*anyopaque{ &p[2], &p[3], &p[5], &scratch, &scratch };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.functions[1], @intCast(r.rows), 1, 1, 128, 1, 1, 16, ctx.stream, &last, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
    }
};

test "LightOnOCR3 fused head bounds alignment and scratch ownership" {
    const valid = Request{ .input = .{ .ptr = 0x100004, .len = 7 * 1024 * 4 }, .weight = .{ .ptr = 0x10000000, .len = 248320 * 1024 * 2 }, .partial = .{ .ptr = 0x40000000, .len = 7 * tiles * 4 }, .indices = .{ .ptr = 0x50000000, .len = 7 * tiles * 4 }, .suppress = .{}, .output = .{ .ptr = 0x60000004, .len = 7 * 4 }, .rows = 7, .suppress_count = 0 };
    try validate(valid);
    inline for (.{ "input", "weight", "partial", "indices", "output" }) |field| {
        var bad = valid;
        @field(bad, field).len -= 1;
        try std.testing.expectError(error.InvalidShape, validate(bad));
        bad = valid;
        @field(bad, field).ptr += 2;
        try std.testing.expectError(error.InvalidShape, validate(bad));
    }
    inline for (.{ "partial", "indices", "output" }) |written| {
        inline for (.{ "input", "weight", "partial", "indices", "output" }) |other| {
            if (comptime !std.mem.eql(u8, written, other)) {
                var bad = valid;
                @field(bad, written).ptr = @field(bad, other).ptr;
                try std.testing.expectError(error.InvalidShape, validate(bad));
            }
        }
    }
    var bad = valid;
    bad.suppress_count = 1;
    try std.testing.expectError(error.InvalidShape, validate(bad));
    try std.testing.expect(!supported(12, 1, 129, 1024, 248320));
    try std.testing.expect(!supported(12, 1, 7, 2560, 248320));
    try std.testing.expect(!supported(12, 0, 7, 1024, 248320));
}
