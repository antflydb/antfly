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

//! Fused static K1024/N248320 projection/top1. F32 input is rounded to BF16 exactly
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
    try buffer.validateRegions(6, regions, lengths, alignment, 3);
}

// These historical device entries are retained compatibility ABI names.
const Programs = @import("embedded_module.zig").EmbeddedPrograms(.{
    .partial = .{ .image = @embedFile("projection_top1_bf16_k1024_n248320_partial.ptx") ++ "\x00", .entry = "lighton_head_top1_partial" },
    .finish = .{ .image = @embedFile("projection_top1_bf16_k1024_n248320_finish.ptx") ++ "\x00", .entry = "lighton_head_top1_finish" },
});

pub const Module = struct {
    programs: Programs = .{},
    calls: usize = 0,

    pub fn init(self: *Module, ctx: *context.CudaContext) !void {
        if (self.programs.modules[1] != null) return;
        errdefer self.deinit(ctx);
        try self.programs.init(ctx, .partial);
        try self.programs.init(ctx, .finish);
    }

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        self.programs.deinitChecked(ctx) catch return;
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
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.programs.function(.partial), tiles, 1, 1, 256, 1, 1, 32768, ctx.stream, &first, null));
        ctx.noteKernelLaunch();
        var last = [_]?*anyopaque{ &p[2], &p[3], &p[5], &scratch, &scratch };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.programs.function(.finish), @intCast(r.rows), 1, 1, 128, 1, 1, 16, ctx.stream, &last, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
    }
};

test "CUDA BF16 projection top1 bounds alignment and scratch ownership" {
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
