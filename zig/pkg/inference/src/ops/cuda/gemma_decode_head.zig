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

const std = @import("std");
const context = @import("context.zig");
const driver = @import("driver.zig");
const Buffer = @import("buffer.zig").DeviceBuffer;

pub fn supported(major: i32, version: i32, rows: usize, columns: usize, width: usize) bool {
    return major >= 8 and version >= 13000 and rows == 2 and columns == 2560 and width >= 32768 and width % 8 == 0;
}

pub const Module = struct {
    module: driver.CUmodule = null,
    function: driver.CUfunction = null,
    calls: usize = 0,

    pub fn deinit(self: *Module, ctx: *context.CudaContext) void {
        if (self.module != null) {
            ctx.makeCurrent() catch {};
            _ = ctx.driver.fns.cuModuleUnload(self.module);
            self.* = .{};
        }
    }

    pub fn run(self: *Module, ctx: *context.CudaContext, dst: Buffer, input: Buffer, weight: Buffer, rows: usize, columns: usize, width: usize) !bool {
        if (!supported(ctx.info.compute_major, ctx.info.driver_version, rows, columns, width)) return false;
        const output_bytes = try std.math.mul(usize, try std.math.mul(usize, rows, width), 4);
        const weight_bytes = try std.math.mul(usize, width, 2100);
        if (dst.len < output_bytes or input.len < 5760 or weight.len < weight_bytes or width > std.math.maxInt(u32)) return error.InvalidShape;
        try ctx.makeCurrent();
        if (self.module == null) {
            var loaded: driver.CUmodule = null;
            const image = @embedFile("gemma_decode_head.ptx") ++ "\x00";
            try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&loaded, image.ptr, 0, null, null));
            errdefer _ = ctx.driver.fns.cuModuleUnload(loaded);
            var function: driver.CUfunction = null;
            try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&function, loaded, "antfly_e4b_q6_q8_pair"));
            self.module = loaded;
            self.function = function;
        }
        var output = dst.ptr;
        var source = input.ptr;
        var weights = weight.ptr;
        var n: u32 = @intCast(width);
        var args = [_]?*anyopaque{ &output, &source, &weights, &n };
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(self.function, n / 8, 1, 1, 160, 1, 1, 0, ctx.stream, &args, null));
        ctx.noteKernelLaunch();
        self.calls += 1;
        return true;
    }
};

test "cooperative decode batches paired head requires its exact supported layout" {
    try std.testing.expect(supported(12, 13020, 2, 2560, 262144));
    try std.testing.expect(supported(8, 13020, 2, 2560, 262144));
    try std.testing.expect(!supported(8, 12900, 2, 2560, 262144));
    try std.testing.expect(!supported(7, 13020, 2, 2560, 262144));
    try std.testing.expect(!supported(12, 13020, 1, 2560, 262144));
    try std.testing.expect(!supported(12, 13020, 3, 2560, 262144));
    try std.testing.expect(!supported(12, 13020, 2, 3840, 262144));
    try std.testing.expect(!supported(12, 13020, 2, 2560, 262145));
}

test "cooperative decode batches paired head artifact matches CUDA source" {
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(@embedFile("gemma_decode_head.cu"), &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);
    const artifact = @embedFile("gemma_decode_head.ptx");
    const marker = "// source-sha256: ";
    const offset = std.mem.indexOf(u8, artifact, marker) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings(&hex, artifact[offset + marker.len ..][0..64]);
    try std.testing.expect(std.mem.indexOf(u8, artifact, ".version 9.0") != null);
}
