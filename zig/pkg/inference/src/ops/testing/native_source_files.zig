// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const metal_renderer = @import("../../graph/quant_kernel_metal_renderer.zig");
const cuda_renderer = @import("../../graph/quant_kernel_cuda_renderer.zig");

pub fn read(path: []const u8) ![]u8 {
    const allocator = std.testing.allocator;
    const stored = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(1 << 20));
    errdefer allocator.free(stored);
    if (try metal_renderer.expandNativeSourceFile(allocator, stored)) |full| {
        allocator.free(stored);
        return full;
    }
    if (try cuda_renderer.expandNativeSourceFile(allocator, stored)) |full| {
        allocator.free(stored);
        return full;
    }
    return stored;
}
