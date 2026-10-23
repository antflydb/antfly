// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Admission contract for an independently owned exact device prefix. The first
//! adapter supports identity page tables only; arbitrary mappings must never be
//! mistaken for contiguous physical rows.
const std = @import("std");

pub const Donor = struct { layer: u32, heads: u32, dim: u32 };
pub const Request = struct {
    source_sequence: u32,
    destination_sequence: u32,
    tokens: usize,
    capacity: usize,
    page_size: u16,
    source_blocks: []const u32,
    destination_blocks: []const u32,
    donors: []const Donor,
    max_bytes: usize,
    check_ctx: ?*anyopaque = null,
    check_fn: ?*const fn (?*anyopaque) anyerror!void = null,

    pub fn check(self: Request) !void {
        if (self.check_fn) |check_fn| try check_fn(self.check_ctx);
    }

    /// Conservative allocation bound, including a private full-capacity page
    /// table for each donor even when the backend uses contiguous f32 rows.
    pub fn allocationBytes(self: Request, element_bytes: usize) !usize {
        if (self.tokens == 0 or self.capacity < self.tokens or self.page_size == 0 or
            self.capacity % self.page_size != 0 or self.donors.len == 0 or self.donors.len > 256 or
            (element_bytes != 2 and element_bytes != 4)) return error.InvalidDeviceKvClone;
        const pages = std.math.divCeil(usize, self.tokens, self.page_size) catch return error.InvalidDeviceKvClone;
        if (self.source_blocks.len < pages or self.destination_blocks.len < pages) return error.InvalidDeviceKvClone;
        for (self.source_blocks[0..pages], self.destination_blocks[0..pages], 0..) |src, dst, i| {
            if (src != i or dst != i) return error.UnsupportedDeviceKvCloneLayout;
        }
        var bytes: usize = 0;
        for (self.donors, 0..) |donor, i| {
            if (donor.heads == 0 or donor.heads > 256 or donor.dim == 0 or donor.dim > 4096 or
                (i > 0 and donor.layer <= self.donors[i - 1].layer)) return error.InvalidDeviceKvClone;
            const row = try std.math.mul(usize, try std.math.mul(usize, donor.heads, donor.dim), element_bytes * 2);
            bytes = try std.math.add(usize, bytes, try std.math.mul(usize, self.capacity, row));
            bytes = try std.math.add(usize, bytes, try std.math.mul(usize, self.capacity / self.page_size, @sizeOf(u32)));
        }
        if (bytes > self.max_bytes) return error.DeviceKvCloneBudgetExceeded;
        return bytes;
    }
};

pub const Result = struct { allocation_bound_bytes: usize, copied_bytes: usize, donors: usize };

test "device clone validates mappings donors and admission before allocation" {
    const blocks = [_]u32{ 0, 1 };
    var request = Request{ .source_sequence = 7, .destination_sequence = 0, .tokens = 17, .capacity = 48, .page_size = 16, .source_blocks = &blocks, .destination_blocks = &blocks, .donors = &.{.{ .layer = 5, .heads = 2, .dim = 512 }}, .max_bytes = 200000 };
    try std.testing.expectEqual(@as(usize, 196620), try request.allocationBytes(2));
    try std.testing.expectError(error.DeviceKvCloneBudgetExceeded, request.allocationBytes(4));
    request.source_blocks = &.{ 1, 0 };
    try std.testing.expectError(error.UnsupportedDeviceKvCloneLayout, request.allocationBytes(2));
    request.source_blocks = &blocks;
    request.destination_blocks = &.{0};
    try std.testing.expectError(error.InvalidDeviceKvClone, request.allocationBytes(2));
    request.destination_blocks = &blocks;
    request.donors = &.{ .{ .layer = 5, .heads = 2, .dim = 512 }, .{ .layer = 5, .heads = 2, .dim = 512 } };
    try std.testing.expectError(error.InvalidDeviceKvClone, request.allocationBytes(2));
    request.donors = &.{.{ .layer = 5, .heads = 2, .dim = 512 }};
    request.capacity = std.math.maxInt(usize) - 15;
    try std.testing.expectError(error.Overflow, request.allocationBytes(2));
}

test "device clone admits 32K E4B f16 capacity and rejects f32 under the same bound" {
    var blocks: [2048]u32 = undefined;
    for (&blocks, 0..) |*block, i| block.* = @intCast(i);
    var donors: [24]Donor = undefined;
    for (&donors, 0..) |*donor, i| donor.* = .{
        .layer = @intCast(i),
        .heads = 2,
        .dim = if (i % 6 == 5) 512 else 256,
    };
    const request = Request{
        .source_sequence = 0,
        .destination_sequence = 0,
        .tokens = 32768,
        .capacity = 65536,
        .page_size = 16,
        .source_blocks = &blocks,
        .destination_blocks = &blocks,
        .donors = &donors,
        .max_bytes = 4 * 1024 * 1024 * 1024,
    };
    try std.testing.expectEqual(@as(usize, 3758489600), try request.allocationBytes(2));
    try std.testing.expectError(error.DeviceKvCloneBudgetExceeded, request.allocationBytes(4));
}
