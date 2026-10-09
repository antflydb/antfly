// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Borrow caller-owned memory for one synchronous transaction capture.
//! All temporary records and response bytes use this allocator; the caller
//! releases response bytes before the allocator borrow expires.
const std = @import("std");
const memory = @import("runtime_memory_abi");

pub fn receive(raw: ?*const anyopaque) !std.mem.Allocator {
    const pointer = raw orelse return error.InvalidArgument;
    if (@intFromPtr(pointer) % @alignOf(memory.Allocator) != 0) return error.InvalidArgument;
    const borrowed: *const memory.Allocator = @ptrCast(@alignCast(pointer));
    if (!borrowed.valid()) return error.UnsupportedVersion;
    return borrowed.asStd();
}

/// Keep response construction on the borrowed allocator too. The producer
/// must release it on failure or transfer the exact byte slice to the caller.
pub fn encode(allocator: std.mem.Allocator, value: anytype) ![]u8 {
    return try std.json.Stringify.valueAlloc(allocator, value, .{});
}

test "transaction routing control port memory rejects absent or incompatible allocators" {
    try std.testing.expectError(error.InvalidArgument, receive(null));
    var allocator = std.testing.allocator;
    var borrowed = memory.Allocator.fromStd(&allocator);
    borrowed.version +%= 1;
    try std.testing.expectError(error.UnsupportedVersion, receive(&borrowed));
    borrowed.version = memory.Allocator.abi_version;
    borrowed._reserved = 1;
    try std.testing.expectError(error.UnsupportedVersion, receive(&borrowed));
}

test "transaction routing control port memory honors caller capacity and ownership" {
    var bytes: [64]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&bytes);
    var allocator = bounded.allocator();
    const borrowed = memory.Allocator.fromStd(&allocator);
    const foreign = try receive(&borrowed);
    const retained = try foreign.alloc(u8, 48);
    try std.testing.expect(@intFromPtr(retained.ptr) >= @intFromPtr(&bytes));
    try std.testing.expect(@intFromPtr(retained.ptr) + retained.len <= @intFromPtr(&bytes) + bytes.len);
    try std.testing.expectError(error.OutOfMemory, foreign.alloc(u8, 32));
    // Returned response bytes are explicitly caller-owned: no storage owner
    // buffer destructor participates in their release.
    allocator.free(retained);
    try std.testing.expectEqual(@as(usize, 0), bounded.end_index);
    const next = try foreign.alloc(u8, 64);
    foreign.free(next);
    try std.testing.expectEqual(@as(usize, 0), bounded.end_index);
}

test "transaction routing control port memory cleans up response allocation failures" {
    const Check = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var owner = allocator;
            const borrowed = memory.Allocator.fromStd(&owner);
            const foreign = try receive(&borrowed);
            const response = try encode(foreign, .{ .tables = &.{"rows"}, .ranges = &.{ "", "m", "z" }, .description = "a response large enough to grow its allocation several times" });
            defer allocator.free(response);
            try std.testing.expect(std.mem.indexOf(u8, response, "rows") != null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}
