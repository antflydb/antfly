// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const abi = @import("antfly_executor_abi");

extern fn exerciseBorrowedExecutor(borrow: *const abi.Borrow) callconv(.c) bool;

fn warmWorker() void {}

test "borrowed executor wakes idle owning workers across archives" {
    var pool = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer pool.deinit();
    const io = pool.io();
    var warm = try io.concurrent(warmWorker, .{});
    warm.await(io);
    const borrow = abi.Borrow.init(&io);
    try std.testing.expect(exerciseBorrowedExecutor(&borrow));
}
