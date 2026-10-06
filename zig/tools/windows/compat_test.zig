// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Run with the experimental std overlay and a Windows target.
const std = @import("std");
const builtin = @import("builtin");

test "Windows shim positional reads do not depend on the current offset" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = std.testing.io;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const file = try directory.dir.createFile(io, "read.bin", .{ .read = true });
    defer file.close(io);
    try file.writeStreamingAll(io, "abcdefgh");
    var bytes: [3]u8 = undefined;
    try std.testing.expectEqual(@as(isize, 3), std.c.pread(file.handle, &bytes, bytes.len, 2));
    try std.testing.expectEqualStrings("cde", &bytes);
    try std.testing.expectEqual(@as(isize, 3), std.c.pread(file.handle, &bytes, bytes.len, 0));
    try std.testing.expectEqualStrings("abc", &bytes);
    try std.testing.expectEqual(@as(isize, 0), std.c.pread(file.handle, &bytes, bytes.len, 8));
}

test "Windows shim clocks advance and condition timeout preserves the mutex" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    var before: std.c.timespec = undefined;
    var after: std.c.timespec = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.clock_gettime(.MONOTONIC, &before));
    const sleep: std.c.timespec = .{ .sec = 0, .nsec = 2 * std.time.ns_per_ms };
    try std.testing.expectEqual(@as(c_int, 0), std.c.nanosleep(&sleep, null));
    try std.testing.expectEqual(@as(c_int, 0), std.c.clock_gettime(.MONOTONIC, &after));
    try std.testing.expect(after.sec > before.sec or (after.sec == before.sec and after.nsec > before.nsec));
    var mutex: std.c.pthread_mutex_t = .{};
    var condition: std.c.pthread_cond_t = .{};
    try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_mutex_lock(&mutex));
    try std.testing.expectEqual(std.c.E.BUSY, std.c.pthread_mutex_trylock(&mutex));
    var deadline: std.c.timespec = undefined;
    try std.testing.expectEqual(@as(c_int, 0), std.c.clock_gettime(.REALTIME, &deadline));
    deadline.sec += 1;
    try std.testing.expectEqual(std.c.E.TIMEDOUT, std.c.pthread_cond_timedwait(&condition, &mutex, &deadline));
    try std.testing.expectEqual(std.c.E.BUSY, std.c.pthread_mutex_trylock(&mutex));
    try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_mutex_unlock(&mutex));
    try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_mutex_trylock(&mutex));
    try std.testing.expectEqual(std.c.E.SUCCESS, std.c.pthread_mutex_unlock(&mutex));
}
