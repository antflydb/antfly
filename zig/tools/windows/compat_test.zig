// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Run with the experimental std overlay and a Windows target.
const std = @import("std");
const builtin = @import("builtin");

test "Windows loopback sockets listen connect accept and transfer bytes" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = std.testing.io;
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var server = try address.listen(io, .{ .reuse_address = true });
    defer server.deinit(io);
    const client = try server.socket.address.connect(io, .{ .mode = .stream });
    defer client.close(io);
    const peer = try server.accept(io);
    defer peer.close(io);
    var writer = client.writer(io, &.{});
    try writer.interface.writeAll("ping");
    try writer.interface.flush();
    var reader = peer.reader(io, &.{});
    var bytes: [4]u8 = undefined;
    try reader.interface.readSliceAll(&bytes);
    try std.testing.expectEqualStrings("ping", &bytes);
    try client.shutdown(io, .send);
    var eof: [1]u8 = undefined;
    try std.testing.expectError(error.EndOfStream, reader.interface.readSliceAll(&eof));
}

test "Windows secure entropy fills independent buffers" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    var first: [64]u8 = @splat(0);
    var second: [64]u8 = @splat(0);
    try std.testing.io.randomSecure(&first);
    try std.testing.io.randomSecure(&second);
    try std.testing.expect(!std.mem.eql(u8, &first, &second));
    try std.testing.expect(!std.mem.allEqual(u8, &first, 0));
    try std.testing.io.randomSecure(&.{});
}

test "Windows file locks exclude competing handles and allow header reads" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = std.testing.io;
    var directory = std.testing.tmpDir(.{});
    defer directory.cleanup();
    const options: std.Io.Dir.CreateFileOptions = .{ .read = true, .truncate = false, .lock = .exclusive, .lock_nonblocking = true };
    const writer = try directory.dir.createFile(io, "locked.bin", options);
    defer writer.close(io);
    try writer.writeStreamingAll(io, "header");
    try std.testing.expectError(error.WouldBlock, directory.dir.createFile(io, "locked.bin", options));
    try std.testing.expectError(error.WouldBlock, directory.dir.openFile(io, "locked.bin", .{ .lock = .exclusive, .lock_nonblocking = true }));
    const reader = try directory.dir.openFile(io, "locked.bin", .{});
    defer reader.close(io);
    var bytes: [6]u8 = undefined;
    try std.testing.expectEqual(@as(isize, 6), std.c.pread(reader.handle, &bytes, bytes.len, 0));
    try std.testing.expectEqualStrings("header", &bytes);
    writer.unlock(io);
    const next = try directory.dir.createFile(io, "locked.bin", options);
    defer next.close(io);
    try next.downgradeLock(io);
    const shared = try directory.dir.openFile(io, "locked.bin", .{ .lock = .shared, .lock_nonblocking = true });
    defer shared.close(io);
    try std.testing.expect(try reader.tryLock(io, .shared));
    try std.testing.expect(!try writer.tryLock(io, .exclusive));
    reader.unlock(io);
    shared.unlock(io);
    next.unlock(io);
    try writer.lock(io, .exclusive);
    writer.unlock(io);
}

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
