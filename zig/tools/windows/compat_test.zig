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

//! Run with stock Zig, the antfly_platform module, and a Windows target.
const platform = @import("antfly_platform");
const std = @import("std");

const builtin = @import("builtin");

test "Windows loopback sockets listen connect accept and transfer bytes" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = platform.testing.io;
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

fn idleRead(io: std.Io, peer: std.Io.net.Stream, started: *std.Io.Event) std.Io.net.Stream.Reader.Error!usize {
    var byte: [1]u8 = undefined;
    started.set(io);
    var buffers = [_][]u8{&byte};
    return (try peer.readWithControl(io, &buffers, &.{})).data_len;
}

test "Windows canceling idle socket reads drains the operation and preserves the socket" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = platform.testing.io;
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var server = try address.listen(io, .{});
    defer server.deinit(io);
    const client = try server.socket.address.connect(io, .{ .mode = .stream });
    defer client.close(io);
    const peer = try server.accept(io);
    defer peer.close(io);
    for (0..16) |iteration| {
        var started: std.Io.Event = .unset;
        var future = try io.concurrent(idleRead, .{ io, peer, &started });
        defer _ = future.cancel(io) catch {};
        try started.wait(io);
        // Exercise both cancellation at submission and an already pending read.
        if (iteration % 2 == 0) try io.sleep(.fromMilliseconds(20), .awake);
        try std.testing.expectError(error.Canceled, future.cancel(io));
        // The canceled request must be drained: it cannot consume this byte or
        // keep references to the worker's stack after future.cancel returns.
        var writer = client.writer(io, &.{});
        try writer.interface.writeAll("x");
        try writer.interface.flush();
        var byte: [1]u8 = undefined;
        var buffers = [_][]u8{&byte};
        try std.testing.expectEqual(@as(usize, 1), (try peer.readWithControl(io, &buffers, &.{})).data_len);
        try std.testing.expectEqual(@as(u8, 'x'), byte[0]);
    }
}

fn fillSocket(io: std.Io, client: std.Io.net.Stream, started: *std.Io.Event) std.Io.net.Stream.Writer.Error!void {
    var bytes: [64 * 1024]u8 = @splat('x');
    started.set(io);
    while (true) {
        _ = try (try io.operate(.{ .net_write = .{
            .socket_handle = client.socket.handle,
            .header = &.{},
            .data = &.{&bytes},
            .splat = 1,
            .control = &.{},
        } })).net_write;
    }
}

test "Windows socket deadlines and backpressured write cancellation complete" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = platform.testing.io;
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var server = try address.listen(io, .{});
    defer server.deinit(io);
    const client = try server.socket.address.connect(io, .{ .mode = .stream });
    defer client.close(io);
    const peer = try server.accept(io);
    defer peer.close(io);
    // Threaded implements deadlines at the task level; Windows Batch socket
    // concurrency is not supported. The losing read must finish cancellation
    // before the select and its stack storage are released.
    const Result = union(enum) { read: std.Io.net.Stream.Reader.Error!usize, timeout: std.Io.Cancelable!void };
    var results: [2]Result = undefined;
    var selection: std.Io.Select(Result) = .init(io, &results);
    defer selection.cancelDiscard();
    var reading: std.Io.Event = .unset;
    try selection.concurrent(.read, idleRead, .{ io, peer, &reading });
    try reading.wait(io);
    try selection.concurrent(.timeout, std.Io.sleep, .{ io, .fromMilliseconds(20), .awake });
    switch (try selection.await()) {
        .timeout => |result| try result,
        .read => return error.ReadCompletedBeforeDeadline,
    }
    selection.cancelDiscard();
    var started: std.Io.Event = .unset;
    var future = try io.concurrent(fillSocket, .{ io, client, &started });
    defer future.cancel(io) catch {};
    try started.wait(io);
    try io.sleep(.fromMilliseconds(100), .awake);
    try std.testing.expectError(error.Canceled, future.cancel(io));
}

test "Windows secure entropy fills independent buffers" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    var first: [64]u8 = @splat(0);
    var second: [64]u8 = @splat(0);
    try platform.testing.io.randomSecure(&first);
    try platform.testing.io.randomSecure(&second);
    try std.testing.expect(!std.mem.eql(u8, &first, &second));
    try std.testing.expect(!std.mem.allEqual(u8, &first, 0));
    try platform.testing.io.randomSecure(&.{});
}

test "Windows file locks exclude competing handles and allow header reads" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = platform.testing.io;
    var directory = platform.testing.tmpDir(.{});
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
    try std.testing.expectEqual(@as(isize, 6), platform.c.pread(reader.handle, &bytes, bytes.len, 0));
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
    const io = platform.testing.io;
    var directory = platform.testing.tmpDir(.{});
    defer directory.cleanup();
    const file = try directory.dir.createFile(io, "read.bin", .{ .read = true });
    defer file.close(io);
    try file.writeStreamingAll(io, "abcdefgh");
    try io.vtable.fileSeekTo(io.userdata, file, 6);
    var bytes: [3]u8 = undefined;
    try std.testing.expectEqual(@as(isize, 3), platform.c.pread(file.handle, &bytes, bytes.len, 2));
    try std.testing.expectEqualStrings("cde", &bytes);
    try std.testing.expectEqual(@as(isize, 3), platform.c.pread(file.handle, &bytes, bytes.len, 0));
    try std.testing.expectEqualStrings("abc", &bytes);
    try std.testing.expectEqual(@as(isize, 0), platform.c.pread(file.handle, &bytes, bytes.len, 8));
    var iosb: std.os.windows.IO_STATUS_BLOCK = undefined;
    var position: std.os.windows.FILE.POSITION_INFORMATION = undefined;
    try std.testing.expectEqual(std.os.windows.NTSTATUS.SUCCESS, std.os.windows.ntdll.NtQueryInformationFile(file.handle, &iosb, &position, @sizeOf(@TypeOf(position)), .Position));
    try std.testing.expectEqual(@as(i64, 6), position.CurrentByteOffset);
}

test "Windows positional read failures replace stale errno and reject write-only handles" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = platform.testing.io;
    var directory = platform.testing.tmpDir(.{});
    defer directory.cleanup();
    const file = try directory.dir.createFile(io, "write-only.bin", .{});
    defer file.close(io);
    try file.writeStreamingAll(io, "abc");
    var byte: [1]u8 = undefined;
    for ([_]std.c.E{ .SUCCESS, .INTR }) |stale| {
        std.c._errno().* = @backingInt(stale);
        try std.testing.expectEqual(@as(isize, -1), platform.c.pread(file.handle, &byte, 1, 0));
        try std.testing.expectEqual(std.c.E.BADF, std.posix.errno(@as(isize, -1)));
    }
    std.c._errno().* = @backingInt(std.c.E.INTR);
    try std.testing.expectEqual(@as(isize, -1), platform.c.pread(file.handle, &byte, 1, -1));
    try std.testing.expectEqual(std.c.E.INVAL, std.posix.errno(@as(isize, -1)));
    std.c._errno().* = @backingInt(std.c.E.INTR);
    try std.testing.expectEqual(@as(isize, -1), platform.c.pread(std.os.windows.INVALID_HANDLE_VALUE, &byte, 1, 0));
    try std.testing.expectEqual(std.c.E.BADF, std.posix.errno(@as(isize, -1)));
}

fn positionalReads(fd: std.c.fd_t, offset: std.c.off_t, expected: []const u8) !void {
    var bytes: [3]u8 = undefined;
    for (0..32) |_| {
        try std.testing.expectEqual(@as(isize, 3), platform.c.pread(fd, &bytes, bytes.len, offset));
        try std.testing.expectEqualStrings(expected, &bytes);
    }
}

test "Windows concurrent positional reads preserve synchronous offsets and support retained overlapped handles" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = platform.testing.io;
    var directory = platform.testing.tmpDir(.{});
    defer directory.cleanup();
    const writer = try directory.dir.createFile(io, "parallel.bin", .{ .read = true });
    defer writer.close(io);
    try writer.writeStreamingAll(io, "abcdefgh");
    try io.vtable.fileSeekTo(io.userdata, writer, 6);
    const positional = try platform.filesystem.openPositionalReadOnly(io, directory.dir, "parallel.bin");
    defer positional.close(io);
    try std.testing.expect(positional.flags.nonblocking);
    for ([_]std.c.fd_t{ writer.handle, positional.handle }) |fd| {
        var first = try io.concurrent(positionalReads, .{ fd, 0, "abc" });
        defer _ = first.cancel(io) catch {};
        var second = try io.concurrent(positionalReads, .{ fd, 2, "cde" });
        defer _ = second.cancel(io) catch {};
        try first.await(io);
        try second.await(io);
        var bytes: [3]u8 = undefined;
        try std.testing.expectEqual(@as(isize, 0), platform.c.pread(fd, &bytes, bytes.len, 1 << 32));
        try std.testing.expectEqual(@as(isize, 0), platform.c.pread(fd, &bytes, 0, 0));
    }
    var iosb: std.os.windows.IO_STATUS_BLOCK = undefined;
    var position: std.os.windows.FILE.POSITION_INFORMATION = undefined;
    try std.testing.expectEqual(std.os.windows.NTSTATUS.SUCCESS, std.os.windows.ntdll.NtQueryInformationFile(writer.handle, &iosb, &position, @sizeOf(@TypeOf(position)), .Position));
    try std.testing.expectEqual(@as(i64, 6), position.CurrentByteOffset);
    // The same handle also works with the executor's typed positional API.
    var bytes: [3]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 3), try positional.readPositional(io, &.{&bytes}, 2));
    try std.testing.expectEqualStrings("cde", &bytes);
}

test "Windows shim clocks advance and condition timeout preserves the mutex" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    var before: platform.c.timespec = undefined;
    var after: platform.c.timespec = undefined;
    try std.testing.expectEqual(@as(c_int, 0), platform.c.clock_gettime(.MONOTONIC, &before));
    const sleep: platform.c.timespec = .{ .sec = 0, .nsec = 2 * std.time.ns_per_ms };
    try std.testing.expectEqual(@as(c_int, 0), platform.c.nanosleep(&sleep, null));
    try std.testing.expectEqual(@as(c_int, 0), platform.c.clock_gettime(.MONOTONIC, &after));
    try std.testing.expect(after.sec > before.sec or (after.sec == before.sec and after.nsec > before.nsec));
    var mutex: platform.c.pthread_mutex_t = .{};
    var condition: platform.c.pthread_cond_t = .{};
    try std.testing.expectEqual(std.c.E.SUCCESS, platform.c.pthread_mutex_lock(&mutex));
    try std.testing.expectEqual(std.c.E.BUSY, platform.c.pthread_mutex_trylock(&mutex));
    var deadline: platform.c.timespec = undefined;
    try std.testing.expectEqual(@as(c_int, 0), platform.c.clock_gettime(.REALTIME, &deadline));
    deadline.sec += 1;
    try std.testing.expectEqual(std.c.E.TIMEDOUT, platform.c.pthread_cond_timedwait(&condition, &mutex, &deadline));
    try std.testing.expectEqual(std.c.E.BUSY, platform.c.pthread_mutex_trylock(&mutex));
    try std.testing.expectEqual(std.c.E.SUCCESS, platform.c.pthread_mutex_unlock(&mutex));
    try std.testing.expectEqual(std.c.E.SUCCESS, platform.c.pthread_mutex_trylock(&mutex));
    try std.testing.expectEqual(std.c.E.SUCCESS, platform.c.pthread_mutex_unlock(&mutex));
}

fn idleAccept(io: std.Io, server: *std.Io.net.Server, started: *std.Io.Event) std.Io.net.Server.AcceptError!void {
    started.set(io);
    const peer = try server.accept(io);
    peer.close(io);
}

test "Windows canceling idle accepts drains requests and preserves the listener" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = platform.testing.io;
    const address = try std.Io.net.IpAddress.parse("127.0.0.1", 0);
    var server = try address.listen(io, .{});
    defer server.deinit(io);
    for (0..16) |iteration| {
        var started: std.Io.Event = .unset;
        var future = try io.concurrent(idleAccept, .{ io, &server, &started });
        defer future.cancel(io) catch {};
        try started.wait(io);
        if (iteration % 2 == 0) try io.sleep(.fromMilliseconds(20), .awake);
        try std.testing.expectError(error.Canceled, future.cancel(io));
        // A drained accept must not steal the next connection. Do not send
        // bytes before accepting: server-first protocols must also work.
        const client = try server.socket.address.connect(io, .{ .mode = .stream });
        defer client.close(io);
        const peer = try server.accept(io);
        defer peer.close(io);
        try std.testing.expectEqual(std.Io.net.IpAddress.Family.ip4, std.meta.activeTag(peer.socket.address));
        var writer = peer.writer(io, &.{});
        try writer.interface.writeAll("x");
        try writer.interface.flush();
        var reader = client.reader(io, &.{});
        var byte: [1]u8 = undefined;
        try reader.interface.readSliceAll(&byte);
        try std.testing.expectEqual(@as(u8, 'x'), byte[0]);
    }
    // Mirrors server shutdown: cancel/join the accept group before closing
    // the listener, without requiring a connection to wake it.
    var started: std.Io.Event = .unset;
    var tasks: std.Io.Group = .init;
    defer tasks.cancel(io);
    try tasks.concurrent(io, groupedIdleAccept, .{ io, &server, &started });
    try started.wait(io);
    try io.sleep(.fromMilliseconds(20), .awake);
    tasks.cancel(io);
}

fn groupedIdleAccept(io: std.Io, server: *std.Io.net.Server, started: *std.Io.Event) std.Io.Cancelable!void {
    idleAccept(io, server, started) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => std.debug.panic("unexpected accept failure: {t}", .{err}),
    };
}

fn outboundConnect(io: std.Io, address: std.Io.net.IpAddress, started: *std.Io.Event) std.Io.net.IpAddress.ConnectError!void {
    started.set(io);
    const peer = try address.connect(io, .{ .mode = .stream });
    peer.close(io);
}

test "Windows outbound connect deadlines cancel and join stalled requests" {
    if (builtin.os.tag != .windows) return error.SkipZigTest;
    const io = platform.testing.io;
    // RFC 5737 documentation address: normally leaves the TCP handshake
    // pending. Environments rejecting it immediately skip the pending case.
    const address = try std.Io.net.IpAddress.parse("192.0.2.1", 443);
    const Result = union(enum) { connect: std.Io.net.IpAddress.ConnectError!void, timeout: std.Io.Cancelable!void };
    var results: [2]Result = undefined;
    var selection: std.Io.Select(Result) = .init(io, &results);
    defer selection.cancelDiscard();
    var started: std.Io.Event = .unset;
    try selection.concurrent(.connect, outboundConnect, .{ io, address, &started });
    try started.wait(io);
    try selection.concurrent(.timeout, std.Io.sleep, .{ io, .fromMilliseconds(20), .awake });
    switch (try selection.await()) {
        .timeout => |result| try result,
        .connect => |result| {
            result catch |err| switch (err) {
                error.NetworkUnreachable, error.HostUnreachable, error.ConnectionRefused, error.Timeout, error.ConnectionResetByPeer, error.AccessDenied => return error.SkipZigTest,
                else => return err,
            };
            return error.SkipZigTest;
        },
    }
    selection.cancelDiscard();
    // Repeated direct cancellation also exercises cancellation near submission.
    for (0..8) |iteration| {
        started = .unset;
        var future = try io.concurrent(outboundConnect, .{ io, address, &started });
        defer future.cancel(io) catch {};
        try started.wait(io);
        if (iteration % 2 == 0) try io.sleep(.fromMilliseconds(20), .awake);
        try std.testing.expectError(error.Canceled, future.cancel(io));
    }
}
