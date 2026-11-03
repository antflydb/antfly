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

//! Nonblocking open-file-description range locks. Ordinary process-scoped
//! fcntl locks are unsuitable: closing an unrelated descriptor can release
//! them, and two independent owners in one process must still exclude each
//! other. Callers supply cancellable std.Io waits between failed attempts.
const std = @import("std");
const builtin = @import("builtin");

pub const supported = builtin.link_libc and switch (builtin.os.tag) {
    .linux, .macos, .freebsd => true,
    else => false,
};
pub const Kind = enum { shared, exclusive };

fn invoke(file: std.Io.File, command: c_int, request: *std.c.Flock) std.c.E {
    // Linux's native wrapper selects fcntl64 where required, preserving the
    // kernel Flock layout on 32-bit targets without libc large-file ambiguity.
    if (builtin.os.tag == .linux)
        return std.os.linux.errno(std.os.linux.fcntl(file.handle, command, @intFromPtr(request)));
    return std.c.errno(std.c.fcntl(file.handle, command, request));
}

fn requestFor(offset: u64, length: u64, kind: ?Kind) !std.c.Flock {
    if (length == 0) return error.InvalidFileRange;
    const end = try std.math.add(u64, offset, length);
    _ = std.math.cast(std.c.off_t, end) orelse return error.InvalidFileRange;
    var request = std.mem.zeroes(std.c.Flock);
    request.start = @intCast(offset);
    request.len = @intCast(length);
    request.whence = 0; // SEEK_SET, relative to the beginning of the file.
    request.type = if (kind) |value| switch (value) {
        .shared => std.c.F.RDLCK,
        .exclusive => std.c.F.WRLCK,
    } else std.c.F.UNLCK;
    return request;
}

fn change(file: std.Io.File, offset: u64, length: u64, kind: ?Kind) !bool {
    if (comptime !supported) return error.FileLocksUnsupported;
    var request = try requestFor(offset, length, kind);
    // Darwin's published ABI defines F_OFD_SETLK=90; the pinned Zig c.F
    // exposes Flock but omits that command. Linux and FreeBSD expose it.
    const command: c_int = if (builtin.os.tag == .macos) 90 else std.c.F.OFD_SETLK;
    return switch (invoke(file, command, &request)) {
        .SUCCESS => true,
        .AGAIN, .ACCES, .INTR => false,
        .INVAL, .NOSYS => error.FileLocksUnsupported,
        .NOLCK => error.SystemResources,
        else => error.FileRangeLockFailed,
    };
}

/// Query another description's ownership without temporarily acquiring its
/// range. Probing with a shared lock could keep a dead predecessor apparently
/// alive while multiple successors repeatedly inspect it.
pub fn isLocked(file: std.Io.File, offset: u64, length: u64) !bool {
    if (comptime !supported) return error.FileLocksUnsupported;
    var request = try requestFor(offset, length, .exclusive);
    const command: c_int = if (builtin.os.tag == .macos) 92 else std.c.F.OFD_GETLK;
    switch (invoke(file, command, &request)) {
        .SUCCESS => return request.type != std.c.F.UNLCK,
        .INVAL, .NOSYS => return error.FileLocksUnsupported,
        .NOLCK => return error.SystemResources,
        else => return error.FileRangeLockFailed,
    }
}

pub fn tryLock(file: std.Io.File, offset: u64, length: u64, kind: Kind) !bool {
    return change(file, offset, length, kind);
}

pub fn unlock(file: std.Io.File, offset: u64, length: u64) !void {
    if (!try change(file, offset, length, null)) return error.FileRangeLockFailed;
}

test "file range locks exclude independent descriptions and survive unrelated close" {
    if (!supported) return error.SkipZigTest;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const first = try tmp.dir.createFile(io, "ranges", .{ .read = true });
    var first_open = true;
    defer if (first_open) first.close(io);
    const second = try tmp.dir.openFile(io, "ranges", .{ .mode = .read_write });
    defer second.close(io);
    try std.testing.expect(try tryLock(first, 10, 1, .exclusive));
    try std.testing.expect(!try tryLock(second, 10, 1, .exclusive));
    try std.testing.expect(try isLocked(second, 10, 1));
    try std.testing.expect(!try isLocked(first, 10, 1));
    const unrelated = try tmp.dir.openFile(io, "ranges", .{ .mode = .read_write });
    unrelated.close(io);
    try std.testing.expect(!try tryLock(second, 10, 1, .exclusive));
    try std.testing.expect(try tryLock(second, 11, 1, .exclusive));
    first.close(io);
    first_open = false;
    try std.testing.expect(!try isLocked(second, 10, 1));
    try std.testing.expect(try tryLock(second, 10, 1, .exclusive));
    try std.testing.expectEqual(@as(u64, 0), (try second.stat(io)).size);
}

test "file range locks permit shared cohorts and explicit release" {
    if (!supported) return error.SkipZigTest;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const first = try tmp.dir.createFile(io, "ranges", .{ .read = true });
    defer first.close(io);
    const second = try tmp.dir.openFile(io, "ranges", .{ .mode = .read_write });
    defer second.close(io);
    const third = try tmp.dir.openFile(io, "ranges", .{ .mode = .read_write });
    defer third.close(io);
    try std.testing.expect(try tryLock(first, 16384, 1, .shared));
    try std.testing.expect(try tryLock(second, 16384, 1, .shared));
    try std.testing.expect(!try tryLock(third, 16384, 1, .exclusive));
    try unlock(first, 16384, 1);
    try std.testing.expect(!try tryLock(third, 16384, 1, .exclusive));
    try unlock(second, 16384, 1);
    try std.testing.expect(try tryLock(third, 16384, 1, .exclusive));
    try std.testing.expectEqual(@as(u64, 0), (try third.stat(io)).size);
}
