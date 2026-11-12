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
const builtin = @import("builtin");

pub const capacity_supported = builtin.link_libc and switch (builtin.os.tag) {
    .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => true,
    else => false,
};

extern fn antfly_platform_filesystem_capacity(
    path: [*:0]const u8,
    fragment_bytes: *u64,
    blocks: *u64,
    available_blocks: *u64,
) c_int;

pub const Capacity = struct {
    total_bytes: u64,
    available_bytes: u64,
};

/// Return capacity available to an unprivileged writer on the filesystem that
/// contains `path`. Saturating overflow handling keeps the observation safe on
/// unusual filesystems that report sentinel block counts.
pub fn capacity(path: []const u8) !Capacity {
    if (!capacity_supported) return error.UnsupportedPlatform;
    if (path.len > std.fs.max_path_bytes) return error.NameTooLong;

    var path_buf: [std.fs.max_path_bytes + 1]u8 = undefined;
    @memcpy(path_buf[0..path.len], path);
    path_buf[path.len] = 0;

    var fragment_bytes: u64 = undefined;
    var blocks: u64 = undefined;
    var available_blocks: u64 = undefined;
    if (antfly_platform_filesystem_capacity(
        @ptrCast(&path_buf),
        &fragment_bytes,
        &blocks,
        &available_blocks,
    ) != 0) return error.CapacityProbeFailed;

    return .{
        .total_bytes = std.math.mul(u64, fragment_bytes, blocks) catch std.math.maxInt(u64),
        .available_bytes = std.math.mul(u64, fragment_bytes, available_blocks) catch std.math.maxInt(u64),
    };
}

test "filesystem capacity reports the test volume" {
    if (!capacity_supported) return error.SkipZigTest;
    const observation = try capacity(".");
    try std.testing.expect(observation.total_bytes > 0);
    try std.testing.expect(observation.available_bytes <= observation.total_bytes);
}

/// Open without waiting for a FIFO writer, then validate the descriptor that
/// will actually be read. Path metadata checks alone have a replacement race.
/// Callers must supervise blocking filesystem syscalls when imposing deadlines.
pub fn openRegularFile(io: std.Io, path: []const u8) !std.Io.File {
    if (!std.fs.path.isAbsolute(path)) return error.BadPathName;
    if (std.mem.indexOfScalar(u8, path, 0) != null) return error.BadPathName;
    try io.checkCancel();
    const file: std.Io.File = if (comptime switch (builtin.os.tag) {
        .linux, .macos, .freebsd, .netbsd, .openbsd, .dragonfly, .illumos => true,
        else => false,
    }) opened: {
        var flags: std.posix.O = .{ .ACCMODE = .RDONLY, .NONBLOCK = true };
        if (@hasField(std.posix.O, "CLOEXEC")) flags.CLOEXEC = true;
        if (@hasField(std.posix.O, "LARGEFILE")) flags.LARGEFILE = true;
        const fd = try std.posix.openat(std.posix.AT.FDCWD, path, flags, 0);
        break :opened .{ .handle = fd, .flags = .{ .nonblocking = true } };
    } else try std.Io.Dir.openFileAbsolute(io, path, .{ .allow_directory = false });
    errdefer file.close(io);
    if ((try file.stat(io)).kind != .file) return error.NotRegularFile;
    return file;
}

test "regular file opens reject FIFOs without waiting for a writer" {
    if (comptime !builtin.link_libc or (builtin.os.tag != .linux and builtin.os.tag != .macos)) return error.SkipZigTest;
    const Posix = struct {
        extern "c" fn mkfifo([*:0]const u8, std.c.mode_t) c_int;
    };
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const path = try std.fmt.allocPrintSentinel(a, "{s}/pipe", .{root}, 0);
    defer a.free(path);
    try std.testing.expectEqual(@as(c_int, 0), Posix.mkfifo(path, 0o600));
    try std.testing.expectError(error.NotRegularFile, openRegularFile(io, path));
    try tmp.dir.symLink(io, "pipe", "link", .{});
    const link = try std.fmt.allocPrint(a, "{s}/link", .{root});
    defer a.free(link);
    try std.testing.expectError(error.NotRegularFile, openRegularFile(io, link));
}
