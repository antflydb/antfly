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

/// How hard `syncData` pushes a durability barrier past the OS page cache.
pub const DurabilityPolicy = enum {
    /// The platform's cheapest barrier that still guarantees the data (and
    /// the minimum metadata needed to retrieve it, such as file length) a
    /// caller already wrote through this handle is recoverable after a
    /// crash: `fdatasync` on Linux, plain `fsync` elsewhere.
    default,
    /// Also asks the drive to flush its own write cache to persistent media
    /// where the OS barrier alone does not (`F_FULLFSYNC` on Darwin). A
    /// no-op upgrade over `default` on platforms without that distinction.
    full_fsync,
};

pub const SyncDataError = std.posix.SyncError;

/// Raw `fsync(2)`, outside `std.Io`'s vtable (which only exposes the more
/// expensive barrier the host scheduler picked for the file's backend).
/// Mirrors `std.posix.fdatasync`'s error handling.
fn posixFsync(handle: std.posix.fd_t) SyncDataError!void {
    while (true) {
        switch (std.posix.errno(std.posix.system.fsync(handle))) {
            .SUCCESS => return,
            .INTR => continue,
            .IO => return error.InputOutput,
            .NOSPC => return error.NoSpaceLeft,
            .DQUOT => return error.DiskQuota,
            .BADF, .INVAL, .ROFS => unreachable,
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

/// A durability barrier for file data, cheaper than a full `fsync` where the
/// platform offers one (`fdatasync` skips metadata not needed to retrieve
/// the data, such as mtime). Operates on the raw OS handle directly, so it
/// is a leaf primitive outside the `std.Io` scheduler: call it only from a
/// context that already tolerates blocking on this thread until the write
/// is durable (a commit protocol that already serializes writers, not a
/// cooperatively scheduled fiber that must keep making progress).
pub fn syncData(handle: std.posix.fd_t, policy: DurabilityPolicy) SyncDataError!void {
    switch (builtin.os.tag) {
        // fdatasync skips flushing file metadata that is not needed to
        // retrieve the data (e.g. mtime), but Linux still flushes metadata
        // a size-changing write needs for correct retrieval (the inode's
        // block mapping and length), so growth is safe without separately
        // preallocating file extents.
        .linux => try std.posix.fdatasync(handle),
        .macos, .ios, .tvos, .watchos, .visionos => {
            try posixFsync(handle);
            if (policy == .full_fsync) {
                // Best-effort: on filesystems that don't support it (or
                // under a sandbox that denies the fcntl), the plain fsync
                // above already ran; this only asks the drive to also
                // flush its own write cache. A failure here does not
                // revoke the barrier already established by fsync.
                _ = std.c.fcntl(handle, std.c.F.FULLFSYNC, @as(c_int, 0));
            }
        },
        else => try posixFsync(handle),
    }
}

test "syncData persists a write through a plain file handle" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(std.testing.io, "sync-data.bin", .{ .read = true });
    defer file.close(std.testing.io);
    try file.writePositionalAll(std.testing.io, "durable", 0);
    try syncData(file.handle, .default);
    try std.testing.expectEqual(@as(u64, "durable".len), (try file.stat(std.testing.io)).size);
}

test "syncData accepts the full_fsync durability policy" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(std.testing.io, "sync-data-full.bin", .{ .read = true });
    defer file.close(std.testing.io);
    try file.writePositionalAll(std.testing.io, "durable", 0);
    try syncData(file.handle, .full_fsync);
}
