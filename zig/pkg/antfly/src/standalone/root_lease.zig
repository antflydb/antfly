// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Process ownership of a native standalone replica root, independent of the
//! chosen catalog path. The permanent lock inode is never truncated or removed.
const std = @import("std");

const file_name = ".antfly-native-owner.lock";

pub const Lease = struct {
    dir: std.Io.Dir,
    file: std.Io.File,

    pub fn acquire(io: std.Io, path: []const u8) !Lease {
        try std.Io.Dir.cwd().createDirPath(io, path);
        const dir = try std.Io.Dir.cwd().openDir(io, path, .{});
        errdefer dir.close(io);
        // Materialize separately from locking: Darwin's atomic open-and-lock
        // can report ENOENT when two O_CREAT callers race on the first open.
        if (dir.createFile(io, file_name, .{ .exclusive = true, .permissions = .fromMode(0o600) })) |seed| {
            seed.close(io);
        } else |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        }
        const file = dir.openFile(io, file_name, .{
            .mode = .read_write,
            .allow_directory = false,
            .follow_symlinks = false,
            .lock = .exclusive,
            .lock_nonblocking = true,
        }) catch |err| switch (err) {
            error.WouldBlock => return error.StandaloneRootInUse,
            else => return err,
        };
        errdefer file.close(io);
        const lease: Lease = .{ .dir = dir, .file = file };
        try lease.check(io);
        return lease;
    }

    /// Detect an unlinked/replaced sentinel rather than treating a still-open
    /// descriptor as ownership of the new pathname. Physical-root identity is
    /// checked separately by the native binding and signed receipt machinery.
    pub fn check(self: Lease, io: std.Io) !void {
        const held = try self.file.stat(io);
        const named = self.dir.statFile(io, file_name, .{ .follow_symlinks = false }) catch |err| switch (err) {
            error.FileNotFound => return error.StandaloneRootLeaseChanged,
            else => return err,
        };
        if (held.kind != .file or named.kind != .file or held.nlink != 1 or
            held.inode != named.inode) return error.StandaloneRootLeaseChanged;
    }

    pub fn deinit(self: *Lease, io: std.Io) void {
        self.file.close(io);
        self.dir.close(io);
        self.* = undefined;
    }
};

test "standalone physical root lease excludes aliases and is reacquired without unlinking" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/owner", .{tmp.sub_path});
    defer a.free(path);
    const alias = try std.fmt.allocPrint(a, "{s}/.", .{path});
    defer a.free(alias);
    var lease = try Lease.acquire(io, path);
    var open = true;
    defer if (open) lease.deinit(io);
    try lease.check(io);
    const original = try lease.file.stat(io);
    try std.testing.expectError(error.StandaloneRootInUse, Lease.acquire(io, alias));
    lease.deinit(io);
    open = false;
    var reopened = try Lease.acquire(io, alias);
    defer reopened.deinit(io);
    try reopened.check(io);
    try std.testing.expectEqual(original.inode, (try reopened.file.stat(io)).inode);
}

test "standalone physical root lease detects sentinel replacement and rejects symlinks" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/owner", .{tmp.sub_path});
    defer a.free(path);
    var lease = try Lease.acquire(io, path);
    defer lease.deinit(io);
    // Rename only this isolated fixture's sentinel; no production cleanup ever
    // unlinks the inode on which other processes may be waiting.
    try std.Io.Dir.rename(lease.dir, file_name, lease.dir, "old-owner.lock", io);
    try std.testing.expectError(error.StandaloneRootLeaseChanged, lease.check(io));
    try lease.dir.symLink(io, "old-owner.lock", file_name, .{});
    try std.testing.expectError(error.StandaloneRootLeaseChanged, lease.check(io));
    try std.testing.expectError(error.SymLinkLoop, Lease.acquire(io, path));
}
