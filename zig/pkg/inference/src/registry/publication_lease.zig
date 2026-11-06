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

//! Coordinate pathname-based construction with directory swaps and rollback.
//! Lock the stable parent directory, never the model inode that is renamed.
//! Shared leases require no write permission and never copy model weights.
const std = @import("std");

pub const Lease = struct {
    directory: ?std.Io.Dir = null,

    fn file(dir: std.Io.Dir) std.Io.File {
        return .{ .handle = dir.handle, .flags = .{ .nonblocking = false } };
    }

    /// GCS snapshots are immutable after COMPLETE and may not support flock.
    /// Their existing publication contract supplies the same lifetime guarantee.
    fn immutableSnapshot(a: std.mem.Allocator, io: std.Io, model: []const u8) !bool {
        var current: ?[]const u8 = model;
        while (current) |path| {
            const marker = try std.fs.path.join(a, &.{ path, "COMPLETE" });
            defer a.free(marker);
            const bytes = std.Io.Dir.cwd().readFileAlloc(io, marker, a, .limited(4096)) catch |err| switch (err) {
                error.FileNotFound, error.NotDir => {
                    current = std.fs.path.dirname(path);
                    continue;
                },
                else => return err,
            };
            defer a.free(bytes);
            if (std.mem.indexOf(u8, bytes, "CATALOG.json") != null and std.mem.indexOf(u8, bytes, "SHA256SUMS") != null) return true;
            return error.IncompleteManagedDownload;
        }
        return false;
    }

    pub fn shared(a: std.mem.Allocator, io: std.Io, model: []const u8) !Lease {
        if (try immutableSnapshot(a, io, model)) return .{};
        const dir = try std.Io.Dir.cwd().openDir(io, std.fs.path.dirname(model) orelse ".", .{ .iterate = true });
        errdefer dir.close(io);
        // Fail promptly if a publisher owns the namespace; the caller returns
        // a retryable publication error rather than losing its request deadline.
        if (!try file(dir).tryLock(io, .shared)) return error.ModelArtifactsChanging;
        return .{ .directory = dir };
    }

    pub fn assertMutable(a: std.mem.Allocator, io: std.Io, model: []const u8) !void {
        if (try immutableSnapshot(a, io, model)) return error.ReadOnlyFileSystem;
    }

    pub fn exclusive(a: std.mem.Allocator, io: std.Io, model: []const u8) !Lease {
        try assertMutable(a, io, model);
        const dir = try std.Io.Dir.cwd().openDir(io, std.fs.path.dirname(model) orelse ".", .{ .iterate = true });
        errdefer dir.close(io);
        try file(dir).lock(io, .exclusive);
        return .{ .directory = dir };
    }

    pub fn deinit(self: *Lease, io: std.Io) void {
        if (self.directory) |dir| {
            file(dir).unlock(io);
            dir.close(io);
            self.directory = null;
        }
    }
};

test "model selection publication lease fences replacement and rollback without serializing readers" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "model");
    const model = try tmp.dir.realPathFileAlloc(io, "model", a);
    defer a.free(model);
    var first = try Lease.shared(a, io, model);
    defer first.deinit(io);
    var second = try Lease.shared(a, io, model);
    second.deinit(io);
    const writer = try std.Io.Dir.cwd().openDir(io, std.fs.path.dirname(model).?, .{ .iterate = true });
    defer writer.close(io);
    try std.testing.expect(!try Lease.file(writer).tryLock(io, .exclusive));
    first.deinit(io);
    try std.testing.expect(try Lease.file(writer).tryLock(io, .exclusive));
    defer Lease.file(writer).unlock(io);
    try std.testing.expectError(error.ModelArtifactsChanging, Lease.shared(a, io, model));
}

test "model selection publication lease accepts immutable read-only snapshot contract" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "snapshot/readers/model");
    try tmp.dir.writeFile(io, .{ .sub_path = "snapshot/COMPLETE", .data = "CATALOG.json SHA256SUMS" });
    const model = try tmp.dir.realPathFileAlloc(io, "snapshot/readers/model", a);
    defer a.free(model);
    var lease = try Lease.shared(a, io, model);
    defer lease.deinit(io);
    try std.testing.expect(lease.directory == null);
    try std.testing.expectError(error.ReadOnlyFileSystem, Lease.exclusive(a, io, model));
}
