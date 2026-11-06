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
    alias_directories: std.ArrayListUnmanaged(std.Io.Dir) = .empty,
    allocator: ?std.mem.Allocator = null,

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

    fn sharedParent(io: std.Io, model: []const u8) !std.Io.Dir {
        const dir = try std.Io.Dir.cwd().openDir(io, std.fs.path.dirname(model) orelse ".", .{ .iterate = true });
        errdefer dir.close(io);
        if (!try file(dir).tryLock(io, .shared)) return error.ModelArtifactsChanging;
        return dir;
    }

    pub fn shared(a: std.mem.Allocator, io: std.Io, model: []const u8) !Lease {
        if (try immutableSnapshot(a, io, model)) return .{};
        var lease = Lease{ .directory = try sharedParent(io, model), .allocator = a };
        errdefer lease.deinit(io);
        var current = try a.dupe(u8, model);
        defer a.free(current);
        var buffer: [std.fs.max_path_bytes]u8 = undefined;
        // Fence every link in a model alias chain, not only the final target.
        // Nonblocking shared acquisition avoids cycles between alias parents;
        // the bounded walk also rejects loops and unwinds all prior leases.
        for (0..40) |_| {
            const length = std.Io.Dir.cwd().readLink(io, current, &buffer) catch |err| switch (err) {
                error.NotLink, error.FileNotFound, error.NotDir => return lease,
                else => return err,
            };
            if (length == buffer.len) return error.NameTooLong;
            const target = if (std.fs.path.isAbsolute(buffer[0..length]))
                try a.dupe(u8, buffer[0..length])
            else
                try std.fs.path.join(a, &.{ std.fs.path.dirname(current) orelse ".", buffer[0..length] });
            a.free(current);
            current = target;
            if (try immutableSnapshot(a, io, current)) return lease;
            const dir = try sharedParent(io, current);
            lease.alias_directories.append(a, dir) catch |err| {
                file(dir).unlock(io);
                dir.close(io);
                return err;
            };
        }
        return error.SymLinkLoop;
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
        for (self.alias_directories.items) |dir| {
            file(dir).unlock(io);
            dir.close(io);
        }
        if (self.allocator) |a| self.alias_directories.deinit(a);
        self.alias_directories = .empty;
        if (self.directory) |dir| {
            file(dir).unlock(io);
            dir.close(io);
            self.directory = null;
        }
    }
};

/// Capability retained by a publisher across preparation and namespace mutation.
/// Borrow it in commit/recovery instead of recursively locking the same parent.
pub const WriteLease = struct {
    allocator: std.mem.Allocator,
    lease: Lease,
    destination: []const u8,

    pub fn acquire(a: std.mem.Allocator, io: std.Io, destination: []const u8) !WriteLease {
        var lease = try Lease.exclusive(a, io, destination);
        errdefer lease.deinit(io);
        return .{ .allocator = a, .lease = lease, .destination = try @import("managed_receipt.zig").resolveRequestedFilePath(a, io, destination) };
    }
    pub fn requireDestination(self: *const WriteLease, io: std.Io, destination: []const u8) !void {
        if (self.lease.directory == null) return error.ModelArtifactsChanging;
        const canonical = try @import("managed_receipt.zig").resolveRequestedFilePath(self.allocator, io, destination);
        defer self.allocator.free(canonical);
        if (!std.mem.eql(u8, canonical, self.destination)) return error.ModelArtifactsChanging;
    }
    pub fn deinit(self: *WriteLease, io: std.Io) void {
        if (self.lease.directory == null) return;
        self.lease.deinit(io);
        self.allocator.free(self.destination);
        self.destination = "";
    }
};

/// Called only immediately before an active installation can change.
pub const MutationJournal = struct {
    ptr: *anyopaque,
    begin_fn: *const fn (*anyopaque) anyerror!void,
    pub fn beforeMutation(self: MutationJournal) !void {
        try self.begin_fn(self.ptr);
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

test "model selection publication lease fences both model aliases and their installation parents" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "aliases");
    try tmp.dir.createDirPath(io, "middle");
    try tmp.dir.createDirPath(io, "installed/model");
    try tmp.dir.symLink(io, "../installed/model", "middle/model", .{ .is_directory = true });
    try tmp.dir.symLink(io, "../middle/model", "aliases/model", .{ .is_directory = true });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const alias = try std.fs.path.join(a, &.{ root, "aliases/model" });
    defer a.free(alias);
    var lease = try Lease.shared(a, io, alias);
    defer lease.deinit(io);
    try std.testing.expectEqual(@as(usize, 2), lease.alias_directories.items.len);
    for ([_][]const u8{ "aliases", "middle", "installed" }) |parent| {
        const dir = try tmp.dir.openDir(io, parent, .{ .iterate = true });
        defer dir.close(io);
        try std.testing.expect(!try Lease.file(dir).tryLock(io, .exclusive));
    }
    lease.deinit(io);
    for ([_][]const u8{ "aliases", "middle", "installed" }) |parent| {
        const dir = try tmp.dir.openDir(io, parent, .{ .iterate = true });
        defer dir.close(io);
        try std.testing.expect(try Lease.file(dir).tryLock(io, .exclusive));
        Lease.file(dir).unlock(io);
    }
}

test "model selection alias lease contention releases previously acquired parents" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "aliases");
    try tmp.dir.createDirPath(io, "installed/model");
    try tmp.dir.symLink(io, "../installed/model", "aliases/model", .{ .is_directory = true });
    const root = try tmp.dir.realPathFileAlloc(io, ".", a);
    defer a.free(root);
    const alias = try std.fs.path.join(a, &.{ root, "aliases/model" });
    defer a.free(alias);
    const writer = try tmp.dir.openDir(io, "installed", .{ .iterate = true });
    defer writer.close(io);
    try Lease.file(writer).lock(io, .exclusive);
    defer Lease.file(writer).unlock(io);
    try std.testing.expectError(error.ModelArtifactsChanging, Lease.shared(a, io, alias));
    const alias_parent = try tmp.dir.openDir(io, "aliases", .{ .iterate = true });
    defer alias_parent.close(io);
    try std.testing.expect(try Lease.file(alias_parent).tryLock(io, .exclusive));
    Lease.file(alias_parent).unlock(io);
}

test "model selection write lease rejects another installation and released ownership" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.createDirPath(io, "first");
    try tmp.dir.createDirPath(io, "second");
    const first = try tmp.dir.realPathFileAlloc(io, "first", a);
    defer a.free(first);
    const second = try tmp.dir.realPathFileAlloc(io, "second", a);
    defer a.free(second);
    var lease = try WriteLease.acquire(a, io, first);
    defer lease.deinit(io);
    try lease.requireDestination(io, first);
    try std.testing.expectError(error.ModelArtifactsChanging, lease.requireDestination(io, second));
    lease.deinit(io);
    try std.testing.expectError(error.ModelArtifactsChanging, lease.requireDestination(io, first));
}
