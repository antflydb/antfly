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

//! Incremental collection of filesystem query links. Directory retirement uses
//! the publication lock; traversal and unlinking use a separate collector lease.
//! The only durable progress required after a crash is the retired tree itself.
const std = @import("std");
const fs = @import("antfly_runtime_fs").fs_paths;
const Cancellation = @import("antfly_cancellation").CancellationToken;
const A = std.mem.Allocator;
pub const Page = struct { complete: bool, visited: usize = 0 };

pub const Collector = struct {
    a: A,
    io: std.Io,
    root: []u8,
    trash: []u8,
    lease: std.Io.File,
    live_dir: std.Io.Dir,
    live: std.Io.Dir.Iterator,
    trash_dir: std.Io.Dir,
    retired: std.Io.Dir.Iterator,
    phase: enum { retired, live, done } = .retired,
    pending: ?[]u8 = null,
    reaper: ?Reaper = null,

    pub fn init(a: A, io: std.Io, root: []const u8) !?Collector {
        const lease_path = try std.fs.path.join(a, &.{ root, ".gc.lock" });
        defer a.free(lease_path);
        const lease = try std.Io.Dir.cwd().createFile(io, lease_path, .{ .truncate = false });
        errdefer lease.close(io);
        if (!try lease.tryLock(io, .exclusive)) {
            lease.close(io);
            return null;
        }
        defer lease.unlock(io);
        const owned_root = try a.dupe(u8, root);
        errdefer a.free(owned_root);
        const trash = try std.fs.path.join(a, &.{ root, ".trash" });
        errdefer a.free(trash);
        try fs.createDirPathPortable(io, trash);
        try fs.syncDirPortable(io, root);
        const live_dir = try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true });
        errdefer live_dir.close(io);
        const trash_dir = try std.Io.Dir.cwd().openDir(io, trash, .{ .iterate = true });
        return .{ .a = a, .io = io, .root = owned_root, .trash = trash, .lease = lease, .live_dir = live_dir, .live = live_dir.iterate(), .trash_dir = trash_dir, .retired = trash_dir.iterate() };
    }
    pub fn deinit(self: *Collector) void {
        if (self.reaper) |*value| value.deinit();
        if (self.pending) |value| self.a.free(value);
        self.live_dir.close(self.io);
        self.trash_dir.close(self.io);
        self.lease.close(self.io);
        self.a.free(self.root);
        self.a.free(self.trash);
    }
    pub fn step(self: *Collector, now: u64, budget: usize, cancellation: Cancellation) !Page {
        try cancellation.check();
        if (!try self.lease.tryLock(self.io, .exclusive)) return .{ .complete = false };
        defer self.lease.unlock(self.io);
        var visited: usize = 0;
        while (visited < budget) {
            try cancellation.check();
            if (self.reaper) |*value| {
                visited += 1;
                if (try value.step()) {
                    value.deinit();
                    self.reaper = null;
                    try fs.syncDirPortable(self.io, self.trash);
                }
                continue;
            }
            if (self.pending) |name| {
                // Contention with a publisher yields this maintenance turn.
                // Never wait behind a large publication or copy under GC.
                const path = try std.fs.path.join(self.a, &.{ self.root, ".lock" });
                defer self.a.free(path);
                const lock = try std.Io.Dir.cwd().createFile(self.io, path, .{ .truncate = false });
                defer lock.close(self.io);
                if (!try lock.tryLock(self.io, .exclusive)) return .{ .complete = false, .visited = visited };
                {
                    defer lock.unlock(self.io);
                    // Reader admission also holds the publication lock while
                    // acquiring this shared lease. Never retire an expiry tree
                    // whose files are still addressed by a lazy reader.
                    const lease_path = try std.fs.path.join(self.a, &.{ self.root, name, ".lease" });
                    defer self.a.free(lease_path);
                    const reader_lease = try std.Io.Dir.cwd().createFile(self.io, lease_path, .{ .truncate = false });
                    defer reader_lease.close(self.io);
                    if (!try reader_lease.tryLock(self.io, .exclusive)) {
                        self.pending = null;
                        self.a.free(name);
                        visited += 1;
                        continue;
                    }
                    try std.Io.Dir.rename(self.live_dir, name, self.trash_dir, name, self.io);
                    try fs.syncDirPortable(self.io, self.trash);
                    try fs.syncDirPortable(self.io, self.root);
                }
                visited += 1;
                self.pending = null;
                defer self.a.free(name);
                const retired_path = try std.fs.path.join(self.a, &.{ self.trash, name });
                defer self.a.free(retired_path);
                self.reaper = try Reaper.init(self.a, self.io, retired_path);
                continue;
            }
            switch (self.phase) {
                .done => return .{ .complete = true, .visited = visited },
                .retired => {
                    const entry = try self.retired.next(self.io);
                    visited += 1;
                    if (entry) |item| {
                        if (item.kind != .directory) continue;
                        _ = std.fmt.parseInt(u64, item.name, 10) catch continue;
                        const path = try std.fs.path.join(self.a, &.{ self.trash, item.name });
                        defer self.a.free(path);
                        self.reaper = try Reaper.init(self.a, self.io, path);
                    } else self.phase = .live;
                },
                .live => {
                    const entry = try self.live.next(self.io);
                    visited += 1;
                    if (entry) |item| {
                        if (item.kind != .directory) continue;
                        const expires = std.fmt.parseInt(u64, item.name, 10) catch continue;
                        if (expires +| 30_000 < now) self.pending = try self.a.dupe(u8, item.name);
                    } else self.phase = .done;
                },
            }
        }
        return .{ .complete = self.phase == .done and self.reaper == null and self.pending == null, .visited = visited };
    }
};

/// One directory handle, regardless of tree depth. Every descent, unlink and
/// ascent consumes one work unit. Reopening a parent enumerates only remaining
/// entries, so neither a million-file tree nor a long prefix is rescanned on
/// every maintenance page. Restart simply resumes the still-present trash.
const Reaper = struct {
    a: A,
    io: std.Io,
    root_len: usize,
    path: []u8,
    dir: ?std.Io.Dir,
    iterator: std.Io.Dir.Iterator,
    fn init(a: A, io: std.Io, root: []const u8) !Reaper {
        const path = try a.dupe(u8, root);
        errdefer a.free(path);
        const dir = try std.Io.Dir.cwd().openDir(io, path, .{ .iterate = true, .follow_symlinks = false });
        return .{ .a = a, .io = io, .root_len = path.len, .path = path, .dir = dir, .iterator = dir.iterate() };
    }
    fn deinit(self: *Reaper) void {
        if (self.dir) |dir| dir.close(self.io);
        self.a.free(self.path);
    }
    fn step(self: *Reaper) !bool {
        const dir = self.dir.?;
        if (try self.iterator.next(self.io)) |entry| {
            if (entry.kind == .directory) {
                const path = try std.fs.path.join(self.a, &.{ self.path, entry.name });
                errdefer self.a.free(path);
                const child = try dir.openDir(self.io, entry.name, .{ .iterate = true, .follow_symlinks = false });
                dir.close(self.io);
                self.a.free(self.path);
                self.path = path;
                self.dir = child;
                self.iterator = child.iterate();
            } else try dir.deleteFile(self.io, entry.name);
            return false;
        }
        // Removing entries can invalidate filesystem directory cookies. Retry
        // a nonempty directory from its beginning instead of dropping work.
        std.Io.Dir.cwd().deleteDir(self.io, self.path) catch |err| switch (err) {
            error.DirNotEmpty => {
                self.iterator = dir.iterate();
                return false;
            },
            else => return err,
        };
        dir.close(self.io);
        self.dir = null;
        if (self.path.len == self.root_len) return true;
        const parent = try self.a.dupe(u8, std.fs.path.dirname(self.path).?);
        self.a.free(self.path);
        self.path = parent;
        const opened = try std.Io.Dir.cwd().openDir(self.io, parent, .{ .iterate = true, .follow_symlinks = false });
        self.dir = opened;
        self.iterator = opened.iterate();
        return false;
    }
};

test "external lake native linked GC bounds scans and deletion outside publication locks and resumes after restart" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Probe = struct {
        var root: []const u8 = "";
        var unlinks: usize = 0;
        fn unlink(raw: ?*anyopaque, dir: std.Io.Dir, name: []const u8) std.Io.Dir.DeleteFileError!void {
            const original = std.testing.io;
            var buffer: [std.fs.max_path_bytes]u8 = undefined;
            const path = std.fmt.bufPrint(&buffer, "{s}/.lock", .{root}) catch return error.Unexpected;
            const lock = std.Io.Dir.cwd().createFile(original, path, .{ .truncate = false }) catch return error.Unexpected;
            defer lock.close(original);
            const available = lock.tryLock(original, .exclusive) catch return error.Unexpected;
            if (!available) return error.Unexpected;
            lock.unlock(original);
            if (!std.mem.eql(u8, name, ".lease")) unlinks += 1;
            return original.vtable.dirDeleteFile(raw, dir, name);
        }
    };
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("linked-gc-pages");
    defer directory.cleanup();
    const root = try std.fs.path.join(a, &.{ directory.path(), "links" });
    defer a.free(root);
    const expired = try std.fs.path.join(a, &.{ root, "00000000000000000001" });
    defer a.free(expired);
    const nested = try std.fs.path.join(a, &.{ expired, "query", "nested" });
    defer a.free(nested);
    try fs.createDirPathPortable(io, nested);
    for (0..120) |i| {
        const path = try std.fmt.allocPrint(a, "{s}/file-{d}", .{ nested, i });
        defer a.free(path);
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = "immutable" });
    }
    for (0..200) |i| {
        const path = try std.fmt.allocPrint(a, "{s}/{d:0>20}", .{ root, 100000 + i });
        defer a.free(path);
        try fs.createDirPathPortable(io, path);
    }
    Probe.root = root;
    Probe.unlinks = 0;
    var vtable = io.vtable.*;
    vtable.dirDeleteFile = Probe.unlink;
    const measured: std.Io = .{ .userdata = io.userdata, .vtable = &vtable };
    var collector = (try Collector.init(a, measured, root)).?;
    var open = true;
    defer if (open) collector.deinit();
    // Contention yields instead of waiting on the publisher. Scanning hundreds
    // of unexpired entries must also resume rather than restart its prefix.
    const lock_path = try std.fs.path.join(a, &.{ root, ".lock" });
    defer a.free(lock_path);
    const publisher = try std.Io.Dir.cwd().createFile(io, lock_path, .{ .truncate = false });
    defer publisher.close(io);
    try std.testing.expect(try publisher.tryLock(io, .exclusive));
    var turns: usize = 0;
    while (collector.pending == null) : (turns += 1) {
        const page = try collector.step(60000, 7, .none);
        try std.testing.expect(page.visited <= 7);
        try std.testing.expect(turns < 1000);
    }
    const busy = try collector.step(60000, 7, .none);
    try std.testing.expect(!busy.complete);
    try std.testing.expectEqual(@as(usize, 0), busy.visited);
    publisher.unlock(io);
    while (Probe.unlinks == 0) : (turns += 1) {
        const before = Probe.unlinks;
        const page = try collector.step(60000, 7, .none);
        try std.testing.expect(page.visited <= 7 and Probe.unlinks - before <= 7);
        try std.testing.expect(turns < 1000);
    }
    try std.testing.expect(Probe.unlinks < 120);
    // Simulate process loss mid-tree: discovery starts with durable trash.
    collector.deinit();
    open = false;
    collector = (try Collector.init(a, measured, root)).?;
    open = true;
    const Stop = struct {
        fn check(_: *const anyopaque) !void {
            return error.Canceled;
        }
    };
    var stop: u8 = 0;
    try std.testing.expectError(error.Canceled, collector.step(60000, 7, .{ .ptr = &stop, .check_fn = Stop.check }));
    while (true) : (turns += 1) {
        const before = Probe.unlinks;
        const page = try collector.step(60000, 7, .none);
        try std.testing.expect(page.visited <= 7 and Probe.unlinks - before <= 7);
        try std.testing.expect(turns < 1000);
        if (page.complete) break;
    }
    try std.testing.expectEqual(@as(usize, 120), Probe.unlinks);
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.accessAbsolute(io, expired, .{}));
    const retained = try std.fs.path.join(a, &.{ root, "00000000000000100000" });
    defer a.free(retained);
    try std.Io.Dir.accessAbsolute(io, retained, .{});
}
