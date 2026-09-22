// Copyright 2026 Antfly, Inc.
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

//! Bounded removal of an exclusively owned immutable directory tree. Deleted
//! entries are the recovery cursor; the persisted deepest directory avoids
//! repeatedly walking ancestors after restart. No file payload is read or
//! truncated (SSTs may still be hard-linked by a live owner).
const std = @import("std");
const fs = @import("../../common/fs_paths.zig");

pub const max_path = 4096;
pub const max_depth = 64;
pub const Cursor = struct {
    path: [max_path]u8 = undefined,
    len: usize = 0,

    pub fn bytes(self: *const Cursor) []const u8 {
        return self.path[0..self.len];
    }
    pub fn validate(path: []const u8) !void {
        if (path.len > max_path or std.mem.indexOfScalar(u8, path, 0) != null or std.mem.indexOfScalar(u8, path, '\\') != null) return error.OnlineSourceCorrupt;
        if (path.len == 0) return;
        var parts = std.mem.splitScalar(u8, path, '/');
        var depth: usize = 0;
        while (parts.next()) |part| {
            if (part.len == 0 or std.mem.eql(u8, part, ".") or std.mem.eql(u8, part, "..")) return error.OnlineSourceCorrupt;
            depth += 1;
            if (depth > max_depth) return error.OnlineSourceCorrupt;
        }
    }
    fn push(self: *Cursor, name: []const u8) !void {
        if (name.len == 0 or std.mem.indexOfScalar(u8, name, '/') != null) return error.OnlineSourceCorrupt;
        const needed = self.len + @intFromBool(self.len != 0) + name.len;
        if (needed > max_path) return error.OnlineSourceCorrupt;
        if (self.len != 0) {
            self.path[self.len] = '/';
            self.len += 1;
        }
        @memcpy(self.path[self.len..][0..name.len], name);
        self.len += name.len;
        try validate(self.bytes());
    }
    fn pop(self: *Cursor) void {
        self.len = std.mem.lastIndexOfScalar(u8, self.bytes(), '/') orelse 0;
    }
};

pub const Budget = struct {
    max_entries: usize = 128,
    max_metadata_bytes: usize = 64 * 1024,
    max_duration_ns: u64 = 2 * std.time.ns_per_ms,
};

pub const Work = struct {
    budget: Budget,
    started: std.Io.Timestamp,
    entries: usize = 0,
    metadata_bytes: usize = 0,
    files: usize = 0,
    directories: usize = 0,
    logical_bytes_unlinked: u64 = 0,
    completed_units: usize = 0,

    pub fn init(io: std.Io, budget: Budget) Work {
        return .{ .budget = budget, .started = std.Io.Clock.awake.now(io) };
    }
    pub fn exhausted(self: *const Work, io: std.Io) bool {
        return self.entries >= self.budget.max_entries or self.metadata_bytes >= self.budget.max_metadata_bytes or self.budget.max_duration_ns == 0 or
            (self.completed_units != 0 and self.started.durationTo(std.Io.Clock.awake.now(io)).toNanoseconds() >= self.budget.max_duration_ns);
    }
    pub fn charge(self: *Work, io: std.Io, bytes: usize) bool {
        if (self.exhausted(io) or bytes > self.budget.max_metadata_bytes - self.metadata_bytes) return false;
        self.entries += 1;
        self.metadata_bytes += bytes;
        return true;
    }
};

fn openRelative(io: std.Io, root: []const u8, relative: []const u8, work: *Work) !std.Io.Dir {
    if (!work.charge(io, root.len + 128)) return error.GcBudgetExhausted;
    var dir = try std.Io.Dir.cwd().openDir(io, root, .{ .iterate = true, .follow_symlinks = false });
    errdefer dir.close(io);
    if (relative.len == 0) return dir;
    var parts = std.mem.splitScalar(u8, relative, '/');
    while (parts.next()) |part| {
        if (!work.charge(io, part.len + 128)) return error.GcBudgetExhausted;
        const child = try dir.openDir(io, part, .{ .iterate = true, .follow_symlinks = false });
        dir.close(io);
        dir = child;
    }
    return dir;
}

/// One bounded tree unit is guaranteed even when lock/receipt I/O already used
/// the elapsed allowance. It includes at most max_depth anchored opens plus
/// one entry operation; subsequent work checks the deadline. Each path open is
/// charged independently. A bounded fsync/cursor epilogue is mandatory after
/// mutations. Logical SST bytes are never mistaken for payload I/O.
pub fn advance(alloc: std.mem.Allocator, io: std.Io, root: []const u8, cursor: *Cursor, work: *Work) !bool {
    try Cursor.validate(cursor.bytes());
    while (!work.exhausted(io)) {
        if (!work.charge(io, root.len + cursor.len + 128)) return false;
        var dir = openRelative(io, root, cursor.bytes(), work) catch |err| switch (err) {
            error.GcBudgetExhausted => return false,
            error.FileNotFound => {
                if (cursor.len == 0) {
                    // A prior unlink may have succeeded before its directory
                    // sync failed. Absence alone is not a durable GC receipt.
                    try syncExistingParent(io, root);
                    work.completed_units += 1;
                    return true;
                }
                cursor.pop(); // Interrupted unlink, before its cursor receipt.
                work.completed_units += 1;
                continue;
            },
            error.NotDir, error.SymLinkLoop => {
                if (cursor.len != 0) {
                    cursor.pop();
                    work.completed_units += 1;
                    continue;
                }
                // An owned root replaced by a symlink is unlinked, never
                // traversed. Its external target is not cleanup authority.
                try std.Io.Dir.cwd().deleteFile(io, root);
                try fs.syncDirPortable(io, std.fs.path.dirname(root).?);
                work.files += 1;
                work.completed_units += 1;
                return true;
            },
            else => return err,
        };
        var dir_open = true;
        defer if (dir_open) dir.close(io);
        const path = if (cursor.len == 0) try alloc.dupe(u8, root) else try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, cursor.bytes() });
        defer alloc.free(path);
        var iterator = dir.iterate();
        var changed = false;
        while (!work.exhausted(io)) {
            if (!work.charge(io, cursor.len + 384)) break;
            const entry = try iterator.next(io) orelse {
                if (changed) try fs.syncDirPortable(io, path);
                dir.close(io);
                dir_open = false;
                std.Io.Dir.cwd().deleteDir(io, path) catch |err| switch (err) {
                    error.FileNotFound => {},
                    else => return err,
                };
                try fs.syncDirPortable(io, std.fs.path.dirname(path).?);
                work.directories += 1;
                work.completed_units += 1;
                if (cursor.len == 0) return true;
                cursor.pop();
                break;
            };
            if (!work.charge(io, entry.name.len + 128)) break;
            if (entry.kind == .directory) {
                if (changed) try fs.syncDirPortable(io, path);
                try cursor.push(entry.name);
                work.completed_units += 1;
                break;
            }
            const stat = dir.statFile(io, entry.name, .{ .follow_symlinks = false }) catch |err| switch (err) {
                error.FileNotFound => continue,
                else => return err,
            };
            // Never trust an ambiguous d_type enough to follow a directory.
            if (stat.kind == .directory) {
                if (changed) try fs.syncDirPortable(io, path);
                try cursor.push(entry.name);
                work.completed_units += 1;
                break;
            }
            if (work.exhausted(io)) break;
            try dir.deleteFile(io, entry.name);
            work.files += 1;
            work.logical_bytes_unlinked +|= stat.size;
            work.completed_units += 1;
            changed = true;
        }
        if (dir_open and changed) try fs.syncDirPortable(io, path);
    }
    return false;
}

fn syncExistingParent(io: std.Io, path: []const u8) !void {
    var parent = std.fs.path.dirname(path) orelse ".";
    for (0..max_depth) |_| {
        fs.syncDirPortable(io, parent) catch |err| switch (err) {
            error.FileNotFound => {
                const next = std.fs.path.dirname(parent) orelse ".";
                if (std.mem.eql(u8, next, parent)) return err;
                parent = next;
                continue;
            },
            else => return err,
        };
        return;
    }
    return error.OnlineSourceCorrupt;
}

test "relational index system source pin GC bounds a ten thousand file directory without reading payloads" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/gc", .{tmp.sub_path});
    defer alloc.free(root);
    try fs.createDirPathPortable(std.testing.io, root);
    var dir = try std.Io.Dir.cwd().openDir(std.testing.io, root, .{});
    for (0..10_000) |i| {
        var name: [32]u8 = undefined;
        const file = try dir.createFile(std.testing.io, try std.fmt.bufPrint(&name, "{d}.sst", .{i}), .{});
        file.close(std.testing.io);
    }
    dir.close(std.testing.io);
    var cursor: Cursor = .{};
    var files: usize = 0;
    var passes: usize = 0;
    while (true) {
        var work = Work.init(std.testing.io, .{ .max_entries = 127, .max_metadata_bytes = 32 * 1024, .max_duration_ns = std.time.ns_per_s });
        const done = try advance(alloc, std.testing.io, root, &cursor, &work);
        try std.testing.expect(work.entries <= 127);
        try std.testing.expect(work.metadata_bytes <= 32 * 1024);
        files += work.files;
        passes += 1;
        if (done) break;
        try std.testing.expect(passes < 1000);
    }
    try std.testing.expectEqual(@as(usize, 10_000), files);
    try std.testing.expect(passes > 100);
    std.debug.print("\nsource pin GC 10000 files: {} bounded pages, zero payload reads\n", .{passes});
}

test "relational index system source pin GC resumes lost unlink progress and never follows symlinks" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/root", .{tmp.sub_path});
    defer alloc.free(root);
    const nested = try std.fmt.allocPrint(alloc, "{s}/a/b", .{root});
    defer alloc.free(nested);
    try fs.createDirPathPortable(std.testing.io, nested);
    var cursor: Cursor = .{};
    try cursor.push("a");
    try cursor.push("b");
    // The unlink reached disk but its cursor update did not.
    try std.Io.Dir.cwd().deleteDir(std.testing.io, nested);
    const outside = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/outside", .{tmp.sub_path});
    defer alloc.free(outside);
    try fs.createDirPathPortable(std.testing.io, outside);
    const sentinel = try std.fmt.allocPrint(alloc, "{s}/keep", .{outside});
    defer alloc.free(sentinel);
    const file = try std.Io.Dir.cwd().createFile(std.testing.io, sentinel, .{});
    file.close(std.testing.io);
    const link = try std.fmt.allocPrint(alloc, "{s}/outside-link", .{root});
    defer alloc.free(link);
    try std.Io.Dir.cwd().symLink(std.testing.io, "../outside", link, .{ .is_directory = true });
    var work = Work.init(std.testing.io, .{ .max_entries = 128, .max_metadata_bytes = 64 * 1024, .max_duration_ns = std.time.ns_per_s });
    try std.testing.expect(try advance(alloc, std.testing.io, root, &cursor, &work));
    try std.Io.Dir.cwd().access(std.testing.io, sentinel, .{});
    try std.testing.expectError(error.OnlineSourceCorrupt, Cursor.validate("../outside"));
    try std.testing.expectError(error.OnlineSourceCorrupt, Cursor.validate("a//b"));
    try std.testing.expectError(error.OnlineSourceCorrupt, Cursor.validate("a/./b"));
}

test "relational index system source pin GC allocation failure and zero budgets leave retryable ownership" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/root", .{tmp.sub_path});
    defer alloc.free(root);
    try fs.createDirPathPortable(std.testing.io, root);
    var cursor: Cursor = .{};
    var zero = Work.init(std.testing.io, .{ .max_entries = 0 });
    try std.testing.expect(!try advance(alloc, std.testing.io, root, &cursor, &zero));
    try std.testing.expectEqual(@as(usize, 0), zero.entries);
    var failing = std.testing.FailingAllocator.init(alloc, .{ .fail_index = 0 });
    var work = Work.init(std.testing.io, .{ .max_duration_ns = std.time.ns_per_s });
    try std.testing.expectError(error.OutOfMemory, advance(failing.allocator(), std.testing.io, root, &cursor, &work));
    try std.Io.Dir.cwd().access(std.testing.io, root, .{});
    work = Work.init(std.testing.io, .{ .max_duration_ns = std.time.ns_per_s });
    try std.testing.expect(try advance(alloc, std.testing.io, root, &cursor, &work));
}

test "relational index system source pin GC default page makes progress at maximum legal cursor depth" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const root = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/deep", .{tmp.sub_path});
    defer alloc.free(root);
    var cursor: Cursor = .{};
    for (0..max_depth) |_| try cursor.push("d");
    const path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ root, cursor.bytes() });
    defer alloc.free(path);
    try fs.createDirPathPortable(std.testing.io, path);
    var work = Work.init(std.testing.io, .{});
    // Model slow lock/receipt I/O before entering the tree page.
    work.started.nanoseconds -= std.time.ns_per_s;
    _ = try advance(alloc, std.testing.io, root, &cursor, &work);
    try std.testing.expect(work.completed_units >= 1);
    try std.testing.expect(work.entries <= work.budget.max_entries);
    try std.testing.expect(cursor.len < max_depth * 2 - 1);
}
