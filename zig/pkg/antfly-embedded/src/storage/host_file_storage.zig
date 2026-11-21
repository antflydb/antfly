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

//! `Storage` over a small synchronous file-operation table supplied by a host.
//!
//! Browser hosts implement the table with an OPFS pool of synchronous access
//! handles (see `wasm_opfs_fs.mjs`); tests use an in-memory table. The host
//! owns durability: data writes become durable at `sync`, and namespace
//! changes (create, rename, delete) are durable when the call returns. Rename
//! must atomically replace an existing destination.
//!
//! Status codes returned by every operation: a non-negative value is success
//! (a size or byte count where documented), and negative values map to errors.
const std = @import("std");
const storage_io = @import("lsm_backend/storage_io.zig");

const Allocator = std.mem.Allocator;
pub const Storage = storage_io.Storage;
const FileTrailer = Storage.Trailer;

pub const status = struct {
    pub const not_found: i64 = -1;
    pub const end_of_stream: i64 = -2;
    pub const no_space: i64 = -3;
    pub const access_denied: i64 = -4;
    pub const io: i64 = -5;
};

pub const Ops = struct {
    ctx: ?*anyopaque = null,
    /// Logical file size, or `not_found`.
    size: *const fn (?*anyopaque, []const u8) i64,
    /// Read up to `out.len` bytes at `offset`; returns the count read.
    read: *const fn (?*anyopaque, []const u8, u64, []u8) i64,
    /// Replace (`append = false`) or extend the file, creating it if missing.
    write: *const fn (?*anyopaque, []const u8, []const u8, bool) i64,
    /// Make the file's written bytes durable.
    sync: *const fn (?*anyopaque, []const u8) i64,
    /// Atomically move `old` over `new`.
    rename: *const fn (?*anyopaque, []const u8, []const u8) i64,
    /// Remove one file (`tree = false`) or every file at or below a path.
    remove: *const fn (?*anyopaque, []const u8, bool) i64,
    /// Immediate child file (`dirs = false`) or directory names of `path`,
    /// newline-separated into `out`. Returns the total length required; when
    /// that exceeds `out.len` the caller retries with a larger buffer.
    list: *const fn (?*anyopaque, []const u8, bool, []u8) i64,
    now_ns: *const fn (?*anyopaque) u64,
};

pub const HostFileStorage = struct {
    ops: Ops,

    pub fn init(ops: Ops) HostFileStorage {
        return .{ .ops = ops };
    }

    /// The returned storage borrows `self`, which must outlive it.
    pub fn storage(self: *HostFileStorage) Storage {
        return .{ .ptr = self, .vtable = &vtable };
    }
};

const vtable: Storage.VTable = .{
    // No physical scratch directory: build staging stays in storage.
    .private_scratch_directory = noScratch,
    .create_dir_path = createDirPath,
    // Directories exist only as path prefixes of files.
    .virtual_directories = true,
    .read_file_alloc = readFileAlloc,
    .read_file_range_alloc = readFileRangeAlloc,
    .read_file_range_into = readFileRangeInto,
    .file_size = fileSize,
    .read_file_trailer_alloc = readFileTrailerAlloc,
    .write_file_absolute = writeFileAbsolute,
    .append_file_absolute = appendFileAbsolute,
    .sync_contents_absolute = syncContents,
    .sync_parent_absolute = syncParent,
    .rename_absolute = renameAbsolute,
    .delete_file_absolute = deleteFileAbsolute,
    .delete_tree = deleteTree,
    .list_file_names_alloc = listFileNames,
    .list_directory_names_alloc = listDirectoryNames,
    .now_ns = nowNs,
    .rename_is_atomic = true,
};

fn self_(ptr: *anyopaque) *HostFileStorage {
    return @ptrCast(@alignCast(ptr));
}

fn check(code: i64) !u64 {
    if (code >= 0) return @intCast(code);
    return switch (code) {
        status.not_found => error.FileNotFound,
        status.end_of_stream => error.EndOfStream,
        status.no_space => error.NoSpaceLeft,
        status.access_denied => error.AccessDenied,
        else => error.InputOutput,
    };
}

fn noScratch(_: *anyopaque) ?[]const u8 {
    return null;
}

fn createDirPath(_: *anyopaque, _: []const u8) !void {}

fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
    const s = self_(ptr);
    return check(s.ops.size(s.ops.ctx, path));
}

fn readExact(s: *HostFileStorage, path: []const u8, offset: u64, out: []u8) !void {
    var done: usize = 0;
    while (done < out.len) {
        const n = try check(s.ops.read(s.ops.ctx, path, offset + done, out[done..]));
        if (n == 0) return error.EndOfStream;
        done += @intCast(n);
    }
}

fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    const s = self_(ptr);
    const size = try fileSize(ptr, path);
    if (size > max_bytes) return error.FileTooBig;
    const out = try allocator.alloc(u8, @intCast(size));
    errdefer allocator.free(out);
    try readExact(s, path, 0, out);
    return out;
}

fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const out = try allocator.alloc(u8, len);
    errdefer allocator.free(out);
    try readFileRangeInto(ptr, path, offset, out);
    return out;
}

fn readFileRangeInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
    const size = try fileSize(ptr, path);
    if (offset > size or size - offset < out.len) return error.EndOfStream;
    try readExact(self_(ptr), path, offset, out);
}

fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !FileTrailer {
    const size = try fileSize(ptr, path);
    if (size < len) return error.EndOfStream;
    const bytes = try allocator.alloc(u8, len);
    errdefer allocator.free(bytes);
    try readExact(self_(ptr), path, size - len, bytes);
    return .{ .bytes = bytes, .file_size = size };
}

fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
    const s = self_(ptr);
    _ = try check(s.ops.write(s.ops.ctx, path, contents, false));
}

fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
    const s = self_(ptr);
    _ = try check(s.ops.write(s.ops.ctx, path, contents, true));
    if (sync) _ = try check(s.ops.sync(s.ops.ctx, path));
}

fn syncContents(ptr: *anyopaque, path: []const u8) !void {
    const s = self_(ptr);
    _ = try check(s.ops.sync(s.ops.ctx, path));
}

// Hosts make namespace changes durable before returning.
fn syncParent(_: *anyopaque, _: []const u8) !void {}

fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
    const s = self_(ptr);
    _ = try check(s.ops.rename(s.ops.ctx, old_path, new_path));
}

fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const s = self_(ptr);
    _ = try check(s.ops.remove(s.ops.ctx, path, false));
}

fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
    const s = self_(ptr);
    _ = try check(s.ops.remove(s.ops.ctx, path, true));
}

fn listNames(ptr: *anyopaque, allocator: Allocator, path: []const u8, dirs: bool) ![][]u8 {
    const s = self_(ptr);
    var capacity: usize = 4096;
    var joined: []u8 = undefined;
    while (true) {
        const buffer = try allocator.alloc(u8, capacity);
        const needed: usize = @intCast(check(s.ops.list(s.ops.ctx, path, dirs, buffer)) catch |err| {
            allocator.free(buffer);
            return err;
        });
        if (needed <= capacity) {
            joined = buffer[0..needed];
            defer allocator.free(buffer);
            var names: std.ArrayListUnmanaged([]u8) = .empty;
            errdefer {
                for (names.items) |name| allocator.free(name);
                names.deinit(allocator);
            }
            var it = std.mem.splitScalar(u8, joined, '\n');
            while (it.next()) |name| {
                if (name.len == 0) continue;
                const owned = try allocator.dupe(u8, name);
                errdefer allocator.free(owned);
                try names.append(allocator, owned);
            }
            return names.toOwnedSlice(allocator);
        }
        allocator.free(buffer);
        capacity = needed;
    }
}

fn listFileNames(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
    return listNames(ptr, allocator, path, false);
}

fn listDirectoryNames(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
    return listNames(ptr, allocator, path, true);
}

fn nowNs(ptr: *anyopaque) u64 {
    const s = self_(ptr);
    return s.ops.now_ns(s.ops.ctx);
}

/// In-memory host table for tests and native embedders: the same contract a
/// browser host implements, with an optional fault injector.
pub const MemoryHost = struct {
    allocator: Allocator,
    files: std.StringHashMapUnmanaged(std.ArrayListUnmanaged(u8)) = .empty,
    tick: u64 = 1,
    syncs: u64 = 0,
    /// Return a status to fail the next matching operation.
    fail_next_write: ?i64 = null,

    pub fn init(allocator: Allocator) MemoryHost {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *MemoryHost) void {
        var it = self.files.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.files.deinit(self.allocator);
    }

    pub fn ops(self: *MemoryHost) Ops {
        return .{
            .ctx = self,
            .size = size,
            .read = read,
            .write = write,
            .sync = sync,
            .rename = rename,
            .remove = remove,
            .list = list,
            .now_ns = now,
        };
    }

    fn get(ctx: ?*anyopaque) *MemoryHost {
        return @ptrCast(@alignCast(ctx.?));
    }

    fn size(ctx: ?*anyopaque, path: []const u8) i64 {
        const file = get(ctx).files.get(path) orelse return status.not_found;
        return @intCast(file.items.len);
    }

    fn read(ctx: ?*anyopaque, path: []const u8, offset: u64, out: []u8) i64 {
        const file = get(ctx).files.get(path) orelse return status.not_found;
        if (offset >= file.items.len) return 0;
        const n = @min(out.len, file.items.len - @as(usize, @intCast(offset)));
        @memcpy(out[0..n], file.items[@intCast(offset)..][0..n]);
        return @intCast(n);
    }

    fn write(ctx: ?*anyopaque, path: []const u8, bytes: []const u8, append: bool) i64 {
        const self = get(ctx);
        if (self.fail_next_write) |code| {
            self.fail_next_write = null;
            return code;
        }
        const gop = self.files.getOrPut(self.allocator, path) catch return status.no_space;
        if (!gop.found_existing) {
            gop.key_ptr.* = self.allocator.dupe(u8, path) catch {
                _ = self.files.remove(path);
                return status.no_space;
            };
            gop.value_ptr.* = .empty;
        }
        if (!append) gop.value_ptr.clearRetainingCapacity();
        gop.value_ptr.appendSlice(self.allocator, bytes) catch return status.no_space;
        return 0;
    }

    fn sync(ctx: ?*anyopaque, path: []const u8) i64 {
        const self = get(ctx);
        if (!self.files.contains(path)) return status.not_found;
        self.syncs += 1;
        return 0;
    }

    fn rename(ctx: ?*anyopaque, old: []const u8, new: []const u8) i64 {
        const self = get(ctx);
        if (!self.files.contains(old)) return status.not_found;
        if (std.mem.eql(u8, old, new)) return 0;
        const new_key = self.allocator.dupe(u8, new) catch return status.no_space;
        self.files.ensureUnusedCapacity(self.allocator, 1) catch {
            self.allocator.free(new_key);
            return status.no_space;
        };
        const moved = self.files.fetchRemove(old).?;
        self.allocator.free(moved.key);
        if (self.files.fetchRemove(new)) |replaced| {
            self.allocator.free(replaced.key);
            var bytes = replaced.value;
            bytes.deinit(self.allocator);
        }
        self.files.putAssumeCapacityNoClobber(new_key, moved.value);
        return 0;
    }

    fn remove(ctx: ?*anyopaque, path: []const u8, tree: bool) i64 {
        const self = get(ctx);
        if (!tree) {
            const removed = self.files.fetchRemove(path) orelse return status.not_found;
            self.allocator.free(removed.key);
            var bytes = removed.value;
            bytes.deinit(self.allocator);
            return 0;
        }
        while (true) {
            var it = self.files.keyIterator();
            const victim = while (it.next()) |key| {
                if (contains(path, key.*)) break key.*;
            } else return 0;
            const removed = self.files.fetchRemove(victim).?;
            self.allocator.free(removed.key);
            var bytes = removed.value;
            bytes.deinit(self.allocator);
        }
    }

    fn list(ctx: ?*anyopaque, path: []const u8, dirs: bool, out: []u8) i64 {
        const self = get(ctx);
        const names = storage_io.listMapNamesAlloc(self.allocator, path, &self.files, dirs) catch return status.no_space;
        defer {
            for (names) |name| self.allocator.free(name);
            self.allocator.free(names);
        }
        var needed: usize = 0;
        for (names) |name| needed += name.len + 1;
        if (needed <= out.len) {
            var at: usize = 0;
            for (names) |name| {
                @memcpy(out[at..][0..name.len], name);
                out[at + name.len] = '\n';
                at += name.len + 1;
            }
        }
        return @intCast(needed);
    }

    fn now(ctx: ?*anyopaque) u64 {
        const self = get(ctx);
        defer self.tick += 1;
        return self.tick;
    }

    fn contains(prefix: []const u8, path: []const u8) bool {
        const trimmed = std.mem.trimEnd(u8, prefix, "/");
        if (trimmed.len == 0) return path.len > 0 and path[0] == '/';
        if (!std.mem.startsWith(u8, path, trimmed)) return false;
        return path.len == trimmed.len or path[trimmed.len] == '/';
    }
};

test "embedded host file storage implements the storage file contract" {
    const a = std.testing.allocator;
    var host = MemoryHost.init(a);
    defer host.deinit();
    var backend = HostFileStorage.init(host.ops());
    const s = backend.storage();

    try std.testing.expectError(error.FileNotFound, s.fileSize("/db/missing"));
    try s.writeFileAbsolute("/db/a/one", "hello");
    try s.appendFileAbsolute(a, "/db/a/one", " world", true);
    try std.testing.expectEqual(@as(u64, 11), try s.fileSize("/db/a/one"));
    try std.testing.expect(host.syncs == 1);

    const all = try s.readFileAlloc(a, "/db/a/one", 64);
    defer a.free(all);
    try std.testing.expectEqualStrings("hello world", all);
    try std.testing.expectError(error.FileTooBig, s.readFileAlloc(a, "/db/a/one", 4));

    const range = try s.readFileRangeAlloc(a, "/db/a/one", 6, 5);
    defer a.free(range);
    try std.testing.expectEqualStrings("world", range);
    try std.testing.expectError(error.EndOfStream, s.readFileRangeAlloc(a, "/db/a/one", 7, 5));
    try std.testing.expectError(error.EndOfStream, s.readFileRangeAlloc(a, "/db/a/one", 12, 0));

    var trailer = try s.readFileTrailerAlloc(a, "/db/a/one", 5);
    defer trailer.deinit(a);
    try std.testing.expectEqualStrings("world", trailer.bytes);
    try std.testing.expectEqual(@as(u64, 11), trailer.file_size);

    // Rename atomically replaces an existing destination.
    try s.writeFileAbsolute("/db/a/two", "old");
    try s.writeFileAbsolute("/db/a/two.tmp", "new");
    try s.renameAbsolute("/db/a/two.tmp", "/db/a/two");
    const two = try s.readFileAlloc(a, "/db/a/two", 16);
    defer a.free(two);
    try std.testing.expectEqualStrings("new", two);
    try std.testing.expectError(error.FileNotFound, s.fileSize("/db/a/two.tmp"));
    try std.testing.expectError(error.FileNotFound, s.renameAbsolute("/db/none", "/db/x"));

    try s.writeFileAbsolute("/db/b/deep/three", "3");
    const files = try s.listFileNamesAlloc(a, "/db/a");
    defer {
        for (files) |name| a.free(name);
        a.free(files);
    }
    try std.testing.expectEqual(@as(usize, 2), files.len);
    const dirs = try s.listDirectoryNamesAlloc(a, "/db");
    defer {
        for (dirs) |name| a.free(name);
        a.free(dirs);
    }
    try std.testing.expectEqual(@as(usize, 2), dirs.len);

    try s.deleteFileAbsolute("/db/a/one");
    try std.testing.expectError(error.FileNotFound, s.deleteFileAbsolute("/db/a/one"));
    try s.deleteTree("/db/b");
    try std.testing.expectError(error.FileNotFound, s.fileSize("/db/b/deep/three"));
    try std.testing.expectEqual(@as(u64, 3), try s.fileSize("/db/a/two"));

    host.fail_next_write = status.no_space;
    try std.testing.expectError(error.NoSpaceLeft, s.writeFileAbsolute("/db/full", "x"));
    try std.testing.expect(s.nowNs() < s.nowNs());
}

test "embedded host file storage lists more names than one buffer holds" {
    const a = std.testing.allocator;
    var host = MemoryHost.init(a);
    defer host.deinit();
    var backend = HostFileStorage.init(host.ops());
    const s = backend.storage();
    var name_buf: [64]u8 = undefined;
    for (0..600) |i| try s.writeFileAbsolute(try std.fmt.bufPrint(&name_buf, "/db/segment-{d:0>8}.sst", .{i}), "x");
    const names = try s.listFileNamesAlloc(a, "/db");
    defer {
        for (names) |name| a.free(name);
        a.free(names);
    }
    try std.testing.expectEqual(@as(usize, 600), names.len);
}
