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

//! A bounded cache of directories whose complete ancestor chain was synced.
//! Merely observing that a directory exists is not a durability proof: another
//! process may have created it but not synced its parent yet. Cache misses
//! establish that proof locally; immutable object writes then only sync their
//! immediate rename directories. Owners must invalidate after directory removal.
const std = @import("std");
const builtin = @import("builtin");

pub const Cache = struct {
    mutex: std.Io.Mutex = .init,
    entries: [64]?[]u8 = @splat(null),
    next: usize = 0,

    pub fn deinit(self: *Cache, alloc: std.mem.Allocator) void {
        for (self.entries) |entry| if (entry) |path| alloc.free(path);
        self.* = .{};
    }

    pub fn ensure(self: *Cache, alloc: std.mem.Allocator, io: std.Io, path: []const u8) !void {
        try self.mutex.lock(io);
        defer self.mutex.unlock(io);
        for (self.entries) |entry| if (entry) |known| {
            if (std.mem.eql(u8, known, path)) return;
        };
        try createPath(io, path);
        var current = if (path.len == 0) "." else path;
        while (true) {
            try sync(io, current);
            const parent = std.fs.path.dirname(current) orelse {
                if (!std.mem.eql(u8, current, ".")) try sync(io, ".");
                break;
            };
            if (std.mem.eql(u8, parent, current)) break;
            current = parent;
        }
        const owned = try alloc.dupe(u8, path);
        if (self.entries[self.next]) |old| alloc.free(old);
        self.entries[self.next] = owned;
        self.next = (self.next + 1) % self.entries.len;
    }
};

fn createPath(io: std.Io, path: []const u8) anyerror!void {
    if (path.len == 0 or std.mem.eql(u8, path, ".")) return;
    if (std.Io.Dir.cwd().openDir(io, path, .{})) |opened| {
        var dir = opened;
        dir.close(io);
        return;
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    if (std.fs.path.dirname(path)) |parent| {
        if (!std.mem.eql(u8, parent, path)) try createPath(io, parent);
    }
    std.Io.Dir.cwd().createDir(io, path, .default_dir) catch |err| switch (err) {
        error.PathAlreadyExists => {
            var dir = try std.Io.Dir.cwd().openDir(io, path, .{});
            dir.close(io);
        },
        else => return err,
    };
}

pub fn sync(io: std.Io, path: []const u8) anyerror!void {
    // Experimental Windows support: Win32 cannot flush a directory handle.
    // This is a best-effort experimental path. Flushing file contents does
    // not establish namespace durability; power-loss recovery is unverified.
    if (builtin.os.tag == .windows) return;
    if (builtin.os.tag == .wasi or builtin.os.tag == .freestanding)
        return error.DurableDirectorySyncUnsupported;
    var dir = try std.Io.Dir.cwd().openDir(io, if (path.len == 0) "." else path, .{ .iterate = true });
    defer dir.close(io);
    const file = std.Io.File{ .handle = dir.handle, .flags = .{ .nonblocking = false } };
    try file.sync(io);
}

/// Windows publication also flushes the renamed file's metadata. Keep this
/// through the caller's I/O authority so flush failures are observable. This
/// supplements, rather than establishes, ancestor/directory durability.
pub fn syncPublishedFile(io: std.Io, path: []const u8) !void {
    if (comptime builtin.os.tag != .windows) return;
    const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    try file.sync(io);
}

test "Windows publication flush uses borrowed I/O and propagates failure after closing" {
    if (comptime builtin.os.tag != .windows) return error.SkipZigTest;
    const State = struct {
        closed: bool = false,
        fn open(_: ?*anyopaque, _: std.Io.Dir, _: []const u8, options: std.Io.Dir.OpenFileOptions) std.Io.File.OpenError!std.Io.File {
            if (options.mode != .read_write) return error.AccessDenied;
            return .{ .handle = @ptrFromInt(99), .flags = .{ .nonblocking = false } };
        }
        fn close(ptr: ?*anyopaque, _: []const std.Io.File) void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.closed = true;
        }
        fn flush(_: ?*anyopaque, _: std.Io.File) std.Io.File.SyncError!void {
            return error.InputOutput;
        }
    };
    var state = State{};
    var vtable = std.Io.failing.vtable.*;
    vtable.dirOpenFile = State.open;
    vtable.fileClose = State.close;
    vtable.fileSync = State.flush;
    const io = std.Io{ .userdata = &state, .vtable = &vtable };
    try std.testing.expectError(error.InputOutput, syncPublishedFile(io, "published"));
    try std.testing.expect(state.closed);
}

test "durable directory cache proves ancestor durability once and retries failed sync" {
    const State = struct {
        syncs: usize = 0,
        fail: bool = false,
        fn open(_: ?*anyopaque, _: std.Io.Dir, _: []const u8, _: std.Io.Dir.OpenOptions) std.Io.Dir.OpenError!std.Io.Dir {
            return .{ .handle = 99 };
        }
        fn close(_: ?*anyopaque, _: []const std.Io.Dir) void {}
        fn flush(ptr: ?*anyopaque, _: std.Io.File) std.Io.File.SyncError!void {
            const self: *@This() = @ptrCast(@alignCast(ptr.?));
            self.syncs += 1;
            if (self.fail) return error.InputOutput;
        }
    };
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;
    var state = State{};
    var vtable = std.Io.failing.vtable.*;
    vtable.dirOpenDir = State.open;
    vtable.dirClose = State.close;
    vtable.fileSync = State.flush;
    const io = std.Io{ .userdata = &state, .vtable = &vtable };
    var cache = Cache{};
    defer cache.deinit(std.testing.allocator);
    try cache.ensure(std.testing.allocator, io, "root/domain/attempt");
    try std.testing.expectEqual(@as(usize, 4), state.syncs);
    try cache.ensure(std.testing.allocator, io, "root/domain/attempt");
    try std.testing.expectEqual(@as(usize, 4), state.syncs);
    state.fail = true;
    try std.testing.expectError(error.InputOutput, cache.ensure(std.testing.allocator, io, "root/domain/other"));
    state.fail = false;
    try cache.ensure(std.testing.allocator, io, "root/domain/other");
    try std.testing.expectEqual(@as(usize, 9), state.syncs);
    try cache.ensure(std.testing.allocator, io, "root/domain/other");
    try std.testing.expectEqual(@as(usize, 9), state.syncs);
}
