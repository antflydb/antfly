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
const Control = @import("httpx").socket.WriteControl;

pub const Cache = struct {
    mutex: std.Io.Mutex = .init,
    entries: [64]?[]u8 = @splat(null),
    next: usize = 0,

    pub fn deinit(self: *Cache, alloc: std.mem.Allocator) void {
        for (self.entries) |entry| if (entry) |path| alloc.free(path);
        self.* = .{};
    }

    pub fn ensure(self: *Cache, alloc: std.mem.Allocator, io: std.Io, path: []const u8) !void {
        return self.ensureWithControl(alloc, io, path, .{});
    }

    /// Cache state never spans provider waits. Duplicate proofs are harmless;
    /// each miss proves ancestor durability before publishing its cache entry.
    pub fn ensureWithControl(self: *Cache, alloc: std.mem.Allocator, io: std.Io, path: []const u8, control: Control) !void {
        try lock(&self.mutex, io, control);
        for (self.entries) |entry| if (entry) |known| {
            if (std.mem.eql(u8, known, path)) {
                self.mutex.unlock(io);
                return;
            }
        };
        self.mutex.unlock(io);
        try createPathControlled(io, path, control);
        var current = if (path.len == 0) "." else path;
        while (true) {
            try control.withWait(sync, .{ io, current });
            const parent = std.fs.path.dirname(current) orelse {
                if (!std.mem.eql(u8, current, ".")) try control.withWait(sync, .{ io, "." });
                break;
            };
            if (std.mem.eql(u8, parent, current)) break;
            current = parent;
        }
        const owned = try alloc.dupe(u8, path);
        errdefer alloc.free(owned);
        try lock(&self.mutex, io, control);
        defer self.mutex.unlock(io);
        for (self.entries) |entry| if (entry) |known| if (std.mem.eql(u8, known, path)) {
            alloc.free(owned);
            return;
        };
        if (self.entries[self.next]) |old| alloc.free(old);
        self.entries[self.next] = owned;
        self.next = (self.next + 1) % self.entries.len;
    }
};

fn lock(mutex: *std.Io.Mutex, io: std.Io, control: Control) !void {
    while (!mutex.tryLock()) try control.withWait(std.Io.sleep, .{ io, std.Io.Duration.fromMilliseconds(1), std.Io.Clock.awake });
}
fn closeDir(dir: std.Io.Dir, args: anytype) void {
    dir.close(args[1]);
}
fn createPathControlled(io: std.Io, path: []const u8, control: Control) anyerror!void {
    if (path.len == 0 or std.mem.eql(u8, path, ".")) return;
    var rejected = false;
    if (control.withOwnedWait(&rejected, std.Io.Dir.openDir, .{ std.Io.Dir.cwd(), io, path, std.Io.Dir.OpenOptions{} }, closeDir)) |opened| {
        opened.close(io);
        return;
    } else |err| switch (err) {
        error.FileNotFound => {},
        else => return err,
    }
    if (std.fs.path.dirname(path)) |parent| {
        if (!std.mem.eql(u8, parent, path)) try createPathControlled(io, parent, control);
    }
    control.withWait(std.Io.Dir.createDir, .{ std.Io.Dir.cwd(), io, path, std.Io.Dir.Permissions.default_dir }) catch |err| switch (err) {
        error.PathAlreadyExists => {
            const dir = try control.withOwnedWait(&rejected, std.Io.Dir.openDir, .{ std.Io.Dir.cwd(), io, path, std.Io.Dir.OpenOptions{} }, closeDir);
            dir.close(io);
        },
        else => return err,
    };
}

pub fn sync(io: std.Io, path: []const u8) anyerror!void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding)
        return error.DurableDirectorySyncUnsupported;
    var dir = try std.Io.Dir.cwd().openDir(io, if (path.len == 0) "." else path, .{ .iterate = true });
    defer dir.close(io);
    const file = std.Io.File{ .handle = dir.handle, .flags = .{ .nonblocking = false } };
    try file.sync(io);
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
