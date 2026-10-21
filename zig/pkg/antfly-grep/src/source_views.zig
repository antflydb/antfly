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

//! Bounded shared immutable snapshots of packed source buckets. Positive keys
//! are content addressed; missing keys reopen the atomically replaced bucket.
const std = @import("std");
const Cache = @import("artifact_cache.zig").Cache;
const A = std.mem.Allocator;
const Shard = struct {
    mutex: std.Io.Mutex = .init,
    pages: [64]?[]const u8 = @splat(null),
    bytes: usize = 0,
    clock: usize = 0,
};
pub const Store = struct {
    a: A,
    prepared: ?@import("preparation_store.zig").Store = null,
    prepared_v4: ?*@import("batch_store.zig").Store = null,
    legacy_checked: bool = false,
    legacy_present: bool = false,
    prepared_mutex: std.Io.Mutex = .init,
    shards: [16]Shard = @splat(.{}),
    pub const prepared_body_limit = 4096 - 80;
    pub const prepared_disk_limit = @import("preparation_store.zig").disk_limit;
    pub fn loadPrepared(self: *Store, a: A, io: std.Io, o: *const @import("options.zig").Options, key: [32]u8) ?[]const u8 {
        self.prepared_mutex.lockUncancelable(io);
        if (self.prepared == null) self.prepared = .{ .a = self.a };
        self.prepared_mutex.unlock(io);
        return self.prepared.?.load(a, io, o, key);
    }
    pub fn loadLegacyPrepared(self: *Store, a: A, io: std.Io, o: *const @import("options.zig").Options, key: [32]u8) ?[]const u8 {
        // Probe the old namespace once, not once per missing source key.
        self.prepared_mutex.lockUncancelable(io);
        defer self.prepared_mutex.unlock(io);
        if (!self.legacy_checked) {
            self.legacy_checked = true;
            var directory = std.Io.Dir.cwd().openDir(io, o.cache_dir.?, .{ .iterate = true }) catch return null;
            defer directory.close(io);
            var it = directory.iterate();
            while (it.next(io) catch null) |entry| if (std.mem.startsWith(u8, entry.name, "source-v4-")) {
                self.legacy_present = true;
                break;
            };
        }
        if (!self.legacy_present) return null;
        // Source validation in source_cache precedes promotion to V5.
        if (self.prepared_v4 == null) {
            const store = self.a.create(@import("batch_store.zig").Store) catch return null;
            store.* = .{ .preparations = true };
            self.prepared_v4 = store;
        }
        return self.prepared_v4.?.load(a, io, o, 0, key);
    }
    pub fn storePrepared(self: *Store, a: A, io: std.Io, o: *const @import("options.zig").Options, key: [32]u8, body: []const u8) bool {
        if (body.len > prepared_body_limit or o.cached_only) return false;
        _ = a;
        self.prepared_mutex.lockUncancelable(io);
        if (self.prepared == null) self.prepared = .{ .a = self.a };
        self.prepared_mutex.unlock(io);
        self.prepared.?.store(io, o, key, body);
        return true;
    }
    pub const memory_limit = 4 * 1024 * 1024;
    pub fn deinit(self: *Store, io: std.Io) void {
        if (self.prepared) |*prepared| prepared.deinit(io);
        if (self.prepared_v4) |store| {
            store.deinit(io);
            self.a.destroy(store);
        }
        for (self.shards) |shard| for (shard.pages) |page| if (page) |bytes| self.a.free(bytes);
    }
    pub fn load(self: *Store, a: A, io: std.Io, cache: Cache, key: [32]u8) ?[]const u8 {
        const bucket = std.mem.readInt(u16, key[0..2], .little) % 1024;
        const shard = &self.shards[bucket % 16];
        const slot = bucket / 16;
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        if (shard.pages[slot]) |page| if (cache.view(key, page)) |body| return a.dupe(u8, body) catch null;
        const path = std.fmt.allocPrint(self.a, "{s}/{s}-{x:0>3}.bin", .{ cache.directory, cache.namespace, bucket }) catch return null;
        defer self.a.free(path);
        const file = std.Io.Dir.cwd().openFile(io, path, .{}) catch return null;
        defer file.close(io);
        const info = file.stat(io) catch return cache.loadFile(a, io, file, key);
        // Large buckets use the original bounded selected-record path. Avoid
        // read amplification for a small record sharing a large packed bucket.
        if (info.size > memory_limit / 16) return cache.loadFile(a, io, file, key);
        const page = self.a.alloc(u8, @intCast(info.size)) catch return null;
        if ((file.readPositionalAll(io, page, 0) catch 0) != page.len) {
            self.a.free(page);
            return null;
        }
        var retained = false;
        defer if (!retained) self.a.free(page);
        const body = cache.view(key, page) orelse return null;
        const copy = a.dupe(u8, body) catch return null;
        if (page.len <= memory_limit / 16) {
            if (shard.pages[slot]) |old| {
                shard.bytes -= old.len;
                self.a.free(old);
                shard.pages[slot] = null;
            }
            while (shard.bytes + page.len > memory_limit / 16) {
                const victim = shard.clock % shard.pages.len;
                shard.clock += 1;
                if (shard.pages[victim]) |old| {
                    shard.bytes -= old.len;
                    self.a.free(old);
                    shard.pages[victim] = null;
                }
            }
            shard.pages[slot] = page;
            shard.bytes += page.len;
            retained = true;
        }
        return copy;
    }
};

test "immutable source views refresh missing generations and own returned bodies" {
    const io = std.testing.io;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const cache: Cache = .{ .directory = directory, .namespace = "source-v3", .buckets = 1024, .limit = 256 * 1024 };
    var views: Store = .{ .a = a };
    defer views.deinit(io);
    var key: [32]u8 = @splat(0);
    cache.store(a, io, key, "first revision");
    const first = views.load(a, io, cache, key).?;
    defer a.free(first);
    key[31] = 1;
    cache.store(a, io, key, "second revision");
    const second = views.load(a, io, cache, key).?;
    defer a.free(second);
    try std.testing.expectEqualStrings("second revision", second);
    try std.testing.expectEqualStrings("first revision", first);
    var absent: [32]u8 = @splat(0);
    absent[31] = 20;
    try std.testing.expect(views.load(a, io, cache, absent) == null);
    // Force enough distinct full buckets into one stripe to evict snapshots.
    const body: [100000]u8 = @splat('x');
    for (0..8) |i| {
        std.mem.writeInt(u16, key[0..2], @intCast(i * 16), .little);
        cache.store(a, io, key, &body);
        const loaded = views.load(a, io, cache, key).?;
        a.free(loaded);
        try std.testing.expect(views.shards[0].bytes <= Store.memory_limit / 16);
    }
    try std.testing.expectEqualStrings("first revision", first);
    // Large packed buckets must read the selected record, without being cached.
    for ([_]u8{ 2, 3 }) |id| {
        key[31] = id;
        cache.store(a, io, key, &body);
    }
    const large = views.load(a, io, cache, key).?;
    defer a.free(large);
    try std.testing.expectEqualSlices(u8, &body, large);
    try std.testing.expect(views.shards[0].bytes <= Store.memory_limit / 16);
}
