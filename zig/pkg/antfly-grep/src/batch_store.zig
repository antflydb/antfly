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

//! Shared bounded score-batch shards. Positive bucket offsets are immutable;
//! warm hits use one positional page read and no open/stat/close operations.
//! A packed directory allocates only occupied bucket pages, including on APFS.
const std = @import("std");
const A = std.mem.Allocator;
const Options = @import("options.zig").Options;
const Codec = @import("artifact_cache.zig").Cache;
const shards = 16;
const prefix = 4096;
const magic = "AGSHD001";
const Class = struct {
    id: usize,
    name: []const u8,
    buckets: usize,
    ways: usize,
    slot: usize,
    fn select(count: usize) Class {
        const size = 84 + count * 16;
        return if (size <= 256) .{ .id = 0, .name = "small", .buckets = 4096, .ways = 16, .slot = 256 } else if (size <= 1024) .{ .id = 1, .name = "medium", .buckets = 2048, .ways = 8, .slot = 1024 } else .{ .id = 2, .name = "large", .buckets = 512, .ways = 8, .slot = 4096 };
    }
    fn preparation() Class {
        return .{ .id = 0, .name = "prepared", .buckets = 1024, .ways = 8, .slot = 4096 };
    }
    fn bytes(self: Class) usize {
        return self.ways * self.slot;
    }
    fn codec(self: Class) Codec {
        return .{ .directory = "", .namespace = "", .buckets = @intCast(self.buckets), .ways = self.ways, .limit = self.slot };
    }
};
const Shard = struct {
    mutex: std.Io.Mutex = .init,
    file: ?std.Io.File = null,
    writable: bool = false,
    valid: bool = false,
    mapping: [256]u32 = @splat(0),
    fn open(self: *Shard, a: A, io: std.Io, o: *const Options, class: Class, number: usize, write: bool, preparations: bool) ?std.Io.File {
        if (self.file != null and (!write or self.writable)) return self.file;
        const path = std.fmt.allocPrint(a, "{s}/{s}-{s}-{x:0>2}.bin", .{ o.cache_dir.?, if (preparations) "source-v4" else "batch-v10", class.name, number }) catch return null;
        defer a.free(path);
        const f = if (write) blk: {
            std.Io.Dir.cwd().createDirPath(io, o.cache_dir.?) catch return null;
            break :blk std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .truncate = false, .permissions = if (@import("builtin").os.tag == .windows) .default_file else .fromMode(0o600) }) catch return null;
        } else std.Io.Dir.cwd().openFile(io, path, .{ .mode = if (o.cached_only) .read_only else .read_write }) catch std.Io.Dir.cwd().openFile(io, path, .{}) catch return null;
        if (self.file) |old| old.close(io);
        self.file = f;
        // A read-only fallback is never used for writes; upgrading happens
        // under the shard mutex, after all local readers have finished.
        self.writable = write;
        var directory: [prefix]u8 = @splat(0);
        self.valid = (f.readPositionalAll(io, &directory, 0) catch 0) == prefix and validDirectory(&directory, class);
        @memset(&self.mapping, 0);
        if (self.valid) for (0..class.buckets / shards) |i| {
            const value = std.mem.readInt(u32, directory[64 + i * 4 ..][0..4], .little);
            if (value <= class.buckets / shards) self.mapping[i] = value;
        };
        return f;
    }
};
fn validDirectory(bytes: *const [prefix]u8, class: Class) bool {
    return std.mem.eql(u8, bytes[0..8], magic) and std.mem.readInt(u32, bytes[8..12], .little) == class.bytes() and std.mem.readInt(u32, bytes[12..16], .little) == class.buckets / shards;
}
/// Optional workspace-only immutable record cache. TTL/score validation stays
/// in the caller, on every hit; timestamps are never refreshed by reading.
pub const Memo = struct {
    a: A,
    io: std.Io,
    mutex: std.Io.Mutex = .init,
    slots: [2048]?struct { key: [32]u8, body: []const u8 } = @splat(null),
    bytes: usize = 0,
    clock: usize = 0,
    pub const limit = 4 * 1024 * 1024;
    pub fn deinit(self: *Memo) void {
        for (self.slots) |slot| if (slot) |entry| self.a.free(entry.body);
    }
    fn get(self: *Memo, a: A, key: [32]u8) ?[]const u8 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const slot = std.mem.readInt(u16, key[0..2], .little) % self.slots.len;
        if (self.slots[slot]) |entry| if (std.mem.eql(u8, &entry.key, &key)) return a.dupe(u8, entry.body) catch null;
        return null;
    }
    fn put(self: *Memo, key: [32]u8, body: []const u8) void {
        if (body.len > 4096) return;
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const slot = std.mem.readInt(u16, key[0..2], .little) % self.slots.len;
        const copy = self.a.dupe(u8, body) catch return;
        if (self.slots[slot]) |entry| {
            self.bytes -= entry.body.len;
            self.a.free(entry.body);
            self.slots[slot] = null;
        }
        while (self.bytes + body.len > limit) {
            const victim = self.clock % self.slots.len;
            self.clock +%= 1;
            if (self.slots[victim]) |entry| {
                self.bytes -= entry.body.len;
                self.a.free(entry.body);
                self.slots[victim] = null;
            }
        }
        self.slots[slot] = .{ .key = key, .body = copy };
        self.bytes += copy.len;
    }
};
pub const Store = struct {
    memo: ?*Memo = null,
    preparations: bool = false,
    handles: [3][shards]Shard = @splat(@splat(.{})),
    pub const max_handles = 3 * shards;
    pub const max_bytes = 48 * 1024 * 1024 + max_handles * prefix;
    pub fn deinit(self: *Store, io: std.Io) void {
        for (&self.handles) |*group| for (group) |*shard| if (shard.file) |file| file.close(io);
    }
    pub fn load(self: *Store, a: A, io: std.Io, o: *const Options, count: usize, key: [32]u8) ?[]const u8 {
        if (self.memo) |memo| if (memo.get(a, key)) |body| return body;
        const class = if (self.preparations) Class.preparation() else Class.select(count);
        const bucket = std.mem.readInt(u16, key[0..2], .little) % class.buckets;
        const local = bucket / shards;
        const shard = &self.handles[class.id][bucket % shards];
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        const file = shard.open(a, io, o, class, bucket % shards, false, self.preparations) orelse return null;
        if (!shard.valid) {
            var directory: [prefix]u8 = undefined;
            shard.valid = (file.readPositionalAll(io, &directory, 0) catch 0) == prefix and validDirectory(&directory, class);
            if (!shard.valid) return null;
            for (0..class.buckets / shards) |i| {
                const value = std.mem.readInt(u32, directory[64 + i * 4 ..][0..4], .little);
                shard.mapping[i] = if (value <= class.buckets / shards) value else 0;
            }
        }
        if (shard.mapping[local] == 0) {
            var entry: [4]u8 = undefined;
            if ((file.readPositionalAll(io, &entry, 64 + local * 4) catch 0) != 4) return null;
            const value = std.mem.readInt(u32, &entry, .little);
            if (value == 0 or value > class.buckets / shards) return null;
            shard.mapping[local] = value;
        }
        const offset = prefix + @as(u64, shard.mapping[local] - 1) * class.bytes();
        if (self.preparations) return class.codec().loadAt(a, io, file, key, offset);
        var page: [32768]u8 = undefined;
        const n = file.readPositionalAll(io, page[0..class.bytes()], offset) catch return null;
        const body = class.codec().fixedGet(a, key, page[0..n]) orelse return null;
        if (self.memo) |memo| memo.put(key, body);
        return body;
    }
    pub fn store(self: *Store, a: A, io: std.Io, o: *const Options, count: usize, key: [32]u8, body: []const u8) void {
        const class = if (self.preparations) Class.preparation() else Class.select(count);
        if (body.len > class.slot - 80) return;
        if (self.memo) |memo| memo.put(key, body);
        const bucket = std.mem.readInt(u16, key[0..2], .little) % class.buckets;
        const local = bucket / shards;
        const shard = &self.handles[class.id][bucket % shards];
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        const file = shard.open(a, io, o, class, bucket % shards, true, self.preparations) orelse return;
        // Best-effort cache writes never wait behind another process.
        if (!(file.tryLock(io, .exclusive) catch false)) return;
        defer file.unlock(io);
        var directory: [prefix]u8 = @splat(0);
        if ((file.readPositionalAll(io, &directory, 0) catch 0) != prefix or !validDirectory(&directory, class)) {
            @memset(&directory, 0);
            directory[0..8].* = magic.*;
            std.mem.writeInt(u32, directory[8..12], @intCast(class.bytes()), .little);
            std.mem.writeInt(u32, directory[12..16], @intCast(class.buckets / shards), .little);
            file.setLength(io, 0) catch return;
            file.writePositionalAll(io, &directory, 0) catch return;
            @memset(&shard.mapping, 0);
        }
        shard.valid = true;
        var index = std.mem.readInt(u32, directory[64 + local * 4 ..][0..4], .little);
        const existing = index > 0 and index <= class.buckets / shards;
        if (!existing) {
            // Reuse orphaned pages left by a crash before directory publication.
            var used: [256]bool = @splat(false);
            for (0..class.buckets / shards) |i| {
                const value = std.mem.readInt(u32, directory[64 + i * 4 ..][0..4], .little);
                if (value > 0 and value <= class.buckets / shards) used[value - 1] = true;
            }
            const free = std.mem.indexOfScalar(bool, used[0 .. class.buckets / shards], false) orelse return;
            index = @intCast(free + 1);
        }
        // Both score and preparation pages fit this bounded stack buffer;
        // preparation reads fetch only headers and the selected payload.
        var storage: [32768]u8 = undefined;
        const page = storage[0..class.bytes()];
        @memset(page, 0);
        const offset = prefix + @as(u64, index - 1) * class.bytes();
        if (existing) _ = file.readPositionalAll(io, page[0..class.bytes()], offset) catch 0;
        if (self.preparations) {
            // Keep the bounded logical page address while writing only live
            // headers/payloads. Empty ways do not allocate filesystem blocks.
            const encoded_page = class.codec().packedPut(a, io, key, body, if (existing) page else null) orelse return;
            defer a.free(encoded_page);
            file.writePositionalAll(io, encoded_page, offset) catch return;
        } else {
            class.codec().fixedPut(io, key, body, page[0..class.bytes()]);
            file.writePositionalAll(io, page[0..class.bytes()], offset) catch return;
        }
        if (!existing) {
            var entry: [4]u8 = undefined;
            std.mem.writeInt(u32, &entry, index, .little);
            file.writePositionalAll(io, &entry, 64 + local * 4) catch return;
        }
        shard.mapping[local] = index;
    }
};

test "packed shards reuse handles and recover cold missing directories" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const io = std.testing.io;
    var options: Options = .{};
    options.cache_dir = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    var reader: Store = .{};
    defer reader.deinit(io);
    var writer: Store = .{};
    defer writer.deinit(io);
    for ([_]usize{ 1, 16, 128 }) |count| {
        const class = Class.select(count);
        const path = try std.fmt.allocPrint(a, "{s}/batch-v10-{s}-00.bin", .{ options.cache_dir.?, class.name });
        const empty = try std.Io.Dir.cwd().createFile(io, path, .{});
        empty.close(io);
        var key: [32]u8 = @splat(0);
        try std.testing.expect(reader.load(a, io, &options, count, key) == null);
        writer.store(a, io, &options, count, key, "first");
        try std.testing.expectEqualStrings("first", reader.load(a, io, &options, count, key).?);
        const first_handle = reader.handles[class.id][0].file.?.handle;
        key[0] = shards;
        writer.store(a, io, &options, count, key, "second");
        try std.testing.expectEqualStrings("second", reader.load(a, io, &options, count, key).?);
        try std.testing.expectEqual(first_handle, reader.handles[class.id][0].file.?.handle);
        try std.testing.expectEqual(@as(u64, prefix + 2 * class.bytes()), (try std.Io.Dir.cwd().statFile(io, path, .{})).size);
        const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
        defer file.close(io);
        try file.writePositionalAll(io, "bad", prefix + class.bytes() + class.ways * 80);
        try std.testing.expect(reader.load(a, io, &options, count, key) == null);
        writer.store(a, io, &options, count, key, "repaired");
        try std.testing.expectEqualStrings("repaired", reader.load(a, io, &options, count, key).?);
        // A page written before its mapping is published is reusable.
        try file.writePositionalAll(io, &@as([4]u8, @splat(0)), 64 + 4);
        writer.store(a, io, &options, count, key, "orphan reused");
        try std.testing.expectEqual(@as(u64, prefix + 2 * class.bytes()), (try file.stat(io)).size);
        try std.testing.expectEqualStrings("orphan reused", reader.load(a, io, &options, count, key).?);
    }
}

test "independent shard writers cannot return a torn identity" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const a = std.testing.allocator;
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var failed = std.atomic.Value(bool).init(false);
    const Worker = struct {
        directory: []const u8,
        id: u8,
        failed: *std.atomic.Value(bool),
        fn run(w: @This()) void {
            const io = std.testing.io;
            var store: Store = .{};
            defer store.deinit(io);
            var o: Options = .{};
            o.cache_dir = w.directory;
            const alloc = std.heap.page_allocator;
            var key: [32]u8 = @splat(0);
            key[31] = w.id;
            const body: [128]u8 = @splat(w.id);
            for (0..80) |_| {
                store.store(alloc, io, &o, 1, key, &body);
                if (store.load(alloc, io, &o, 1, key)) |actual| {
                    defer alloc.free(actual);
                    if (!std.mem.eql(u8, actual, &body)) w.failed.store(true, .monotonic);
                }
            }
        }
    };
    var threads: [4]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    for (&threads, 0..) |*thread, i| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{Worker{ .directory = directory, .id = @intCast(i + 1), .failed = &failed }});
        started += 1;
    }
    for (threads[0..started]) |thread| thread.join();
    started = 0;
    try std.testing.expect(!failed.load(.monotonic));
}

test "prepared shard handles retain eight colliding revisions and bound pages" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const o: Options = .{ .cache_dir = directory };
    var writer: Store = .{ .preparations = true };
    defer writer.deinit(io);
    var reader: Store = .{ .preparations = true };
    defer reader.deinit(io);
    var key: [32]u8 = @splat(0);
    const body: [3000]u8 = @splat('s');
    for (0..8) |i| {
        key[31] = @intCast(i);
        writer.store(a, io, &o, 0, key, &body);
    }
    for (0..8) |i| {
        key[31] = @intCast(i);
        const value = reader.load(a, io, &o, 0, key).?;
        defer a.free(value);
        try std.testing.expectEqualSlices(u8, &body, value);
    }
    const path = try std.fmt.allocPrint(a, "{s}/source-v4-prepared-00.bin", .{directory});
    defer a.free(path);
    try std.testing.expectEqual(@as(u64, prefix + 8 * 80 + 8 * body.len), (try std.Io.Dir.cwd().statFile(io, path, .{})).size);
    const handle = reader.handles[0][0].file.?.handle;
    key[31] = 9;
    writer.store(a, io, &o, 0, key, "ninth");
    const value = reader.load(a, io, &o, 0, key).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("ninth", value);
    try std.testing.expectEqual(handle, reader.handles[0][0].file.?.handle);
    const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    // Deliberately tear every candidate payload. No torn record is evidence.
    const broken: [8 * 3000]u8 = @splat('!');
    try file.writePositionalAll(io, &broken, prefix + 8 * 80);
    try std.testing.expect(reader.load(a, io, &o, 0, key) == null);
}

test "workspace record memo owns bytes and remains bounded under replacement" {
    const a = std.testing.allocator;
    var memo: Memo = .{ .a = a, .io = std.testing.io };
    defer memo.deinit();
    var key: [32]u8 = @splat(0);
    var body: [4096]u8 = @splat('a');
    for (0..3000) |i| {
        std.mem.writeInt(u32, key[0..4], @intCast(i), .little);
        memo.put(key, &body);
        try std.testing.expect(memo.bytes <= Memo.limit);
    }
    body[0] = 'b';
    const read = memo.get(a, key).?;
    defer a.free(read);
    try std.testing.expectEqual(@as(u8, 'a'), read[0]);
    memo.put(key, "replacement");
    try std.testing.expectEqual(@as(u8, 'a'), read[0]);
}
