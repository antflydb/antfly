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

//! Source V5: eight-way content-addressed directory, densely packed variable-length records.
//! Each shard retains only its small index. Warm hits need one payload pread;
//! disk allocation follows occupied records rather than partially full buckets.
//! Publication follows payload writes, and every read checks the full identity,
//! metadata and payload digest. Concurrent eviction or torn writes are misses.
const std = @import("std");
const A = std.mem.Allocator;
const Options = @import("options.zig").Options;
const Codec = @import("artifact_cache.zig").Cache;
const shards = 16;
const buckets = 1024;
const ways = 8;
const entries = buckets / shards * ways;
const stride = 84;
const index_bytes = entries * stride;
const hint_stride = 16;
const hint_bytes = entries * hint_stride;
const prefix = 45056;
const page_size = 4096;
const data_limit = entries * page_size;
const magic = "AGSRC005";
const codec: Codec = .{ .directory = "", .namespace = "", .buckets = 1, .ways = 1, .limit = page_size };
const Shard = struct {
    mutex: std.Io.Mutex = .init,
    file: ?std.Io.File = null,
    writable: bool = false,
    valid_format: bool = false,
    directory: ?[]u8 = null,
    fn open(self: *Shard, a: A, io: std.Io, o: *const Options, number: usize, write: bool) ?std.Io.File {
        if (self.file != null and self.directory != null and (!write or self.writable)) return self.file;
        const path = std.fmt.allocPrint(a, "{s}/source-v5-{x:0>2}.bin", .{ o.cache_dir.?, number }) catch return null;
        defer a.free(path);
        const file = if (write) blk: {
            std.Io.Dir.cwd().createDirPath(io, o.cache_dir.?) catch return null;
            break :blk std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .truncate = false, .permissions = if (@import("builtin").os.tag == .windows) .default_file else .fromMode(0o600) }) catch return null;
        } else std.Io.Dir.cwd().openFile(io, path, .{}) catch return null;
        if (self.file) |old| old.close(io);
        self.file = file;
        self.writable = write;
        if (self.directory == null) self.directory = a.alloc(u8, hint_bytes) catch return null;
        @memset(self.directory.?, 0);
        self.valid_format = valid(io, file);
        if (self.valid_format) {
            var headers: [index_bytes]u8 = @splat(0);
            _ = file.readPositionalAll(io, &headers, 64) catch 0;
            for (0..entries) |i| hint(self.directory.?[i * hint_stride ..][0..hint_stride], headers[i * stride ..][0..stride]);
        }
        return file;
    }
};
fn valid(io: std.Io, file: std.Io.File) bool {
    var header: [20]u8 = undefined;
    return (file.readPositionalAll(io, &header, 0) catch 0) == header.len and
        std.mem.eql(u8, header[0..8], magic) and std.mem.readInt(u32, header[8..12], .little) == prefix and
        std.mem.readInt(u32, header[12..16], .little) == page_size and std.mem.readInt(u32, header[16..20], .little) == 1;
}
const Range = struct { start: usize, end: usize };
fn location(header: []const u8) ?Range {
    const value = std.mem.readInt(u32, header[80..84], .little);
    const len = std.mem.readInt(u32, header[64..68], .little);
    if (value == 0 or value > data_limit or len > body_limit or !std.mem.eql(u8, header[76..80], "AGB1")) return null;
    const start = value - 1;
    const size = 80 + len;
    if (size > data_limit - start) return null;
    return .{ .start = start, .end = start + size };
}
fn hint(out: []u8, header: []const u8) void {
    @memset(out, 0);
    if (location(header)) |span| {
        @memcpy(out[0..8], header[2..10]);
        std.mem.writeInt(u32, out[8..12], @intCast(span.start + 1), .little);
        std.mem.writeInt(u16, out[12..14], @intCast(span.end - span.start), .little);
    }
}
/// Find a free extent, excluding the replaced record. Fragmented or corrupt
/// cache state can cause a miss, but can never make the shard exceed its cap.
fn freeExtent(directory: []const u8, excluded: usize, needed: usize) ?usize {
    var used: [entries]Range = undefined;
    var count: usize = 0;
    for (0..entries) |i| {
        if (i == excluded) continue;
        if (location(directory[i * stride ..][0..stride])) |span| {
            used[count] = span;
            count += 1;
        }
    }
    std.mem.sort(Range, used[0..count], {}, struct {
        fn less(_: void, a: Range, b: Range) bool {
            return a.start < b.start;
        }
    }.less);
    var cursor: usize = 0;
    for (used[0..count]) |span| {
        if (span.start > cursor and span.start - cursor >= needed) return cursor;
        cursor = @max(cursor, span.end);
    }
    return if (needed <= data_limit - cursor) cursor else null;
}
pub const body_limit = page_size - 80;
pub const disk_limit = shards * (prefix + entries * page_size);
pub const memory_limit = shards * hint_bytes;
pub const Store = struct {
    a: A,
    handles: [shards]Shard = @splat(.{}),
    pub fn deinit(self: *Store, io: std.Io) void {
        for (self.handles) |shard| {
            if (shard.file) |file| file.close(io);
            if (shard.directory) |directory| self.a.free(directory);
        }
    }
    pub fn load(self: *Store, a: A, io: std.Io, o: *const Options, key: [32]u8) ?[]const u8 {
        const bucket = std.mem.readInt(u16, key[0..2], .little) % buckets;
        const shard = &self.handles[bucket % shards];
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        const file = shard.open(self.a, io, o, bucket % shards, false) orelse return null;
        if (!shard.valid_format) {
            if (!valid(io, file)) return null;
            shard.valid_format = true;
        }
        const local = bucket / shards;
        const hints = shard.directory.?[local * ways * hint_stride ..][0 .. ways * hint_stride];
        // Fingerprints are routing hints only. The full key and digest in the
        // selected record reject collisions, eviction races and torn writes.
        for (0..2) |attempt| {
            for (0..ways) |way| {
                const h = hints[way * hint_stride ..][0..hint_stride];
                const position = std.mem.readInt(u32, h[8..12], .little);
                const size = std.mem.readInt(u16, h[12..14], .little);
                if (position == 0 or position > data_limit or size < 80 or size > page_size or size > data_limit - (position - 1) or !std.mem.eql(u8, h[0..8], key[2..10])) continue;
                var record: [page_size]u8 = undefined;
                const n = file.readPositionalAll(io, record[0..size], prefix + @as(u64, position - 1)) catch 0;
                if (codec.fixedGet(a, key, record[0..n])) |body| return body;
            }
            if (attempt == 0) {
                var headers: [ways * stride]u8 = @splat(0);
                _ = file.readPositionalAll(io, &headers, 64 + local * ways * stride) catch 0;
                for (0..ways) |way| hint(hints[way * hint_stride ..][0..hint_stride], headers[way * stride ..][0..stride]);
            }
        }
        return null;
    }
    pub fn store(self: *Store, io: std.Io, o: *const Options, key: [32]u8, body: []const u8) void {
        if (o.cached_only or body.len > body_limit) return;
        const bucket = std.mem.readInt(u16, key[0..2], .little) % buckets;
        const shard = &self.handles[bucket % shards];
        shard.mutex.lockUncancelable(io);
        defer shard.mutex.unlock(io);
        const file = shard.open(self.a, io, o, bucket % shards, true) orelse return;
        if (!(file.tryLock(io, .exclusive) catch false)) return;
        defer file.unlock(io);
        if (!valid(io, file)) {
            file.setLength(io, 0) catch return;
            file.setLength(io, prefix) catch return;
            var header: [64]u8 = @splat(0);
            header[0..8].* = magic.*;
            std.mem.writeInt(u32, header[8..12], prefix, .little);
            std.mem.writeInt(u32, header[12..16], page_size, .little);
            std.mem.writeInt(u32, header[16..20], 1, .little);
            file.writePositionalAll(io, &header, 0) catch return;
            @memset(shard.directory.?, 0);
        }
        shard.valid_format = true;
        const offset = bucket / shards * ways * stride;
        var headers: [ways * stride]u8 = @splat(0);
        _ = file.readPositionalAll(io, &headers, 64 + offset) catch 0;
        var selected: ?usize = null;
        var empty: ?usize = null;
        var oldest: i64 = std.math.maxInt(i64);
        var victim: usize = 0;
        for (0..ways) |way| {
            const h = headers[way * stride ..][0..stride];
            if (location(h) == null) {
                if (empty == null) empty = way;
            } else if (std.mem.eql(u8, h[0..32], &key)) {
                selected = way;
                break;
            } else {
                const stamp = std.mem.readInt(i64, h[68..76], .little);
                if (stamp < oldest) {
                    oldest = stamp;
                    victim = way;
                }
            }
        }
        const way = selected orelse empty orelse victim;
        const h = headers[way * stride ..][0..stride];
        const record_size = 80 + body.len;
        const position = blk: {
            if (location(h)) |span| if (span.end - span.start >= record_size) break :blk span.start;
            const size = (file.stat(io) catch return).size;
            if (size > prefix + data_limit) file.setLength(io, prefix + data_limit) catch return;
            const next = size -| prefix;
            if (next <= data_limit and record_size <= data_limit - next) break :blk @as(usize, @intCast(next));
            // Reuse unpublished extents and gaps from shorter replacements.
            // The directory has a fixed bound, so reclamation is bounded too.
            const directory = self.a.alloc(u8, index_bytes) catch return;
            defer self.a.free(directory);
            @memset(directory, 0);
            _ = file.readPositionalAll(io, directory, 64) catch 0;
            break :blk freeExtent(directory, bucket / shards * ways + way, record_size) orelse return;
        };
        var record: [page_size]u8 = @splat(0);
        codec.fixedPut(io, key, body, &record);
        file.writePositionalAll(io, record[0..record_size], prefix + position) catch return;
        var published: [stride]u8 = @splat(0);
        @memcpy(published[0..80], record[0..80]);
        std.mem.writeInt(u32, published[80..84], @intCast(position + 1), .little);
        file.writePositionalAll(io, &published, 64 + offset + way * stride) catch return;
        hint(shard.directory.?[(bucket / shards * ways + way) * hint_stride ..][0..hint_stride], &published);
    }
};

test "dense preparation records retain collisions, refresh readers and reject torn payloads" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const o: Options = .{ .cache_dir = directory };
    var writer: Store = .{ .a = a };
    defer writer.deinit(io);
    var reader: Store = .{ .a = a };
    defer reader.deinit(io);
    var key: [32]u8 = @splat(0);
    const body: [2700]u8 = @splat('s');
    for (0..8) |i| {
        key[31] = @intCast(i);
        writer.store(io, &o, key, &body);
        const value = reader.load(a, io, &o, key).?;
        defer a.free(value);
        try std.testing.expectEqualSlices(u8, &body, value);
    }
    for (0..8) |i| {
        key[31] = @intCast(i);
        const value = reader.load(a, io, &o, key).?;
        a.free(value);
    }
    const path = try std.fmt.allocPrint(a, "{s}/source-v5-00.bin", .{directory});
    defer a.free(path);
    const before = (try std.Io.Dir.cwd().statFile(io, path, .{})).size;
    try std.testing.expect(before <= prefix + 8 * page_size);
    key[31] = 9;
    writer.store(io, &o, key, "ninth");
    try std.testing.expectEqual(before, (try std.Io.Dir.cwd().statFile(io, path, .{})).size);
    const value = reader.load(a, io, &o, key).?;
    defer a.free(value);
    try std.testing.expectEqualStrings("ninth", value);
    const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    const broken: [page_size * 8]u8 = @splat('!');
    try file.writePositionalAll(io, &broken, prefix);
    try std.testing.expect(reader.load(a, io, &o, key) == null);
    // Simulate an interrupted publication leaving every remaining physical
    // page orphaned. New buckets reuse free slots at the hard file ceiling.
    try file.setLength(io, prefix + entries * page_size);
    key[0] = 16;
    writer.store(io, &o, key, "after crash");
    const recovered = reader.load(a, io, &o, key).?;
    defer a.free(recovered);
    try std.testing.expectEqualStrings("after crash", recovered);
    try std.testing.expectEqual(@as(u64, prefix + entries * page_size), (try file.stat(io)).size);
    var readonly = o;
    readonly.cached_only = true;
    const before_readonly = (try file.stat(io)).mtime;
    writer.store(io, &readonly, key, "never written");
    try std.testing.expectEqual(before_readonly, (try file.stat(io)).mtime);
}

test "independent preparation handles never publish another writers payload" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    var failed: std.atomic.Value(bool) = .init(false);
    const Worker = struct {
        fn run(path: []const u8, id: u8, failure: *std.atomic.Value(bool)) void {
            const alloc = std.heap.page_allocator;
            var store: Store = .{ .a = alloc };
            defer store.deinit(std.testing.io);
            const o: Options = .{ .cache_dir = path };
            var key: [32]u8 = @splat(0);
            key[31] = id;
            const body: [2700]u8 = @splat(id);
            for (0..64) |_| {
                store.store(std.testing.io, &o, key, &body);
                if (store.load(alloc, std.testing.io, &o, key)) |actual| {
                    defer alloc.free(actual);
                    if (!std.mem.eql(u8, actual, &body)) failure.store(true, .monotonic);
                }
            }
        }
    };
    var threads: [4]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    for (&threads, 0..) |*thread, i| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{ directory, @as(u8, @intCast(i + 1)), &failed });
        started += 1;
    }
    for (threads[0..started]) |thread| thread.join();
    started = 0;
    try std.testing.expect(!failed.load(.monotonic));
}

test "variable preparations pack small records and reclaim fragmented extents within the cap" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(directory);
    const o: Options = .{ .cache_dir = directory };
    var writer: Store = .{ .a = a };
    defer writer.deinit(io);
    var key: [32]u8 = @splat(0);
    writer.store(io, &o, key, "small");
    key[0] = 16;
    const medium: [1000]u8 = @splat('m');
    writer.store(io, &o, key, &medium);
    const path = try std.fmt.allocPrint(a, "{s}/source-v5-00.bin", .{directory});
    defer a.free(path);
    const file = try std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_write });
    defer file.close(io);
    try std.testing.expectEqual(@as(u64, prefix + 80 + 5 + 80 + 1000), (try file.stat(io)).size);
    try file.setLength(io, prefix + data_limit);
    key[0] = 0;
    const large: [3000]u8 = @splat('L');
    writer.store(io, &o, key, &large);
    const value = writer.load(a, io, &o, key).?;
    defer a.free(value);
    try std.testing.expectEqualSlices(u8, &large, value);
    key[0] = 32;
    writer.store(io, &o, key, "tiny");
    var header: [stride]u8 = undefined;
    try std.testing.expectEqual(@as(usize, stride), try file.readPositionalAll(io, &header, 64 + 2 * ways * stride));
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, header[80..84], .little));
    try std.testing.expectEqual(@as(u64, prefix + data_limit), (try file.stat(io)).size);
    // A reader rejects another format even when record checksums still verify,
    // and observes a subsequent repair by another existing handle.
    try file.writePositionalAll(io, &.{0}, 16);
    var reader: Store = .{ .a = a };
    defer reader.deinit(io);
    try std.testing.expect(reader.load(a, io, &o, key) == null);
    writer.store(io, &o, key, "repaired");
    const repaired = reader.load(a, io, &o, key).?;
    defer a.free(repaired);
    try std.testing.expectEqualStrings("repaired", repaired);
}
