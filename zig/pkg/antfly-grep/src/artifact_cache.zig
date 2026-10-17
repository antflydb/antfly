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

//! Bounded associative atomic artifact cache. Complete keys and checksums make
//! collisions, concurrent replacements and corrupt records harmless misses.
const std = @import("std");
const A = std.mem.Allocator;
// One lazy inventory avoids dozens of negative legacy probes per new key.
// New-format writers never create legacy names; a new invocation rechecks.
pub const LegacyPresence = struct {
    batches: bool = false,
    buckets: bool = false,
    scalars: bool = false,
    sources: bool = false,
    pub fn scan(io: std.Io, directory: []const u8) LegacyPresence {
        var result: LegacyPresence = .{};
        var dir = std.Io.Dir.cwd().openDir(io, directory, .{ .iterate = true }) catch return result;
        defer dir.close(io);
        var iterator = dir.iterate();
        while (iterator.next(io) catch null) |entry| {
            if (std.mem.startsWith(u8, entry.name, "batch-v8-") or std.mem.startsWith(u8, entry.name, "batch-v7-") or std.mem.startsWith(u8, entry.name, "batch-v6-")) result.batches = true;
            if (std.mem.startsWith(u8, entry.name, "batch-v9-")) result.buckets = true;
            if (std.mem.startsWith(u8, entry.name, "source-v2-")) result.sources = true;
            if (entry.name.len == 64) result.scalars = true;
        }
        return result;
    }
};
// Bucket headers are contiguous: one read probes every way, followed by
// one bounded payload read. Small fixed slots bound allocation and disk size;
// large artifacts use compact atomic buckets to avoid allocated sparse gaps.
// No shared lock, descriptor cache, mmap, directory scan or startup sweep.
// A checksum covers identity + length + age + body; racing/torn in-place
// writes can evict or miss, but cannot turn one key's payload into another's.
pub const Cache = struct {
    directory: []const u8,
    namespace: []const u8,
    buckets: u16,
    limit: usize,
    ways: usize = 4,
    const header_size = 80;
    const max_ways = 16;
    fn path(self: Cache, a: A, key: [32]u8) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/{s}-{x:0>3}.bin", .{ self.directory, self.namespace, std.mem.readInt(u16, key[0..2], .little) % self.buckets });
    }
    fn bodyOffset(self: Cache, way: usize) u64 {
        return self.ways * header_size + way * (self.limit - header_size);
    }
    fn payloadOffset(self: Cache, headers: []const u8, way: usize) u64 {
        if (!std.mem.eql(u8, headers[way * header_size + 76 ..][0..4], "AGP1")) return self.bodyOffset(way);
        var offset: u64 = self.ways * header_size;
        for (0..way) |prior| {
            const h = headers[prior * header_size ..][0..header_size];
            if (self.validHeader(h)) offset += std.mem.readInt(u32, h[64..68], .little);
        }
        return offset;
    }
    fn digest(header: *const [header_size]u8, body: []const u8) [32]u8 {
        var hash = std.crypto.hash.sha2.Sha256.init(.{});
        hash.update(header[0..32]);
        hash.update(header[64..80]);
        hash.update(body);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
    fn validHeader(self: Cache, header: *const [header_size]u8) bool {
        return (std.mem.eql(u8, header[76..80], "AGB1") or std.mem.eql(u8, header[76..80], "AGP1")) and std.mem.readInt(u32, header[64..68], .little) <= self.limit - header_size;
    }
    /// Decode an already-read fixed bucket. Identity, metadata, and payload
    /// share one checksum; short/torn pages are disposable misses.
    pub fn fixedGet(self: Cache, a: A, key: [32]u8, bucket: []const u8) ?[]const u8 {
        if (bucket.len < self.ways * header_size) return null;
        for (0..self.ways) |way| {
            const h = bucket[way * header_size ..][0..header_size];
            if (!self.validHeader(h) or !std.mem.eql(u8, h[76..80], "AGB1") or !std.mem.eql(u8, h[0..32], &key)) continue;
            const offset = self.bodyOffset(way);
            const len = std.mem.readInt(u32, h[64..68], .little);
            if (offset > bucket.len or len > bucket.len - offset) continue;
            const body = bucket[@intCast(offset)..][0..len];
            if (std.mem.eql(u8, h[32..64], &digest(h, body))) return a.dupe(u8, body) catch null;
        }
        return null;
    }
    pub fn fixedPut(self: Cache, io: std.Io, key: [32]u8, body: []const u8, bucket: []u8) void {
        std.debug.assert(bucket.len == self.ways * self.limit and body.len <= self.limit - header_size);
        var selected: ?usize = null;
        var empty: ?usize = null;
        var oldest: i64 = std.math.maxInt(i64);
        var victim: usize = 0;
        for (0..self.ways) |way| {
            const h = bucket[way * header_size ..][0..header_size];
            if (!self.validHeader(h)) {
                if (empty == null) empty = way;
                continue;
            }
            if (std.mem.eql(u8, h[0..32], &key)) {
                selected = way;
                break;
            }
            const written = std.mem.readInt(i64, h[68..76], .little);
            if (written < oldest) {
                oldest = written;
                victim = way;
            }
        }
        const way = selected orelse empty orelse victim;
        const h = bucket[way * header_size ..][0..header_size];
        @memset(h, 0);
        h[0..32].* = key;
        std.mem.writeInt(u32, h[64..68], @intCast(body.len), .little);
        std.mem.writeInt(i64, h[68..76], @intCast(std.Io.Clock.real.now(io).nanoseconds), .little);
        h[76..80].* = "AGB1".*;
        h[32..64].* = digest(h, body);
        @memcpy(bucket[@intCast(self.bodyOffset(way))..][0..body.len], body);
    }
    /// Borrow a verified record from an immutable bucket snapshot.
    pub fn view(self: Cache, key: [32]u8, bytes: []const u8) ?[]const u8 {
        if (bytes.len < self.ways * header_size) return null;
        for (0..self.ways) |way| {
            const h = bytes[way * header_size ..][0..header_size];
            if (!self.validHeader(h) or !std.mem.eql(u8, h[0..32], &key)) continue;
            const offset = self.payloadOffset(bytes, way);
            const length = std.mem.readInt(u32, h[64..68], .little);
            if (offset > bytes.len or length > bytes.len - offset) continue;
            const body = bytes[@intCast(offset)..][0..length];
            if (std.mem.eql(u8, h[32..64], &digest(h, body))) return body;
        }
        return null;
    }
    pub fn load(self: Cache, a: A, io: std.Io, key: [32]u8) ?[]const u8 {
        std.debug.assert(self.ways <= max_ways and self.limit >= header_size);
        const name = self.path(a, key) catch return null;
        defer a.free(name);
        const file = std.Io.Dir.cwd().openFile(io, name, .{}) catch return null;
        defer file.close(io);
        return self.loadFile(a, io, file, key);
    }
    /// Caller owns the descriptor; read just the headers and selected payload.
    pub fn loadFile(self: Cache, a: A, io: std.Io, file: std.Io.File, key: [32]u8) ?[]const u8 {
        var headers: [header_size * max_ways]u8 = @splat(0);
        const n = file.readPositionalAll(io, headers[0 .. self.ways * header_size], 0) catch return null;
        for (0..n / header_size) |way| {
            const header = headers[way * header_size ..][0..header_size];
            if (!self.validHeader(header) or !std.mem.eql(u8, header[0..32], &key)) continue;
            const len = std.mem.readInt(u32, header[64..68], .little);
            const body = a.alloc(u8, len) catch return null;
            if ((file.readPositionalAll(io, body, self.payloadOffset(&headers, way)) catch 0) == len and std.mem.eql(u8, header[32..64], &digest(header, body))) return body;
            a.free(body);
        }
        return null;
    }
    pub fn store(self: Cache, a: A, io: std.Io, key: [32]u8, body: []const u8) void {
        std.debug.assert(self.ways <= max_ways and self.limit >= header_size);
        if (body.len > self.limit - header_size) return;
        if (self.limit > 4096) return self.storePacked(a, io, key, body);
        std.Io.Dir.cwd().createDirPath(io, self.directory) catch return;
        const name = self.path(a, key) catch return;
        defer a.free(name);
        const file = std.Io.Dir.cwd().createFile(io, name, .{ .read = true, .truncate = false, .permissions = if (@import("builtin").os.tag == .windows) .default_file else .fromMode(0o600) }) catch return;
        defer file.close(io);
        var headers: [header_size * max_ways]u8 = @splat(0);
        const n = file.readPositionalAll(io, headers[0 .. self.ways * header_size], 0) catch 0;
        var selected: usize = 0;
        var oldest: i64 = std.math.maxInt(i64);
        for (0..self.ways) |way| {
            const header = headers[way * header_size ..][0..header_size];
            if ((way + 1) * header_size > n or !self.validHeader(header) or std.mem.eql(u8, header[0..32], &key)) {
                selected = way;
                break;
            }
            const written = std.mem.readInt(i64, header[68..76], .little);
            if (written < oldest) {
                oldest = written;
                selected = way;
            }
        }
        var header: [header_size]u8 = @splat(0);
        header[0..32].* = key;
        std.mem.writeInt(u32, header[64..68], @intCast(body.len), .little);
        std.mem.writeInt(i64, header[68..76], @intCast(std.Io.Clock.real.now(io).nanoseconds), .little);
        header[76..80].* = "AGB1".*;
        header[32..64].* = digest(&header, body);
        file.writePositionalAll(io, body, self.bodyOffset(selected)) catch return;
        file.writePositionalAll(io, &header, selected * header_size) catch {};
    }
    fn storePacked(self: Cache, a: A, io: std.Io, key: [32]u8, body: []const u8) void {
        std.Io.Dir.cwd().createDirPath(io, self.directory) catch return;
        const name = self.path(a, key) catch return;
        defer a.free(name);
        const previous = std.Io.Dir.cwd().readFileAlloc(io, name, a, .limited(self.ways * self.limit)) catch null;
        defer if (previous) |bytes| a.free(bytes);
        var headers: [header_size * max_ways]u8 = @splat(0);
        var records: [max_ways]?[]const u8 = @splat(null);
        if (previous) |bytes| if (bytes.len >= self.ways * header_size) {
            @memcpy(headers[0 .. self.ways * header_size], bytes[0 .. self.ways * header_size]);
            for (0..self.ways) |way| {
                const h = headers[way * header_size ..][0..header_size];
                if (!self.validHeader(h)) continue;
                const offset = self.payloadOffset(&headers, way);
                const len = std.mem.readInt(u32, h[64..68], .little);
                if (offset > bytes.len or len > bytes.len - offset) continue;
                const payload = bytes[@intCast(offset)..][0..len];
                if (std.mem.eql(u8, h[32..64], &digest(h, payload))) records[way] = payload;
            }
        };
        var selected: usize = 0;
        var oldest: i64 = std.math.maxInt(i64);
        for (0..self.ways) |way| {
            const h = headers[way * header_size ..][0..header_size];
            if (records[way] == null or std.mem.eql(u8, h[0..32], &key)) {
                selected = way;
                break;
            }
            const written = std.mem.readInt(i64, h[68..76], .little);
            if (written < oldest) {
                oldest = written;
                selected = way;
            }
        }
        records[selected] = body;
        const h = headers[selected * header_size ..][0..header_size];
        h[0..32].* = key;
        std.mem.writeInt(u32, h[64..68], @intCast(body.len), .little);
        std.mem.writeInt(i64, h[68..76], @intCast(std.Io.Clock.real.now(io).nanoseconds), .little);
        var size = self.ways * header_size;
        for (records[0..self.ways]) |record| if (record) |payload| {
            size += payload.len;
        };
        const encoded = a.alloc(u8, size) catch return;
        defer a.free(encoded);
        @memset(encoded[0 .. self.ways * header_size], 0);
        var offset = self.ways * header_size;
        for (records[0..self.ways], 0..) |record, way| if (record) |payload| {
            const header = headers[way * header_size ..][0..header_size];
            header[76..80].* = "AGP1".*;
            header[32..64].* = digest(header, payload);
            @memcpy(encoded[way * header_size ..][0..header_size], header);
            @memcpy(encoded[offset..][0..payload.len], payload);
            offset += payload.len;
        };
        var nonce: [12]u8 = undefined;
        io.random(&nonce);
        const temporary = std.fmt.allocPrint(a, "{s}.{s}.tmp", .{ name, std.fmt.bytesToHex(nonce, .lower) }) catch return;
        defer a.free(temporary);
        const file = std.Io.Dir.cwd().createFile(io, temporary, .{ .exclusive = true, .permissions = if (@import("builtin").os.tag == .windows) .default_file else .fromMode(0o600) }) catch return;
        defer std.Io.Dir.cwd().deleteFile(io, temporary) catch {};
        {
            defer file.close(io);
            file.writeStreamingAll(io, encoded) catch return;
        }
        std.Io.Dir.cwd().rename(temporary, .cwd(), name, io) catch {};
    }
};

// Read/migrate the previous immutable per-way files. Kept separate so new
// writes never recreate the old inode-heavy layout.
pub const Legacy = struct {
    directory: []const u8,
    namespace: []const u8,
    buckets: u16,
    limit: usize,
    ways: usize = 4,
    fn path(self: Legacy, a: A, key: [32]u8, way: usize) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/{s}-{x:0>3}-{d}", .{ self.directory, self.namespace, std.mem.readInt(u16, key[0..2], .little) % self.buckets, way });
    }
    pub fn load(self: Legacy, a: A, io: std.Io, key: [32]u8) ?[]const u8 {
        for (0..self.ways) |way| {
            const name = self.path(a, key, way) catch return null;
            defer a.free(name);
            const file = std.Io.Dir.cwd().openFile(io, name, .{}) catch continue;
            defer file.close(io);
            var h: [64]u8 = undefined;
            if ((file.readPositionalAll(io, &h, 0) catch continue) != 64 or !std.mem.eql(u8, h[0..32], &key)) continue;
            const stat = file.stat(io) catch continue;
            if (stat.size < 64 or stat.size > self.limit) continue;
            const body = a.alloc(u8, @intCast(stat.size - 64)) catch return null;
            if ((file.readPositionalAll(io, body, 64) catch 0) != body.len) {
                a.free(body);
                continue;
            }
            var digest: [32]u8 = undefined;
            std.crypto.hash.sha2.Sha256.hash(body, &digest, .{});
            if (!std.mem.eql(u8, h[32..64], &digest)) {
                a.free(body);
                continue;
            }
            return body;
        }
        return null;
    }
    pub fn store(self: Legacy, a: A, io: std.Io, key: [32]u8, body: []const u8) void {
        if (body.len > self.limit - 64) return;
        std.Io.Dir.cwd().createDirPath(io, self.directory) catch return;
        var selected: usize = 0;
        var oldest: i128 = std.math.maxInt(i128);
        for (0..self.ways) |way| {
            const name = self.path(a, key, way) catch return;
            defer a.free(name);
            const file = std.Io.Dir.cwd().openFile(io, name, .{}) catch {
                selected = way;
                break;
            };
            defer file.close(io);
            var h: [64]u8 = undefined;
            if ((file.readPositionalAll(io, &h, 0) catch 0) != 64 or std.mem.eql(u8, h[0..32], &key)) {
                selected = way;
                break;
            }
            const stat = file.stat(io) catch {
                selected = way;
                break;
            };
            if (stat.mtime.nanoseconds < oldest) {
                oldest = stat.mtime.nanoseconds;
                selected = way;
            }
        }
        const name = self.path(a, key, selected) catch return;
        defer a.free(name);
        var nonce: [12]u8 = undefined;
        io.random(&nonce);
        const temporary = std.fmt.allocPrint(a, "{s}.{s}.tmp", .{ name, std.fmt.bytesToHex(nonce, .lower) }) catch return;
        defer a.free(temporary);
        defer std.Io.Dir.cwd().deleteFile(io, temporary) catch {};
        {
            const file = std.Io.Dir.cwd().createFile(io, temporary, .{ .exclusive = true, .permissions = if (@import("builtin").os.tag == .windows) .default_file else .fromMode(0o600) }) catch return;
            defer file.close(io);
            var h: [64]u8 = undefined;
            h[0..32].* = key;
            std.crypto.hash.sha2.Sha256.hash(body, h[32..64], .{});
            file.writeStreamingAll(io, &h) catch return;
            file.writeStreamingAll(io, body) catch return;
        }
        std.Io.Dir.cwd().rename(temporary, .cwd(), name, io) catch {};
    }
};

test "associative artifacts retain collisions and bound replacement" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const io = std.testing.io;
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const cache = Legacy{ .directory = directory, .namespace = "fixture", .buckets = 1, .limit = 256 };
    var keys: [5][32]u8 = @splat(@splat(0));
    for (&keys, 0..) |*key, i| key[31] = @intCast(i);
    for (keys[0..4]) |key| cache.store(a, io, key, "prepared");
    for (keys[0..4]) |key| try std.testing.expectEqualStrings("prepared", cache.load(a, io, key).?);
    cache.store(a, io, keys[4], "new");
    try std.testing.expectEqualStrings("new", cache.load(a, io, keys[4]).?);
    var count: usize = 0;
    for (keys) |key| if (cache.load(a, io, key) != null) {
        count += 1;
    };
    try std.testing.expectEqual(@as(usize, 4), count);
    cache.store(a, io, keys[4], &@as([256]u8, @splat(0)));
    try std.testing.expectEqualStrings("new", cache.load(a, io, keys[4]).?);
    const name = try cache.path(a, keys[4], 0);
    // Corruption in any way cannot become a hit for another complete identity.
    const file = try std.Io.Dir.cwd().createFile(io, name, .{});
    try file.writeStreamingAll(io, "broken");
    file.close(io);
    var absent = keys[4];
    absent[30] = 99;
    try std.testing.expect(cache.load(a, io, absent) == null);
}

test "bucket artifacts isolate identities and reject torn headers and bodies" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const io = std.testing.io;
    const cache = Cache{ .directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path}), .namespace = "bucket", .buckets = 1, .ways = 4, .limit = 256 };
    var keys: [5][32]u8 = @splat(@splat(0));
    for (&keys, 0..) |*key, i| key[31] = @intCast(i);
    for (keys[0..4], 0..) |key, i| cache.store(a, io, key, &@as([100]u8, @splat(@intCast(i))));
    for (keys[0..4], 0..) |key, i| try std.testing.expectEqualSlices(u8, &@as([100]u8, @splat(@intCast(i))), cache.load(a, io, key).?);
    const name = try cache.path(a, keys[0]);
    const file = try std.Io.Dir.cwd().openFile(io, name, .{ .mode = .read_write });
    defer file.close(io);
    try std.testing.expect((try file.stat(io)).size <= cache.ways * cache.limit);
    // Publish key B's header identity over key A's checksum/body: must miss.
    try file.writePositionalAll(io, &keys[4], 0);
    try std.testing.expect(cache.load(a, io, keys[4]) == null);
    try std.testing.expect(cache.load(a, io, keys[0]) == null);
    try file.writePositionalAll(io, &keys[0], 0);
    try std.testing.expect(cache.load(a, io, keys[0]) != null);
    // A torn payload cannot be returned under an intact header.
    try file.writePositionalAll(io, "bad", cache.bodyOffset(0));
    try std.testing.expect(cache.load(a, io, keys[0]) == null);
    cache.store(a, io, keys[4], "replacement");
    try std.testing.expectEqualStrings("replacement", cache.load(a, io, keys[4]).?);
    cache.store(a, io, keys[4], &@as([256]u8, @splat(0)));
    try std.testing.expectEqualStrings("replacement", cache.load(a, io, keys[4]).?);
}

test "concurrent bucket readers never observe another identity's payload" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const io = std.testing.io;
    const directory = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(directory);
    var failed = std.atomic.Value(bool).init(false);
    const Worker = struct {
        cache: Cache,
        io: std.Io,
        id: u8,
        failed: *std.atomic.Value(bool),
        fn run(self: @This()) void {
            const a = std.heap.page_allocator;
            var key: [32]u8 = @splat(0);
            key[31] = self.id;
            const body: [128]u8 = @splat(self.id);
            for (0..80) |iteration| {
                const wanted = body[0 .. 64 + iteration % 64];
                self.cache.store(a, self.io, key, wanted);
                if (self.cache.load(a, self.io, key)) |actual| {
                    defer a.free(actual);
                    for (actual) |byte| if (byte != self.id) {
                        self.failed.store(true, .monotonic);
                    };
                }
            }
        }
    };
    const cache = Cache{ .directory = directory, .namespace = "racing", .buckets = 1, .ways = 4, .limit = 256 };
    var threads: [4]std.Thread = undefined;
    var started: usize = 0;
    defer for (threads[0..started]) |thread| thread.join();
    for (&threads, 0..) |*thread, i| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{Worker{ .cache = cache, .io = io, .id = @intCast(i + 1), .failed = &failed }});
        started += 1;
    }
    for (threads[0..started]) |thread| thread.join();
    started = 0;
    try std.testing.expect(!failed.load(.monotonic));
}

test "large artifact buckets pack bodies without sparse slot gaps" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const io = std.testing.io;
    const cache = Cache{ .directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path}), .namespace = "packed", .buckets = 1, .limit = 256 * 1024 };
    var keys: [4][32]u8 = @splat(@splat(0));
    for (&keys, 0..) |*key, i| {
        key[31] = @intCast(i);
        cache.store(a, io, key.*, "small source preparation");
    }
    for (keys) |key| try std.testing.expectEqualStrings("small source preparation", cache.load(a, io, key).?);
    const name = try cache.path(a, keys[0]);
    try std.testing.expectEqual(@as(u64, 4 * 80 + 4 * "small source preparation".len), (try std.Io.Dir.cwd().statFile(io, name, .{})).size);
}
