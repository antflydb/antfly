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

//! Fixed-size, disposable second-level score cache. Exact batch hits retain
//! their one-record fast path; individual scores survive changed batch shapes.
//! Four-way buckets coalesce reads; collisions evict, never alias. Checksummed records make torn
//! concurrent writes a miss without process locks or an append-only log.
const std = @import("std");
const Options = @import("options.zig").Options;
const A = std.mem.Allocator;
pub const Entry = struct { score: f64, written: i64 };
pub const Store = struct {
    file: ?std.Io.File = null,
    legacy: ?std.Io.File = null,
    legacy_opened: bool = false,
    writable: bool = false,
    write_failed: bool = false,
    const slots = 16384;
    const ways = 4;
    const record_size = 64;
    pub const max_bytes = slots * record_size * ways;
    pub fn deinit(self: *Store, io: std.Io) void {
        if (self.file) |file| file.close(io);
        if (self.legacy) |file| file.close(io);
    }
    fn path(a: A, o: *const Options) ![]u8 {
        return std.fs.path.join(a, &.{ o.cache_dir.?, "scores-v2.bin" });
    }
    fn readFile(self: *Store, a: A, io: std.Io, o: *const Options) ?std.Io.File {
        if (self.file == null) {
            const name = path(a, o) catch return null;
            defer a.free(name);
            self.file = std.Io.Dir.cwd().openFile(io, name, .{}) catch null;
        }
        return self.file;
    }
    // A failed open is a wave-local observation: another client may create
    // the table later. Callers probe once per wave, then reuse live descriptors.
    pub fn available(self: *Store, a: A, io: std.Io, o: *const Options) bool {
        const current = self.readFile(a, io, o);
        const old = self.readLegacyFile(a, io, o);
        return current != null or old != null;
    }
    fn readLegacyFile(self: *Store, a: A, io: std.Io, o: *const Options) ?std.Io.File {
        if (!self.legacy_opened) {
            self.legacy_opened = true;
            const name = std.fs.path.join(a, &.{ o.cache_dir.?, "scores-v1.bin" }) catch return null;
            defer a.free(name);
            self.legacy = std.Io.Dir.cwd().openFile(io, name, .{}) catch null;
        }
        return self.legacy;
    }
    fn writeFile(self: *Store, a: A, io: std.Io, o: *const Options) ?std.Io.File {
        if (self.writable) return self.file;
        if (self.write_failed) return null;
        self.write_failed = true;
        const name = path(a, o) catch return null;
        defer a.free(name);
        std.Io.Dir.cwd().createDirPath(io, o.cache_dir.?) catch return null;
        const file = std.Io.Dir.cwd().createFile(io, name, .{ .read = true, .truncate = false, .permissions = if (@import("builtin").os.tag == .windows) .default_file else .fromMode(0o600) }) catch return null;
        if (self.file) |previous| previous.close(io);
        self.file = file;
        self.writable = true;
        return file;
    }
    fn keyBytes(key_hex: *const [64]u8) [32]u8 {
        var key: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&key, key_hex) catch unreachable;
        return key;
    }
    fn offset(key: *const [32]u8) u64 {
        return @as(u64, std.mem.readInt(u16, key[0..2], .little) % slots) * record_size * ways;
    }
    fn checksum(bytes: []const u8) [16]u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        return digest[0..16].*;
    }
    pub fn get(self: *Store, a: A, io: std.Io, o: *const Options, key_hex: *const [64]u8) ?Entry {
        const key = keyBytes(key_hex);
        if (self.readFile(a, io, o)) |file| {
            var bucket: [record_size * ways]u8 = @splat(0);
            const n = file.readPositionalAll(io, &bucket, offset(&key)) catch 0;
            for (0..n / record_size) |way| if (valid(io, o, &key, bucket[way * record_size ..][0..record_size])) |entry| return entry;
        }
        _ = self.readLegacyFile(a, io, o);
        if (self.legacy) |file| {
            var record: [record_size]u8 = undefined;
            const old_offset = @as(u64, std.mem.readInt(u16, key[0..2], .little)) * record_size;
            if ((file.readPositionalAll(io, &record, old_offset) catch 0) == record_size) return valid(io, o, &key, &record);
        }
        return null;
    }
    fn valid(io: std.Io, o: *const Options, key: *const [32]u8, record: *const [record_size]u8) ?Entry {
        if (!std.mem.eql(u8, record[0..32], key) or !std.mem.eql(u8, record[48..64], &checksum(record[0..48]))) return null;
        const written = std.mem.readInt(i64, record[32..40], .little);
        const age = @as(i128, std.Io.Clock.real.now(io).toSeconds()) - written;
        if (age < -300 or age >= o.cache_ttl_seconds) return null;
        const score: f64 = @bitCast(std.mem.readInt(u64, record[40..48], .little));
        if (!std.math.isFinite(score) or (score < 0 and score != -1) or score > 1) return null;
        return .{ .score = score, .written = written };
    }
    pub fn put(self: *Store, a: A, io: std.Io, o: *const Options, key_hex: *const [64]u8, score: f64) void {
        self.putEntry(a, io, o, key_hex, .{ .score = score, .written = std.Io.Clock.real.now(io).toSeconds() });
    }
    pub fn putEntry(self: *Store, a: A, io: std.Io, o: *const Options, key_hex: *const [64]u8, entry: Entry) void {
        const score = entry.score;
        if (o.no_cache or o.cached_only or o.cache_ttl_seconds == 0 or o.cache_dir == null or !std.math.isFinite(score)) return;
        const file = self.writeFile(a, io, o) orelse return;
        const key = keyBytes(key_hex);
        var record: [record_size]u8 = undefined;
        record[0..32].* = key;
        std.mem.writeInt(i64, record[32..40], entry.written, .little);
        std.mem.writeInt(u64, record[40..48], @bitCast(score), .little);
        record[48..64].* = checksum(record[0..48]);
        var bucket: [record_size * ways]u8 = @splat(0);
        const n = file.readPositionalAll(io, &bucket, offset(&key)) catch 0;
        var selected: usize = 0;
        var oldest: i64 = std.math.maxInt(i64);
        for (0..ways) |way| {
            const prior = bucket[way * record_size ..][0..record_size];
            if ((way + 1) * record_size > n or std.mem.eql(u8, prior[0..32], &key) or !std.mem.eql(u8, prior[48..64], &checksum(prior[0..48]))) {
                selected = way;
                break;
            }
            const timestamp = std.mem.readInt(i64, prior[32..40], .little);
            if (timestamp < oldest) {
                oldest = timestamp;
                selected = way;
            }
        }
        file.writePositionalAll(io, &record, offset(&key) + selected * record_size) catch {};
    }
};

test "score buckets coalesce collisions without exceeding fixed capacity" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const io = std.testing.io;
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const options = Options{ .cache_dir = directory };
    var store: Store = .{};
    defer store.deinit(io);
    var keys: [5][64]u8 = undefined;
    for (&keys, 0..) |*key, i| {
        var digest: [32]u8 = @splat(0);
        digest[31] = @intCast(i);
        key.* = std.fmt.bytesToHex(digest, .lower);
    }
    for (keys[0..4], 0..) |*key, i| store.put(a, io, &options, key, @as(f64, @floatFromInt(i)) / 4);
    for (keys[0..4], 0..) |*key, i| try std.testing.expectEqual(@as(f64, @floatFromInt(i)) / 4, store.get(a, io, &options, key).?.score);
    store.put(a, io, &options, &keys[4], 0.9);
    try std.testing.expectEqual(@as(f64, 0.9), store.get(a, io, &options, &keys[4]).?.score);
    var hits: usize = 0;
    for (&keys) |*key| if (store.get(a, io, &options, key) != null) {
        hits += 1;
    };
    try std.testing.expectEqual(@as(usize, 4), hits);
    try std.testing.expect((try store.file.?.stat(io)).size <= Store.max_bytes);
}

test "score reader discovers a table created by another client" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const io = std.testing.io;
    const directory = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const options = Options{ .cache_dir = directory };
    var reader: Store = .{};
    defer reader.deinit(io);
    var writer: Store = .{};
    defer writer.deinit(io);
    const key: [64]u8 = @splat('a');
    try std.testing.expect(!reader.available(a, io, &options));
    writer.put(a, io, &options, &key, 0.75);
    try std.testing.expect(reader.available(a, io, &options));
    try std.testing.expectEqual(@as(f64, 0.75), reader.get(a, io, &options, &key).?.score);
}
