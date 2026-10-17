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
//! Direct-mapped collisions evict, never alias. Checksummed records make torn
//! concurrent writes a miss without process locks or an append-only log.
const std = @import("std");
const Options = @import("options.zig").Options;
const A = std.mem.Allocator;
pub const Entry = struct { score: f64, written: i64 };
pub const Store = struct {
    file: ?std.Io.File = null,
    opened: bool = false,
    writable: bool = false,
    write_failed: bool = false,
    const slots = 65536;
    const record_size = 64;
    pub const max_bytes = slots * record_size;
    pub fn deinit(self: *Store, io: std.Io) void {
        if (self.file) |file| file.close(io);
    }
    fn path(a: A, o: *const Options) ![]u8 {
        return std.fs.path.join(a, &.{ o.cache_dir.?, "scores-v1.bin" });
    }
    fn readFile(self: *Store, a: A, io: std.Io, o: *const Options) ?std.Io.File {
        if (!self.opened) {
            self.opened = true;
            const name = path(a, o) catch return null;
            defer a.free(name);
            self.file = std.Io.Dir.cwd().openFile(io, name, .{}) catch null;
        }
        return self.file;
    }
    fn writeFile(self: *Store, a: A, io: std.Io, o: *const Options) ?std.Io.File {
        if (self.writable) return self.file;
        if (self.write_failed) return null;
        self.write_failed = true;
        const name = path(a, o) catch return null;
        defer a.free(name);
        std.Io.Dir.cwd().createDirPath(io, o.cache_dir.?) catch return null;
        const file = std.Io.Dir.cwd().createFile(io, name, .{ .read = true, .truncate = false, .permissions = .fromMode(0o600) }) catch return null;
        if (self.file) |previous| previous.close(io);
        self.file = file;
        self.opened = true;
        self.writable = true;
        return file;
    }
    fn keyBytes(key_hex: *const [64]u8) [32]u8 {
        var key: [32]u8 = undefined;
        _ = std.fmt.hexToBytes(&key, key_hex) catch unreachable;
        return key;
    }
    fn offset(key: *const [32]u8) u64 {
        return @as(u64, std.mem.readInt(u16, key[0..2], .little)) * record_size;
    }
    fn checksum(bytes: []const u8) [16]u8 {
        var digest: [32]u8 = undefined;
        std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
        return digest[0..16].*;
    }
    pub fn get(self: *Store, a: A, io: std.Io, o: *const Options, key_hex: *const [64]u8) ?Entry {
        const file = self.readFile(a, io, o) orelse return null;
        const key = keyBytes(key_hex);
        var record: [record_size]u8 = undefined;
        if ((file.readPositionalAll(io, &record, offset(&key)) catch return null) != record_size) return null;
        if (!std.mem.eql(u8, record[0..32], &key) or !std.mem.eql(u8, record[48..64], &checksum(record[0..48]))) return null;
        const written = std.mem.readInt(i64, record[32..40], .little);
        const now = std.Io.Clock.real.now(io).toSeconds();
        const age = @as(i128, now) - written;
        if (age < -300 or age >= o.cache_ttl_seconds) return null;
        const score: f64 = @bitCast(std.mem.readInt(u64, record[40..48], .little));
        if (!std.math.isFinite(score) or (score < 0 and score != -1) or score > 1) return null;
        return .{ .score = score, .written = written };
    }
    pub fn put(self: *Store, a: A, io: std.Io, o: *const Options, key_hex: *const [64]u8, score: f64) void {
        if (o.no_cache or o.cached_only or o.cache_ttl_seconds == 0 or o.cache_dir == null or !std.math.isFinite(score)) return;
        const file = self.writeFile(a, io, o) orelse return;
        const key = keyBytes(key_hex);
        var record: [record_size]u8 = undefined;
        record[0..32].* = key;
        std.mem.writeInt(i64, record[32..40], std.Io.Clock.real.now(io).toSeconds(), .little);
        std.mem.writeInt(u64, record[40..48], @bitCast(score), .little);
        record[48..64].* = checksum(record[0..48]);
        file.writePositionalAll(io, &record, offset(&key)) catch {};
    }
};
