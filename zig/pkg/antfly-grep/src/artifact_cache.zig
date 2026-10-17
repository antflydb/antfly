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
pub const Cache = struct {
    directory: []const u8,
    namespace: []const u8,
    buckets: u16,
    limit: usize,
    ways: usize = 4,
    fn path(self: Cache, a: A, key: [32]u8, way: usize) ![]u8 {
        return std.fmt.allocPrint(a, "{s}/{s}-{x:0>3}-{d}", .{ self.directory, self.namespace, std.mem.readInt(u16, key[0..2], .little) % self.buckets, way });
    }
    pub fn load(self: Cache, a: A, io: std.Io, key: [32]u8) ?[]const u8 {
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
    pub fn store(self: Cache, a: A, io: std.Io, key: [32]u8, body: []const u8) void {
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
            const file = std.Io.Dir.cwd().createFile(io, temporary, .{ .exclusive = true, .permissions = .fromMode(0o600) }) catch return;
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
    const cache = Cache{ .directory = directory, .namespace = "fixture", .buckets = 1, .limit = 256 };
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
