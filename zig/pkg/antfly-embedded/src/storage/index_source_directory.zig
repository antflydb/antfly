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

//! Bounded authenticated directory for large immutable source maps.
//! Page hashes stream through four fixed fanout buffers; lookup and collection
//! use deterministic coordinates, without retaining a corpus-sized directory.
const std = @import("std");
const publication = @import("index_publication.zig");
const Digest = publication.Digest;
pub const fanout = 128;
pub const maximum_levels = 4;
pub const maximum_node_bytes = 38 + fanout * 32;
const prefix = "index_source_directory_v1:";
pub fn key(binding: publication.Binding, level: u8, number: u32) [prefix.len + 69]u8 {
    var out: [prefix.len + 69]u8 = undefined;
    @memcpy(out[0..prefix.len], prefix);
    @memcpy(out[prefix.len..][0..32], &binding.owner);
    @memcpy(out[prefix.len + 32 ..][0..32], &binding.publication);
    out[prefix.len + 64] = level;
    std.mem.writeInt(u32, out[prefix.len + 65 ..][0..4], number, .big);
    return out;
}
fn digest(bytes: []const u8) Digest {
    var result: Digest = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &result, .{});
    return result;
}
fn boundDigest(address: []const u8, bytes: []const u8) Digest {
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    hash.update(address);
    hash.update(bytes);
    return hash.finalResult();
}
pub fn nodesAt(pages: u32, level: u8) u32 {
    var nodes = pages;
    for (0..@as(usize, level) + 1) |_| nodes = (nodes - 1) / fanout + 1;
    return nodes;
}
pub fn levels(pages: u32) !u8 {
    if (pages == 0) return error.InvalidIndexSourceMap;
    var count: u8 = 1;
    while (nodesAt(pages, count - 1) > 1) : (count += 1) {
        if (count == maximum_levels) return error.InvalidIndexSourceMap;
    }
    return count;
}
pub fn nodeCount(pages: u32) !u32 {
    const depth = try levels(pages);
    var total: u32 = 0;
    for (0..depth) |level| total = try std.math.add(u32, total, nodesAt(pages, @intCast(level)));
    return total;
}
/// Writer is borrowed synchronously. Null writers prepare exactly the same root
/// without staging metadata. Once emitted, a bucket can be reused immediately.
pub const Builder = struct {
    binding: publication.Binding,
    pages: u32,
    depth: u8,
    seen: u32 = 0,
    used: [maximum_levels]u16 = @splat(0),
    emitted: [maximum_levels]u32 = @splat(0),
    buffers: [maximum_levels][maximum_node_bytes]u8 = undefined,
    root: ?Digest = null,
    pub fn init(binding: publication.Binding, pages: u32) !Builder {
        if (!binding.valid()) return error.InvalidIndexPublication;
        return .{ .binding = binding, .pages = pages, .depth = try levels(pages) };
    }
    pub fn append(self: *Builder, page_digest: Digest, writer: anytype) !void {
        if (self.seen >= self.pages) return error.InvalidIndexSourceMap;
        try self.add(0, page_digest, writer);
        self.seen += 1;
    }
    fn add(self: *Builder, level: u8, child: Digest, writer: anytype) anyerror!void {
        if (level >= self.depth) return error.InvalidIndexSourceMap;
        const offset = 6 + @as(usize, self.used[level]) * 32;
        @memcpy(self.buffers[level][offset..][0..32], &child);
        self.used[level] += 1;
        if (self.used[level] == fanout) try self.flush(level, writer);
    }
    fn flush(self: *Builder, level: u8, writer: anytype) anyerror!void {
        const children = self.used[level];
        if (children == 0) return;
        const raw = self.buffers[level][0 .. 38 + @as(usize, children) * 32];
        @memcpy(raw[0..4], "ISD1");
        std.mem.writeInt(u16, raw[4..6], children, .little);
        const address = key(self.binding, level, self.emitted[level]);
        @memcpy(raw[raw.len - 32 ..], &boundDigest(&address, raw[0 .. raw.len - 32]));
        const hash = digest(raw);
        if (comptime @TypeOf(writer) != @TypeOf(null)) try writer.put(&address, raw);
        self.used[level] = 0;
        self.emitted[level] += 1;
        if (level + 1 == self.depth) {
            if (self.root != null) return error.InvalidIndexSourceMap;
            self.root = hash;
        } else try self.add(level + 1, hash, writer);
    }
    pub fn finish(self: *Builder, writer: anytype) !Digest {
        if (self.seen != self.pages) return error.InvalidIndexSourceMap;
        for (0..self.depth) |level| {
            try self.flush(@intCast(level), writer);
            if (self.emitted[level] != nodesAt(self.pages, @intCast(level))) return error.InvalidIndexSourceMap;
        }
        return self.root orelse error.InvalidIndexSourceMap;
    }
};
pub fn lookup(reader: anytype, binding: publication.Binding, pages: u32, root: Digest, page: u32) !Digest {
    const depth = try levels(pages);
    if (page >= pages) return error.InvalidIndexSourceMap;
    var expected = root;
    var level = depth;
    while (level > 0) {
        level -= 1;
        var stride: u32 = 1;
        for (0..level) |_| stride *= fanout;
        const ordinal = page / stride;
        const number = ordinal / fanout;
        const child = ordinal % fanout;
        const children = if (level == 0) pages else nodesAt(pages, level - 1);
        const count = @min(fanout, children - number * fanout);
        const address = key(binding, level, number);
        const raw = try reader.get(&address);
        if (raw.len != 38 + @as(usize, count) * 32 or !std.mem.eql(u8, raw[0..4], "ISD1") or
            std.mem.readInt(u16, raw[4..6], .little) != count or !std.mem.eql(u8, &expected, &digest(raw)) or
            !std.mem.eql(u8, raw[raw.len - 32 ..], &boundDigest(&address, raw[0 .. raw.len - 32]))) return error.InvalidIndexSourceMap;
        expected = raw[6 + @as(usize, child) * 32 ..][0..32].*;
    }
    return expected;
}
/// Delete a contiguous node frontier after leaf pages have been collected.
/// The enclosing source map commits this frontier with its retirement proof.
pub fn deleteAt(writer: anytype, binding: publication.Binding, pages: u32, ordinal: u32) !void {
    const depth = try levels(pages);
    var within = ordinal;
    for (0..depth) |level| {
        const count = nodesAt(pages, @intCast(level));
        if (within < count) {
            writer.delete(&key(binding, @intCast(level), within)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            return;
        }
        within -= count;
    }
    return error.InvalidIndexSourceMap;
}

const TestStore = struct {
    allocator: std.mem.Allocator,
    values: std.StringHashMapUnmanaged([]u8) = .empty,
    calls: usize = 0,
    scratch: [maximum_node_bytes]u8 = undefined,
    fn deinit(self: *TestStore) void {
        var it = self.values.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.values.deinit(self.allocator);
    }
    pub fn put(self: *TestStore, address: []const u8, bytes: []const u8) !void {
        const owned_key = try self.allocator.dupe(u8, address);
        errdefer self.allocator.free(owned_key);
        const owned_bytes = try self.allocator.dupe(u8, bytes);
        errdefer self.allocator.free(owned_bytes);
        const slot = try self.values.getOrPut(self.allocator, owned_key);
        if (slot.found_existing) return error.InvalidIndexSourceMap;
        slot.value_ptr.* = owned_bytes;
    }
    pub fn get(self: *TestStore, address: []const u8) ![]const u8 {
        self.calls += 1;
        const bytes = self.values.get(address) orelse return error.NotFound;
        @memcpy(self.scratch[0..bytes.len], bytes);
        return self.scratch[0..bytes.len];
    }
    pub fn delete(self: *TestStore, address: []const u8) anyerror!void {
        const removed = self.values.fetchRemove(address) orelse return error.NotFound;
        self.allocator.free(removed.key);
        self.allocator.free(removed.value);
    }
};
fn testLeaf(page: u32) Digest {
    var raw: [4]u8 = undefined;
    std.mem.writeInt(u32, &raw, page, .little);
    return digest(&raw);
}
test "index source directory streams bounded fanout roots and rejects corrupted paths" {
    const a = std.testing.allocator;
    const binding: publication.Binding = .{ .owner = @splat(71), .publication = @splat(72) };
    try std.testing.expect(@sizeOf(Builder) < 18 * 1024);
    for ([_]u32{ 1, 128, 129, 16384, 16385 }) |pages| {
        var store: TestStore = .{ .allocator = a };
        defer store.deinit();
        var prepared = try Builder.init(binding, pages);
        var staged = try Builder.init(binding, pages);
        for (0..pages) |page| {
            const hash = testLeaf(@intCast(page));
            try prepared.append(hash, null);
            try staged.append(hash, &store);
        }
        const root = try prepared.finish(null);
        try std.testing.expectEqualDeep(root, try staged.finish(&store));
        try std.testing.expectEqual(try nodeCount(pages), store.values.count());
        for ([_]u32{ 0, pages / 2, pages - 1 }) |page| {
            store.calls = 0;
            try std.testing.expectEqualDeep(testLeaf(page), try lookup(&store, binding, pages, root, page));
            try std.testing.expectEqual(@as(usize, try levels(pages)), store.calls);
        }
        try std.testing.expectError(error.InvalidIndexSourceMap, lookup(&store, binding, pages, root, pages));
        try std.testing.expectError(error.InvalidIndexSourceMap, staged.append(testLeaf(pages), &store));
        const address = key(binding, 0, 0);
        const bytes = store.values.get(&address).?;
        bytes[6] ^= 1;
        try std.testing.expectError(error.InvalidIndexSourceMap, lookup(&store, binding, pages, root, 0));
        bytes[6] ^= 1;
        for (0..try nodeCount(pages)) |ordinal| try deleteAt(&store, binding, pages, @intCast(ordinal));
        try std.testing.expectEqual(@as(u32, 0), store.values.count());
    }
    try std.testing.expectEqual(@as(u8, 4), try levels((@as(u32, std.math.maxInt(u32)) - 1) / 64 + 1));
    try std.testing.expectError(error.InvalidIndexSourceMap, levels(0));
    var incomplete = try Builder.init(binding, 129);
    try incomplete.append(testLeaf(0), null);
    try std.testing.expectError(error.InvalidIndexSourceMap, incomplete.finish(null));
}
