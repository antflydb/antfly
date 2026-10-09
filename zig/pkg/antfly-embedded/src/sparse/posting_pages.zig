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

//! Immutable posting blocks addressed by segment, term and final ordinal.
//! Roots retain the legacy term directory. Queries seek posting blocks and
//! native maintenance merges streams into spooled copy-on-write output.
const std = @import("std");
const daat = @import("daat.zig");
const A = std.mem.Allocator;
const magic = "ASPSPG01";
const legacy_magic = "ASPSSEG1";
const tag: u8 = 0x0d;
pub const route_tag: u8 = 0x0e;
const route_ready = [_]u8{0x0f};
pub const bucket_tag: u8 = 0x14;
const summary_tag: u8 = 0x11;
pub fn routeKey(term: u32, id: u64) [13]u8 {
    var bytes: [13]u8 = undefined;
    bytes[0] = route_tag;
    std.mem.writeInt(u32, bytes[1..5], term, .big);
    std.mem.writeInt(u64, bytes[5..13], id, .big);
    return bytes;
}
pub fn routed(txn: anytype) !bool {
    const bytes = txn.get(&route_ready) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    return bytes.len == 1 and (bytes[0] == 1 or bytes[0] == 2);
}
/// One dyadic node covers a segment's interval of 16-bit ordinal buckets.
/// Point selections seek ancestors instead of duplicating wide intervals into
/// thousands of leaf routes. Exact bounds reject false-positive cover nodes.
pub fn intervalNode(first: u32, last: u32) u32 {
    const low = first >> 16;
    const high = last >> 16;
    const depth: u32 = @clz(@as(u16, @intCast(low ^ high)));
    return (depth << 16) | (low >> @as(u5, @intCast(16 - depth)));
}
pub fn bucketKey(term: u32, node: u32, id: u64) [16]u8 {
    var bytes: [16]u8 = undefined;
    bytes[0] = bucket_tag;
    std.mem.writeInt(u32, bytes[1..5], term, .big);
    bytes[5] = @intCast(node >> 16);
    std.mem.writeInt(u16, bytes[6..8], @truncate(node), .big);
    std.mem.writeInt(u64, bytes[8..16], id, .big);
    return bytes;
}
pub fn bucketed(txn: anytype) !bool {
    const bytes = txn.get(&route_ready) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    return std.mem.eql(u8, bytes, &.{2});
}
fn route(txn: anytype, id: u64, root: []const u8) !void {
    const dir = try directory(root);
    const bounds = if (std.mem.readInt(u32, root[8..12], .little) == 2) try ordinalBounds(root) else null;
    var value: [8]u8 = undefined;
    if (bounds) |range| {
        std.mem.writeInt(u32, value[0..4], range[0], .little);
        std.mem.writeInt(u32, value[4..8], range[1], .little);
    }
    var at: usize = 0;
    while (at < dir.len) : (at += 20) {
        const term = std.mem.readInt(u32, dir[at..][0..4], .little);
        try txn.put(&routeKey(term, id), if (bounds != null) &value else &.{});
        const node = if (bounds) |range| intervalNode(range[0], range[1]) else 0;
        try txn.put(&bucketKey(term, node, id), if (bounds != null) &value else &.{});
    }
}
pub fn publishRoutes(txn: anytype, id: u64, root: []const u8) !void {
    try route(txn, id, root);
}
/// Older snapshots keep the discovery fallback until the first publication
/// builds a complete directory. Routes and the coverage fence commit together.
pub fn ensureRoutes(a: A, txn: anytype) !void {
    if (try bucketed(txn)) return;
    var lower: [9]u8 = @splat(0);
    lower[0] = 0x03;
    while (true) {
        var id: u64 = undefined;
        const root = blk: {
            var cursor = try txn.openCursor();
            defer cursor.close();
            const entry = (try cursor.seekAtOrAfter(&lower)) orelse break;
            if (entry.key.len == 0 or entry.key[0] != 0x03) break;
            if (entry.key.len != 9) return error.InvalidSparseSegment;
            id = std.mem.readInt(u64, entry.key[1..9], .big);
            break :blk try a.dupe(u8, entry.value);
        };
        defer a.free(root);
        try route(txn, id, root);
        if (id == std.math.maxInt(u64)) break;
        std.mem.writeInt(u64, lower[1..9], id + 1, .big);
    }
    try txn.put(&route_ready, &.{2});
}
pub const max_block_bytes = 1024 * 1024;
pub fn paged(root: []const u8) bool {
    return root.len >= 16 and std.mem.eql(u8, root[0..8], magic);
}
pub fn directory(root: []const u8) ![]const u8 {
    if (root.len < 16) return error.InvalidSparseSegment;
    const count = std.mem.readInt(u32, root[12..16], .little);
    const end = 16 + @as(u64, count) * 20;
    if (end > root.len or (paged(root) and end != root.len and end + 12 != root.len)) return error.InvalidSparseSegment;
    return root[16..@intCast(end)];
}
/// Conservative authenticated segment bounds; older paged roots omit them.
pub fn ordinalBounds(root: []const u8) !?[2]u32 {
    if (!paged(root)) return null;
    const dir = try directory(root);
    const tail = root[16 + dir.len ..];
    if (tail.len == 0) return null;
    if (tail.len != 12 or !std.mem.eql(u8, tail[0..4], "O32B")) return error.InvalidSparseSegment;
    const bounds: [2]u32 = .{ std.mem.readInt(u32, tail[4..8], .little), std.mem.readInt(u32, tail[8..12], .little) };
    if (bounds[0] > bounds[1]) return error.InvalidSparseSegment;
    return bounds;
}
pub fn hasTerm(root: []const u8, term: u32) !bool {
    const dir = try directory(root);
    var lower: usize = 0;
    var upper = dir.len / 20;
    while (lower < upper) {
        const middle = lower + (upper - lower) / 2;
        const value = std.mem.readInt(u32, dir[middle * 20 ..][0..4], .little);
        if (value < term) lower = middle + 1 else upper = middle;
    }
    return lower < dir.len / 20 and std.mem.readInt(u32, dir[lower * 20 ..][0..4], .little) == term;
}
pub fn key(id: u64, term: u32, last: u32) [17]u8 {
    var result: [17]u8 = undefined;
    result[0] = tag;
    std.mem.writeInt(u64, result[1..9], id, .big);
    std.mem.writeInt(u32, result[9..13], term, .big);
    std.mem.writeInt(u32, result[13..17], last, .big);
    return result;
}
pub fn putBlock(txn: anytype, id: u64, term: u32, last: u32, block: []const u8) !void {
    if (block.len < 26) return error.InvalidChunk;
    const chunk = block[8..];
    if (chunk[0] != 1) return error.InvalidChunk;
    const first = std.mem.readInt(u32, chunk[13..17], .little);
    const max: f32 = @bitCast(std.mem.readInt(u32, chunk[5..9], .little));
    const min: f32 = @bitCast(std.mem.readInt(u32, chunk[9..13], .little));
    const step = (if (max > min) max - min else @as(f32, 1)) / 255.0;
    var summary: [12]u8 = undefined;
    std.mem.writeInt(u32, summary[0..4], first, .little);
    std.mem.writeInt(u32, summary[4..8], @bitCast(min), .little);
    std.mem.writeInt(u32, summary[8..12], @bitCast(min + @as(f32, 255) * step), .little);
    var summary_key = key(id, term, last);
    summary_key[0] = summary_tag;
    try txn.put(&summary_key, &summary);
    try txn.put(&key(id, term, last), block);
}
pub fn publish(a: A, txn: anytype, id: u64, legacy: []const u8) ![]u8 {
    try ensureRoutes(a, txn);
    const dir = try directory(legacy);
    var position: usize = 0;
    var first_ordinal: ?u32 = null;
    var last_ordinal: u32 = 0;
    while (position < dir.len) : (position += 20) {
        const entry = dir[position..][0..20];
        const term = std.mem.readInt(u32, entry[0..4], .little);
        const offset = std.mem.readInt(u64, entry[8..16], .little);
        const length = std.mem.readInt(u32, entry[16..20], .little);
        if (offset > legacy.len or length > legacy.len - offset) return error.InvalidSparseSegment;
        const payload = legacy[@intCast(offset)..][0..length];
        var at: usize = 0;
        var previous: ?u32 = null;
        while (at < payload.len) {
            if (payload.len - at < 8) return error.InvalidSparseSegment;
            const chunk_length = std.mem.readInt(u32, payload[at..][0..4], .little);
            const range_length = std.mem.readInt(u32, payload[at + 4 ..][0..4], .little);
            const size = 8 + @as(u64, chunk_length) + range_length;
            if (size > payload.len - at or size > max_block_bytes) return error.InvalidSparseSegment;
            const block = payload[at..][0..@intCast(size)];
            const chunk = block[8..][0..chunk_length];
            if (chunk.len < 18 or chunk[0] != 1) return error.InvalidChunk;
            const count = std.mem.readInt(u32, chunk[1..5], .little);
            if (count == 0 or 13 + @as(u64, count) * 5 != chunk.len) return error.InvalidChunk;
            var last: u32 = 0;
            for (0..count) |i| last = std.math.add(u32, last, std.mem.readInt(u32, chunk[13 + i * 4 ..][0..4], .little)) catch return error.InvalidChunk;
            if (previous) |prior| if (last <= prior) return error.InvalidChunk;
            const first = std.mem.readInt(u32, chunk[13..17], .little);
            first_ordinal = if (first_ordinal) |before| @min(before, first) else first;
            last_ordinal = @max(last_ordinal, last);
            try putBlock(txn, id, term, last, block);
            previous = last;
            at += @intCast(size);
        }
    }
    const header_len = 16 + dir.len;
    const root = try a.alloc(u8, header_len + @as(usize, if (first_ordinal != null) 12 else 0));
    @memcpy(root[0..header_len], legacy[0..header_len]);
    @memcpy(root[0..8], magic);
    if (first_ordinal) |first| {
        const tail = root[header_len..];
        @memcpy(tail[0..4], "O32B");
        std.mem.writeInt(u32, tail[4..8], first, .little);
        std.mem.writeInt(u32, tail[8..12], last_ordinal, .little);
    }
    errdefer a.free(root);
    try route(txn, id, root);
    return root;
}
pub fn materializedSize(root: []const u8) !usize {
    const dir = try directory(root);
    var size: u64 = 16 + dir.len;
    var at: usize = 0;
    while (at < dir.len) : (at += 20) size = std.math.add(u64, size, std.mem.readInt(u32, dir[at + 16 ..][0..4], .little)) catch return error.InvalidSparseSegment;
    return std.math.cast(usize, size) orelse error.InvalidSparseSegment;
}
pub fn materialize(a: A, txn: anytype, id: u64, root: []const u8) ![]u8 {
    if (!paged(root)) return a.dupe(u8, root);
    const result = try a.alloc(u8, try materializedSize(root));
    errdefer a.free(result);
    const header_len = 16 + (try directory(root)).len;
    @memcpy(result[0..header_len], root[0..header_len]);
    @memcpy(result[0..8], legacy_magic);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const prefix = key(id, 0, 0);
    var next = try cursor.seekAtOrAfter(prefix[0..9]);
    var at = header_len;
    while (next) |entry| {
        if (entry.key.len != 17 or !std.mem.eql(u8, entry.key[0..9], prefix[0..9])) break;
        if (entry.value.len > result.len - at) return error.InvalidSparseSegment;
        @memcpy(result[at..][0..entry.value.len], entry.value);
        at += entry.value.len;
        next = try cursor.next();
    }
    if (at != result.len) return error.InvalidSparseSegment;
    return result;
}
pub fn removeRoutes(txn: anytype, id: u64) !void {
    var root_key: [9]u8 = undefined;
    root_key[0] = 0x03;
    std.mem.writeInt(u64, root_key[1..9], id, .big);
    if (txn.get(&root_key)) |root| {
        const dir = try txn.allocator.dupe(u8, try directory(root));
        defer txn.allocator.free(dir);
        const bounds = if (std.mem.readInt(u32, root[8..12], .little) == 2) try ordinalBounds(root) else null;
        var at: usize = 0;
        while (at < dir.len) : (at += 20) {
            const term = std.mem.readInt(u32, dir[at..][0..4], .little);
            const node = if (bounds) |range| intervalNode(range[0], range[1]) else 0;
            txn.delete(&bucketKey(term, node, id)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(&routeKey(term, id)) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    } else |err| switch (err) {
        error.NotFound => {},
        else => return err,
    }
}
pub fn remove(txn: anytype, id: u64) !void {
    try removeRoutes(txn, id);
    const prefix = key(id, 0, 0);
    while (true) {
        var keys: [128][17]u8 = undefined;
        var count: usize = 0;
        {
            var cursor = try txn.openCursor();
            defer cursor.close();
            var next = try cursor.seekAtOrAfter(prefix[0..9]);
            while (next) |entry| {
                if (entry.key.len != 17 or !std.mem.eql(u8, entry.key[0..9], prefix[0..9])) break;
                keys[count] = entry.key[0..17].*;
                count += 1;
                if (count == keys.len) break;
                next = try cursor.next();
            }
        }
        if (count == 0) return;
        // Never mutate through a live cursor: backends differ in how deleting
        // its current entry affects subsequent navigation.
        for (keys[0..count]) |*entry| {
            try txn.delete(entry);
            var summary = entry.*;
            summary[0] = summary_tag;
            txn.delete(&summary) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
    }
}
pub fn Reader(comptime Txn: type) type {
    return struct {
        const Cursor = @typeInfo(@TypeOf(@as(*Txn, undefined).openCursor())).error_union.payload;
        const scheduler = @import("../sql/parallel_scheduler.zig");
        const Warm = struct {
            snapshot: ?Txn = null,
            id: u64 = 0,
            term: u32 = 0,
            lower: u32 = 0,
            task: ?scheduler.Task(anyerror!void) = null,
            fn run(self: *@This()) anyerror!void {
                var cursor = try self.snapshot.?.openCursor();
                defer cursor.close();
                const seek = key(self.id, self.term, self.lower);
                // The fork has independent scratch and pins the exact query
                // snapshot. Loading this cursor entry warms native read caches.
                _ = try cursor.seekAtOrAfter(&seek);
            }
            fn retire(self: *@This(), io: std.Io) void {
                if (self.task) |*task| task.cancel(io) catch {};
                if (comptime @hasDecl(Txn, "forkRead")) {
                    if (self.snapshot) |*snapshot| snapshot.abort();
                }
                self.* = .{};
            }
        };
        cursor: ?Cursor = null,
        summaries: ?Cursor = null,
        io: ?std.Io = null,
        warming: [8]Warm = @splat(.{}),
        txn: *Txn,
        resident: usize = 0,
        budget: usize = 64 * 1024 * 1024,
        blocks: usize = 0,
        bytes: usize = 0,
        pub fn deinit(self: *@This()) void {
            if (self.io) |io| for (&self.warming) |*warm| warm.retire(io);
            if (self.cursor) |*cursor| cursor.close();
            self.cursor = null;
            if (self.summaries) |*cursor| cursor.close();
            self.summaries = null;
        }
        fn prefetch(self: *@This(), id: u64, term: u32, last: u32) void {
            if (comptime !@hasDecl(Txn, "forkRead")) return;
            const io = self.io orelse return;
            if (last == std.math.maxInt(u32) or self.blocks < 2) return;
            for (&self.warming) |*warm| {
                if (warm.task) |*task| {
                    if (!task.isComplete()) continue;
                    warm.retire(io);
                }
                warm.snapshot = self.txn.forkRead() catch return;
                warm.id = id;
                warm.term = term;
                warm.lower = last + 1;
                warm.task = scheduler.global().submitTransient(io, max_block_bytes, Warm.run, .{warm});
                if (warm.task == null) warm.retire(io);
                return;
            }
        }
        pub fn interface(self: *@This()) daat.BlockReader {
            return .{ .ptr = self, .read = read, .probe = probe, .resident = &self.resident };
        }
        fn probe(raw: *anyopaque, id: u64, term: u32, lower: u64) !daat.BlockProbe {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (lower > std.math.maxInt(u32)) return .end;
            if (self.summaries == null) self.summaries = try self.txn.openCursor();
            var seek = key(id, term, @intCast(lower));
            seek[0] = summary_tag;
            const entry = (try self.summaries.?.seekAtOrAfter(&seek)) orelse return .legacy;
            if (entry.key.len != 17 or !std.mem.eql(u8, entry.key[0..13], seek[0..13])) return .legacy;
            if (entry.value.len != 12) return error.InvalidSparseSegment;
            return .{ .block = .{
                .first = std.mem.readInt(u32, entry.value[0..4], .little),
                .last = std.mem.readInt(u32, entry.key[13..17], .big),
                .min = @bitCast(std.mem.readInt(u32, entry.value[4..8], .little)),
                .max = @bitCast(std.mem.readInt(u32, entry.value[8..12], .little)),
            } };
        }
        fn read(raw: *anyopaque, a: A, id: u64, term: u32, lower: u64) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (lower > std.math.maxInt(u32)) return null;
            if (self.cursor == null) self.cursor = try self.txn.openCursor();
            const cursor = &self.cursor.?;
            const seek = key(id, term, @intCast(lower));
            const entry = (try cursor.seekAtOrAfter(&seek)) orelse return null;
            if (entry.key.len != 17 or !std.mem.eql(u8, entry.key[0..13], seek[0..13])) return null;
            if (entry.value.len > max_block_bytes or entry.value.len > self.budget -| self.resident) return error.ResourceBudgetExceeded;
            const bytes = try a.dupe(u8, entry.value);
            self.resident += bytes.len;
            self.blocks += 1;
            self.bytes += bytes.len;
            self.prefetch(id, term, std.mem.readInt(u32, entry.key[13..17], .big));
            return bytes;
        }
    };
}

pub fn forEachBlock(txn: anytype, id: u64, term: ?u32, context: anytype, comptime visit: fn (@TypeOf(context), u32, []const u8) anyerror!void) !void {
    var cursor = try txn.openCursor();
    defer cursor.close();
    const prefix = key(id, term orelse 0, 0);
    const prefix_len: usize = if (term != null) 13 else 9;
    var next = try cursor.seekAtOrAfter(prefix[0..prefix_len]);
    while (next) |entry| {
        if (entry.key.len != 17 or !std.mem.eql(u8, entry.key[0..prefix_len], prefix[0..prefix_len])) break;
        try visit(context, std.mem.readInt(u32, entry.key[9..13], .big), entry.value);
        next = try cursor.next();
    }
}

test "paged posting codec seeks one block bounds memory and reclaims every page under OOM" {
    const FakeTxn = struct {
        a: A,
        allocator: A,
        get_calls: usize = 0,
        entries: std.ArrayList(Entry) = .empty,
        const Entry = struct { key: []const u8, value: []const u8 };
        fn deinit(self: *@This()) void {
            for (self.entries.items) |entry| {
                self.a.free(entry.key);
                self.a.free(entry.value);
            }
            self.entries.deinit(self.a);
        }
        fn get(self: *@This(), k: []const u8) anyerror![]const u8 {
            self.get_calls += 1;
            for (self.entries.items) |entry| if (std.mem.eql(u8, entry.key, k)) return entry.value;
            return error.NotFound;
        }
        fn put(self: *@This(), k: []const u8, value: []const u8) !void {
            const owned_key = try self.a.dupe(u8, k);
            errdefer self.a.free(owned_key);
            const owned_value = try self.a.dupe(u8, value);
            errdefer self.a.free(owned_value);
            try self.entries.append(self.a, .{ .key = owned_key, .value = owned_value });
            std.mem.sort(Entry, self.entries.items, {}, struct {
                fn less(_: void, x: Entry, y: Entry) bool {
                    return std.mem.lessThan(u8, x.key, y.key);
                }
            }.less);
        }
        fn delete(self: *@This(), k: []const u8) anyerror!void {
            for (self.entries.items, 0..) |entry, i| if (std.mem.eql(u8, entry.key, k)) {
                self.a.free(entry.key);
                self.a.free(entry.value);
                _ = self.entries.orderedRemove(i);
                return;
            };
            return error.NotFound;
        }
        const Self = @This();
        const Cursor = struct {
            owner: *Self,
            position: usize = 0,
            fn close(_: *@This()) void {}
            fn seekAtOrAfter(self: *@This(), k: []const u8) !?Entry {
                self.position = 0;
                while (self.position < self.owner.entries.items.len and std.mem.lessThan(u8, self.owner.entries.items[self.position].key, k)) self.position += 1;
                return if (self.position < self.owner.entries.items.len) self.owner.entries.items[self.position] else null;
            }
            fn next(self: *@This()) !?Entry {
                self.position += 1;
                return if (self.position < self.owner.entries.items.len) self.owner.entries.items[self.position] else null;
            }
        };
        fn openCursor(self: *@This()) !Cursor {
            return .{ .owner = self };
        }
    };
    const Probe = struct {
        fn run(a: A) !void {
            var txn: FakeTxn = .{ .a = a, .allocator = a };
            defer txn.deinit();
            var legacy: [36 + 3 * 39]u8 = @splat(0);
            @memcpy(legacy[0..8], legacy_magic);
            std.mem.writeInt(u32, legacy[8..12], 2, .little);
            std.mem.writeInt(u32, legacy[12..16], 1, .little);
            std.mem.writeInt(u32, legacy[16..20], 7, .little);
            std.mem.writeInt(u32, legacy[20..24], 3, .little);
            std.mem.writeInt(u64, legacy[24..32], 36, .little);
            std.mem.writeInt(u32, legacy[32..36], 3 * 39, .little);
            for (0..3) |block| {
                const frame = legacy[36 + block * 39 ..][0..39];
                std.mem.writeInt(u32, frame[0..4], 23, .little);
                std.mem.writeInt(u32, frame[4..8], 8, .little);
                const chunk = frame[8..31];
                chunk[0] = 1;
                std.mem.writeInt(u32, chunk[1..5], 2, .little);
                const weight: f32 = @floatFromInt(block + 1);
                std.mem.writeInt(u32, chunk[5..9], @bitCast(weight), .little);
                std.mem.writeInt(u32, chunk[9..13], @bitCast(weight), .little);
                std.mem.writeInt(u32, chunk[13..17], @intCast(block * 2), .little);
                std.mem.writeInt(u32, chunk[17..21], 1, .little);
            }
            const root = try publish(a, &txn, 42, &legacy);
            defer a.free(root);
            try std.testing.expect(paged(root));
            try std.testing.expectEqual([2]u32{ 0, 5 }, (try ordinalBounds(root)).?);
            try std.testing.expectEqual(null, try ordinalBounds(root[0 .. root.len - 12]));
            try std.testing.expect(try hasTerm(root, 7));
            try std.testing.expect(!try hasTerm(root, 8));
            const restored = try materialize(a, &txn, 42, root);
            defer a.free(restored);
            try std.testing.expectEqualSlices(u8, &legacy, restored);
            var reader: Reader(FakeTxn) = .{ .txn = &txn, .budget = 64 };
            defer reader.deinit();
            var stream: daat.Stream = .{ .weight = 1, .segment = 42, .term = 7, .reader = reader.interface(), .allocator = a };
            defer stream.deinit();
            try stream.seek(4);
            try std.testing.expectEqual(@as(?u32, 4), stream.doc);
            try std.testing.expectEqual(@as(f32, 3), try stream.contribution());
            try std.testing.expectEqual(@as(usize, 1), reader.blocks);
            try stream.advance();
            try std.testing.expectEqual(@as(?u32, 5), stream.doc);
            try stream.advance();
            try std.testing.expectEqual(null, stream.doc);
            try std.testing.expectEqual(@as(usize, 0), reader.resident);
            reader.budget = 1;
            try stream.seek(0);
            try std.testing.expectError(error.ResourceBudgetExceeded, stream.contribution());
            try std.testing.expectEqual(@as(usize, 0), reader.resident);
            var root_key: [9]u8 = @splat(0);
            root_key[0] = 0x03;
            std.mem.writeInt(u64, root_key[1..9], 42, .big);
            try txn.put(&root_key, root);
            try std.testing.expect(try routed(&txn));
            _ = try txn.get(&routeKey(7, 42));
            const gets_before_cleanup = txn.get_calls;
            try remove(&txn, 42);
            try std.testing.expectEqual(gets_before_cleanup + 1, txn.get_calls);
            try std.testing.expectError(error.NotFound, txn.get(&routeKey(7, 42)));
            try txn.delete(&root_key);
            try std.testing.expectEqual(@as(usize, 1), txn.entries.items.len);
        }
    };
    try Probe.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

/// Incremental compaction output. Native owners spool blocks into a private,
/// capacity-accounted run; only the term directory remains in memory.
pub const Builder = struct {
    pub const Run = @import("../postings_run.zig").Run;
    a: A,
    run: ?*Run = null,
    payload: std.ArrayList(u8) = .empty,
    dir: std.ArrayList(u8) = .empty,
    current: ?u32 = null,
    term_start: u64 = 0,
    total: u64 = 0,
    chunks: u32 = 0,
    first: ?u32 = null,
    last: u32 = 0,
    pub fn init(a: A, scratch: ?@import("../spill_sort.zig").Options) !Builder {
        return .{ .a = a, .run = if (scratch) |options| try Run.createWithResources(a, options.io, options.directory, options.resource_manager) else null };
    }
    pub fn deinit(self: *Builder) void {
        if (self.run) |run| run.deinit();
        self.payload.deinit(self.a);
        self.dir.deinit(self.a);
    }
    fn finishTerm(self: *Builder) !void {
        const term = self.current orelse return;
        var entry: [20]u8 = undefined;
        std.mem.writeInt(u32, entry[0..4], term, .little);
        std.mem.writeInt(u32, entry[4..8], self.chunks, .little);
        std.mem.writeInt(u64, entry[8..16], self.term_start, .little);
        std.mem.writeInt(u32, entry[16..20], std.math.cast(u32, self.total - self.term_start) orelse return error.ResourceBudgetExceeded, .little);
        try self.dir.appendSlice(self.a, &entry);
    }
    pub fn append(self: *Builder, term: u32, first: u32, last: u32, block: []const u8) !void {
        if (block.len > max_block_bytes) return error.ResourceBudgetExceeded;
        if (self.current == null or self.current.? != term) {
            if (self.current) |previous| if (term <= previous) return error.InvalidSparseSegment;
            try self.finishTerm();
            self.current = term;
            self.term_start = self.total;
            self.chunks = 0;
        }
        if (self.run) |run| {
            var header: [12]u8 = undefined;
            std.mem.writeInt(u32, header[0..4], term, .little);
            std.mem.writeInt(u32, header[4..8], last, .little);
            std.mem.writeInt(u32, header[8..12], @intCast(block.len), .little);
            try run.appendSlice(&header);
            try run.appendSlice(block);
        } else try self.payload.appendSlice(self.a, block);
        self.total = try std.math.add(u64, self.total, block.len);
        self.chunks = try std.math.add(u32, self.chunks, 1);
        self.first = if (self.first) |prior| @min(prior, first) else first;
        self.last = @max(self.last, last);
    }
    pub fn finish(self: *Builder) !?[]u8 {
        if (self.first == null) return null;
        try self.finishTerm();
        const header_len = 16 + self.dir.items.len;
        const bytes = try self.a.alloc(u8, header_len + if (self.run != null) @as(usize, 12) else self.payload.items.len);
        errdefer self.a.free(bytes);
        @memcpy(bytes[0..8], if (self.run != null) magic else legacy_magic);
        std.mem.writeInt(u32, bytes[8..12], 2, .little);
        std.mem.writeInt(u32, bytes[12..16], @intCast(self.dir.items.len / 20), .little);
        @memcpy(bytes[16..header_len], self.dir.items);
        var at: usize = 16;
        while (at < header_len) : (at += 20) {
            const offset = std.mem.readInt(u64, bytes[at + 8 ..][0..8], .little);
            std.mem.writeInt(u64, bytes[at + 8 ..][0..8], try std.math.add(u64, header_len, offset), .little);
        }
        if (self.run) |run| {
            try run.seal(0);
            @memcpy(bytes[header_len..][0..4], "O32B");
            std.mem.writeInt(u32, bytes[header_len + 4 ..][0..4], self.first.?, .little);
            std.mem.writeInt(u32, bytes[header_len + 8 ..][0..4], self.last, .little);
        } else @memcpy(bytes[header_len..], self.payload.items);
        return bytes;
    }
};

pub fn publishRun(a: A, txn: anytype, id: u64, root: []const u8, run: *Builder.Run) !void {
    if (!paged(root)) return error.InvalidSparseSegment;
    try ensureRoutes(a, txn);
    const view = try run.sealedView();
    var offset: u64 = 0;
    while (offset < view.length) {
        var header: [12]u8 = undefined;
        try view.readInto(offset, &header);
        offset += 12;
        const term = std.mem.readInt(u32, header[0..4], .little);
        const last = std.mem.readInt(u32, header[4..8], .little);
        const length = std.mem.readInt(u32, header[8..12], .little);
        if (length > max_block_bytes) return error.InvalidSparseSegment;
        const bytes = try a.alloc(u8, length);
        defer a.free(bytes);
        try view.readInto(offset, bytes);
        try putBlock(txn, id, term, last, bytes);
        offset += length;
    }
    try route(txn, id, root);
}

test "sparse interval routes cover boundary buckets without route duplication" {
    const intervals = [_][2]u32{
        .{ 0, 0 },          .{ 65535, 65536 },           .{ 0xffff0000, 0xffffffff },
        .{ 0, 0xffffffff }, .{ 0x12340001, 0x1235ffff }, .{ 0x7fffffff, 0x80000000 },
    };
    for (intervals) |range| {
        const node = intervalNode(range[0], range[1]);
        const depth = node >> 16;
        const prefix = node & 0xffff;
        try std.testing.expect(depth <= 16);
        for ((range[0] >> 16)..@as(usize, range[1] >> 16) + 1) |bucket| {
            try std.testing.expectEqual(prefix, @as(u32, @intCast(bucket)) >> @as(u5, @intCast(16 - depth)));
        }
        const route_key = bucketKey(7, node, 42);
        try std.testing.expectEqual(@as(u8, @intCast(depth)), route_key[5]);
        try std.testing.expectEqual(@as(u16, @intCast(prefix)), std.mem.readInt(u16, route_key[6..8], .big));
    }
}
