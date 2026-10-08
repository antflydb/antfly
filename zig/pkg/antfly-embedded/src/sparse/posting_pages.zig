// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable posting blocks addressed by segment, term and final ordinal.
//! Roots retain the legacy term directory; maintenance reconstructs legacy
//! bytes, while query cursors seek blocks without pinning whole segment values.
const std = @import("std");
const daat = @import("daat.zig");
const A = std.mem.Allocator;
const magic = "ASPSPG01";
const legacy_magic = "ASPSSEG1";
const tag: u8 = 0x0d;
pub const max_block_bytes = 1024 * 1024;
pub fn paged(root: []const u8) bool {
    return root.len >= 16 and std.mem.eql(u8, root[0..8], magic);
}
pub fn directory(root: []const u8) ![]const u8 {
    if (root.len < 16) return error.InvalidSparseSegment;
    const count = std.mem.readInt(u32, root[12..16], .little);
    const end = 16 + @as(u64, count) * 20;
    if (end > root.len or (paged(root) and end != root.len)) return error.InvalidSparseSegment;
    return root[16..@intCast(end)];
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
pub fn publish(a: A, txn: anytype, id: u64, legacy: []const u8) ![]u8 {
    const dir = try directory(legacy);
    var position: usize = 0;
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
            try txn.put(&key(id, term, last), block);
            previous = last;
            at += @intCast(size);
        }
    }
    const root = try a.dupe(u8, legacy[0 .. 16 + dir.len]);
    @memcpy(root[0..8], magic);
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
    @memcpy(result[0..root.len], root);
    @memcpy(result[0..8], legacy_magic);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const prefix = key(id, 0, 0);
    var next = try cursor.seekAtOrAfter(prefix[0..9]);
    var at = root.len;
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
pub fn remove(txn: anytype, id: u64) !void {
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
        for (keys[0..count]) |*entry| try txn.delete(entry);
    }
}
pub fn Reader(comptime Txn: type) type {
    return struct {
        txn: *Txn,
        resident: usize = 0,
        budget: usize = 64 * 1024 * 1024,
        blocks: usize = 0,
        bytes: usize = 0,
        pub fn interface(self: *@This()) daat.BlockReader {
            return .{ .ptr = self, .read = read, .resident = &self.resident };
        }
        fn read(raw: *anyopaque, a: A, id: u64, term: u32, lower: u64) !?[]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (lower > std.math.maxInt(u32)) return null;
            var cursor = try self.txn.openCursor();
            defer cursor.close();
            const seek = key(id, term, @intCast(lower));
            const entry = (try cursor.seekAtOrAfter(&seek)) orelse return null;
            if (entry.key.len != 17 or !std.mem.eql(u8, entry.key[0..13], seek[0..13])) return null;
            if (entry.value.len > max_block_bytes or entry.value.len > self.budget -| self.resident) return error.ResourceBudgetExceeded;
            const bytes = try a.dupe(u8, entry.value);
            self.resident += bytes.len;
            self.blocks += 1;
            self.bytes += bytes.len;
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
        entries: std.ArrayList(Entry) = .empty,
        const Entry = struct { key: []const u8, value: []const u8 };
        fn deinit(self: *@This()) void {
            for (self.entries.items) |entry| {
                self.a.free(entry.key);
                self.a.free(entry.value);
            }
            self.entries.deinit(self.a);
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
        fn delete(self: *@This(), k: []const u8) !void {
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
            var txn: FakeTxn = .{ .a = a };
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
            try std.testing.expect(try hasTerm(root, 7));
            try std.testing.expect(!try hasTerm(root, 8));
            const restored = try materialize(a, &txn, 42, root);
            defer a.free(restored);
            try std.testing.expectEqualSlices(u8, &legacy, restored);
            var reader: Reader(FakeTxn) = .{ .txn = &txn, .budget = 64 };
            var stream: daat.Stream = .{ .weight = 1, .segment = 42, .term = 7, .reader = reader.interface(), .allocator = a };
            defer stream.deinit();
            try stream.seek(4);
            try std.testing.expectEqual(@as(?u32, 4), stream.doc);
            try std.testing.expectEqual(@as(f32, 3), stream.contribution());
            try std.testing.expectEqual(@as(usize, 1), reader.blocks);
            try stream.advance();
            try std.testing.expectEqual(@as(?u32, 5), stream.doc);
            try stream.advance();
            try std.testing.expectEqual(null, stream.doc);
            try std.testing.expectEqual(@as(usize, 0), reader.resident);
            reader.budget = 1;
            try std.testing.expectError(error.ResourceBudgetExceeded, stream.seek(0));
            try std.testing.expectEqual(@as(usize, 0), reader.resident);
            try remove(&txn, 42);
            try std.testing.expectEqual(@as(usize, 0), txn.entries.items.len);
        }
    };
    try Probe.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}
