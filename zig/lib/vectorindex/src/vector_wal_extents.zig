// Copyright 2026 Antfly, Inc.
// Licensed under the Apache License, Version 2.0.

//! Immutable vector-WAL extent receipts. A source watermark alone cannot
//! identify a byte prefix: coverage-only commits may reuse that watermark.
const std = @import("std");

pub const max_extents = 8;
pub const encoded_extent_size = 48;

pub const Extent = struct {
    generation: u64 = 0,
    committed_bytes: u64 = 0,
    covered_source_sequence: u64 = 0,
    last_batch: u64 = 0,
    /// Null denotes an extent containing only coverage. Zero is a valid
    /// mutation sequence and is not used as a sentinel.
    min_mutation_sequence: ?u64 = null,

    pub fn encode(self: Extent, out: []u8) void {
        std.debug.assert(out.len == encoded_extent_size);
        const words = [_]u64{ self.generation, self.committed_bytes, self.covered_source_sequence, self.last_batch, @intFromBool(self.min_mutation_sequence != null), self.min_mutation_sequence orelse 0 };
        for (words, 0..) |word, i| std.mem.writeInt(u64, out[i * 8 ..][0..8], word, .big);
    }

    pub fn decode(bytes: []const u8) !Extent {
        if (bytes.len != encoded_extent_size) return error.InvalidVectorWalExtent;
        var words: [6]u64 = undefined;
        for (&words, 0..) |*word, i| word.* = std.mem.readInt(u64, bytes[i * 8 ..][0..8], .big);
        if (words[4] > 1 or (words[4] == 0 and words[5] != 0)) return error.InvalidVectorWalExtent;
        return .{ .generation = words[0], .committed_bytes = words[1], .covered_source_sequence = words[2], .last_batch = words[3], .min_mutation_sequence = if (words[4] != 0) words[5] else null };
    }
};

pub const Set = struct {
    count: u8 = 0,
    items: [max_extents]Extent = [_]Extent{.{}} ** max_extents,

    pub fn slice(self: *const Set) []const Extent {
        return self.items[0..self.count];
    }

    pub fn bytes(self: *const Set) !u64 {
        if (self.count > max_extents) return error.InvalidVectorWalExtent;
        var total: u64 = 0;
        for (self.slice()) |extent| total = std.math.add(u64, total, extent.committed_bytes) catch
            return error.InvalidVectorWalExtent;
        return total;
    }

    pub fn validate(self: *const Set, active_generation: u64, committed_bytes: u64) !void {
        if (try self.bytes() > committed_bytes) return error.InvalidVectorWalExtent;
        var previous: ?Extent = null;
        for (self.slice()) |extent| {
            if (extent.generation == 0 or extent.generation >= active_generation or
                extent.committed_bytes == 0) return error.InvalidVectorWalExtent;
            if (extent.min_mutation_sequence) |sequence|
                if (sequence > extent.covered_source_sequence) return error.InvalidVectorWalExtent;
            if (previous) |before| if (extent.generation <= before.generation or
                extent.last_batch <= before.last_batch or extent.covered_source_sequence < before.covered_source_sequence)
                return error.InvalidVectorWalExtent;
            previous = extent;
        }
        for (self.items[self.count..]) |extent|
            if (!std.meta.eql(extent, Extent{})) return error.InvalidVectorWalExtent;
    }

    /// Receipt for an exact sealed byte prefix. Newer extents are retained by
    /// identity, never selected merely because their source sequence differs.
    pub fn afterPrefix(self: *const Set, prefix_bytes: u64, sequence: u64) ?Set {
        var total: u64 = 0;
        for (self.slice(), 0..) |extent, i| {
            total = std.math.add(u64, total, extent.committed_bytes) catch return null;
            if (total != prefix_bytes or extent.covered_source_sequence != sequence) continue;
            var result: Set = .{ .count = @intCast(self.count - i - 1) };
            @memcpy(result.items[0..result.count], self.items[i + 1 .. self.count]);
            return result;
        }
        return null;
    }
};

test "vector WAL extents distinguish same-sequence prefixes and zero mutation sequences" {
    var set: Set = .{ .count = 2 };
    set.items[0] = .{ .generation = 1, .committed_bytes = 40, .covered_source_sequence = 7, .last_batch = 2, .min_mutation_sequence = 0 };
    set.items[1] = .{ .generation = 2, .committed_bytes = 24, .covered_source_sequence = 7, .last_batch = 3 };
    try set.validate(3, 80);
    const tail = set.afterPrefix(40, 7).?;
    try std.testing.expectEqual(@as(u8, 1), tail.count);
    try std.testing.expectEqual(@as(u64, 2), tail.items[0].generation);
    try std.testing.expect(set.afterPrefix(41, 7) == null);
    try std.testing.expect(set.afterPrefix(40, 8) == null);
    var encoded: [encoded_extent_size]u8 = undefined;
    for (set.slice()) |extent| {
        extent.encode(&encoded);
        try std.testing.expectEqualDeep(extent, try Extent.decode(&encoded));
    }
    try std.testing.expectError(error.InvalidVectorWalExtent, set.validate(2, 80));
    try std.testing.expectError(error.InvalidVectorWalExtent, set.validate(3, 63));
    set.items[1].last_batch = 2;
    try std.testing.expectError(error.InvalidVectorWalExtent, set.validate(3, 80));
}
