// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Compact typed refresh tuples. The publication's ordering cohort binds field
//! positions; every row retains its exact input version. Historical JSON remains
//! readable, so unchanged entries can migrate without rebuilding source indexes.
const std = @import("std");
const local = @import("antfly_local_sources");
const A = std.mem.Allocator;
const Native = local.segment.SegmentIndexSortBoundValue;
const Bound = @import("lake_index_sort_bounds.zig").Bound;
const max_bytes = @import("../serverless/graph_segment/page_tree.zig").max_record_bytes;
const magic = "AFOK001\x00";
const header_bytes = magic.len + 32 + 1;
const Tag = enum(u8) { datetime = 1, unsigned = 2, signed = 3, float = 4, boolean = 5, bytes = 6 };
pub const Row = struct { version: [32]u8, keys: []const Native };

/// Oversized or unsupported optional tuples retain ordinary source fallback.
/// Empty tuples need no per-row refresh state.
pub fn encode(a: A, version: [32]u8, keys: []const Native) !?[]u8 {
    if (keys.len == 0 or keys.len > 32) return null;
    var length: usize = header_bytes;
    for (keys) |key| {
        const payload: usize = switch (key) {
            .datetime_ns => 16,
            .u64_val, .i64_val => 8,
            .f64_val => |n| if (std.math.isFinite(n)) 8 else return null,
            .bool_val => 1,
            .bytes_val => |bytes| std.math.add(usize, 4, bytes.len) catch return null,
            .id => return null,
        };
        const entry_length = std.math.add(usize, payload, 1) catch return null;
        length = std.math.add(usize, length, entry_length) catch return null;
        if (length > max_bytes) return null;
    }
    const bytes = try a.alloc(u8, length);
    @memcpy(bytes[0..magic.len], magic);
    @memcpy(bytes[magic.len..][0..32], &version);
    bytes[header_bytes - 1] = @intCast(keys.len);
    var offset: usize = header_bytes;
    for (keys) |key| {
        const tag: Tag = switch (key) {
            .datetime_ns => .datetime,
            .u64_val => .unsigned,
            .i64_val => .signed,
            .f64_val => .float,
            .bool_val => .boolean,
            .bytes_val => .bytes,
            .id => unreachable,
        };
        bytes[offset] = @backingInt(tag);
        offset += 1;
        switch (key) {
            .datetime_ns => |n| {
                std.mem.writeInt(i128, bytes[offset..][0..16], n, .little);
                offset += 16;
            },
            .u64_val => |n| {
                std.mem.writeInt(u64, bytes[offset..][0..8], n, .little);
                offset += 8;
            },
            .i64_val => |n| {
                std.mem.writeInt(i64, bytes[offset..][0..8], n, .little);
                offset += 8;
            },
            .f64_val => |n| {
                std.mem.writeInt(u64, bytes[offset..][0..8], @bitCast(n), .little);
                offset += 8;
            },
            .bool_val => |n| {
                bytes[offset] = @intFromBool(n);
                offset += 1;
            },
            .bytes_val => |value| {
                std.mem.writeInt(u32, bytes[offset..][0..4], @intCast(value.len), .little);
                offset += 4;
                @memcpy(bytes[offset..][0..value.len], value);
                offset += value.len;
            },
            .id => unreachable,
        }
    }
    std.debug.assert(offset == length);
    return bytes;
}
const Reader = struct {
    bytes: []const u8,
    offset: usize = header_bytes,
    fn take(self: *Reader, n: usize) ![]const u8 {
        if (n > self.bytes.len - self.offset) return error.InvalidLogicalRowIdentity;
        const result = self.bytes[self.offset..][0..n];
        self.offset += n;
        return result;
    }
};
/// The destination arena owns all returned values, including bytes keys. A
/// retained tuple never borrows a tree cursor's current leaf or input buffer.
pub fn decodeIntoArena(a: A, bytes: []const u8) !Row {
    if (bytes.len > max_bytes) return error.InvalidLogicalRowIdentity;
    if (!std.mem.startsWith(u8, bytes, magic)) {
        if (bytes.len == 0 or bytes[0] != '{') return error.InvalidLogicalRowIdentity;
        const Legacy = struct { version: [32]u8, keys: []const Bound.Value };
        const row = try std.json.parseFromSliceLeaky(Legacy, a, bytes, .{ .allocate = .alloc_always });
        if (row.keys.len > 32) return error.InvalidLogicalRowIdentity;
        const keys = try a.alloc(Native, row.keys.len);
        for (keys, row.keys) |*key, value| {
            key.* = try value.native();
            if (key.* == .id or (key.* == .f64_val and !std.math.isFinite(key.f64_val))) return error.InvalidLogicalRowIdentity;
        }
        return .{ .version = row.version, .keys = keys };
    }
    if (bytes.len < header_bytes or bytes[header_bytes - 1] == 0 or bytes[header_bytes - 1] > 32) return error.InvalidLogicalRowIdentity;
    const keys = try a.alloc(Native, bytes[header_bytes - 1]);
    var reader: Reader = .{ .bytes = bytes };
    for (keys) |*key| {
        const tag = std.enums.fromInt(Tag, (try reader.take(1))[0]) orelse return error.InvalidLogicalRowIdentity;
        key.* = switch (tag) {
            .datetime => .{ .datetime_ns = std.mem.readInt(i128, (try reader.take(16))[0..16], .little) },
            .unsigned => .{ .u64_val = std.mem.readInt(u64, (try reader.take(8))[0..8], .little) },
            .signed => .{ .i64_val = std.mem.readInt(i64, (try reader.take(8))[0..8], .little) },
            .float => value: {
                const n: f64 = @bitCast(std.mem.readInt(u64, (try reader.take(8))[0..8], .little));
                if (!std.math.isFinite(n)) return error.InvalidLogicalRowIdentity;
                break :value .{ .f64_val = n };
            },
            .boolean => value: {
                const n = (try reader.take(1))[0];
                if (n > 1) return error.InvalidLogicalRowIdentity;
                break :value .{ .bool_val = n == 1 };
            },
            .bytes => value: {
                const n = std.mem.readInt(u32, (try reader.take(4))[0..4], .little);
                break :value .{ .bytes_val = try a.dupe(u8, try reader.take(n)) };
            },
        };
    }
    if (reader.offset != bytes.len) return error.InvalidLogicalRowIdentity;
    return .{ .version = bytes[magic.len..][0..32].*, .keys = keys };
}

test "external lake logical fragments binary ordering cache owns typed tuples and reads historical JSON" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const values = [_]Native{ .{ .datetime_ns = -922337203685477580900 }, .{ .u64_val = std.math.maxInt(u64) }, .{ .i64_val = std.math.minInt(i64) }, .{ .f64_val = -1.2345678901234567e-200 }, .{ .bool_val = true }, .{ .bytes_val = "a\x00b\xf0\x9f\x8c\x8a" } };
    const held: [32]u8 = @splat(171);
    const encoded = (try encode(out, held, &values)).?;
    const decoded = try decodeIntoArena(out, encoded);
    try std.testing.expectEqual(held, decoded.version);
    try std.testing.expectEqualDeep(&values, decoded.keys);
    @memset(encoded, 0);
    try std.testing.expectEqualStrings(values[5].bytes_val, decoded.keys[5].bytes_val);
    const legacy_keys = try out.alloc(Bound.Value, values.len);
    for (legacy_keys, values) |*key, value| key.* = try Bound.Value.from(out, value);
    const legacy = try std.json.Stringify.valueAlloc(out, .{ .version = held, .keys = legacy_keys }, .{});
    const restored = try decodeIntoArena(out, legacy);
    @memset(legacy, 0);
    try std.testing.expectEqualDeep(&values, restored.keys);
    const pair = values[1..3];
    const binary = (try encode(out, held, pair)).?;
    const json = try std.json.Stringify.valueAlloc(out, .{ .version = held, .keys = legacy_keys[1..3] }, .{});
    try std.testing.expect(binary.len * 3 < json.len);
    std.debug.print("ORDERING_CODEC integers=2 json_bytes={d} binary_bytes={d}\n", .{ json.len, binary.len });
}

test "external lake logical fragments binary ordering cache rejects malformed data and releases failed allocations" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const out = arena.allocator();
    const encoded = (try encode(out, @splat(7), &.{ .{ .bytes_val = "owned" }, .{ .bool_val = true } })).?;
    for (0..encoded.len) |n| try std.testing.expectError(error.InvalidLogicalRowIdentity, decodeIntoArena(out, encoded[0..n]));
    const trailing = try std.mem.concat(out, u8, &.{ encoded, "x" });
    try std.testing.expectError(error.InvalidLogicalRowIdentity, decodeIntoArena(out, trailing));
    const invalid_tag = try out.dupe(u8, encoded);
    invalid_tag[header_bytes] = 255;
    try std.testing.expectError(error.InvalidLogicalRowIdentity, decodeIntoArena(out, invalid_tag));
    const invalid_bool = try out.dupe(u8, encoded);
    invalid_bool[invalid_bool.len - 1] = 2;
    try std.testing.expectError(error.InvalidLogicalRowIdentity, decodeIntoArena(out, invalid_bool));
    try std.testing.expect(try encode(out, @splat(0), &.{}) == null);
    try std.testing.expect(try encode(out, @splat(0), &.{.{ .f64_val = std.math.nan(f64) }}) == null);
    const Failure = struct {
        fn run(alloc: A, input: []const u8) !void {
            var owner = std.heap.ArenaAllocator.init(alloc);
            defer owner.deinit();
            const row = try decodeIntoArena(owner.allocator(), input);
            const bytes = (try encode(owner.allocator(), row.version, row.keys)).?;
            try std.testing.expectEqualSlices(u8, input, bytes);
        }
    };
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Failure.run, .{encoded});
}
