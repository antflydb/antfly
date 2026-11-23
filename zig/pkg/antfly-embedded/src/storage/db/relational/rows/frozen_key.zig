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

//! Frozen births have identities independent of logical primary keys. Hex
//! preserves arbitrary logical-key byte ordering; ! terminates each logical
//! key before its extensions. A fixed-width ordinal retains repeated births.
const std = @import("std");
const A = std.mem.Allocator;
pub const max_logical_bytes = 64 * 1024;
pub const max_physical_bytes = 2 * max_logical_bytes + 17;
const ordinal_bits = 62;
const ordinal_mask = (@as(u64, 1) << ordinal_bits) - 1;

pub const Lineage = enum(u2) {
    unknown = 0,
    superseded = 1,
    surviving = 2,
};

pub const View = struct {
    logical_hex: []const u8,
    ordinal: u64,
    lineage: Lineage,
    pub fn logicalAlloc(self: View, a: A) ![]u8 {
        const result = try a.alloc(u8, self.logical_hex.len / 2);
        errdefer a.free(result);
        _ = try std.fmt.hexToBytes(result, self.logical_hex);
        return result;
    }
};
fn prefix(a: A, logical: []const u8) ![]u8 {
    if (logical.len > max_logical_bytes) return error.InvalidRelationPopulation;
    const result = try a.alloc(u8, 2 * logical.len + 1);
    const digits = "0123456789abcdef";
    for (logical, 0..) |byte, i| {
        result[2 * i] = digits[byte >> 4];
        result[2 * i + 1] = digits[byte & 15];
    }
    result[result.len - 1] = '!';
    return result;
}
pub fn encode(a: A, logical: []const u8, ordinal: u64) ![]u8 {
    if (logical.len == 0 or logical.len > max_logical_bytes) return error.InvalidRelationPopulation;
    const result = try a.alloc(u8, 2 * logical.len + 17);
    const digits = "0123456789abcdef";
    for (logical, 0..) |byte, i| {
        result[2 * i] = digits[byte >> 4];
        result[2 * i + 1] = digits[byte & 15];
    }
    result[2 * logical.len] = '!';
    var remaining = ordinal;
    var position = result.len;
    while (position > 2 * logical.len + 1) {
        position -= 1;
        result[position] = digits[@as(usize, @intCast(remaining & 15))];
        remaining >>= 4;
    }
    return result;
}
/// The tuple suffix binds lineage into the existing manifested key. Earlier
/// births of the same key are superseded and sort before its surviving birth.
pub fn encodeBirth(a: A, logical: []const u8, ordinal: u64, lineage: Lineage) ![]u8 {
    if (ordinal > ordinal_mask or lineage == .unknown) return error.InvalidRelationPopulation;
    return encode(a, logical, (@as(u64, @backingInt(lineage)) << ordinal_bits) | ordinal);
}

pub fn decode(physical: []const u8) !View {
    if (physical.len < 19 or physical.len > max_physical_bytes or physical[physical.len - 17] != '!') return error.InvalidRelationPopulation;
    const logical = physical[0 .. physical.len - 17];
    if (logical.len % 2 != 0) return error.InvalidRelationPopulation;
    for (physical, 0..) |byte, i| {
        if (i == physical.len - 17) continue;
        if (!(byte >= '0' and byte <= '9') and !(byte >= 'a' and byte <= 'f')) return error.InvalidRelationPopulation;
    }
    const suffix = try std.fmt.parseInt(u64, physical[physical.len - 16 ..], 16);
    const lineage: Lineage = switch (suffix >> ordinal_bits) {
        0 => .unknown,
        1 => .superseded,
        2 => .surviving,
        else => return error.InvalidRelationPopulation,
    };
    return .{ .logical_hex = logical, .ordinal = suffix & ordinal_mask, .lineage = lineage };
}
/// Equal-key lookup uses [lower(key), upper(key)); logical range scans use
/// [lower(start), lower(end)). Neither enumerates unrelated tuple births.
pub fn lower(a: A, logical: []const u8) ![]u8 {
    return prefix(a, logical);
}
pub fn upper(a: A, logical: []const u8) ![]u8 {
    const result = try prefix(a, logical);
    result[result.len - 1] = '"';
    return result;
}

/// Convert both ends of a half-open logical shard range. Empty ends retain
/// their meaning as unbounded sentinels when provisioning a historical owner.
pub fn physicalRange(a: A, logical: @import("../../../byte_range.zig").ByteRange) !@import("../../../byte_range.zig").ByteRange {
    const start = if (logical.start.len != 0) try lower(a, logical.start) else "";
    errdefer if (start.len != 0) a.free(start);
    return .{
        .start = start,
        .end = if (logical.end.len != 0) try lower(a, logical.end) else "",
    };
}

test "SQL generation intents frozen tuple keys retain duplicate births and bounded logical ranges" {
    const a = std.testing.allocator;
    const first = try encode(a, "a\x00", 0);
    defer a.free(first);
    const second = try encode(a, "a\x00", 1);
    defer a.free(second);
    const later = try encode(a, "a\x00x", 0);
    defer a.free(later);
    const lo = try lower(a, "a\x00");
    defer a.free(lo);
    const hi = try upper(a, "a\x00");
    defer a.free(hi);
    try std.testing.expect(std.mem.order(u8, lo, first) == .lt);
    try std.testing.expect(std.mem.order(u8, first, second) == .lt);
    try std.testing.expect(std.mem.order(u8, second, hi) == .lt);
    try std.testing.expect(std.mem.order(u8, hi, later) == .lt);
    const left = try physicalRange(a, .{ .start = "", .end = "a\x00" });
    defer a.free(left.end);
    const right = try physicalRange(a, .{ .start = "a\x00", .end = "" });
    defer a.free(right.start);
    try std.testing.expectEqualStrings(left.end, right.start);
    try std.testing.expect(!left.contains(first));
    try std.testing.expect(right.contains(first));
    try std.testing.expect(right.contains(second));
    const view = try decode(second);
    try std.testing.expectEqual(@as(u64, 1), view.ordinal);
    try std.testing.expectEqual(Lineage.unknown, view.lineage);
    const superseded = try encodeBirth(a, "a\x00", 0, .superseded);
    defer a.free(superseded);
    const surviving = try encodeBirth(a, "a\x00", 1, .surviving);
    defer a.free(surviving);
    try std.testing.expect(std.mem.order(u8, superseded, surviving) == .lt);
    try std.testing.expect(right.contains(superseded) and right.contains(surviving));
    try std.testing.expectEqual(Lineage.superseded, (try decode(superseded)).lineage);
    try std.testing.expectEqual(Lineage.surviving, (try decode(surviving)).lineage);
    try std.testing.expectEqual(@as(u64, 1), (try decode(surviving)).ordinal);
    try std.testing.expectError(error.InvalidRelationPopulation, decode("61!c000000000000000"));
    const key = try view.logicalAlloc(a);
    defer a.free(key);
    try std.testing.expectEqualStrings("a\x00", key);
    try std.testing.expectError(error.InvalidRelationPopulation, decode("61!000000000000000A"));
    try std.testing.expectError(error.InvalidRelationPopulation, decode("!0000000000000000"));
}
