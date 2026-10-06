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

//! Bounded memcomparable key prefixes. Exact type layouts prevent mixed
//! integer/float comparisons from rounding; truncated/complex keys fall back
//! to the authoritative scalar comparator. Signed zero is canonicalized.
const std = @import("std");
const Datum = @import("scalar.zig").Datum;
pub const Key = struct {
    bytes: [32]u8 = @splat(0),
    types: u64 = 0,
    len: u8 = 0,
    complete: bool = true,
    fn push(self: *Key, byte: u8, descending: bool) void {
        if (self.len == self.bytes.len) {
            self.complete = false;
            return;
        }
        self.bytes[self.len] = if (descending) ~byte else byte;
        self.len += 1;
    }
};
pub fn encode(values: []const Datum, orders: anytype) ?Key {
    if (values.len != orders.len or values.len > 16) return null;
    var key: Key = .{};
    for (values, orders, 0..) |value, order, index| {
        if (value.array != null) return null;
        const kind: u4 = if (value.sql_null) 0 else switch (value.value) {
            .integer => 1,
            .float => 2,
            .string => 3,
            .bool => 4,
            .null => 5,
            else => return null,
        };
        key.types |= @as(u64, kind) << @as(u6, @intCast(index * 4));
        const null_first = order.nulls_first orelse order.descending;
        key.push(if (value.sql_null) if (null_first) 0 else 2 else 1, false);
        if (value.sql_null) continue;
        switch (value.value) {
            .integer => |integer| {
                var bytes: [8]u8 = undefined;
                std.mem.writeInt(u64, &bytes, @as(u64, @bitCast(integer)) ^ (@as(u64, 1) << 63), .big);
                for (bytes) |byte| key.push(byte, order.descending);
            },
            .float => |number| {
                if (!std.math.isFinite(number)) return null;
                const bits: u64 = @bitCast(if (number == 0) @as(f64, 0) else number);
                const sortable = if (bits >> 63 == 1) ~bits else bits ^ (@as(u64, 1) << 63);
                var bytes: [8]u8 = undefined;
                std.mem.writeInt(u64, &bytes, sortable, .big);
                for (bytes) |byte| key.push(byte, order.descending);
            },
            .bool => |boolean| key.push(@intFromBool(boolean), order.descending),
            .string => |text| {
                for (text) |byte| {
                    key.push(byte, order.descending);
                    if (byte == 0) key.push(255, order.descending);
                    if (!key.complete) break;
                }
                key.push(0, order.descending);
                key.push(0, order.descending);
            },
            .null => {},
            else => unreachable,
        }
    }
    return key;
}
pub fn compare(left: Key, right: Key) ?std.math.Order {
    if (left.types != right.types) return null;
    const shared = @min(left.len, right.len);
    const order = std.mem.order(u8, left.bytes[0..shared], right.bytes[0..shared]);
    if (order != .eq) return order;
    return if (left.complete and right.complete) std.math.order(left.len, right.len) else null;
}
test "SQL normalized prefixes match exact null direction zero and embedded-byte ordering" {
    const operators = @import("operators.zig");
    const values = [_]Datum{ .{}, Datum.json(.null), Datum.json(.{ .integer = std.math.minInt(i64) }), Datum.json(.{ .integer = 9007199254740993 }), Datum.json(.{ .integer = std.math.maxInt(i64) }), Datum.json(.{ .float = -0.0 }), Datum.json(.{ .float = 0.0 }), Datum.json(.{ .float = -1.25 }), Datum.json(.{ .string = "" }), Datum.json(.{ .string = "a\x00b" }), Datum.json(.{ .string = "a\x00c" }), Datum.json(.{ .string = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz1" }), Datum.json(.{ .string = "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz2" }) };
    for ([_]bool{ false, true }) |descending| for ([_]?bool{ null, false, true }) |nulls_first| {
        const orders = [_]operators.Order{.{ .descending = descending, .nulls_first = nulls_first }};
        for (values) |left| for (values) |right| {
            const expected = try operators.compareRows(.{ .values = &.{}, .keys = &.{left}, .ordinal = 0 }, .{ .values = &.{}, .keys = &.{right}, .ordinal = 0 }, &orders);
            if (compare(encode(&.{left}, &orders).?, encode(&.{right}, &orders).?)) |actual| try std.testing.expectEqual(expected, actual);
        };
    };
}

test "SQL normalized composite prefixes preserve field boundaries and stable ties" {
    const operators = @import("operators.zig");
    const tuples = [_][2]Datum{
        .{ Datum.json(.{ .string = "a" }), Datum.json(.{ .integer = 9 }) },
        .{ Datum.json(.{ .string = "a\x00" }), Datum.json(.{ .integer = -9 }) },
        .{ Datum.json(.{ .string = "a" }), Datum.json(.{ .integer = -9 }) },
        .{ Datum.json(.{ .string = "" }), .{} },
        .{ .{}, Datum.json(.{ .integer = 1 }) },
        .{ Datum.json(.{ .string = "abcdefghijklmnopqrstuvwxyzabcdef" }), Datum.json(.{ .integer = 1 }) },
        .{ Datum.json(.{ .string = "abcdefghijklmnopqrstuvwxyzabcdef" }), Datum.json(.{ .integer = 2 }) },
    };
    for ([_]bool{ false, true }) |first_desc| for ([_]bool{ false, true }) |second_desc| {
        const orders = [_]operators.Order{ .{ .descending = first_desc }, .{ .descending = second_desc, .nulls_first = true } };
        for (tuples) |left| for (tuples) |right| {
            const plain_left: operators.Row = .{ .values = &.{}, .keys = &left, .ordinal = 3 };
            const plain_right: operators.Row = .{ .values = &.{}, .keys = &right, .ordinal = 7 };
            var cached_left = plain_left;
            var cached_right = plain_right;
            cached_left.normalized = encode(&left, &orders);
            cached_right.normalized = encode(&right, &orders);
            try std.testing.expectEqual(try operators.compareRows(plain_left, plain_right, &orders), try operators.compareRows(cached_left, cached_right, &orders));
        };
    };
}
