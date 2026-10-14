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

//! Stable scalar identities shared by ingestion and accepted/composed reads.
const std = @import("std");
const datetime = @import("../../../datetime.zig");
const V = std.json.Value;
pub const Kind = enum { scalar, number, timestamp };
pub fn normalize(a: std.mem.Allocator, input: V, kind: Kind) !V {
    if (kind == .timestamp) {
        const ns: i128 = switch (input) {
            .string => datetime.parseRfc3339ToSignedNs(input.string) orelse return error.InvalidLakeKey,
            // Iceberg timestamp values in the native WAL use microseconds.
            .integer => @as(i128, input.integer) * std.time.ns_per_us,
            else => return error.InvalidLakeKey,
        };
        return .{ .string = try datetime.formatDateTimeSignedNsAlloc(a, ns) };
    }
    const value = if (input == .number_string) blk: {
        if (std.fmt.parseInt(i64, input.number_string, 10)) |integer| break :blk V{ .integer = integer } else |_| {}
        break :blk V{ .float = std.fmt.parseFloat(f64, input.number_string) catch return error.InvalidLakeKey };
    } else input;
    if (kind == .number and value != .integer and value != .float) return error.InvalidLakeKey;
    return switch (value) {
        .integer, .string, .bool => value,
        .float => |number| blk: {
            if (!std.math.isFinite(number)) return error.InvalidLakeKey;
            // Preserve exact integers and canonicalize -0 without converting
            // large integer keys through f64 (which loses low bits).
            if (number >= -9223372036854775808.0 and number < 9223372036854775808.0 and @trunc(number) == number) break :blk .{ .integer = @intFromFloat(number) };
            break :blk value;
        },
        else => error.InvalidLakeKey,
    };
}
pub fn identity(a: std.mem.Allocator, keys: []const []const u8, row: V, kinds: []const Kind) ![]u8 {
    if (row != .object or (kinds.len != 0 and kinds.len != keys.len)) return error.InvalidLakeKey;
    const values = try a.alloc(V, keys.len);
    defer a.free(values);
    var initialized: usize = 0;
    defer for (values[0..initialized], 0..) |value, index| {
        if (kinds.len != 0 and kinds[index] == .timestamp) a.free(value.string);
    };
    for (keys, values, 0..) |key, *value, index| {
        value.* = try normalize(a, row.object.get(key) orelse return error.InvalidLakeKey, if (kinds.len == 0) .scalar else kinds[index]);
        initialized += 1;
    }
    return std.json.Stringify.valueAlloc(a, values, .{});
}
pub fn icebergKind(type_name: []const u8) Kind {
    if (std.mem.eql(u8, type_name, "timestamp") or std.mem.eql(u8, type_name, "timestamptz")) return .timestamp;
    if (std.mem.eql(u8, type_name, "float") or std.mem.eql(u8, type_name, "double")) return .number;
    return .scalar;
}

test "external lake scalar keys preserve integers and normalize numeric and timestamp equality" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    try std.testing.expectEqual(@as(i64, 9007199254740993), (try normalize(a, .{ .integer = 9007199254740993 }, .number)).integer);
    try std.testing.expectEqual(@as(i64, 7), (try normalize(a, .{ .float = 7.0 }, .number)).integer);
    try std.testing.expectEqual(@as(i64, 0), (try normalize(a, .{ .float = -0.0 }, .number)).integer);
    const left = try normalize(a, .{ .string = "2026-10-09T08:00:00-07:00" }, .timestamp);
    const right = try normalize(a, .{ .string = "2026-10-09T15:00:00Z" }, .timestamp);
    try std.testing.expectEqualStrings(left.string, right.string);
    try std.testing.expectError(error.InvalidLakeKey, normalize(a, .{ .float = std.math.nan(f64) }, .number));
    try std.testing.expectError(error.InvalidLakeKey, normalize(a, .{ .string = "invalid" }, .timestamp));
}
