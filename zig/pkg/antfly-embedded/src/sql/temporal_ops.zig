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

//! Shared temporal equality/order/hash for physical SQL operators. Normalized
//! comparison identity is not physical row encoding or a persistent index key.
//! Cross-domain coercion belongs to the binder and its pinned timezone rules.
const std = @import("std");
const temporal = @import("temporal_value.zig");
pub const Error = temporal.Error || error{SqlTypeMismatch};
pub const key_size = 21;

const Identity = struct { kind: temporal.Kind, primary: i128, secondary: i32 = 0 };

fn identity(value: temporal.Value) temporal.Error!Identity {
    var frame: [16]u8 = undefined;
    _ = try temporal.encode(value, &frame);
    return switch (value) {
        .date => |v| .{ .kind = .date, .primary = v.days },
        .time => |v| .{ .kind = .time, .primary = v.micros },
        .timestamp => |v| .{ .kind = .timestamp, .primary = v.micros },
        .timestamp_tz => |v| .{ .kind = .timestamp_tz, .primary = v.micros },
        // PostgreSQL compares unwrapped GMT time, then the stored zone. Two
        // equal GMT times with different offsets are NOT equal timetz values.
        .time_tz => |v| .{
            .kind = .time_tz,
            .primary = @as(i128, v.time.micros) + @as(i128, v.seconds_west) * 1_000_000,
            .secondary = v.seconds_west,
        },
        // Comparison uses 30-day months; storage/arithmetic still retain all
        // three independent components. Widen before any multiply/add.
        .interval => |v| .{
            .kind = .interval,
            .primary = (@as(i128, v.months) * 30 + v.days) * temporal.micros_per_day + v.micros,
        },
    };
}

pub fn compare(left: temporal.Value, right: temporal.Value) Error!std.math.Order {
    if (left.kind() != right.kind()) return error.SqlTypeMismatch;
    const a = try identity(left);
    const b = try identity(right);
    const primary = std.math.order(a.primary, b.primary);
    return if (primary == .eq) std.math.order(a.secondary, b.secondary) else primary;
}

/// Fixed-width logical key, shared by equality hashing and memory-sort prefixes.
/// Explicit big-endian fields avoid host endian, struct padding and JSON text.
/// Do not persist this as a native index codec without a capability/version gate.
pub fn key(value: temporal.Value, output: *[key_size]u8) temporal.Error![]const u8 {
    const id = try identity(value);
    output[0] = @backingInt(id.kind);
    std.mem.writeInt(u128, output[1..17], @as(u128, @bitCast(id.primary)) ^ (@as(u128, 1) << 127), .big);
    std.mem.writeInt(u32, output[17..21], @as(u32, @bitCast(id.secondary)) ^ (@as(u32, 1) << 31), .big);
    return output;
}

pub fn hash(value: temporal.Value) temporal.Error!u64 {
    var bytes: [key_size]u8 = undefined;
    return std.hash.Wyhash.hash(0, try key(value, &bytes));
}

test "SQL temporal operators match independent PostgreSQL comparisons and equality hashes" {
    const Entry = struct { kind: temporal.Kind, left: []const u8, right: []const u8, order: i8 };
    const fixture = try std.json.parseFromSlice(struct { comparisons: []const Entry }, std.testing.allocator, @embedFile("fixtures/sql_temporal_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 21), fixture.value.comparisons.len);
    for (fixture.value.comparisons) |entry| {
        var left: [16]u8 = undefined;
        var right: [16]u8 = undefined;
        const a = try temporal.decode(entry.kind, try std.fmt.hexToBytes(&left, entry.left));
        const b = try temporal.decode(entry.kind, try std.fmt.hexToBytes(&right, entry.right));
        const expected: std.math.Order = if (entry.order < 0) .lt else if (entry.order > 0) .gt else .eq;
        try std.testing.expectEqual(expected, try compare(a, b));
        var a_key: [key_size]u8 = undefined;
        var b_key: [key_size]u8 = undefined;
        try std.testing.expectEqual(expected, std.mem.order(u8, try key(a, &a_key), try key(b, &b_key)));
        if (expected == .eq) try std.testing.expectEqual(try hash(a), try hash(b));
    }
}

test "SQL temporal operators retain kinds and validate before key publication" {
    const t = std.testing;
    const zero: temporal.Value = .{ .timestamp = .{ .micros = 0 } };
    try t.expectError(error.SqlTypeMismatch, compare(zero, .{ .timestamp_tz = .{ .micros = 0 } }));
    const month: temporal.Value = .{ .interval = .{ .months = 1 } };
    const days: temporal.Value = .{ .interval = .{ .days = 30 } };
    try t.expectEqual(std.math.Order.eq, try compare(month, days));
    try t.expectEqual(try hash(month), try hash(days));
    var output: [key_size]u8 = @splat(0xa5);
    try t.expectError(error.TemporalOutOfRange, key(.{ .time = .{ .micros = -1 } }, &output));
    for (output) |byte| try t.expectEqual(@as(u8, 0xa5), byte);
    const extreme: temporal.Value = .{ .interval = .{ .months = std.math.maxInt(i32), .days = std.math.maxInt(i32), .micros = std.math.minInt(i64) } };
    try t.expectEqual(std.math.Order.gt, try compare(extreme, .{ .interval = .{} }));
}
