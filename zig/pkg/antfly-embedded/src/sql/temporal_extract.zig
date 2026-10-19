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

//! Exact UTC timestamp extraction. The result is an integral coefficient and
//! decimal scale, so EXTRACT never round-trips timestamp precision through f64.
const std = @import("std");
const temporal = @import("temporal_value.zig");
const text = @import("temporal_text.zig");
pub const Part = struct { coefficient: i128, scale: u16 = 0 };

pub fn timestamp(value: temporal.Timestamp, unit: []const u8) !Part {
    if (value.infinity() != 0) return error.UnsupportedSqlShape;
    const fields = try text.Fields.fromTimestamp(value);
    const civil = try text.Civil.fromDate(fields.date);
    const micros = fields.time.micros;
    const second = @mod(micros, 60_000_000);
    const days = @as(i64, fields.date.days) + 10_957;
    const isodow = @mod(days + 3, 7) + 1;
    if (eq(unit, "epoch")) return .{ .coefficient = @as(i128, value.micros) + temporal.unix_epoch_offset_micros, .scale = 6 };
    if (eq(unit, "microseconds") or eq(unit, "microsecond")) return .{ .coefficient = second };
    if (eq(unit, "milliseconds") or eq(unit, "millisecond")) return .{ .coefficient = second, .scale = 3 };
    if (eq(unit, "second") or eq(unit, "seconds")) return .{ .coefficient = second, .scale = 6 };
    if (eq(unit, "minute") or eq(unit, "minutes")) return .{ .coefficient = @divTrunc(@mod(micros, 3_600_000_000), 60_000_000) };
    if (eq(unit, "hour") or eq(unit, "hours")) return .{ .coefficient = @divTrunc(micros, 3_600_000_000) };
    if (eq(unit, "day") or eq(unit, "days")) return .{ .coefficient = civil.day };
    if (eq(unit, "month") or eq(unit, "months")) return .{ .coefficient = civil.month };
    if (eq(unit, "quarter")) return .{ .coefficient = @divTrunc(civil.month - 1, 3) + 1 };
    if (eq(unit, "year") or eq(unit, "years")) return .{ .coefficient = if (civil.year <= 0) @as(i64, civil.year) - 1 else civil.year };
    if (eq(unit, "decade") or eq(unit, "decades")) return .{ .coefficient = @divFloor(civil.year, 10) };
    if (eq(unit, "century") or eq(unit, "centuries")) return .{ .coefficient = if (civil.year > 0) @divTrunc(@as(i64, civil.year) + 99, 100) else @divTrunc(@as(i64, civil.year) - 100, 100) };
    if (eq(unit, "millennium") or eq(unit, "millennia")) return .{ .coefficient = if (civil.year > 0) @divTrunc(@as(i64, civil.year) + 999, 1000) else @divTrunc(@as(i64, civil.year) - 1000, 1000) };
    if (eq(unit, "dow")) return .{ .coefficient = @mod(days + 4, 7) };
    if (eq(unit, "isodow")) return .{ .coefficient = isodow };
    if (eq(unit, "doy")) {
        const start = try (text.Civil{ .year = civil.year, .month = 1, .day = 1 }).toDate();
        return .{ .coefficient = @as(i64, fields.date.days) - start.days + 1 };
    }
    if (eq(unit, "week") or eq(unit, "isoyear")) {
        const thursday = try temporal.Date.init(std.math.cast(i32, @as(i64, fields.date.days) + 4 - isodow) orelse return error.TemporalOutOfRange);
        const iso = try text.Civil.fromDate(thursday);
        if (eq(unit, "isoyear")) return .{ .coefficient = if (iso.year <= 0) @as(i64, iso.year) - 1 else iso.year };
        const start = try (text.Civil{ .year = iso.year, .month = 1, .day = 1 }).toDate();
        return .{ .coefficient = @divFloor(@as(i64, thursday.days) - start.days, 7) + 1 };
    }
    // The active timestamp domain is UTC; named session timezones require the
    // separately pinned timezone capability, not process-local TZ state.
    if (eq(unit, "timezone") or eq(unit, "timezone_hour") or eq(unit, "timezone_minute")) return .{ .coefficient = 0 };
    return error.SqlInvalidParameterValue;
}

fn eq(left: []const u8, right: []const u8) bool {
    return std.ascii.eqlIgnoreCase(left, right);
}

test "SQL timestamp extraction preserves exact PostgreSQL decimal and calendar boundaries" {
    const Entry = struct { timestamp: []const u8, unit: []const u8, coefficient: []const u8, scale: u16, floating: f64 };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, std.testing.allocator, @embedFile("fixtures/sql_temporal_extract_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 126), fixture.value.entries.len);
    for (fixture.value.entries) |entry| {
        var bytes: [8]u8 = undefined;
        const value = try temporal.decode(.timestamp_tz, try std.fmt.hexToBytes(&bytes, entry.timestamp));
        const result = try timestamp(value.timestamp_tz, entry.unit);
        errdefer std.debug.print("extract unit={s} timestamp={s}\n", .{ entry.unit, entry.timestamp });
        try std.testing.expectEqual(try std.fmt.parseInt(i128, entry.coefficient, 10), result.coefficient);
        try std.testing.expectEqual(entry.scale, result.scale);
        const numerator: f64 = @floatFromInt(result.coefficient);
        const floating = numerator / std.math.pow(f64, 10, @floatFromInt(result.scale));
        try std.testing.expectApproxEqRel(entry.floating, floating, 0.000000000000001);
    }
}
