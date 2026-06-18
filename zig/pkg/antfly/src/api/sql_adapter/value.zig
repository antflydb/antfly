// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");

const parser = @import("parser.zig");
const token_mod = @import("token.zig");

pub const Token = token_mod.Token;

pub const SqlValue = union(enum) {
    null,
    bool: bool,
    integer: i64,
    float: f64,
    string: []const u8,
    json: []const u8,

    pub fn jsonAlloc(self: SqlValue, alloc: std.mem.Allocator) ![]const u8 {
        return switch (self) {
            .null => try alloc.dupe(u8, "null"),
            .bool => |value| try alloc.dupe(u8, if (value) "true" else "false"),
            .integer => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
            .float => |value| try std.fmt.allocPrint(alloc, "{d}", .{value}),
            .string => |value| try std.json.Stringify.valueAlloc(alloc, value, .{}),
            .json => |value| try alloc.dupe(u8, value),
        };
    }

    pub fn asU32(self: SqlValue) !u32 {
        return switch (self) {
            .integer => |value| if (value >= 0 and value <= std.math.maxInt(u32)) @intCast(value) else error.UnsupportedSqlShape,
            else => error.UnsupportedSqlShape,
        };
    }
};

pub fn parseSqlUntypedValueJsonAlloc(
    alloc: std.mem.Allocator,
    tokens: []const Token,
    pos: *usize,
) ![]const u8 {
    const cursor = parser.Cursor.init(tokens, pos);
    if (cursor.matchKeyword("true")) return try alloc.dupe(u8, "true");
    if (cursor.matchKeyword("false")) return try alloc.dupe(u8, "false");
    if (cursor.matchKeyword("null")) return try alloc.dupe(u8, "null");
    if (cursor.matchToken(.string)) |token| return try std.json.Stringify.valueAlloc(alloc, token.text, .{});
    if (cursor.matchToken(.number)) |token| return try alloc.dupe(u8, token.text);
    if (cursor.matchToken(.minus) != null) {
        const token = cursor.matchToken(.number) orelse return error.UnsupportedSqlShape;
        return try std.fmt.allocPrint(alloc, "-{s}", .{token.text});
    }
    return error.UnsupportedSqlShape;
}

pub fn parseSqlTimestampLiteralNs(raw: []const u8) !i64 {
    const text = std.mem.trim(u8, trimSqlRangeEndpointQuotes(raw), " \t\r\n");
    if (text.len < "0000-00-00".len) return error.UnsupportedSqlShape;
    if (text[4] != '-' or text[7] != '-') return error.UnsupportedSqlShape;
    const year = try parseSqlFixedDigits(i64, text[0..4]);
    const month = try parseSqlFixedDigits(u8, text[5..7]);
    const day = try parseSqlFixedDigits(u8, text[8..10]);
    if (month < 1 or month > 12) return error.UnsupportedSqlShape;
    const max_day = sqlDaysInMonth(year, month);
    if (day < 1 or day > max_day) return error.UnsupportedSqlShape;

    var i: usize = 10;
    var hour: u8 = 0;
    var minute: u8 = 0;
    var second: u8 = 0;
    var fractional_ns: i64 = 0;
    var offset_seconds: i64 = 0;

    if (i < text.len) {
        if (text[i] != 'T' and text[i] != 't' and text[i] != ' ') return error.UnsupportedSqlShape;
        i += 1;
        if (i + 8 > text.len or text[i + 2] != ':' or text[i + 5] != ':') return error.UnsupportedSqlShape;
        hour = try parseSqlFixedDigits(u8, text[i .. i + 2]);
        minute = try parseSqlFixedDigits(u8, text[i + 3 .. i + 5]);
        second = try parseSqlFixedDigits(u8, text[i + 6 .. i + 8]);
        if (hour > 23 or minute > 59 or second > 59) return error.UnsupportedSqlShape;
        i += 8;
        if (i < text.len and text[i] == '.') {
            i += 1;
            const fraction_start = i;
            var multiplier: i64 = 100_000_000;
            while (i < text.len and std.ascii.isDigit(text[i])) : (i += 1) {
                if (i - fraction_start < 9) {
                    fractional_ns += @as(i64, text[i] - '0') * multiplier;
                    multiplier = @divExact(multiplier, 10);
                }
            }
            if (i == fraction_start) return error.UnsupportedSqlShape;
        }
        if (i < text.len) {
            offset_seconds = try parseSqlTimestampOffsetSeconds(text[i..]);
            i = text.len;
        }
    }

    if (i != text.len) return error.UnsupportedSqlShape;
    const day_ns = (@as(i64, hour) * 60 * 60 + @as(i64, minute) * 60 + @as(i64, second)) * std.time.ns_per_s + fractional_ns;
    const days = sqlDaysFromCivil(year, month, day);
    const total_ns = @as(i128, days) * @as(i128, std.time.ns_per_day) + @as(i128, day_ns) - @as(i128, offset_seconds) * @as(i128, std.time.ns_per_s);
    if (total_ns < std.math.minInt(i64) or total_ns > std.math.maxInt(i64)) return error.UnsupportedSqlShape;
    return @intCast(total_ns);
}

fn trimSqlRangeEndpointQuotes(text: []const u8) []const u8 {
    if (text.len >= 2 and ((text[0] == '"' and text[text.len - 1] == '"') or (text[0] == '\'' and text[text.len - 1] == '\''))) {
        return text[1 .. text.len - 1];
    }
    return text;
}

fn parseSqlFixedDigits(comptime T: type, text: []const u8) !T {
    if (text.len == 0) return error.UnsupportedSqlShape;
    var value: T = 0;
    for (text) |ch| {
        if (!std.ascii.isDigit(ch)) return error.UnsupportedSqlShape;
        value = value * 10 + @as(T, @intCast(ch - '0'));
    }
    return value;
}

fn parseSqlTimestampOffsetSeconds(text: []const u8) !i64 {
    if (text.len == 0) return 0;
    if (std.ascii.eqlIgnoreCase(text, "Z")) return 0;
    const sign: i64 = if (text[0] == '+') 1 else if (text[0] == '-') -1 else return error.UnsupportedSqlShape;
    if (text.len != 3 and text.len != 5 and text.len != 6) return error.UnsupportedSqlShape;
    const hour = try parseSqlFixedDigits(i64, text[1..3]);
    var minute: i64 = 0;
    if (text.len == 5) {
        minute = try parseSqlFixedDigits(i64, text[3..5]);
    } else if (text.len == 6) {
        if (text[3] != ':') return error.UnsupportedSqlShape;
        minute = try parseSqlFixedDigits(i64, text[4..6]);
    }
    if (hour > 23 or minute > 59) return error.UnsupportedSqlShape;
    return sign * (hour * 60 * 60 + minute * 60);
}

fn sqlDaysFromCivil(year_value: i64, month_value: u8, day_value: u8) i64 {
    var year = year_value;
    const month: i64 = month_value;
    const day: i64 = day_value;
    year -= if (month <= 2) 1 else 0;
    const era = @divFloor(year, 400);
    const yoe = year - era * 400;
    const month_prime = month + if (month > 2) @as(i64, -3) else @as(i64, 9);
    const doy = @divFloor(153 * month_prime + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

fn sqlDaysInMonth(year: i64, month: u8) u8 {
    return switch (month) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (sqlIsLeapYear(year)) 29 else 28,
        else => 0,
    };
}

fn sqlIsLeapYear(year: i64) bool {
    return @mod(year, 4) == 0 and (@mod(year, 100) != 0 or @mod(year, 400) == 0);
}

pub const SqlIntervalLiteral = struct {
    fixed_ns: u64,
    calendar_months: u64,
    saw_fixed: bool,
    saw_calendar: bool,
};

pub fn sqlIntervalLiteral(text: []const u8) !SqlIntervalLiteral {
    var parts = std.mem.tokenizeAny(u8, text, " \t\r\n");
    var fixed_total: u128 = 0;
    var calendar_total: u128 = 0;
    var saw_pair = false;
    var saw_fixed = false;
    var saw_calendar = false;
    while (parts.next()) |amount_text| {
        const unit_text = parts.next() orelse return error.UnsupportedSqlShape;
        const amount = std.fmt.parseInt(u64, amount_text, 10) catch return error.UnsupportedSqlShape;
        if (sqlIntervalUnitNs(unit_text)) |multiplier| {
            saw_fixed = true;
            fixed_total += @as(u128, amount) * @as(u128, multiplier);
            if (fixed_total > std.math.maxInt(u64)) return error.UnsupportedSqlShape;
        } else if (sqlIntervalUnitMonths(unit_text)) |months| {
            saw_calendar = true;
            calendar_total += @as(u128, amount) * @as(u128, months);
            if (calendar_total > std.math.maxInt(u64)) return error.UnsupportedSqlShape;
        } else {
            return error.UnsupportedSqlShape;
        }
        saw_pair = true;
    }
    if (!saw_pair) return error.UnsupportedSqlShape;
    return .{
        .fixed_ns = @intCast(fixed_total),
        .calendar_months = @intCast(calendar_total),
        .saw_fixed = saw_fixed,
        .saw_calendar = saw_calendar,
    };
}

fn sqlIntervalUnitNs(unit: []const u8) ?u64 {
    if (std.ascii.eqlIgnoreCase(unit, "ns") or
        std.ascii.eqlIgnoreCase(unit, "nanosecond") or
        std.ascii.eqlIgnoreCase(unit, "nanoseconds"))
        return 1;
    if (std.ascii.eqlIgnoreCase(unit, "us") or
        std.ascii.eqlIgnoreCase(unit, "microsecond") or
        std.ascii.eqlIgnoreCase(unit, "microseconds"))
        return 1_000;
    if (std.ascii.eqlIgnoreCase(unit, "ms") or
        std.ascii.eqlIgnoreCase(unit, "millisecond") or
        std.ascii.eqlIgnoreCase(unit, "milliseconds"))
        return 1_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "s") or
        std.ascii.eqlIgnoreCase(unit, "sec") or
        std.ascii.eqlIgnoreCase(unit, "second") or
        std.ascii.eqlIgnoreCase(unit, "seconds"))
        return 1_000_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "m") or
        std.ascii.eqlIgnoreCase(unit, "min") or
        std.ascii.eqlIgnoreCase(unit, "minute") or
        std.ascii.eqlIgnoreCase(unit, "minutes"))
        return 60 * 1_000_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "h") or
        std.ascii.eqlIgnoreCase(unit, "hr") or
        std.ascii.eqlIgnoreCase(unit, "hour") or
        std.ascii.eqlIgnoreCase(unit, "hours"))
        return 60 * 60 * 1_000_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "d") or
        std.ascii.eqlIgnoreCase(unit, "day") or
        std.ascii.eqlIgnoreCase(unit, "days"))
        return 24 * 60 * 60 * 1_000_000_000;
    if (std.ascii.eqlIgnoreCase(unit, "w") or
        std.ascii.eqlIgnoreCase(unit, "week") or
        std.ascii.eqlIgnoreCase(unit, "weeks"))
        return 7 * 24 * 60 * 60 * 1_000_000_000;
    return null;
}

fn sqlIntervalUnitMonths(unit: []const u8) ?u64 {
    if (std.ascii.eqlIgnoreCase(unit, "month") or
        std.ascii.eqlIgnoreCase(unit, "months"))
        return 1;
    if (std.ascii.eqlIgnoreCase(unit, "year") or
        std.ascii.eqlIgnoreCase(unit, "years"))
        return 12;
    return null;
}

test "sql adapter value parses timestamp literals" {
    try std.testing.expectEqual(@as(i64, 0), try parseSqlTimestampLiteralNs("1970-01-01"));
    try std.testing.expectEqual(@as(i64, 1_000_000_000), try parseSqlTimestampLiteralNs("'1970-01-01T00:00:01Z'"));
    try std.testing.expectEqual(@as(i64, 0), try parseSqlTimestampLiteralNs("1970-01-01 01:00:00+01:00"));
    try std.testing.expectError(error.UnsupportedSqlShape, parseSqlTimestampLiteralNs("2026-02-29"));
}

test "sql adapter value parses interval literals" {
    const fixed = try sqlIntervalLiteral("1 day 2 hours 3 minutes");
    try std.testing.expectEqual(@as(u64, 93_780_000_000_000), fixed.fixed_ns);
    try std.testing.expectEqual(@as(u64, 0), fixed.calendar_months);
    try std.testing.expect(fixed.saw_fixed);
    try std.testing.expect(!fixed.saw_calendar);

    const mixed = try sqlIntervalLiteral("1 year 2 months 3 days");
    try std.testing.expectEqual(@as(u64, 14), mixed.calendar_months);
    try std.testing.expectEqual(@as(u64, 259_200_000_000_000), mixed.fixed_ns);
    try std.testing.expect(mixed.saw_fixed);
    try std.testing.expect(mixed.saw_calendar);

    try std.testing.expectError(error.UnsupportedSqlShape, sqlIntervalLiteral("1 parsec"));
}
