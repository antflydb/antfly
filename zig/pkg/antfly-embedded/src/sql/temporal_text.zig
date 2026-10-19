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

//! Full-domain Gregorian calendar and canonical ISO/Postgres temporal output.
//! Timestamp-with-zone output here is explicitly UTC. Session zone resolution,
//! DateStyle selection and text input belong to the execution/protocol boundary.
const std = @import("std");
const temporal = @import("temporal_value.zig");
const calendar = @import("../datetime.zig");
pub const Error = temporal.Error || error{ InvalidCalendarDate, InfiniteCalendarValue };
pub const output_capacity = 128;
const pg_epoch_days = 10_957;

/// Internal astronomical year numbering: year 0 is SQL 1 BC. SQL output never
/// emits year zero. All arithmetic is bounded by the validated DATE domain.
pub const Civil = struct {
    year: i32,
    month: u8,
    day: u8,

    pub fn fromDate(value: temporal.Date) Error!Civil {
        _ = try temporal.Date.init(value.days);
        if (value.infinity() != 0) return error.InfiniteCalendarValue;
        const fields = calendar.civilFromDays(@as(i64, value.days) + pg_epoch_days);
        return .{ .year = @intCast(fields.year), .month = fields.month, .day = fields.day };
    }

    pub fn toDate(self: Civil) Error!temporal.Date {
        if (self.year < -4713 or self.year > 5_874_897) return error.TemporalOutOfRange;
        const leap = @mod(self.year, 4) == 0 and (@mod(self.year, 100) != 0 or @mod(self.year, 400) == 0);
        const month_days: u8 = switch (self.month) {
            1, 3, 5, 7, 8, 10, 12 => 31,
            4, 6, 9, 11 => 30,
            2 => if (leap) 29 else 28,
            else => return error.InvalidCalendarDate,
        };
        if (self.day == 0 or self.day > month_days) return error.InvalidCalendarDate;
        const days = calendar.daysFromCivil(self.year, self.month, self.day) - pg_epoch_days;
        return temporal.Date.init(std.math.cast(i32, days) orelse return error.TemporalOutOfRange);
    }
};

pub const Fields = struct {
    date: temporal.Date,
    time: temporal.Time,

    pub fn fromTimestamp(value: temporal.Timestamp) Error!Fields {
        _ = try temporal.Timestamp.init(value.micros);
        if (value.infinity() != 0) return error.InfiniteCalendarValue;
        return .{
            .date = try temporal.Date.init(@intCast(@divFloor(value.micros, temporal.micros_per_day))),
            .time = try temporal.Time.init(@mod(value.micros, temporal.micros_per_day)),
        };
    }

    pub fn toTimestamp(self: Fields) Error!temporal.Timestamp {
        _ = try temporal.Date.init(self.date.days);
        _ = try temporal.Time.init(self.time.micros);
        if (self.date.infinity() != 0) return error.InfiniteCalendarValue;
        // 24:00 normalizes to the following day for timestamps, not TIME.
        const micros = @as(i128, self.date.days) * temporal.micros_per_day + self.time.micros;
        return temporal.Timestamp.init(std.math.cast(i64, micros) orelse return error.TemporalOutOfRange);
    }
};

/// Returns canonical PostgreSQL ISO/YMD output (UTC for timestamptz) and
/// postgres IntervalStyle. Validates public fields before touching the output.
/// The fixed capacity covers every valid domain value, including i64-min time
/// components and independently signed i32 month/day interval components.
pub fn format(value: temporal.Value, output: *[output_capacity]u8) temporal.Error![]const u8 {
    var binary: [16]u8 = undefined;
    _ = try temporal.encode(value, &binary);
    var writer: std.Io.Writer = .fixed(output);
    write(value, &writer) catch unreachable; // validated bounded fields fit 128 bytes
    return writer.buffered();
}

fn write(value: temporal.Value, writer: *std.Io.Writer) !void {
    const infinity = value.infinity();
    if (infinity != 0) return writer.writeAll(if (infinity < 0) "-infinity" else "infinity");
    switch (value) {
        .date => |v| {
            const civil = Civil.fromDate(v) catch unreachable;
            try writeDate(civil, writer);
            if (civil.year <= 0) try writer.writeAll(" BC");
        },
        .time => |v| try writeTime(@intCast(v.micros), writer),
        .time_tz => |v| {
            try writeTime(@intCast(v.time.micros), writer);
            const east = -v.seconds_west;
            const magnitude: u32 = @intCast(if (east < 0) -east else east);
            try writer.print("{s}{d:0>2}", .{ if (east < 0) "-" else "+", magnitude / 3600 });
            if (magnitude % 3600 != 0) try writer.print(":{d:0>2}", .{magnitude % 3600 / 60});
            if (magnitude % 60 != 0) try writer.print(":{d:0>2}", .{magnitude % 60});
        },
        .timestamp, .timestamp_tz => |v| {
            const fields = Fields.fromTimestamp(v) catch unreachable;
            const civil = Civil.fromDate(fields.date) catch unreachable;
            try writeDate(civil, writer);
            try writer.writeByte(' ');
            try writeTime(@intCast(fields.time.micros), writer);
            if (value.kind() == .timestamp_tz) try writer.writeAll("+00");
            if (civil.year <= 0) try writer.writeAll(" BC");
        },
        .interval => |v| try writeInterval(v, writer),
    }
}

fn writeDate(civil: Civil, writer: *std.Io.Writer) !void {
    const year: u32 = @intCast(if (civil.year <= 0) 1 - civil.year else civil.year);
    try writer.print("{d:0>4}-{d:0>2}-{d:0>2}", .{ year, civil.month, civil.day });
}

fn writeTime(micros: u64, writer: *std.Io.Writer) !void {
    const seconds = micros / 1_000_000;
    try writer.print("{d:0>2}:{d:0>2}:{d:0>2}", .{ seconds / 3600, seconds % 3600 / 60, seconds % 60 });
    const fraction = micros % 1_000_000;
    if (fraction != 0) {
        var digits: [6]u8 = undefined;
        _ = std.fmt.bufPrint(&digits, "{d:0>6}", .{fraction}) catch unreachable;
        var len = digits.len;
        while (digits[len - 1] == '0') len -= 1;
        try writer.writeByte('.');
        try writer.writeAll(digits[0..len]);
    }
}

fn intervalField(number: i32, singular: []const u8, plural: []const u8, writer: *std.Io.Writer, previous_negative: *bool, any: *bool) !void {
    if (number == 0) return;
    if (any.*) try writer.writeByte(' ');
    if (previous_negative.* and number > 0) try writer.writeByte('+');
    try writer.print("{d} {s}", .{ number, if (number == 1) singular else plural });
    previous_negative.* = number < 0;
    any.* = true;
}

fn writeInterval(value: temporal.Interval, writer: *std.Io.Writer) !void {
    var negative = false;
    var any = false;
    try intervalField(@divTrunc(value.months, 12), "year", "years", writer, &negative, &any);
    try intervalField(@rem(value.months, 12), "mon", "mons", writer, &negative, &any);
    try intervalField(value.days, "day", "days", writer, &negative, &any);
    if (value.micros != 0 or !any) {
        if (any) try writer.writeByte(' ');
        if (value.micros < 0) try writer.writeByte('-') else if (negative) try writer.writeByte('+');
        // Widen before abs: i64 minimum is a valid finite interval component.
        const signed: i128 = value.micros;
        const magnitude: u64 = @intCast(if (signed < 0) -signed else signed);
        try writeTime(magnitude, writer);
    }
}

pub const ParseError = Error || error{InvalidTemporalText};

/// Strict canonical codec for typed-row/protocol boundaries, not PostgreSQL's
/// general SQL literal parser. Alternate DateStyle, named zones, relative dates
/// and arbitrary input spellings must be resolved by the SQL ingress owner.
/// Length and work are bounded independently of caller input size.
pub fn parseCanonical(kind: temporal.Kind, text: []const u8) ParseError!temporal.Value {
    if (text.len == 0 or text.len > output_capacity) return error.InvalidTemporalText;
    const value = if (std.mem.eql(u8, text, "infinity") or std.mem.eql(u8, text, "-infinity")) blk: {
        const negative = text[0] == '-';
        break :blk switch (kind) {
            .date => temporal.Value{ .date = .{ .days = if (negative) std.math.minInt(i32) else std.math.maxInt(i32) } },
            .timestamp => temporal.Value{ .timestamp = .{ .micros = if (negative) std.math.minInt(i64) else std.math.maxInt(i64) } },
            .timestamp_tz => temporal.Value{ .timestamp_tz = .{ .micros = if (negative) std.math.minInt(i64) else std.math.maxInt(i64) } },
            .interval => temporal.Value{ .interval = .{
                .months = if (negative) std.math.minInt(i32) else std.math.maxInt(i32),
                .days = if (negative) std.math.minInt(i32) else std.math.maxInt(i32),
                .micros = if (negative) std.math.minInt(i64) else std.math.maxInt(i64),
            } },
            else => return error.InvalidTemporalText,
        };
    } else try parseFinite(kind, text);
    var canonical: [output_capacity]u8 = undefined;
    if (!std.mem.eql(u8, text, try format(value, &canonical))) return error.InvalidTemporalText;
    return value;
}

fn parseFinite(kind: temporal.Kind, text: []const u8) ParseError!temporal.Value {
    switch (kind) {
        .date => {
            const bc = std.mem.endsWith(u8, text, " BC");
            return .{ .date = try parseDate(if (bc) text[0 .. text.len - 3] else text, bc) };
        },
        .time => return .{ .time = try temporal.Time.init(std.math.cast(i64, try parseTime(text)) orelse return error.TemporalOutOfRange) },
        .time_tz => {
            const zone_start = std.mem.indexOfAny(u8, text, "+-") orelse return error.InvalidTemporalText;
            const clock = std.math.cast(i64, try parseTime(text[0..zone_start])) orelse return error.TemporalOutOfRange;
            const zone = text[zone_start + 1 ..];
            var parts = std.mem.splitScalar(u8, zone, ':');
            const hours = try unsigned(parts.next() orelse return error.InvalidTemporalText);
            const minutes = if (parts.next()) |part| try unsigned(part) else 0;
            const seconds = if (parts.next()) |part| try unsigned(part) else 0;
            if (parts.next() != null or hours > 15 or minutes > 59 or seconds > 59) return error.TemporalOutOfRange;
            const magnitude: i32 = @intCast(hours * 3600 + minutes * 60 + seconds);
            return .{ .time_tz = try temporal.TimeTz.init(clock, if (text[zone_start] == '+') -magnitude else magnitude) };
        },
        .timestamp, .timestamp_tz => {
            const bc = std.mem.endsWith(u8, text, " BC");
            const body = if (bc) text[0 .. text.len - 3] else text;
            const separator = std.mem.indexOfScalar(u8, body, ' ') orelse return error.InvalidTemporalText;
            const date = try parseDate(body[0..separator], bc);
            var clock = body[separator + 1 ..];
            if (kind == .timestamp_tz) {
                if (!std.mem.endsWith(u8, clock, "+00")) return error.InvalidTemporalText;
                clock = clock[0 .. clock.len - 3];
            }
            const fields: Fields = .{ .date = date, .time = try temporal.Time.init(std.math.cast(i64, try parseTime(clock)) orelse return error.TemporalOutOfRange) };
            const value = try fields.toTimestamp();
            return if (kind == .timestamp) .{ .timestamp = value } else .{ .timestamp_tz = value };
        },
        .interval => return .{ .interval = try parseInterval(text) },
    }
}

fn unsigned(text: []const u8) ParseError!u64 {
    if (text.len == 0) return error.InvalidTemporalText;
    for (text) |byte| if (byte < '0' or byte > '9') return error.InvalidTemporalText;
    return std.fmt.parseInt(u64, text, 10) catch return error.InvalidTemporalText;
}

fn parseDate(text: []const u8, bc: bool) ParseError!temporal.Date {
    var parts = std.mem.splitScalar(u8, text, '-');
    const year = try unsigned(parts.next() orelse return error.InvalidTemporalText);
    const month = try unsigned(parts.next() orelse return error.InvalidTemporalText);
    const day = try unsigned(parts.next() orelse return error.InvalidTemporalText);
    if (parts.next() != null or year == 0) return error.InvalidTemporalText;
    if (year > 5_874_897 or month > 12 or day > 31) return error.TemporalOutOfRange;
    return (Civil{ .year = if (bc) 1 - @as(i32, @intCast(year)) else @intCast(year), .month = @intCast(month), .day = @intCast(day) }).toDate();
}

/// Unsigned magnitude also handles the abs(i64-min) interval component.
fn parseTime(text: []const u8) ParseError!u64 {
    var parts = std.mem.splitScalar(u8, text, ':');
    const hour = try unsigned(parts.next() orelse return error.InvalidTemporalText);
    const minute = try unsigned(parts.next() orelse return error.InvalidTemporalText);
    const last = parts.next() orelse return error.InvalidTemporalText;
    if (parts.next() != null or minute > 59) return error.InvalidTemporalText;
    const point = std.mem.indexOfScalar(u8, last, '.');
    const second = try unsigned(if (point) |pos| last[0..pos] else last);
    if (second > 59) return error.InvalidTemporalText;
    var fraction: u64 = 0;
    if (point) |pos| {
        const digits = last[pos + 1 ..];
        if (digits.len == 0 or digits.len > 6) return error.InvalidTemporalText;
        fraction = try unsigned(digits);
        for (digits.len..6) |_| fraction *= 10;
    }
    const total: u128 = (@as(u128, hour) * 3600 + minute * 60 + second) * 1_000_000 + fraction;
    return std.math.cast(u64, total) orelse return error.TemporalOutOfRange;
}

fn parseInterval(text: []const u8) ParseError!temporal.Interval {
    var parts = std.mem.splitScalar(u8, text, ' ');
    var months: i64 = 0;
    var days: i32 = 0;
    var micros: i64 = 0;
    while (parts.next()) |number| {
        if (std.mem.indexOfScalar(u8, number, ':') != null) {
            const negative = number.len != 0 and number[0] == '-';
            const sign = number.len != 0 and (number[0] == '-' or number[0] == '+');
            const magnitude: i128 = try parseTime(if (sign) number[1..] else number);
            micros = std.math.cast(i64, if (negative) -magnitude else magnitude) orelse return error.TemporalOutOfRange;
            if (parts.next() != null) return error.InvalidTemporalText;
            break;
        }
        const unit = parts.next() orelse return error.InvalidTemporalText;
        const value = std.fmt.parseInt(i32, number, 10) catch return error.InvalidTemporalText;
        if (std.mem.eql(u8, unit, "year") or std.mem.eql(u8, unit, "years"))
            months += @as(i64, value) * 12
        else if (std.mem.eql(u8, unit, "mon") or std.mem.eql(u8, unit, "mons"))
            months += value
        else if (std.mem.eql(u8, unit, "day") or std.mem.eql(u8, unit, "days"))
            days = value
        else
            return error.InvalidTemporalText;
    }
    return .{ .months = std.math.cast(i32, months) orelse return error.TemporalOutOfRange, .days = days, .micros = micros };
}

test "SQL temporal canonical text exactly matches independent PostgreSQL output" {
    const Entry = struct { kind: temporal.Kind, binary: []const u8, text: []const u8 };
    const fixture = try std.json.parseFromSlice(struct { entries: []const Entry }, std.testing.allocator, @embedFile("fixtures/sql_temporal_reference.json"), .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    var count: usize = 0;
    for (fixture.value.entries) |entry| {
        var frame: [16]u8 = undefined;
        const binary = try std.fmt.hexToBytes(&frame, entry.binary);
        const value = try temporal.decode(entry.kind, binary);
        var output: [output_capacity]u8 = undefined;
        try std.testing.expectEqualStrings(entry.text, try format(value, &output));
        const parsed = try parseCanonical(entry.kind, entry.text);
        var encoded: [16]u8 = undefined;
        try std.testing.expectEqualSlices(u8, binary, try temporal.encode(parsed, &encoded));
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 174), count);
}

test "SQL temporal calendar fields preserve full date and timestamp domains without allocations" {
    const t = std.testing;
    for ([_]i32{ temporal.min_date, -730_485, -10_958, -10_957, -1, 0, 59, temporal.end_date - 1 }) |days| {
        const date = try temporal.Date.init(days);
        try t.expectEqual(days, (try (try Civil.fromDate(date)).toDate()).days);
    }
    var state: u64 = 0x8be3_029d_efe7_a659;
    for (0..20_000) |_| {
        state = state *% 6364136223846793005 +% 1442695040888963407;
        const day: i32 = @intCast(@as(i64, temporal.min_date) + @as(i64, @intCast(state % @as(u64, temporal.end_date - @as(i64, temporal.min_date)))));
        const date = try temporal.Date.init(day);
        try t.expectEqual(day, (try (try Civil.fromDate(date)).toDate()).days);
        const micros: i64 = @intCast(@as(i128, temporal.min_timestamp) + @as(i128, state % @as(u64, temporal.end_timestamp - @as(i128, temporal.min_timestamp))));
        const timestamp = try temporal.Timestamp.init(micros);
        try t.expectEqual(micros, (try (try Fields.fromTimestamp(timestamp)).toTimestamp()).micros);
    }
    const bc = try (Civil{ .year = 0, .month = 1, .day = 1 }).toDate();
    var output: [output_capacity]u8 = undefined;
    try t.expectEqualStrings("0001-01-01 BC", try format(.{ .date = bc }, &output));
    try t.expectEqual(@as(i64, temporal.micros_per_day), (try (Fields{ .date = .{ .days = 0 }, .time = .{ .micros = temporal.micros_per_day } }).toTimestamp()).micros);
    try t.expectError(error.InvalidCalendarDate, (Civil{ .year = 1900, .month = 2, .day = 29 }).toDate());
    _ = try (Civil{ .year = 2000, .month = 2, .day = 29 }).toDate();
    try t.expectError(error.InvalidCalendarDate, (Civil{ .year = 2000, .month = 0, .day = 1 }).toDate());
    try t.expectError(error.InvalidCalendarDate, (Civil{ .year = 2000, .month = 1, .day = 0 }).toDate());
    try t.expectError(error.TemporalOutOfRange, (Civil{ .year = std.math.minInt(i32), .month = 1, .day = 1 }).toDate());
    try t.expectError(error.InfiniteCalendarValue, Civil.fromDate(.{ .days = std.math.maxInt(i32) }));
    try t.expectError(error.InfiniteCalendarValue, Fields.fromTimestamp(.{ .micros = std.math.minInt(i64) }));
    try t.expectError(error.TemporalOutOfRange, (Fields{ .date = .{ .days = temporal.end_date - 1 }, .time = .{ .micros = 0 } }).toTimestamp());
    output = @splat(0xa5);
    try t.expectError(error.TemporalOutOfRange, format(.{ .time_tz = .{ .time = .{ .micros = 0 }, .seconds_west = std.math.minInt(i32) } }, &output));
    for (output) |byte| try t.expectEqual(@as(u8, 0xa5), byte);
}

test "SQL temporal canonical decoder rejects alternate spellings malformed text and unbounded input" {
    const t = std.testing;
    const Case = struct { kind: temporal.Kind, text: []const u8 };
    for ([_]Case{
        .{ .kind = .date, .text = "0000-01-01" },
        .{ .kind = .date, .text = "2000-1-01" },
        .{ .kind = .date, .text = "2000-01-01 AD" },
        .{ .kind = .date, .text = "1900-02-29" },
        .{ .kind = .date, .text = "2000-01-01\x00" },
        .{ .kind = .date, .text = "2147483647-01-01 BC" },
        .{ .kind = .time, .text = "infinity" },
        .{ .kind = .time, .text = "12:00:00.100000" },
        .{ .kind = .time, .text = "12:00:00." },
        .{ .kind = .time, .text = "12:00:00.1234567" },
        .{ .kind = .time, .text = "12:00:60" },
        .{ .kind = .time, .text = "24:00:00.000001" },
        .{ .kind = .time, .text = "18446744073709551615:00:00" },
        .{ .kind = .time_tz, .text = "01:00:00+00:00" },
        .{ .kind = .time_tz, .text = "01:00:00-00" },
        .{ .kind = .time_tz, .text = "01:00:00+16" },
        .{ .kind = .time_tz, .text = "01:00:00+00:60" },
        .{ .kind = .time_tz, .text = "01:00:00+00:00:60" },
        .{ .kind = .time_tz, .text = "01:00:00+00:00:00:00" },
        .{ .kind = .timestamp, .text = "2000-01-01 24:00:00" },
        .{ .kind = .timestamp, .text = "2000-01-01 00:00:00+00" },
        .{ .kind = .timestamp_tz, .text = "2000-01-01 00:00:00" },
        .{ .kind = .timestamp_tz, .text = "2000-01-01 00:00:00+01" },
        .{ .kind = .interval, .text = "1 year 1 year" },
        .{ .kind = .interval, .text = "0 days" },
        .{ .kind = .interval, .text = "1 mons" },
        .{ .kind = .interval, .text = "-1 mon" },
        .{ .kind = .interval, .text = "1 mon 1 mon" },
        .{ .kind = .interval, .text = "1 day 2 days" },
        .{ .kind = .interval, .text = "00:00:00 00:00:00" },
        .{ .kind = .interval, .text = "2147483647 years" },
        .{ .kind = .interval, .text = "2562047788:00:54.775808" },
    }) |case| {
        if (parseCanonical(case.kind, case.text)) |_| return error.TestExpectedError else |_| {}
    }
    var bytes: [output_capacity + 1]u8 = @splat('1');
    for (std.enums.values(temporal.Kind)) |kind| {
        try t.expectError(error.InvalidTemporalText, parseCanonical(kind, ""));
        try t.expectError(error.InvalidTemporalText, parseCanonical(kind, &bytes));
    }
    var state: u64 = 0x1945_ccdb_e291_351b;
    for (0..3_000) |i| {
        for (&bytes) |*byte| {
            state = state *% 6364136223846793005 +% 1442695040888963407;
            byte.* = @truncate(state >> 23);
        }
        const text = bytes[0 .. i % bytes.len];
        for (std.enums.values(temporal.Kind)) |kind| {
            if (parseCanonical(kind, text)) |value| {
                var output: [output_capacity]u8 = undefined;
                try t.expectEqualStrings(text, try format(value, &output));
            } else |_| {}
        }
    }
}

test "SQL temporal canonical text roundtrips arbitrary domain components without allocations" {
    var state: u64 = 0x3fc1_5f3a_0a56_61bd;
    for (0..12_000) |i| {
        state = state *% 6364136223846793005 +% 1442695040888963407;
        const value: temporal.Value = switch (i % 6) {
            0 => .{ .date = .{ .days = @intCast(@as(i64, temporal.min_date) + @as(i64, @intCast(state % @as(u64, temporal.end_date - @as(i64, temporal.min_date))))) } },
            1 => .{ .time = .{ .micros = @intCast(state % (temporal.micros_per_day + 1)) } },
            2 => .{ .time_tz = .{
                .time = .{ .micros = @intCast(state % (temporal.micros_per_day + 1)) },
                .seconds_west = @as(i32, @intCast(state % 115_199)) - 57_599,
            } },
            3, 4 => blk: {
                const instant: temporal.Timestamp = .{ .micros = @intCast(@as(i128, temporal.min_timestamp) + @as(i128, state % @as(u64, temporal.end_timestamp - @as(i128, temporal.min_timestamp)))) };
                break :blk if (i % 6 == 3) .{ .timestamp = instant } else .{ .timestamp_tz = instant };
            },
            5 => .{ .interval = .{ .micros = @bitCast(state), .months = @bitCast(@as(u32, @truncate(state >> 17))), .days = @bitCast(@as(u32, @truncate(state >> 33))) } },
            else => unreachable,
        };
        var text: [output_capacity]u8 = undefined;
        const parsed = try parseCanonical(value.kind(), try format(value, &text));
        var original: [16]u8 = undefined;
        var restored: [16]u8 = undefined;
        try std.testing.expectEqualSlices(u8, try temporal.encode(value, &original), try temporal.encode(parsed, &restored));
    }
}

test "SQL temporal mixed canonical text throughput is bounded and allocation free" {
    const values = [_]temporal.Value{
        .{ .date = .{ .days = temporal.min_date } },
        .{ .time = .{ .micros = temporal.micros_per_day } },
        .{ .time_tz = .{ .time = .{ .micros = 45_296_123_456 }, .seconds_west = -20_700 } },
        .{ .timestamp = .{ .micros = temporal.end_timestamp - 1 } },
        .{ .timestamp_tz = .{ .micros = -946_684_800_000_001 } },
        .{ .interval = .{ .months = -14, .days = 3, .micros = std.math.minInt(i64) } },
    };
    var output: [output_capacity]u8 = undefined;
    var checksum: u64 = 0;
    const start = std.Io.Clock.now(.awake, std.testing.io).nanoseconds;
    for (0..100_000) |i| {
        const value = values[i % values.len];
        const text = try format(value, &output);
        const decoded = try parseCanonical(value.kind(), text);
        checksum +%= text.len;
        std.mem.doNotOptimizeAway(decoded);
    }
    std.mem.doNotOptimizeAway(checksum);
    std.debug.print("SQL temporal canonical text: rows=100000 output_bytes=128 heap_bytes=0 elapsed_ns={}\n", .{std.Io.Clock.now(.awake, std.testing.io).nanoseconds - start});
}
