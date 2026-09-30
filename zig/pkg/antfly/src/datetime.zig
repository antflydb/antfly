// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Shared canonical UTC datetime contract used by storage and SQL.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Unit = enum { year, quarter, month, week, day, hour, minute, second, milliseconds, microseconds, epoch, dow, isodow, doy };

pub fn unit(text: []const u8) ?Unit {
    inline for (std.meta.fields(Unit)) |field| if (std.ascii.eqlIgnoreCase(text, field.name)) return @enumFromInt(field.value);
    return null;
}

pub fn part(ns: u64, field: Unit) f64 {
    const seconds = ns / std.time.ns_per_s;
    const days: i64 = @intCast(seconds / 86400);
    const civil = civilFromDays(days);
    const seconds_of_day = seconds % 86400;
    const isodow = @mod(days + 3, 7) + 1;
    return switch (field) {
        .year => @floatFromInt(civil.year),
        .quarter => @floatFromInt((civil.month - 1) / 3 + 1),
        .month => @floatFromInt(civil.month),
        .day => @floatFromInt(civil.day),
        .hour => @floatFromInt(seconds_of_day / 3600),
        .minute => @floatFromInt(seconds_of_day % 3600 / 60),
        .second => @as(f64, @floatFromInt(ns % (60 * std.time.ns_per_s))) / std.time.ns_per_s,
        .milliseconds => @as(f64, @floatFromInt(ns % (60 * std.time.ns_per_s))) / std.time.ns_per_ms,
        .microseconds => @as(f64, @floatFromInt(ns % (60 * std.time.ns_per_s))) / std.time.ns_per_us,
        .epoch => @as(f64, @floatFromInt(ns)) / std.time.ns_per_s,
        .dow => @floatFromInt(@mod(days + 4, 7)),
        .isodow => @floatFromInt(isodow),
        .doy => @floatFromInt(days - daysFromCivil(civil.year, 1, 1) + 1),
        .week => blk: {
            const thursday = days + 4 - isodow;
            const iso_year = civilFromDays(thursday).year;
            break :blk @floatFromInt(@divFloor(thursday - daysFromCivil(iso_year, 1, 1), 7) + 1);
        },
    };
}

pub fn truncate(ns: u64, field: Unit) ?u64 {
    const days: i64 = @intCast(ns / std.time.ns_per_s / 86400);
    const civil = civilFromDays(days);
    return switch (field) {
        .year => civilDateTimeToNs(civil.year, 1, 1, 0, 0, 0, 0),
        .quarter => civilDateTimeToNs(civil.year, (civil.month - 1) / 3 * 3 + 1, 1, 0, 0, 0, 0),
        .month => civilDateTimeToNs(civil.year, civil.month, 1, 0, 0, 0, 0),
        .week => std.math.cast(u64, @as(i128, days - @mod(days + 3, 7)) * 86400 * std.time.ns_per_s),
        .day => ns / std.time.ns_per_day * std.time.ns_per_day,
        .hour => ns / std.time.ns_per_hour * std.time.ns_per_hour,
        .minute => ns / std.time.ns_per_min * std.time.ns_per_min,
        .second => ns / std.time.ns_per_s * std.time.ns_per_s,
        .milliseconds => ns / std.time.ns_per_ms * std.time.ns_per_ms,
        .microseconds => ns / std.time.ns_per_us * std.time.ns_per_us,
        else => null,
    };
}

pub fn parseDateTimeToNs(text: []const u8) ?u64 {
    return parseRfc3339ToNs(text) orelse parseDateToNs(text);
}

pub fn formatDateTimeNsAlloc(alloc: Allocator, ns: u64) ![]u8 {
    const seconds = ns / std.time.ns_per_s;
    const nanos = ns % std.time.ns_per_s;
    const days: i64 = @intCast(seconds / 86_400);
    const seconds_of_day = seconds % 86_400;
    const civil = civilFromDays(days);
    if (civil.year < 0 or civil.year > 9999) return error.InvalidDateTime;
    return try std.fmt.allocPrint(alloc, "{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}.{d:0>9}Z", .{
        @as(u64, @intCast(civil.year)),
        civil.month,
        civil.day,
        seconds_of_day / 3_600,
        (seconds_of_day % 3_600) / 60,
        seconds_of_day % 60,
        nanos,
    });
}

pub fn parseRfc3339ToNs(text: []const u8) ?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or
        (text[10] != 'T' and text[10] != 't') or
        text[13] != ':' or text[16] != ':') return null;
    if (!digits(text[0..4]) or !digits(text[5..7]) or !digits(text[8..10]) or
        !digits(text[11..13]) or !digits(text[14..16]) or !digits(text[17..19])) return null;

    const year = std.fmt.parseInt(i64, text[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, text[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, text[8..10], 10) catch return null;
    const hour = std.fmt.parseInt(i64, text[11..13], 10) catch return null;
    const minute = std.fmt.parseInt(i64, text[14..16], 10) catch return null;
    const second = std.fmt.parseInt(i64, text[17..19], 10) catch return null;

    var idx: usize = 19;
    var nanos: u64 = 0;
    if (idx < text.len and text[idx] == '.') {
        idx += 1;
        const frac_start = idx;
        while (idx < text.len and text[idx] >= '0' and text[idx] <= '9') : (idx += 1) {}
        const frac = text[frac_start..idx];
        if (frac.len == 0 or frac.len > 9) return null;
        var frac_ns = std.fmt.parseInt(u64, frac, 10) catch return null;
        var scale: usize = frac.len;
        while (scale < 9) : (scale += 1) frac_ns *= 10;
        nanos = frac_ns;
    }
    if (idx >= text.len) return null;
    var offset_seconds: i64 = 0;
    if (text[idx] == 'Z' or text[idx] == 'z') {
        idx += 1;
    } else if (text[idx] == '+' or text[idx] == '-') {
        if (idx + 6 > text.len or text[idx + 3] != ':') return null;
        if (!digits(text[idx + 1 .. idx + 3]) or !digits(text[idx + 4 .. idx + 6])) return null;
        const offset_hour = std.fmt.parseInt(i64, text[idx + 1 .. idx + 3], 10) catch return null;
        const offset_minute = std.fmt.parseInt(i64, text[idx + 4 .. idx + 6], 10) catch return null;
        if (offset_hour > 23 or offset_minute > 59) return null;
        offset_seconds = offset_hour * 3_600 + offset_minute * 60;
        if (text[idx] == '-') offset_seconds = -offset_seconds;
        idx += 6;
    } else {
        return null;
    }
    if (idx != text.len) return null;

    const local_ns = civilDateTimeToSignedNs(year, month, day, hour, minute, second, nanos) orelse return null;
    const offset_ns = @as(i128, offset_seconds) * std.time.ns_per_s;
    return std.math.cast(u64, local_ns - offset_ns);
}

pub fn parseDateToNs(value: []const u8) ?u64 {
    if (value.len != 10 or value[4] != '-' or value[7] != '-') return null;
    if (!digits(value[0..4]) or !digits(value[5..7]) or !digits(value[8..10])) return null;
    const year = std.fmt.parseInt(i64, value[0..4], 10) catch return null;
    const month = std.fmt.parseInt(i64, value[5..7], 10) catch return null;
    const day = std.fmt.parseInt(i64, value[8..10], 10) catch return null;
    return civilDateTimeToNs(year, month, day, 0, 0, 0, 0);
}

fn digits(value: []const u8) bool {
    for (value) |byte| if (!std.ascii.isDigit(byte)) return false;
    return true;
}

test "datetime shared contract rejects malformed signed fields and normalizes UTC" {
    for ([_][]const u8{ "2024-+1-01", "2023-02-29", "2024-02-29T12:00:60Z", "2024-02-29T12:00:00+-1:00", "2024-02-29T+1:00:00Z", "1969-12-31" }) |invalid| try std.testing.expect(parseDateTimeToNs(invalid) == null);
    const ns = parseDateTimeToNs("2024-02-29T13:14:15+01:00").?;
    const output = try formatDateTimeNsAlloc(std.testing.allocator, ns);
    defer std.testing.allocator.free(output);
    try std.testing.expectEqualStrings("2024-02-29T12:14:15.000000000Z", output);
    try std.testing.expectEqual(@as(f64, 60), part(ns, .doy));
    try std.testing.expectEqual(@as(f64, 9), part(ns, .week));
}

pub fn civilDateTimeToNs(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, nanos: u64) ?u64 {
    return std.math.cast(u64, civilDateTimeToSignedNs(year, month, day, hour, minute, second, nanos) orelse return null);
}

fn civilDateTimeToSignedNs(year: i64, month: i64, day: i64, hour: i64, minute: i64, second: i64, nanos: u64) ?i128 {
    if (month < 1 or month > 12) return null;
    const max_day = daysInMonth(year, month) orelse return null;
    if (day < 1 or day > max_day) return null;
    if (hour < 0 or hour > 23) return null;
    if (minute < 0 or minute > 59) return null;
    // Leap-second validation requires an up-to-date leap-second table. Reject
    // `:60` instead of accepting it at arbitrary minutes and silently
    // normalizing it to the following minute.
    if (second < 0 or second > 59) return null;
    if (nanos >= std.time.ns_per_s) return null;

    const days = daysFromCivil(year, month, day);
    const seconds = @as(i128, days) * 86_400 + hour * 3_600 + minute * 60 + second;
    return seconds * std.time.ns_per_s + nanos;
}

fn daysInMonth(year: i64, month: i64) ?i64 {
    return switch (month) {
        1, 3, 5, 7, 8, 10, 12 => 31,
        4, 6, 9, 11 => 30,
        2 => if (isLeapYear(year)) 29 else 28,
        else => null,
    };
}

fn isLeapYear(year: i64) bool {
    return @mod(year, 4) == 0 and (@mod(year, 100) != 0 or @mod(year, 400) == 0);
}

pub const CivilDate = struct {
    year: i64,
    month: u8,
    day: u8,
};

pub fn civilFromDays(days_since_epoch: i64) CivilDate {
    const z = days_since_epoch + 719_468;
    const era = @divFloor(if (z >= 0) z else z - 146_096, 146_097);
    const doe = z - era * 146_097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36_524) - @divFloor(doe, 146_096), 365);
    var year = yoe + era * 400;
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const day = doy - @divFloor(153 * mp + 2, 5) + 1;
    const month = mp + if (mp < 10) @as(i64, 3) else @as(i64, -9);
    year += if (month <= 2) @as(i64, 1) else @as(i64, 0);
    return .{
        .year = year,
        .month = @intCast(month),
        .day = @intCast(day),
    };
}

pub fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}
