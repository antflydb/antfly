// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license.

//! Allocation-free RFC 3339 parsing for public query contracts.
//!
//! Parsed values are normalized to UTC nanoseconds. Numeric offsets are
//! accepted as required by OpenAPI `date-time`; calendar and clock fields are
//! validated before conversion instead of being normalized implicitly.

const std = @import("std");

pub fn parseToUnixNs(text: []const u8) ?u64 {
    if (text.len < 20) return null;
    if (text[4] != '-' or text[7] != '-' or
        (text[10] != 'T' and text[10] != 't') or
        text[13] != ':' or text[16] != ':') return null;

    const year = parseDigits(text[0..4]) orelse return null;
    const month = parseDigits(text[5..7]) orelse return null;
    const day = parseDigits(text[8..10]) orelse return null;
    const hour = parseDigits(text[11..13]) orelse return null;
    const minute = parseDigits(text[14..16]) orelse return null;
    const second = parseDigits(text[17..19]) orelse return null;
    if (!validDate(year, month, day) or hour > 23 or minute > 59 or second > 59) return null;

    var index: usize = 19;
    var nanos: u64 = 0;
    if (index < text.len and text[index] == '.') {
        index += 1;
        const fraction_start = index;
        while (index < text.len and std.ascii.isDigit(text[index])) : (index += 1) {}
        const fraction = text[fraction_start..index];
        if (fraction.len == 0 or fraction.len > 9) return null;
        nanos = parseDigits(fraction) orelse return null;
        var scale = fraction.len;
        while (scale < 9) : (scale += 1) nanos *= 10;
    }

    var offset_seconds: i64 = 0;
    if (index < text.len and (text[index] == 'Z' or text[index] == 'z')) {
        index += 1;
    } else {
        if (index + 6 != text.len or (text[index] != '+' and text[index] != '-') or text[index + 3] != ':') return null;
        const offset_hour = parseDigits(text[index + 1 .. index + 3]) orelse return null;
        const offset_minute = parseDigits(text[index + 4 .. index + 6]) orelse return null;
        if (offset_hour > 23 or offset_minute > 59) return null;
        offset_seconds = @as(i64, @intCast(offset_hour * 3_600 + offset_minute * 60));
        if (text[index] == '-') offset_seconds = -offset_seconds;
        index += 6;
    }
    if (index != text.len) return null;

    const days = daysFromCivil(@intCast(year), @intCast(month), @intCast(day));
    const local_seconds = @as(i128, days) * 86_400 +
        @as(i128, @intCast(hour)) * 3_600 +
        @as(i128, @intCast(minute)) * 60 +
        @as(i128, @intCast(second));
    const utc_seconds = local_seconds - offset_seconds;
    if (utc_seconds < 0) return null;
    const total_ns = utc_seconds * std.time.ns_per_s + nanos;
    if (total_ns > std.math.maxInt(u64)) return null;
    return @intCast(total_ns);
}

pub fn parseDateToUnixNs(text: []const u8) ?u64 {
    if (text.len != 10 or text[4] != '-' or text[7] != '-') return null;
    const year = parseDigits(text[0..4]) orelse return null;
    const month = parseDigits(text[5..7]) orelse return null;
    const day = parseDigits(text[8..10]) orelse return null;
    if (!validDate(year, month, day)) return null;
    const days = daysFromCivil(@intCast(year), @intCast(month), @intCast(day));
    if (days < 0) return null;
    const total_ns = @as(i128, days) * 86_400 * std.time.ns_per_s;
    if (total_ns > std.math.maxInt(u64)) return null;
    return @intCast(total_ns);
}

fn parseDigits(text: []const u8) ?u64 {
    if (text.len == 0) return null;
    var value: u64 = 0;
    for (text) |byte| {
        if (!std.ascii.isDigit(byte)) return null;
        value = value * 10 + (byte - '0');
    }
    return value;
}

fn validDate(year: u64, month: u64, day: u64) bool {
    if (year == 0 or month < 1 or month > 12 or day < 1) return false;
    const days_in_month = [_]u8{ 31, if (isLeapYear(year)) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    return day <= days_in_month[month - 1];
}

fn isLeapYear(year: u64) bool {
    return @mod(year, 4) == 0 and (@mod(year, 100) != 0 or @mod(year, 400) == 0);
}

fn daysFromCivil(year: i64, month: i64, day: i64) i64 {
    var y = year;
    y -= if (month <= 2) @as(i64, 1) else @as(i64, 0);
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400;
    const mp = month + (if (month > 2) @as(i64, -3) else @as(i64, 9));
    const doy = @divFloor(153 * mp + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

test "RFC3339 offsets normalize to the same instant" {
    const utc = parseToUnixNs("2026-08-24T19:00:00Z").?;
    try std.testing.expectEqual(utc, parseToUnixNs("2026-08-24T12:00:00-07:00").?);
    try std.testing.expectEqual(utc, parseToUnixNs("2026-08-24T21:30:00+02:30").?);
}

test "RFC3339 parser validates calendar clock and offset fields" {
    try std.testing.expect(parseToUnixNs("2026-02-29T00:00:00Z") == null);
    try std.testing.expect(parseToUnixNs("2024-02-29T23:59:59.123456789Z") != null);
    try std.testing.expect(parseToUnixNs("2026-01-01T24:00:00Z") == null);
    try std.testing.expect(parseToUnixNs("2026-01-01T00:00:00+24:00") == null);
    try std.testing.expect(parseDateToUnixNs("2026-04-31") == null);
}
