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

const std = @import("std");

/// Parse all three HTTP-date wire formats, without allocation or locale state.
pub fn parse(raw: []const u8, now: i64) ?i64 {
    const value = std.mem.trim(u8, raw, " \t");
    var parts = std.mem.tokenizeAny(u8, value, " ,:-\t");
    const weekday = parts.next() orelse return null;
    const weekdays = [_][]const u8{ "Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" };
    var valid_day = false;
    for (weekdays) |name| if (std.mem.eql(u8, weekday, name)) {
        valid_day = true;
        break;
    };
    if (!valid_day) return null;
    const first = parts.next() orelse return null;
    const asctime = first.len == 3 and std.ascii.isAlphabetic(first[0]);
    const month_name = if (asctime) first else parts.next() orelse return null;
    const day = number(if (asctime) parts.next() orelse return null else first) orelse return null;
    const year_text = if (asctime) null else parts.next() orelse return null;
    const hour = number(parts.next() orelse return null) orelse return null;
    const minute = number(parts.next() orelse return null) orelse return null;
    const second = number(parts.next() orelse return null) orelse return null;
    const years = year_text orelse parts.next() orelse return null;
    var year: i64 = number(years) orelse return null;
    if (years.len == 2 and !asctime) {
        // RFC 850 dates more than 50 years in the future denote the past century.
        const current_year: i64 = @intCast((std.time.epoch.EpochSeconds{ .secs = @intCast(@max(0, now)) }).getEpochDay().calculateYearDay().year);
        year += @divFloor(current_year, 100) * 100;
        if (year > current_year + 50) year -= 100;
    } else if (years.len != 4) return null;
    if (!asctime and !std.mem.eql(u8, parts.next() orelse return null, "GMT")) return null;
    if (parts.next() != null or year < 1601 or year > 9999 or hour > 23 or minute > 59 or second > 59) return null;
    const months = [_][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
    var month: i64 = 0;
    for (months, 1..) |name, index| if (std.mem.eql(u8, name, month_name)) {
        month = @intCast(index);
        break;
    };
    if (month == 0) return null;
    if (years.len == 2 and !asctime) {
        const epoch = std.time.epoch.EpochSeconds{ .secs = @intCast(@max(0, now)) };
        const current = epoch.getEpochDay().calculateYearDay();
        const md = current.calculateMonthDay();
        const current_month: i64 = @backingInt(md.month);
        const later = month > current_month or (month == current_month and (day > @as(i64, md.day_index) + 1 or (day == @as(i64, md.day_index) + 1 and hour * 3600 + minute * 60 + second > @mod(now, 86400))));
        if (year == @as(i64, current.year) + 50 and later) year -= 100;
    }
    const leap = @mod(year, 4) == 0 and (@mod(year, 100) != 0 or @mod(year, 400) == 0);
    const month_days = [_]u8{ 31, if (leap) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (day == 0 or day > month_days[@intCast(month - 1)]) return null;
    // Gregorian civil date to days since Unix epoch, using 400-year eras.
    const y = year - @as(i64, if (month <= 2) 1 else 0);
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const doy = @divFloor(153 * (month + @as(i64, if (month > 2) -3 else 9)) + 2, 5) + day - 1;
    const days = era * 146097 + yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy - 719468;
    return days * 86400 + hour * 3600 + minute * 60 + second;
}

fn number(value: []const u8) ?i64 {
    if (value.len == 0) return null;
    for (value) |c| if (!std.ascii.isDigit(c)) return null;
    return std.fmt.parseInt(i64, value, 10) catch null;
}

pub fn retryMilliseconds(value: []const u8, now: i64, cap: u64) ?u64 {
    const text = std.mem.trim(u8, value, " \t");
    if (std.fmt.parseInt(u64, text, 10)) |seconds| return @min(cap, seconds *| 1000) else |_| {}
    const date = parse(text, now) orelse return null;
    if (date <= now) return 0;
    return @min(cap, @as(u64, @intCast(date - now)) *| 1000);
}

test "HTTP-date formats, obsolete year rollover and bounded Retry-After" {
    const t = std.testing;
    const timestamp = 784111777;
    for ([_][]const u8{ "Sun, 06 Nov 1994 08:49:37 GMT", "Sunday, 06-Nov-94 08:49:37 GMT", "Sun Nov  6 08:49:37 1994" }) |value| try t.expectEqual(@as(?i64, timestamp), parse(value, timestamp));
    try t.expectEqual(@as(?i64, null), parse("Sun, 31 Feb 1994 08:49:37 GMT", timestamp));
    try t.expectEqual(@as(?i64, null), parse("Sun, 06 Nov 1994 08:49:37 PST", timestamp));
    try t.expectEqual(@as(?u64, 2000), retryMilliseconds("Sun, 06 Nov 1994 08:49:37 GMT", timestamp - 2, 30_000));
    try t.expectEqual(@as(?u64, 0), retryMilliseconds("Sun, 06 Nov 1994 08:49:37 GMT", timestamp + 2, 30_000));
    try t.expectEqual(@as(?u64, 30_000), retryMilliseconds("18446744073709551615", timestamp, 30_000));
    try t.expectEqual(@as(?u64, null), retryMilliseconds("-1", timestamp, 30_000));
}

test "RFC850 fifty-year boundary includes month day and time" {
    const now = parse("Fri, 16 Oct 2026 12:00:00 GMT", 0).?;
    try std.testing.expectEqual(parse("Fri, 16 Oct 2076 12:00:00 GMT", now), parse("Friday, 16-Oct-76 12:00:00 GMT", now));
    try std.testing.expectEqual(parse("Fri, 16 Oct 1976 12:00:01 GMT", now), parse("Friday, 16-Oct-76 12:00:01 GMT", now));
    try std.testing.expectEqual(parse("Fri, 17 Oct 1976 00:00:00 GMT", now), parse("Friday, 17-Oct-76 00:00:00 GMT", now));
}
