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

//! Strict parsing of provider retention timestamps. Unsupported shapes decline
//! optional certificates. Millisecond truncation expires protection early.
const std = @import("std");
fn digits(text: []const u8) ?u64 {
    for (text) |byte| if (byte < '0' or byte > '9') return null;
    return std.fmt.parseUnsigned(u64, text, 10) catch null;
}
fn utc(year: u64, month: u64, day: u64, hour: u64, minute: u64, second: u64) ?u64 {
    if (year < 1970 or year > 9999 or month < 1 or month > 12 or day < 1 or hour > 23 or minute > 59 or second > 59) return null;
    const leap = year % 4 == 0 and (year % 100 != 0 or year % 400 == 0);
    const lengths = [_]u8{ 31, if (leap) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (day > lengths[month - 1]) return null;
    const y = year - 1;
    var days = 365 * (year - 1970) + (y / 4 - 1969 / 4) - (y / 100 - 1969 / 100) + (y / 400 - 1969 / 400);
    for (lengths[0 .. month - 1]) |length| days += length;
    days += day - 1;
    return ((days * 24 + hour) * 60 + minute) * 60_000 + second * 1000;
}
pub fn rfc3339(text: []const u8) ?u64 {
    if (text.len < 20 or text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':' or text[text.len - 1] != 'Z') return null;
    const base = utc(digits(text[0..4]) orelse return null, digits(text[5..7]) orelse return null, digits(text[8..10]) orelse return null, digits(text[11..13]) orelse return null, digits(text[14..16]) orelse return null, digits(text[17..19]) orelse return null) orelse return null;
    if (text.len == 20) return base;
    if (text[19] != '.' or text.len < 22 or text.len > 30) return null;
    const fraction = text[20 .. text.len - 1];
    _ = digits(fraction) orelse return null;
    var ms: u64 = 0;
    for (0..3) |i| ms = ms * 10 + if (i < fraction.len) fraction[i] - '0' else @as(u64, 0);
    return base + ms;
}
pub fn httpDate(text: []const u8) ?u64 {
    if (text.len != 29 or !std.mem.eql(u8, text[3..5], ", ") or text[7] != ' ' or text[11] != ' ' or text[16] != ' ' or text[19] != ':' or text[22] != ':' or !std.mem.eql(u8, text[25..], " GMT")) return null;
    var month: ?u64 = null;
    for ([_][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" }, 1..) |name, index| if (std.mem.eql(u8, name, text[8..11])) {
        month = index;
        break;
    };
    return utc(digits(text[12..16]) orelse return null, month orelse return null, digits(text[5..7]) orelse return null, digits(text[17..19]) orelse return null, digits(text[20..22]) orelse return null, digits(text[23..25]) orelse return null);
}
test "provider retention timestamps reject malformed dates and truncate expiry conservatively" {
    try std.testing.expectEqual(@as(?u64, 0), rfc3339("1970-01-01T00:00:00Z"));
    try std.testing.expectEqual(rfc3339("2026-10-11T12:34:56Z"), httpDate("Sun, 11 Oct 2026 12:34:56 GMT"));
    try std.testing.expectEqual(rfc3339("2024-02-29T00:00:00.123Z"), rfc3339("2024-02-29T00:00:00.123999999Z"));
    try std.testing.expect(rfc3339("2025-02-29T00:00:00Z") == null);
    try std.testing.expect(rfc3339("2024-02-29T00:00:00.Z") == null);
    try std.testing.expect(httpDate("Sun, 11 Oct 2026 12:34:56 UTC") == null);
}
