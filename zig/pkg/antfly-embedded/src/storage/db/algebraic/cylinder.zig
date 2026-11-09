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
const Allocator = std.mem.Allocator;

const datetime = @import("../../../datetime.zig");
pub const Bucket = union(enum) {
    minute,
    hour,
    day,
    week,
    month,
    quarter,
    year,
    fixed: u64,

    pub fn parse(text: []const u8) ?Bucket {
        inline for (.{ "minute", "hour", "day", "week", "month", "quarter", "year" }) |name| {
            if (std.mem.eql(u8, text, name)) return @unionInit(Bucket, name, {});
        }
        return .{ .fixed = datetime.durationNs(text) orelse return null };
    }
};

/// Canonical aliases preserve existing whole-hour/day materializations. Fixed
/// weeks remain epoch aligned and must never alias calendar Monday weeks.
pub fn canonicalBucketName(text: []const u8) ?[]const u8 {
    const bucket = Bucket.parse(text) orelse return null;
    if (bucket == .fixed) {
        if (bucket.fixed == std.time.ns_per_min) return "minute";
        if (bucket.fixed == std.time.ns_per_hour) return "hour";
        if (bucket.fixed == std.time.ns_per_day) return "day";
    }
    return text;
}

pub fn bucketStartAlloc(alloc: Allocator, bucket: Bucket, rfc3339: []const u8) ![]u8 {
    const ns = datetime.parseRfc3339ToSignedNs(rfc3339) orelse return error.InvalidTimestamp;
    const start = if (bucket == .fixed) datetime.binSigned(ns, bucket.fixed, 0).? else datetime.truncateSigned(ns, switch (bucket) {
        .minute => .minute,
        .hour => .hour,
        .day => .day,
        .week => .week,
        .month => .month,
        .quarter => .quarter,
        .year => .year,
        .fixed => unreachable,
    }) orelse return error.InvalidTimestamp;
    const formatted = try datetime.formatDateTimeSignedNsAlloc(alloc, start);
    // Preserve existing whole-second materialization keys across upgrades.
    if (@mod(start, std.time.ns_per_s) == 0) {
        defer alloc.free(formatted);
        return std.fmt.allocPrint(alloc, "{s}Z", .{formatted[0..19]});
    }
    return formatted;
}

pub fn unixSeconds(rfc3339: []const u8) !i64 {
    if (!isSupportedRfc3339Utc(rfc3339)) return error.InvalidTimestamp;
    const year = try std.fmt.parseInt(i32, rfc3339[0..4], 10);
    const month = try std.fmt.parseInt(u8, rfc3339[5..7], 10);
    const day = try std.fmt.parseInt(u8, rfc3339[8..10], 10);
    const hour = try std.fmt.parseInt(u8, rfc3339[11..13], 10);
    const minute = try std.fmt.parseInt(u8, rfc3339[14..16], 10);
    const second = try std.fmt.parseInt(u8, rfc3339[17..19], 10);
    if (month < 1 or month > 12 or day < 1 or day > 31 or hour > 23 or minute > 59 or second > 60) return error.InvalidTimestamp;
    const days = daysFromCivil(year, month, day);
    return days * 86_400 + @as(i64, hour) * 3_600 + @as(i64, minute) * 60 + @as(i64, second);
}

fn daysFromCivil(year_in: i32, month_in: u8, day_in: u8) i64 {
    var year = @as(i64, year_in);
    const month = @as(i64, month_in);
    const day = @as(i64, day_in);
    year -= @intFromBool(month <= 2);
    const era = @divFloor(year, 400);
    const yoe = year - era * 400;
    const month_prime = month + if (month > 2) @as(i64, -3) else 9;
    const doy = @divFloor(153 * month_prime + 2, 5) + day - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146_097 + doe - 719_468;
}

fn isSupportedRfc3339Utc(text: []const u8) bool {
    if (text.len < 20 or text[text.len - 1] != 'Z') return false;
    if (text[4] != '-' or text[7] != '-' or text[10] != 'T' or text[13] != ':' or text[16] != ':') return false;
    const digit_positions = [_]usize{ 0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, 17, 18 };
    for (digit_positions) |idx| {
        if (text[idx] < '0' or text[idx] > '9') return false;
    }
    if (text.len == 20) return true;
    if (text[19] != '.') return false;
    if (text.len == 21) return false;
    for (text[20 .. text.len - 1]) |ch| {
        if (ch < '0' or ch > '9') return false;
    }
    return true;
}

test "bucket start normalizes simple RFC3339 strings" {
    const alloc = std.testing.allocator;
    const hour = try bucketStartAlloc(alloc, .hour, "2026-05-10T14:03:00Z");
    defer alloc.free(hour);
    try std.testing.expectEqualStrings("2026-05-10T14:00:00Z", hour);
    const day = try bucketStartAlloc(alloc, .day, "2026-05-10T14:03:00Z");
    defer alloc.free(day);
    try std.testing.expectEqualStrings("2026-05-10T00:00:00Z", day);
    const month = try bucketStartAlloc(alloc, .month, "2026-05-10T14:03:00Z");
    defer alloc.free(month);
    try std.testing.expectEqualStrings("2026-05-01T00:00:00Z", month);
    try std.testing.expectError(error.InvalidTimestamp, bucketStartAlloc(alloc, .day, "2026-05-10"));
    const offset = try bucketStartAlloc(alloc, .day, "2026-05-10T14:03:00-07:00");
    defer alloc.free(offset);
    try std.testing.expectEqualStrings("2026-05-10T00:00:00Z", offset);
    try std.testing.expectEqual(@as(i64, 0), try unixSeconds("1970-01-01T00:00:00Z"));
    try std.testing.expectEqual(@as(i64, 3600), try unixSeconds("1970-01-01T01:00:00Z"));
}

test "time series algebraic buckets share fixed duration and normalized instants" {
    const a = std.testing.allocator;
    const first = try bucketStartAlloc(a, Bucket.parse("5m").?, "2026-11-06T13:04:59.999999999+01:00");
    defer a.free(first);
    try std.testing.expectEqualStrings("2026-11-06T12:00:00Z", first);
    const precise = try bucketStartAlloc(a, Bucket.parse("10ns").?, "2026-11-06T12:00:00.000000019Z");
    defer a.free(precise);
    try std.testing.expectEqualStrings("2026-11-06T12:00:00.000000010Z", precise);
    try std.testing.expect(Bucket.parse("0s") == null);
}

pub fn equivalentBucketNames(left: []const u8, right: []const u8) bool {
    const a = Bucket.parse(canonicalBucketName(left) orelse return false) orelse return false;
    const b = Bucket.parse(canonicalBucketName(right) orelse return false) orelse return false;
    return std.meta.eql(a, b);
}
