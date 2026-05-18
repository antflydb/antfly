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
const Allocator = std.mem.Allocator;

pub const Bucket = enum {
    hour,
    day,
    month,

    pub fn parse(text: []const u8) ?Bucket {
        inline for (std.meta.fields(Bucket)) |field| {
            if (std.mem.eql(u8, text, field.name)) return @enumFromInt(field.value);
        }
        return null;
    }
};

pub fn bucketStartAlloc(alloc: Allocator, bucket: Bucket, rfc3339: []const u8) ![]u8 {
    if (!isSupportedRfc3339Utc(rfc3339)) return error.InvalidTimestamp;
    return switch (bucket) {
        .hour => try std.fmt.allocPrint(alloc, "{s}:00:00Z", .{rfc3339[0..13]}),
        .day => try std.fmt.allocPrint(alloc, "{s}T00:00:00Z", .{rfc3339[0..10]}),
        .month => try std.fmt.allocPrint(alloc, "{s}-01T00:00:00Z", .{rfc3339[0..7]}),
    };
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
    try std.testing.expectError(error.InvalidTimestamp, bucketStartAlloc(alloc, .day, "2026-05-10T14:03:00-07:00"));
    try std.testing.expectEqual(@as(i64, 0), try unixSeconds("1970-01-01T00:00:00Z"));
    try std.testing.expectEqual(@as(i64, 3600), try unixSeconds("1970-01-01T01:00:00Z"));
}
