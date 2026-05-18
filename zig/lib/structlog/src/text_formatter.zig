// Copyright 2026 Antfly, Inc.
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
const Field = @import("fields.zig").Field;

/// Formats log entries as human-readable text.
///
/// Output format:
///   2026-04-09T12:00:00Z INFO  [metadata] table created table=users ranges=3
pub fn format(
    writer: *std.Io.Writer,
    level: std.log.Level,
    scope: []const u8,
    message: []const u8,
    fields: []const Field,
) !void {
    try writeTimestamp(writer);
    try writer.writeByte(' ');
    try writePaddedLevel(writer, level);
    try writer.writeAll(" [");
    try writer.writeAll(scope);
    try writer.writeAll("] ");
    try writer.writeAll(message);

    for (fields) |f| {
        try writer.writeByte(' ');
        try writer.writeAll(f.key);
        try writer.writeByte('=');
        try f.writeTextValue(writer);
    }

    try writer.writeByte('\n');
}

fn writePaddedLevel(writer: *std.Io.Writer, level: std.log.Level) !void {
    const name = @tagName(level);
    try writer.writeAll(name);
    // Pad to 5 chars (longest is "debug")
    var i: usize = name.len;
    while (i < 5) : (i += 1) {
        try writer.writeByte(' ');
    }
}

fn writeTimestamp(writer: *std.Io.Writer) !void {
    var ts: std.c.timespec = undefined;
    _ = std.c.clock_gettime(.REALTIME, &ts);
    const epoch_secs: i64 = ts.sec;
    const epoch_day: i32 = @intCast(@divFloor(epoch_secs, 86400));
    const day_secs: u32 = @intCast(@mod(epoch_secs, 86400));

    const civil = civilFromDays(epoch_day);

    try writer.print("{d:0>4}-{d:0>2}-{d:0>2}T{d:0>2}:{d:0>2}:{d:0>2}Z", .{
        civil.year,
        civil.month,
        civil.day,
        day_secs / 3600,
        (day_secs % 3600) / 60,
        day_secs % 60,
    });
}

const CivilDate = struct { year: i32, month: u32, day: u32 };

fn civilFromDays(z_arg: i32) CivilDate {
    var z = z_arg;
    z += 719468;
    const era_v: i32 = if (z >= 0) z else z - 146096;
    const era: i32 = @divFloor(era_v, 146097);
    const doe: u32 = @intCast(z - era * 146097);
    const yoe: u32 = @intCast(@divFloor(@as(i32, @intCast(doe)) - @as(i32, @intCast(doe / 1460)) + @as(i32, @intCast(doe / 36524)) - @as(i32, @intCast(doe / 146096)), 365));
    const y: i32 = @as(i32, @intCast(yoe)) + era * 400;
    const doy: u32 = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const mp: u32 = (5 * doy + 2) / 153;
    const d: u32 = doy - (153 * mp + 2) / 5 + 1;
    const m: u32 = if (mp < 10) mp + 3 else mp - 9;
    return .{ .year = if (m <= 2) y + 1 else y, .month = m, .day = d };
}

test "text_formatter: basic output" {
    var w: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer w.deinit();

    try format(&w.writer, .info, "metadata", "table created", &.{
        Field.str("table", "users"),
        Field.uint("ranges", 3),
    });

    const output = w.writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, "info ") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "[metadata]") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "table created") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "table=users") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "ranges=3") != null);
    try std.testing.expect(std.mem.endsWith(u8, output, "\n"));
}
