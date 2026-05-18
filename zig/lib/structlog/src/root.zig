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

pub const Field = @import("fields.zig").Field;
pub const json_formatter = @import("json_formatter.zig");
pub const text_formatter = @import("text_formatter.zig");
const level_mod = @import("level.zig");
pub const setLevel = level_mod.setLevel;
pub const getLevel = level_mod.getLevel;
pub const isEnabled = level_mod.isEnabled;

pub const Formatter = enum {
    json,
    text,
};

/// Global formatter selection. Defaults to text for human-readable output.
var global_formatter: Formatter = .text;

/// Initialize the structured logging system.
pub fn init(config: struct {
    formatter: Formatter = .text,
    level: std.log.Level = .info,
}) void {
    global_formatter = config.formatter;
    setLevel(config.level);
}

/// Drop-in replacement for std.log's logFn.
/// Set this as `pub const std_options: std.Options = .{ .logFn = structlog.logFn };`
/// in your root source file to route all std.log calls through structured logging.
pub fn logFn(
    comptime message_level: std.log.Level,
    comptime scope: @EnumLiteral(),
    comptime format: []const u8,
    args: anytype,
) void {
    @disableInstrumentation();
    if (!isEnabled(message_level)) return;

    const scope_name = @tagName(scope);

    // Use stderr via std.debug
    var buffer: [64]u8 = undefined;
    const locked = std.debug.lockStderr(&buffer);
    defer std.debug.unlockStderr();

    const writer = &locked.file_writer.interface;

    // Format the message into the log line directly
    switch (global_formatter) {
        .json => {
            writer.writeAll("{\"ts\":\"") catch return;
            writeTimestamp(writer) catch return;
            writer.writeAll("\",\"level\":\"") catch return;
            writer.writeAll(@tagName(message_level)) catch return;
            writer.writeAll("\",\"scope\":\"") catch return;
            writer.writeAll(scope_name) catch return;
            writer.writeAll("\",\"msg\":\"") catch return;
            writer.print(format, args) catch return;
            writer.writeAll("\"}\n") catch return;
        },
        .text => {
            writeTimestamp(writer) catch return;
            writer.writeByte(' ') catch return;
            writer.writeAll(@tagName(message_level)) catch return;
            // Pad level to 5 chars
            var pad: usize = @tagName(message_level).len;
            while (pad < 5) : (pad += 1) {
                writer.writeByte(' ') catch return;
            }
            writer.writeAll(" [") catch return;
            writer.writeAll(scope_name) catch return;
            writer.writeAll("] ") catch return;
            writer.print(format, args) catch return;
            writer.writeByte('\n') catch return;
        },
    }
}

fn writeTimestamp(writer: *std.Io.Writer) !void {
    var ts: std.posix.timespec = undefined;
    const epoch_secs: i64 = switch (std.posix.errno(std.posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => ts.sec,
        else => 0,
    };
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

/// Create a scoped logger for structured logging with fields.
pub fn scoped(comptime scope: @EnumLiteral()) Logger {
    return .{ .scope_name = @tagName(scope), .fields = &.{} };
}

/// Structured logger that supports key-value fields.
pub const Logger = struct {
    scope_name: []const u8,
    fields: []const Field,

    pub fn info(self: Logger, comptime msg: []const u8, extra_fields: anytype) void {
        self.log(.info, msg, extra_fields);
    }

    pub fn warn(self: Logger, comptime msg: []const u8, extra_fields: anytype) void {
        self.log(.warn, msg, extra_fields);
    }

    pub fn err(self: Logger, comptime msg: []const u8, extra_fields: anytype) void {
        self.log(.err, msg, extra_fields);
    }

    pub fn debug(self: Logger, comptime msg: []const u8, extra_fields: anytype) void {
        self.log(.debug, msg, extra_fields);
    }

    fn log(self: Logger, level: std.log.Level, msg: []const u8, extra_fields: anytype) void {
        if (!isEnabled(level)) return;

        // Use stderr via std.debug
        var buffer: [64]u8 = undefined;
        const locked = std.debug.lockStderr(&buffer);
        defer std.debug.unlockStderr();

        const writer = &locked.file_writer.interface;

        switch (global_formatter) {
            .json => self.writeJson(writer, level, msg, extra_fields) catch return,
            .text => self.writeText(writer, level, msg, extra_fields) catch return,
        }
    }

    fn writeJson(self: Logger, writer: *std.Io.Writer, level: std.log.Level, msg: []const u8, extra_fields: anytype) !void {
        try writer.writeAll("{\"ts\":\"");
        try writeTimestamp(writer);
        try writer.writeAll("\",\"level\":\"");
        try writer.writeAll(@tagName(level));
        try writer.writeAll("\",\"scope\":\"");
        try writer.writeAll(self.scope_name);
        try writer.writeAll("\",\"msg\":\"");
        try writer.writeAll(msg);
        try writer.writeByte('"');

        // Write base fields
        for (self.fields) |f| {
            try writer.writeAll(",\"");
            try writer.writeAll(f.key);
            try writer.writeAll("\":");
            try f.writeJsonValue(writer);
        }

        // Write extra struct fields
        const ExtraType = @TypeOf(extra_fields);
        const extra_info = @typeInfo(ExtraType);
        switch (extra_info) {
            .@"struct" => |s| {
                inline for (s.fields) |f| {
                    const field = structFieldToLogField(f.name, @field(extra_fields, f.name));
                    try writer.writeAll(",\"");
                    try writer.writeAll(field.key);
                    try writer.writeAll("\":");
                    try field.writeJsonValue(writer);
                }
            },
            .pointer => |p| switch (@typeInfo(p.child)) {
                .array => {
                    for (extra_fields) |f| {
                        try writer.writeAll(",\"");
                        try writer.writeAll(f.key);
                        try writer.writeAll("\":");
                        try f.writeJsonValue(writer);
                    }
                },
                else => {},
            },
            else => {},
        }

        try writer.writeAll("}\n");
    }

    fn writeText(self: Logger, writer: *std.Io.Writer, level: std.log.Level, msg: []const u8, extra_fields: anytype) !void {
        try writeTimestamp(writer);
        try writer.writeByte(' ');
        const name = @tagName(level);
        try writer.writeAll(name);
        var pad: usize = name.len;
        while (pad < 5) : (pad += 1) {
            try writer.writeByte(' ');
        }
        try writer.writeAll(" [");
        try writer.writeAll(self.scope_name);
        try writer.writeAll("] ");
        try writer.writeAll(msg);

        // Write base fields
        for (self.fields) |f| {
            try writer.writeByte(' ');
            try writer.writeAll(f.key);
            try writer.writeByte('=');
            try f.writeTextValue(writer);
        }

        // Write extra struct fields
        const ExtraType = @TypeOf(extra_fields);
        const extra_info = @typeInfo(ExtraType);
        switch (extra_info) {
            .@"struct" => |s| {
                inline for (s.fields) |f| {
                    const field = structFieldToLogField(f.name, @field(extra_fields, f.name));
                    try writer.writeByte(' ');
                    try writer.writeAll(field.key);
                    try writer.writeByte('=');
                    try field.writeTextValue(writer);
                }
            },
            .pointer => |p| switch (@typeInfo(p.child)) {
                .array => {
                    for (extra_fields) |f| {
                        try writer.writeByte(' ');
                        try writer.writeAll(f.key);
                        try writer.writeByte('=');
                        try f.writeTextValue(writer);
                    }
                },
                else => {},
            },
            else => {},
        }

        try writer.writeByte('\n');
    }
};

fn structFieldToLogField(comptime key: []const u8, value: anytype) Field {
    const T = @TypeOf(value);
    return switch (@typeInfo(T)) {
        .int => |int_info| if (int_info.signedness == .signed)
            Field.int(key, @intCast(value))
        else
            Field.uint(key, @intCast(value)),
        .comptime_int => if (value < 0)
            Field.int(key, @intCast(value))
        else
            Field.uint(key, @intCast(value)),
        .float, .comptime_float => Field.float(key, @floatCast(value)),
        .bool => Field.boolean(key, value),
        .pointer => |ptr| blk: {
            if (ptr.size == .slice and ptr.child == u8) {
                break :blk Field.str(key, value);
            }
            break :blk Field.str(key, "<unsupported>");
        },
        .@"enum" => Field.str(key, @tagName(value)),
        else => Field.str(key, "<unsupported>"),
    };
}

test {
    std.testing.refAllDecls(@This());
}

test "scoped logger with struct fields" {
    // Just verify this compiles and doesn't crash.
    // Output goes to stderr which we can't easily capture in tests.
    const original_level = getLevel();
    defer setLevel(original_level);

    // Set level to err so nothing actually prints during tests
    setLevel(.err);

    const logger = scoped(.test_scope);
    logger.info("hello", .{ .count = 42, .name = "test" });
    logger.debug("debug msg", .{});
}

test "logFn compatibility" {
    const original_level = getLevel();
    defer setLevel(original_level);

    setLevel(.err);

    // This simulates what std.log would call
    logFn(.info, .test_scope, "table={s} count={d}", .{ "users", 42 });
}
