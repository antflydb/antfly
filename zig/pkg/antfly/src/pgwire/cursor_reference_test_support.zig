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

//! Shared byte-level assertions for mock and real-owner cursor campaigns.
const std = @import("std");
const protocol = @import("protocol.zig");
pub const Step = struct {
    sql: []const u8,
    result_format: ?u16,
    columns: []const []const u8,
    oids: []const u32,
    formats: []const u16,
    rows: []const []const ?[]const u8,
    tag: []const u8,
};
pub const Profile = struct { binary: bool, scroll: bool, hold: bool, steps: []const Step };
pub const Reference = struct { original_id: []const u8, original_sql: []const u8, profiles: []const Profile };
pub const bytes = @embedFile("fixtures/cursor_reference.json");
pub fn frame(out: *std.Io.Writer, tag: u8, body: []const u8) !void {
    try out.writeByte(tag);
    try out.writeInt(u32, @intCast(body.len + 4), .big);
    try out.writeAll(body);
}
pub fn startup(alloc: std.mem.Allocator, out: *std.Io.Writer, username: []const u8, password: []const u8, database: []const u8) !void {
    var body = std.Io.Writer.Allocating.init(alloc);
    defer body.deinit();
    try body.writer.print("user\x00{s}\x00database\x00{s}\x00\x00", .{ username, database });
    try out.writeInt(u32, @intCast(body.written().len + 8), .big);
    try out.writeInt(u32, 196608, .big);
    try out.writeAll(body.written());
    body.writer.end = 0;
    try body.writer.print("{s}\x00", .{password});
    try frame(out, 'p', body.written());
}
pub fn input(alloc: std.mem.Allocator, out: *std.Io.Writer, profile: Profile, username: []const u8, password: []const u8, database: []const u8) !void {
    try startup(alloc, out, username, password, database);
    var body = std.Io.Writer.Allocating.init(alloc);
    defer body.deinit();
    for (profile.steps) |step| {
        body.writer.end = 0;
        if (step.result_format) |format| {
            // Unnamed statement and portal are reclaimed by the next Bind or
            // Sync. These frames exercise the real protocol owner end to end.
            try body.writer.print("\x00{s}\x00", .{step.sql});
            try body.writer.writeInt(u16, 0, .big);
            try frame(out, 'P', body.written());
            body.writer.end = 0;
            try body.writer.writeAll("\x00\x00");
            try body.writer.writeInt(u16, 0, .big);
            try body.writer.writeInt(u16, 0, .big);
            try body.writer.writeInt(u16, 1, .big);
            try body.writer.writeInt(u16, format, .big);
            try frame(out, 'B', body.written());
            if (step.columns.len != 0) try frame(out, 'D', "P\x00");
            try frame(out, 'E', "\x00\x00\x00\x00\x00");
            try frame(out, 'S', "");
        } else {
            try body.writer.print("{s}\x00", .{step.sql});
            try frame(out, 'Q', body.written());
        }
    }
    try frame(out, 'X', "");
}
pub fn verify(alloc: std.mem.Allocator, raw: []const u8, profile: Profile) !void {
    var messages: protocol.Cursor = .{ .bytes = raw };
    var step_index: usize = 0;
    var row_index: usize = 0;
    var descriptions: usize = 0;
    while (messages.offset < messages.bytes.len) {
        const tag = try messages.int(u8);
        const size = try messages.int(u32);
        var body: protocol.Cursor = .{ .bytes = try messages.take(size - 4) };
        if (tag == 'E') {
            std.debug.print("cursor oracle step={d} error={s}\n", .{ step_index, body.bytes });
            return error.UnexpectedCursorProtocolError;
        }
        if (tag != 'T' and tag != 'D' and tag != 'C') continue;
        try std.testing.expect(step_index < profile.steps.len);
        const step = profile.steps[step_index];
        if (tag == 'T') {
            try std.testing.expectEqual(@as(u16, @intCast(step.columns.len)), try body.int(u16));
            for (step.columns, step.oids, step.formats) |name, oid, format| {
                try std.testing.expectEqualStrings(name, try body.string());
                _ = try body.int(u32);
                _ = try body.int(u16);
                try std.testing.expectEqual(oid, try body.int(u32));
                _ = try body.int(u16);
                _ = try body.int(u32);
                try std.testing.expectEqual(format, try body.int(u16));
            }
            descriptions += 1;
        } else if (tag == 'D') {
            try std.testing.expect(row_index < step.rows.len);
            const expected = step.rows[row_index];
            try std.testing.expectEqual(@as(u16, @intCast(expected.len)), try body.int(u16));
            for (expected) |hex| {
                const len = try body.int(i32);
                if (hex) |text| {
                    try std.testing.expect(len >= 0);
                    const decoded = try alloc.alloc(u8, text.len / 2);
                    defer alloc.free(decoded);
                    _ = try std.fmt.hexToBytes(decoded, text);
                    try std.testing.expectEqualSlices(u8, decoded, try body.take(@intCast(len)));
                } else try std.testing.expectEqual(@as(i32, -1), len);
            }
            row_index += 1;
        } else {
            try std.testing.expectEqualStrings(step.tag, try body.string());
            try std.testing.expectEqual(step.rows.len, row_index);
            try std.testing.expectEqual(@as(usize, @intFromBool(step.columns.len != 0)), descriptions);
            step_index += 1;
            row_index = 0;
            descriptions = 0;
        }
        try body.finish();
    }
    try std.testing.expectEqual(profile.steps.len, step_index);
}

/// This transcript is also executed independently against PostgreSQL by the
/// oracle's verify_suspended_fetch (libpq exposes no Execute row-count limit).
fn suspendedPrefix(alloc: std.mem.Allocator, out: *std.Io.Writer, scroll: bool, username: []const u8, password: []const u8, database: []const u8) !void {
    try startup(alloc, out, username, password, database);
    try frame(out, 'Q', "BEGIN ISOLATION LEVEL READ COMMITTED\x00");
    try frame(out, 'Q', if (scroll) "DECLARE rows BINARY SCROLL CURSOR FOR SELECT id FROM usage_records ORDER BY id\x00" else "DECLARE rows BINARY NO SCROLL CURSOR FOR SELECT id FROM usage_records ORDER BY id\x00");
    try frame(out, 'P', "fetch\x00FETCH FORWARD 4 FROM rows\x00\x00\x00");
    try frame(out, 'B', "portal\x00fetch\x00\x00\x00\x00\x00\x00\x01\x00\x01");
    try frame(out, 'E', "portal\x00\x00\x00\x00\x01");
    try frame(out, 'H', "");
}
pub fn suspendedInput(alloc: std.mem.Allocator, out: *std.Io.Writer, scroll: bool, username: []const u8, password: []const u8, database: []const u8) !void {
    try suspendedPrefix(alloc, out, scroll, username, password, database);
    try frame(out, 'Q', "FETCH NEXT FROM rows\x00");
    try frame(out, 'Q', "CLOSE rows\x00");
    try frame(out, 'Q', "DECLARE rows CURSOR FOR SELECT 99::bigint\x00");
    try frame(out, 'E', "portal\x00\x00\x00\x00\x00");
    try frame(out, 'E', "portal\x00\x00\x00\x00\x00");
    try frame(out, 'S', "");
    try frame(out, 'Q', "ROLLBACK\x00");
    try frame(out, 'X', "");
}
pub fn verifySuspended(raw: []const u8) !void {
    var messages: protocol.Cursor = .{ .bytes = raw };
    const expected = [_]i64{ 0, 4, 1, 2, 3 };
    const commands = [_][]const u8{ "BEGIN", "DECLARE CURSOR", "FETCH 1", "CLOSE CURSOR", "DECLARE CURSOR", "FETCH 3", "FETCH 0", "ROLLBACK" };
    var rows: usize = 0;
    var suspended: usize = 0;
    var completions: usize = 0;
    while (messages.offset < messages.bytes.len) {
        const tag = try messages.int(u8);
        const size = try messages.int(u32);
        var body: protocol.Cursor = .{ .bytes = try messages.take(size - 4) };
        if (tag == 'E') {
            std.debug.print("suspended cursor error={s}\n", .{body.bytes});
            return error.UnexpectedCursorProtocolError;
        }
        if (tag == 's') suspended += 1;
        if (tag == 'D') {
            try std.testing.expect(rows < expected.len);
            try std.testing.expectEqual(@as(u16, 1), try body.int(u16));
            try std.testing.expectEqual(@as(i32, 8), try body.int(i32));
            try std.testing.expectEqual(expected[rows], try body.int(i64));
            rows += 1;
            try body.finish();
        }
        if (tag == 'C') {
            try std.testing.expect(completions < commands.len);
            try std.testing.expectEqualStrings(commands[completions], try body.string());
            completions += 1;
            try body.finish();
        }
    }
    try std.testing.expectEqual(expected.len, rows);
    try std.testing.expectEqual(@as(usize, 1), suspended);
    try std.testing.expectEqual(commands.len, completions);
}

pub fn failedInput(alloc: std.mem.Allocator, out: *std.Io.Writer, scroll: bool, username: []const u8, password: []const u8, database: []const u8) !void {
    try suspendedPrefix(alloc, out, scroll, username, password, database);
    try frame(out, 'Q', "CLOSE missing_cursor\x00");
    try frame(out, 'E', "portal\x00\x00\x00\x00\x00");
    try frame(out, 'S', "");
    try frame(out, 'Q', "ROLLBACK\x00");
    try frame(out, 'X', "");
}
pub fn verifyFailed(raw: []const u8) !void {
    var messages: protocol.Cursor = .{ .bytes = raw };
    var rows: usize = 0;
    var errors: usize = 0;
    while (messages.offset < messages.bytes.len) {
        const tag = try messages.int(u8);
        const size = try messages.int(u32);
        var body: protocol.Cursor = .{ .bytes = try messages.take(size - 4) };
        if (tag == 'D') {
            try std.testing.expectEqual(@as(u16, 1), try body.int(u16));
            try std.testing.expectEqual(@as(i32, 8), try body.int(i32));
            try std.testing.expectEqual(@as(i64, 0), try body.int(i64));
            rows += 1;
        }
        if (tag == 'E') {
            const code = if (errors == 0) "C34000\x00" else "C25P02\x00";
            if (std.mem.indexOf(u8, body.bytes, code) == null) {
                std.debug.print("cursor failure expected={s} actual={s}\n", .{ code, body.bytes });
                return error.UnexpectedCursorSqlstate;
            }
            errors += 1;
        }
        if (tag == 'C') try std.testing.expect(!std.mem.startsWith(u8, body.bytes, "FETCH"));
    }
    try std.testing.expectEqual(@as(usize, 1), rows);
    try std.testing.expectEqual(@as(usize, 2), errors);
}
