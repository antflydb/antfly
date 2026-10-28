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

//! Exact PostgreSQL callable campaign response comparisons.
const std = @import("std");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const compare = @import("sql_parity_reference.zig");
const wire = @import("antfly_metadata_openapi").types;
pub const Output = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const std.json.Value, sql_nulls: []const []const bool };
pub const Step = struct { id: ?[]const u8 = null, sql: []const u8, command_tag: ?[]const u8 = null, sqlstate: ?[]const u8 = null, output: ?Output = null, notices: []const struct { code: []const u8, message: []const u8 } };
pub const Prepared = struct { setup: []const Step, statement: []const u8, before: Output, mutation: Step, after: Output };
pub const Reference = struct { before: []const Step, cold: []const Step, after: []const Step, prepared: ?Prepared = null };
pub fn observe(ctx: *fixture.Context, step: Step, reader: bool) !void {
    var authenticated = ctx.*;
    authenticated.headers = if (reader) ctx.reader_headers else ctx.admin_headers;
    var response = try client.execute(&authenticated, step.sql, .write_admission);
    defer response.deinit(ctx.alloc);
    return compareResponse(ctx, step, response);
}
pub fn compareResponse(ctx: *fixture.Context, step: Step, response: anytype) !void {
    if (response.status != (if (step.sqlstate != null) @as(u16, 400) else @as(u16, 200))) std.debug.print("callable status={d} sql={s} body={s}\n", .{ response.status, step.sql, response.body });
    const notices: []const wire.SQLNotice = if (step.sqlstate) |code| rejected: {
        try std.testing.expectEqual(@as(u16, 400), response.status);
        var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings(code, diagnostic.value.code);
        // Compare while the parsed diagnostic owns its strings.
        const actual = diagnostic.value.notices orelse &.{};
        try std.testing.expectEqual(step.notices.len, actual.len);
        for (step.notices, actual) |expected, notice| {
            try std.testing.expectEqualStrings(expected.code, notice.code);
            try std.testing.expectEqualStrings(expected.message, notice.message);
        }
        break :rejected &.{};
    } else accepted: {
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer result.deinit();
        if (step.output) |expected| {
            try std.testing.expectEqual(expected.columns.len, result.value.columns.len);
            for (expected.columns, expected.column_oids, result.value.columns) |name, oid, column| {
                try std.testing.expectEqualStrings(name, column.name);
                try std.testing.expect(compare.postgresColumnMatches(column, oid));
            }
            try std.testing.expectEqual(expected.rows.len, result.value.rows.len);
            const nulls = result.value.sql_nulls orelse return error.MissingSqlNullBitmap;
            if (std.mem.startsWith(u8, step.sql, "UPDATE")) {
                const matched = try ctx.alloc.alloc(bool, result.value.rows.len);
                defer ctx.alloc.free(matched);
                @memset(matched, false);
                for (expected.rows, expected.sql_nulls) |want, flags| {
                    const selected = for (result.value.rows, nulls, 0..) |actual, actual_flags, ordinal| {
                        if (!matched[ordinal] and try compare.rowMatchesWithNulls(ctx.alloc, result.value.columns, actual, actual_flags, want, flags)) break ordinal;
                    } else return error.OperatorReturningMultisetMismatch;
                    matched[selected] = true;
                }
            } else for (expected.rows, expected.sql_nulls, result.value.rows, nulls) |want, flags, actual, actual_flags| {
                if (!try compare.rowMatchesWithNulls(ctx.alloc, result.value.columns, actual, actual_flags, want, flags)) {
                    std.debug.print("callable postimage sql={s} expected={f} body={s}\n", .{ step.sql, std.json.fmt(want, .{}), response.body });
                    return error.OperatorPostimageMismatch;
                }
            }
        } else if (std.mem.startsWith(u8, step.command_tag.?, "INSERT")) {
            try std.testing.expectEqual(@as(i64, 3), result.value.rows_affected);
        } else try std.testing.expectEqualStrings(step.command_tag.?, result.value.command_tag);
        const actual = result.value.notices orelse &.{};
        try std.testing.expectEqual(step.notices.len, actual.len);
        for (step.notices, actual) |expected, notice| {
            try std.testing.expectEqualStrings(expected.code, notice.code);
            try std.testing.expectEqualStrings(expected.message, notice.message);
        }
        break :accepted &.{};
    };
    _ = notices;
}
