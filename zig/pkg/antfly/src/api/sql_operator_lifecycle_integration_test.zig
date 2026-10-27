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

//! Native operator catalog, typed row execution and cold dependency ownership.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const compare = @import("sql_parity_reference.zig");
const wire = @import("antfly_metadata_openapi").types;
const Output = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const std.json.Value, sql_nulls: []const []const bool };
const Step = struct { id: ?[]const u8 = null, sql: []const u8, command_tag: ?[]const u8 = null, sqlstate: ?[]const u8 = null, output: ?Output = null, notices: []const struct { code: []const u8, message: []const u8 } };
const Reference = struct { before: []const Step, cold: []const Step, after: []const Step };
fn observe(ctx: *fixture.Context, step: Step, reader: bool) !void {
    var authenticated = ctx.*;
    authenticated.headers = if (reader) ctx.reader_headers else ctx.admin_headers;
    var response = try client.execute(&authenticated, step.sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != (if (step.sqlstate != null) @as(u16, 400) else @as(u16, 200))) std.debug.print("operator status={d} sql={s} body={s}\n", .{ response.status, step.sql, response.body });
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
                    std.debug.print("operator postimage sql={s} expected={f} body={s}\n", .{ step.sql, std.json.fmt(want, .{}), response.body });
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
const Campaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    reference: Reference,
    credentials: client.ApiKeyHeaders = .{},
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
        var usage = try users.Permission.initOwned(manager.alloc, .namespace, "*", .read);
        defer usage.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-reader", usage);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        for (self.reference.before) |step| {
            try observe(ctx, step, false);
            if (std.mem.startsWith(u8, step.sql, "CREATE TABLE operator_rows")) {
                const serving = try fixture.waitServingSchema(ctx, "operator_rows", 0);
                try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
                try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "operator_rows");
            }
        }
        for (self.reference.cold) |step| try observe(ctx, step, true);
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        var denied = try client.execute(&reader, "DROP OPERATOR ===(text,text)", .write_admission);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (self.reference.cold) |step| try observe(ctx, step, false);
        for (self.reference.after) |step| try observe(ctx, step, false);
    }
};
test "mounted SQL original operators preserve typed overloads stable function identities and cold dependency retirement" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.operator_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const originals = [_][]const u8{ "sql-0133", "sql-0134", "sql-0135" };
    var checked: usize = 0;
    for ([_][]const Step{ reference.value.before, reference.value.after }) |steps| for (steps) |step| if (step.id) |id| {
        try std.testing.expectEqualStrings(originals[checked], id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, step.sql);
        checked += 1;
    };
    try std.testing.expectEqual(originals.len, checked);
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}
