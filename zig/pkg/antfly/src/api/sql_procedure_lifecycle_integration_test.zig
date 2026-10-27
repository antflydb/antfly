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

//! Original procedure contracts on real HTTP, Raft and cold catalog owners.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;
const Step = struct { id: ?[]const u8 = null, sql: []const u8, command_tag: ?[]const u8 = null, sqlstate: ?[]const u8 = null, notices: []const struct { code: []const u8, message: []const u8 } };
const Reference = struct { entries: []const Step, calls: []const Step, cold: []const Step, teardown: []const Step };
fn observe(ctx: *fixture.Context, step: Step) !void {
    var region: std.heap.ArenaAllocator = .init(ctx.alloc);
    defer region.deinit();
    const a = region.allocator();
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    var response = try client.execute(&authenticated, step.sql, .write_admission);
    defer response.deinit(ctx.alloc);
    const expected_status: u16 = if (step.sqlstate != null) 400 else 200;
    if (response.status != expected_status) std.debug.print("procedure status={d} sql={s} body={s}\n", .{ response.status, step.sql, response.body });
    try std.testing.expectEqual(expected_status, response.status);
    const notices: []const wire.SQLNotice = if (step.sqlstate) |code| diagnostic: {
        const result = try std.json.parseFromSliceLeaky(wire.SQLDiagnostic, a, response.body, .{});
        try std.testing.expectEqualStrings(code, result.code);
        break :diagnostic result.notices orelse &.{};
    } else success: {
        const result = try std.json.parseFromSliceLeaky(wire.SQLResponse, a, response.body, .{});
        try std.testing.expectEqualStrings(step.command_tag.?, result.command_tag);
        try std.testing.expectEqual(@as(usize, 0), result.columns.len);
        try std.testing.expectEqual(@as(usize, 0), result.rows.len);
        break :success result.notices orelse &.{};
    };
    try std.testing.expectEqual(step.notices.len, notices.len);
    for (step.notices, notices) |expected, actual| {
        try std.testing.expectEqualStrings(expected.code, actual.code);
        try std.testing.expectEqualStrings(expected.message, actual.message);
    }
}
fn transactions(ctx: *fixture.Context) !void {
    var authenticated = ctx.*;
    authenticated.headers = ctx.admin_headers;
    const steps = [_]struct { sql: []const u8, error_code: ?[]const u8 = null }{
        .{ .sql = "BEGIN READ ONLY" },
        .{ .sql = "CALL rotate_usage()" },
        .{ .sql = "COMMIT" },
        .{ .sql = "BEGIN" },
        .{ .sql = "SAVEPOINT before_call" },
        .{ .sql = "CALL notice_failure()", .error_code = "22012" },
        .{ .sql = "ROLLBACK TO SAVEPOINT before_call" },
        .{ .sql = "CALL rotate_usage()" },
        .{ .sql = "COMMIT" },
    };
    var session: ?[32]u8 = null;
    for (steps) |step| {
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = step.sql, .session_id = if (session) |*id| @as([]const u8, id) else null }, .{ .emit_null_optional_fields = false });
        defer ctx.alloc.free(body);
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, authenticated.headers, ctx.base, "/db/v1/sql", .POST, body, 15000);
        defer response.deinit(ctx.alloc);
        if (response.status != (if (step.error_code != null) @as(u16, 400) else @as(u16, 200))) std.debug.print("procedure session sql={s} status={d} body={s}\n", .{ step.sql, response.status, response.body });
        if (step.error_code) |code| {
            try std.testing.expectEqual(@as(u16, 400), response.status);
            var result = try std.json.parseFromSlice(wire.SQLDiagnostic, ctx.alloc, response.body, .{});
            defer result.deinit();
            try std.testing.expectEqualStrings(code, result.value.code);
            try std.testing.expectEqual(.failed, result.value.transaction_status.?);
            try std.testing.expectEqual(@as(usize, 1), result.value.notices.?.len);
        } else {
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            if (std.mem.eql(u8, step.sql, "COMMIT")) {
                try std.testing.expectEqual(.idle, result.value.transaction_status.?);
                session = null;
            } else {
                try std.testing.expectEqual(.in_transaction, result.value.transaction_status.?);
                if (result.value.session_id) |id| {
                    try std.testing.expectEqual(@as(usize, 32), id.len);
                    session = id[0..32].*;
                }
                try std.testing.expect(session != null);
            }
        }
    }
}

const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
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
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        for (self.reference.entries) |step| try observe(ctx, step);
        for (self.reference.calls) |step| try observe(ctx, step);
        try transactions(ctx);
        try @import("sql_routine_lifecycle_integration_test.zig").lostAck(ctx, true);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (self.reference.cold) |step| try observe(ctx, step);
        for (self.reference.teardown) |step| try observe(ctx, step);
    }
};
test "mounted SQL original procedures preserve demanded calls notices errors and cold ownership" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.procedure_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const originals = [_][]const u8{ "sql-0414", "sql-0415", "sql-0416", "sql-0418", "sql-0417", "sql-0419", "sql-0420" };
    var checked: usize = 0;
    for ([_][]const Step{ reference.value.entries, reference.value.teardown }) |steps| for (steps) |step| if (step.id) |id| {
        try std.testing.expectEqualStrings(originals[checked], id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, step.sql);
        checked += 1;
    };
    try std.testing.expectEqual(originals.len, checked);
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}
