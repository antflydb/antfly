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

//! Native aggregate execution, bounded states and cold dependency ownership.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const reference_support = @import("sql_callable_lifecycle_reference.zig");
const Reference = reference_support.Reference;
const Step = reference_support.Step;
const observe = reference_support.observe;
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
        var usage = try users.Permission.initOwned(manager.alloc, .namespace, "*", .read);
        defer usage.deinit(manager.alloc);
        try manager.addPermissionToUser("pk-reader", usage);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        for (self.reference.before) |step| {
            try observe(ctx, step, false);
            if (std.mem.startsWith(u8, step.sql, "CREATE TABLE aggregate_rows")) {
                const serving = try fixture.waitServingSchema(ctx, "aggregate_rows", 0);
                try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
                try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "aggregate_rows");
            }
        }
        try preparedOverloads(ctx, self.reference.prepared orelse return error.MissingAggregatePreparedWitness);
        try @import("sql_routine_lifecycle_integration_test.zig").lostAggregateAck(ctx);
        for (self.reference.cold) |step| try observe(ctx, step, true);
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        var denied = try client.execute(&reader, "DROP AGGREGATE first_value_text(text)", .write_admission);
        defer denied.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 403), denied.status);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        for (self.reference.cold) |step| try observe(ctx, step, false);
        for (self.reference.after) |step| try observe(ctx, step, false);
    }
};
test "mounted SQL original aggregates preserve typed transitions bounded states and cold dependency retirement" {
    const a = std.testing.allocator;
    var reference = try std.json.parseFromSlice(Reference, a, local.sql_parity_fixtures.aggregate_lifecycle_reference, .{ .ignore_unknown_fields = true });
    defer reference.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const originals = [_][]const u8{ "sql-0136", "sql-0137", "sql-0138" };
    var checked: usize = 0;
    for ([_][]const Step{ reference.value.before, reference.value.after }) |steps| for (steps) |step| if (step.id) |id| {
        try std.testing.expectEqualStrings(originals[checked], id);
        try std.testing.expectEqualStrings((try corpus.get(id)).sql, step.sql);
        checked += 1;
    };
    try std.testing.expectEqual(originals.len, checked);
    try fixture.runWithState(Campaign, .{ .reference = reference.value });
}

fn preparedCreate(ctx: *fixture.Context, statement: []const u8) ![]u8 {
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .statement = statement }, .{});
    defer ctx.alloc.free(body);
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/prepared", .POST, body, 15_000);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("aggregate prepare status={d} body={s}\n", .{ response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var value = try std.json.parseFromSlice(struct { prepared_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer value.deinit();
    return ctx.alloc.dupe(u8, value.value.prepared_id);
}
fn closePrepared(ctx: *fixture.Context, id: []const u8) !void {
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/prepared/{s}", .{id});
    defer ctx.alloc.free(path);
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .DELETE, null, 15_000);
    defer response.deinit(ctx.alloc);
    try std.testing.expect(response.status == 200 or response.status == 204);
}
fn preparedResult(ctx: *fixture.Context, id: []const u8, step: Step) !void {
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/prepared/{s}/execute", .{id});
    defer ctx.alloc.free(path);
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .POST, "{}", 15_000);
    defer response.deinit(ctx.alloc);
    try reference_support.compareResponse(ctx, step, response);
}
fn preparedOverloads(ctx: *fixture.Context, reference: reference_support.Prepared) !void {
    for (reference.setup) |step| {
        try observe(ctx, step, false);
        if (std.mem.startsWith(u8, step.sql, "CREATE TABLE ")) {
            const serving = try fixture.waitServingSchema(ctx, "prepared_aggregate_rows", 0);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
            try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "prepared_aggregate_rows");
        }
    }
    const initial = try preparedCreate(ctx, reference.statement);
    defer ctx.alloc.free(initial);
    defer closePrepared(ctx, initial) catch {};
    try preparedResult(ctx, initial, .{ .sql = reference.statement, .output = reference.before, .notices = &.{} });
    try observe(ctx, reference.mutation, false);
    // HTTP prepared resources retain immutable dependency manifests. A changed
    // support generation requires a new resource, never an implicit retarget.
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/prepared/{s}/execute", .{initial});
    defer ctx.alloc.free(path);
    var stale = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .POST, "{}", 15_000);
    defer stale.deinit(ctx.alloc);
    if (stale.status == 200) std.debug.print("stale aggregate prepared unexpectedly executed body={s}\n", .{stale.body});
    try std.testing.expect(stale.status != 200);
    var diagnostic = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLDiagnostic, ctx.alloc, stale.body, .{});
    defer diagnostic.deinit();
    try std.testing.expectEqualStrings("40001", diagnostic.value.code);
    const fresh = try preparedCreate(ctx, reference.statement);
    defer ctx.alloc.free(fresh);
    defer closePrepared(ctx, fresh) catch {};
    try preparedResult(ctx, fresh, .{ .sql = reference.statement, .output = reference.after, .notices = &.{} });
    try closePrepared(ctx, fresh);
    try closePrepared(ctx, initial);
}
