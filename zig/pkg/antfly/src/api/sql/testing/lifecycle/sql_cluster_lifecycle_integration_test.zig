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

//! Exact PostgreSQL CLUSTER originals through the mounted native SQL owners.
//! Physical order, atomic rollback and row-version invariance are additionally
//! qualified against independent cold LSM owners by the DocStore heap gate.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("../owner_test_fixture.zig");
const client = @import("../owner_test_client.zig");
const Json = std.json.Value;
const Output = struct { columns: []const []const u8, column_oids: []const u32, rows: []const []const Json, sql_nulls: []const []const bool };
const Reference = struct {
    cases: []const struct {
        id: []const u8,
        sql: []const u8,
        setup: []const []const u8,
        query: []const u8,
        output: Output,
        command_tag: []const u8,
        verbose_notice: bool,
        after_writes: []const []const u8,
        changed_output: Output,
        preferred_recluster: []const u8,
    },
};
fn open(ctx: *fixture.Context) ![32]u8 {
    var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/connections", .POST, "{}");
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(struct { connection_id: []const u8 }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 32), parsed.value.connection_id.len);
    return parsed.value.connection_id[0..32].*;
}
fn close(ctx: *fixture.Context, connection: []const u8) !void {
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/sql/connections/{s}", .{connection});
    defer ctx.alloc.free(path);
    var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .DELETE, null);
    defer response.deinit(ctx.alloc);
    try std.testing.expect(response.status == 200 or response.status == 204);
}
fn ok(ctx: *fixture.Context, connection: []const u8, sql: []const u8) !void {
    var response = try client.executeConnected(ctx, null, connection, sql, .write_admission);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) std.debug.print("CLUSTER SQL={s} status={d} body={s}\n", .{ sql, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 200), response.status);
    if (std.mem.startsWith(u8, sql, "CREATE INDEX")) {
        var receipt = try std.json.parseFromSlice(struct { ddl_receipt: ?struct { index_publication_token: ?[]const u8 = null } = null }, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer receipt.deinit();
        if (receipt.value.ddl_receipt) |value| if (value.index_publication_token != null)
            try @import("sql_index_lifecycle_integration_test.zig").awaitIndexPublication(ctx, "public", sql, response);
    }
}
fn waitRouted(ctx: *fixture.Context, minimum: u32) !fixture.ServingSchema {
    const serving = try fixture.waitServingSchema(ctx, "usage_records", minimum);
    try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
    return serving;
}
fn waitStable(ctx: *fixture.Context, minimum: u32) !void {
    const serving = try waitRouted(ctx, minimum);
    const time = @import("antfly_platform").time;
    const deadline = time.monotonicNs() +| 15 * std.time.ns_per_s;
    while (time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var cut = try ctx.metadata.server.svc.adminSnapshot();
        defer ctx.metadata.server.svc.freeAdminSnapshot(&cut);
        for (cut.tables) |table| {
            if (table.table_id != serving.table_id) continue;
            if (table.read_schema_json.len == 0 and table.storage_migration == null) return;
        }
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.SqlCatalogPublicationTimeout;
}
fn rows(ctx: *fixture.Context, connection: []const u8, sql: []const u8, expected: Output) !void {
    var response = try client.executeConnected(ctx, null, connection, sql, .read);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    var parsed = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    const result = parsed.value;
    try std.testing.expectEqual(expected.columns.len, result.columns.len);
    for (expected.columns, expected.column_oids, result.columns) |name, oid, column| {
        try std.testing.expectEqualStrings(name, column.name);
        try std.testing.expect(@import("../parity_reference.zig").postgresColumnMatches(column, oid));
    }
    try std.testing.expectEqual(expected.rows.len, result.rows.len);
    const nulls = result.sql_nulls orelse return error.MissingSqlNullBitmap;
    try std.testing.expectEqual(result.rows.len, nulls.len);
    for (expected.rows, expected.sql_nulls, result.rows, nulls) |want, flags, actual, actual_flags| {
        try std.testing.expect(try @import("../parity_reference.zig").rowMatchesWithNulls(ctx.alloc, result.columns, actual, actual_flags, want, flags));
    }
}
fn rewrittenRows(ctx: *fixture.Context, minimum: u32) !void {
    const contract = local.api_sql_maintenance_contract;
    const serving = try waitRouted(ctx, minimum);
    var cut = try ctx.metadata.server.svc.adminSnapshot();
    defer ctx.metadata.server.svc.freeAdminSnapshot(&cut);
    const table = for (cut.tables) |table| {
        if (table.table_id == serving.table_id) break table;
    } else return error.TableNotFound;
    const writer = ctx.data.http_server.?.table_writes orelse return error.UnsupportedSqlExecution;
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, contract.Request{
        .table_id = serving.table_id,
        .schema_version = serving.version,
        .request_id = @splat(29),
        .operation = .cluster_check,
        .cluster_index = "usage_records_status_idx",
    }, .{});
    defer ctx.alloc.free(body);
    var total: u64 = 0;
    var owners: usize = 0;
    for (cut.ranges) |range| {
        if (range.table_id != serving.table_id) continue;
        const raw = (try writer.sqlMaintenanceGroup(ctx.alloc, range.group_id, table.name, body, .{})) orelse return error.UnsupportedSqlExecution;
        defer ctx.alloc.free(raw);
        var response = try std.json.parseFromSlice(contract.Response, ctx.alloc, raw, .{});
        defer response.deinit();
        try std.testing.expectEqual(serving.table_id, response.value.table_id);
        try std.testing.expectEqual(serving.version, response.value.schema_version);
        try std.testing.expect(response.value.done and response.value.available);
        try std.testing.expectEqual(@as(?u8, 1), response.value.row_heap_format);
        total += response.value.rows_scanned;
        owners += 1;
    }
    try std.testing.expect(owners > 0);
    try std.testing.expectEqual(@as(u64, 4), total);
}
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    case_index: usize = 0,
    credentials: client.ApiKeyHeaders = .{},
    pub fn configureAuth(manager: *@import("../../../../usermgr/mod.zig").UserManager) !void {
        const users = @import("../../../../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        const connection = try open(ctx);
        errdefer close(ctx, &connection) catch {};
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        var reference = try std.json.parseFromSlice(Reference, ctx.alloc, local.sql_parity_fixtures.cluster_reference, .{ .ignore_unknown_fields = true });
        defer reference.deinit();
        try std.testing.expectEqual(@as(usize, 2), reference.value.cases.len);
        try std.testing.expect(self.case_index < reference.value.cases.len);
        for (reference.value.cases, 0..) |step, ordinal| {
            if (ordinal != self.case_index) continue;
            try std.testing.expectEqualStrings((try corpus.get(step.id)).sql, step.sql);
            for (step.setup) |sql| {
                try ok(ctx, &connection, sql);
                if (std.mem.startsWith(u8, sql, "CREATE TABLE")) try waitStable(ctx, 0);
                if (std.mem.startsWith(u8, sql, "CREATE INDEX")) try waitStable(ctx, 1);
            }
            try rows(ctx, &connection, step.query, step.output);
            if (ordinal == 0) {
                var missing = try client.execute(ctx, step.sql, .never);
                defer missing.deinit(ctx.alloc);
                try std.testing.expectEqual(@as(u16, 400), missing.status);
                var diagnostic = try std.json.parseFromSlice(struct { code: []const u8, message: []const u8 }, ctx.alloc, missing.body, .{ .ignore_unknown_fields = true });
                defer diagnostic.deinit();
                try std.testing.expectEqualStrings("08003", diagnostic.value.code);
                try std.testing.expect(std.mem.indexOf(u8, diagnostic.value.message, "connection_id") != null);
            }
            var response = try client.executeConnected(ctx, null, &connection, step.sql, .write_admission);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("original={s} status={d} body={s}\n", .{ step.id, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var result = try std.json.parseFromSlice(@import("antfly_metadata_openapi").types.SQLResponse, ctx.alloc, response.body, .{});
            defer result.deinit();
            try std.testing.expectEqualStrings(step.command_tag, result.value.command_tag);
            const notices = result.value.notices orelse &.{};
            try std.testing.expectEqual(@as(usize, if (step.verbose_notice) 1 else 0), notices.len);
            if (step.verbose_notice) try std.testing.expect(std.mem.indexOf(u8, notices[0].message, "CLUSTER completed") != null);
            try rewrittenRows(ctx, 1);
            try rows(ctx, &connection, step.query, step.output);
            var reader = ctx.*;
            reader.headers = ctx.reader_headers;
            var denied = try client.execute(&reader, step.sql, .never);
            defer denied.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 403), denied.status);
            for (step.after_writes) |sql| try ok(ctx, &connection, sql);
            try rows(ctx, &connection, step.query, step.changed_output);
            try ok(ctx, &connection, step.preferred_recluster);
            try rewrittenRows(ctx, 1);
            try rows(ctx, &connection, step.query, step.changed_output);
        }
        try ok(ctx, &connection, "ALTER TABLE usage_records ADD COLUMN cluster_note TEXT");
        try waitStable(ctx, 2);
        const last = reference.value.cases[self.case_index];
        try rows(ctx, &connection, last.query, last.changed_output);
        try ok(ctx, &connection, last.preferred_recluster);
        try rewrittenRows(ctx, 2);
        try close(ctx, &connection);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        const connection = try open(ctx);
        errdefer close(ctx, &connection) catch {};
        try waitStable(ctx, 2);
        var reference = try std.json.parseFromSlice(Reference, ctx.alloc, local.sql_parity_fixtures.cluster_reference, .{ .ignore_unknown_fields = true });
        defer reference.deinit();
        const last = reference.value.cases[self.case_index];
        try rows(ctx, &connection, last.query, last.changed_output);
        try ok(ctx, &connection, last.preferred_recluster);
        try rewrittenRows(ctx, 2);
        try rows(ctx, &connection, last.query, last.changed_output);
        try ok(ctx, &connection, "UPDATE usage_records SET body='after restart' WHERE id='00000000-0000-0000-0000-000000000001'");
        try rows(ctx, &connection, "SELECT body FROM usage_records WHERE id='00000000-0000-0000-0000-000000000001'", .{
            .columns = &.{"body"},
            .column_oids = &.{25},
            .rows = &.{&.{.{ .string = "after restart" }}},
            .sql_nulls = &.{&.{false}},
        });
        try close(ctx, &connection);
    }
};
test "mounted SQL original CLUSTER preserves logical rows future writes preferred indexes authorization and cold owners" {
    for (0..2) |ordinal| try fixture.runWithState(Campaign, .{ .case_index = ordinal });
}
