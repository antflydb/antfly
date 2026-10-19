// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Original view statements through authenticated SQL, catalog Raft and the
//! remote data owner. No fixture-created view records or fabricated table IDs.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const wire = @import("antfly_metadata_openapi").types;

fn execute(ctx: *fixture.Context, statement: []const u8, reader: bool) !@import("../raft/transport/http_common.zig").HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = if (reader) ctx.reader_headers else ctx.admin_headers;
    authenticated.base = ctx.metadata_uri;
    return client.execute(&authenticated, statement, .read) catch |err| {
        std.debug.print("view SQL transport failure err={s} sql={s}\n", .{ @errorName(err), statement });
        return err;
    };
}

fn command(ctx: *fixture.Context, statement: []const u8) !void {
    var response = try execute(ctx, statement, false);
    defer response.deinit(ctx.alloc);
    if (response.status != 200 and response.status != 202) {
        std.debug.print("view command status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
        return error.SqlViewCommandFailed;
    }
}

fn rejected(ctx: *fixture.Context, statement: []const u8, code: []const u8) !void {
    var response = try execute(ctx, statement, false);
    defer response.deinit(ctx.alloc);
    if (response.status < 400) return error.SqlViewExpectedRejection;
    var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqualStrings(code, (try fixture.field(parsed.value, "code")).string);
}

fn rows(ctx: *fixture.Context, statement: []const u8, column: []const u8, expected: []const i64) !void {
    var response = try execute(ctx, statement, true);
    defer response.deinit(ctx.alloc);
    if (response.status != 200) {
        std.debug.print("view query status={d} sql={s} body={s}\n", .{ response.status, statement, response.body });
        return error.SqlViewQueryFailed;
    }
    var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.columns.len);
    try std.testing.expectEqualStrings(column, parsed.value.columns[0].name);
    try std.testing.expectEqual(expected.len, parsed.value.rows.len);
    for (parsed.value.rows, expected) |row, value| {
        try std.testing.expectEqual(@as(usize, 1), row.len);
        try std.testing.expectEqual(value, try fixture.sqlInteger(row[0]));
    }
}

const Campaign = struct {
    credentials: client.ApiKeyHeaders = .{},

    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        {
            var created = try client.execute(ctx, "CREATE TABLE usage_records(id BIGINT,status TEXT)", .write_admission);
            defer created.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), created.status);
            var observed = try fixture.table(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "usage_records");
            defer observed.deinit(ctx.alloc);
            try std.testing.expectEqual(@as(u16, 200), observed.status);
            var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, observed.body, .{});
            defer parsed.deinit();
            const id = try std.fmt.parseUnsigned(u64, (try fixture.field(parsed.value, "table_id")).string, 10);
            try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, id);
        }
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        try command(ctx, "INSERT INTO usage_records(_id,id,status) VALUES('one',1,'pk-rewriter'),('two',2,'pk-reader')");
        try rows(ctx, "SELECT id FROM usage_records ORDER BY id", "id", &.{ 1, 2 });
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        for ([_][]const u8{ "sql-0425", "sql-0426", "sql-0427" }) |id| {
            try command(ctx, (try corpus.get(id)).sql);
            const column = if (std.mem.eql(u8, id, "sql-0425")) "id" else "record_id";
            const query = try std.fmt.allocPrint(ctx.alloc, "SELECT {s} FROM usage_records_v ORDER BY {s}", .{ column, column });
            defer ctx.alloc.free(query);
            try rows(ctx, query, column, &.{ 1, 2 });
            try command(ctx, "DROP VIEW usage_records_v");
        }
        try command(ctx, (try corpus.get("sql-0425")).sql);
        try command(ctx, (try corpus.get("sql-0428")).sql);
        try rejected(ctx, (try corpus.get("sql-0429")).sql, "42601");
        try command(ctx, "CREATE VIEW dependent_view AS SELECT id FROM usage_records_v");
        try command(ctx, (try corpus.get("sql-0430")).sql);
        // A dependent view is bound to durable identity, not its old spelling.
        try rows(ctx, "SELECT id FROM dependent_view ORDER BY id", "id", &.{ 1, 2 });
        try rejected(ctx, "DROP VIEW usage_records_visible_v", "2BP01");
        try command(ctx, "ALTER VIEW usage_records_visible_v RENAME TO usage_records_v");
        try command(ctx, (try corpus.get("sql-0432")).sql);
        try rejected(ctx, "SELECT id FROM dependent_view", "42P01");
        try command(ctx, (try corpus.get("sql-0432")).sql);
        try command(ctx, (try corpus.get("sql-0425")).sql);
        try command(ctx, (try corpus.get("sql-0431")).sql);
        try command(ctx, "CREATE VIEW durable_view AS SELECT id FROM usage_records WHERE id = 2");
        try rows(ctx, "SELECT id FROM durable_view", "id", &.{2});
        // Replacing the source name cannot retarget a stored dependency.
        try command(ctx, "CREATE VIEW nested_view AS SELECT id FROM durable_view");
        try command(ctx, "ALTER VIEW durable_view RENAME TO renamed_view");
        try command(ctx, "CREATE VIEW durable_view AS SELECT 99 AS id");
        try rows(ctx, "SELECT id FROM nested_view", "id", &.{2});
        try @import("sql_view_security_campaign.zig").beforeRestart(ctx);
    }

    pub fn afterRestart(_: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try rows(ctx, "SELECT id FROM nested_view", "id", &.{2});
        try rows(ctx, "SELECT id FROM renamed_view", "id", &.{2});
        try rows(ctx, "SELECT id FROM durable_view", "id", &.{99});
        try @import("sql_view_security_campaign.zig").afterRestart(ctx);
        try command(ctx, "DROP VIEW renamed_view CASCADE");
        try rejected(ctx, "SELECT id FROM nested_view", "42P01");
        try rows(ctx, "SELECT id FROM durable_view", "id", &.{99});
    }
};

test "mounted SQL original views preserve identity dependencies and postimages across owner restart" {
    var corpus = try local.sql_parity_fixtures.Corpus.init(std.testing.allocator);
    defer corpus.deinit();
    for ([_][]const u8{ "sql-0425", "sql-0426", "sql-0427", "sql-0428", "sql-0429", "sql-0430", "sql-0431", "sql-0432" }) |id|
        _ = try corpus.get(id);
    try fixture.run(Campaign);
}
