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

//! Original multi-table reads through authenticated native owners and restart.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const compare = @import("sql_parity_reference.zig");
const wire = @import("antfly_metadata_openapi").types;
const Json = std.json.Value;
const Row = struct { key: []const u8, value: Json };
const Golden = struct {
    profile: struct { schema: Json, rows: []const Row, additional_tables: []const struct { name: []const u8, schema: Json, rows: []const Row } },
    entries: []const struct {
        id: []const u8,
        columns: []const []const u8,
        column_oids: []const u32,
        rows: []const []const Json,
        ordered_groups: []const compare.OrderedGroup,
    },
};
fn query(ctx: *fixture.Context, statement: []const u8, admin: bool, parameters: []const Json) !@import("../raft/transport/http_common.zig").HttpResponse {
    var authenticated = ctx.*;
    authenticated.headers = if (admin) ctx.admin_headers else ctx.reader_headers;
    authenticated.base = ctx.metadata_uri;
    return client.executeParameters(&authenticated, statement, parameters, .read);
}
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        for ([_][]const u8{ "pk-rewriter", "pk-reader" }) |username| {
            inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
                var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
                defer permission.deinit(manager.alloc);
                try manager.addPermissionToUser(username, permission);
            }
        }
    }
    fn seed(ctx: *fixture.Context, name: []const u8, input_schema: Json, rows: []const Row) !void {
        var schema = input_schema;
        _ = schema.object.orderedRemove("version");
        const body = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .schema = schema }, .{});
        defer ctx.alloc.free(body);
        const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/tables/{s}", .{name});
        defer ctx.alloc.free(path);
        var created = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .POST, body);
        defer created.deinit(ctx.alloc);
        if (created.status != 201 and created.status != 200) std.debug.print("join CREATE {s}: {d} {s}\n", .{ name, created.status, created.body });
        try std.testing.expect(created.status == 201 or created.status == 200);
        _ = try fixture.waitServingSchema(ctx, name, 0);
        var inserts: std.json.ObjectMap = .empty;
        defer inserts.deinit(ctx.alloc);
        for (rows) |row| try inserts.put(ctx.alloc, row.key, row.value);
        const batch = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .inserts = Json{ .object = inserts }, .sync_level = "write" }, .{});
        defer ctx.alloc.free(batch);
        var inserted = try fixture.batch(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, name, batch);
        defer inserted.deinit(ctx.alloc);
        if (inserted.status != 201) std.debug.print("join seed {s}: {d} {s}\n", .{ name, inserted.status, inserted.body });
        try std.testing.expectEqual(@as(u16, 201), inserted.status);
    }
    fn qualify(ctx: *fixture.Context) !void {
        var golden = try std.json.parseFromSlice(Golden, ctx.alloc, local.sql_parity_fixtures.join_read_reference, .{ .ignore_unknown_fields = true });
        defer golden.deinit();
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        try std.testing.expectEqual(@as(usize, 23), golden.value.entries.len);
        for (golden.value.entries) |expected| {
            const original = try corpus.get(expected.id);
            var parameter_arena = std.heap.ArenaAllocator.init(ctx.alloc);
            defer parameter_arena.deinit();
            const parameters = try local.sql_parity_fixtures.Corpus.logicalParameters(parameter_arena.allocator(), original);
            var response = try query(ctx, original.sql, true, parameters);
            defer response.deinit(ctx.alloc);
            if (response.status != 200) std.debug.print("join {s}: {d} {s}\n", .{ expected.id, response.status, response.body });
            try std.testing.expectEqual(@as(u16, 200), response.status);
            var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
            defer parsed.deinit();
            const actual = parsed.value;
            try std.testing.expectEqualStrings("SELECT", actual.command_tag);
            try std.testing.expectEqual(@as(i64, 0), actual.rows_affected);
            try std.testing.expectEqual(expected.rows.len, actual.rows.len);
            try std.testing.expectEqual(expected.columns.len, actual.columns.len);
            for (actual.columns, expected.columns, expected.column_oids) |column, name, oid| {
                try std.testing.expectEqualStrings(name, column.name);
                try std.testing.expect(compare.postgresColumnMatches(column, oid));
            }
            if (!try compare.orderedPrefixMatches(ctx.alloc, actual, expected.ordered_groups)) {
                std.debug.print("join PostgreSQL peer frontier differs id={s} body={s}\n", .{ expected.id, response.body });
                return error.JoinReferenceMismatch;
            }
        }
        try qualifyPolicies(ctx);
    }
    fn qualifyPolicies(ctx: *fixture.Context) !void {
        const api = ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi;
        const manager = api.cfg.user_manager orelse return error.MissingUserManager;
        // Both leaves retain mandatory native filters even when a null-rejecting
        // WHERE turns the LEFT join into an inner join. One matching tenant is
        // deliberately present on both sides of the cross-tenant fixture.
        for ([_][]const u8{ "usage_records", "customer_records" }) |name|
            try manager.setRowFilter("pk-reader", name, "{\"term\":{\"tenant_id\":\"t1\"}}");
        defer {
            manager.removeRowFilter("pk-reader", "usage_records") catch {};
            manager.removeRowFilter("pk-reader", "customer_records") catch {};
        }
        var response = try query(ctx, "SELECT DISTINCT o.tenant_id AS owner_tenant, c.tenant_id AS customer_tenant FROM usage_records o LEFT JOIN customer_records c ON o.tenant_id=c.tenant_id AND o.customer_id=c.id WHERE c.kind='customer'", false, &.{});
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("join filtered read: {d} {s}\n", .{ response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(usize, 1), parsed.value.rows.len);
        for (parsed.value.rows[0]) |cell| try std.testing.expectEqualStrings("t1", cell.string);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        var golden = try std.json.parseFromSlice(Golden, ctx.alloc, local.sql_parity_fixtures.join_read_reference, .{ .ignore_unknown_fields = true });
        defer golden.deinit();
        try seed(ctx, "usage_records", golden.value.profile.schema, golden.value.profile.rows);
        for (golden.value.profile.additional_tables) |table| try seed(ctx, table.name, table.schema, table.rows);
        try qualify(ctx);
    }
    pub fn afterRestart(_: *@This(), ctx: *fixture.Context) !void {
        _ = try fixture.waitServingSchema(ctx, "usage_records", 0);
        _ = try fixture.waitServingSchema(ctx, "customer_records", 0);
        try qualify(ctx);
    }
};

test "mounted SQL original joins preserve catalog cuts policies null padding and peer pagination across restart" {
    try fixture.run(Campaign);
}
