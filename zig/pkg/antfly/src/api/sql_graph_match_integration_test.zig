// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! SQL MATCH runs the canonical native graph engine, not a SQL-only model.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const Wire = @import("antfly_metadata_openapi").types;
fn identityFilter(a: std.mem.Allocator, ids: []const []const u8) ![]u8 {
    const bytes = try std.json.Stringify.valueAlloc(a, .{ .doc_id = @import("antfly_query_openapi").DocIdQuery{ .ids = ids } }, .{});
    errdefer a.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    try local.storage_db_control_root.validateStructuredFilterValueAlloc(a, parsed.value);
    return bytes;
}

const Campaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},

    pub fn configureAuth(manager: *@import("../usermgr/mod.zig").UserManager) !void {
        const users = @import("../usermgr/mod.zig");
        inline for (.{ users.ResourceType.database, users.ResourceType.namespace }) |kind| {
            var permission = try users.Permission.initOwned(manager.alloc, kind, "*", .admin);
            defer permission.deinit(manager.alloc);
            try manager.addPermissionToUser("pk-rewriter", permission);
        }
    }
    fn command(ctx: *fixture.Context, sql: []const u8) !void {
        var response = try client.execute(ctx, sql, .write_admission);
        defer response.deinit(ctx.alloc);
        if (response.status != 200 and response.status != 202) std.debug.print("graph setup status={d} body={s}\n", .{ response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202);
    }
    fn observe(ctx: *fixture.Context, sql: []const u8, targets: []const []const u8, ordered: bool) !void {
        var response = try client.execute(ctx, sql, .read);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("SQL graph status={d} statement={s} body={s}\n", .{ response.status, sql, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(Wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(usize, 2), parsed.value.columns.len);
        try std.testing.expectEqualStrings("source_id", parsed.value.columns[0].name);
        try std.testing.expectEqualStrings("target_id", parsed.value.columns[1].name);
        try std.testing.expectEqual(targets.len, parsed.value.rows.len);
        const seen = try ctx.alloc.alloc(bool, targets.len);
        defer ctx.alloc.free(seen);
        @memset(seen, false);
        for (parsed.value.rows, 0..) |row, i| {
            try std.testing.expectEqual(@as(usize, 2), row.len);
            try std.testing.expectEqualStrings("doc:root", row[0].string);
            if (ordered) try std.testing.expectEqualStrings(targets[i], row[1].string) else {
                const position = for (targets, 0..) |target, position| {
                    if (std.mem.eql(u8, target, row[1].string)) break position;
                } else return error.UnexpectedGraphBinding;
                try std.testing.expect(!seen[position]);
                seen[position] = true;
            }
        }
    }
    fn check(ctx: *fixture.Context) !void {
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        // The original statement executes byte-for-byte, including its
        // unprojected native depth ordering and outer SQL limit.
        try observe(ctx, (try corpus.get("sql-1209")).sql, &.{ "doc:a", "doc:b", "doc:c" }, false);
        try observe(ctx, "MATCH (doc)-[:cites]->(target) WITH GRAPH docs_edge_graph ON usage_records START 'doc:root' RETURN doc.key AS source_id,target.key AS target_id ORDER BY target.key DESC LIMIT 2", &.{ "doc:c", "doc:b" }, true);
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        const manager = (ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi).cfg.user_manager orelse return error.MissingUserManager;
        const visible = try identityFilter(ctx.alloc, &.{ "doc:root", "doc:a", "doc:b" });
        defer ctx.alloc.free(visible);
        try manager.setRowFilter("pk-reader", "usage_records", visible);
        defer manager.removeRowFilter("pk-reader", "usage_records") catch {};
        try observe(&reader, (try corpus.get("sql-1209")).sql, &.{ "doc:a", "doc:b" }, false);
        const hidden_anchor = try identityFilter(ctx.alloc, &.{ "doc:a", "doc:b" });
        defer ctx.alloc.free(hidden_anchor);
        try manager.setRowFilter("pk-reader", "usage_records", hidden_anchor);
        try observe(&reader, (try corpus.get("sql-1209")).sql, &.{}, false);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try command(ctx, "CREATE TABLE usage_records(id TEXT PRIMARY KEY,graph_target JSONB)");
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        var index = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/usage_records/indexes/docs_edge_graph", .POST, "{\"name\":\"docs_edge_graph\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"cites\",\"field\":\"graph_target\"}]}");
        defer index.deinit(ctx.alloc);
        if (index.status != 201) std.debug.print("SQL graph index status={d} body={s}\n", .{ index.status, index.body });
        try std.testing.expectEqual(@as(u16, 201), index.status);
        // Native explicit document keys are graph identities; SQL primary-key
        // encoding must not substitute a different domain for these fixtures.
        // A graph field contains ordinary JSON values, not the dimensioned
        // public envelope required by PostgreSQL TEXT[] columns.
        const seed = "{\"inserts\":{\"doc:root\":{\"id\":\"doc:root\",\"graph_target\":[\"doc:c\",\"doc:a\",\"doc:b\"]},\"doc:a\":{\"id\":\"doc:a\"},\"doc:b\":{\"id\":\"doc:b\"},\"doc:c\":{\"id\":\"doc:c\"}},\"sync_level\":\"full_index\"}";
        var typed_seed = try std.json.parseFromSlice(Wire.BatchRequest, ctx.alloc, seed, .{});
        defer typed_seed.deinit();
        const seed_body = try std.json.Stringify.valueAlloc(ctx.alloc, typed_seed.value, .{});
        defer ctx.alloc.free(seed_body);
        var batch = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/usage_records/batch", .POST, seed_body);
        defer batch.deinit(ctx.alloc);
        if (batch.status != 201) std.debug.print("SQL graph seed status={d} body={s}\n", .{ batch.status, batch.body });
        try std.testing.expectEqual(@as(u16, 201), batch.status);
        try check(ctx);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, serving.table_id);
        try check(ctx);
    }
};

test "mounted SQL original canonical MATCH preserves binding projection depth order filters and cold graph state" {
    try fixture.run(Campaign);
}
