// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Real SQL RLS and mandatory native credentials compose on every binding,
//! including hidden intermediates whose otherwise-visible descendants must
//! never become traversal seeds. Graph hit IDs remain native TEXT identities.
const std = @import("std");
const local = @import("antfly_local_sources");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const Wire = @import("antfly_metadata_openapi").types;
const baseline = "id NOT IN ('mid:hidden','target:hidden','fact:hidden')";
const two_hops = "SELECT d.id AS doc_id,gm.score AS graph_score FROM usage_records AS d JOIN antfly.graph_match(table_name=>'usage_records',index=>'docs_edge_graph',start=>'doc:root',pattern=>'(a)-[:cites]->(b)-[:cites]->(c)',return=>'c') AS gm ON d.id=gm.id ORDER BY doc_id";

const Campaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    table_id: u64 = 0,
    generation: u64 = 0,
    expanded: bool = false,

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
        if (response.status != 200 and response.status != 202) std.debug.print("graph SQL policy command={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expect(response.status == 200 or response.status == 202);
    }
    fn expect(ctx: *fixture.Context, sql: []const u8, keys: []const []const u8) !void {
        var reader = ctx.*;
        reader.headers = ctx.reader_headers;
        var response = try client.execute(&reader, sql, .read);
        defer response.deinit(ctx.alloc);
        if (response.status != 200) std.debug.print("graph SQL policy read={s} status={d} body={s}\n", .{ sql, response.status, response.body });
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(Wire.SQLResponse, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        if (keys.len != parsed.value.rows.len) for (parsed.value.rows) |row| {
            if (row.len != 0 and row[0] == .string) std.debug.print("graph policy result key={s}\n", .{row[0].string});
        };
        try std.testing.expectEqual(keys.len, parsed.value.rows.len);
        try std.testing.expectEqual(@as(usize, 2), parsed.value.columns.len);
        try std.testing.expectEqualStrings("doc_id", parsed.value.columns[0].name);
        try std.testing.expectEqualStrings("graph_score", parsed.value.columns[1].name);
        try std.testing.expect(@import("sql_parity_reference.zig").postgresColumnMatches(parsed.value.columns[0], 25));
        try std.testing.expect(@import("sql_parity_reference.zig").postgresColumnMatches(parsed.value.columns[1], 701));
        const nulls = parsed.value.sql_nulls orelse return error.MissingSqlNullBitmap;
        try std.testing.expectEqual(parsed.value.rows.len, nulls.len);
        for (nulls) |flags| {
            try std.testing.expectEqual(@as(usize, 2), flags.len);
            try std.testing.expect(!flags[0]);
            try std.testing.expect(flags[1]);
        }
        for (parsed.value.rows, keys) |row, key| {
            try std.testing.expectEqualStrings(key, row[0].string);
            try std.testing.expect(row[1] == .null);
        }
    }
    fn check(self: *@This(), ctx: *fixture.Context, visible: bool) !void {
        var corpus = try local.sql_parity_fixtures.Corpus.init(ctx.alloc);
        defer corpus.deinit();
        inline for (.{ "sql-1207", "sql-1208" }) |id| try expect(ctx, (try corpus.get(id)).sql, if (visible) &.{"mid:visible"} else &.{});
        try expect(ctx, two_hops, if (!visible) &.{} else if (self.expanded) &.{ "leaf:added", "leaf:fact-visible", "leaf:legacy-visible", "leaf:visible" } else &.{ "leaf:fact-visible", "leaf:legacy-visible", "leaf:visible" });
    }
    fn wait(self: *@This(), ctx: *fixture.Context) !void {
        self.generation = try @import("sql_policy_lifecycle_integration_test.zig").waitPublication(ctx, self.table_id, .active, self.generation);
    }
    fn rawGraphDenied(self: *@This(), ctx: *fixture.Context) !void {
        var snapshot = try ctx.metadata.server.svc.adminSnapshot();
        defer ctx.metadata.server.svc.freeAdminSnapshot(&snapshot);
        const group = for (snapshot.ranges) |entry| {
            if (entry.table_id == self.table_id) break entry.group_id;
        } else return error.MissingGraphPolicyOwner;
        const physical = for (snapshot.tables) |entry| {
            if (entry.table_id == self.table_id) break entry.name;
        } else return error.MissingGraphPolicyTable;
        const reader = ctx.data.http_server.?.table_reads orelse return error.MissingOwnerReadSource;
        var program = try local.api_local_graph.graphEdgesTensorProgramEnvelopeAlloc(ctx.alloc, "docs_edge_graph");
        defer program.deinit(ctx.alloc);
        // Privileged raw edge transport still has no SQL principal proof and
        // must fail even when its caller has administrative credentials.
        const path = local.api_local_graph.algebraic_ir.graphEdgeAccessPath("docs_edge_graph");
        const fragments = try ctx.alloc.alloc([]const u8, path.fragments.len);
        defer ctx.alloc.free(fragments);
        for (path.fragments, fragments) |item, *name| name.* = @tagName(item);
        const dimensions = try ctx.alloc.alloc([]const u8, path.output_dims.len);
        defer ctx.alloc.free(dimensions);
        for (path.output_dims, dimensions) |item, *name| name.* = @tagName(item);
        const laws = try ctx.alloc.alloc([]const u8, path.law_ids.len);
        defer ctx.alloc.free(laws);
        for (path.law_ids, laws) |item, *name| name.* = @tagName(item);
        var native_program = try program.asProgramAlloc(ctx.alloc);
        defer native_program.deinit(ctx.alloc);
        const program_bytes = try local.api_query_contract.encodeAlgebraicTensorProgramEnvelopeAlloc(ctx.alloc, native_program.program);
        defer ctx.alloc.free(program_bytes);
        var program_json = try std.json.parseFromSlice(std.json.Value, ctx.alloc, program_bytes, .{});
        defer program_json.deinit();
        const wire: local.api_local_graph.GraphEdgesRequestJson = .{
            .index_name = "docs_edge_graph",
            .key = "doc:root",
            .direction = "out",
            .tensor_access_path = .{ .owner = path.owner, .layout = @tagName(path.layout), .fragments = fragments, .output_dims = dimensions, .law_ids = laws },
            .tensor_program = program_json.value,
        };
        const bytes = try std.json.Stringify.valueAlloc(ctx.alloc, wire, .{});
        defer ctx.alloc.free(bytes);
        var request = try local.api_local_graph.parseGraphEdgesRequest(ctx.alloc, bytes);
        defer request.deinit(ctx.alloc);
        try std.testing.expectError(error.RowPolicyUnsupported, reader.graphEdgesGroupLocal(ctx.alloc, group, physical, request, .read_index));
        try std.testing.expectError(error.RowPolicyUnsupported, reader.graphHydrateGroupLocal(ctx.alloc, group, physical, .{ .keys = &.{}, .include_hits = false }, .read_index));
    }
    fn credentialIntersection(self: *@This(), ctx: *fixture.Context) !void {
        const manager = (ctx.metadata.server.owned_public_http_server orelse return error.MissingMetadataPublicApi).cfg.user_manager orelse return error.MissingUserManager;
        // SQL permits mid:visible, but the independently mandatory credential
        // excludes it. The credential includes the other branch and both leaves;
        // SQL RLS must continue denying its intermediate and direct target.
        const filter = try std.json.Stringify.valueAlloc(ctx.alloc, .{ .doc_id = @import("antfly_query_openapi").DocIdQuery{ .ids = &.{ "doc:root", "mid:hidden", "target:hidden", "leaf:behind-hidden", "leaf:visible" } } }, .{});
        defer ctx.alloc.free(filter);
        try manager.setRowFilter("pk-reader", "usage_records", filter);
        defer manager.removeRowFilter("pk-reader", "usage_records") catch {};
        try self.check(ctx, false);
    }
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        try command(ctx, "CREATE TABLE usage_records(id TEXT PRIMARY KEY,graph_target JSONB)");
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        self.table_id = serving.table_id;
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.table_id);
        try fixture.waitRelationPublication(ctx.alloc, ctx.io, ctx.metadata, "usage_records");
        var index = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/usage_records/indexes/docs_edge_graph", .POST, "{\"name\":\"docs_edge_graph\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"cites\",\"field\":\"graph_target\"}]}");
        defer index.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), index.status);
        const seed = "{\"inserts\":{\"doc:root\":{\"id\":\"doc:root\",\"graph_target\":[\"mid:hidden\",\"mid:visible\",\"target:hidden\"]},\"mid:hidden\":{\"id\":\"mid:hidden\",\"graph_target\":[\"leaf:behind-hidden\"]},\"mid:visible\":{\"id\":\"mid:visible\",\"graph_target\":[\"leaf:visible\"]},\"target:hidden\":{\"id\":\"target:hidden\"},\"leaf:behind-hidden\":{\"id\":\"leaf:behind-hidden\"},\"leaf:visible\":{\"id\":\"leaf:visible\"},\"leaf:added\":{\"id\":\"leaf:added\"}},\"sync_level\":\"full_index\"}";
        var typed = try std.json.parseFromSlice(Wire.BatchRequest, ctx.alloc, seed, .{});
        defer typed.deinit();
        const bytes = try std.json.Stringify.valueAlloc(ctx.alloc, typed.value, .{});
        defer ctx.alloc.free(bytes);
        var batch = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/usage_records/batch", .POST, bytes);
        defer batch.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), batch.status);
        var facts = try fixture.request(ctx.alloc, ctx.transport, ctx.headers, ctx.base, "/db/v1/tables/usage_records/batch", .POST, "{\"inserts\":{\"fact:hidden\":{\"id\":\"fact:hidden\"},\"fact:visible\":{\"id\":\"fact:visible\"},\"leaf:fact-hidden\":{\"id\":\"leaf:fact-hidden\"},\"leaf:fact-visible\":{\"id\":\"leaf:fact-visible\"},\"leaf:legacy-hidden\":{\"id\":\"leaf:legacy-hidden\"},\"leaf:legacy-visible\":{\"id\":\"leaf:legacy-visible\"}},\"sync_level\":\"full_index\"}");
        defer facts.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), facts.status);
        // This disposable administrator uses the real internal native graph
        // producer before policy publication. User wire cannot forge owners.
        // A graph-only internal batch has no document mutation from which
        // generic batch fanout can infer an owner. Capture the actual source
        // route and use its existing fenced atomic Raft batch port.
        const catalogs = @import("table_catalog.zig");
        const domain = local.system_catalog_domain;
        const lookup: domain.ResolveMany = .{ .relations = &.{.{ .database = "default", .namespace = "public", .name = "usage_records" }}, .include_query_definitions = true };
        const metadata = @import("http_server.zig").StatusSource.fromMetadataHttpService(ctx.metadata.server.svc);
        const resolved_bytes = try metadata.systemCatalog(ctx.alloc, .{}, .{ .resolve_many = lookup });
        defer ctx.alloc.free(resolved_bytes);
        var resolved = try std.json.parseFromSlice(domain.ResolvedMany, ctx.alloc, resolved_bytes, .{});
        defer resolved.deinit();
        try resolved.value.validateRelations(lookup);
        const captured = resolved.value.relations[0] orelse return error.MissingGraphPolicyOwner;
        try std.testing.expectEqual(self.table_id, captured.owner.table_id);
        const physical_name = captured.table.name;
        var routing = (try catalogs.tableRoutingSnapshotForWrite(ctx.alloc, ctx.data.write_source.catalog, physical_name, ctx.data.write_source.catalog.budget(null).nowNs() +| 30 * std.time.ns_per_s)) orelse return error.MissingGraphPolicyOwner;
        defer routing.deinit(ctx.alloc);
        try std.testing.expectEqual(self.table_id, routing.table_id);
        const route = routing.resolveRouteForKey("mid:visible") orelse return error.MissingGraphPolicyOwner;
        for ([_][]const u8{ "fact:hidden", "fact:visible" }) |key| {
            const owner = routing.resolveRouteForKey(key) orelse return error.MissingGraphPolicyOwner;
            try std.testing.expect(std.meta.eql(route, owner));
        }
        const batcher = ctx.data.write_source.raft_batcher orelse return error.MissingGraphPolicyOwner;
        try batcher.batchGroupRoutedWithCancellation(ctx.alloc, routing.fenceForRoute(route), physical_name, .{ .graph_writes = &.{
            .{ .index_name = "docs_edge_graph", .source = "mid:visible", .target = "leaf:fact-hidden", .edge_type = "cites", .edge_id = "fact:hidden", .owner_document = "fact:hidden" },
            .{ .index_name = "docs_edge_graph", .source = "mid:visible", .target = "leaf:fact-visible", .edge_type = "cites", .edge_id = "fact:visible", .owner_document = "fact:visible" },
            .{ .index_name = "docs_edge_graph", .source = "mid:visible", .target = "leaf:legacy-hidden", .edge_type = "cites", .owner = "fact:hidden" },
            .{ .index_name = "docs_edge_graph", .source = "mid:visible", .target = "leaf:legacy-visible", .edge_type = "cites", .owner = "fact:visible" },
        }, .sync_level = .full_index }, .none);
        try command(ctx, "CREATE POLICY graph_reader ON usage_records TO \"pk-reader\" USING (" ++ baseline ++ ")");
        try command(ctx, "CREATE POLICY graph_auditor ON usage_records TO \"pk-rewriter\" USING (true)");
        try command(ctx, "ALTER TABLE usage_records ENABLE ROW LEVEL SECURITY");
        try self.wait(ctx);
        try self.rawGraphDenied(ctx);
        try self.check(ctx, true);
        // This is a real authorized SQL mutation after policy activation, not
        // a pre-policy graph seed. Subsequent reads must observe new primary
        // bytes and derived adjacency at one complete source watermark.
        try command(ctx, "UPDATE usage_records SET graph_target = '[\"leaf:visible\",\"leaf:added\",\"target:hidden\"]'::jsonb WHERE id = 'mid:visible'");
        self.expanded = true;
        try self.check(ctx, true);
        try self.credentialIntersection(ctx);
        try command(ctx, "ALTER POLICY graph_reader ON usage_records USING (id NOT IN ('doc:root','mid:hidden','target:hidden'))");
        try self.wait(ctx);
        try self.check(ctx, false);
        try command(ctx, "ALTER POLICY graph_reader ON usage_records USING (" ++ baseline ++ ")");
        try self.wait(ctx);
        try self.check(ctx, true);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        ctx.headers = ctx.admin_headers;
        const serving = try fixture.waitServingSchema(ctx, "usage_records", 0);
        try std.testing.expectEqual(self.table_id, serving.table_id);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, self.table_id);
        try self.rawGraphDenied(ctx);
        try self.check(ctx, true);
        try self.credentialIntersection(ctx);
    }
};

test "mounted SQL graph joined hits compose credential and SQL policies for anchors intermediates targets and cold epochs" {
    try fixture.run(Campaign);
}
