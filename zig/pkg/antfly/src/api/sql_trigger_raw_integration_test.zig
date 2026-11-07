// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Whole public ingress qualification: coalesced transforms and FK actions use
//! one trigger pipeline, while owner rows and graph deltas share the decision.
const std = @import("std");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const http = @import("../raft/transport/http_common.zig");
const wire = @import("antfly_metadata_openapi").types;
const platform = @import("antfly_platform");
const A = std.mem.Allocator;
fn query(ctx: *fixture.Context, statement: []const u8) !std.json.Parsed(wire.SQLResponse) {
    var authorized = ctx.*;
    authorized.headers = ctx.admin_headers;
    var response = try client.execute(&authorized, statement, if (std.mem.startsWith(u8, statement, "SELECT") or std.mem.startsWith(u8, statement, "MATCH")) .read else .write_admission);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("raw trigger SQL={s} status={d} body={s}\n", .{ statement, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 202);
    return std.json.parseFromSlice(wire.SQLResponse, ctx.alloc, response.body, .{ .allocate = .alloc_always });
}
fn command(ctx: *fixture.Context, statement: []const u8) !void {
    var result = try query(ctx, statement);
    defer result.deinit();
    const receipt = result.value.catalog_receipt orelse return;
    if (receipt.state == .committed) return;
    if (receipt.state == .rejected) return error.RawTriggerDdlRejected;
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, wire.SQLCatalogRecoveryRequest{ .recovery_token = receipt.recovery_token }, .{});
    defer ctx.alloc.free(body);
    const deadline = platform.time.monotonicNs() +| 60 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/sql/operations/status", .POST, body, 5000);
        defer response.deinit(ctx.alloc);
        if (response.status == 503 or response.status == 504) continue;
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var current = try std.json.parseFromSlice(wire.SQLCatalogOperationReceipt, ctx.alloc, response.body, .{});
        defer current.deinit();
        try std.testing.expectEqualStrings(receipt.recovery_token, current.value.recovery_token);
        if (current.value.state == .committed) return;
        if (current.value.state == .rejected) return error.RawTriggerDdlRejected;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.RawTriggerDdlDeadline;
}
fn batch(ctx: *fixture.Context, table: []const u8, source: []const u8, status: u16) !void {
    var typed = try std.json.parseFromSlice(wire.BatchRequest, ctx.alloc, source, .{});
    defer typed.deinit();
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, typed.value, .{});
    defer ctx.alloc.free(body);
    var response = try fixture.batch(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, table, body);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("raw trigger batch table={s} expected={d} status={d} body={s}\n", .{ table, status, response.status, response.body });
    try std.testing.expectEqual(status, response.status);
    if (status == 201) {
        var outcome = try std.json.parseFromSlice(wire.BatchResponse, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
        defer outcome.deinit();
        try std.testing.expect(outcome.value.failure == null);
    }
}
fn scalar(ctx: *fixture.Context, statement: []const u8, expected: i64) !void {
    var actual = try query(ctx, statement);
    defer actual.deinit();
    try std.testing.expectEqual(@as(usize, 1), actual.value.rows.len);
    try std.testing.expectEqual(@as(usize, 1), actual.value.rows[0].len);
    try std.testing.expectEqual(expected, try fixture.sqlInteger(actual.value.rows[0][0]));
}
fn graph(ctx: *fixture.Context, expected: []const []const u8) !void {
    // SQL graph hydration uses the same RLS-authorized endpoint visibility as
    // ordinary SQL reads. Raw graph routes remain independently guarded.
    var actual = try query(ctx, "MATCH (doc)-[:\"R\"]->(target) WITH GRAPH g ON children START 'a' RETURN target.key AS target_id ORDER BY target.key");
    defer actual.deinit();
    try std.testing.expectEqual(expected.len, actual.value.rows.len);
    for (actual.value.rows, expected) |row, key| {
        try std.testing.expectEqual(@as(usize, 1), row.len);
        try std.testing.expectEqualStrings(key, row[0].string);
    }
}
const ReplyLoss = struct {
    inner: http.RequestExecutor,
    lost: bool = false,
    fn execute(raw: *anyopaque, a: A, request: http.HttpRequest) !http.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var response = try self.inner.execute(a, request);
        if (!self.lost and request.method == .POST and response.status == 201 and std.mem.endsWith(u8, request.uri, "/children/batch")) {
            self.lost = true;
            response.deinit(a);
            return error.ConnectionResetByPeer;
        }
        return response;
    }
    fn executor(self: *@This()) http.RequestExecutor {
        return .{ .ptr = self, .vtable = &.{ .execute = execute }, .clock_io = self.inner.clock_io };
    }
};
// The SQL seed is observed through the actual resident callback and forced
// HTTP group lookup before any raw VM. Both own flags after provider teardown.
fn expectTypedNullObservations(ctx: *fixture.Context) !void {
    const local = @import("antfly_local_sources");
    const tables = @import("table_catalog.zig");
    var catalog = ctx.catalogClient();
    var reply = try catalog.readSystemCatalog(ctx.metadata_uri, .{ .resolve_many = .{ .sql_sources = &.{.{ .name = .{ .table = "children" } }} } }, 15000, null);
    defer reply.deinit(ctx.alloc);
    var parsed = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, reply.body, .{});
    defer parsed.deinit();
    const table = parsed.value.sql_sources[0].?.value.table.binding;
    const server = if (ctx.data.http_server) |*value| value else return error.MissingDataPublicApi;
    const captured = try tables.routedGroupSnapshot(ctx.alloc, server.catalogSource(), table.name, "a");
    const route = captured.route orelse return error.MissingTypedNullRoute;
    const reader = server.table_reads orelse return error.MissingTypedNullReader;
    var resident = (try reader.lookupGroupLocal(ctx.alloc, route.group_id, table.name, "a", .{ .include_primary_digest = true, .include_json_null_fields = true }, .read_index)) orelse return error.MissingTypedNullRow;
    defer resident.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(usize, 1), resident.json_null_fields.len);
    try std.testing.expectEqualStrings("payload", resident.json_null_fields[0]);
    var remote = ctx.internalDataClient();
    var observed = try remote.fetchGroupLookupWithOptions(ctx.base, route.group_id, table.name, "a", null, "read_index", 15000, null, .{ .catalog_route_fence = captured.fence(), .include_primary_digest = true, .include_json_null_fields = true });
    defer observed.deinit(ctx.alloc);
    try std.testing.expectEqualStrings(resident.json, observed.body);
    try std.testing.expectEqual(resident.expected_content_digest, observed.expected_content_digest);
    try std.testing.expectEqual(@as(usize, 1), observed.json_null_fields.len);
    try std.testing.expectEqualStrings("payload", observed.json_null_fields[0]);
    var ordinary = try remote.fetchGroupLookupWithOptions(ctx.base, route.group_id, table.name, "a", null, "read_index", 15000, null, .{ .catalog_route_fence = captured.fence(), .include_primary_digest = true });
    defer ordinary.deinit(ctx.alloc);
    try std.testing.expectEqualStrings(resident.json, ordinary.body);
    try std.testing.expectEqual(@as(usize, 0), ordinary.json_null_fields.len);
}
const Campaign = struct {
    pub const native_auth = true;
    pub const native_sessions = true;
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
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        try command(ctx, "CREATE TABLE parents(id TEXT PRIMARY KEY,n BIGINT UNIQUE)");
        _ = try fixture.waitServingSchema(ctx, "parents", 0);
        try command(ctx, "CREATE TABLE children(id TEXT PRIMARY KEY,parent_n BIGINT REFERENCES parents(n) ON UPDATE CASCADE ON DELETE CASCADE,n BIGINT NOT NULL CHECK(n>=0 AND n<=50),audit BIGINT NOT NULL DEFAULT 0,payload JSONB)");
        const owner = try fixture.waitServingSchema(ctx, "children", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, owner.table_id);
        try batch(ctx, "parents", "{\"inserts\":{\"p\":{\"id\":\"p\",\"n\":10}},\"sync_level\":\"full_index\"}", 201);
        try batch(ctx, "children", "{\"inserts\":{\"a\":{\"id\":\"a\",\"parent_n\":10,\"n\":1,\"audit\":0,\"payload\":null},\"b\":{\"id\":\"b\",\"parent_n\":10,\"n\":2,\"audit\":0,\"payload\":null},\"c\":{\"id\":\"c\",\"parent_n\":10,\"n\":0,\"audit\":0,\"payload\":null},\"d\":{\"id\":\"d\",\"parent_n\":10,\"n\":0,\"audit\":0,\"payload\":null}},\"sync_level\":\"full_index\"}", 201);
        // Ordinary relational REST null has SQL-NULL provenance. Seed an
        // actual JSON datum through SQL before graph/trigger/FK preservation.
        try scalar(ctx, "SELECT count(*) FROM children WHERE id='b' AND payload IS NULL", 1);
        try command(ctx, "UPDATE children SET payload='null'::jsonb WHERE id='a'");
        try scalar(ctx, "SELECT count(*) FROM children WHERE id='a' AND payload='null'::jsonb AND payload IS NOT NULL", 1);
        try expectTypedNullObservations(ctx);
        var index = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/tables/children/indexes/g", .POST, "{\"name\":\"g\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"R\"}]}");
        defer index.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), index.status);
        inline for (.{
            "CREATE FUNCTION count_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.audit := OLD.audit+1; RETURN NEW; END $$",
            "CREATE FUNCTION skip_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NULL; END $$",
            "CREATE FUNCTION clamp_change() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.n := 5; RETURN NEW; END $$",
            "CREATE FUNCTION ignore_after() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.audit := NEW.audit+100; RETURN NEW; END $$",
            "CREATE FUNCTION count_cascade() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.audit := OLD.audit+10; RETURN NEW; END $$",
            "CREATE FUNCTION demanded_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM 1/0; RETURN NULL; END $$",
            "CREATE TRIGGER a_skip BEFORE UPDATE OF n ON children FOR EACH ROW WHEN(NEW.n<0) EXECUTE FUNCTION skip_change()",
            "CREATE TRIGGER b_count BEFORE UPDATE OF n ON children FOR EACH ROW EXECUTE FUNCTION count_change()",
            "CREATE TRIGGER c_clamp BEFORE UPDATE OF n ON children FOR EACH ROW WHEN(NEW.n=20) EXECUTE FUNCTION clamp_change()",
            "CREATE TRIGGER d_after AFTER UPDATE OF n ON children FOR EACH ROW EXECUTE FUNCTION ignore_after()",
            "CREATE TRIGGER e_cascade BEFORE UPDATE OF parent_n ON children FOR EACH ROW EXECUTE FUNCTION count_cascade()",
        }) |statement| try command(ctx, statement);
        _ = try fixture.waitServingSchema(ctx, "children", owner.version);
        // Repeated same-key transforms coalesce before the row VM: one audit
        // increment, complete numeric transformation and one graph decision.
        try batch(ctx, "children", "{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1}]},{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":2},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"b\",\"weight\":2}}]}],\"sync_level\":\"full_index\"}", 201);
        try scalar(ctx, "SELECT n FROM children WHERE id='a'", 4);
        try scalar(ctx, "SELECT audit FROM children WHERE id='a'", 1);
        try graph(ctx, &.{"b"});
        try scalar(ctx, "SELECT count(*) FROM children WHERE id='a' AND payload='null'::jsonb AND payload IS NOT NULL", 1);
        // Graph-only updates do not pretend to assign n (UPDATE OF).
        try batch(ctx, "children", "{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"c\",\"weight\":3}}]}],\"sync_level\":\"full_index\"}", 201);
        try scalar(ctx, "SELECT audit FROM children WHERE id='a'", 1);
        try graph(ctx, &.{ "b", "c" });
        try scalar(ctx, "SELECT count(*) FROM children WHERE id='a' AND payload='null'::jsonb AND payload IS NOT NULL", 1);
        // A skipped owner row must also discard its otherwise valid edge.
        try batch(ctx, "children", "{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":-10},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"d\"}}]}],\"sync_level\":\"full_index\"}", 201);
        try scalar(ctx, "SELECT n FROM children WHERE id='a'", 4);
        try scalar(ctx, "SELECT audit FROM children WHERE id='a'", 1);
        try graph(ctx, &.{ "b", "c" });
        // One later constraint violation aborts all earlier row/edge effects.
        try batch(ctx, "children", "{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"d\"}}]},{\"key\":\"b\",\"operations\":[{\"op\":\"$set\",\"path\":\"n\",\"value\":60}]}],\"sync_level\":\"full_index\"}", 409);
        try scalar(ctx, "SELECT n FROM children WHERE id='a'", 4);
        try scalar(ctx, "SELECT n FROM children WHERE id='b'", 2);
        try scalar(ctx, "SELECT audit FROM children WHERE id='a'", 1);
        try graph(ctx, &.{ "b", "c" });
        // The parent has no own trigger: its indexed incoming-FK occupancy
        // still captures the child closure and fires generated UPDATE OF once.
        try batch(ctx, "parents", "{\"transforms\":[{\"key\":\"p\",\"operations\":[{\"op\":\"$set\",\"path\":\"n\",\"value\":20}]}],\"sync_level\":\"full_index\"}", 201);
        try scalar(ctx, "SELECT count(*) FROM children WHERE parent_n=20", 4);
        try scalar(ctx, "SELECT audit FROM children WHERE id='a'", 11);
        try scalar(ctx, "SELECT audit FROM children WHERE id='b'", 10);
        // Prove SQL NULL and JSON null remain distinct across a graph-only
        // projection and then a declared JSON transform.
        try scalar(ctx, "SELECT count(*) FROM children WHERE id='a' AND payload='null'::jsonb AND payload IS NOT NULL", 1);
        try command(ctx, "UPDATE children SET payload=NULL WHERE id='a'");
        try batch(ctx, "children", "{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$set\",\"path\":\"payload\",\"value\":null}]}],\"sync_level\":\"full_index\"}", 201);
        try scalar(ctx, "SELECT count(*) FROM children WHERE id='a' AND payload='null'::jsonb AND payload IS NOT NULL", 1);
        inline for (.{
            "CREATE POLICY read_rows ON children FOR SELECT TO \"pk-rewriter\" USING(true)",
            "CREATE POLICY write_rows ON children FOR UPDATE TO \"pk-rewriter\" USING(true) WITH CHECK(n<10)",
            "CREATE POLICY no_delete ON children FOR DELETE TO \"pk-rewriter\" USING(false)",
            "ALTER TABLE children ENABLE ROW LEVEL SECURITY",
        }) |statement| try command(ctx, statement);
        _ = try @import("sql_policy_lifecycle_integration_test.zig").waitPublication(ctx, owner.table_id, .active, 0);
        // WITH CHECK runs on final BEFORE output, and before any AFTER body.
        try command(ctx, "CREATE TRIGGER z_after_fault AFTER UPDATE OF n ON children FOR EACH ROW EXECUTE FUNCTION demanded_failure()");
        _ = try fixture.waitServingSchema(ctx, "children", 0);
        try batch(ctx, "children", "{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$set\",\"path\":\"n\",\"value\":30},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"d\"}}]}],\"sync_level\":\"full_index\"}", 403);
        try batch(ctx, "children", "{\"deletes\":[\"b\"],\"sync_level\":\"full_index\"}", 403);
        try scalar(ctx, "SELECT n FROM children WHERE id='a'", 4);
        try scalar(ctx, "SELECT n FROM children WHERE id='b'", 2);
        try graph(ctx, &.{ "b", "c" });
        try batch(ctx, "children", "{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"d\"}}]}],\"sync_level\":\"full_index\"}", 400);
        try scalar(ctx, "SELECT n FROM children WHERE id='a'", 4);
        try scalar(ctx, "SELECT audit FROM children WHERE id='a'", 11);
        try graph(ctx, &.{ "b", "c" });
        try command(ctx, "DROP TRIGGER z_after_fault ON children");
        try command(ctx, "CREATE TRIGGER zero_statement BEFORE UPDATE ON children EXECUTE FUNCTION demanded_failure()");
        _ = try fixture.waitServingSchema(ctx, "children", 0);
        try batch(ctx, "children", "{\"transforms\":[{\"key\":\"missing\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1}]}],\"sync_level\":\"full_index\"}", 400);
        try command(ctx, "DROP TRIGGER zero_statement ON children");
        _ = try fixture.waitServingSchema(ctx, "children", 0);
        // The permitted candidate is clamped by BEFORE to satisfy WITH CHECK.
        // Lose the actual successful response; do not replay a non-idempotent
        // batch. Durable native recovery and cold replay must not refire it.
        var loss: ReplyLoss = .{ .inner = ctx.transport };
        var uncertain = ctx.*;
        uncertain.transport = loss.executor();
        try std.testing.expectError(error.ConnectionResetByPeer, batch(&uncertain, "children", "{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$set\",\"path\":\"n\",\"value\":20},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"d\"}}]}],\"sync_level\":\"full_index\"}", 201));
        try std.testing.expect(loss.lost);
        try finalState(ctx);
    }
    fn finalState(ctx: *fixture.Context) !void {
        try scalar(ctx, "SELECT n FROM children WHERE id='a'", 5);
        try scalar(ctx, "SELECT audit FROM children WHERE id='a'", 12);
        try scalar(ctx, "SELECT audit FROM children WHERE id='b'", 10);
        try scalar(ctx, "SELECT count(*) FROM children WHERE parent_n=20", 4);
        try scalar(ctx, "SELECT count(*) FROM children WHERE id='a' AND payload='null'::jsonb AND payload IS NOT NULL", 1);
        try scalar(ctx, "SELECT count(*) FROM children WHERE id='missing'", 0);
        try graph(ctx, &.{ "b", "c", "d" });
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        const owner = try fixture.waitServingSchema(ctx, "children", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, owner.table_id);
        _ = try @import("sql_policy_lifecycle_integration_test.zig").waitPublication(ctx, owner.table_id, .active, 0);
        try finalState(ctx);
    }
};
test "mounted raw trigger transforms FK cascades and graph effects share RLS constraints one atomic decision and cold no refire" {
    try fixture.run(Campaign);
}

const OrdinaryGraph = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    pub const configureAuth = Campaign.configureAuth;
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        // This witness isolates the established single-owner graph lane.
        // Constrained transforms need the activated coordinated preparation
        // path; its no-trigger PK cases are tested separately below.
        try command(ctx, "CREATE TABLE ordinary_graph(id TEXT,n BIGINT)");
        const owner = try fixture.waitServingSchema(ctx, "ordinary_graph", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, owner.table_id);
        var index = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/tables/ordinary_graph/indexes/g", .POST, "{\"name\":\"g\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"R\"}]}");
        defer index.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), index.status);
        try batch(ctx, "ordinary_graph", "{\"inserts\":{\"a\":{\"id\":\"a\",\"n\":1},\"b\":{\"id\":\"b\",\"n\":0}},\"sync_level\":\"full_index\"}", 201);
        // No CREATE TRIGGER, trigger State or protocol7 admission is needed
        // for the established ordinary atomic single-owner mutation lane.
        try batch(ctx, "ordinary_graph", "{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"b\",\"weight\":2}}]}],\"sync_level\":\"full_index\"}", 201);
        try check(ctx);
    }
    fn check(ctx: *fixture.Context) !void {
        try scalar(ctx, "SELECT n FROM ordinary_graph WHERE id='a'", 2);
        var rows = try query(ctx, "MATCH (doc)-[:\"R\"]->(target) WITH GRAPH g ON ordinary_graph START 'a' RETURN target.key AS target_id");
        defer rows.deinit();
        try std.testing.expectEqual(@as(usize, 1), rows.value.rows.len);
        try std.testing.expectEqualStrings("b", rows.value.rows[0][0].string);
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        const owner = try fixture.waitServingSchema(ctx, "ordinary_graph", 0);
        try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, owner.table_id);
        try check(ctx);
    }
};
test "mounted ordinary single owner graph transforms remain available without trigger state or preparation grants across cold restart" {
    try fixture.run(OrdinaryGraph);
}

fn noTriggerState(ctx: *fixture.Context, table: []const u8) !void {
    var transport = ctx.catalogClient();
    var response = try transport.readSystemCatalog(ctx.metadata_uri, .{ .resolve_many = .{ .sql_sources = &.{.{ .name = .{ .table = table } }}, .include_query_definitions = true } }, 5000, null);
    defer response.deinit(ctx.alloc);
    var snapshot = try std.json.parseFromSlice(@import("antfly_local_sources").system_catalog_domain.ResolvedMany, ctx.alloc, response.body, .{});
    defer snapshot.deinit();
    const binding = snapshot.value.sql_sources[0].?.value.table.binding;
    var schema = try std.json.parseFromSlice(std.json.Value, ctx.alloc, binding.query_definition.?.schema_json, .{});
    defer schema.deinit();
    // A native absence proof must not create a fabricated empty generation.
    try std.testing.expect(!schema.value.object.contains("sql_triggers"));
}
fn multiBatch(ctx: *fixture.Context, source: []const u8, expected_error: ?u16) !void {
    var request = try std.json.parseFromSlice(wire.MultiBatchRequest, ctx.alloc, source, .{});
    defer request.deinit();
    const bytes = try std.json.Stringify.valueAlloc(ctx.alloc, request.value, .{});
    defer ctx.alloc.free(bytes);
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/batch", .POST, bytes, 30000);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("absence graph multi expected={any} status={d} body={s}\n", .{ expected_error, response.status, response.body });
    if (expected_error) |status| return std.testing.expectEqual(status, response.status);
    try std.testing.expect(response.status == 201 or response.status == 202);
    var result = try std.json.parseFromSlice(wire.MultiBatchResponse, ctx.alloc, response.body, .{});
    defer result.deinit();
    const status = result.value.status orelse return error.MissingMultiGraphDecision;
    try std.testing.expect(std.mem.startsWith(u8, status, "committed"));
    try std.testing.expect(!std.mem.eql(u8, status, "committed_repair_required"));
    const tables = result.value.tables orelse return error.MissingMultiGraphDecision;
    try std.testing.expectEqual(@as(usize, 2), tables.map.count());
    var iterator = tables.map.iterator();
    while (iterator.next()) |table| try std.testing.expect(table.value_ptr.failure == null);
}
fn awaitScalar(ctx: *fixture.Context, statement: []const u8, expected: i64) !void {
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var result = try query(ctx, statement);
        defer result.deinit();
        if (result.value.rows.len == 1 and result.value.rows[0].len == 1 and try fixture.sqlInteger(result.value.rows[0][0]) == expected) return;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.MultiGraphVisibilityDeadline;
}
fn plainEdges(ctx: *fixture.Context, expected: []const []const u8) !void {
    var rows = try query(ctx, "MATCH (doc)-[:\"R\"]->(target) WITH GRAPH g ON plain_graph START 'a' RETURN target.key AS target_id ORDER BY target.key");
    defer rows.deinit();
    try std.testing.expectEqual(expected.len, rows.value.rows.len);
    for (rows.value.rows, expected) |row, key| try std.testing.expectEqualStrings(key, row[0].string);
}
const MultiReplyLoss = struct {
    inner: http.RequestExecutor,
    lost: bool = false,
    fn execute(raw: *anyopaque, a: A, request: http.HttpRequest) !http.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var response = try self.inner.execute(a, request);
        if (!self.lost and request.method == .POST and (response.status == 201 or response.status == 202) and std.mem.endsWith(u8, request.uri, "/db/v1/batch")) {
            self.lost = true;
            response.deinit(a);
            return error.ConnectionResetByPeer;
        }
        return response;
    }
    fn executor(self: *@This()) http.RequestExecutor {
        return .{ .ptr = self, .vtable = &.{ .execute = execute }, .clock_io = self.inner.clock_io };
    }
};
const AbsentGraph = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    pub const configureAuth = Campaign.configureAuth;
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        try command(ctx, "CREATE TABLE plain_graph(id TEXT PRIMARY KEY,n BIGINT CHECK(n>=0))");
        try command(ctx, "CREATE TABLE plain_peer(id TEXT PRIMARY KEY,n BIGINT CHECK(n>=0))");
        _ = try fixture.waitServingSchema(ctx, "plain_graph", 0);
        _ = try fixture.waitServingSchema(ctx, "plain_peer", 0);
        try batch(ctx, "plain_graph", "{\"inserts\":{\"a\":{\"id\":\"a\",\"n\":1},\"b\":{\"id\":\"b\",\"n\":0},\"c\":{\"id\":\"c\",\"n\":0}},\"sync_level\":\"full_index\"}", 201);
        try batch(ctx, "plain_peer", "{\"inserts\":{\"p\":{\"id\":\"p\",\"n\":5}},\"sync_level\":\"full_index\"}", 201);
        var index = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/tables/plain_graph/indexes/g", .POST, "{\"name\":\"g\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"R\"}]}");
        defer index.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), index.status);
        try noTriggerState(ctx, "plain_graph");
        try noTriggerState(ctx, "plain_peer");
        // Two independent physical table owners force the public 2PC path.
        // No trigger routine, empty attachment State or test activation exists.
        try multiBatch(ctx, "{\"tables\":{\"plain_graph\":{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"b\",\"weight\":2}}]}]},\"plain_peer\":{\"transforms\":[{\"key\":\"p\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1}]}]}},\"sync_level\":\"full_index\"}", null);
        try awaitScalar(ctx, "SELECT n FROM plain_graph WHERE id='a'", 2);
        try awaitScalar(ctx, "SELECT n FROM plain_peer WHERE id='p'", 6);
        try plainEdges(ctx, &.{"b"});
        inline for (.{ "plain_graph", "plain_peer" }) |table| {
            const read = try std.fmt.allocPrint(ctx.alloc, "CREATE POLICY read_rows ON {s} FOR SELECT TO \"pk-rewriter\" USING(true)", .{table});
            defer ctx.alloc.free(read);
            try command(ctx, read);
            const write = try std.fmt.allocPrint(ctx.alloc, "CREATE POLICY write_rows ON {s} FOR UPDATE TO \"pk-rewriter\" USING(true) WITH CHECK(n<10)", .{table});
            defer ctx.alloc.free(write);
            try command(ctx, write);
            const deletion = try std.fmt.allocPrint(ctx.alloc, "CREATE POLICY no_delete ON {s} FOR DELETE TO \"pk-rewriter\" USING(false)", .{table});
            defer ctx.alloc.free(deletion);
            try command(ctx, deletion);
            const enable = try std.fmt.allocPrint(ctx.alloc, "ALTER TABLE {s} ENABLE ROW LEVEL SECURITY", .{table});
            defer ctx.alloc.free(enable);
            try command(ctx, enable);
            const owner = try fixture.waitServingSchema(ctx, table, 0);
            _ = try @import("sql_policy_lifecycle_integration_test.zig").waitPublication(ctx, owner.table_id, .active, 0);
        }
        try multiBatch(ctx, "{\"tables\":{\"plain_graph\":{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"c\"}}]}]},\"plain_peer\":{\"transforms\":[{\"key\":\"p\",\"operations\":[{\"op\":\"$set\",\"path\":\"n\",\"value\":20}]}]}},\"sync_level\":\"full_index\"}", 403);
        try scalar(ctx, "SELECT n FROM plain_graph WHERE id='a'", 2);
        try scalar(ctx, "SELECT n FROM plain_peer WHERE id='p'", 6);
        try batch(ctx, "plain_graph", "{\"deletes\":[\"b\"],\"sync_level\":\"full_index\"}", 403);
        try scalar(ctx, "SELECT count(*) FROM plain_graph WHERE id='b'", 1);
        try plainEdges(ctx, &.{"b"});
        // Both public coordinated entrypoints must mint real mutation proofs
        // after graph preparation. Move permitted values forward then back;
        // addToSet uses the supported graph materializer without changing the
        // existing edge, preserving the later lost-reply predecessor witness.
        for ([_][]const u8{ "/db/v1/batch", "/db/v1/transactions/commit" }) |path| {
            const body = if (std.mem.endsWith(u8, path, "/commit"))
                "{\"read_set\":[],\"tables\":{\"plain_graph\":{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$set\",\"path\":\"n\",\"value\":2},{\"op\":\"$addToSet\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"b\"}}]}]},\"plain_peer\":{\"transforms\":[{\"key\":\"p\",\"operations\":[{\"op\":\"$set\",\"path\":\"n\",\"value\":6}]}]}},\"sync_level\":\"full_index\"}"
            else
                "{\"tables\":{\"plain_graph\":{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$set\",\"path\":\"n\",\"value\":3},{\"op\":\"$addToSet\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"b\"}}]}]},\"plain_peer\":{\"transforms\":[{\"key\":\"p\",\"operations\":[{\"op\":\"$set\",\"path\":\"n\",\"value\":7}]}]}},\"sync_level\":\"full_index\"}";
            var accepted = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .POST, body, 30000);
            defer accepted.deinit(ctx.alloc);
            const committed_status: u16 = if (std.mem.endsWith(u8, path, "/commit")) 200 else 201;
            if (accepted.status != committed_status and accepted.status != 202) std.debug.print("coordinated policy admission path={s} status={d} body={s}\n", .{ path, accepted.status, accepted.body[0..@min(accepted.body.len, 512)] });
            try std.testing.expect(accepted.status == committed_status or accepted.status == 202);
            try awaitScalar(ctx, "SELECT n FROM plain_graph WHERE id='a'", if (std.mem.endsWith(u8, path, "/commit")) 2 else 3);
            try awaitScalar(ctx, "SELECT n FROM plain_peer WHERE id='p'", if (std.mem.endsWith(u8, path, "/commit")) 6 else 7);
            try plainEdges(ctx, &.{"b"});
        }
        try scalar(ctx, "SELECT n FROM plain_graph WHERE id='a'", 2);
        try scalar(ctx, "SELECT n FROM plain_peer WHERE id='p'", 6);
        try plainEdges(ctx, &.{"b"});
        var loss: MultiReplyLoss = .{ .inner = ctx.transport };
        var uncertain = ctx.*;
        uncertain.transport = loss.executor();
        try std.testing.expectError(error.ConnectionResetByPeer, multiBatch(&uncertain, "{\"tables\":{\"plain_graph\":{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"c\"}}]}]},\"plain_peer\":{\"transforms\":[{\"key\":\"p\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1}]}]}},\"sync_level\":\"full_index\"}", null));
        try std.testing.expect(loss.lost);
        // The unknown client response is observed, never replayed as a new
        // transform. Native intent recovery must retain the prepared postimage.
        try check(ctx);
    }
    fn check(ctx: *fixture.Context) !void {
        try awaitScalar(ctx, "SELECT n FROM plain_graph WHERE id='a'", 3);
        try awaitScalar(ctx, "SELECT n FROM plain_peer WHERE id='p'", 7);
        try plainEdges(ctx, &.{ "b", "c" });
        try noTriggerState(ctx, "plain_graph");
        try noTriggerState(ctx, "plain_peer");
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activateWithCatalog(ctx, true);
        _ = try fixture.waitServingSchema(ctx, "plain_graph", 0);
        _ = try fixture.waitServingSchema(ctx, "plain_peer", 0);
        try check(ctx);
    }
};
test "mounted coordinated graph rows use genuine trigger absence RLS atomicity lost replies and cold prepared image replay" {
    try fixture.run(AbsentGraph);
}
