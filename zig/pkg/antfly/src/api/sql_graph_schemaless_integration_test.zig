// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Public default dynamic-document graph 2PC. The actual version-zero schema
//! is retained; trigger absence is proved without inventing an empty State.
//! Genuine schema-NONE images remain covered by storage-level owner tests.
const std = @import("std");
const fixture = @import("sql_owner_test_fixture.zig");
const client = @import("sql_owner_test_client.zig");
const http = @import("../raft/transport/http_common.zig");
const local = @import("antfly_local_sources");
const wire = @import("antfly_metadata_openapi").types;
const platform = @import("antfly_platform");
const A = std.mem.Allocator;

fn defaultDocument(ctx: *fixture.Context, name: []const u8) !u64 {
    var catalog = ctx.catalogClient();
    // Public CREATE publishes a logical name whose physical owner may have a
    // different storage name. Resolve its actual existing binding; a bare
    // logical name is never authority to fabricate a reverse-storage row.
    const request: local.system_catalog_domain.ResolveMany = .{ .targets = &.{.{ .table = name }}, .include_query_definitions = true };
    var response = try catalog.readSystemCatalog(ctx.metadata_uri, .{ .resolve_many = request }, 5000, null);
    defer response.deinit(ctx.alloc);
    var parsed = try std.json.parseFromSlice(local.system_catalog_domain.ResolvedMany, ctx.alloc, response.body, .{});
    defer parsed.deinit();
    try parsed.value.validateAuthority(ctx.alloc, request);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.tables.len);
    const record = parsed.value.tables[0] orelse return error.MissingDynamicDocumentTable;
    const definition = record.query_definition orelse return error.MissingDynamicDocumentDefinition;
    try std.testing.expectEqual(record.table_id, definition.table_id);
    var schema = try local.schema_mod.parseValidatedTableSchema(ctx.alloc, definition.schema_json);
    defer schema.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u32, 0), schema.version);
    try std.testing.expectEqual(.document, schema.storage_mode);
    try std.testing.expectEqualStrings("doc", schema.default_type);
    try std.testing.expect(!schema.enforce_types);
    try std.testing.expect(schema.document_schemas.len != 0);
    try std.testing.expect(schema.sql_triggers == null);
    var raw = try std.json.parseFromSlice(std.json.Value, ctx.alloc, definition.schema_json, .{});
    defer raw.deinit();
    try std.testing.expect(raw.value == .object);
    try std.testing.expect(raw.value.object.get("sql_triggers") == null);
    return record.table_id;
}
fn create(ctx: *fixture.Context, name: []const u8) !void {
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/tables/{s}", .{name});
    defer ctx.alloc.free(path);
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, wire.CreateTableRequest{}, .{ .emit_null_optional_fields = false });
    defer ctx.alloc.free(body);
    var admitted = try @import("table_contract.zig").parseCreateTableRequest(ctx.alloc, body);
    defer admitted.deinit(ctx.alloc);
    var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.metadata_uri, path, .POST, body);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("dynamic document create {s} status={d} body={s}\n", .{ name, response.status, response.body });
    try std.testing.expect(response.status == 200 or response.status == 201);
    try fixture.waitForDonorRoute(ctx.alloc, ctx.io, ctx.metadata, ctx.data, try defaultDocument(ctx, name));
}
fn batch(ctx: *fixture.Context, name: []const u8, source: []const u8) !void {
    var request = try std.json.parseFromSlice(wire.BatchRequest, ctx.alloc, source, .{});
    defer request.deinit();
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, request.value, .{});
    defer ctx.alloc.free(body);
    var response = try fixture.batch(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, name, body);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("dynamic document batch {s} status={d} body={s}\n", .{ name, response.status, response.body });
    try std.testing.expectEqual(@as(u16, 201), response.status);
}
fn multi(ctx: *fixture.Context, source: []const u8, expected: ?u16) !void {
    var request = try std.json.parseFromSlice(wire.MultiBatchRequest, ctx.alloc, source, .{});
    defer request.deinit();
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, request.value, .{});
    defer ctx.alloc.free(body);
    var response = try fixture.requestWithTimeout(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/batch", .POST, body, 30000);
    defer response.deinit(ctx.alloc);
    errdefer std.debug.print("dynamic document multi status={d} body={s}\n", .{ response.status, response.body });
    if (expected) |status| return std.testing.expectEqual(status, response.status);
    try std.testing.expect(response.status == 201 or response.status == 202);
    var result = try std.json.parseFromSlice(wire.MultiBatchResponse, ctx.alloc, response.body, .{});
    defer result.deinit();
    const status = result.value.status orelse return error.MissingDynamicDocumentDecision;
    // Preserve the actual decision/visibility classification before the
    // separate native graph read; never infer completion from HTTP success.
    std.debug.print("dynamic document graph decision http={d} status={s}\n", .{ response.status, status });
    try std.testing.expect(std.mem.startsWith(u8, status, "committed"));
    try std.testing.expect(!std.mem.eql(u8, status, "committed_repair_required"));
    // A stateless pending reply has no receipt identity to reconcile here.
    // Keep that observation explicitly unresolved, without retrying effects.
    if (response.status != 201 or !std.mem.eql(u8, status, "committed")) return error.DynamicDocumentCompletionReceiptRequired;
}
fn value(ctx: *fixture.Context, table: []const u8, key: []const u8, wanted: i64) !void {
    const path = try std.fmt.allocPrint(ctx.alloc, "/db/v1/tables/{s}/documents/{s}", .{ table, key });
    defer ctx.alloc.free(path);
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, path, .GET, null);
        defer response.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 200), response.status);
        var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        if (parsed.value == .object and parsed.value.object.get("n") != null and parsed.value.object.get("n").? == .integer and parsed.value.object.get("n").?.integer == wanted) return;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.DynamicDocumentVisibilityDeadline;
}
fn waitGraphReady(ctx: *fixture.Context) !void {
    const table_id = try defaultDocument(ctx, "opaque_graph");
    const deadline = platform.time.monotonicNs() +| 30 * std.time.ns_per_s;
    const Status = struct {
        config: struct { name: []const u8, type: []const u8 },
        status: struct {
            incarnation: []const u8 = "",
            target_revision: ?u64 = null,
            published_revision: ?u64 = null,
            readiness: struct { state: []const u8, queryable: bool, complete: bool, incarnation: []const u8 = "" },
        },
    };
    while (platform.time.monotonicNs() < deadline) {
        try ctx.checkDrivers();
        var response = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/tables/opaque_graph/indexes/g", .GET, null);
        defer response.deinit(ctx.alloc);
        if (response.status == 200) {
            var parsed = try std.json.parseFromSlice(Status, ctx.alloc, response.body, .{ .ignore_unknown_fields = true });
            defer parsed.deinit();
            const index = parsed.value;
            try std.testing.expectEqualStrings("g", index.config.name);
            try std.testing.expectEqualStrings("graph", index.config.type);
            if (std.mem.eql(u8, index.status.readiness.state, "failed")) return error.DynamicDocumentGraphBuildFailed;
            if (index.status.readiness.complete and index.status.readiness.queryable and std.mem.eql(u8, index.status.readiness.state, "ready")) {
                // Missing runtime observations legitimately omit these facts
                // while pending; a claimed ready index must prove all of them.
                if (index.status.incarnation.len == 0 or !std.mem.eql(u8, index.status.incarnation, index.status.readiness.incarnation)) return error.DynamicDocumentIndexIncarnationMissing;
                const target = index.status.target_revision orelse return error.DynamicDocumentIndexFrontierMissing;
                const published = index.status.published_revision orelse return error.DynamicDocumentIndexFrontierMissing;
                if (published < target) return error.DynamicDocumentIndexPublicationIncomplete;
                if (try defaultDocument(ctx, "opaque_graph") != table_id) return error.DynamicDocumentOwnerChanged;
                std.debug.print("dynamic document graph ready incarnation={s} target={d} published={d}\n", .{ index.status.incarnation, target, published });
                return;
            }
        } else if (response.status != 404 and response.status != 409 and response.status != 503 and response.status != 504) return error.DynamicDocumentIndexStatusUnavailable;
        try ctx.io.sleep(.fromMilliseconds(20), .awake);
    }
    return error.DynamicDocumentGraphBuildDeadline;
}

fn edges(ctx: *fixture.Context, wanted: []const []const u8) !void {
    const table_id = try defaultDocument(ctx, "opaque_graph");
    var captured = try ctx.metadata.server.svc.adminSnapshot();
    defer ctx.metadata.server.svc.freeAdminSnapshot(&captured);
    const group = for (captured.ranges) |range| {
        if (range.table_id == table_id) break range.group_id;
    } else return error.MissingDynamicDocumentRange;
    const table = for (captured.tables) |record| {
        if (record.table_id == table_id) break record.name;
    } else return error.MissingDynamicDocumentOwner;
    var envelope = try local.api_local_graph.graphEdgesTensorProgramEnvelopeAlloc(ctx.alloc, "g");
    defer envelope.deinit(ctx.alloc);
    const path = local.api_local_graph.algebraic_ir.graphEdgeAccessPath("g");
    const fragments = try ctx.alloc.alloc([]const u8, path.fragments.len);
    defer ctx.alloc.free(fragments);
    for (fragments, path.fragments) |*name, item| name.* = @tagName(item);
    const dimensions = try ctx.alloc.alloc([]const u8, path.output_dims.len);
    defer ctx.alloc.free(dimensions);
    for (dimensions, path.output_dims) |*name, item| name.* = @tagName(item);
    const laws = try ctx.alloc.alloc([]const u8, path.law_ids.len);
    defer ctx.alloc.free(laws);
    for (laws, path.law_ids) |*name, item| name.* = @tagName(item);
    var program = try envelope.asProgramAlloc(ctx.alloc);
    defer program.deinit(ctx.alloc);
    const bytes = try local.api_query_contract.encodeAlgebraicTensorProgramEnvelopeAlloc(ctx.alloc, program.program);
    defer ctx.alloc.free(bytes);
    var json = try std.json.parseFromSlice(std.json.Value, ctx.alloc, bytes, .{});
    defer json.deinit();
    const body = try std.json.Stringify.valueAlloc(ctx.alloc, local.api_local_graph.GraphEdgesRequestJson{ .index_name = "g", .key = "a", .direction = "out", .tensor_access_path = .{ .owner = path.owner, .layout = @tagName(path.layout), .fragments = fragments, .output_dims = dimensions, .law_ids = laws }, .tensor_program = json.value }, .{});
    defer ctx.alloc.free(body);
    var request = try local.api_local_graph.parseGraphEdgesRequest(ctx.alloc, body);
    defer request.deinit(ctx.alloc);
    const reader = ctx.data.http_server.?.table_reads orelse return error.MissingDynamicDocumentReader;
    var result = (try reader.graphEdgesGroupLocal(ctx.alloc, group, table, request, .read_index)) orelse return error.MissingDynamicDocumentGraph;
    defer result.deinit(ctx.alloc);
    try std.testing.expectEqual(wanted.len, result.edges.len);
    for (wanted) |key| {
        var found: usize = 0;
        for (result.edges) |edge| if (std.mem.eql(u8, key, edge.target)) {
            found += 1;
        };
        try std.testing.expectEqual(@as(usize, 1), found);
    }
}
fn visible(ctx: *fixture.Context, headers: []const http.RequestHeader, expected: ?bool) !void {
    var response = try fixture.request(ctx.alloc, ctx.transport, headers, ctx.base, "/db/v1/tables/opaque_graph/documents/a", .GET, null);
    defer response.deinit(ctx.alloc);
    try std.testing.expectEqual(@as(u16, if (expected == null) 404 else 200), response.status);
    if (expected) |wanted| {
        var parsed = try std.json.parseFromSlice(std.json.Value, ctx.alloc, response.body, .{});
        defer parsed.deinit();
        try std.testing.expectEqual(wanted, parsed.value.object.get("visible").?.bool);
    }
}
fn filteredReadHidden(ctx: *fixture.Context) !void {
    const users = @import("../usermgr/mod.zig");
    const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
    var permission = try users.Permission.initOwned(ctx.alloc, .table, "*", .read);
    defer permission.deinit(ctx.alloc);
    var key = try manager.createApiKey("pk-reader", "dynamic-document-filtered-reader", &.{permission}, &.{.{ .table = @constCast("opaque_graph"), .filter = @constCast("{\"term\":{\"visible\":true}}") }}, null);
    defer key.deinit(manager.alloc);
    const authorization = try std.fmt.allocPrint(ctx.alloc, "ApiKey {s}", .{key.encoded});
    defer ctx.alloc.free(authorization);
    try visible(ctx, &.{.{ .name = "authorization", .value = authorization }}, null);
}

const Lost = struct {
    inner: http.RequestExecutor,
    lost: bool = false,
    completed: bool = false,
    fn execute(raw: *anyopaque, a: A, request: http.HttpRequest) !http.HttpResponse {
        const self: *@This() = @ptrCast(@alignCast(raw));
        var response = try self.inner.execute(a, request);
        if (!self.lost and request.method == .POST and (response.status == 201 or response.status == 202) and std.mem.endsWith(u8, request.uri, "/db/v1/batch")) {
            self.lost = true;
            var decision = std.json.parseFromSlice(struct { status: []const u8 }, a, response.body, .{ .ignore_unknown_fields = true }) catch |err| {
                response.deinit(a);
                return err;
            };
            defer decision.deinit();
            self.completed = response.status == 201 and std.mem.eql(u8, decision.value.status, "committed");
            std.debug.print("lost dynamic document graph decision http={d} status={s}\n", .{ response.status, decision.value.status });
            response.deinit(a);
            return error.ConnectionResetByPeer;
        }
        return response;
    }
    fn executor(self: *@This()) http.RequestExecutor {
        return .{ .ptr = self, .vtable = &.{ .execute = execute }, .clock_io = self.inner.clock_io };
    }
};
const Campaign = struct {
    pub const native_auth = true;
    pub const cold_catalog = true;
    credentials: client.ApiKeyHeaders = .{},
    pub fn beforeRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        try create(ctx, "opaque_graph");
        try create(ctx, "opaque_peer");
        try batch(ctx, "opaque_graph", "{\"inserts\":{\"a\":{\"n\":1,\"visible\":true},\"b\":{\"n\":0},\"c\":{\"n\":0},\"d\":{\"n\":0}},\"sync_level\":\"full_index\"}");
        try batch(ctx, "opaque_peer", "{\"inserts\":{\"p\":{\"n\":5,\"visible\":true}},\"sync_level\":\"full_index\"}");
        var index = try fixture.request(ctx.alloc, ctx.transport, ctx.admin_headers, ctx.base, "/db/v1/tables/opaque_graph/indexes/g", .POST, "{\"name\":\"g\",\"type\":\"graph\",\"edge_types\":[{\"name\":\"R\"}]}");
        defer index.deinit(ctx.alloc);
        try std.testing.expectEqual(@as(u16, 201), index.status);
        // Creation admits a build over existing rows. Await its actual
        // owner/index readiness; never poll the edge result or replay writes.
        try waitGraphReady(ctx);
        try multi(ctx, "{\"tables\":{\"opaque_graph\":{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"b\"}}]}]},\"opaque_peer\":{\"transforms\":[{\"key\":\"p\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1}]}]}},\"sync_level\":\"full_index\"}", null);
        try value(ctx, "opaque_graph", "a", 2);
        try value(ctx, "opaque_peer", "p", 6);
        try edges(ctx, &.{"b"});
        const users = @import("../usermgr/mod.zig");
        const manager = ctx.metadata.server.owned_public_http_server.?.cfg.user_manager.?;
        var permission = try users.Permission.initOwned(ctx.alloc, .table, "*", .admin);
        defer permission.deinit(ctx.alloc);
        var key = try manager.createApiKey("pk-rewriter", "dynamic-document-filtered-writer", &.{permission}, &.{.{ .table = @constCast("opaque_graph"), .filter = @constCast("{\"term\":{\"visible\":true}}") }}, null);
        defer key.deinit(manager.alloc);
        const authorization = try std.fmt.allocPrint(ctx.alloc, "ApiKey {s}", .{key.encoded});
        defer ctx.alloc.free(authorization);
        const headers = [_]http.RequestHeader{.{ .name = "authorization", .value = authorization }};
        var restricted = ctx.*;
        restricted.admin_headers = &headers;
        // Credential query filters narrow reads; they are not SQL WITH CHECK.
        // This authorized write may leave the credential's visible row set.
        try multi(&restricted, "{\"tables\":{\"opaque_graph\":{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$set\",\"path\":\"visible\",\"value\":false},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"c\"}}]}]},\"opaque_peer\":{\"transforms\":[{\"key\":\"p\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1}]}]}},\"sync_level\":\"full_index\"}", null);
        try value(ctx, "opaque_graph", "a", 2);
        try value(ctx, "opaque_peer", "p", 7);
        try visible(ctx, ctx.admin_headers, false);
        try visible(ctx, &headers, null);
        try edges(ctx, &.{ "b", "c" });
        // A genuine missing peer-table write grant rejects the whole decision.
        // API-key minting checks the parent's exact action grant. Narrow
        // its wildcard admin grant to this table, retaining the same action;
        // the resulting key still has no authority over opaque_peer.
        var narrow = try users.Permission.initOwned(ctx.alloc, .table, "opaque_graph", .admin);
        defer narrow.deinit(ctx.alloc);
        var narrow_key = try manager.createApiKey("pk-rewriter", "dynamic-document-no-peer-write", &.{narrow}, &.{}, null);
        defer narrow_key.deinit(manager.alloc);
        const narrow_authorization = try std.fmt.allocPrint(ctx.alloc, "ApiKey {s}", .{narrow_key.encoded});
        defer ctx.alloc.free(narrow_authorization);
        const narrow_headers = [_]http.RequestHeader{.{ .name = "authorization", .value = narrow_authorization }};
        var denied = ctx.*;
        denied.admin_headers = &narrow_headers;
        try multi(&denied, "{\"tables\":{\"opaque_graph\":{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1}]}]},\"opaque_peer\":{\"transforms\":[{\"key\":\"p\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1}]}]}},\"sync_level\":\"full_index\"}", 403);
        try value(ctx, "opaque_graph", "a", 2);
        try value(ctx, "opaque_peer", "p", 7);
        try edges(ctx, &.{ "b", "c" });
        var loss: Lost = .{ .inner = ctx.transport };
        var uncertain = ctx.*;
        uncertain.transport = loss.executor();
        try std.testing.expectError(error.ConnectionResetByPeer, multi(&uncertain, "{\"tables\":{\"opaque_graph\":{\"transforms\":[{\"key\":\"a\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1},{\"op\":\"$push\",\"path\":\"_edges.g.R\",\"value\":{\"target\":\"d\"}}]}]},\"opaque_peer\":{\"transforms\":[{\"key\":\"p\",\"operations\":[{\"op\":\"$inc\",\"path\":\"n\",\"value\":1}]}]}},\"sync_level\":\"full_index\"}", null));
        try std.testing.expect(loss.lost);
        try std.testing.expect(loss.completed);
        try check(ctx);
    }
    fn check(ctx: *fixture.Context) !void {
        try value(ctx, "opaque_graph", "a", 3);
        try value(ctx, "opaque_peer", "p", 8);
        try visible(ctx, ctx.admin_headers, false);
        try filteredReadHidden(ctx);
        try edges(ctx, &.{ "b", "c", "d" });
        _ = try defaultDocument(ctx, "opaque_peer");
    }
    pub fn afterRestart(self: *@This(), ctx: *fixture.Context) !void {
        try self.credentials.activate(ctx);
        try check(ctx);
    }
};
test "mounted default dynamic document graph transactions coalesce once enforce credential filters and recover exact cold images" {
    try fixture.run(Campaign);
}
