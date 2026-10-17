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

const std = @import("std");
const local = @import("antfly_local_sources");
const db_mod = local.storage_db_selected_root.db;
const metadata = @import("../metadata/api.zig");
const domain = local.system_catalog_domain;
const reads_mod = @import("table_reads.zig");
const server_mod = @import("http_server.zig");
const Handler = @import("httpx_handler.zig").AntflyApiHandler;
const httpx = @import("httpx");
const wire = @import("antfly_metadata_openapi").types;

const schema_json =
    \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"amount":{"type":"integer"}},"additionalProperties":true}}}}
;
const Source = struct {
    records: [1]local.common_topology_records.TableRecord = .{.{ .table_id = 7, .name = "docs", .schema_json = schema_json }},
    fn status(_: *anyopaque) !metadata.MetadataStatus {
        return .{ .metadata_group_id = 1, .metrics = .{} };
    }
    fn catalog(_: *anyopaque, a: std.mem.Allocator, context: local.api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
        try context.ensureActive();
        if (call == .policy_publication_status) return a.dupe(u8, "null");
        if (call == .write_validation) return std.json.Stringify.valueAlloc(a, .{ .schema_json = schema_json }, .{});
        if (call != .resolve_many) return error.UnexpectedCatalogCall;
        const input = call.resolve_many;
        if (input.expected_revision) |revision| if (revision != 7) return error.CatalogGenerationChanged;
        if (input.storage_names.len != 0) {
            if (input.storage_names.len != 1 or !std.mem.eql(u8, input.storage_names[0], "docs")) return error.UnexpectedCatalogCall;
            const name = try (domain.Target{ .table = "docs" }).resourceNameAlloc(a);
            defer a.free(name);
            return std.json.Stringify.valueAlloc(a, domain.ResolvedMany{ .revision = 7, .tables = &.{}, .logical_names = &.{name} }, .{});
        }
        const tables = try a.alloc(?domain.ResolvedTable, input.targets.len);
        defer a.free(tables);
        for (input.targets, tables) |target, *table| {
            if (!std.mem.eql(u8, target.table, "docs")) return error.UnexpectedCatalogCall;
            table.* = .{ .table_id = 7, .name = "docs", .query_definition = if (input.include_query_definitions) .{ .table_id = 7, .schema_json = schema_json, .read_schema_json = "", .indexes_json = "{}" } else null };
        }
        return std.json.Stringify.valueAlloc(a, domain.ResolvedMany{ .revision = 7, .tables = tables }, .{});
    }
    fn snapshot(raw: *anyopaque) !metadata.AdminSnapshot {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return .{ .status = try status(raw), .tables = &self.records, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
    }
    fn freeSnapshot(_: *anyopaque, _: *metadata.AdminSnapshot) void {}
};

fn execute(a: std.mem.Allocator, handler: *Handler, sql: []const u8, status: u16) !std.json.Parsed(wire.SQLResponse) {
    const body = try std.json.Stringify.valueAlloc(a, .{ .statement = sql }, .{});
    defer a.free(body);
    var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
    defer request.deinit();
    request.body = body;
    var context: httpx.Context = .init(a, std.testing.io, &request);
    defer context.deinit();
    var response = try handler.executeSQL(&context);
    defer response.deinit();
    if (response.status.code != status) std.debug.print("document root SQL {s}: status={d} body={s}\n", .{ sql, response.status.code, response.body orelse "" });
    try std.testing.expectEqual(status, response.status.code);
    return std.json.parseFromSlice(wire.SQLResponse, a, response.body.?, .{ .parse_numbers = false });
}

fn expectRoot(root: std.json.Value, title: []const u8) !void {
    try std.testing.expect(root == .object);
    try std.testing.expectEqual(@as(usize, 3), root.object.count());
    try std.testing.expectEqualStrings(title, root.object.get("title").?.string);
    try std.testing.expectEqualStrings("1", root.object.get("amount").?.number_string);
    const unknown = root.object.get("unknown").?;
    try std.testing.expectEqual(@as(usize, 1), unknown.object.count());
    try std.testing.expectEqualStrings("9007199254740993.1250", unknown.object.get("n").?.number_string);
}

test "httpx SQL document root originals prepare durable replacements and preserve rejected batches" {
    // Unchanged originals sql-0881 and sql-0882; other root/patch/version
    // originals remain open until their complete public contracts pass.
    const a = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("sql-document-root");
    defer directory.cleanup();
    var db = try db_mod.DB.open(a, directory.path(), .{});
    var db_live = true;
    defer if (db_live) db.close();
    try db.setSchemaJson(a, schema_json);
    var reads = reads_mod.BoundTableReadSource.init("docs", 7, &db, @import("../raft/mod.zig").read_gate.alreadyReadSafeBarrier());
    var writes = @import("antfly_source_root").antfly_sources.table_writes.BoundTableWriteSource.init("docs", &db);
    var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    var runtime_live = true;
    defer if (runtime_live) runtime.deinit();
    var source: Source = .{};
    var server = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.catalog, .admin_snapshot = Source.snapshot, .free_admin_snapshot = Source.freeSnapshot, .supports_query_definitions = true } }, reads.source(), writes.source());
    var server_live = true;
    defer if (server_live) server.deinit();
    var handler: Handler = .{ .api_server = &server };
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    for ([_][]const u8{ "sql-0881", "sql-0882" }) |id| {
        try db.batch(.{ .deletes = &.{"doc:a"} });
        const original = try corpus.get(id);
        var response = try execute(a, &handler, original.sql, 200);
        defer response.deinit();
        try std.testing.expectEqual(@as(i64, 1), response.value.rows_affected);
        if (std.mem.eql(u8, id, "sql-0882")) try std.testing.expectEqualStrings("doc:a", response.value.rows[0][0].string);
        const stored = (try db.get(a, "doc:a")).?;
        defer a.free(stored);
        var document = try std.json.parseFromSlice(std.json.Value, a, stored, .{});
        defer document.deinit();
        try std.testing.expectEqual(@as(usize, 0), document.value.object.count());
    }
    for ([_][]const u8{
        "UPDATE docs SET _doc='{\"title\":\"before\",\"amount\":1,\"unknown\":{\"n\":9007199254740993.1250}}' WHERE _id='doc:a' RETURNING _doc",
        "SELECT _doc FROM docs WHERE _id='doc:a'",
        "WITH c AS (SELECT _doc FROM docs) SELECT _doc FROM c",
        "INSERT INTO docs (_id,_doc) SELECT 'doc:b',_doc FROM docs WHERE _id='doc:a' RETURNING _doc",
        "UPDATE docs SET _doc=jsonb_set(_doc,'{title}','\"after\"',true) WHERE _id='doc:a' RETURNING _doc",
    }, 0..) |sql, ordinal| {
        var response = try execute(a, &handler, sql, 200);
        defer response.deinit();
        try std.testing.expectEqual(@as(usize, 1), response.value.rows.len);
        try expectRoot(response.value.rows[0][0], if (ordinal == 4) "after" else "before");
    }
    for ([_][]const u8{ "doc:a", "doc:b" }, [_][]const u8{ "after", "before" }) |key, title| {
        const stored = (try db.get(a, key)).?;
        defer a.free(stored);
        var document = try std.json.parseFromSlice(std.json.Value, a, stored, .{ .parse_numbers = false });
        defer document.deinit();
        try expectRoot(document.value, title);
    }
    const before = (try db.get(a, "doc:a")).?;
    defer a.free(before);
    var prior = (try db.lookup(a, "doc:a", .{ .include_primary_digest = true })).?;
    defer prior.deinit(a);
    try std.testing.expect(prior.version != null and prior.expected_content_digest != null);
    for ([_][]const u8{
        "INSERT INTO docs (_id,_doc) VALUES ('doc:c','{}'),('doc:d','[]')",
        "INSERT INTO docs (_id,_doc) VALUES ('doc:c','{}'),('doc:d','{\"amount\":\"wrong\"}')",
        "UPDATE docs SET _doc='{\"amount\":\"wrong\"}' WHERE _id='doc:a'",
    }) |sql| {
        const body = try std.json.Stringify.valueAlloc(a, .{ .statement = sql }, .{});
        defer a.free(body);
        var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
        defer request.deinit();
        request.body = body;
        var context: httpx.Context = .init(a, std.testing.io, &request);
        defer context.deinit();
        var response = try handler.executeSQL(&context);
        defer response.deinit();
        if (response.status.code != 400) std.debug.print("document root rejection {s}: {s}\n", .{ sql, response.body orelse "" });
        try std.testing.expectEqual(@as(u16, 400), response.status.code);
        try std.testing.expect((try db.get(a, "doc:c")) == null);
        try std.testing.expect((try db.get(a, "doc:d")) == null);
        const after = (try db.get(a, "doc:a")).?;
        defer a.free(after);
        try std.testing.expectEqualStrings(before, after);
        var current = (try db.lookup(a, "doc:a", .{ .include_primary_digest = true })).?;
        defer current.deinit(a);
        try std.testing.expectEqual(prior.version, current.version);
        try std.testing.expectEqual(prior.expected_content_digest, current.expected_content_digest);
    }
    server.deinit();
    server_live = false;
    runtime.deinit();
    runtime_live = false;
    db.close();
    db_live = false;
    var reopened = try db_mod.DB.open(a, directory.path(), .{});
    defer reopened.close();
    for ([_][]const u8{ "doc:a", "doc:b" }, [_][]const u8{ "after", "before" }) |key, title| {
        const stored = (try reopened.get(a, key)).?;
        defer a.free(stored);
        var document = try std.json.parseFromSlice(std.json.Value, a, stored, .{ .parse_numbers = false });
        defer document.deinit();
        try expectRoot(document.value, title);
    }
    var cold = (try reopened.lookup(a, "doc:a", .{ .include_primary_digest = true })).?;
    defer cold.deinit(a);
    try std.testing.expectEqual(prior.version, cold.version);
    try std.testing.expectEqual(prior.expected_content_digest, cold.expected_content_digest);
}
