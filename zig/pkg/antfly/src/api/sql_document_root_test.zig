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

test "httpx SQL original PostgreSQL mutation syntax errors never access storage" {
    const ids = [_][]const u8{
        "sql-0573",
        "sql-0574",
        "sql-0575",
        "sql-0596",
        "sql-0597",
        "sql-0603",
        "sql-0604",
        "sql-0605",
        "sql-0613",
        "sql-0614",
        "sql-0615",
        "sql-0621",
        "sql-0622",
        "sql-0624",
        "sql-0625",
        "sql-0626",
        "sql-0627",
        "sql-0628",
        "sql-0629",
        "sql-0630",
        "sql-0631",
        "sql-0632",
        "sql-0633",
        "sql-0634",
        "sql-0635",
        "sql-0636",
        "sql-0637",
        "sql-0638",
        "sql-0639",
        "sql-0640",
        "sql-0643",
        "sql-0644",
        "sql-0645",
        "sql-0646",
        "sql-0647",
        "sql-0648",
        "sql-0649",
        "sql-0650",
        "sql-0651",
        "sql-0652",
        "sql-0653",
        "sql-0654",
        "sql-0655",
        "sql-0656",
        "sql-0658",
        "sql-0659",
        "sql-0660",
        "sql-0661",
        "sql-0662",
        "sql-0663",
        "sql-0664",
        "sql-0665",
        "sql-0666",
        "sql-0668",
        "sql-1523",
        "sql-1524",
        "sql-1525",
        "sql-1526",
        "sql-1527",
        "sql-1528",
        "sql-1529",
        "sql-1530",
        "sql-1534",
        "sql-1535",
        "sql-1536",
        "sql-1537",
        "sql-1538",
        "sql-1539",
        "sql-1540",
        "sql-1541",
        "sql-1542",
        "sql-1543",
        "sql-1544",
        "sql-1545",
        "sql-1546",
        "sql-1547",
        "sql-1548",
        "sql-1549",
        "sql-1550",
        "sql-1551",
        "sql-1552",
        "sql-1553",
        "sql-1554",
        "sql-1555",
        "sql-1556",
        "sql-1557",
        "sql-1558",
        "sql-1559",
        "sql-1560",
        "sql-1561",
        "sql-1562",
        "sql-1563",
        "sql-1565",
        "sql-1566",
        "sql-1567",
        "sql-1568",
        "sql-1569",
        "sql-1570",
        "sql-1571",
        "sql-1572",
        "sql-1573",
        "sql-1574",
        "sql-1575",
        "sql-1576",
        "sql-1577",
    };
    try expectClosedSyntax(&ids);
}

fn expectClosedSyntax(ids: []const []const u8) !void {
    return expectClosedDiagnostic(ids, "42601");
}

test "httpx SQL original routine definitions reject missing bodies before catalog access" {
    const ids = [_][]const u8{
        "sql-0156", "sql-0384", "sql-0385", "sql-0386", "sql-0387", "sql-0388", "sql-0389",
        "sql-0390", "sql-0391", "sql-0392", "sql-0393", "sql-0396", "sql-0413",
    };
    try expectClosedDiagnostic(&ids, "42P13");
    try expectClosedSyntax(&.{"sql-0395"});
}

fn expectClosedDiagnostic(ids: []const []const u8, code: []const u8) !void {
    const a = std.testing.allocator;
    const Closed = struct {
        calls: usize = 0,
        fn status(raw: *anyopaque) !metadata.MetadataStatus {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedStorageAccess;
        }
        fn catalog(raw: *anyopaque, _: std.mem.Allocator, _: local.api_operation.RequestContext, _: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedStorageAccess;
        }
    };
    var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var closed: Closed = .{};
    var server = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &closed, .vtable = &.{ .status = Closed.status, .system_catalog = Closed.catalog, .supports_query_definitions = true } }, null, null);
    defer server.deinit();
    var handler: Handler = .{ .api_server = &server };
    for (ids) |id| {
        errdefer std.debug.print("PostgreSQL syntax original {s}\\n", .{id});
        try @import("sql_parity_reference.zig").expectRejection(a, &handler, id, code);
        try std.testing.expectEqual(@as(usize, 0), closed.calls);
    }
}

test "httpx SQL original PostgreSQL invalid grammar rejects before catalog access" {
    const ids = [_][]const u8{
        "sql-0021",
        "sql-0022",
        "sql-0023",
        "sql-0024",
        "sql-0025",
        "sql-0026",
        "sql-0027",
        "sql-0028",
        "sql-0029",
        "sql-0030",
        "sql-0031",
        "sql-0032",
        "sql-0033",
        "sql-0067",
        "sql-0354",
        "sql-0355",
        "sql-0682",
        "sql-0684",
        "sql-0686",
        "sql-0687",
        "sql-0688",
        "sql-0689",
        "sql-0690",
        "sql-0691",
        "sql-0693",
        "sql-0694",
        "sql-0698",
        "sql-0846",
        "sql-0847",
        "sql-0848",
        "sql-0849",
        "sql-0850",
        "sql-0851",
        "sql-0852",
    };
    try expectClosedSyntax(&ids);
}

const schema_json =
    \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"keyword"},"amount":{"type":"integer"},"metadata":{"type":"object","nullable":true}},"additionalProperties":true}}}}
;
const Source = struct {
    schema: []const u8 = schema_json,
    records: [1]local.common_topology_records.TableRecord = .{.{ .table_id = 7, .name = "docs", .schema_json = schema_json }},
    fn status(_: *anyopaque) !metadata.MetadataStatus {
        return .{ .metadata_group_id = 1, .metrics = .{} };
    }
    fn catalog(raw: *anyopaque, a: std.mem.Allocator, context: local.api_operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        try context.ensureActive();
        if (call == .policy_publication_status) return a.dupe(u8, "null");
        if (call == .write_validation) return std.json.Stringify.valueAlloc(a, .{ .schema_json = self.schema }, .{});
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
            table.* = .{ .table_id = 7, .name = "docs", .query_definition = if (input.include_query_definitions) .{ .table_id = 7, .schema_json = self.schema, .read_schema_json = "", .indexes_json = "{}" } else null };
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
    // The HTTP response retires here; the returned parse must own even
    // unescaped labels and strings rather than borrow its body buffer.
    return std.json.parseFromSlice(wire.SQLResponse, a, response.body.?, .{ .parse_numbers = false, .allocate = .alloc_always });
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
    for ([_][]const u8{
        "INSERT INTO docs(_id,_doc) VALUES('doc:a','{\"title\":\"upserted\",\"amount\":1,\"unknown\":{\"n\":9007199254740993.1250}}') ON CONFLICT(_id) DO UPDATE SET _doc=excluded._doc RETURNING _doc",
        "UPDATE docs SET title='after' WHERE _id='doc:a' RETURNING _doc",
    }, [_][]const u8{ "upserted", "after" }) |sql, title| {
        var response = try execute(a, &handler, sql, 200);
        defer response.deinit();
        try std.testing.expectEqual(@as(i64, 1), response.value.rows_affected);
        try expectRoot(response.value.rows[0][0], title);
    }
    for ([_][]const u8{
        "INSERT INTO docs(_id,_doc) VALUES('doc:null','{\"title\":\"inserted\",\"metadata\":null}') RETURNING _doc,metadata",
        "INSERT INTO docs(_id,_doc) SELECT 'doc:null-source',_doc FROM docs WHERE _id='doc:null' RETURNING _doc,metadata",
        "UPDATE docs SET _doc=jsonb_set(_doc,'{title}','\"updated\"',true) WHERE _id='doc:null' RETURNING _doc,metadata",
        "UPDATE docs d SET _doc='{\"title\":\"joined\",\"metadata\":null}' FROM (VALUES('doc:null')) s(id) WHERE d._id=s.id RETURNING d._doc,d.metadata",
        "INSERT INTO docs(_id,_doc) VALUES('doc:null','{\"title\":\"conflicted\",\"metadata\":null}') ON CONFLICT(_id) DO UPDATE SET _doc=excluded._doc RETURNING _doc,metadata",
    }, [_][]const u8{ "inserted", "inserted", "updated", "joined", "conflicted" }, 0..) |sql, title, ordinal| {
        var response = try execute(a, &handler, sql, 200);
        defer response.deinit();
        try std.testing.expectEqual(@as(i64, 1), response.value.rows_affected);
        const row = response.value.rows[0];
        try std.testing.expectEqual(@as(usize, 2), row[0].object.count());
        try std.testing.expectEqualStrings(title, row[0].object.get("title").?.string);
        try std.testing.expect(row[0].object.get("metadata").? == .null and row[1] == .null);
        try std.testing.expect(!response.value.sql_nulls.?[0][1]);
        const stored = (try db.get(a, if (ordinal == 1) "doc:null-source" else "doc:null")).?;
        defer a.free(stored);
        var physical = try std.json.parseFromSlice(std.json.Value, a, stored, .{});
        defer physical.deinit();
        try std.testing.expect(physical.value.object.get("metadata").? == .null);
        try std.testing.expectEqual(@as(usize, 2), physical.value.object.count());
    }
    {
        // This local provider has point fences, not coordinated range guards.
        // The coordinated runtime fixture covers successful MERGE postimages.
        const prior_bytes = (try db.get(a, "doc:null")).?;
        defer a.free(prior_bytes);
        var prior_row = (try db.lookup(a, "doc:null", .{ .include_primary_digest = true })).?;
        defer prior_row.deinit(a);
        const merge_body = try std.json.Stringify.valueAlloc(a, .{ .statement = "MERGE INTO docs d USING (VALUES('doc:null')) s(id) ON d._id=s.id WHEN MATCHED THEN UPDATE SET _doc='{\"title\":\"unadmitted\",\"metadata\":null}' RETURNING d._doc" }, .{});
        defer a.free(merge_body);
        var merge_request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
        defer merge_request.deinit();
        merge_request.body = merge_body;
        var merge_context: httpx.Context = .init(a, std.testing.io, &merge_request);
        defer merge_context.deinit();
        var refused = try handler.executeSQL(&merge_context);
        defer refused.deinit();
        try std.testing.expectEqual(@as(u16, 501), refused.status.code);
        var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, a, refused.body.?, .{});
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("0A000", diagnostic.value.code);
        const after_bytes = (try db.get(a, "doc:null")).?;
        defer a.free(after_bytes);
        try std.testing.expectEqualStrings(prior_bytes, after_bytes);
        var after_row = (try db.lookup(a, "doc:null", .{ .include_primary_digest = true })).?;
        defer after_row.deinit(a);
        try std.testing.expectEqual(prior_row.version, after_row.version);
        try std.testing.expectEqual(prior_row.expected_content_digest, after_row.expected_content_digest);
        var skipped = try execute(a, &handler, "INSERT INTO docs(_id,_doc) VALUES('doc:null','{}') ON CONFLICT DO NOTHING RETURNING _doc", 200);
        defer skipped.deinit();
        try std.testing.expectEqual(@as(i64, 0), skipped.value.rows_affected);
        try std.testing.expectEqual(@as(usize, 0), skipped.value.rows.len);
        const retained = (try db.get(a, "doc:null")).?;
        defer a.free(retained);
        var original = try std.json.parseFromSlice(std.json.Value, a, retained, .{});
        defer original.deinit();
        try std.testing.expectEqual(@as(usize, 2), original.value.object.count());
        try std.testing.expectEqualStrings("conflicted", original.value.object.get("title").?.string);
        try std.testing.expect(original.value.object.get("metadata").? == .null);
        var response = try execute(a, &handler, "UPDATE docs SET metadata=NULL WHERE _id='doc:null-source' RETURNING metadata", 200);
        defer response.deinit();
        try std.testing.expect(response.value.sql_nulls.?[0][0]);
        const stored = (try db.get(a, "doc:null-source")).?;
        defer a.free(stored);
        var physical = try std.json.parseFromSlice(std.json.Value, a, stored, .{});
        defer physical.deinit();
        try std.testing.expect(!physical.value.object.contains("metadata"));
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

test "httpx SQL original document patches preserve complete native postimages and cold versions" {
    const ids = [_][]const u8{ "sql-0895", "sql-0896", "sql-0897", "sql-0898", "sql-0899", "sql-0900", "sql-0901", "sql-0902", "sql-0903", "sql-0904", "sql-0905", "sql-0906" };
    try runDocumentRootCampaign(local.sql_parity_fixtures.document_patch_reference, &ids);
}

test "httpx SQL original document root extensions preserve identities JSON and restart" {
    const ids = [_][]const u8{ "sql-0884", "sql-0885", "sql-0889", "sql-0894" };
    try runDocumentRootCampaign(local.sql_parity_fixtures.document_root_extension_reference, &ids);
}

test "httpx SQL original document envelope mutations retain opaque fields and durable versions" {
    const ids = [_][]const u8{ "sql-0907", "sql-0908", "sql-0914", "sql-0919", "sql-0931", "sql-0936", "sql-0937", "sql-0942", "sql-0944", "sql-0945", "sql-1068", "sql-1071", "sql-1078", "sql-1080" };
    try runDocumentRootCampaign(local.sql_parity_fixtures.document_envelope_reference, &ids);
}

fn runDocumentRootCampaign(bytes: []const u8, ids: []const []const u8) !void {
    const a = std.testing.allocator;
    const Seed = struct { key: []const u8, value: std.json.Value };
    var golden = try std.json.parseFromSlice(struct {
        seed_version: u64,
        seeds: []const Seed,
        entries: []const struct { id: []const u8, schema: std.json.Value, requires_native_session: bool = false, command_tag: []const u8 = "UPDATE", affected: i64, sqlstate: ?[]const u8 = null, untouched: ?[]const []const u8 = null, final: []const Seed, generated_final: []const std.json.Value = &.{}, columns: []const []const u8 = &.{}, column_oids: []const u32 = &.{}, rows: []const []const std.json.Value = &.{}, sql_nulls: []const []const bool = &.{} },
    }, a, bytes, .{ .ignore_unknown_fields = true });
    defer golden.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    var failures: usize = 0;
    var index: usize = 0;
    for (golden.value.entries) |entry| {
        // MERGE retains its real serializable admission barrier and runs in
        // the mounted native-session campaign, never in this bound writer.
        if (entry.requires_native_session) continue;
        if (index >= ids.len) return error.UnexpectedDocumentMutation;
        const id = ids[index];
        index += 1;
        try std.testing.expectEqualStrings(id, entry.id);
        runDocumentRootCase(a, &corpus, &golden, id, entry) catch |err| {
            std.debug.print("original document mutation {s} failed: {s}\n", .{ id, @errorName(err) });
            failures += 1;
        };
    }
    try std.testing.expectEqual(ids.len, index);
    if (failures != 0) return error.UnqualifiedDocumentMutations;
}

fn runDocumentRootCase(a: std.mem.Allocator, corpus: anytype, golden: anytype, id: []const u8, entry: anytype) !void {
    const schema = try std.json.Stringify.valueAlloc(a, entry.schema, .{});
    defer a.free(schema);
    var directory = try local.common_test_directory.TestDirectory.init("sql-document-patch");
    defer directory.cleanup();
    var db = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
    var db_open = true;
    defer if (db_open) db.close();
    try db.setSchemaJson(a, schema);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const writes = try arena.allocator().alloc(db_mod.types.BatchWrite, golden.value.seeds.len);
    for (golden.value.seeds, writes) |seed, *write| write.* = .{ .key = seed.key, .value = try std.json.Stringify.valueAlloc(arena.allocator(), seed.value, .{}) };
    try db.batch(.{ .writes = writes, .timestamp_ns = golden.value.seed_version });
    {
        var reads = reads_mod.BoundTableReadSource.init("docs", 7, &db, @import("../raft/mod.zig").read_gate.alreadyReadSafeBarrier());
        var writes_source = @import("antfly_source_root").antfly_sources.table_writes.BoundTableWriteSource.init("docs", &db);
        var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
        defer runtime.deinit();
        var source: Source = .{ .schema = schema, .records = .{.{ .table_id = 7, .name = "docs", .schema_json = schema }} };
        var server = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.catalog, .admin_snapshot = Source.snapshot, .free_admin_snapshot = Source.freeSnapshot, .supports_query_definitions = true } }, reads.source(), writes_source.source());
        defer server.deinit();
        var handler: Handler = .{ .api_server = &server };
        const original = try corpus.get(id);
        try std.testing.expectEqual(@as(usize, 0), original.params.len);
        if (entry.sqlstate) |code| {
            const body = try std.json.Stringify.valueAlloc(a, .{ .statement = original.sql }, .{});
            defer a.free(body);
            var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
            defer request.deinit();
            request.body = body;
            var context: httpx.Context = .init(a, std.testing.io, &request);
            defer context.deinit();
            var response = try handler.executeSQL(&context);
            defer response.deinit();
            try std.testing.expectEqual(@as(u16, 400), response.status.code);
            var diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, a, response.body.?, .{});
            defer diagnostic.deinit();
            try std.testing.expectEqualStrings(code, diagnostic.value.code);
        } else {
            var result = try execute(a, &handler, original.sql, 200);
            defer result.deinit();
            try std.testing.expectEqualStrings(entry.command_tag, result.value.command_tag);
            try std.testing.expectEqual(entry.affected, result.value.rows_affected);
            try std.testing.expectEqual(entry.columns.len, result.value.columns.len);
            for (result.value.columns, entry.columns, entry.column_oids) |column, name, oid| {
                try std.testing.expectEqualStrings(name, column.name);
                try std.testing.expect(@import("sql_parity_reference.zig").postgresColumnMatches(column, oid));
            }
            try std.testing.expectEqual(entry.rows.len, result.value.rows.len);
            const used = try arena.allocator().alloc(bool, entry.rows.len);
            @memset(used, false);
            for (result.value.rows, result.value.sql_nulls orelse if (result.value.rows.len == 0) &.{} else return error.MissingSqlNullBitmap) |row, flags| {
                const found = for (entry.rows, entry.sql_nulls, 0..) |want, want_flags, position| {
                    if (used[position]) continue;
                    const matches = for (row, flags, want, want_flags, result.value.columns) |cell, flag, expected, expected_flag, column| {
                        if (std.mem.eql(u8, column.name, "_version") and !flag and !expected_flag) {
                            const version = if (cell == .string) try std.fmt.parseUnsigned(u64, cell.string, 10) else @as(u64, @intCast(cell.integer));
                            if (version != @as(u64, @intCast(expected.integer))) break false;
                        } else if (!try @import("sql_parity_reference.zig").rowMatchesWithNulls(a, &.{column}, &.{cell}, &.{flag}, &.{expected}, &.{expected_flag})) break false;
                    } else true;
                    if (!matches) continue;
                    used[position] = true;
                    break true;
                } else false;
                try std.testing.expect(found);
            }
        }
        if (std.mem.eql(u8, id, "sql-0919")) {
            // The original exercises duplicate rejection. Also prove real
            // persisted NULL assignments through every shared write shape;
            // optional scalar absence must not erase opaque raw JSON nulls.
            const Probe = struct {
                fn absent(alloc: std.mem.Allocator, owner: *db_mod.DB, key: []const u8, keep_opaque: bool) !void {
                    var stored = (try owner.lookup(alloc, key, .{})) orelse return error.MissingNullAssignment;
                    defer stored.deinit(alloc);
                    var root = try std.json.parseFromSlice(std.json.Value, alloc, stored.json, .{});
                    defer root.deinit();
                    try std.testing.expect(!root.value.object.contains("title"));
                    if (keep_opaque) try std.testing.expect(root.value.object.get("opaque").? == .null);
                }
            };
            for ([_][]const u8{
                "INSERT INTO docs (_id,title) VALUES ('probe:null',NULL)",
                "INSERT INTO docs (_id,_doc) VALUES ('probe:target','{\"title\":\"before\",\"opaque\":null}')",
                "UPDATE docs SET title=NULL WHERE _id='probe:target'",
                "INSERT INTO docs (_id,title) SELECT 'probe:copy',title FROM docs WHERE _id='probe:null'",
            }) |statement| {
                var result = try execute(a, &handler, statement, 200);
                defer result.deinit();
                try std.testing.expectEqual(@as(i64, 1), result.value.rows_affected);
            }
            try Probe.absent(a, &db, "probe:null", false);
            try Probe.absent(a, &db, "probe:copy", false);
            try Probe.absent(a, &db, "probe:target", true);
            for ([_][]const u8{
                "UPDATE docs SET title=source.title FROM docs AS source WHERE docs._id='probe:target' AND source._id='probe:null'",
            }) |statement| {
                var reset = try execute(a, &handler, "UPDATE docs SET title='before' WHERE _id='probe:target'", 200);
                defer reset.deinit();
                var result = try execute(a, &handler, statement, 200);
                defer result.deinit();
                try std.testing.expectEqual(@as(i64, 1), result.value.rows_affected);
                try Probe.absent(a, &db, "probe:target", true);
            }
            var cleanup = try execute(a, &handler, "DELETE FROM docs WHERE _id IN ('probe:null','probe:target','probe:copy')", 200);
            defer cleanup.deinit();
            try std.testing.expectEqual(@as(i64, 3), cleanup.value.rows_affected);
        }
        if (std.mem.eql(u8, id, "sql-0896") or std.mem.eql(u8, id, "sql-0897") or
            std.mem.eql(u8, id, "sql-0907") or std.mem.eql(u8, id, "sql-0908") or
            std.mem.eql(u8, id, "sql-0905") or
            std.mem.eql(u8, id, "sql-1078") or std.mem.eql(u8, id, "sql-1080"))
        {
            // The original compares the actual physical seed version42.
            // Its successful commit retires that version: replay is now
            // a definite zero-effect predicate miss, not a second write.
            var stale = try execute(a, &handler, original.sql, 200);
            defer stale.deinit();
            try std.testing.expectEqual(@as(i64, 0), stale.value.rows_affected);
        }
        var visible = try execute(a, &handler, "SELECT _id,_version FROM docs ORDER BY _id", 200);
        defer visible.deinit();
        try std.testing.expectEqual(entry.final.len + entry.generated_final.len, visible.value.rows.len);
        for (visible.value.rows) |row| {
            var primary = (try db.lookup(a, row[0].string, .{ .include_primary_digest = true })) orelse return error.MissingVisibleDocument;
            defer primary.deinit(a);
            const version = if (row[1] == .string) try std.fmt.parseUnsigned(u64, row[1].string, 10) else @as(u64, @intCast(row[1].integer));
            try std.testing.expectEqual(primary.version orelse return error.MissingVisibleVersion, version);
        }
    }
    var postimage = try db.scan(arena.allocator(), "", "", .{ .include_documents = true, .limit = 4097 });
    defer postimage.deinit(arena.allocator());
    try std.testing.expectEqual(entry.final.len + entry.generated_final.len, postimage.documents.len);
    const versions = try arena.allocator().alloc(u64, postimage.documents.len);
    const digests = try arena.allocator().alloc([32]u8, postimage.documents.len);
    var generated: usize = 0;
    for (postimage.documents, versions, digests) |document, *version, *digest| {
        const want: @TypeOf(golden.value.seeds[0]) = for (entry.final) |known| {
            if (std.mem.eql(u8, known.key, document.id)) break known;
        } else unknown: {
            if (generated >= entry.generated_final.len) return error.UnexpectedDocumentIdentity;
            for (golden.value.seeds) |seed| try std.testing.expect(!std.mem.eql(u8, seed.key, document.id));
            try std.testing.expectEqual(@as(usize, 32), document.id.len);
            for (document.id) |digit| try std.testing.expect(std.ascii.isHex(digit));
            const value = entry.generated_final[generated];
            generated += 1;
            break :unknown .{ .key = document.id, .value = value };
        };
        var stored = (try db.lookup(a, want.key, .{ .include_primary_digest = true })) orelse return error.MissingDocumentPostimage;
        defer stored.deinit(a);
        var parsed = try std.json.parseFromSlice(std.json.Value, a, stored.json, .{});
        defer parsed.deinit();
        try std.testing.expect(try @import("sql_parity_reference.zig").rowMatchesWithNulls(a, &.{.{ .name = "_doc", .type = .json }}, &.{parsed.value}, &.{false}, &.{want.value}, &.{false}));
        version.* = stored.version orelse return error.MissingDocumentVersion;
        digest.* = stored.expected_content_digest orelse return error.MissingDocumentDigest;
        const untouched = if (entry.untouched) |keys| blk: {
            for (keys) |key| if (std.mem.eql(u8, key, want.key)) break :blk true;
            break :blk false;
        } else entry.affected == 0 or std.mem.eql(u8, want.key, "doc:c");
        if (untouched) try std.testing.expectEqual(golden.value.seed_version, version.*);
    }
    try std.testing.expectEqual(entry.generated_final.len, generated);
    db.close();
    db_open = false;
    var reopened = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .primary_backend = .{ .lsm = .{} } });
    defer reopened.close();
    var cold_rows = try reopened.scan(a, "", "", .{ .include_documents = true, .limit = 4097 });
    defer cold_rows.deinit(a);
    try std.testing.expectEqual(postimage.documents.len, cold_rows.documents.len);
    for (postimage.documents, versions, digests) |want, version, digest| {
        var stored = (try reopened.lookup(a, want.id, .{ .include_primary_digest = true })) orelse return error.MissingColdDocument;
        defer stored.deinit(a);
        try std.testing.expectEqual(version, stored.version orelse return error.MissingColdVersion);
        try std.testing.expectEqual(digest, stored.expected_content_digest orelse return error.MissingColdDigest);
        try std.testing.expectEqualStrings(want.json, stored.json);
    }
}

fn expectConflictDocument(a: std.mem.Allocator, bytes: []const u8, seeded: bool, title: []const u8, note: []const u8) !void {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{ .parse_numbers = false });
    defer parsed.deinit();
    const root = parsed.value;
    try std.testing.expectEqual(@as(usize, if (seeded) 5 else 1), root.object.count());
    try std.testing.expectEqualStrings(title, root.object.get("title").?.string);
    if (seeded) {
        try std.testing.expectEqualStrings("draft", root.object.get("status").?.string);
        try std.testing.expectEqualStrings(note, root.object.get("note").?.string);
        try std.testing.expectEqualStrings("1", root.object.get("amount").?.number_string);
        const unknown = root.object.get("unknown").?.object;
        try std.testing.expectEqual(@as(usize, 1), unknown.count());
        try std.testing.expectEqualStrings("9007199254740993.1250", unknown.get("n").?.number_string);
    }
}

test "httpx SQL document conflict originals preserve complete postimages skips and cold fences" {
    const a = std.testing.allocator;
    const Campaign = struct { entries: []const struct { id: []const u8, schema: std.json.Value } };
    var campaign = try std.json.parseFromSlice(Campaign, a, local.sql_parity_fixtures.document_campaign, .{ .ignore_unknown_fields = true });
    defer campaign.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    for ([_][]const u8{ "sql-0923", "sql-0924", "sql-0925", "sql-0926", "sql-0939", "sql-0940" }) |id| {
        const original = try corpus.get(id);
        try std.testing.expectEqual(@as(usize, 0), original.params.len);
        const schema = for (campaign.value.entries) |entry| {
            if (std.mem.eql(u8, entry.id, id)) break entry.schema;
        } else return error.MissingDocumentProfile;
        const encoded_schema = try std.json.Stringify.valueAlloc(a, schema, .{});
        defer a.free(encoded_schema);
        const source_case = std.mem.eql(u8, id, "sql-0939") or std.mem.eql(u8, id, "sql-0940");
        const runs: usize = if (source_case) 1 else 2;
        for (0..runs) |run| {
            const seeded = source_case or run == 1;
            var directory = try local.common_test_directory.TestDirectory.init("sql-document-conflict");
            defer directory.cleanup();
            var db = try db_mod.DB.open(a, directory.path(), .{});
            var db_live = true;
            defer if (db_live) db.close();
            try db.setSchemaJson(a, encoded_schema);
            if (seeded) try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"Before\",\"status\":\"draft\",\"amount\":1,\"note\":\"keep\",\"unknown\":{\"n\":9007199254740993.1250}}" }} });
            const before = try db.get(a, "doc:a");
            defer if (before) |bytes| a.free(bytes);
            var prior = try db.lookup(a, "doc:a", .{ .include_primary_digest = true });
            defer if (prior) |*value| value.deinit(a);
            var reads = reads_mod.BoundTableReadSource.init("docs", 7, &db, @import("../raft/mod.zig").read_gate.alreadyReadSafeBarrier());
            var writes = @import("antfly_source_root").antfly_sources.table_writes.BoundTableWriteSource.init("docs", &db);
            var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
            var runtime_live = true;
            defer if (runtime_live) runtime.deinit();
            var source: Source = .{ .schema = encoded_schema };
            source.records[0].schema_json = encoded_schema;
            var server = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.catalog, .admin_snapshot = Source.snapshot, .free_admin_snapshot = Source.freeSnapshot, .supports_query_definitions = true } }, reads.source(), writes.source());
            var server_live = true;
            defer if (server_live) server.deinit();
            var handler: Handler = .{ .api_server = &server };
            var response = try execute(a, &handler, original.sql, 200);
            defer response.deinit();
            const skipped = seeded and (std.mem.eql(u8, id, "sql-0924") or std.mem.eql(u8, id, "sql-0926") or std.mem.eql(u8, id, "sql-0939"));
            const returns = std.mem.eql(u8, id, "sql-0925") or std.mem.eql(u8, id, "sql-0926") or std.mem.eql(u8, id, "sql-0940");
            try std.testing.expectEqual(@as(i64, if (skipped) 0 else 1), response.value.rows_affected);
            try std.testing.expectEqual(@as(usize, if (returns and !skipped) 1 else 0), response.value.rows.len);
            const column_count: usize = if (!returns) 0 else if (std.mem.eql(u8, id, "sql-0926")) 1 else 2;
            try std.testing.expectEqual(column_count, response.value.columns.len);
            if (returns) {
                try std.testing.expectEqualStrings("_id", response.value.columns[0].name);
                for (response.value.columns) |field| try std.testing.expectEqual(wire.SQLColumnType.string, field.type);
                if (column_count == 2) try std.testing.expectEqualStrings(if (std.mem.eql(u8, id, "sql-0940")) "note" else "title", response.value.columns[1].name);
            }
            if (returns and !skipped) {
                const row = response.value.rows[0];
                for (response.value.sql_nulls.?[0]) |sql_null| try std.testing.expect(!sql_null);
                try std.testing.expectEqual(@as(usize, if (std.mem.eql(u8, id, "sql-0926")) 1 else 2), row.len);
                try std.testing.expectEqualStrings("doc:a", row[0].string);
                if (row.len == 2) try std.testing.expectEqualStrings(if (std.mem.eql(u8, id, "sql-0940")) "source-conflict" else "Launch", row[1].string);
            }
            const title = if (seeded and (skipped or source_case)) "Before" else "Launch";
            const note = if (std.mem.eql(u8, id, "sql-0940")) "source-conflict" else "keep";
            const stored = (try db.get(a, "doc:a")).?;
            defer a.free(stored);
            try expectConflictDocument(a, stored, seeded, title, note);
            var current = (try db.lookup(a, "doc:a", .{ .include_primary_digest = true })).?;
            defer current.deinit(a);
            try std.testing.expect(current.version != null and current.expected_content_digest != null);
            if (skipped) {
                try std.testing.expectEqualStrings(before.?, stored);
                try std.testing.expectEqual(prior.?.version, current.version);
                try std.testing.expectEqual(prior.?.expected_content_digest, current.expected_content_digest);
            }
            server.deinit();
            server_live = false;
            runtime.deinit();
            runtime_live = false;
            db.close();
            db_live = false;
            var reopened = try db_mod.DB.open(a, directory.path(), .{});
            defer reopened.close();
            const cold_bytes = (try reopened.get(a, "doc:a")).?;
            defer a.free(cold_bytes);
            try expectConflictDocument(a, cold_bytes, seeded, title, note);
            var cold = (try reopened.lookup(a, "doc:a", .{ .include_primary_digest = true })).?;
            defer cold.deinit(a);
            try std.testing.expectEqual(current.version, cold.version);
            try std.testing.expectEqual(current.expected_content_digest, cold.expected_content_digest);
        }
    }
}
