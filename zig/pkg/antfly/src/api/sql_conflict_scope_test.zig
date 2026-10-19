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
const raft = @import("../raft/mod.zig");
const reads = @import("antfly_source_root").antfly_sources.table_reads;
const writes = @import("antfly_source_root").antfly_sources.table_writes;
const Server = @import("http_server.zig").ApiHttpServer;
const Handler = @import("httpx_handler.zig").AntflyApiHandler;
const httpx = @import("httpx");
const wire = @import("antfly_metadata_openapi").types;

fn expectSuccess(a: std.mem.Allocator, handler: *Handler, statement: []const u8, id: []const u8, status: []const u8, quantity: i64) !void {
    const body = try std.json.Stringify.valueAlloc(a, .{ .statement = statement }, .{});
    defer a.free(body);
    var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
    defer request.deinit();
    request.body = body;
    var context = httpx.Context.init(a, std.testing.io, &request);
    defer context.deinit();
    var response = try handler.executeSQL(&context);
    defer response.deinit();
    if (response.status.code != 200) std.debug.print("conflict scope: {s}\n", .{response.body orelse ""});
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    const result = try std.json.parseFromSlice(wire.SQLResponse, a, response.body.?, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1), result.value.rows_affected);
    try std.testing.expectEqual(@as(usize, 1), result.value.rows.len);
    try std.testing.expectEqualStrings(id, result.value.rows[0][0].string);
    try std.testing.expectEqualStrings(status, result.value.rows[0][1].string);
    const expected_quantity = try std.fmt.allocPrint(a, "{d}", .{quantity});
    defer a.free(expected_quantity);
    // int8 uses the public lossless decimal-string representation.
    try std.testing.expectEqualStrings(expected_quantity, result.value.rows[0][2].string);
    try std.testing.expectEqualStrings("id", result.value.columns[0].name);
    try std.testing.expectEqualStrings("status", result.value.columns[1].name);
    try std.testing.expectEqualStrings("quantity", result.value.columns[2].name);
}

test "httpx SQL original conflict references reject ambiguity before reads and preserve qualified writes" {
    const a = std.testing.allocator;
    const Source = @import("sql_parity_sources.zig").Tables(1);
    var declaration = try local.sql_compiler.compile(a, "CREATE TABLE usage_records(id TEXT PRIMARY KEY,status TEXT DEFAULT 'initial',quantity BIGINT,amount BIGINT,enabled BOOLEAN)", .{});
    defer declaration.deinit();
    const schema = try local.sql_ddl_runtime.createSchemaAlloc(a, declaration.statement.create_table);
    defer a.free(schema);
    var directory = try local.common_test_directory.TestDirectory.init("sql-conflict-scope-originals");
    defer directory.cleanup();
    var db = try db_mod.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false, .identity_namespace = .{ .table_id = 7, .shard_id = 1, .range_id = 1 } });
    defer db.close();
    try db.setSchemaJson(a, schema);
    var ranges = [_]local.common_topology_records.RangeRecord{.{ .table_id = 7, .group_id = 7, .range_id = 1, .start_key = "", .doc_identity_shard_id = 1, .doc_identity_range_id = 1 }};
    var source: Source = .{
        .records = .{.{ .table_id = 7, .name = "usage_records", .schema_json = schema }},
        .reads = .{reads.BoundTableReadSource.init("usage_records", 7, &db, raft.read_gate.alreadyReadSafeBarrier())},
        .ranges = &ranges,
    };
    var writer = writes.BoundTableWriteSource.init("usage_records", &db);
    var runtime = try db_mod.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var server = Server.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = Source.status, .system_catalog = Source.systemCatalog, .admin_snapshot = Source.snapshot, .free_admin_snapshot = Source.freeSnapshot, .supports_query_definitions = true } }, source.source(), writer.source());
    defer server.deinit();
    var handler: Handler = .{ .api_server = &server };
    for ([_]bool{ false, true }) |seeded| {
        if (seeded) try expectSuccess(a, &handler, "INSERT INTO usage_records(id,status,quantity,amount,enabled) VALUES('u1','ready',1,5,true) RETURNING id,status,quantity", "u1", "ready", 1);
        var before = try db.scan(a, "", "", .{ .include_documents = true, .limit = 10 });
        defer before.deinit(a);
        var before_primary = if (seeded) try db.lookup(a, before.documents[0].id, .{ .include_primary_digest = true }) else null;
        defer if (before_primary) |*primary| primary.deinit(a);
        const native_reads = source.native_reads.load(.monotonic);
        const lookups = source.lookup_calls.load(.monotonic);
        for ([_][]const u8{ "sql-1388", "sql-1389", "sql-1391", "sql-1438", "sql-1444", "sql-1478", "sql-1479" }) |id| {
            errdefer std.debug.print("conflict scope original {s}, seeded={}\n", .{ id, seeded });
            try @import("sql_parity_reference.zig").expectRejection(a, &handler, id, "42702");
            var after = try db.scan(a, "", "", .{ .include_documents = true, .limit = 10 });
            defer after.deinit(a);
            try std.testing.expectEqual(before.documents.len, after.documents.len);
            for (before.documents, after.documents, before.hashes, after.hashes) |prior, current, prior_hash, current_hash| {
                try std.testing.expectEqualStrings(prior.id, current.id);
                try std.testing.expectEqualStrings(prior.json, current.json);
                try std.testing.expectEqualDeep(prior_hash, current_hash);
                var actual = (try db.lookup(a, prior.id, .{ .include_primary_digest = true })).?;
                defer actual.deinit(a);
                // The complete row bytes and its durable version must survive
                // rejection; a rewrite to equal content is not a no-op proof.
                try std.testing.expect(before_primary.?.version != null);
                try std.testing.expect(before_primary.?.expected_content_digest != null);
                try std.testing.expectEqual(before_primary.?.version, actual.version);
                try std.testing.expectEqual(before_primary.?.expected_content_digest, actual.expected_content_digest);
            }
        }
        try std.testing.expectEqual(native_reads, source.native_reads.load(.monotonic));
        try std.testing.expectEqual(lookups, source.lookup_calls.load(.monotonic));
    }
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const valid = try corpus.get("sql-1480");
    // Exact original alias-qualified predicate; RETURNING keeps target-only
    // unqualified names legal. Read back its values independently below.
    const body = try std.json.Stringify.valueAlloc(a, .{ .statement = valid.sql }, .{});
    defer a.free(body);
    var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
    defer request.deinit();
    request.body = body;
    var context = httpx.Context.init(a, std.testing.io, &request);
    defer context.deinit();
    var response = try handler.executeSQL(&context);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    const result = try std.json.parseFromSlice(wire.SQLResponse, a, response.body.?, .{});
    defer result.deinit();
    try std.testing.expectEqual(@as(i64, 1), result.value.rows_affected);
    try std.testing.expectEqualStrings("u1", result.value.rows[0][0].string);
    try std.testing.expectEqualStrings("ready", result.value.rows[0][1].string);
    try expectSuccess(a, &handler, "INSERT INTO usage_records AS u(id,status,quantity) VALUES('u1','open',2) ON CONFLICT(id) DO UPDATE SET quantity=u.quantity+excluded.quantity RETURNING id,status,quantity", "u1", "ready", 3);
    try expectSuccess(a, &handler, "INSERT INTO usage_records AS u(id,status,quantity) VALUES('u1','open',2) ON CONFLICT(id) DO UPDATE SET (status,quantity)=ROW(DEFAULT,u.quantity+excluded.quantity) RETURNING id,status,quantity", "u1", "initial", 5);
    var final = try db.scan(a, "", "", .{ .include_documents = true, .limit = 10 });
    defer final.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), final.documents.len);
    const value = try std.json.parseFromSlice(std.json.Value, a, final.documents[0].json, .{});
    defer value.deinit();
    try std.testing.expectEqualStrings("u1", value.value.object.get("id").?.string);
    try std.testing.expectEqualStrings("initial", value.value.object.get("status").?.string);
    try std.testing.expectEqual(@as(i64, 5), value.value.object.get("quantity").?.integer);
    try std.testing.expectEqual(@as(i64, 5), value.value.object.get("amount").?.integer);
    try std.testing.expect(value.value.object.get("enabled").?.bool);
    try std.testing.expectEqual(@as(usize, 5), value.value.object.count());
    var prior = (try db.lookup(a, final.documents[0].id, .{ .include_primary_digest = true })).?;
    defer prior.deinit(a);
    // The same exact original now reaches the false conflict predicate. It
    // returns no row and must not rewrite an unchanged primary or its version.
    var skipped_request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
    defer skipped_request.deinit();
    skipped_request.body = body;
    var skipped_context = httpx.Context.init(a, std.testing.io, &skipped_request);
    defer skipped_context.deinit();
    var skipped_response = try handler.executeSQL(&skipped_context);
    defer skipped_response.deinit();
    try std.testing.expectEqual(@as(u16, 200), skipped_response.status.code);
    const skipped = try std.json.parseFromSlice(wire.SQLResponse, a, skipped_response.body.?, .{});
    defer skipped.deinit();
    try std.testing.expectEqual(@as(i64, 0), skipped.value.rows_affected);
    try std.testing.expectEqual(@as(usize, 0), skipped.value.rows.len);
    var current = (try db.lookup(a, final.documents[0].id, .{ .include_primary_digest = true })).?;
    defer current.deinit(a);
    try std.testing.expect(prior.version != null and prior.expected_content_digest != null);
    try std.testing.expectEqual(prior.version, current.version);
    try std.testing.expectEqual(prior.expected_content_digest, current.expected_content_digest);
    try std.testing.expectEqualStrings(prior.json, current.json);
}
