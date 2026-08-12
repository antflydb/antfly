// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

const std = @import("std");
const db_mod = @import("../storage/db/mod.zig");
const lite_sql_db_source = @import("../storage/lite/sql_source.zig");
const metadata_table_manager = @import("../metadata/table_manager.zig");
const runtime = @import("lite_sql_runtime.zig");

fn executeOneJsonAlloc(
    allocator: std.mem.Allocator,
    db: *db_mod.DB,
    session: *runtime.Session,
    sql: []const u8,
) ![]u8 {
    var storage = lite_sql_db_source.DbSource.init(db);
    return try runtime.executeOneWithSourceJsonAlloc(allocator, storage.source(), session, sql);
}

test "lite sql reads legacy local schema metadata without table record" {
    const allocator = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"},"amount":{"type":"numeric"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-legacy", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try db_mod.DB.open(allocator, path, .{});
    defer db.close();
    try db.applyTableSchemaJson(allocator, schema_json, .{});
    try db.batch(.{
        .writes = &.{.{ .key = "row:a", .value = "{\"id\":\"row:a\",\"status\":\"open\",\"amount\":42}" }},
    });

    const stored_table_record = try db.getLiteSqlTableRecordAlloc(allocator);
    defer if (stored_table_record) |record| metadata_table_manager.freeTable(allocator, record);
    try std.testing.expect(stored_table_record == null);

    var session = try runtime.Session.init(allocator, .{});
    defer session.deinit(allocator);
    const body = try executeOneJsonAlloc(allocator, &db, &session, "SELECT id, amount FROM usage_records WHERE status = 'open';");
    defer allocator.free(body);

    try std.testing.expect(std.mem.indexOf(u8, body, "\"kind\":\"read\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"id\":\"row:a\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, body, "\"amount\":42") != null);
}

test "lite sql ddl updates catalog for subsequent statements" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-table-record", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try db_mod.DB.open(allocator, path, .{});
    defer db.close();

    var session = try runtime.Session.init(allocator, .{});
    defer session.deinit(allocator);

    const ddl_body = try executeOneJsonAlloc(allocator, &db, &session, "CREATE TABLE usage_records (id text PRIMARY KEY, status text);");
    defer allocator.free(ddl_body);
    var parsed_ddl = try std.json.parseFromSlice(std.json.Value, allocator, ddl_body, .{ .allocate = .alloc_always });
    defer parsed_ddl.deinit();
    try std.testing.expectEqualStrings("ddl", parsed_ddl.value.object.get("kind").?.string);

    const read_body = try executeOneJsonAlloc(allocator, &db, &session, "SELECT id, status FROM usage_records;");
    defer allocator.free(read_body);
    try std.testing.expect(std.mem.indexOf(u8, read_body, "\"kind\":\"read\"") != null);

    try std.testing.expectError(error.InvalidSqlCatalog, executeOneJsonAlloc(allocator, &db, &session, "SELECT id, status FROM other_records;"));
    try std.testing.expectError(error.InvalidSqlCatalog, executeOneJsonAlloc(allocator, &db, &session, "INSERT INTO other_records (id, status) VALUES ('row:a', 'open');"));
}

test "lite sql applies session plans before later logical planning" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-session-plan", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try db_mod.DB.open(allocator, path, .{});
    defer db.close();

    var session = try runtime.Session.init(allocator, .{});
    defer session.deinit(allocator);

    const set_body = try executeOneJsonAlloc(allocator, &db, &session, "SET search_path TO tenant_schema;");
    defer allocator.free(set_body);
    var parsed_set = try std.json.parseFromSlice(std.json.Value, allocator, set_body, .{ .allocate = .alloc_always });
    defer parsed_set.deinit();
    try std.testing.expectEqualStrings("session", parsed_set.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("session", parsed_set.value.object.get("statement_kind").?.string);
    try std.testing.expectEqualStrings("tenant_schema", session.catalog.search_path[0]);

    const ddl_body = try executeOneJsonAlloc(allocator, &db, &session, "CREATE TABLE usage_records (id text PRIMARY KEY, status text);");
    defer allocator.free(ddl_body);
    var parsed_ddl = try std.json.parseFromSlice(std.json.Value, allocator, ddl_body, .{ .allocate = .alloc_always });
    defer parsed_ddl.deinit();
    try std.testing.expectEqualStrings("ddl", parsed_ddl.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("table_ddl", parsed_ddl.value.object.get("statement_kind").?.string);

    const table_record = (try db.getLiteSqlTableRecordAlloc(allocator)) orelse return error.TestUnexpectedResult;
    defer metadata_table_manager.freeTable(allocator, table_record);
    try std.testing.expectEqualStrings("tenant_schema", table_record.namespace_name);
}

test "lite sql antfly query functions use native document query path" {
    const allocator = std.testing.allocator;
    const schema_json =
        \\{"version":1,"default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"}},"additionalProperties":true}}}}
    ;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-query-functions", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try db_mod.DB.open(allocator, path, .{});
    defer db.close();
    try db.applyLiteSqlTableRecord(allocator, .{
        .table_id = 1,
        .name = "docs",
        .database_name = "default",
        .namespace_name = "public",
        .placement_role = "data",
        .desired_replica_count = 1,
        .schema_json = schema_json,
        .indexes_json = "{\"full_text_index_v0\":{\"type\":\"full_text\",\"field\":\"title\"}}",
    });
    try db.addIndex(.{ .name = "full_text_index_v0", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"status\":\"active\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"status\":\"archived\"}" },
        },
        .sync_level = .full_index,
    });

    var session = try runtime.Session.init(allocator, .{});
    defer session.deinit(allocator);
    const body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT * FROM antfly.full_text_search(table_name => 'docs', index => 'full_text_index_v0', field => 'title', query => 'alpha', limit => 5);",
    );
    defer allocator.free(body);

    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("read", parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", parsed.value.object.get("statement_kind").?.string);
    const result = parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), result.get("total").?.integer);
    const row = result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", row.get("_source").?.object.get("title").?.string);

    const projected_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id, _score FROM antfly.full_text_search(table_name => 'docs', index => 'full_text_index_v0', field => 'title', query => 'alpha', limit => 5);",
    );
    defer allocator.free(projected_body);

    var projected = try std.json.parseFromSlice(std.json.Value, allocator, projected_body, .{ .allocate = .alloc_always });
    defer projected.deinit();
    const projected_result = projected.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), projected_result.get("total").?.integer);
    const projected_row = projected_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", projected_row.get("_id").?.string);
    try std.testing.expect(projected_row.get("_score") != null);
    try std.testing.expect(projected_row.get("_source") == null);
}

test "lite sql document table reads use typed document plan path" {
    const allocator = std.testing.allocator;
    const schema_json =
        \\{"version":1,"storage_mode":"document","default_type":"doc","document_schemas":{"doc":{"schema":{"type":"object","properties":{"title":{"type":"text"},"status":{"type":"keyword"},"amount":{"type":"numeric"},"note":{"type":"keyword"},"metadata":{"type":"json"},"tags":{"type":"array","items":{"type":"keyword"}}},"additionalProperties":true}}}}
    ;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-document-table", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try db_mod.DB.open(allocator, path, .{});
    defer db.close();

    var session = try runtime.Session.init(allocator, .{});
    defer session.deinit(allocator);

    try db.applyLiteSqlTableRecord(allocator, .{
        .table_id = 1,
        .name = "docs",
        .database_name = session.catalog.session().currentDatabase(),
        .namespace_name = session.catalog.session().primarySearchPathNamespace(),
        .placement_role = "data",
        .desired_replica_count = 1,
        .schema_json = schema_json,
        .indexes_json = "{\"typed_paths\":{\"keyword\":[\"metadata.plan\"]}}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"status\":\"active\",\"amount\":10,\"note\":null,\"metadata\":{\"plan\":\"pro\"},\"tags\":[\"urgent\",\"vip\"]}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"status\":\"archived\",\"amount\":20,\"metadata\":{\"plan\":\"free\"},\"tags\":[\"stale\"]}" },
        },
        .sync_level = .full_index,
    });

    const lookup_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id, title, metadata->>'plan' AS plan FROM docs WHERE _id = 'doc:a';",
    );
    defer allocator.free(lookup_body);
    var lookup = try std.json.parseFromSlice(std.json.Value, allocator, lookup_body, .{ .allocate = .alloc_always });
    defer lookup.deinit();
    try std.testing.expectEqualStrings("read", lookup.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("query", lookup.value.object.get("statement_kind").?.string);
    const lookup_result = lookup.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), lookup_result.get("total").?.integer);
    const lookup_row = lookup_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", lookup_row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", lookup_row.get("title").?.string);
    try std.testing.expectEqualStrings("pro", lookup_row.get("plan").?.string);

    const star_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT * FROM docs WHERE _id = 'doc:a';",
    );
    defer allocator.free(star_body);
    var star = try std.json.parseFromSlice(std.json.Value, allocator, star_body, .{ .allocate = .alloc_always });
    defer star.deinit();
    const star_result = star.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), star_result.get("total").?.integer);
    const star_row = star_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", star_row.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", star_row.get("title").?.string);
    try std.testing.expectEqualStrings("active", star_row.get("status").?.string);
    const star_doc = star_row.get("_doc").?.object;
    try std.testing.expectEqualStrings("alpha", star_doc.get("title").?.string);
    try std.testing.expectEqualStrings("pro", star_doc.get("metadata").?.object.get("plan").?.string);

    const bounded_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id, status FROM docs WHERE status = 'active' LIMIT 10;",
    );
    defer allocator.free(bounded_body);
    var bounded = try std.json.parseFromSlice(std.json.Value, allocator, bounded_body, .{ .allocate = .alloc_always });
    defer bounded.deinit();
    const bounded_result = bounded.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), bounded_result.get("total").?.integer);
    const bounded_row = bounded_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", bounded_row.get("_id").?.string);
    try std.testing.expectEqualStrings("active", bounded_row.get("status").?.string);

    const scalar_ops_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id, amount FROM docs WHERE status IN ('active', 'pending') AND title LIKE 'alp%' AND amount BETWEEN 5 AND 15 LIMIT 10;",
    );
    defer allocator.free(scalar_ops_body);
    var scalar_ops = try std.json.parseFromSlice(std.json.Value, allocator, scalar_ops_body, .{ .allocate = .alloc_always });
    defer scalar_ops.deinit();
    const scalar_ops_result = scalar_ops.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), scalar_ops_result.get("total").?.integer);
    const scalar_ops_row = scalar_ops_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", scalar_ops_row.get("_id").?.string);
    try std.testing.expectEqual(@as(i64, 10), scalar_ops_row.get("amount").?.integer);

    const null_predicate_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id FROM docs WHERE note IS NULL ORDER BY _id ASC LIMIT 10;",
    );
    defer allocator.free(null_predicate_body);
    var null_predicate = try std.json.parseFromSlice(std.json.Value, allocator, null_predicate_body, .{ .allocate = .alloc_always });
    defer null_predicate.deinit();
    const null_predicate_rows = null_predicate.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), null_predicate_rows.len);
    try std.testing.expectEqualStrings("doc:a", null_predicate_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("doc:b", null_predicate_rows[1].object.get("_id").?.string);

    const not_null_predicate_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id FROM docs WHERE status IS NOT NULL ORDER BY _id ASC LIMIT 10;",
    );
    defer allocator.free(not_null_predicate_body);
    var not_null_predicate = try std.json.parseFromSlice(std.json.Value, allocator, not_null_predicate_body, .{ .allocate = .alloc_always });
    defer not_null_predicate.deinit();
    const not_null_predicate_rows = not_null_predicate.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), not_null_predicate_rows.len);
    try std.testing.expectEqualStrings("doc:a", not_null_predicate_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("doc:b", not_null_predicate_rows[1].object.get("_id").?.string);

    const ordered_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT _id, title FROM docs ORDER BY title DESC LIMIT 2;",
    );
    defer allocator.free(ordered_body);
    var ordered = try std.json.parseFromSlice(std.json.Value, allocator, ordered_body, .{ .allocate = .alloc_always });
    defer ordered.deinit();
    const ordered_rows = ordered.value.object.get("result").?.object.get("rows").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), ordered_rows.len);
    try std.testing.expectEqualStrings("doc:b", ordered_rows[0].object.get("_id").?.string);
    try std.testing.expectEqualStrings("beta", ordered_rows[0].object.get("title").?.string);
    try std.testing.expectEqualStrings("doc:a", ordered_rows[1].object.get("_id").?.string);
    try std.testing.expectEqualStrings("alpha", ordered_rows[1].object.get("title").?.string);

    const unnest_body = try executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "SELECT d._id, tag FROM docs AS d, UNNEST(d.tags) AS tag WHERE tag = 'urgent' LIMIT 10;",
    );
    defer allocator.free(unnest_body);
    var unnest = try std.json.parseFromSlice(std.json.Value, allocator, unnest_body, .{ .allocate = .alloc_always });
    defer unnest.deinit();
    const unnest_result = unnest.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), unnest_result.get("total").?.integer);
    const unnest_row = unnest_result.get("rows").?.array.items[0].object;
    try std.testing.expectEqualStrings("doc:a", unnest_row.get("_id").?.string);
    try std.testing.expectEqualStrings("urgent", unnest_row.get("tag").?.string);

    try std.testing.expectError(
        error.DocumentSqlWriteUnsupported,
        executeOneJsonAlloc(allocator, &db, &session, "INSERT INTO docs (_id, _doc) VALUES ('doc:c', '{\"title\":\"gamma\"}');"),
    );
}
