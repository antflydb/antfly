// Copyright 2026 Antfly, Inc.
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
const antfly = @import("antfly-zig");
const lite_sql = @import("cmd/lite_sql.zig");
const metadata_table_manager = antfly.metadata.table_manager;
const internal_keys = antfly.internal_keys;
const relational_rows_api = antfly.public_api.relational_rows;
const runtime_schema_api = antfly.schema;
const schema_api = antfly.table_schema;

test {
    _ = @import("cmd/lite.zig");
    _ = @import("cmd/cli/backup.zig");
}

test "lite sql applies session plans before later logical planning" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-session-plan-root", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try antfly.db.DB.open(allocator, path, .{});
    defer db.close();

    var session = try lite_sql.Session.init(allocator, .{});
    defer session.deinit(allocator);

    const set_body = try lite_sql.executeOneJsonAlloc(allocator, &db, &session, "SET search_path TO tenant_schema;");
    defer allocator.free(set_body);
    var parsed_set = try std.json.parseFromSlice(std.json.Value, allocator, set_body, .{ .allocate = .alloc_always });
    defer parsed_set.deinit();
    try std.testing.expectEqualStrings("session", parsed_set.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("session", parsed_set.value.object.get("statement_kind").?.string);
    try std.testing.expectEqualStrings("tenant_schema", session.catalog.search_path[0]);

    const ddl_body = try lite_sql.executeOneJsonAlloc(allocator, &db, &session, "CREATE TABLE usage_records (id text PRIMARY KEY, status text);");
    defer allocator.free(ddl_body);
    var parsed_ddl = try std.json.parseFromSlice(std.json.Value, allocator, ddl_body, .{ .allocate = .alloc_always });
    defer parsed_ddl.deinit();
    try std.testing.expectEqualStrings("ddl", parsed_ddl.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("table_ddl", parsed_ddl.value.object.get("statement_kind").?.string);

    const table_record = (try db.getLiteSqlTableRecordAlloc(allocator)) orelse return error.TestUnexpectedResult;
    defer metadata_table_manager.freeTable(allocator, table_record);
    try std.testing.expectEqualStrings("tenant_schema", table_record.namespace_name);
}

test "lite sql executes insert values scalar subqueries through literal insert source rows" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-insert-values-scalar-root", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try antfly.db.DB.open(allocator, path, .{});
    defer db.close();

    var session = try lite_sql.Session.init(allocator, .{});
    defer session.deinit(allocator);

    const ddl_body = try lite_sql.executeOneJsonAlloc(allocator, &db, &session, "CREATE TABLE usage_records (id text PRIMARY KEY, status text, organization_id text);");
    defer allocator.free(ddl_body);

    const seed_one = try lite_sql.executeOneJsonAlloc(allocator, &db, &session, "INSERT INTO usage_records (id, status, organization_id) VALUES ('u1', 'queued', 'o1');");
    defer allocator.free(seed_one);
    const seed_two = try lite_sql.executeOneJsonAlloc(allocator, &db, &session, "INSERT INTO usage_records (id, status, organization_id) VALUES ('u2', 'ready', 'o1');");
    defer allocator.free(seed_two);

    const body = try lite_sql.executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "INSERT INTO usage_records (id, status, organization_id) VALUES ('u3', (SELECT status FROM usage_records WHERE id = 'u1'), 'o2') RETURNING id, status, organization_id;",
    );
    defer allocator.free(body);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("write", parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("insert_source", parsed.value.object.get("statement_kind").?.string);
    const result = parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), result.get("inserted").?.integer);
    const returning = result.get("returning").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), returning.len);
    const row = returning[0].object;
    try std.testing.expectEqualStrings("u3", row.get("id").?.string);
    try std.testing.expectEqualStrings("queued", row.get("status").?.string);
    try std.testing.expectEqualStrings("o2", row.get("organization_id").?.string);

    try std.testing.expectError(
        error.InvalidRowsRequest,
        lite_sql.executeOneJsonAlloc(
            allocator,
            &db,
            &session,
            "INSERT INTO usage_records (id, status, organization_id) VALUES ('u4', (SELECT status FROM usage_records WHERE organization_id = 'o1'), 'o2') RETURNING id;",
        ),
    );
}

test "lite sql resolves scalar subquery defaults through local row reads" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-scalar-default-root", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try antfly.db.DB.open(allocator, path, .{});
    defer db.close();

    var session = try lite_sql.Session.init(allocator, .{});
    defer session.deinit(allocator);

    const ddl_body = try lite_sql.executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "CREATE TABLE usage_records (id text PRIMARY KEY, status text DEFAULT (SELECT status FROM usage_records ORDER BY id));",
    );
    defer allocator.free(ddl_body);

    const seed = try lite_sql.executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "INSERT INTO usage_records (id, status) VALUES ('u1', 'queued');",
    );
    defer allocator.free(seed);

    const defaulted = try lite_sql.executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "INSERT INTO usage_records (id) VALUES ('u2') RETURNING id, status;",
    );
    defer allocator.free(defaulted);
    var parsed = try std.json.parseFromSlice(std.json.Value, allocator, defaulted, .{ .allocate = .alloc_always });
    defer parsed.deinit();
    try std.testing.expectEqualStrings("write", parsed.value.object.get("kind").?.string);
    try std.testing.expectEqualStrings("insert", parsed.value.object.get("statement_kind").?.string);
    const result = parsed.value.object.get("result").?.object;
    try std.testing.expectEqual(@as(i64, 1), result.get("inserted").?.integer);
    const returning = result.get("returning").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), returning.len);
    const row = returning[0].object;
    try std.testing.expectEqualStrings("u2", row.get("id").?.string);
    try std.testing.expectEqualStrings("queued", row.get("status").?.string);

    try std.testing.expectError(
        error.InvalidRowsRequest,
        lite_sql.executeOneJsonAlloc(
            allocator,
            &db,
            &session,
            "INSERT INTO usage_records (id) VALUES ('u3');",
        ),
    );
}

test "lite sql local ordered tuple index backs relational row queries" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/lite-sql-ordered-tuple-index-root", .{tmp.sub_path});
    defer allocator.free(path);

    var db = try antfly.db.DB.open(allocator, path, .{});
    defer db.close();

    var session = try lite_sql.Session.init(allocator, .{});
    defer session.deinit(allocator);

    const ddl_body = try lite_sql.executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "CREATE TABLE usage_records (id text PRIMARY KEY, tenant_id text, rank numeric);",
    );
    defer allocator.free(ddl_body);
    const index_body = try lite_sql.executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "CREATE INDEX usage_records_tenant_rank_idx ON usage_records (tenant_id, rank);",
    );
    defer allocator.free(index_body);

    const insert_body = try lite_sql.executeOneJsonAlloc(
        allocator,
        &db,
        &session,
        "INSERT INTO usage_records (id, tenant_id, rank) VALUES ('u1', 't1', 1), ('u2', 't1', 2), ('u3', 't1', 3), ('u4', 't2', 3);",
    );
    defer allocator.free(insert_body);

    const table_record = (try db.getLiteSqlTableRecordAlloc(allocator)) orelse return error.TestUnexpectedResult;
    defer metadata_table_manager.freeTable(allocator, table_record);
    var parsed_schema = try schema_api.parseValidatedTableSchema(allocator, table_record.schema_json);
    defer parsed_schema.deinit(allocator);
    const runtime_schema = try schema_api.deriveRuntimeTableSchema(allocator, parsed_schema);
    defer runtime_schema_api.freeSchema(allocator, runtime_schema);

    const row_jsons = [_][]const u8{
        "{\"id\":\"u1\",\"tenant_id\":\"t1\",\"rank\":1}",
        "{\"id\":\"u2\",\"tenant_id\":\"t1\",\"rank\":2}",
        "{\"id\":\"u3\",\"tenant_id\":\"t1\",\"rank\":3}",
        "{\"id\":\"u4\",\"tenant_id\":\"t2\",\"rank\":3}",
    };
    for (row_jsons) |row_json| {
        const doc_key = try relational_rows_api.physicalPrimaryKeyFromRowJsonAlloc(allocator, runtime_schema, row_json);
        defer allocator.free(doc_key);
        const tenant_index_key = try internal_keys.relationalColumnIndexKeyAlloc(allocator, "tenant_id", doc_key);
        defer allocator.free(tenant_index_key);
        db.core.store.delete(tenant_index_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        const rank_index_key = try internal_keys.relationalColumnIndexKeyAlloc(allocator, "rank", doc_key);
        defer allocator.free(rank_index_key);
        db.core.store.delete(rank_index_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    const predicates = [_]runtime_schema_api.RelationalCheck{
        .{ .name = "", .field = "tenant_id", .op = .eq, .value_json = "\"t1\"" },
        .{ .name = "", .field = "rank", .op = .gt, .value_json = "1" },
    };
    const select = [_][]const u8{"id"};
    const order_by = [_]antfly.db.types.RelationalRowsQueryOrder{.{
        .field = "id",
        .direction = .asc,
    }};
    var result = try db.queryRelationalRows(allocator, runtime_schema, .{
        .predicates = predicates[0..],
        .select = select[0..],
        .select_all = false,
        .order_by = order_by[0..],
    });
    defer result.deinit(allocator);

    try std.testing.expectEqual(antfly.db.types.RelationalRowsQueryResult.AccessMethod.ordered_tuple_doc_set, result.profile.access_method);
    try std.testing.expectEqual(@as(u32, 2), result.total);
    try std.testing.expectEqual(@as(usize, 2), result.rows.len);
    try std.testing.expectEqualStrings("{\"id\":\"u2\"}", result.rows[0]);
    try std.testing.expectEqualStrings("{\"id\":\"u3\"}", result.rows[1]);
}
