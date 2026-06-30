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
