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
const db = local.storage_db_selected_root.db;
const raft = @import("../raft/mod.zig");
const reads = @import("antfly_source_root").antfly_sources.table_reads;

test "httpx SQL PostgreSQL original text reads preserve escapes unicode and ordering" {
    const ids = [_][]const u8{ "sql-0190", "sql-0194", "sql-0210", "sql-0211", "sql-0212", "sql-0213", "sql-0217", "sql-0219", "sql-0252", "sql-0260", "sql-0280", "sql-0283", "sql-0285", "sql-0286", "sql-0287", "sql-0295", "sql-0296", "sql-1236", "sql-1374" };
    try runCampaign(&ids, local.sql_parity_fixtures.text_read_reference, "30", false);
}

test "httpx SQL PostgreSQL original numeric windows preserve exact averages and ordering" {
    try runCampaign(&.{ "sql-1366", "sql-1376" }, local.sql_parity_fixtures.numeric_window_reference, "72", true);
}

fn runCampaign(ids: []const []const u8, reference: []const u8, quantity_sum: ?[]const u8, exact_builtin_types: bool) !void {
    const a = std.testing.allocator;
    const Row = struct { key: []const u8, value: std.json.Value };
    var parsed = try std.json.parseFromSlice(struct {
        format: u8,
        reference: []const u8,
        exact_builtin_types: bool = false,
        profile: struct { schema: std.json.Value, rows: []const Row },
        entries: []const struct { id: []const u8 },
        excluded: []const std.json.Value,
    }, a, reference, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 2), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    try std.testing.expectEqual(exact_builtin_types, parsed.value.exact_builtin_types);
    try std.testing.expectEqual(ids.len, parsed.value.entries.len);
    try std.testing.expectEqual(@as(usize, 0), parsed.value.excluded.len);
    var directory = try local.common_test_directory.TestDirectory.init("sql-original-text");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var database = try db.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer database.close();
    const schema = try std.json.Stringify.valueAlloc(scratch, parsed.value.profile.schema, .{});
    try database.setSchemaJson(a, schema);
    const writes = try scratch.alloc(db.types.BatchWrite, parsed.value.profile.rows.len);
    for (parsed.value.profile.rows, writes) |row, *write| write.* = .{ .key = row.key, .value = try std.json.Stringify.valueAlloc(scratch, row.value, .{}) };
    try database.batch(.{ .writes = writes, .timestamp_ns = 42 });
    var source: @import("sql_parity_sources.zig").Tables(1) = .{
        .records = .{.{ .table_id = 7, .name = "usage_records", .schema_json = schema }},
        .reads = .{reads.BoundTableReadSource.init("usage_records", 7, &database, raft.read_gate.alreadyReadSafeBarrier())},
    };
    var runtime = try db.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var server = @import("http_server.zig").ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = @TypeOf(source).status, .system_catalog = @TypeOf(source).systemCatalog, .supports_query_definitions = true } }, source.source(), null);
    defer server.deinit();
    var handler: @import("httpx_handler.zig").AntflyApiHandler = .{ .api_server = &server };
    try @import("sql_parity_reference.zig").runReferenceDescribedTransportStrict(a, &handler, ids, reference);
    if (ids.len == 1 and std.mem.eql(u8, ids[0], "sql-0274")) try byteaBufferedQueries(a, &handler);
    // Single-table reads retain native views directly, not the multi-table
    // statement-vector fixture. Full key ranges are legitimate here; the
    // fixture's scan/query fallback callbacks still fail closed.
    try std.testing.expectEqual(@as(usize, 0), source.captures);
    // Binding also opens metadata-only schema views; an admitted native view
    // is not a data-page read and must not be counted as one.
    try std.testing.expect(source.native_reads.load(.monotonic) >= ids.len);
    try std.testing.expectEqual(@as(usize, 0), source.lookup_calls.load(.monotonic));
    // Exact NUMERIC assertions bypass the generic floating reference matcher.
    try exactSum(a, &handler, "SELECT sum(n) FROM (VALUES (9223372036854775807::bigint),(9223372036854775807::bigint)) v(n)", "18446744073709551614");
    try exactSum(a, &handler, "SELECT CASE WHEN TRUE THEN SUM(1::bigint) ELSE 0 END AS x UNION SELECT 1", "1");
    if (quantity_sum) |sum| try exactSum(a, &handler, "SELECT sum(quantity) FROM usage_records GROUP BY organization_id ORDER BY sum(quantity) DESC LIMIT 1", sum);
    try exactSum(a, &handler, "SELECT avg(n) FROM (VALUES (9007199254740993::bigint),(9007199254740994)) v(n)", "9007199254740993.5000");
}

fn exactSum(a: std.mem.Allocator, handler: anytype, statement: []const u8, expected: []const u8) !void {
    const httpx = @import("httpx");
    const wire = @import("antfly_metadata_openapi").types;
    var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
    defer request.deinit();
    const body = try std.json.Stringify.valueAlloc(a, .{ .statement = statement }, .{});
    defer a.free(body);
    request.body = body;
    var context = httpx.Context.init(a, std.testing.io, &request);
    defer context.deinit();
    var response = try handler.executeSQL(&context);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    const result = try std.json.parseFromSlice(wire.SQLResponse, a, response.body.?, .{});
    defer result.deinit();
    try std.testing.expectEqual(wire.SQLColumnType.number, result.value.columns[0].type);
    try std.testing.expectEqual(@as(?wire.SQLArrayElementType, .numeric), result.value.columns[0].element_type);
    try std.testing.expectEqual(@as(usize, 1), result.value.rows.len);
    try std.testing.expectEqualStrings(expected, result.value.rows[0][0].string);
}

test "httpx SQL PostgreSQL original bytea conversion preserves typed parameters and JSON filtering" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(struct { read: std.json.Value }, a, local.sql_parity_fixtures.bytea_reference, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    const read = try std.json.Stringify.valueAlloc(a, parsed.value.read, .{});
    defer a.free(read);
    try runCampaign(&.{"sql-0274"}, read, null, false);
}

fn byteaBufferedQueries(a: std.mem.Allocator, handler: anytype) !void {
    const httpx = @import("httpx");
    const wire = @import("antfly_metadata_openapi").types;
    const Json = std.json.Value;
    var fixture = try std.json.parseFromSlice(struct { queries: []const struct { sql: []const u8, rows: []const []const Json, oids: []const u32 } }, a, local.sql_parity_fixtures.bytea_reference, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    try std.testing.expectEqual(@as(usize, 5), fixture.value.queries.len);
    for (fixture.value.queries) |entry| {
        errdefer std.debug.print("binary buffered query={s}\n", .{entry.sql});
        var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
        defer request.deinit();
        const body = try std.json.Stringify.valueAlloc(a, .{ .statement = entry.sql }, .{});
        defer a.free(body);
        request.body = body;
        var context = httpx.Context.init(a, std.testing.io, &request);
        defer context.deinit();
        var response = try handler.executeSQL(&context);
        defer response.deinit();
        if (response.status.code != 200) std.debug.print("binary query status={d} body={s}\n", .{ response.status.code, response.body orelse "" });
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
        var result = try std.json.parseFromSlice(wire.SQLResponse, a, response.body.?, .{});
        defer result.deinit();
        try std.testing.expectEqual(entry.oids.len, result.value.columns.len);
        for (entry.oids, result.value.columns) |oid, column| try std.testing.expectEqual(if (oid == 17) wire.SQLColumnType.bytea else wire.SQLColumnType.integer, column.type);
        errdefer std.debug.print("binary buffered actual={s}\n", .{response.body.?});
        try std.testing.expectEqual(entry.rows.len, result.value.rows.len);
        for (entry.rows, result.value.rows) |expected, actual| {
            try std.testing.expectEqual(expected.len, actual.len);
            for (expected, actual) |left, right| try std.testing.expectEqual(std.math.Order.eq, try local.sql_scalar.compare(left, right));
        }
    }
}

test "httpx SQL PostgreSQL original phonetic filtering and UUID generation execute on stored rows" {
    const a = std.testing.allocator;
    const Json = std.json.Value;
    const httpx = @import("httpx");
    const wire = @import("antfly_metadata_openapi").types;
    const Profile = struct { id: []const u8, sql: []const u8, params: []const Json, table: []const u8, schema: Json, rows: []const struct { key: []const u8, value: Json }, oids: []const u32, expected_rows: ?[]const []const Json, row_count: usize };
    var fixture = try std.json.parseFromSlice(struct { profiles: []const Profile }, a, local.sql_parity_fixtures.phonetic_reference, .{ .ignore_unknown_fields = true });
    defer fixture.deinit();
    var corpus = try local.sql_parity_fixtures.Corpus.init(a);
    defer corpus.deinit();
    const ids = [_][]const u8{ "sql-0838", "sql-0178" };
    try std.testing.expectEqual(ids.len, fixture.value.profiles.len);
    for (ids, fixture.value.profiles) |id, profile| {
        try std.testing.expectEqualStrings(id, profile.id);
        const original = try corpus.get(id);
        try std.testing.expectEqualStrings(original.sql, profile.sql);
        const original_params = try std.json.Stringify.valueAlloc(a, original.params, .{});
        defer a.free(original_params);
        const witness_params = try std.json.Stringify.valueAlloc(a, profile.params, .{});
        defer a.free(witness_params);
        try std.testing.expectEqualStrings(original_params, witness_params);
        var directory = try local.common_test_directory.TestDirectory.init("sql-original-phonetic");
        defer directory.cleanup();
        var region: std.heap.ArenaAllocator = .init(a);
        defer region.deinit();
        const owned = region.allocator();
        var database = try db.DB.open(a, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
        defer database.close();
        const schema = try std.json.Stringify.valueAlloc(owned, profile.schema, .{});
        try database.setSchemaJson(a, schema);
        const writes = try owned.alloc(db.types.BatchWrite, profile.rows.len);
        for (profile.rows, writes) |row, *write| write.* = .{ .key = row.key, .value = try std.json.Stringify.valueAlloc(owned, row.value, .{}) };
        try database.batch(.{ .writes = writes, .timestamp_ns = 42 });
        var source: @import("sql_parity_sources.zig").Tables(1) = .{
            .records = .{.{ .table_id = 7, .name = profile.table, .schema_json = schema }},
            .reads = .{reads.BoundTableReadSource.init(profile.table, 7, &database, raft.read_gate.alreadyReadSafeBarrier())},
        };
        var runtime = try db.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
        defer runtime.deinit();
        var server = @import("http_server.zig").ApiHttpServer.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = @TypeOf(source).status, .system_catalog = @TypeOf(source).systemCatalog, .supports_query_definitions = true } }, source.source(), null);
        defer server.deinit();
        var handler: @import("httpx_handler.zig").AntflyApiHandler = .{ .api_server = &server };
        var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
        defer request.deinit();
        request.body = try std.json.Stringify.valueAlloc(owned, .{ .statement = original.sql, .parameters = try local.sql_parity_fixtures.Corpus.logicalParameters(owned, original) }, .{});
        var context = httpx.Context.init(a, std.testing.io, &request);
        defer context.deinit();
        var response = try handler.executeSQL(&context);
        defer response.deinit();
        if (response.status.code != 200) std.debug.print("phonetic original id={s} body={s}\n", .{ id, response.body orelse "" });
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
        var result = try std.json.parseFromSlice(wire.SQLResponse, a, response.body.?, .{});
        defer result.deinit();
        try std.testing.expectEqual(profile.oids.len, result.value.columns.len);
        for (profile.oids, result.value.columns) |oid, column| try std.testing.expect(@import("sql_parity_reference.zig").postgresColumnMatches(column, oid));
        try std.testing.expectEqual(profile.row_count, result.value.rows.len);
        try std.testing.expect(profile.row_count > 0);
        if (profile.expected_rows) |rows| {
            for (rows, result.value.rows) |expected, actual| for (expected, actual) |left, right| try std.testing.expectEqual(std.math.Order.eq, try local.sql_scalar.compare(left, right));
        } else {
            const value = result.value.rows[0][0].string;
            try std.testing.expectEqual(@as(usize, 36), value.len);
            try std.testing.expectEqual(@as(u8, '4'), value[14]);
            try std.testing.expect(std.mem.indexOfScalar(u8, "89ab", value[19]) != null);
        }
        try std.testing.expectEqual(@as(usize, 0), source.captures);
        try std.testing.expectEqual(@as(usize, 0), source.lookup_calls.load(.monotonic));
        try std.testing.expect(source.native_reads.load(.monotonic) > 0);
    }
}
