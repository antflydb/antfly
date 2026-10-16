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
const db = local.storage_db_selected_root.db;
const raft = @import("../raft/mod.zig");
const reads = @import("antfly_source_root").antfly_sources.table_reads;

test "httpx SQL PostgreSQL original text reads preserve escapes unicode and ordering" {
    const ids = [_][]const u8{ "sql-0190", "sql-0194", "sql-0210", "sql-0211", "sql-0212", "sql-0213", "sql-0217", "sql-0219", "sql-0252", "sql-0260", "sql-0280", "sql-0283", "sql-0285", "sql-0286", "sql-0287", "sql-0295", "sql-0296", "sql-1236", "sql-1374" };
    const a = std.testing.allocator;
    const reference = local.sql_parity_fixtures.text_read_reference;
    const Row = struct { key: []const u8, value: std.json.Value };
    var parsed = try std.json.parseFromSlice(struct {
        format: u8,
        reference: []const u8,
        profile: struct { schema: std.json.Value, rows: []const Row },
        entries: []const struct { id: []const u8 },
        excluded: []const std.json.Value,
    }, a, reference, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 2), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
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
    try @import("sql_parity_reference.zig").runReferenceDescribedTransportStrict(a, &handler, &ids, reference);
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
    try exactSum(a, &handler, "SELECT sum(quantity) FROM usage_records GROUP BY organization_id ORDER BY sum(quantity) DESC LIMIT 1", "30");
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
