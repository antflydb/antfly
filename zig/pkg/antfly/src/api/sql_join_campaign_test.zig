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
const Server = @import("http_server.zig").ApiHttpServer;
const Handler = @import("httpx_handler.zig").AntflyApiHandler;

test "httpx SQL PostgreSQL original joins preserve match scopes nulls and bags" {
    const ids = [_][]const u8{
        "sql-1320", "sql-1321", "sql-1322", "sql-1323", "sql-1324", "sql-1325",
        "sql-1326", "sql-1327", "sql-1328", "sql-1329", "sql-1330", "sql-1331",
        "sql-1332", "sql-1334", "sql-1335", "sql-1336", "sql-1338", "sql-1339",
        "sql-1341", "sql-1342", "sql-1343", "sql-1344",
    };
    const a = std.testing.allocator;
    const reference = local.sql_parity_fixtures.join_read_reference;
    const Row = struct { key: []const u8, value: std.json.Value };
    const Table = struct { name: []const u8, schema: std.json.Value, rows: []const Row };
    var parsed = try std.json.parseFromSlice(struct {
        format: u8,
        reference: []const u8,
        profile: struct { schema: std.json.Value, rows: []const Row, additional_tables: []const Table },
        entries: []const struct { id: []const u8 },
        excluded: []const std.json.Value,
    }, a, reference, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    try std.testing.expectEqual(@as(u8, 2), parsed.value.format);
    try std.testing.expectEqualStrings("PostgreSQL exact SQL", parsed.value.reference);
    try std.testing.expectEqual(ids.len, parsed.value.entries.len);
    try std.testing.expectEqual(@as(usize, 0), parsed.value.excluded.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.value.profile.additional_tables.len);
    var directory = try local.common_test_directory.TestDirectory.init("sql-original-joins");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var source: @import("sql_parity_sources.zig").Tables(2) = .{ .records = undefined, .reads = undefined };
    var databases: [2]db.DB = undefined;
    var opened: usize = 0;
    defer for (databases[0..opened]) |*database| database.close();
    for ([_]Table{
        .{ .name = "usage_records", .schema = parsed.value.profile.schema, .rows = parsed.value.profile.rows },
        parsed.value.profile.additional_tables[0],
    }, &databases, &source.records, &source.reads, 0..) |table, *database, *record, *read, i| {
        const path = try std.fmt.allocPrint(scratch, "{s}/{s}", .{ directory.path(), table.name });
        database.* = try db.DB.open(a, path, .{ .start_optional_runtimes = false, .start_index_workers = false });
        opened += 1;
        const schema = try std.json.Stringify.valueAlloc(scratch, table.schema, .{});
        try database.setSchemaJson(a, schema);
        const writes = try scratch.alloc(db.types.BatchWrite, table.rows.len);
        for (table.rows, writes) |row, *write| write.* = .{ .key = row.key, .value = try std.json.Stringify.valueAlloc(scratch, row.value, .{}) };
        try database.batch(.{ .writes = writes, .timestamp_ns = 42 });
        const id: u64 = @intCast(7 + i);
        record.* = .{ .table_id = id, .name = table.name, .schema_json = schema };
        read.* = reads.BoundTableReadSource.init(table.name, id, database, raft.read_gate.alreadyReadSafeBarrier());
    }
    var runtime = try db.background_runtime.BackendRuntimeHandle.init(a, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var server = Server.init(a, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &source, .vtable = &.{ .status = @TypeOf(source).status, .system_catalog = @TypeOf(source).systemCatalog, .supports_query_definitions = true } }, source.source(), null);
    defer server.deinit();
    var handler: Handler = .{ .api_server = &server };
    // Inputs are immutable throughout this local qualification. The retained
    // native capture proves execution/ownership, not distributed snapshot or
    // concurrent schema-publication correctness.
    try @import("sql_parity_reference.zig").runReferenceStrict(a, &handler, &ids, reference);
    try std.testing.expectEqual(ids.len, source.captures);
    try std.testing.expectEqual(@as(usize, 0), source.lookup_calls.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), source.unbounded_reads.load(.monotonic));
}
