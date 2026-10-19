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
const native = local.api_table_read_source;
const raft = @import("../raft/mod.zig");
const reads = @import("antfly_source_root").antfly_sources.table_reads;
const Server = @import("http_server.zig").ApiHttpServer;
const Handler = @import("httpx_handler.zig").AntflyApiHandler;
const httpx = @import("httpx");
const wire = @import("antfly_metadata_openapi").types;

test "httpx SQL original index names and stream-only COPY reject before catalog access" {
    const Guard = struct {
        calls: usize = 0,
        fn status(_: *anyopaque) !@import("../metadata/api.zig").MetadataStatus {
            return .{ .metadata_group_id = 1, .metrics = .{} };
        }
        fn catalogCall(raw: *anyopaque, _: std.mem.Allocator, _: local.api_operation.RequestContext, _: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedBackendRead;
        }
    };
    const a = std.testing.allocator;
    var guard: Guard = .{};
    var server = Server.init(a, .{}, .{ .ptr = &guard, .vtable = &.{ .status = Guard.status, .system_catalog = Guard.catalogCall, .supports_query_definitions = true } }, null, null);
    defer server.deinit();
    var handler: Handler = .{ .api_server = &server };
    for ([_][]const u8{ "sql-0708", "sql-0709" }) |id| try @import("sql_parity_reference.zig").expectRejection(a, &handler, id, "42601");
    for ([_][]const u8{ "COPY usage_records TO STDOUT", "COPY (SELECT 1 AS n) TO STDOUT" }) |statement| {
        const body = try std.json.Stringify.valueAlloc(a, .{ .statement = statement }, .{});
        defer a.free(body);
        var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
        defer request.deinit();
        request.body = body;
        var context: httpx.Context = .init(a, std.testing.io, &request);
        defer context.deinit();
        var response = try handler.executeSQL(&context);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 501), response.status.code);
        const diagnostic = try std.json.parseFromSlice(wire.SQLDiagnostic, a, response.body.?, .{});
        defer diagnostic.deinit();
        try std.testing.expectEqualStrings("0A000", diagnostic.value.code);
    }
    try std.testing.expectEqual(@as(usize, 0), guard.calls);
    // Qualified table ownership remains legal; only the new index name must
    // be unqualified. DROP INDEX may still address a qualified existing index.
    var control = try local.sql_compiler.compile(a, "CREATE INDEX status_idx ON public.usage_records(status)", .{});
    defer control.deinit();
    try std.testing.expectEqualStrings("public", control.statement.catalog_ddl.index_table.?.namespace.?);
}

test "httpx SQL original PostgreSQL name resolution rejects aliases before data reads" {
    try run(&.{
        "sql-1228", "sql-1233", "sql-1234", "sql-1235",
        "sql-1238", "sql-1239", "sql-1333", "sql-1582",
    });
}

const ReadGuard = struct {
    attempts: std.atomic.Value(usize) = .init(0),
    fn fail(ptr: *anyopaque) error{UnexpectedBackendRead} {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        _ = self.attempts.fetchAdd(1, .monotonic);
        return error.UnexpectedBackendRead;
    }
    fn lookup(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: db.types.LookupOptions, _: raft.ReadConsistency) !?native.LookupResponse {
        return fail(ptr);
    }
    fn openRead(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db.types.ScanOptions, _: raft.ReadConsistency) !?native.RelationalReadView {
        return fail(ptr);
    }
    fn openStatement(ptr: *anyopaque, _: std.mem.Allocator, _: []const native.RelationalStatementScan, _: raft.ReadConsistency) !native.RelationalStatementRead {
        return fail(ptr);
    }
    fn scan(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: []const u8, _: db.types.ScanOptions, _: raft.ReadConsistency) !?native.ScanResponse {
        return fail(ptr);
    }
    fn query(ptr: *anyopaque, _: std.mem.Allocator, _: []const u8, _: db.types.SearchRequest, _: raft.ReadConsistency) !?local.api_query.QueryResponse {
        return fail(ptr);
    }
    fn source(self: *@This()) native.TableReadSource {
        return .{ .ptr = self, .vtable = &.{ .lookup = lookup, .open_relational_read = openRead, .open_relational_statement = openStatement, .scan = scan, .query = query } };
    }
};

pub fn run(ids: []const []const u8) !void {
    const a = std.testing.allocator;
    const Row = struct { key: []const u8, value: std.json.Value };
    const Entry = struct { id: []const u8, sqlstate: []const u8, missing_column: []const u8 };
    var campaign = try std.json.parseFromSlice(struct {
        format: u8,
        entries: []const Entry,
        document_schema: std.json.Value,
        document_rows: []const Row,
        controls: []const struct { sql: []const u8, first_integer: []const u8 },
    }, a, local.sql_parity_fixtures.name_resolution_campaign, .{});
    defer campaign.deinit();
    try std.testing.expectEqual(@as(u8, 1), campaign.value.format);
    try std.testing.expectEqual(ids.len, campaign.value.entries.len);
    var profile = try std.json.parseFromSlice(struct { format: u8, schema: std.json.Value, rows: []const Row }, a, local.sql_parity_fixtures.read_campaign_profile, .{});
    defer profile.deinit();
    var directory = try local.common_test_directory.TestDirectory.init("sql-name-resolution");
    defer directory.cleanup();
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var source: @import("sql_parity_sources.zig").Tables(2) = .{ .records = undefined, .reads = undefined };
    var databases: [2]db.DB = undefined;
    var opened: usize = 0;
    defer for (databases[0..opened]) |*database| database.close();
    for ([_]struct { name: []const u8, schema: std.json.Value, rows: []const Row }{
        .{ .name = "usage_records", .schema = profile.value.schema, .rows = profile.value.rows },
        .{ .name = "docs", .schema = campaign.value.document_schema, .rows = campaign.value.document_rows },
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
    const status = @import("http_server.zig").StatusSource{ .ptr = &source, .vtable = &.{ .status = @TypeOf(source).status, .system_catalog = @TypeOf(source).systemCatalog, .supports_query_definitions = true } };
    var guard: ReadGuard = .{};
    var server = Server.init(a, .{ .backend_runtime = runtime.ptr() }, status, guard.source(), null);
    defer server.deinit();
    var handler: Handler = .{ .api_server = &server };
    for (ids, campaign.value.entries) |id, entry| {
        try std.testing.expectEqualStrings(id, entry.id);
        @import("sql_parity_reference.zig").expectRejection(a, &handler, id, entry.sqlstate) catch |err| {
            std.debug.print("original name-resolution case {s} failed\n", .{id});
            return err;
        };
        try std.testing.expectEqual(@as(usize, 0), guard.attempts.load(.monotonic));
    }
    // Valid bare labels and compound input references must remain distinct.
    // This second endpoint uses the actual pinned native owners, not ReadGuard.
    var valid_server = Server.init(a, .{ .backend_runtime = runtime.ptr() }, status, source.source(), null);
    defer valid_server.deinit();
    var valid_handler: Handler = .{ .api_server = &valid_server };
    try std.testing.expectEqual(@as(usize, 2), campaign.value.controls.len);
    for (campaign.value.controls) |control| {
        const body = try std.json.Stringify.valueAlloc(a, .{ .statement = control.sql }, .{});
        defer a.free(body);
        var request = try httpx.Request.init(a, .POST, "http://127.0.0.1/db/v1/sql");
        defer request.deinit();
        request.body = body;
        var context = httpx.Context.init(a, std.testing.io, &request);
        defer context.deinit();
        var response = try valid_handler.executeSQL(&context);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 200), response.status.code);
        var result = try std.json.parseFromSlice(wire.SQLResponse, a, response.body.?, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 1), result.value.rows.len);
        try std.testing.expectEqual(@as(usize, 1), result.value.columns.len);
        try std.testing.expectEqual(wire.SQLColumnType.integer, result.value.columns[0].type);
        try std.testing.expectEqualStrings("amount", result.value.columns[0].name);
        try std.testing.expectEqualStrings(control.first_integer, result.value.rows[0][0].string);
    }
}
