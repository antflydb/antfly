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
const server_mod = @import("http_server.zig");
const Adapter = @import("sql_execution.zig").Adapter;
const domain = @import("../system_catalog/domain.zig");
const operation = @import("operation.zig");
const reads = @import("table_read_source.zig");
const read_view = @import("../storage/relational_read_view.zig").View;

const Fixture = struct {
    lake_schema: []const u8,
    native_opened: usize = 0,
    native_closed: usize = 0,
    row_returned: bool = false,
    policy_catalog_changed: bool = false,
    view: [1]reads.RelationalReadView = undefined,
    const native_schema = "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"enforce_types\":true,\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"amount\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}";
    fn catalog(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        if (call == .policy_publication_status) {
            if (self.policy_catalog_changed) return error.RowPolicyCatalogChanged;
            return alloc.dupe(u8, "null");
        }
        if (call != .resolve_many) return error.UnexpectedCatalogCall;
        const input = call.resolve_many;
        if (input.expected_revision) |revision| if (revision != 1) return error.CatalogGenerationChanged;
        const tables = try alloc.alloc(?domain.ResolvedTable, input.targets.len);
        defer alloc.free(tables);
        for (input.targets, tables) |target, *table| {
            const native = std.mem.eql(u8, target.table, "native");
            table.* = .{ .table_id = if (native) 8 else 7, .name = if (native) "native" else "events", .query_definition = if (input.include_query_definitions) .{ .table_id = if (native) 8 else 7, .schema_json = if (native) native_schema else self.lake_schema, .read_schema_json = "", .indexes_json = "{}" } else null };
        }
        return std.json.Stringify.valueAlloc(alloc, domain.ResolvedMany{ .revision = 1, .tables = tables }, .{});
    }
    fn openNative(raw: *anyopaque, _: std.mem.Allocator, scans: []const reads.RelationalStatementScan, _: @import("../raft/read_gate.zig").ReadConsistency) !reads.RelationalStatementRead {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        try std.testing.expectEqual(@as(usize, 1), scans.len);
        try std.testing.expectEqualStrings("native", scans[0].table);
        self.row_returned = false;
        self.native_opened += 1;
        self.view[0] = .{ .ptr = self, .vtable = &.{ .next = next, .close = closeView } };
        return .{ .ptr = self, .views = &self.view, .vtable = &.{ .close = closeNative } };
    }
    fn next(raw: *anyopaque, alloc: std.mem.Allocator, _: u32) !read_view.Page {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        if (self.row_returned) return .{ .arena = arena, .rows = &.{}, .after = null };
        self.row_returned = true;
        const rows = try arena.allocator().alloc(read_view.Row, 1);
        rows[0] = .{ .id = "local", .version = 1, .schema_version = 1, .value = try std.json.parseFromSliceLeaky(std.json.Value, arena.allocator(), "{\"amount\":3}", .{}) };
        return .{ .arena = arena, .rows = rows, .after = null };
    }
    fn closeView(_: *anyopaque) void {}
    fn closeNative(raw: *anyopaque) void {
        const self: *Fixture = @ptrCast(@alignCast(raw));
        self.native_closed += 1;
    }
};

test "lake SQL API binds external catalog sources for aggregates joins public rows and read-only rejection" {
    const alloc = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("lake");
    defer directory.cleanup();
    const parquet = try @import("../serverless/query/lake_parquet_rowgroup.zig").buildTestPlainI64ParquetObjectAlloc(alloc, &.{.{ .column_id = "amount", .field_id = 1, .values = &.{ 1, 2, 3, 4, 5 } }});
    defer alloc.free(parquet);
    var filesystem = try @import("../storage/object_storage.zig").FilesystemObjectStorage.init(alloc, directory.path());
    defer filesystem.deinit();
    var client = filesystem.client();
    var written = try client.putObject("antfly", "part.parquet", parquet, .{});
    defer written.deinit(alloc);
    const source_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{directory.path()});
    defer alloc.free(source_uri);
    const iceberg = @import("../serverless/query/lake_iceberg_snapshot.zig");
    const manifest = try iceberg.buildTestDataManifestAlloc(alloc, &.{.{ .path = "object://antfly/part.parquet", .rows = 5, .bytes = parquet.len }});
    defer alloc.free(manifest);
    const manifest_list = try iceberg.buildTestManifestListAlloc(alloc, "object://antfly/metadata/data.avro", manifest.len, 1, 5);
    defer alloc.free(manifest_list);
    const metadata = "{\"format-version\":2,\"table-uuid\":\"events\",\"location\":\"object://antfly\",\"current-schema-id\":7,\"current-snapshot-id\":12,\"snapshots\":[{\"snapshot-id\":12,\"sequence-number\":42,\"timestamp-ms\":1700000000000,\"manifest-list\":\"object://antfly/metadata/snap.avro\"}]}";
    const objects = [_]struct { key: []const u8, data: []const u8 }{
        .{ .key = "metadata/version-hint.text", .data = "1\n" },
        .{ .key = "metadata/v1.metadata.json", .data = metadata },
        .{ .key = "metadata/data.avro", .data = manifest },
        .{ .key = "metadata/snap.avro", .data = manifest_list },
    };
    for (objects) |object| {
        var put = try client.putObject("antfly", object.key, object.data, .{});
        put.deinit(alloc);
    }
    for ([_][]const u8{ "parquet", "iceberg" }) |format| {
        var schema_json = try std.json.parseFromSlice(std.json.Value, alloc, Fixture.native_schema, .{});
        defer schema_json.deinit();
        const source_json = try std.json.Stringify.valueAlloc(alloc, .{ .kind = "external", .table_id = "events", .format = format, .uri = source_uri, .schema_fingerprint = if (std.mem.eql(u8, format, "iceberg")) "iceberg-schema:7" else "schema-v1" }, .{});
        defer alloc.free(source_json);
        var source = try std.json.parseFromSlice(std.json.Value, alloc, source_json, .{});
        defer source.deinit();
        try schema_json.value.object.put(alloc, "base_source", source.value);
        const encoded = try std.json.Stringify.valueAlloc(alloc, schema_json.value, .{});
        defer alloc.free(encoded);
        var fixture: Fixture = .{ .lake_schema = encoded };
        var backend_runtime = try @import("../storage/background_runtime.zig").BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
        defer backend_runtime.deinit();
        var server = server_mod.ApiHttpServer.init(alloc, .{ .backend_runtime = backend_runtime.ptr() }, .{ .ptr = &fixture, .vtable = &.{ .status = undefined, .system_catalog = Fixture.catalog, .supports_query_definitions = true } }, .{ .ptr = &fixture, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .open_relational_statement = Fixture.openNative } }, null);
        defer server.deinit();
        var identity: ?server_mod.AuthenticatedIdentity = null;
        const cases = [_]struct { sql: []const u8, expected: []const u8 }{
            .{ .sql = "SELECT COUNT(*) FROM events", .expected = "5" },
            .{ .sql = "SELECT COUNT(*) AS total FROM events WHERE amount > 2", .expected = "3" },
            .{ .sql = "SELECT SUM(amount) FROM events WHERE amount > 2", .expected = "12" },
            .{ .sql = "SELECT COUNT(*) FROM events e JOIN events x ON e.amount = x.amount", .expected = "5" },
            .{ .sql = "SELECT SUM(e.amount) FROM events e JOIN native n ON e.amount = n.amount", .expected = "3" },
            .{ .sql = "WITH selected AS (SELECT amount FROM events WHERE amount > 2) SELECT COUNT(*) FROM selected", .expected = "3" },
        };
        for (cases) |case| {
            var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
            var compiled = try @import("../sql/compiler.zig").compile(alloc, case.sql, .{});
            defer compiled.deinit();
            var result = try @import("../sql/runtime.zig").execute(alloc, adapter.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
            try std.testing.expectEqualStrings(case.expected, result.output.rows[0][0].string);
        }
        // Catalog changes must remain retryable failures, never permission to
        // treat a lake source as unprotected after the latest-main policy fix.
        fixture.policy_catalog_changed = true;
        var changing_adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
        var changing_count = try @import("../sql/compiler.zig").compile(alloc, "SELECT COUNT(*) FROM events", .{});
        defer changing_count.deinit();
        try std.testing.expectError(error.RowPolicyCatalogChanged, @import("../sql/runtime.zig").execute(alloc, changing_adapter.backend(), &changing_count, &.{}, .{}));
        fixture.policy_catalog_changed = false;
        try std.testing.expectEqual(fixture.native_opened, fixture.native_closed);
        try std.testing.expectEqual(@as(usize, 1), fixture.native_opened);
        var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
        var explanation = try @import("../sql/compiler.zig").compile(alloc, "EXPLAIN (VERBOSE, FORMAT JSON) SELECT SUM(amount) FROM events", .{});
        defer explanation.deinit();
        var explained = try @import("../sql/runtime.zig").execute(alloc, adapter.backend(), &explanation, &.{}, .{});
        defer explained.deinit();
        try std.testing.expect(std.mem.indexOf(u8, explained.output.rows[0][0].string, "Lake Scan") != null);
        try std.testing.expect(std.mem.indexOf(u8, explained.output.rows[0][0].string, format) != null);
        var insert = try @import("../sql/compiler.zig").compile(alloc, "INSERT INTO events (_id, amount) VALUES ('new', 7)", .{});
        defer insert.deinit();
        try std.testing.expectError(error.ExternalLakeReadOnly, @import("../sql/runtime.zig").execute(alloc, adapter.backend(), &insert, &.{}, .{}));
        var request = try @import("http_route_helpers.zig").parseRelationalRowQueryRequest(alloc, "{\"fields\":[\"amount\"],\"conditions\":[{\"column\":\"amount\",\"op\":\"gt\",\"value\":3}],\"limit\":1}");
        defer request.deinit(alloc);
        const ndjson = (try @import("lake_table_reads.zig").query(alloc, &adapter, .{ .database = "default", .namespace = "public", .table = "events" }, 7, request)).?;
        defer alloc.free(ndjson);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, std.mem.trim(u8, ndjson, "\n"), .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(i64, 4), parsed.value.object.get("row").?.object.get("amount").?.integer);
    }
}
