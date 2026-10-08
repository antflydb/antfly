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
const server_mod = @import("http_server.zig");
const Adapter = @import("sql_execution.zig").Adapter;
const domain = @import("antfly_local_sources").system_catalog_domain;
const operation = @import("antfly_local_sources").api_operation;
const reads = @import("antfly_local_sources").api_table_read_source;
const read_view = @import("antfly_local_sources").storage_relational_read_view.View;

test "external lake persistence owns its executor and reuses immutable bytes after restart" {
    const a = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("lake-cache-restart");
    defer directory.cleanup();
    // No request concurrency is available. Cache startup must not depend on
    // this lane, nor retain it in its write worker.
    var request_io = std.Io.Threaded.init(a, .{ .concurrent_limit = .nothing });
    defer request_io.deinit();
    const cfg: server_mod.ApiHttpServerConfig = .{
        .lake_cache_enabled = true,
        .lake_cache_root = directory.path(),
        .imported_runtime_io = .{ .api = request_io.io() },
    };
    const Provider = struct {
        calls: usize = 0,
        fn load(raw: *anyopaque, alloc: std.mem.Allocator) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return alloc.dupe(u8, "authenticated search sidecar");
        }
    };
    var provider: Provider = .{};
    var token: u8 = 0;
    const status: server_mod.StatusSource = .{ .ptr = &token, .vtable = &.{ .status = undefined } };
    const bytes = "authenticated search sidecar";
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const loader: @import("antfly_local_sources").serverless_query_lake_serving_cache.Cache.ImmutableLoader = .{ .ptr = &provider, .load = Provider.load };
    {
        var server = server_mod.ApiHttpServer.init(a, cfg, status, null, null);
        defer server.deinit();
        {
            var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
            server.owner_alloc = failing.allocator();
            defer server.owner_alloc = a;
            try server.prepareLakeCache();
            try std.testing.expect(server.lake_cache_io == null);
            try std.testing.expectEqualStrings("OutOfMemory", server.requestStats().lake_range_cache.disk_unavailable.?);
        }
        try server.prepareLakeCache();
        try server.prepareLakeCache();
        try std.testing.expect(server.lake_read_cache.persistent != null);
        try std.testing.expect(server.lake_read_cache.persistentReady());
        {
            // A published owner must not queue warm queries behind startup.
            try server.lake_cache_start_mutex.lock(request_io.io());
            defer server.lake_cache_start_mutex.unlock(request_io.io());
            try server.prepareLakeCache();
        }
        try std.testing.expectEqual(std.Io.Limit.limited(1), server.lake_cache_io.?.concurrent_limit);
        var lease = try server.lake_read_cache.readImmutableBlockLease(a, @splat(9), "sidecar", bytes.len, digest, .{ .io = request_io.io() }, loader);
        defer lease.deinit();
        try std.testing.expectEqualStrings(bytes, lease.bytes());
        try std.testing.expectEqual(@as(usize, 1), provider.calls);
        // No explicit flush: server shutdown must drain accepted writes.
    }
    {
        var server = server_mod.ApiHttpServer.init(a, cfg, status, null, null);
        defer server.deinit();
        try server.prepareLakeCache();
        var lease = try server.lake_read_cache.readImmutableBlockLease(a, @splat(9), "sidecar", bytes.len, digest, .{ .io = request_io.io() }, loader);
        defer lease.deinit();
        try std.testing.expectEqualStrings(bytes, lease.bytes());
        try std.testing.expectEqual(@as(usize, 1), provider.calls);
        const stats = server.requestStats();
        try std.testing.expectEqual(@as(u64, 0), stats.lake_range_cache.provider_reads);
        try std.testing.expectEqual(@as(u64, 0), stats.lake_range_cache.provider_bytes);
        try std.testing.expectEqual(@as(u64, 1), stats.lake_range_cache.disk_hits);
        try std.testing.expect(stats.lake_range_cache.disk_unavailable == null);
    }
}

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
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("lake");
    defer directory.cleanup();
    const parquet = try @import("antfly_local_sources").serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(alloc, &.{.{ .column_id = "amount", .field_id = 1, .values = &.{ 1, 2, 3, 4, 5 } }});
    defer alloc.free(parquet);
    var filesystem = try @import("antfly_local_sources").storage_object_storage.FilesystemObjectStorage.init(alloc, directory.path());
    defer filesystem.deinit();
    var client = filesystem.client();
    var written = try client.putObject("antfly", "part.parquet", parquet, .{});
    defer written.deinit(alloc);
    const source_uri = try std.fmt.allocPrint(alloc, "file://{s}", .{directory.path()});
    defer alloc.free(source_uri);
    const iceberg = @import("antfly_local_sources").serverless_query_lake_iceberg_snapshot;
    const manifest = try iceberg.buildTestDataManifestAlloc(alloc, &.{.{ .path = "object://antfly/part.parquet", .rows = 5, .bytes = parquet.len }});
    defer alloc.free(manifest);
    const manifest_list = try iceberg.buildTestManifestListAlloc(alloc, "object://antfly/metadata/data.avro", manifest.len, 1, 5);
    defer alloc.free(manifest_list);
    const metadata = "{\"format-version\":2,\"table-uuid\":\"events\",\"location\":\"object://antfly\",\"schemas\":[{\"schema-id\":7,\"fields\":[{\"id\":1,\"name\":\"amount\",\"required\":true,\"type\":\"long\"}]}],\"current-schema-id\":7,\"current-snapshot-id\":12,\"snapshots\":[{\"snapshot-id\":12,\"sequence-number\":42,\"timestamp-ms\":1700000000000,\"manifest-list\":\"object://antfly/metadata/snap.avro\"}]}";
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
        var backend_runtime = try @import("antfly_local_sources").storage_background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
        defer backend_runtime.deinit();
        const cache_root = try std.fmt.allocPrint(alloc, "{s}/cache-{s}", .{ directory.path(), format });
        defer alloc.free(cache_root);
        var server = server_mod.ApiHttpServer.init(alloc, .{ .backend_runtime = backend_runtime.ptr(), .lake_cache_root = cache_root }, .{ .ptr = &fixture, .vtable = &.{ .status = undefined, .system_catalog = Fixture.catalog, .supports_query_definitions = true } }, .{ .ptr = &fixture, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined, .open_relational_statement = Fixture.openNative } }, null);
        var server_live = true;
        defer if (server_live) server.deinit();
        // Exercise the shared creation hook before SQL sees the durable schema.
        _ = schema_json.value.object.swapRemove("document_schemas");
        _ = source.value.object.swapRemove("schema_fingerprint");
        try schema_json.value.object.put(alloc, "base_source", source.value);
        const provisional = try std.json.Stringify.valueAlloc(alloc, schema_json.value, .{});
        defer alloc.free(provisional);
        const inferred = try server.bindForeignKeySchema(alloc, try domain.Target.parse("default.public.events"), "events", provisional, "", null, .{});
        defer alloc.free(inferred);
        fixture.lake_schema = inferred;
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
            var compiled = try @import("antfly_local_sources").sql_compiler.compile(alloc, case.sql, .{});
            defer compiled.deinit();
            var result = try @import("antfly_local_sources").sql_runtime.execute(alloc, adapter.backend(), &compiled, &.{}, .{});
            defer result.deinit();
            try std.testing.expectEqual(@as(usize, 1), result.output.rows.len);
            try std.testing.expectEqualStrings(case.expected, result.output.rows[0][0].string);
        }
        if (std.mem.eql(u8, format, "iceberg")) {
            var snapshots: usize = 0;
            var cached = server.lake_read_cache.decoded.entries.valueIterator();
            while (cached.next()) |entry| if (entry.*.payload == .snapshot) {
                snapshots += 1;
            };
            try std.testing.expectEqual(@as(usize, 1), snapshots);
            try std.testing.expect(server.lake_read_cache.decoded.hits > 0);
        }
        // Catalog changes must remain retryable failures, never permission to
        // treat a lake source as unprotected after the latest-main policy fix.
        fixture.policy_catalog_changed = true;
        var changing_adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
        var changing_count = try @import("antfly_local_sources").sql_compiler.compile(alloc, "SELECT COUNT(*) FROM events", .{});
        defer changing_count.deinit();
        try std.testing.expectError(error.RowPolicyCatalogChanged, @import("antfly_local_sources").sql_runtime.execute(alloc, changing_adapter.backend(), &changing_count, &.{}, .{}));
        fixture.policy_catalog_changed = false;
        try std.testing.expectEqual(fixture.native_opened, fixture.native_closed);
        try std.testing.expectEqual(@as(usize, 1), fixture.native_opened);
        var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
        var explanation = try @import("antfly_local_sources").sql_compiler.compile(alloc, "EXPLAIN (VERBOSE, FORMAT JSON) SELECT SUM(amount) FROM events", .{});
        defer explanation.deinit();
        var explained = try @import("antfly_local_sources").sql_runtime.execute(alloc, adapter.backend(), &explanation, &.{}, .{});
        defer explained.deinit();
        try std.testing.expect(std.mem.indexOf(u8, explained.output.rows[0][0].string, "Lake Scan") != null);
        try std.testing.expect(std.mem.indexOf(u8, explained.output.rows[0][0].string, format) != null);
        var insert = try @import("antfly_local_sources").sql_compiler.compile(alloc, "INSERT INTO events (_id, amount) VALUES ('new', 7)", .{});
        defer insert.deinit();
        try std.testing.expectError(error.ExternalLakeReadOnly, @import("antfly_local_sources").sql_runtime.execute(alloc, adapter.backend(), &insert, &.{}, .{}));
        var request = try @import("http_route_helpers.zig").parseRelationalRowQueryRequest(alloc, "{\"fields\":[\"amount\"],\"conditions\":[{\"column\":\"amount\",\"op\":\"gt\",\"value\":3}],\"limit\":1}");
        defer request.deinit(alloc);
        const ndjson = (try @import("lake_table_reads.zig").query(alloc, &adapter, .{ .database = "default", .namespace = "public", .table = "events" }, 7, request)).?;
        defer alloc.free(ndjson);
        var parsed = try std.json.parseFromSlice(std.json.Value, alloc, std.mem.trim(u8, ndjson, "\n"), .{});
        defer parsed.deinit();
        try std.testing.expectEqual(@as(i64, 4), parsed.value.object.get("row").?.object.get("amount").?.integer);
        var resumed_request = request;
        resumed_request.from = parsed.value.object.get("_id").?.string;
        const resumed = (try @import("lake_table_reads.zig").query(alloc, &adapter, .{ .database = "default", .namespace = "public", .table = "events" }, 7, resumed_request)).?;
        defer alloc.free(resumed);
        var resumed_row = try std.json.parseFromSlice(std.json.Value, alloc, std.mem.trim(u8, resumed, "\n"), .{});
        defer resumed_row.deinit();
        try std.testing.expectEqual(@as(i64, 5), resumed_row.value.object.get("row").?.object.get("amount").?.integer);
        // Reading ahead within a native page must not skip the public boundary.
        resumed_request.to = resumed_row.value.object.get("_id").?.string;
        const bounded = (try @import("lake_table_reads.zig").query(alloc, &adapter, .{ .database = "default", .namespace = "public", .table = "events" }, 7, resumed_request)).?;
        defer alloc.free(bounded);
        try std.testing.expectEqualStrings("", bounded);
        resumed_request.to = "invalid";
        try std.testing.expectError(error.ExternalLakeSnapshotMismatch, @import("lake_table_reads.zig").query(alloc, &adapter, .{ .database = "default", .namespace = "public", .table = "events" }, 7, resumed_request));
        try std.testing.expect(server.lake_read_cache.persistent != null);
        const cfg = server.cfg;
        const status_source = server.source;
        const table_reads = server.table_reads;
        server.deinit();
        server_live = false;
        var restarted = server_mod.ApiHttpServer.init(alloc, cfg, status_source, table_reads, null);
        defer restarted.deinit();
        var restarted_adapter: Adapter = .{ .server = &restarted, .identity = &identity, .context = .{} };
        var restart_query = try @import("antfly_local_sources").sql_compiler.compile(alloc, "SELECT SUM(amount) FROM events", .{});
        defer restart_query.deinit();
        var restart_result = try @import("antfly_local_sources").sql_runtime.execute(alloc, restarted_adapter.backend(), &restart_query, &.{}, .{});
        defer restart_result.deinit();
        try std.testing.expectEqualStrings("15", restart_result.output.rows[0][0].string);
        const stats = restarted.requestStats();
        try std.testing.expect(stats.lake_range_cache.disk_hits > 0);
        try std.testing.expect(stats.lake_range_cache.disk_bytes > 0);
        try std.testing.expectEqual(@as(u64, 0), stats.lake_range_cache.provider_reads);
        try std.testing.expect(stats.lake_disk_cache.?.read_hits > 0);
    }
}

test "lake SQL public residual scan reclaims page and distant timestamp memory" {
    const alloc = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("lake-residual-memory");
    defer directory.cleanup();
    var fs = try @import("antfly_local_sources").storage_object_storage.FilesystemObjectStorage.init(alloc, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const values = try alloc.alloc(i64, 20_000);
    defer alloc.free(values);
    @memset(values, 1);
    const bytes = try @import("antfly_local_sources").serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(alloc, &.{.{ .column_id = "amount", .converted_type = 9, .values = values, .page_rows = 1024 }});
    defer alloc.free(bytes);
    var put = try client.putObject("antfly", "part.parquet", bytes, .{});
    defer put.deinit(alloc);
    const schema_json = try std.fmt.allocPrint(alloc, "{{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{{\"row\":{{\"schema\":{{\"type\":\"object\",\"properties\":{{\"amount\":{{\"type\":\"datetime\"}}}}}}}}}},\"base_source\":{{\"kind\":\"external\",\"table_id\":\"events\",\"format\":\"parquet\",\"uri\":\"file://{s}\",\"schema_fingerprint\":\"v1\"}}}}", .{directory.path()});
    defer alloc.free(schema_json);
    var fixture: Fixture = .{ .lake_schema = schema_json };
    var runtime = try @import("antfly_local_sources").storage_background_runtime.BackendRuntimeHandle.init(alloc, .{ .backend = .io_threaded });
    defer runtime.deinit();
    var server = server_mod.ApiHttpServer.init(alloc, .{ .backend_runtime = runtime.ptr() }, .{ .ptr = &fixture, .vtable = &.{ .status = undefined, .system_catalog = Fixture.catalog, .supports_query_definitions = true } }, .{ .ptr = &fixture, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined } }, null);
    defer server.deinit();
    var identity: ?server_mod.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{} };
    var request = try @import("http_route_helpers.zig").parseRelationalRowQueryRequest(alloc, "{\"fields\":[\"amount\"],\"conditions\":[{\"column\":\"amount\",\"op\":\"is_not_distinct\",\"value\":\"2500-01-01\"}],\"limit\":1}");
    defer request.deinit(alloc);
    var budget: @import("antfly_local_sources").sql_memory_budget = .{ .backing = alloc, .limit = 4 * 1024 * 1024 };
    {
        const ndjson = (try @import("lake_table_reads.zig").query(budget.allocator(), &adapter, .{ .database = "default", .namespace = "public", .table = "events" }, 7, request)).?;
        defer budget.allocator().free(ndjson);
        try std.testing.expectEqualStrings("", ndjson);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.peak < budget.limit);
}
