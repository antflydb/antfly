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

test "lake SQL admission retains physical float key precision in its WAL payload" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const commit = @import("antfly_local_sources").serverless_external_source_mod.lake_catalog.row_commit;
    const batch = try std.json.parseFromSliceLeaky(commit.Batch, a, "{\"batch_id\":\"precision\",\"source\":\"hook\",\"epoch\":\"1\",\"checkpoint\":\"1\",\"key_fields\":[\"id\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"id\":16777216}},{\"op\":\"delete\",\"row\":{\"id\":16777217}}]}", .{});
    try commit.validate(a, "{\"current-schema-id\":0,\"schemas\":[{\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"id\",\"type\":\"float\",\"required\":true}]}]}", batch, .{});
    for (batch.changes) |change| try std.testing.expectEqual(@as(f64, 16777216), change.row.object.get("id").?.float);
    const payload = try std.json.Stringify.valueAlloc(a, batch, .{});
    try std.testing.expect(std.mem.indexOf(u8, payload, "16777217") == null);
}
const server_mod = @import("http_server.zig");
const Adapter = @import("sql_execution.zig").Adapter;
const domain = @import("antfly_local_sources").system_catalog_domain;
const operation = @import("antfly_local_sources").api_operation;
const reads = @import("antfly_local_sources").api_table_read_source;
const read_view = @import("antfly_local_sources").storage_relational_read_view.View;

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

test "lake SQL accepted transaction retains WAL images after publication and prepares executions against that cut" {
    try acceptedTransactionFixture(.read_committed);
}
test "lake SQL serializable accepted reads validate metadata and the monotone WAL head at commit" {
    try acceptedTransactionFixture(.serializable);
}
fn acceptedTransactionFixture(isolation: @import("antfly_local_sources").sql_session.Isolation) !void {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const table_manager = @import("../metadata/table_manager.zig");
    const metadata_api = @import("../metadata/api.zig");
    const lake_api = @import("lake_catalog_http.zig");
    const Source = struct {
        fixture: Fixture,
        table: [1]table_manager.TableRecord,
        fn snapshot(raw: *anyopaque, context: operation.RequestContext) !?metadata_api.AdminSnapshot {
            try context.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .status = .{ .metadata_group_id = 1, .metrics = .{} }, .tables = &self.table, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn catalog(raw: *anyopaque, alloc: std.mem.Allocator, context: operation.RequestContext, call: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return Fixture.catalog(&self.fixture, alloc, context, call);
        }
        fn replace(raw: *anyopaque, expected: table_manager.TableRecord, replacement: table_manager.TableRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!table_manager.tableDefinitionsEqual(self.table[0], expected)) return error.TableGenerationChanged;
            const owned = try std.testing.allocator.dupe(u8, replacement.schema_json);
            std.testing.allocator.free(self.table[0].schema_json);
            self.table[0].schema_json = owned;
            self.fixture.lake_schema = owned;
        }
    };
    var directory = try local.common_test_directory.TestDirectory.init("sql-pinned-accepted");
    defer directory.cleanup();
    const config_json = try std.json.Stringify.valueAlloc(a, .{ .deployment_mode = "standalone", .storage = .{ .engine = "local", .local = .{ .base_dir = directory.path() } }, .connections = .{ .artifacts = .{ .kind = "external_io", .capabilities = .{"storage.primary"}, .external_io = .{ .protocol = "filesystem", .root = directory.path() } } } }, .{});
    defer a.free(config_json);
    var config = try local.common_config.Config.parseFromSlice(a, config_json);
    defer config.deinit();
    const uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()});
    defer a.free(uri);
    var schema = try std.json.parseFromSlice(std.json.Value, a, Fixture.native_schema, .{});
    defer schema.deinit();
    const base_bytes = try std.json.Stringify.valueAlloc(a, .{ .kind = "external", .format = "iceberg", .uri = uri, .table_id = "events", .schema_fingerprint = "auto", .write_policy = "iceberg_writer", .catalog = .{ .type = "managed" } }, .{});
    defer a.free(base_bytes);
    var base = try std.json.parseFromSlice(std.json.Value, a, base_bytes, .{});
    defer base.deinit();
    try schema.value.object.put(schema.arena.allocator(), "base_source", base.value);
    const schema_json = try std.json.Stringify.valueAlloc(a, schema.value, .{});
    var source: Source = .{ .fixture = .{ .lake_schema = schema_json }, .table = .{.{ .table_id = 7, .name = "events", .schema_json = schema_json, .indexes_json = "{}" }} };
    defer a.free(source.table[0].schema_json);
    var backend = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{});
    defer backend.deinit();
    var server = server_mod.ApiHttpServer.init(a, .{ .node_config = &config, .backend_runtime = backend.ptr(), .deployment_mode = .standalone, .native_lake_artifact_base_dir = directory.path() }, .{ .ptr = &source, .vtable = &.{ .status = undefined, .linearizable_snapshot = Source.snapshot, .free_admin_snapshot = Source.free, .replace_table_definition = Source.replace, .system_catalog = Source.catalog, .supports_query_definitions = true } }, .{ .ptr = &source, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined } }, null);
    defer server.deinit();
    var initial = try lake_api.execute(a, &server, "events", 7, null, .{}, .{ .action = .create, .body = "{\"commit_id\":\"create\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"amount\",\"type\":\"long\",\"required\":true}]}}" });
    defer initial.deinit(a);
    var accepted = try lake_api.execute(a, &server, "events", 7, null, .{}, .{ .action = .changes, .body = "{\"batch_id\":\"first\",\"source\":\"test\",\"epoch\":\"1\",\"checkpoint\":\"1\",\"key_fields\":[\"amount\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"amount\":1}}]}" });
    defer accepted.deinit(a);
    const transaction = try server.txn_sessions.beginForPrincipal(a, .{ .sql = .{ .database = "default", .namespace = "public", .isolation = isolation, .mode = .read_only } }, server.localSessionNodeId(), null);
    const id = std.fmt.bytesToHex(transaction.txn_id, .lower);
    var identity: ?server_mod.AuthenticatedIdentity = null;
    var adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .session_id = &id, .lake_visibility = .accepted };
    var compiled = try local.sql_compiler.compile(a, "SELECT SUM(amount) FROM events", .{});
    defer compiled.deinit();
    var first = try adapter.execute(a, &compiled, &.{}, .{}, null);
    defer first.deinit();
    try std.testing.expectEqualStrings("1", first.output.rows[0][0].string);
    if (isolation == .serializable) try adapter.validateAcceptedSerializable(a, transaction.txn_id);
    var next = try lake_api.execute(a, &server, "events", 7, null, .{}, .{ .action = .changes, .body = "{\"batch_id\":\"second\",\"source\":\"test\",\"epoch\":\"1\",\"checkpoint\":\"2\",\"expected_checkpoint\":\"1\",\"key_fields\":[\"amount\"],\"changes\":[{\"op\":\"upsert\",\"row\":{\"amount\":2}}]}" });
    defer next.deinit(a);
    if (isolation == .serializable) try std.testing.expectError(error.SqlWriteConflict, adapter.validateAcceptedSerializable(a, transaction.txn_id));
    var binding = (try local.serverless_external_source_schema_binding.externalBindingFromSchemaJsonAlloc(a, source.table[0].schema_json)).?;
    defer binding.deinit(a);
    try std.testing.expect(try @import("../serverless/lake_ingestion.zig").drain(a, binding.binding, .{ .node_config = &config, .catalog_table_id = 7 }, .{ .io = std.testing.io }));
    // New request and recompiled/prepared statement reuse the durable cut.
    // The current catalog now contains both rows, unlike the initial empty
    // snapshot plus the single copied WAL image held by this transaction.
    var resumed: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .session_id = &id };
    var again = try resumed.execute(a, &compiled, &.{}, .{}, null);
    defer again.deinit();
    try std.testing.expectEqualStrings("1", again.output.rows[0][0].string);
    var joined = try local.sql_compiler.compile(a, "SELECT SUM(e.amount) FROM events e JOIN events f ON e.amount = f.amount", .{});
    defer joined.deinit();
    var joined_adapter: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .session_id = &id };
    var joined_result = try joined_adapter.execute(a, &joined, &.{}, .{}, null);
    defer joined_result.deinit();
    try std.testing.expectEqualStrings("1", joined_result.output.rows[0][0].string);
    var fresh: Adapter = .{ .server = &server, .identity = &identity, .context = .{}, .lake_visibility = .accepted };
    var latest = try fresh.execute(a, &compiled, &.{}, .{}, null);
    defer latest.deinit();
    try std.testing.expectEqualStrings("3", latest.output.rows[0][0].string);
}

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

test "lake SQL object table document WAL publishes through native hosting without data ranges" {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const object = @import("object_table_runtime.zig");
    var directory = try local.common_test_directory.TestDirectory.init("object-table");
    defer directory.cleanup();
    var table: @import("../metadata/table_manager.zig").TableRecord = .{
        .table_id = 7,
        .name = "docs",
        .storage = .{ .engine = .object },
        .schema_json = @import("tables.zig").default_schema_json,
        .indexes_json = @import("tables.zig").default_indexes_json,
        .min_ranges = 0,
        .desired_replica_count = 0,
        .object_storage_generation = 11,
    };
    const options: object.Options = .{ .deployment = .standalone, .local_base_dir = directory.path() };
    var store = try @import("lake_index_store.zig").Store.openNative(a, null, null, false, .standalone, directory.path());
    defer store.deinit();
    table.object_storage_identity = try object.storeIdentity(a, &store);
    const ranges = try @import("tables.zig").deriveInitialRanges(a, table);
    defer a.free(ranges);
    try std.testing.expectEqual(@as(usize, 0), ranges.len);
    {
        var manager: object.Manager = .{};
        defer manager.deinit(a);
        // Deadline expiry prevents cold runtime creation and any WAL effect.
        try std.testing.expectError(error.DeadlineExceeded, manager.handle(a, std.testing.io, table, options, .post, "batch", "{}", .{ .deadline_ns = 0 }));
        try std.testing.expectEqual(@as(usize, 0), manager.entries.count());
        var write = try manager.handle(a, std.testing.io, table, options, .post, "batch", "{\"inserts\":{\"doc:a\":{\"body\":\"alpha\"}},\"sync_level\":\"full_index\"}", .{});
        defer write.deinit(a);
        try std.testing.expectEqual(@as(u16, 201), write.status);
        var lookup = try manager.handle(a, std.testing.io, table, options, .get, "lookup", "doc:a", .{});
        defer lookup.deinit(a);
        try std.testing.expectEqual(@as(u16, 200), lookup.status);
        try std.testing.expect(std.mem.indexOf(u8, lookup.body, "alpha") != null);
        var query = try manager.handle(a, std.testing.io, table, options, .post, "query", "{\"full_text_search\":{\"query\":\"body:alpha\"}}", .{});
        defer query.deinit(a);
        try std.testing.expectEqual(@as(u16, 200), query.status);
        try std.testing.expect(std.mem.indexOf(u8, query.body, "doc:a") != null);
    }
    {
        var reopened: object.Manager = .{};
        defer reopened.deinit(a);
        var query = try reopened.handle(a, std.testing.io, table, options, .post, "query", "{\"full_text_search\":{\"query\":\"body:alpha\"}}", .{});
        defer query.deinit(a);
        try std.testing.expectEqual(@as(u16, 200), query.status);
        try std.testing.expect(std.mem.indexOf(u8, query.body, "doc:a") != null);
        var recreated = table;
        recreated.object_storage_generation += 1;
        var empty = try reopened.handle(a, std.testing.io, recreated, options, .get, "query", "", .{});
        defer empty.deinit(a);
        try std.testing.expect(std.mem.indexOf(u8, empty.body, "doc:a") == null);
    }
}

test "lake SQL object table admission rejects owned relational semantics" {
    const a = std.testing.allocator;
    var schema = try std.json.parseFromSlice(std.json.Value, a, Fixture.native_schema, .{});
    defer schema.deinit();
    _ = schema.value.object.swapRemove("version");
    const owned_body = try std.json.Stringify.valueAlloc(a, .{ .storage = .{ .engine = "object" }, .schema = schema.value }, .{});
    defer a.free(owned_body);
    try std.testing.expectError(error.RelationalStorageUnavailable, @import("table_contract.zig").parseCreateTableRequest(a, owned_body));
    var external = try @import("table_contract.zig").parseCreateTableRequest(a, "{\"storage\":{\"engine\":\"object\"},\"schema\":{\"storage_mode\":\"relational\",\"base_source\":{\"kind\":\"external\",\"format\":\"parquet\",\"uri\":\"s3://lake/events\",\"table_id\":\"events\",\"write_policy\":\"read_only\"}}}");
    defer external.deinit(a);
    try std.testing.expectEqual(.object, external.storage.?.engine);
    var sidecar = external;
    sidecar.indexes_json = @constCast("{\"ordered\":{\"type\":\"relational\"}}");
    try @import("tables.zig").validateObjectCreateDefinition(a, sidecar);
}

test "lake SQL object table native API binds storage and fails closed on policy authority" {
    const a = std.testing.allocator;
    const metadata = @import("../metadata/table_manager.zig");
    const metadata_api = @import("../metadata/api.zig");
    const Source = struct {
        table: [1]metadata.TableRecord = .{.{
            .table_id = 7,
            .name = "docs",
            .storage = .{ .engine = .object },
            .schema_json = @import("tables.zig").default_schema_json,
            .indexes_json = @import("tables.zig").default_indexes_json,
            .min_ranges = 0,
            .desired_replica_count = 0,
            .object_storage_generation = 3,
        }},
        unavailable: bool = false,
        bindings: usize = 0,
        definition_reads: usize = 0,
        authoritative_reads: usize = 0,
        local_queries: usize = 0,
        recreate_on_snapshot: enum { none, table_id, generation, engine } = .none,
        const local_table: metadata.TableRecord = .{
            .table_id = 8,
            .name = "local_docs",
            .schema_json = @import("tables.zig").default_schema_json,
            .indexes_json = @import("tables.zig").default_indexes_json,
        };
        fn localQuery(raw: *anyopaque, alloc: std.mem.Allocator, name: []const u8, _: @import("antfly_local_sources").storage_db_types.SearchRequest, _: @import("../raft/read_gate.zig").ReadConsistency) !?@import("antfly_local_sources").api_query.QueryResponse {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqualStrings("local_docs", name);
            self.local_queries += 1;
            return .{ .json = try alloc.dupe(u8, "{\"responses\":[{\"table\":\"local_docs\",\"hits\":{\"hits\":[]}}]}") };
        }
        fn snapshot(raw: *anyopaque) !metadata_api.AdminSnapshot {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .status = .{ .metadata_group_id = 1, .metrics = .{} }, .tables = &self.table, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn authoritative(raw: *anyopaque, context: operation.RequestContext) !?metadata_api.AdminSnapshot {
            try context.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.authoritative_reads += 1;
            switch (self.recreate_on_snapshot) {
                .none => {},
                .table_id => self.table[0].table_id += 1,
                .generation => self.table[0].object_storage_generation += 1,
                .engine => self.table[0].storage.engine = .local,
            }
            self.recreate_on_snapshot = .none;
            return try snapshot(raw);
        }
        fn forbiddenSnapshot(_: *anyopaque) !metadata_api.AdminSnapshot {
            return error.UnexpectedFullCatalogSnapshot;
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn replace(raw: *anyopaque, expected: metadata.TableRecord, replacement: metadata.TableRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!metadata.tableDefinitionsEqual(self.table[0], expected)) return error.TableGenerationChanged;
            try metadata.validateObjectTableMutation(std.testing.allocator, expected, replacement);
            self.table[0].object_storage_identity = replacement.object_storage_identity;
            self.bindings += 1;
        }
        fn catalog(raw: *anyopaque, alloc: std.mem.Allocator, _: operation.RequestContext, input: @import("../system_catalog/server_call.zig").Call) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return switch (input) {
                .query_definition => |name| blk: {
                    self.definition_reads += 1;
                    if (!std.mem.eql(u8, name, "local_docs") and !std.mem.eql(u8, name, "docs")) break :blk alloc.dupe(u8, "null");
                    break :blk std.json.Stringify.valueAlloc(alloc, domain.QueryDefinition.fromTable(if (std.mem.eql(u8, name, "local_docs")) local_table else self.table[0]), .{});
                },
                .resolve => std.json.Stringify.valueAlloc(alloc, domain.ResolvedTable.fromTable(self.table[0]), .{}),
                .resolve_many => |request| blk: {
                    const tables = try alloc.alloc(?domain.ResolvedTable, request.targets.len);
                    defer alloc.free(tables);
                    for (request.targets, tables) |target, *table| {
                        const resolved = if (std.mem.eql(u8, target.table, "local_docs")) local_table else self.table[0];
                        table.* = domain.ResolvedTable.fromTable(resolved);
                        if (request.include_query_definitions) table.*.?.query_definition = domain.QueryDefinition.fromTable(resolved);
                    }
                    break :blk std.json.Stringify.valueAlloc(alloc, domain.ResolvedMany{ .revision = 1, .tables = tables }, .{});
                },
                .policy_publication_status => if (self.unavailable) error.RowPolicyCatalogChanged else alloc.dupe(u8, "null"),
                else => error.UnexpectedCatalogCall,
            };
        }
    };
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("object-native-api");
    defer directory.cleanup();
    var backend = try @import("antfly_local_sources").storage_background_runtime.BackendRuntimeHandle.init(a, .{});
    defer backend.deinit();
    var source: Source = .{};
    var server = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = backend.ptr(), .deployment_mode = .standalone, .native_lake_artifact_base_dir = directory.path(), .graph_execution_limits = .{ .max_explored_nodes = 1 } }, .{ .ptr = &source, .vtable = &.{ .status = undefined, .supports_object_tables = true, .supports_query_definitions = true, .admin_snapshot = Source.forbiddenSnapshot, .linearizable_snapshot = Source.authoritative, .free_admin_snapshot = Source.free, .replace_table_definition = Source.replace, .system_catalog = Source.catalog } }, .{ .ptr = &source, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = Source.localQuery } }, null);
    defer server.deinit();
    var write = (try server.tryObjectTableRequest("docs", .post, "batch", "{\"inserts\":{\"a\":{\"body\":\"alpha\"}},\"sync_level\":\"full_index\"}", null, .{})).?;
    defer write.deinit(a);
    try std.testing.expectEqual(@as(u16, 201), write.status);
    try std.testing.expectEqual(@as(usize, 1), source.bindings);
    try std.testing.expect(!std.mem.allEqual(u8, &source.table[0].object_storage_identity, 0));
    var query = try server.handleAdmittedResolvedTableQueryWithContentTypeCancellation("docs", "{\"full_text_search\":{\"query\":\"body:alpha\"}}", null, null, null, "public.docs", null, null, null);
    defer query.deinit(a);
    try std.testing.expectEqual(@as(u16, 200), query.status);
    try std.testing.expect(std.mem.indexOf(u8, query.body, "public.docs") != null);
    const native_before_retrieval = source.local_queries;
    var retrieval = try server.executeCatalogRetrievalQuery(a, .{}, "docs", "{\"full_text_search\":{\"query\":\"body:alpha\"}}", null);
    defer retrieval.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, retrieval.json, "alpha") != null);
    try std.testing.expectEqual(native_before_retrieval, source.local_queries);

    // The definition captured by query binding must not switch to another
    // incarnation at authoritative dispatch, even when the name is unchanged.
    for ([_]@TypeOf(source.recreate_on_snapshot){ .table_id, .generation, .engine }) |change| {
        const original = source.table[0];
        source.recreate_on_snapshot = change;
        var conflict = try server.handlePublicTableQueryWithContentType("docs", "{\"full_text_search\":{\"query\":\"body:alpha\"}}", null, null);
        defer conflict.deinit(a);
        try std.testing.expectEqual(@as(u16, 409), conflict.status);
        source.table[0] = original;
        source.recreate_on_snapshot = change;
        try std.testing.expectError(error.TableGenerationChanged, server.executeCatalogRetrievalQuery(a, .{}, "docs", "{\"full_text_search\":{\"query\":\"body:alpha\"}}", null));
        source.table[0] = original;
        source.recreate_on_snapshot = change;
        try std.testing.expectError(error.TableGenerationChanged, server.tryObjectTableLookup("docs", "a", .stale, null, .{}));
        source.table[0] = original;
    }
    try std.testing.expectEqual(native_before_retrieval, source.local_queries);
    try std.testing.expectEqual(@as(u64, 1), server.object_tables.entries.get(.{ 7, 3 }).?.stack.handler.graph_execution_limits.max_explored_nodes);
    // Both primary and every nested native RHS are rejected before native
    // execution, even when the local left side would return no hits.
    const queries_before_join = source.local_queries;
    for ([_][]const u8{
        "{\"full_text_search\":{\"match_all\":{}},\"join\":{\"right_table\":\"docs\",\"on\":{\"left_field\":\"body\",\"right_field\":\"body\"}}}",
        "{\"full_text_search\":{\"match_all\":{}},\"join\":{\"right_table\":\"local_docs\",\"on\":{\"left_field\":\"body\",\"right_field\":\"body\"},\"nested_join\":{\"right_table\":\"docs\",\"on\":{\"left_field\":\"body\",\"right_field\":\"body\"}}}}",
    }) |join_body| {
        for ([_]?[]const u8{ null, "application/x-ndjson" }) |content_type| {
            var rejected = try server.handlePublicTableQueryWithContentType("local_docs", join_body, content_type, null);
            defer rejected.deinit(a);
            try std.testing.expectEqual(@as(u16, 400), rejected.status);
            try std.testing.expect(std.mem.indexOf(u8, rejected.body, "object table joins") != null);
        }
        const global_join = try std.fmt.allocPrint(a, "{{\"table\":\"local_docs\",{s}", .{join_body[1..]});
        defer a.free(global_join);
        var rejected_multi = try server.handlePublicGlobalMultiQuery(global_join, null);
        defer rejected_multi.deinit(a);
        try std.testing.expectEqual(@as(u16, 400), rejected_multi.status);
    }
    try std.testing.expectEqual(queries_before_join, source.local_queries);
    var expired_query = try server.handleAdmittedResolvedTableQueryWithContentTypeCancellation("docs", "{\"full_text_search\":{\"match_all\":{}}}", null, null, null, null, null, null, .{ .primary_foreign = false, .context = .{ .deadline_ns = 0 } });
    defer expired_query.deinit(a);
    try std.testing.expectEqual(@as(u16, 504), expired_query.status);
    const definitions_before = source.definition_reads;
    for ([_]?[]const u8{ null, "application/x-ndjson" }) |content_type| {
        var routed = try server.handlePublicTableQueryWithContentType("docs", "{\"full_text_search\":{\"query\":\"body:alpha\"}}", content_type, null);
        defer routed.deinit(a);
        try std.testing.expectEqual(@as(u16, 200), routed.status);
        try std.testing.expect(std.mem.indexOf(u8, routed.body, "alpha") != null);
    }
    // Bound queries reuse the definition captured by resolve_many.
    try std.testing.expectEqual(definitions_before, source.definition_reads);
    var multi = try server.handlePublicGlobalMultiQuery("{\"table\":\"docs\",\"full_text_search\":{\"query\":\"body:alpha\"}}\n{\"table\":\"docs\",\"full_text_search\":{\"query\":\"body:alpha\"}}", null);
    defer multi.deinit(a);
    try std.testing.expectEqual(@as(u16, 200), multi.status);
    var parsed_multi = try std.json.parseFromSlice(std.json.Value, a, multi.body, .{});
    defer parsed_multi.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_multi.value.object.get("responses").?.array.items.len);
    var mixed = try server.handlePublicGlobalMultiQuery("{\"table\":\"docs\",\"full_text_search\":{\"query\":\"body:alpha\"}}\n{\"table\":\"local_docs\",\"full_text_search\":{\"query\":\"body:alpha\"}}", null);
    defer mixed.deinit(a);
    try std.testing.expectEqual(@as(u16, 200), mixed.status);
    try std.testing.expectEqual(@as(usize, 1), source.local_queries);
    var parsed_mixed = try std.json.parseFromSlice(std.json.Value, a, mixed.body, .{});
    defer parsed_mixed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed_mixed.value.object.get("responses").?.array.items.len);

    // A WAL-only acknowledgment must never be presented as read-index-safe.
    var async_write = (try server.tryObjectTableRequest("docs", .post, "batch", "{\"inserts\":{\"a\":{\"body\":\"beta\"}},\"sync_level\":\"write\"}", null, .{})).?;
    defer async_write.deinit(a);
    try std.testing.expectEqual(@as(u16, 201), async_write.status);
    const httpx = @import("httpx");
    var handler = @import("httpx_handler.zig").AntflyApiHandler{ .api_server = &server };
    for ([_][]const u8{ "", "?consistency=read_index", "?consistency=leader_lease", "?consistency=garbage", "?read_consistency=garbage", "?consistency=stale" }) |suffix| {
        const url = try std.fmt.allocPrint(a, "http://127.0.0.1/db/v1/tables/docs/documents/a{s}", .{suffix});
        defer a.free(url);
        var request = try httpx.Request.init(a, .GET, url);
        defer request.deinit();
        var ctx = httpx.Context.init(a, undefined, &request);
        defer ctx.deinit();
        var response = try handler.lookupKey(&ctx, "docs", "a", .{});
        defer response.deinit();
        const stale = std.mem.eql(u8, suffix, "?consistency=stale");
        try std.testing.expectEqual(@as(u16, if (stale) 200 else 400), response.status.code);
        if (stale) try std.testing.expect(std.mem.indexOf(u8, response.body.?, "alpha") != null);
    }
    // Ordinary tables must not need an admin or authoritative snapshot.
    source.table[0].storage.engine = .local;
    const authoritative_before = source.authoritative_reads;
    try std.testing.expect((try server.tryObjectTableRequest("docs", .post, "batch", "{}", null, .{})) == null);
    try std.testing.expect((try server.tryObjectTableLookup("docs", "a", .read_index, null, .{})) == null);
    try std.testing.expectEqual(authoritative_before, source.authoritative_reads);
    source.table[0].storage.engine = .object;
    source.unavailable = true;
    try std.testing.expectError(error.RowPolicyCatalogChanged, server.tryObjectTableRequest("docs", .post, "batch", "{\"inserts\":{\"b\":{\"body\":\"beta\"}}}", null, .{}));
}

test "lake SQL native catalog initialization commits and restart retain authoritative metadata" {
    const a = std.testing.allocator;
    const local = @import("antfly_local_sources");
    const metadata = @import("../metadata/table_manager.zig");
    const metadata_api = @import("../metadata/api.zig");
    const api = @import("lake_catalog_http.zig");
    const Source = struct {
        table: [1]metadata.TableRecord,
        fn snapshot(raw: *anyopaque, ctx: operation.RequestContext) !?metadata_api.AdminSnapshot {
            try ctx.ensureActive();
            const self: *@This() = @ptrCast(@alignCast(raw));
            return .{ .status = .{ .metadata_group_id = 1, .metrics = .{} }, .tables = &self.table, .ranges = &.{}, .stores = &.{}, .placement_intents = &.{}, .split_transitions = &.{}, .merge_transitions = &.{} };
        }
        fn free(_: *anyopaque, _: *metadata_api.AdminSnapshot) void {}
        fn replace(raw: *anyopaque, expected: metadata.TableRecord, replacement: metadata.TableRecord) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!metadata.tableDefinitionsEqual(self.table[0], expected)) return error.TableGenerationChanged;
            const owned = try std.testing.allocator.dupe(u8, replacement.schema_json);
            std.testing.allocator.free(self.table[0].schema_json);
            self.table[0].schema_json = owned;
        }
    };
    var directory = try local.common_test_directory.TestDirectory.init("native-catalog-api");
    defer directory.cleanup();
    const uri = try std.fmt.allocPrint(a, "file://{s}/warehouse", .{directory.path()});
    defer a.free(uri);
    var schema = try std.json.parseFromSlice(std.json.Value, a, Fixture.native_schema, .{});
    defer schema.deinit();
    const base_source = try std.json.Stringify.valueAlloc(a, .{ .kind = "external", .format = "iceberg", .uri = uri, .table_id = "hn", .schema_fingerprint = "auto", .write_policy = "iceberg_writer", .catalog = .{ .type = "managed" } }, .{});
    defer a.free(base_source);
    var base = try std.json.parseFromSlice(std.json.Value, a, base_source, .{});
    defer base.deinit();
    try schema.value.object.put(schema.arena.allocator(), "base_source", base.value);
    var source: Source = .{ .table = .{.{ .table_id = 7, .name = "hn", .schema_json = try std.json.Stringify.valueAlloc(a, schema.value, .{}), .indexes_json = "{}" }} };
    defer a.free(source.table[0].schema_json);
    var backend = try local.storage_background_runtime.BackendRuntimeHandle.init(a, .{});
    defer backend.deinit();
    const status_source: server_mod.StatusSource = .{ .ptr = &source, .vtable = &.{ .status = undefined, .linearizable_snapshot = Source.snapshot, .free_admin_snapshot = Source.free, .replace_table_definition = Source.replace } };
    const reads_source: reads.TableReadSource = .{ .ptr = &source, .vtable = &.{ .lookup = undefined, .scan = undefined, .query = undefined } };
    var server = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = backend.ptr(), .deployment_mode = .standalone }, status_source, reads_source, null);
    defer server.deinit();
    try std.testing.expectError(error.LakeTableNotFound, api.execute(a, &server, "hn", 7, null, .{}, .{ .action = .load }));
    const body = "{\"commit_id\":\"initialize\",\"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[{\"id\":1,\"name\":\"amount\",\"type\":\"long\",\"required\":false}]}}";
    var initial = try api.execute(a, &server, "hn", 7, null, .{}, .{ .action = .create, .body = body });
    defer initial.deinit(a);
    try std.testing.expectEqual(@as(u16, 200), initial.status);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, initial.body, .{});
    defer parsed.deinit();
    const commit_body = try std.json.Stringify.valueAlloc(a, .{ .commit_id = "update", .expected_metadata_location = parsed.value.object.get("metadata_location").?.string, .requirements = .{}, .updates = .{.{ .action = "set-properties", .updates = .{ .owner = "hackernews" } }} }, .{});
    defer a.free(commit_body);
    var committed = try api.execute(a, &server, "hn", 7, null, .{}, .{ .action = .commit, .body = commit_body });
    defer committed.deinit(a);
    try std.testing.expectEqual(@as(u16, 200), committed.status);
    var outcome = try std.json.parseFromSlice(std.json.Value, a, committed.body, .{});
    defer outcome.deinit();
    var restarted = server_mod.ApiHttpServer.init(a, .{ .backend_runtime = backend.ptr(), .deployment_mode = .standalone }, status_source, reads_source, null);
    defer restarted.deinit();
    var replay = try api.execute(a, &restarted, "hn", 7, null, .{}, .{ .action = .commit, .body = commit_body });
    defer replay.deinit(a);
    try std.testing.expectEqualStrings(committed.body, replay.body);
    var resolved = try api.execute(a, &restarted, "hn", 7, null, .{}, .{ .action = .resolve, .commit_id = "update", .request_hash = outcome.value.object.get("request_hash").?.string });
    defer resolved.deinit(a);
    try std.testing.expect(std.mem.indexOf(u8, resolved.body, "committed") != null);
    try std.testing.expectError(error.TableGenerationChanged, api.execute(a, &restarted, "hn", 8, null, .{}, .{ .action = .commit, .body = commit_body }));
}
