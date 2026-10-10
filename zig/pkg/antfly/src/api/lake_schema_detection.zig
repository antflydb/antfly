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

//! Creation-only schema inference. Publication receives ordinary, fully bound
//! schema JSON; SQL Describe and Execute never discover or mutate catalog types.
const std = @import("std");
const schema = @import("antfly_local_sources").serverless_query_lake_schema;
const binding_api = @import("antfly_local_sources").serverless_external_source_schema_binding;
const A = std.mem.Allocator;
pub fn prepare(a: A, input: []const u8, options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions, context: @import("antfly_local_sources").serverless_query_lake_read_context.Context) !?[]u8 {
    var parsed = try std.json.parseFromSlice(std.json.Value, a, input, .{ .allocate = .alloc_always, .parse_numbers = false });
    defer parsed.deinit();
    const owned = parsed.arena.allocator();
    if (parsed.value != .object) return null;
    const source = parsed.value.object.getPtr("base_source") orelse return null;
    if (source.* != .object) return null;
    const kind = source.object.get("kind") orelse return null;
    if (kind != .string or !std.mem.eql(u8, kind.string, "external")) return null;
    const documents = parsed.value.object.get("document_schemas");
    const infer = documents == null or documents.? == .null or (documents.? == .object and documents.?.object.count() == 0);
    const fp = source.object.get("schema_fingerprint");
    const auto = fp == null or (fp.? == .string and std.mem.eql(u8, fp.?.string, "auto"));
    if (!infer and !auto) return null;
    var binding = (try binding_api.externalBindingFromSchemaJsonAlloc(a, input)) orelse return error.InvalidExternalTableBinding;
    defer binding.deinit(a);
    try context.ensureActive();
    var store = try @import("../serverless/configured_object_store_support.zig").openBindingObjectStoreAlloc(a, binding.binding, options);
    defer store.deinit();
    var contextual: @import("antfly_local_sources").serverless_query_lake_read_context.Store = .{ .base = store.client, .context = context };
    const client = contextual.client(a);
    const base = if (store.fs_client != null) try std.fmt.allocPrint(a, "object://{s}/{s}", .{ store.bucket, store.prefix }) else null;
    defer if (base) |value| a.free(value);
    var detected = switch (binding.binding.format) {
        .parquet => blk: {
            var inventory = try @import("antfly_local_sources").serverless_external_source_mod.planParquetPrefixInventoryFromObjectStorageAlloc(a, .{ .client = client, .bucket = store.bucket, .prefix = store.prefix, .source_id = binding.binding.table_id, .source_uri = binding.binding.source_uri, .object_uri_base = base, .schema_fingerprint = "auto" });
            defer inventory.deinit(a);
            if (binding.binding.snapshot_mode == .object_version_digest) if (!std.mem.eql(u8, inventory.snapshot_id, binding.binding.snapshot_mode.object_version_digest)) return error.ExternalLakeSnapshotMismatch;
            var reader = @import("antfly_local_sources").serverless_query_lake_object_reader.ObjectStorageRangeReader.init(client);
            break :blk try schema.parquetSchema(a, inventory, reader.parquetReader());
        },
        .iceberg => blk: {
            if (binding.binding.catalog != null) {
                var result = @import("../serverless/configured_object_store_support.zig").executeLakeCatalogAlloc(a, binding.binding, options, context, .load) catch |err| {
                    // A declared writable table can be bound before its initial
                    // catalog commit. Initialization persists the inferred fingerprint.
                    if (err == error.LakeTableNotFound and !infer and binding.binding.write_policy == .iceberg_writer) return null;
                    return err;
                };
                defer result.deinit(a);
                break :blk try schema.icebergSchema(a, result.table.metadata_json, binding.binding.snapshot_mode.pinnedSnapshotId());
            }
            const uri = try @import("antfly_local_sources").serverless_query_lake_serving.ServingSource.icebergMetadataUriForOpenedStoreAlloc(a, client, store.bucket, store.prefix, binding.binding.source_uri, base);
            defer a.free(uri);
            var reader_client = client;
            const bytes = try @import("antfly_local_sources").serverless_query_lake_iceberg_snapshot.readFullObjectAlloc(a, &reader_client, null, uri, .iceberg_metadata, null, 16 * 1024 * 1024);
            defer a.free(bytes);
            break :blk try schema.icebergSchema(a, bytes, binding.binding.snapshot_mode.pinnedSnapshotId());
        },
        .lance => return error.UnsupportedExternalLakeSchemaType,
    };
    defer detected.deinit();
    try context.ensureActive();
    if (!auto and !std.mem.eql(u8, fp.?.string, detected.fingerprint)) return error.ExternalLakeSchemaMismatch;
    try source.object.put(owned, "schema_fingerprint", .{ .string = try owned.dupe(u8, detected.fingerprint) });
    if (infer) {
        var properties: std.json.ObjectMap = .empty;
        var required: std.json.Array = .init(owned);
        for (detected.columns) |column| {
            var definition: std.json.ObjectMap = .empty;
            try definition.put(owned, "type", .{ .string = try owned.dupe(u8, column.kind) });
            const name = try owned.dupe(u8, column.name);
            try properties.put(owned, name, .{ .object = definition });
            if (column.required) try required.append(.{ .string = name });
        }
        var row_schema: std.json.ObjectMap = .empty;
        try row_schema.put(owned, "type", .{ .string = "object" });
        try row_schema.put(owned, "properties", .{ .object = properties });
        try row_schema.put(owned, "required", .{ .array = required });
        try row_schema.put(owned, "additionalProperties", .{ .bool = false });
        var row: std.json.ObjectMap = .empty;
        try row.put(owned, "schema", .{ .object = row_schema });
        var documents_map: std.json.ObjectMap = .empty;
        try documents_map.put(owned, "row", .{ .object = row });
        try parsed.value.object.put(owned, "document_schemas", .{ .object = documents_map });
        try parsed.value.object.put(owned, "default_type", .{ .string = "row" });
        try parsed.value.object.put(owned, "enforce_types", .{ .bool = true });
    }
    const resolved = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(resolved);
    return try @import("antfly_local_sources").schema_mod.parseSchemaUpdateRequest(a, resolved);
}

test "lake SQL schema detection persists Parquet and Iceberg columns without data decoding" {
    const a = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("lake-schema");
    defer directory.cleanup();
    var fs = try @import("antfly_local_sources").storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const bytes = try @import("antfly_local_sources").serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ 1, 2 }, .field_id = 1 }});
    defer a.free(bytes);
    var put = try client.putObject("antfly", "part.parquet", bytes, .{});
    put.deinit(a);
    const metadata_json = "{\"table-uuid\":\"empty-test\",\"location\":\"object://antfly/\",\"format-version\":2,\"current-schema-id\":7,\"schemas\":[{\"schema-id\":7,\"fields\":[{\"id\":1,\"name\":\"amount\",\"required\":true,\"type\":\"long\"},{\"id\":2,\"name\":\"label\",\"required\":false,\"type\":\"string\"}]}]}";
    put = try client.putObject("antfly", "metadata/v1.metadata.json", metadata_json, .{});
    put.deinit(a);
    put = try client.putObject("antfly", "metadata/version-hint.text", "1\n", .{});
    put.deinit(a);
    for ([_][]const u8{ "parquet", "iceberg" }) |format| {
        const input = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"events\",\"format\":\"{s}\",\"uri\":\"file://{s}\"}}}}", .{ format, directory.path() });
        defer a.free(input);
        const draft = try @import("antfly_local_sources").schema_table_schema_impl.parseCreateSchemaRequest(a, input);
        defer a.free(draft);
        try std.testing.expectError(error.InvalidSchemaUpdateRequest, @import("antfly_local_sources").schema_mod.parseSchemaUpdateRequest(a, draft));
        const result = (try prepare(a, draft, .{}, .{})).?;
        defer a.free(result);
        var document = try std.json.parseFromSlice(std.json.Value, a, result, .{});
        defer document.deinit();
        const row = document.value.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object;
        try std.testing.expectEqualStrings("integer", row.get("properties").?.object.get("amount").?.object.get("type").?.string);
        try std.testing.expectEqualStrings("amount", row.get("required").?.array.items[0].string);
        const fingerprint = document.value.object.get("base_source").?.object.get("schema_fingerprint").?.string;
        try std.testing.expect(std.mem.startsWith(u8, fingerprint, if (std.mem.eql(u8, format, "parquet")) "parquet-schema:" else "iceberg-schema:7:"));
        if (std.mem.eql(u8, format, "iceberg")) try std.testing.expectEqualStrings("string", row.get("properties").?.object.get("label").?.object.get("type").?.string);
        var bound = (try binding_api.externalBindingFromSchemaJsonAlloc(a, result)).?;
        defer bound.deinit(a);
        try std.testing.expect((try prepare(a, result, .{}, .{})) == null);
        if (std.mem.eql(u8, format, "iceberg")) {
            const table: @import("antfly_local_sources").sql_catalog.Table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .columns = &.{.{ .name = "amount", .path = "amount", .type = .integer, .nullable = false }}, .external_base_source = bound };
            const cursor = try @import("lake_sql_cursor.zig").open(a, table, .{ .fields = &.{"amount"}, .limit = 2 }, .{}, .{});
            defer cursor.close(cursor.ptr);
            try std.testing.expectEqual(@as(?u64, 0), try cursor.count_rows.?(cursor.ptr));
        }
    }
}

test "lake SQL inferred Parquet union supplies missing nullable columns and fences type changes" {
    const a = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("lake-schema-union");
    defer directory.cleanup();
    var fs = try @import("antfly_local_sources").storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    const parquet = @import("antfly_local_sources").serverless_query_lake_parquet_rowgroup;
    for ([_][]const u8{ "amount", "note" }) |name| {
        const bytes = try parquet.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = name, .values = &.{ 1, 2 }, .field_id = 1 }});
        defer a.free(bytes);
        const key = try std.fmt.allocPrint(a, "{s}.parquet", .{name});
        defer a.free(key);
        var put = try client.putObject("antfly", key, bytes, .{});
        put.deinit(a);
    }
    const input = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"events\",\"format\":\"parquet\",\"uri\":\"file://{s}\"}}}}", .{directory.path()});
    defer a.free(input);
    const result = (try prepare(a, input, .{}, .{})).?;
    defer a.free(result);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, result, .{});
    defer parsed.deinit();
    const row_schema = parsed.value.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object;
    try std.testing.expectEqual(@as(usize, 2), row_schema.get("properties").?.object.count());
    try std.testing.expectEqual(@as(usize, 0), row_schema.get("required").?.array.items.len);
    var binding = (try binding_api.externalBindingFromSchemaJsonAlloc(a, result)).?;
    defer binding.deinit(a);
    const catalog = @import("antfly_local_sources").sql_catalog;
    var table: catalog.Table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .columns = &.{ .{ .name = "amount", .path = "amount", .type = .integer }, .{ .name = "note", .path = "note", .type = .integer } }, .external_base_source = binding };
    const cursor = try @import("lake_sql_cursor.zig").open(a, table, .{ .fields = &.{ "amount", "note" }, .limit = 2 }, .{}, .{});
    defer cursor.close(cursor.ptr);
    var visited: usize = 0;
    var nulls: usize = 0;
    while (true) {
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const page = try cursor.next_columns.?(cursor.ptr, arena.allocator(), 2);
        for (0..page.selection.len) |i| {
            visited += 1;
            for ([_][]const u8{ "amount", "note" }) |name| nulls += @intFromBool((try page.cell(arena.allocator(), i, name)).sql_null);
        }
        if (page.after == null) break;
    }
    try std.testing.expectEqual(@as(usize, 4), visited);
    try std.testing.expectEqual(@as(usize, 4), nulls);
    table.columns = &.{.{ .name = "amount", .path = "amount", .type = .string }};
    const invalid = try @import("lake_sql_cursor.zig").open(a, table, .{ .fields = &.{"amount"}, .limit = 2 }, .{}, .{});
    defer invalid.close(invalid.ptr);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    while (true) {
        const page = invalid.next_columns.?(invalid.ptr, arena.allocator(), 2) catch |err| {
            try std.testing.expectEqual(error.ExternalLakeSchemaMismatch, err);
            break;
        };
        if (page.after == null) return error.TestExpectedError;
        _ = arena.reset(.free_all);
    }
}

test "lake SQL inferred decimal128 and signed timestamps execute through Parquet and Iceberg" {
    const a = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("lake-exact-types");
    defer directory.cleanup();
    var filesystem = try @import("antfly_local_sources").storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer filesystem.deinit();
    var client = filesystem.client();
    var big: [16]u8 = undefined;
    std.mem.writeInt(i128, &big, 99999999999999999999999999999999999999, .big);
    const parquet = try @import("antfly_local_sources").serverless_query_lake_parquet_rowgroup.buildTestPlainI64AndByteArrayParquetObjectAlloc(a, &.{
        .{ .column_id = "ts", .field_id = 1, .converted_type = 9, .values = &.{ -1, 0, 1 } },
        .{ .column_id = "price", .field_id = 2, .converted_type = 5, .decimal_precision = 18, .decimal_scale = 2, .values = &.{ 9007199254740993, -1, 12300 } },
    }, &.{.{ .column_id = "big", .field_id = 3, .converted_type = 5, .decimal_precision = 38, .decimal_scale = 2, .values = &.{ &big, &big, &big } }});
    defer a.free(parquet);
    var put = try client.putObject("antfly", "part.parquet", parquet, .{});
    put.deinit(a);
    const iceberg = @import("antfly_local_sources").serverless_query_lake_iceberg_snapshot;
    const manifest = try iceberg.buildTestDataManifestAlloc(a, &.{.{ .path = "object://antfly/part.parquet", .rows = 3, .bytes = parquet.len }});
    defer a.free(manifest);
    const manifest_list = try iceberg.buildTestManifestListAlloc(a, "object://antfly/metadata/data.avro", manifest.len, 1, 3);
    defer a.free(manifest_list);
    const metadata_json = "{\"format-version\":2,\"table-uuid\":\"events\",\"location\":\"object://antfly\",\"schemas\":[{\"schema-id\":7,\"fields\":[{\"id\":1,\"name\":\"ts\",\"required\":true,\"type\":\"timestamptz\"},{\"id\":2,\"name\":\"price\",\"required\":true,\"type\":\"decimal(18, 2)\"},{\"id\":3,\"name\":\"big\",\"required\":true,\"type\":\"decimal(38, 2)\"}]}],\"current-schema-id\":7,\"current-snapshot-id\":12,\"snapshots\":[{\"snapshot-id\":12,\"sequence-number\":42,\"timestamp-ms\":1700000000000,\"manifest-list\":\"object://antfly/metadata/snap.avro\"}]}";
    for ([_]struct { key: []const u8, bytes: []const u8 }{
        .{ .key = "metadata/version-hint.text", .bytes = "1\n" },
        .{ .key = "metadata/v1.metadata.json", .bytes = metadata_json },
        .{ .key = "metadata/data.avro", .bytes = manifest },
        .{ .key = "metadata/snap.avro", .bytes = manifest_list },
    }) |object| {
        put = try client.putObject("antfly", object.key, object.bytes, .{});
        put.deinit(a);
    }
    const catalog = @import("antfly_local_sources").sql_catalog;
    const Backend = struct {
        table: catalog.Table,
        fn resolve(raw: *anyopaque, _: A, _: @import("antfly_local_sources").sql_ast.Name, _: catalog.Action) !catalog.Table {
            return (@as(*@This(), @ptrCast(@alignCast(raw)))).table;
        }
        fn open(raw: *anyopaque, alloc: A, table: catalog.Table, request: catalog.Scan) !?catalog.Cursor {
            _ = raw;
            return try @import("lake_sql_cursor.zig").open(alloc, table, request, .{}, .{});
        }
        fn scan(_: *anyopaque, _: A, _: catalog.Table, _: catalog.Scan) !catalog.Page {
            return error.UnexpectedStatelessScan;
        }
        fn mutate(_: *anyopaque, _: A, _: A, _: catalog.Table, _: []const catalog.Mutation) !catalog.MutationOutcome {
            return error.UnexpectedMutation;
        }
        fn checkpoint(_: *anyopaque) !void {}
        fn backend(self: *@This()) catalog.Backend {
            return .{ .ptr = self, .execution_io = std.testing.io, .vtable = &.{ .resolve = resolve, .scan = scan, .open_scan = open, .mutate = mutate, .checkpoint = checkpoint } };
        }
    };
    for ([_][]const u8{ "parquet", "iceberg" }) |format| {
        const input = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"events\",\"format\":\"{s}\",\"uri\":\"file://{s}\"}}}}", .{ format, directory.path() });
        defer a.free(input);
        const inferred = (try prepare(a, input, .{}, .{})).?;
        defer a.free(inferred);
        var schema_json = try std.json.parseFromSlice(std.json.Value, a, inferred, .{});
        defer schema_json.deinit();
        const properties = schema_json.value.object.get("document_schemas").?.object.get("row").?.object.get("schema").?.object.get("properties").?.object;
        try std.testing.expectEqualStrings("datetime", properties.get("ts").?.object.get("type").?.string);
        try std.testing.expectEqualStrings("string", properties.get("price").?.object.get("type").?.string);
        try std.testing.expectEqualStrings("string", properties.get("big").?.object.get("type").?.string);
        var binding = (try binding_api.externalBindingFromSchemaJsonAlloc(a, inferred)).?;
        defer binding.deinit(a);
        var owner: Backend = .{ .table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .external_base_source = binding, .columns = &.{
            .{ .name = "ts", .path = "ts", .type = .datetime, .nullable = false },
            .{ .name = "price", .path = "price", .type = .string, .nullable = false },
            .{ .name = "big", .path = "big", .type = .string, .nullable = false },
        } } };
        const compiler = @import("antfly_local_sources").sql_compiler;
        const runtime = @import("antfly_local_sources").sql_runtime;
        var select = try compiler.compile(a, "SELECT price, big, ts FROM events ORDER BY ts", .{});
        defer select.deinit();
        var result = try runtime.execute(a, owner.backend(), &select, &.{}, .{});
        defer result.deinit();
        try std.testing.expectEqual(@as(usize, 3), result.output.rows.len);
        try std.testing.expectEqualStrings("90071992547409.93", result.output.rows[0][0].string);
        try std.testing.expectEqualStrings("999999999999999999999999999999999999.99", result.output.rows[0][1].string);
        try std.testing.expectEqualStrings("1969-12-31T23:59:59.999000000Z", result.output.rows[0][2].string);
        const stream = (try @import("antfly_local_sources").sql_read_stream.Stream.open(a, owner.backend(), &select, &.{}, .{ .page_rows = 1 })).?;
        defer stream.close();
        var delivered: usize = 0;
        while (true) {
            var page = try stream.next(1);
            defer page.deinit();
            for (page.output.rows) |row| {
                try std.testing.expectEqualStrings(result.output.rows[delivered][0].string, row[0].string);
                try std.testing.expectEqualStrings(result.output.rows[delivered][1].string, row[1].string);
                try std.testing.expectEqualStrings(result.output.rows[delivered][2].string, row[2].string);
                delivered += 1;
            }
            if (page.exhausted) break;
        }
        try std.testing.expectEqual(@as(usize, 3), delivered);
        const cases = [_]struct { predicate: []const u8, count: usize }{
            .{ .predicate = "ts = '1970-01-01T01:00:00.001+01:00'", .count = 1 },
            .{ .predicate = "ts <> '1970-01-01T00:00:00.001Z'", .count = 2 },
            .{ .predicate = "ts < '1970-01-01'", .count = 1 },
            .{ .predicate = "ts <= '1970-01-01'", .count = 2 },
            .{ .predicate = "ts > '1970-01-01'", .count = 1 },
            .{ .predicate = "ts >= '1970-01-01'", .count = 2 },
            .{ .predicate = "price = '90071992547409.93'", .count = 1 },
        };
        for (cases) |case| {
            const sql = try std.fmt.allocPrint(a, "SELECT price FROM events WHERE {s}", .{case.predicate});
            defer a.free(sql);
            var compiled = try compiler.compile(a, sql, .{});
            defer compiled.deinit();
            var filtered = try runtime.execute(a, owner.backend(), &compiled, &.{}, .{});
            defer filtered.deinit();
            try std.testing.expectEqual(case.count, filtered.output.rows.len);
        }
        var prepared = try compiler.compile(a, "SELECT price FROM events WHERE ts = $1", .{});
        defer prepared.deinit();
        var parameter_result = try runtime.execute(a, owner.backend(), &prepared, &.{.{ .string = "1969-12-31T23:59:59.999Z" }}, .{});
        defer parameter_result.deinit();
        try std.testing.expectEqual(@as(usize, 1), parameter_result.output.rows.len);
    }
}

test "external lake rejects uncommitted metadata and unsigned schema inference" {
    const a = std.testing.allocator;
    var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("lake-commit-pointer");
    defer directory.cleanup();
    var fs = try @import("antfly_local_sources").storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const metadata_json = "{\"table-uuid\":\"orphan\",\"location\":\"object://antfly/\",\"format-version\":2,\"current-schema-id\":7,\"schemas\":[{\"schema-id\":7,\"fields\":[{\"id\":1,\"name\":\"amount\",\"required\":true,\"type\":\"long\"}]}]}";
    var put = try client.putObject("antfly", "metadata/00099-uncommitted.metadata.json", metadata_json, .{});
    put.deinit(a);
    const input = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"events\",\"format\":\"iceberg\",\"uri\":\"file://{s}\"}}}}", .{directory.path()});
    defer a.free(input);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, prepare(a, input, .{}, .{}));
    const Source = @import("antfly_local_sources").serverless_query_lake_serving.ServingSource;
    var orphan_binding = (try binding_api.externalBindingFromSchemaJsonAlloc(a, input)).?;
    defer orphan_binding.deinit(a);
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, Source.open(a, .{ .storage_mode = .relational, .external_base_source = orphan_binding }, .{}));
    try std.testing.expectError(error.ExternalLakeSnapshotMismatch, Source.icebergMetadataUriForOpenedStoreAlloc(a, client, "antfly", "", "object://antfly", null));
    const explicit = try Source.icebergMetadataUriForOpenedStoreAlloc(a, client, "antfly", "", "object://antfly/metadata/00099-uncommitted.metadata.json", null);
    defer a.free(explicit);
    try std.testing.expectEqualStrings("object://antfly/metadata/00099-uncommitted.metadata.json", explicit);
    // Invalid or dangling hints must not permit directory fallback either.
    for ([_][]const u8{ "garbage", "", "99" }) |hint| {
        put = try client.putObject("antfly", "metadata/version-hint.text", hint, .{});
        put.deinit(a);
        try std.testing.expectError(error.ExternalLakeSnapshotMismatch, prepare(a, input, .{}, .{}));
    }
    const parquet = try @import("antfly_local_sources").serverless_query_lake_parquet_rowgroup.buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .converted_type = 14, .values = &.{ 0, std.math.maxInt(i64), std.math.minInt(i64), -1 } }});
    defer a.free(parquet);
    put = try client.putObject("antfly", "part.parquet", parquet, .{});
    put.deinit(a);
    const parquet_input = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"events\",\"format\":\"parquet\",\"uri\":\"file://{s}\"}}}}", .{directory.path()});
    defer a.free(parquet_input);
    try std.testing.expectError(error.UnsupportedExternalLakeSchemaType, prepare(a, parquet_input, .{}, .{}));
    var explicit_binding = (try binding_api.externalBindingFromSchemaJsonAlloc(a, parquet_input)).?;
    defer explicit_binding.deinit(a);
    const table: @import("antfly_local_sources").sql_catalog.Table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .columns = &.{.{ .name = "amount", .path = "amount", .type = .integer }}, .external_base_source = explicit_binding };
    const cursor = try @import("lake_sql_cursor.zig").open(a, table, .{ .fields = &.{"amount"}, .limit = 4 }, .{}, .{});
    defer cursor.close(cursor.ptr);
    try std.testing.expectError(error.UnsupportedParquetPage, cursor.next(cursor.ptr, a, 4));
}

test "lake SQL independent PyArrow compressed nullable fixtures infer and decode complete files" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        @import("antfly_local_sources").serverless_query_lake_fixtures.pyarrow_plain_nullable_snappy,
        @import("antfly_local_sources").serverless_query_lake_fixtures.pyarrow_dictionary_nullable_snappy,
    }) |bytes| {
        var directory = try @import("antfly_local_sources").common_test_directory.TestDirectory.init("lake-pyarrow");
        defer directory.cleanup();
        var fs = try @import("antfly_local_sources").storage_object_storage.FilesystemObjectStorage.init(a, directory.path());
        defer fs.deinit();
        var client = fs.client();
        var put = try client.putObject("antfly", "part.parquet", bytes, .{});
        put.deinit(a);
        const input = try std.fmt.allocPrint(a, "{{\"storage_mode\":\"relational\",\"base_source\":{{\"kind\":\"external\",\"table_id\":\"events\",\"format\":\"parquet\",\"uri\":\"file://{s}\"}}}}", .{directory.path()});
        defer a.free(input);
        const resolved = (try prepare(a, input, .{}, .{})).?;
        defer a.free(resolved);
        var binding = (try binding_api.externalBindingFromSchemaJsonAlloc(a, resolved)).?;
        defer binding.deinit(a);
        const table: @import("antfly_local_sources").sql_catalog.Table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .columns = &.{ .{ .name = "amount", .path = "amount", .type = .integer }, .{ .name = "label", .path = "label", .type = .string } }, .external_base_source = binding };
        const cursor = try @import("lake_sql_cursor.zig").open(a, table, .{ .fields = &.{ "amount", "label" }, .limit = 37 }, .{}, .{});
        defer cursor.close(cursor.ptr);
        var count: usize = 0;
        while (true) {
            var arena = std.heap.ArenaAllocator.init(a);
            defer arena.deinit();
            const page = try cursor.next_columns.?(cursor.ptr, arena.allocator(), 37);
            for (0..page.selection.len) |index| {
                const number = try page.cell(arena.allocator(), index, "amount");
                try std.testing.expectEqual(@as(i64, @intCast(count)), number.value.integer);
                const label = try page.cell(arena.allocator(), index, "label");
                try std.testing.expectEqual(count % 7 == 0, label.sql_null);
                if (!label.sql_null) try std.testing.expectEqualStrings(try std.fmt.allocPrint(arena.allocator(), "row-{d}", .{count}), label.value.string);
                count += 1;
            }
            if (page.after == null) break;
        }
        try std.testing.expectEqual(@as(usize, 1200), count);
    }
}
