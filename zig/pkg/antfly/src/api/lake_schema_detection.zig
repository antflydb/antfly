// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Creation-only schema inference. Publication receives ordinary, fully bound
//! schema JSON; SQL Describe and Execute never discover or mutate catalog types.
const std = @import("std");
const schema = @import("../serverless/query/lake_schema.zig");
const binding_api = @import("../serverless/external_source/schema_binding.zig");
const A = std.mem.Allocator;
pub fn prepare(a: A, input: []const u8, options: @import("../serverless/configured_object_store_support.zig").BindingObjectStoreOpenOptions, context: @import("../serverless/query/lake_read_context.zig").Context) !?[]u8 {
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
    var contextual: @import("../serverless/query/lake_read_context.zig").Store = .{ .base = store.client, .context = context };
    const client = contextual.client(a);
    const base = if (store.fs_client != null) try std.fmt.allocPrint(a, "object://{s}/{s}", .{ store.bucket, store.prefix }) else null;
    defer if (base) |value| a.free(value);
    var detected = switch (binding.binding.format) {
        .parquet => blk: {
            var inventory = try @import("../serverless/external_source/mod.zig").planParquetPrefixInventoryFromObjectStorageAlloc(a, .{ .client = client, .bucket = store.bucket, .prefix = store.prefix, .source_id = binding.binding.table_id, .source_uri = binding.binding.source_uri, .object_uri_base = base, .schema_fingerprint = "auto" });
            defer inventory.deinit(a);
            if (binding.binding.snapshot_mode == .object_version_digest) if (!std.mem.eql(u8, inventory.snapshot_id, binding.binding.snapshot_mode.object_version_digest)) return error.ExternalLakeSnapshotMismatch;
            var reader = @import("../serverless/query/lake_object_reader.zig").ObjectStorageRangeReader.init(client);
            break :blk try schema.parquetSchema(a, inventory, reader.parquetReader());
        },
        .iceberg => blk: {
            const uri = try @import("../serverless/query/lake_serving.zig").ServingSource.icebergMetadataUriForOpenedStoreAlloc(a, client, store.bucket, store.prefix, binding.binding.source_uri, base);
            defer a.free(uri);
            var reader_client = client;
            const bytes = try @import("../serverless/query/lake_iceberg_snapshot.zig").readFullObjectAlloc(a, &reader_client, null, uri, .iceberg_metadata, null, 16 * 1024 * 1024);
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
    return try std.json.Stringify.valueAlloc(a, parsed.value, .{});
}

test "lake SQL schema detection persists Parquet and Iceberg columns without data decoding" {
    const a = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("lake-schema");
    defer directory.cleanup();
    var fs = try @import("../storage/object_storage.zig").FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    try client.makeBucket("antfly");
    const bytes = try @import("../serverless/query/lake_parquet_rowgroup.zig").buildTestPlainI64ParquetObjectAlloc(a, &.{.{ .column_id = "amount", .values = &.{ 1, 2 }, .field_id = 1 }});
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
        const result = (try prepare(a, input, .{}, .{})).?;
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
            const table: @import("../sql/catalog.zig").Table = .{ .id = 7, .physical_name = "events", .schema_version = 1, .columns = &.{.{ .name = "amount", .path = "amount", .type = .integer, .nullable = false }}, .external_base_source = bound };
            const cursor = try @import("lake_sql_cursor.zig").open(a, table, .{ .fields = &.{"amount"}, .limit = 2 }, .{}, .{});
            defer cursor.close(cursor.ptr);
            try std.testing.expectEqual(@as(?u64, 0), try cursor.count_rows.?(cursor.ptr));
        }
    }
}

test "lake SQL inferred Parquet union supplies missing nullable columns and fences type changes" {
    const a = std.testing.allocator;
    var directory = try @import("../common/test_directory.zig").TestDirectory.init("lake-schema-union");
    defer directory.cleanup();
    var fs = try @import("../storage/object_storage.zig").FilesystemObjectStorage.init(a, directory.path());
    defer fs.deinit();
    var client = fs.client();
    const parquet = @import("../serverless/query/lake_parquet_rowgroup.zig");
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
    const catalog = @import("../sql/catalog.zig");
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
