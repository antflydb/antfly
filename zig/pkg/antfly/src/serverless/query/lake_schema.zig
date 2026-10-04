// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Metadata-only attachment schema discovery. Persisted SQL types never follow
//! data-file order or silently change with a newly published source snapshot.
const std = @import("std");
const metadata = @import("lake_parquet_metadata.zig");
const range = @import("lake_range_io.zig");
const parquet = @import("lake_parquet_rowgroup.zig");
const external = @import("../external_source/mod.zig");
const storage = @import("../../storage/object_storage.zig");
const A = std.mem.Allocator;
pub const Column = struct { name: []const u8, kind: []const u8, required: bool };
pub const Detected = struct {
    arena: std.heap.ArenaAllocator,
    columns: []const Column,
    fingerprint: []const u8,
    pub fn deinit(self: *Detected) void {
        self.arena.deinit();
    }
};
pub fn readFooter(a: A, reader: parquet.ObjectRangeReader, file: external.FileEntry) !metadata.ParsedFooter {
    const object = try range.objectRefForExternalFileUri(file);
    const read = try range.planParquetFooterRead(object, 64 * 1024);
    const tail = try reader.readPlannedAlloc(a, read);
    defer a.free(tail);
    const preflight = try @import("lake_parquet_footer.zig").parseFooterPreflight(object.byte_len, read.range.offset, tail);
    if (preflight.metadataSlice(tail)) |bytes| return metadata.parseFooterMetadataAlloc(a, bytes, file.byte_len);
    const bytes = try reader.readPlannedAlloc(a, try @import("lake_parquet_footer.zig").planFooterMetadataRead(object, read.range.offset, tail));
    defer a.free(bytes);
    return metadata.parseFooterMetadataAlloc(a, bytes, file.byte_len);
}
pub fn parquetKind(column: metadata.SchemaColumn) ![]const u8 {
    if (column.nested) return error.UnsupportedExternalLakeSchemaType;
    if (std.mem.eql(u8, column.logical_type, "decimal")) return "string"; // preserve decimal precision
    if (std.mem.startsWith(u8, column.logical_type, "timestamp_")) return "datetime";
    if (column.logical_type.len != 0 and !std.mem.eql(u8, column.logical_type, "string")) return error.UnsupportedExternalLakeSchemaType;
    return switch (column.physical_type orelse return error.InvalidParquetMetadata) {
        0 => "boolean",
        1, 2 => "integer",
        3 => "datetime",
        4, 5 => "number",
        6 => if (std.mem.eql(u8, column.logical_type, "string")) "string" else error.UnsupportedExternalLakeSchemaType,
        else => error.UnsupportedExternalLakeSchemaType,
    };
}
pub fn parquetSchema(a: A, inventory: external.Inventory, reader: parquet.ObjectRangeReader) !Detected {
    if (inventory.files.len == 0) return error.ExternalLakeSchemaUnavailable;
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    var columns: std.ArrayList(Column) = .empty;
    for (inventory.files, 0..) |file, file_index| {
        var footer = try readFooter(a, reader, file);
        defer footer.deinit(a);
        for (columns.items) |*column| {
            const present = for (footer.schema_columns) |leaf| {
                if (std.mem.eql(u8, column.name, leaf.column_id)) break true;
            } else false;
            if (!present) column.required = false;
        }
        for (footer.schema_columns) |leaf| {
            const kind = try parquetKind(leaf);
            if (std.mem.eql(u8, leaf.column_id, "_id") or std.mem.indexOfScalar(u8, leaf.column_id, '.') != null) return error.UnsupportedExternalLakeSchemaType;
            const prior = for (columns.items) |*column| {
                if (std.mem.eql(u8, column.name, leaf.column_id)) break column;
            } else null;
            if (prior) |column| {
                if (!std.mem.eql(u8, column.kind, kind)) return error.ExternalLakeSchemaMismatch;
                column.required = column.required and !leaf.nullable;
            } else {
                if (columns.items.len >= 1024) return error.ExternalLakeSchemaTooLarge;
                try columns.append(owned, .{ .name = try owned.dupe(u8, leaf.column_id), .kind = kind, .required = file_index == 0 and !leaf.nullable });
            }
        }
    }
    if (columns.items.len == 0) return error.ExternalLakeSchemaUnavailable;
    std.mem.sort(Column, columns.items, {}, struct {
        fn less(_: void, l: Column, r: Column) bool {
            return std.mem.order(u8, l.name, r.name) == .lt;
        }
    }.less);
    const bytes = try std.json.Stringify.valueAlloc(owned, columns.items, .{});
    var digest: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &digest, .{});
    const fingerprint = try std.fmt.allocPrint(owned, "parquet-schema:{s}", .{std.fmt.bytesToHex(digest, .lower)});
    return .{ .arena = arena, .columns = columns.items, .fingerprint = fingerprint };
}
pub fn icebergSchema(a: A, bytes: []const u8, snapshot: ?[]const u8) !Detected {
    var arena = std.heap.ArenaAllocator.init(a);
    errdefer arena.deinit();
    const owned = arena.allocator();
    const root = try std.json.parseFromSliceLeaky(std.json.Value, owned, bytes, .{});
    if (root != .object) return error.InvalidIcebergMetadata;
    const current = root.object.get("current-schema-id") orelse return error.InvalidIcebergMetadata;
    if (current != .integer) return error.InvalidIcebergMetadata;
    var schema_id = current.integer;
    if (snapshot) |wanted| {
        const snapshots = root.object.get("snapshots") orelse return error.IcebergSnapshotMismatch;
        if (snapshots != .array) return error.InvalidIcebergMetadata;
        var found = false;
        for (snapshots.array.items) |entry| {
            if (entry != .object) return error.InvalidIcebergMetadata;
            const id = entry.object.get("snapshot-id") orelse return error.InvalidIcebergMetadata;
            if (id != .integer) return error.InvalidIcebergMetadata;
            const text = try std.fmt.allocPrint(owned, "{d}", .{id.integer});
            if (!std.mem.eql(u8, wanted, text)) continue;
            const selected = entry.object.get("schema-id") orelse return error.ExternalLakeSchemaUnavailable;
            if (selected != .integer) return error.InvalidIcebergMetadata;
            schema_id = selected.integer;
            found = true;
            break;
        }
        if (!found) return error.IcebergSnapshotMismatch;
    }
    const iceberg = @import("../external_source/iceberg_metadata.zig");
    const fields = try iceberg.schemaFieldsForIdAlloc(owned, root.object, schema_id);
    if (fields.len == 0) return error.ExternalLakeSchemaUnavailable;
    if (fields.len > 1024) return error.ExternalLakeSchemaTooLarge;
    const columns = try owned.alloc(Column, fields.len);
    for (fields, columns) |field, *column| {
        if (std.mem.eql(u8, field.name, "_id") or std.mem.indexOfScalar(u8, field.name, '.') != null) return error.UnsupportedExternalLakeSchemaType;
        const kind: []const u8 = if (std.mem.eql(u8, field.type_name, "int") or std.mem.eql(u8, field.type_name, "long")) "integer" else if (std.mem.eql(u8, field.type_name, "float") or std.mem.eql(u8, field.type_name, "double")) "number" else if (std.mem.eql(u8, field.type_name, "boolean")) "boolean" else if (std.mem.eql(u8, field.type_name, "string") or std.mem.startsWith(u8, field.type_name, "decimal(")) "string" else if (std.mem.eql(u8, field.type_name, "timestamp") or std.mem.eql(u8, field.type_name, "timestamptz")) "datetime" else return error.UnsupportedExternalLakeSchemaType;
        column.* = .{ .name = field.name, .kind = kind, .required = field.required };
    }
    const fingerprint = try iceberg.schemaFingerprintAlloc(owned, root.object, schema_id);
    return .{ .arena = arena, .columns = columns, .fingerprint = fingerprint };
}

test "external lake Iceberg discovery selects pinned schema and preserves requiredness" {
    const a = std.testing.allocator;
    const bytes = "{\"current-schema-id\":8,\"schemas\":[{\"schema-id\":7,\"fields\":[{\"id\":1,\"name\":\"old\",\"required\":true,\"type\":\"int\"}]},{\"schema-id\":8,\"fields\":[{\"id\":1,\"name\":\"new\",\"required\":false,\"type\":\"long\"}]}],\"snapshots\":[{\"snapshot-id\":12,\"schema-id\":7}]}";
    var current = try icebergSchema(a, bytes, null);
    defer current.deinit();
    var pinned = try icebergSchema(a, bytes, "12");
    defer pinned.deinit();
    try std.testing.expectEqualStrings("new", current.columns[0].name);
    try std.testing.expectEqualStrings("old", pinned.columns[0].name);
    try std.testing.expect(!current.columns[0].required and pinned.columns[0].required);
    try std.testing.expectError(error.IcebergSnapshotMismatch, icebergSchema(a, bytes, "99"));
    try std.testing.expectError(error.UnsupportedExternalLakeSchemaType, parquetKind(.{ .column_id = @constCast("binary"), .nullable = false, .physical_type = 6 }));
    try std.testing.expectError(error.UnsupportedExternalLakeSchemaType, parquetKind(.{ .column_id = @constCast("nested"), .nullable = false, .physical_type = 2, .nested = true }));
}
